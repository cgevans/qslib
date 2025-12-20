use anyhow::Context;
use bstr::{BString, ByteSlice};
use dashmap::DashMap;
use hmac::{Hmac, Mac};
use log::{error, trace};
use md5::Md5;
type HmacMd5 = Hmac<Md5>;
use rustls::{
    client::danger::HandshakeSignatureValid, client::danger::ServerCertVerified,
    client::danger::ServerCertVerifier, DigitallySignedStruct, Error as TLSError, SignatureScheme,
};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio::{net::TcpStream, select};
use tokio_rustls::TlsConnector;
use tokio_rustls::{
    client::TlsStream,
    rustls::{ClientConfig, RootCertStore},
};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamMap;

use crate::commands::{self, AccessLevel, CommandBuilder, ReceiveOkResponseError};
use crate::data::{FilterDataCollection, PlateData};
use crate::message_receiver::{MsgReceiveError, MsgRecv};
use crate::plate_setup::PlateSetup;
use crate::protocol::Protocol;
use crate::parser::Command;

use lazy_static::lazy_static;
use std::fs::File;
use std::io::BufReader;

/// TLS configuration options for client certificate authentication
#[derive(Debug, Clone, Default)]
pub struct TlsConfig {
    /// Path to PEM file containing client certificate chain
    pub client_cert_path: Option<String>,
    /// Path to PEM file containing client private key (if separate from cert)
    pub client_key_path: Option<String>,
    /// Path to PEM file containing CA certificate(s) for server verification
    pub server_ca_path: Option<String>,
    /// Expected server name for TLS SNI and hostname verification.
    /// If None and server_ca_path is set, chain verification is performed but hostname is not checked.
    /// If None and server_ca_path is not set, no verification is performed (legacy behavior).
    pub tls_server_name: Option<String>,
}

impl TlsConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_client_cert(mut self, cert_path: &str, key_path: Option<&str>) -> Self {
        self.client_cert_path = Some(cert_path.to_string());
        self.client_key_path = key_path.map(|s| s.to_string());
        self
    }

    pub fn with_server_ca(mut self, ca_path: &str) -> Self {
        self.server_ca_path = Some(ca_path.to_string());
        self
    }

    pub fn with_server_name(mut self, name: &str) -> Self {
        self.tls_server_name = Some(name.to_string());
        self
    }
}

lazy_static! {
    static ref BASE64: data_encoding::Encoding = {
        let mut dec = data_encoding::BASE64.specification();
        dec.ignore.push('\n');
        dec.encoding().expect("Failed to create BASE64 encoding - this should never happen")
    };
    static ref FILTER_DATA_FILENAME_RE: regex::Regex =
        regex::Regex::new(r"S(\d+)_C(\d+)_T(\d+)_P(\d+)_M(\d)_X(\d)_filterdata\.xml$")
            .expect("Invalid regex");
    static ref FILTER_SET_RE: regex::Regex =
        regex::Regex::new(r"x(\d)-m(\d)").expect("Invalid regex");
}

#[derive(Debug)]
pub(crate) struct NoVerifier;

impl ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls_pki_types::CertificateDer,
        _intermediates: &[rustls_pki_types::CertificateDer],
        _server_name: &ServerName,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, TLSError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls_pki_types::CertificateDer,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TLSError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls_pki_types::CertificateDer,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TLSError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA1,
            SignatureScheme::ECDSA_SHA1_Legacy,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
            SignatureScheme::ED448,
        ]
    }
}

/// A certificate verifier that verifies the certificate chain against trusted roots
/// but does not verify the server hostname.
#[derive(Debug)]
pub(crate) struct ChainOnlyVerifier {
    roots: Arc<RootCertStore>,
}

impl ChainOnlyVerifier {
    pub fn new(roots: Arc<RootCertStore>) -> Self {
        Self { roots }
    }
}

impl ServerCertVerifier for ChainOnlyVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls_pki_types::CertificateDer,
        intermediates: &[rustls_pki_types::CertificateDer],
        _server_name: &ServerName,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, TLSError> {
        // Parse the end entity certificate
        let cert = webpki::EndEntityCert::try_from(end_entity).map_err(|_| {
            TLSError::InvalidCertificate(rustls::CertificateError::BadEncoding)
        })?;

        // Verify the certificate chain against our trusted roots
        cert.verify_for_usage(
            webpki::ALL_VERIFICATION_ALGS,
            &self.roots.roots,
            intermediates,
            now,
            webpki::KeyUsage::server_auth(),
            None, // No revocation checking
            None, // No budget limit
        )
        .map_err(|e| {
            TLSError::InvalidCertificate(match e {
                webpki::Error::CertExpired => rustls::CertificateError::Expired,
                webpki::Error::CertNotValidYet => rustls::CertificateError::NotValidYet,
                webpki::Error::UnknownIssuer => rustls::CertificateError::UnknownIssuer,
                webpki::Error::CertNotValidForName => rustls::CertificateError::NotValidForName,
                _ => rustls::CertificateError::BadEncoding,
            })
        })?;

        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls_pki_types::CertificateDer,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TLSError> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls_pki_types::CertificateDer,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TLSError> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[derive(Debug)]
pub struct IOConnection {
    pub stream: TlsStream<TcpStream>,
}

#[derive(Debug, Error)]
pub enum ConnectionError {
    #[error("TLS error: {0}")]
    TLSError(#[from] TLSError),
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("Invalid DNS name: {0}")]
    InvalidDnsNameError(#[from] rustls_pki_types::InvalidDnsNameError),
    #[error("Timeout")]
    Timeout,
}

use crate::parser::{
    self, ErrorResponse, LogMessage, Message, MessageIdent, MessageResponse, OkResponse, Ready,
    Value,
};
use std::collections::HashMap;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{broadcast, mpsc},
};

enum ReadHalfOptions {
    Tls(ReadHalf<TlsStream<TcpStream>>),
    Tcp(ReadHalf<TcpStream>),
}

impl AsyncRead for ReadHalfOptions {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        // Safety: we're not moving the data, just accessing it through the pin
        let this = unsafe { self.get_unchecked_mut() };
        match this {
            ReadHalfOptions::Tls(r) => Pin::new(r).poll_read(cx, buf),
            ReadHalfOptions::Tcp(r) => Pin::new(r).poll_read(cx, buf),
        }
    }
}

pub struct QSConnectionInner {
    stream_read: ReadHalfOptions,
    stream_write: WriteHalfOptions,
    pub receiver: MsgRecv,
    pub logchannels: Arc<DashMap<String, broadcast::Sender<LogMessage>>>,
    pub messagechannels: HashMap<MessageIdent, mpsc::Sender<MessageResponse>>,
    pub commandchannel: mpsc::Receiver<(Message, mpsc::Sender<MessageResponse>)>,
    next_ident: u32,
    buf: [u8; 1024],
}

#[derive(Error, Debug)]
pub enum QSConnectionError {
    #[error("Connection closed.")]
    ConnectionClosed,
    #[error("Message receive error: {0}")]
    MessageReceiveError(MsgReceiveError),
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("QS error: {0}")]
    QS(String),
    #[error("Command error: {0}")]
    CommandError(#[from] ErrorResponse),
}

#[derive(Error, Debug)]
pub enum QSCommError {
    #[error("Command error: {0}")]
    CommandError(#[from] ErrorResponse),
    #[error("Connection error: {0}")]
    ConnectionError(#[from] QSConnectionError),
    #[error("QS error: {0}")]
    QS(String),
}

impl QSConnectionInner {
    async fn handle_receive(&mut self, n: usize) {
        trace!(
            "Received data: {:?}",
            String::from_utf8_lossy(&self.buf[..n])
        );
        if n == 0 {
            return;
        }
        // push_data returns true if a message is ready, false otherwise
        let _ = self.receiver.push_data(&self.buf[..n]);
        'inner: loop {
            let msg = self.receiver.try_get_msg();
            match msg {
                Ok(Some(msg)) => {
                    let parsed_msg = MessageResponse::try_from(&msg[..]);
                    trace!("Received message: {:?}", parsed_msg);
                    match parsed_msg {
                        Ok(MessageResponse::Message(msg)) => {
                            if let Some(channel) = self.logchannels.get(&msg.topic) {
                                match channel.send(msg.clone()) {
                                    Ok(_) => (),
                                    Err(e) => {
                                        trace!("No topic listeners for: {:?}", e);
                                    }
                                }
                            }
                            if let Some(channel) = self.logchannels.get("*") {
                                match channel.send(msg.clone()) {
                                    Ok(_) => (),
                                    Err(e) => {
                                        trace!("No * listeners for: {:?}", e);
                                    }
                                }
                            }
                        }
                        Ok(MessageResponse::Next { ident }) => {
                            let ident_clone = ident.clone();
                            if let Some(channel) = self.messagechannels.get_mut(&ident) {
                                match channel.send(MessageResponse::Next { ident }).await {
                                    Ok(_) => {
                                        // Next is intermediate, keep channel for future responses
                                    }
                                    Err(_) => {
                                        // Receiver dropped, remove from HashMap to prevent leak
                                        self.messagechannels.remove(&ident_clone);
                                        trace!("Removed channel for ident {:?} after send failure", ident_clone);
                                    }
                                }
                            } else {
                                trace!("No channel for message ident: {:?}", ident_clone);
                            }
                        }
                        Ok(MessageResponse::CommandError { ident, error }) => {
                            // CommandError is final response, always remove channel
                            let ident_clone = ident.clone();
                            if let Some(channel) = self.messagechannels.get_mut(&ident) {
                                match channel
                                    .send(MessageResponse::CommandError { ident, error })
                                    .await
                                {
                                    Ok(_) => {
                                        // Successfully sent, remove channel
                                        self.messagechannels.remove(&ident_clone);
                                    }
                                    Err(_) => {
                                        // Receiver dropped, remove channel anyway
                                        self.messagechannels.remove(&ident_clone);
                                        trace!("Removed channel for ident {:?} after send failure", ident_clone);
                                    }
                                }
                            } else {
                                trace!("No channel for message ident: {:?}", ident_clone);
                            }
                        }
                        Ok(MessageResponse::Ok { ident, message }) => {
                            // OK is final response, always remove channel
                            let ident_clone = ident.clone();
                            if let Some(channel) = self.messagechannels.get_mut(&ident) {
                                match channel.send(MessageResponse::Ok { ident, message }).await {
                                    Ok(_) => {
                                        // Successfully sent, remove channel
                                        self.messagechannels.remove(&ident_clone);
                                    }
                                    Err(_) => {
                                        // Receiver dropped, remove channel anyway
                                        self.messagechannels.remove(&ident_clone);
                                        trace!("Removed channel for ident {:?} after send failure", ident_clone);
                                    }
                                }
                            } else {
                                trace!("No channel for message ident: {:?}", ident_clone);
                            }
                        }
                        Err(e) => {
                            error!(
                                "Error receiving message: {:?} ({:?})",
                                e,
                                String::from_utf8_lossy(&msg)
                            );
                        }
                    }
                }
                Err(e) => {
                    error!("Error receiving message: {:?}", e);
                }
                Ok(None) => break 'inner,
            }
        }
    }

    async fn inner_loop(&mut self) -> Result<(), std::io::Error> {
        loop {
            let f_msg_to_send = self.commandchannel.recv();
            let f_data_to_receive = self.stream_read.read(&mut self.buf);

            select! {
                msg = f_msg_to_send => {
                    let Some((mut msg, tx)) = msg else {
                        trace!("Outer channel is closed.");
                        break Ok(());
                    };
                    // Assign ident if not provided
                    msg.ident = match msg.ident {
                        Some(MessageIdent::Number(n)) => Some(MessageIdent::Number(n)),
                        Some(MessageIdent::String(s)) => Some(MessageIdent::String(s)),
                        None => {
                            let i = Some(MessageIdent::Number(self.next_ident));
                            self.next_ident += 1;
                            i
                        }
                    };
                    let ident = msg.ident.as_ref().ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "Message ident is None"
                        )
                    })?.clone();
                    
                    if self.messagechannels.contains_key(&ident) {
                        error!("Message ident collision detected: {:?}. This should not happen with auto-generated idents.", ident);
                    }
                    self.messagechannels.insert(ident, tx);
                    
                    let mut bytes = Vec::new();
                    if msg.content.is_some() {
                        msg.write_bytes(&mut bytes)?;
                        self.stream_write.write_all(&bytes).await?;
                    }
                }
                n = f_data_to_receive => {
                    let n = n?;
                    trace!("Receiving data");
                    self.handle_receive(n).await;
                }
            }
        }
    }
}

enum WriteHalfOptions {
    Tls(WriteHalf<TlsStream<TcpStream>>),
    Tcp(WriteHalf<TcpStream>),
}

impl AsyncWrite for WriteHalfOptions {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        // Safety: we're not moving the data, just accessing it through the pin
        let this = unsafe { self.get_unchecked_mut() };
        match this {
            WriteHalfOptions::Tls(w) => Pin::new(w).poll_write(cx, buf),
            WriteHalfOptions::Tcp(w) => Pin::new(w).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let this = unsafe { self.get_unchecked_mut() };
        match this {
            WriteHalfOptions::Tls(w) => Pin::new(w).poll_flush(cx),
            WriteHalfOptions::Tcp(w) => Pin::new(w).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let this = unsafe { self.get_unchecked_mut() };
        match this {
            WriteHalfOptions::Tls(w) => Pin::new(w).poll_shutdown(cx),
            WriteHalfOptions::Tcp(w) => Pin::new(w).poll_shutdown(cx),
        }
    }
}

pub struct QSConnection {
    pub task: JoinHandle<Result<(), QSConnectionError>>,
    pub commandchannel: mpsc::Sender<(Message, mpsc::Sender<MessageResponse>)>,
    pub connection_type: ConnectionType,
    pub host: String,
    pub port: u16,
    pub logchannels: Arc<DashMap<String, broadcast::Sender<LogMessage>>>,
    pub ready_message: Ready,
    pub initial_timeout: Duration,
    pub next_to_ok_timeout: Duration,
}

pub struct ResponseReceiver {
    receiver: mpsc::Receiver<MessageResponse>,
    initial_timeout: Option<Duration>,
    next_to_ok_timeout: Option<Duration>,
}

impl ResponseReceiver {
    pub async fn recv(&mut self) -> Option<MessageResponse> {
        self.receiver.recv().await
    }

    /// Get the OK or error response from the machine, ignoring NEXT messages.
    /// Uses connection's default timeouts: initial_timeout for first response, next_to_ok_timeout after NEXT.
    pub async fn get_response(
        &mut self,
    ) -> Result<Result<OkResponse, ErrorResponse>, ReceiveOkResponseError> {
        let initial = self.initial_timeout.ok_or(ReceiveOkResponseError::ConnectionClosed)?;
        let next_to_ok = self.next_to_ok_timeout.ok_or(ReceiveOkResponseError::ConnectionClosed)?;
        
        // Wait for first message (NEXT or OK/Error) with initial timeout
        let first_msg = match timeout(initial, self.recv()).await {
            Ok(Some(msg)) => msg,
            Ok(None) => return Err(ReceiveOkResponseError::ConnectionClosed),
            Err(_) => return Err(ReceiveOkResponseError::Timeout),
        };

        match first_msg {
            MessageResponse::Ok { ident: _, message } => Ok(Ok(message)),
            MessageResponse::CommandError { ident: _, error } => Ok(Err(error)),
            MessageResponse::Next { .. } => {
                // Received NEXT, now wait for OK/Error with next_to_ok_timeout
                loop {
                    match timeout(next_to_ok, self.recv()).await {
                        Ok(Some(msg)) => {
                            match msg {
                                MessageResponse::Ok { ident: _, message } => {
                                    return Ok(Ok(message));
                                }
                                MessageResponse::CommandError { ident: _, error } => {
                                    return Ok(Err(error));
                                }
                                MessageResponse::Next { .. } => {
                                    // Another NEXT, continue waiting with same timeout
                                    continue;
                                }
                                MessageResponse::Message(message) => {
                                    return Err(ReceiveOkResponseError::UnexpectedMessage(message));
                                }
                            }
                        }
                        Ok(None) => return Err(ReceiveOkResponseError::ConnectionClosed),
                        Err(_) => return Err(ReceiveOkResponseError::Timeout),
                    }
                }
            }
            MessageResponse::Message(message) => Err(ReceiveOkResponseError::UnexpectedMessage(message)),
        }
    }

    /// Get the OK or error response with a single timeout, ignoring connection defaults.
    /// Times out if OK/Error is not received within the specified timeout.
    pub async fn get_response_with_timeout(
        &mut self,
        timeout_duration: Duration,
    ) -> Result<Result<OkResponse, ErrorResponse>, ReceiveOkResponseError> {
        loop {
            match timeout(timeout_duration, self.recv()).await {
                Ok(Some(msg)) => {
                    match msg {
                        MessageResponse::Ok { ident: _, message } => return Ok(Ok(message)),
                        MessageResponse::CommandError { ident: _, error } => return Ok(Err(error)),
                        MessageResponse::Next { .. } => {
                            // Continue waiting with same timeout
                            continue;
                        }
                        MessageResponse::Message(message) => return Err(ReceiveOkResponseError::UnexpectedMessage(message)),
                    }
                }
                Ok(None) => return Err(ReceiveOkResponseError::ConnectionClosed),
                Err(_) => return Err(ReceiveOkResponseError::Timeout),
            }
        }
    }

    /// Get the OK or error response with custom timeouts for initial wait and post-NEXT wait.
    pub async fn get_response_with_next_and_ok_timeout(
        &mut self,
        initial: Duration,
        next_to_ok: Duration,
    ) -> Result<Result<OkResponse, ErrorResponse>, ReceiveOkResponseError> {
        // Wait for first message (NEXT or OK/Error) with initial timeout
        let first_msg = match timeout(initial, self.recv()).await {
            Ok(Some(msg)) => msg,
            Ok(None) => return Err(ReceiveOkResponseError::ConnectionClosed),
            Err(_) => return Err(ReceiveOkResponseError::Timeout),
        };

        match first_msg {
            MessageResponse::Ok { ident: _, message } => Ok(Ok(message)),
            MessageResponse::CommandError { ident: _, error } => Ok(Err(error)),
            MessageResponse::Next { .. } => {
                // Received NEXT, now wait for OK/Error with next_to_ok timeout
                loop {
                    match timeout(next_to_ok, self.recv()).await {
                        Ok(Some(msg)) => {
                            match msg {
                                MessageResponse::Ok { ident: _, message } => {
                                    return Ok(Ok(message));
                                }
                                MessageResponse::CommandError { ident: _, error } => {
                                    return Ok(Err(error));
                                }
                                MessageResponse::Next { .. } => {
                                    // Another NEXT, continue waiting with same timeout
                                    continue;
                                }
                                MessageResponse::Message(message) => {
                                    return Err(ReceiveOkResponseError::UnexpectedMessage(message));
                                }
                            }
                        }
                        Ok(None) => return Err(ReceiveOkResponseError::ConnectionClosed),
                        Err(_) => return Err(ReceiveOkResponseError::Timeout),
                    }
                }
            }
            MessageResponse::Message(message) => Err(ReceiveOkResponseError::UnexpectedMessage(message)),
        }
    }
}

impl CommandBuilder for String {
    const COMMAND: &'static [u8] = b"";
    type Response = String;
    type Error = ErrorResponse;
    fn to_bytes(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }

    fn write_command(&self, bytes: &mut impl std::io::Write) -> Result<(), QSConnectionError> {
        bytes.write_all(self.as_bytes())?;
        Ok(())
    }
}

impl CommandBuilder for &str {
    const COMMAND: &'static [u8] = b"";
    type Response = String;
    type Error = ErrorResponse;
    fn to_bytes(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

impl CommandBuilder for &[u8] {
    const COMMAND: &'static [u8] = b"";
    type Response = String;
    type Error = ErrorResponse;
    fn to_bytes(&self) -> Vec<u8> {
        self.to_vec()
    }
}

#[derive(Debug, Error)]
pub enum SendCommandError {
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("Connection closed: {0}")]
    ConnectionClosed(String),
}

impl QSConnection {
    pub async fn send_command(
        &self,
        command: impl CommandBuilder,
    ) -> Result<ResponseReceiver, SendCommandError> {
        let msg = Message {
            ident: None,
            content: Some(command.to_bytes().into()),
        };
        // Convert message to bytes for logging
        let mut bytes = Vec::new();
        msg.write_bytes(&mut bytes)?;
        trace!("Sending: {}", String::from_utf8_lossy(&bytes));

        let (tx, rx) = mpsc::channel(5);
        self.commandchannel
            .send((msg, tx))
            .await
            .map_err(|e| SendCommandError::ConnectionClosed(format!("{:?}", e)))?;
        Ok(ResponseReceiver {
            receiver: rx,
            initial_timeout: Some(self.initial_timeout),
            next_to_ok_timeout: Some(self.next_to_ok_timeout),
        })
    }

    pub async fn expect_ident(
        &self,
        ident: MessageIdent,
    ) -> Result<ResponseReceiver, SendCommandError> {
        let msg = Message {
            ident: Some(ident),
            content: None,
        };
        let (tx, rx) = mpsc::channel(5);
        self.commandchannel
            .send((msg, tx))
            .await
            .map_err(|e| SendCommandError::ConnectionClosed(format!("{:?}", e)))?;
        Ok(ResponseReceiver {
            receiver: rx,
            initial_timeout: Some(self.initial_timeout),
            next_to_ok_timeout: Some(self.next_to_ok_timeout),
        })
    }

    pub async fn send_command_bytes(
        &self,
        bytes: impl Into<BString>,
    ) -> Result<ResponseReceiver, SendCommandError> {
        let msg = Message {
            ident: None,
            content: Some(bytes.into()),
        };
        let (tx, rx) = mpsc::channel(5);
        self.commandchannel
            .send((msg, tx))
            .await
            .map_err(|e| SendCommandError::ConnectionClosed(format!("{:?}", e)))?;
        Ok(ResponseReceiver {
            receiver: rx,
            initial_timeout: Some(self.initial_timeout),
            next_to_ok_timeout: Some(self.next_to_ok_timeout),
        })
    }

    pub async fn connect(
        host: &str,
        port: u16,
        connection_type: ConnectionType,
    ) -> Result<QSConnection, ConnectionError> {
        Self::connect_with_config(host, port, connection_type, TlsConfig::default()).await
    }

    pub async fn connect_with_config(
        host: &str,
        port: u16,
        connection_type: ConnectionType,
        tls_config: TlsConfig,
    ) -> Result<QSConnection, ConnectionError> {
        match connection_type {
            ConnectionType::SSL => Self::connect_ssl_with_config(host, port, tls_config).await,
            ConnectionType::TCP => Self::connect_tcp(host, port).await,
            ConnectionType::Auto => {
                // If port is 7443, use SSL
                // If port is 7000, use TCP
                // Otherwise, try an SSL connection first, then fall back to TCP
                if port == 7443 {
                    Self::connect_ssl_with_config(host, port, tls_config).await
                } else if port == 7000 {
                    Self::connect_tcp(host, port).await
                } else {
                    match Self::connect_ssl_with_config(host, port, tls_config.clone()).await {
                        Ok(conn) => Ok(conn),
                        Err(_) => Self::connect_tcp(host, port).await,
                    }
                }
            }
        }
    }

    pub async fn connect_with_timeout(
        host: &str,
        port: u16,
        connection_type: ConnectionType,
        timeout: Duration,
    ) -> Result<QSConnection, ConnectionError> {
        Self::connect_with_timeout_and_config(host, port, connection_type, timeout, TlsConfig::default()).await
    }

    pub async fn connect_with_timeout_and_config(
        host: &str,
        port: u16,
        connection_type: ConnectionType,
        timeout: Duration,
        tls_config: TlsConfig,
    ) -> Result<QSConnection, ConnectionError> {
        select! {
            conn = Self::connect_with_config(host, port, connection_type, tls_config) => conn,
            _ = tokio::time::sleep(timeout) => Err(ConnectionError::Timeout),
        }
    }

    pub async fn connect_ssl(host: &str, port: u16) -> Result<QSConnection, ConnectionError> {
        Self::connect_ssl_with_config(host, port, TlsConfig::default()).await
    }

    pub async fn connect_ssl_with_config(
        host: &str,
        port: u16,
        tls_config: TlsConfig,
    ) -> Result<QSConnection, ConnectionError> {
        // Build root certificate store
        let root_cert_store = if let Some(ca_path) = &tls_config.server_ca_path {
            let mut store = RootCertStore::empty();
            let ca_file = File::open(ca_path).map_err(|e| {
                ConnectionError::IOError(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Failed to open CA file '{}': {}", ca_path, e),
                ))
            })?;
            let mut ca_reader = BufReader::new(ca_file);
            let certs = rustls_pemfile::certs(&mut ca_reader)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    ConnectionError::IOError(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Failed to parse CA certificates: {}", e),
                    ))
                })?;
            for cert in certs {
                store.add(cert).map_err(|e| {
                    ConnectionError::IOError(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Failed to add CA certificate: {}", e),
                    ))
                })?;
            }
            Arc::new(store)
        } else {
            Arc::new(RootCertStore::empty())
        };

        // Build client config with or without client auth
        let config = if let Some(cert_path) = &tls_config.client_cert_path {
            // Load client certificate chain
            let cert_file = File::open(cert_path).map_err(|e| {
                ConnectionError::IOError(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Failed to open client cert file '{}': {}", cert_path, e),
                ))
            })?;
            let mut cert_reader = BufReader::new(cert_file);
            let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    ConnectionError::IOError(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Failed to parse client certificates: {}", e),
                    ))
                })?;

            // Load private key - from separate file or same file as cert
            let key_path = tls_config.client_key_path.as_ref().unwrap_or(cert_path);
            let key_file = File::open(key_path).map_err(|e| {
                ConnectionError::IOError(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Failed to open key file '{}': {}", key_path, e),
                ))
            })?;
            let mut key_reader = BufReader::new(key_file);
            let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut key_reader)
                .map_err(|e| {
                    ConnectionError::IOError(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Failed to parse private key: {}", e),
                    ))
                })?
                .ok_or_else(|| {
                    ConnectionError::IOError(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("No private key found in '{}'", key_path),
                    ))
                })?;

            ClientConfig::builder()
                .with_root_certificates((*root_cert_store).clone())
                .with_client_auth_cert(certs, key)
                .map_err(|e| {
                    ConnectionError::IOError(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Failed to configure client auth: {}", e),
                    ))
                })?
        } else {
            ClientConfig::builder()
                .with_root_certificates((*root_cert_store).clone())
                .with_no_client_auth()
        };

        // Choose the appropriate certificate verifier:
        // - No CA file: NoVerifier (legacy, no verification)
        // - CA file + tls_server_name: default WebPki verification (chain + hostname)
        // - CA file + no tls_server_name: ChainOnlyVerifier (chain verification, no hostname check)
        let config = match (&tls_config.server_ca_path, &tls_config.tls_server_name) {
            (None, _) => {
                // No CA provided - disable all verification (legacy behavior)
                let mut config = config;
                config
                    .dangerous()
                    .set_certificate_verifier(Arc::new(NoVerifier));
                config
            }
            (Some(_), None) => {
                // CA provided but no server name - verify chain only, skip hostname
                let mut config = config;
                config
                    .dangerous()
                    .set_certificate_verifier(Arc::new(ChainOnlyVerifier::new(root_cert_store)));
                config
            }
            (Some(_), Some(_)) => {
                // CA and server name provided - use default verification (chain + hostname)
                // The default verifier was already configured via with_root_certificates
                config
            }
        };

        // Determine the server name for SNI and (if applicable) hostname verification
        let sni_server_name = tls_config
            .tls_server_name
            .as_deref()
            .unwrap_or(host);

        let connector = TlsConnector::from(Arc::new(config));
        let stream = TcpStream::connect((host, port)).await?;

        let mut c = connector
            .connect(ServerName::try_from(sni_server_name.to_string())?, stream)
            .await?;

        let (com_tx, com_rx) = mpsc::channel(100);
        let logchannels = Arc::new(DashMap::new());

        // Read ready message
        let mut b = [0; 1024];
        let m = c.read(&mut b).await?;
        if m == 0 {
            return Err(ConnectionError::Timeout);
        }
        trace!("Ready message: {:?}", String::from_utf8_lossy(&b[..m]));
        let msg = parser::Ready::parse(&mut &b[..m])
            .map_err(|e| ConnectionError::IOError(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to parse ready message: {}", e)
            )))?;
        trace!("Ready message: {:?}", msg);

        let (r, w) = tokio::io::split(c);
        let r = ReadHalfOptions::Tls(r);
        let w = WriteHalfOptions::Tls(w);

        let mut qsi = QSConnectionInner {
            stream_read: r,
            stream_write: w,
            next_ident: 0,
            receiver: MsgRecv::new(),
            logchannels: logchannels.clone(),
            messagechannels: HashMap::new(),
            commandchannel: com_rx,
            buf: [0; 1024],
        };

        Ok(QSConnection {
            task: tokio::spawn(async move {
                qsi.inner_loop().await.map_err(QSConnectionError::IOError)
            }),
            commandchannel: com_tx,
            logchannels,
            ready_message: msg,
            connection_type: ConnectionType::SSL,
            host: host.to_string(),
            port,
            initial_timeout: Duration::from_secs(30),
            next_to_ok_timeout: Duration::from_secs(600),
        })
    }

    pub async fn connect_tcp(host: &str, port: u16) -> Result<QSConnection, ConnectionError> {
        let stream = TcpStream::connect((host, port)).await?;

        let (com_tx, com_rx) = mpsc::channel(100);
        let logchannels = Arc::new(DashMap::new());

        // Read ready message
        let mut b = [0; 1024];
        stream.readable().await?;
        let n = stream.try_read(&mut b)?;
        trace!("Ready message: {:?}", String::from_utf8_lossy(&b[..n]));
        let msg = parser::Ready::parse(&mut &b[..n])
            .map_err(|e| ConnectionError::IOError(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to parse ready message: {}", e)
            )))?;
        trace!("Ready message: {:?}", msg);

        let (r, w) = tokio::io::split(stream);
        let r = ReadHalfOptions::Tcp(r);
        let w = WriteHalfOptions::Tcp(w);

        let mut qsi = QSConnectionInner {
            stream_read: r,
            stream_write: w,
            next_ident: 0,
            receiver: MsgRecv::new(),
            logchannels: logchannels.clone(),
            messagechannels: HashMap::new(),
            commandchannel: com_rx,
            buf: [0; 1024],
        };

        Ok(QSConnection {
            task: tokio::spawn(async move {
                qsi.inner_loop().await.map_err(QSConnectionError::IOError)
            }),
            commandchannel: com_tx,
            logchannels,
            ready_message: msg,
            connection_type: ConnectionType::TCP,
            host: host.to_string(),
            port,
            initial_timeout: Duration::from_secs(30),
            next_to_ok_timeout: Duration::from_secs(600),
        })
    }

    pub async fn subscribe_log(
        &self,
        topics: &[&str],
    ) -> StreamMap<String, BroadcastStream<LogMessage>> {
        let mut s = StreamMap::new();
        for &topic in topics {
            if !self.logchannels.contains_key(topic) {
                let (tx, _) = broadcast::channel(100);
                self.logchannels.insert(topic.to_string(), tx);
            }
            if let Some(channel) = self.logchannels.get(topic) {
                s.insert(topic.to_string(), BroadcastStream::new(channel.subscribe()));
            }
        }
        s
    }

    /// Check if the connection is still active.
    ///
    /// This works by checking if the task is still running. If the connection
    /// is hanging, this might return true.
    pub async fn is_connected(&self) -> bool {
        !self.task.is_finished()
    }

    pub async fn get_exp_file(&self, path: &str) -> Result<Vec<u8>, CommandError<ErrorResponse>> {
        let cmd = format!("EXP:READ? -encoding=base64 {}", path);
        let mut reply = self.send_command_bytes(cmd.as_bytes().as_bstr()).await?;
        let mut reply = reply.get_response().await??;

        let x = match reply
            .args
            .pop()
            .ok_or(CommandError::InternalError(anyhow::anyhow!(
                "Invalid response"
            )))? {
            Value::XmlString { value, .. } => value,
            _ => {
                return Err(CommandError::InternalError(anyhow::anyhow!(
                    "Invalid response"
                )))
            }
        };
        BASE64
            .decode(&x)
            .map_err(|e| CommandError::InternalError(anyhow::anyhow!("Invalid response: {}", e)))
    }

    pub async fn get_sds_file(
        &self,
        path: &str,
        runtitle: Option<String>,
    ) -> Result<Vec<u8>, CommandError<ErrorResponse>> {
        let runtitle = match runtitle {
            Some(rt) => rt,
            None => self.get_run_title().await?,
        };
        self.get_exp_file(&format!("{}/apldbio/sds/{}", runtitle, path))
            .await
    }

    pub async fn get_expfile_list(
        &self,
        glob: &str,
    ) -> Result<Vec<String>, CommandError<ErrorResponse>> {
        let cmd = format!("EXP:LIST? {}", glob);
        let mut reply = self.send_command_bytes(cmd.as_bytes().as_bstr()).await?;
        let mut reply = reply.get_response().await??;

        let x = match reply
            .args
            .pop()
            .ok_or(CommandError::InternalError(anyhow::anyhow!(
                "Invalid response"
            )))? {
            Value::XmlString { value, .. } => value,
            _ => {
                return Err(CommandError::InternalError(anyhow::anyhow!(
                    "Invalid response"
                )))
            }
        };
        let x = x.to_string();
        let x = x.split("\n").collect::<Vec<&str>>();
        Ok(x.iter().map(|s| s.to_string()).collect())
    }

    /// Get the current run title from the machine
    pub async fn get_run_title(&self) -> Result<String, CommandError<ErrorResponse>> {
        let mut response = self.send_command_bytes(b"RUNTitle?".as_bstr()).await?;
        let response = response.get_response().await??;

        // Get the first argument which should be the run title
        let title = response
            .args
            .first()
            .ok_or_else(|| CommandError::InternalError(anyhow::anyhow!("No run title returned")))?;

        // Convert to string and trim any quotes
        let title_str = title.to_string().trim_matches('"').to_string();

        Ok(title_str)
    }

    pub async fn get_plate_setup(&self, run: Option<String>) -> Result<PlateSetup, CommandError<ErrorResponse>> {
        let path = match run {
            Some(r) => format!("{}/apldbio/sds/plate_setup.xml", r),
            None => "${LogFolder}/plate_setup.xml".to_string(),
        };
        let x = self.get_exp_file(&path).await?;
        let plate_setup: PlateSetup = quick_xml::de::from_str(&x.to_str_lossy())
            .with_context(|| "PlateSetup deserialization error")
            .map_err(CommandError::InternalError)?;

        Ok(plate_setup)
    }

    pub async fn get_current_run_name(&self) -> Result<Option<String>, CommandError<ErrorResponse>> {
        let mut response = self.send_command_bytes(b"RUNTitle?".as_bstr()).await?;
        let response = response.get_response().await??;
        let title = response
            .args
            .first()
            .ok_or_else(|| CommandError::InternalError(anyhow::anyhow!("No run title returned")))?;
        if title.to_string() == "-" {
            Ok(None)
        } else {
            Ok(Some(title.to_string().trim_matches('"').to_string()))
        }
    }

    pub async fn get_running_protocol_string(&self) -> Result<String, CommandError<ErrorResponse>> {
        // Check if there's an active run
        let run_name = self.get_current_run_name().await?;
        if run_name.is_none() {
            return Err(CommandError::InternalError(anyhow::anyhow!(
                "No protocol is currently running"
            )));
        }

        // Get protocol content
        let mut response = self.send_command_bytes(b"PROT? ${Protocol}".as_bstr()).await?;
        let response = response.get_response().await??;
        let protocol_content = response
            .args
            .first()
            .ok_or_else(|| CommandError::InternalError(anyhow::anyhow!("No protocol content returned")))?
            .to_string();

        // Get protocol name, volume, and runmode
        let mut response = self.send_command_bytes(b"RET ${Protocol} ${SampleVolume} ${RunMode}".as_bstr()).await?;
        let response = response.get_response().await??;
        let parts: Vec<String> = response
            .args
            .iter()
            .map(|v| v.to_string())
            .collect();
        
        if parts.len() < 3 {
            return Err(CommandError::InternalError(anyhow::anyhow!(
                "No protocol is currently running (RET command returned {} values instead of 3)",
                parts.len()
            )));
        }

        let protocol_name = parts[0].clone();
        let sample_volume = parts[1].clone();
        let run_mode = parts[2].clone();


        println!("protocol_content: {}", protocol_content);

        // Construct full PROT command string
        let prot_command = format!(
            "PROT -volume={} -runmode={} {} <multiline.protocol>\n{}\n</multiline.protocol>",
            sample_volume, run_mode, protocol_name, protocol_content
        );

        Ok(prot_command)
    }

    pub async fn get_running_protocol(&self) -> Result<Protocol, CommandError<ErrorResponse>> {
        let prot_command = self.get_running_protocol_string().await?;

        println!("prot_command: {}", prot_command);

        // Parse into Command and then Protocol
        let cmd = Command::try_from(prot_command.clone())
            .map_err(|e| CommandError::InternalError(anyhow::anyhow!("Failed to parse protocol command: {}", e)))?;
        
        Protocol::from_scpicommand(&cmd)
            .map_err(|e| CommandError::InternalError(anyhow::anyhow!("Failed to parse protocol: {}", e)))
    }


    // In [8]: m.run_command("TBC:SETT?")
    // Out[8]: '-Zone1=25 -Zone2=25 -Zone3=25 -Zone4=25 -Zone5=25 -Zone6=25 -Fan1=44 -Cover=105'
    pub async fn get_current_temperature_setpoints(&self) -> Result<(Vec<f64>, Vec<f64>, f64), CommandError<ErrorResponse>> {
        let mut response = self.send_command_bytes(b"TBC:SETT?".as_bstr()).await?;
        let response = response.get_response().await??;
        let setpoints = response.options;
        let zones: Result<Vec<f64>, _> = setpoints.iter()
            .filter(|(s, _v)| s.starts_with("Zone"))
            .map(|(_s, v)| v.clone().try_into_f64().map_err(|e| CommandError::InternalError(anyhow::anyhow!("Failed to parse zone temperature: {}", e))))
            .collect();
        let fans: Result<Vec<f64>, _> = setpoints.iter()
            .filter(|(s, _v)| s.starts_with("Fan"))
            .map(|(_s, v)| v.clone().try_into_f64().map_err(|e| CommandError::InternalError(anyhow::anyhow!("Failed to parse fan temperature: {}", e))))
            .collect();
        let cover = setpoints.iter()
            .filter(|(s, _v)| s.starts_with("Cover"))
            .map(|(_s, v)| v.clone().try_into_f64())
            .next()
            .ok_or_else(|| CommandError::InternalError(anyhow::anyhow!("No Cover temperature found in response")))?
            .map_err(|e| CommandError::InternalError(anyhow::anyhow!("Failed to parse cover temperature: {}", e)))?;
        Ok((zones?, fans?, cover))
    }

    pub async fn get_filterdata_one(
        &self,
        fref: FilterDataFilename,
        run: Option<String>,
    ) -> Result<PlateData, CommandError<ErrorResponse>> {
        let path = match run {
            Some(r) => format!("{}/apldbio/sds/filter/{}", r, fref),
            None => format!("${{FilterFolder}}/{}", fref),
        };
        let x = self.get_exp_file(&path).await?;

        let filter_data_collection: FilterDataCollection = quick_xml::de::from_str(&x.to_str_lossy())
            .with_context(|| "PlatePointData deserialization error")
            .map_err(CommandError::InternalError)?;

        // Directly access the first PlateData by value without unnecessary clones, if possible.
        // Since we need to return an owned PlateData (not a reference), we can implement this 
        // by consuming the collection to extract the value. 
        // If there are no entries, return an error instead of panicking.
        let plate_point_data = filter_data_collection.plate_point_data.into_iter().next()
            .ok_or_else(|| CommandError::InternalError(anyhow::anyhow!("No PlatePointData found")))?;
        let plate_data = plate_point_data.plate_data.into_iter().next()
            .ok_or_else(|| CommandError::InternalError(anyhow::anyhow!("No PlateData found")))?;
        Ok(plate_data)
    }

    pub async fn set_access_level(
        &self,
        level: AccessLevel,
    ) -> Result<(), CommandError<ErrorResponse>> {
        commands::AccessLevelSet::level(level)
            .send(self)
            .await?
            .receive_response()
            .await??;
        Ok(())
    }

    /// Authenticate with the machine using HMAC-MD5 challenge-response.
    pub async fn authenticate(
        &self,
        password: &str,
    ) -> Result<(), CommandError<ErrorResponse>> {
        // Get challenge
        let mut challenge_recv = self.send_command_bytes(b"CHAL?").await?;
        let challenge_result = challenge_recv
            .get_response()
            .await
            .map_err(|e| CommandError::InternalError(anyhow::anyhow!("Failed to get challenge: {}", e)))?;
        
        let challenge_response = challenge_result
            .map_err(|e| CommandError::InternalError(anyhow::anyhow!("Challenge command failed: {}", e)))?;
        
        let challenge_str = challenge_response
            .args
            .first()
            .ok_or_else(|| CommandError::InternalError(anyhow::anyhow!("No challenge in response")))?
            .clone()
            .try_into_string()
            .map_err(|e| CommandError::InternalError(anyhow::anyhow!("Challenge is not a string: {:?}", e)))?;

        // Compute HMAC-MD5
        let mut mac = HmacMd5::new_from_slice(password.as_bytes())
            .map_err(|e| CommandError::InternalError(anyhow::anyhow!("HMAC error: {}", e)))?;
        mac.update(challenge_str.as_bytes());
        let auth_response = hex::encode(mac.finalize().into_bytes());

        // Send AUTH command
        let auth_cmd = Command::new("AUTH").with_arg(auth_response);
        let mut auth_recv = self.send_command(auth_cmd).await?;
        let auth_result = auth_recv
            .get_response()
            .await
            .map_err(|e| CommandError::InternalError(anyhow::anyhow!("Auth recv error: {}", e)))?;
        
        auth_result.map_err(|e| CommandError::InternalError(anyhow::anyhow!("Authentication failed: {}", e)))?;

        Ok(())
    }

    /// Authenticate and set access level in one call.
    pub async fn authenticate_and_set_access_level(
        &self,
        password: &str,
        level: AccessLevel,
    ) -> Result<(), CommandError<ErrorResponse>> {
        self.authenticate(password).await?;
        self.set_access_level(level).await?;
        Ok(())
    }

    pub async fn get_access_level(
        &self,
    ) -> anyhow::Result<AccessLevel, CommandError<ErrorResponse>> {
        let response = commands::AccessLevelQuery
            .send(self)
            .await?
            .receive_response()
            .await??;
        Ok(response)
    }

    pub async fn abort_current_run(
        &self,
    ) -> Result<Result<(), ErrorResponse>, CommandError<ErrorResponse>> {
        let mut response = commands::AbortRun("${RunTitle}".to_string()).send(self).await?;
        match response.receive_response().await? {
            Ok(_) => Ok(Ok(())),
            Err(e) => Ok(Err(e)),
        }
    }

    pub async fn abort_run(
        &self,
        run_title: &str,
    ) -> Result<Result<(), ErrorResponse>, CommandError<ErrorResponse>> {
        let mut response = commands::AbortRun(run_title.to_string()).send(self).await?;
        match response.receive_response().await? {
            Ok(_) => Ok(Ok(())),
            Err(e) => Ok(Err(e)),
        }
    }

    pub async fn stop_current_run(
        &self,
    ) -> Result<Result<(), ErrorResponse>, CommandError<ErrorResponse>> {
        let mut response = commands::StopRun("${RunTitle}".to_string()).send(self).await?;
        match response.receive_response().await? {
            Ok(_) => Ok(Ok(())),
            Err(e) => Ok(Err(e)),
        }
    }

    pub async fn stop_run(
        &self,
        run_title: &str,
    ) -> Result<Result<(), ErrorResponse>, CommandError<ErrorResponse>> {
        let mut response = commands::StopRun(run_title.to_string()).send(self).await?;
        match response.receive_response().await? {
            Ok(_) => Ok(Ok(())),
            Err(e) => Ok(Err(e)),
        }
    }
}

#[derive(Debug, Error)]
pub enum CommandError<T: From<ErrorResponse>> {
    #[error("Error sending command: {0}")]
    SendCommandError(
        #[source]
        #[from]
        SendCommandError,
    ),
    #[error("Error parsing response: {0}")]
    ParseResponseError(
        #[source]
        #[from]
        ReceiveOkResponseError,
    ),
    #[error("Command error: {0}")]
    CommandError(
        #[source]
        #[from]
        T,
    ),
    #[error("Internal error: {0}")]
    InternalError(#[source] anyhow::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionType {
    SSL,
    TCP,
    Auto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FilterDataFilename {
    pub filterset: FilterSet,
    pub stage: u32,
    pub cycle: u32,
    pub step: u32,
    pub point: u32,
}

impl std::fmt::Display for FilterDataFilename {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "S{:02}_C{:03}_T{:02}_P{:04}_M{}_X{}_filterdata.xml",
            self.stage, self.cycle, self.step, self.point, self.filterset.em, self.filterset.ex
        )
    }
}

impl FilterDataFilename {
    pub fn from_string(s: &str) -> Result<Self, QSConnectionError> {
        let caps = FILTER_DATA_FILENAME_RE.captures(s).ok_or_else(|| {
            QSConnectionError::QS("Invalid filter data filename format".to_string())
        })?;

        Ok(Self {
            stage: caps[1]
                .parse()
                .map_err(|_| QSConnectionError::QS("Invalid stage number".to_string()))?,
            cycle: caps[2]
                .parse()
                .map_err(|_| QSConnectionError::QS("Invalid cycle number".to_string()))?,
            step: caps[3]
                .parse()
                .map_err(|_| QSConnectionError::QS("Invalid step number".to_string()))?,
            point: caps[4]
                .parse()
                .map_err(|_| QSConnectionError::QS("Invalid point number".to_string()))?,
            filterset: FilterSet::from_string(&format!("x{}-m{}", &caps[6], &caps[5]))?,
        })
    }



    pub fn is_same_point(&self, other: &FilterDataFilename) -> bool {
        self.stage == other.stage
            && self.cycle == other.cycle
            && self.step == other.step
            && self.point == other.point
    }
}

impl TryFrom<FilterDataFilename> for String {
    type Error = QSConnectionError;

    fn try_from(value: FilterDataFilename) -> Result<Self, Self::Error> {
        Ok(value.to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FilterSet {
    pub em: u8,
    pub ex: u8,
}

impl FilterSet {
    pub fn from_string(s: &str) -> Result<Self, QSConnectionError> {
        let caps = FILTER_SET_RE
            .captures(s)
            .ok_or_else(|| QSConnectionError::QS("Invalid filter set format".to_string()))?;

        Ok(Self {
            ex: caps[1].parse().map_err(|_| {
                QSConnectionError::QS("Invalid excitation filter number".to_string())
            })?,
            em: caps[2]
                .parse()
                .map_err(|_| QSConnectionError::QS("Invalid emission filter number".to_string()))?,
        })
    }


}
impl std::fmt::Display for FilterSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "x{}-m{}", self.ex, self.em)
    }
}
