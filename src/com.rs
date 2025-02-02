use dashmap::DashMap;
use log::trace;
use rustls::{
    client::danger::HandshakeSignatureValid, client::danger::ServerCertVerified,
    client::danger::ServerCertVerifier, DigitallySignedStruct, Error as TLSError, SignatureScheme,
};
use rustls_pki_types::{ServerName, UnixTime};
use std::pin::Pin;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};
use tokio::task::JoinHandle;
use tokio::{net::TcpStream, select};
use tokio_rustls::TlsConnector;
use tokio_rustls::{
    client::TlsStream,
    rustls::{ClientConfig, RootCertStore},
};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamMap;

#[cfg(feature = "python")]
use pyo3::exceptions::PyValueError;
#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use pyo3::ToPyErr;

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
}

use crate::parser::{self, Command, LogMessage, Message, MessageIdent, MessageResponse, Ready};
use regex::bytes::Regex;
use std::{collections::HashMap, sync::LazyLock};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{broadcast, mpsc},
};

// use pyo3::prelude::*;
static TAG_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^<(/?)([A-Za-z0-9_.-]*)(>|$)").unwrap());

/*
This isn't quite right: the quote mismatchees for InstrumentServer appear to be at a line level.
*/

#[derive(Error, Debug)]
pub enum MsgReceiveError {
    #[error("Unexpected close tag: </{1}> at {0}.")]
    UnexpectedCloseTag(usize, String),
    #[error("Mismatched close tag: </{2}> at {1} closes <{3}> at {0}.")]
    MismatchedCloseTag(usize, usize, String, String),
}

#[derive(Error, Debug)]
pub enum MsgPushError {
    #[error("Message waiting.")]
    MessageWaiting,
}

#[derive(Debug)]
pub struct MsgRecv {
    buf: Vec<u8>,
    tagstack: Vec<(Vec<u8>, usize)>,
    parttag: Option<usize>,
    msg_end: Option<usize>,
    msg_error: Option<MsgReceiveError>,
}

impl MsgRecv {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(1024),
            tagstack: Vec::new(),
            parttag: None,
            msg_end: None,
            msg_error: None,
        }
    }

    pub fn try_get_msg(&mut self) -> Result<Option<Vec<u8>>, MsgReceiveError> {
        match self.msg_end {
            Some(msg_end) => {
                let msg = Vec::from_iter(self.buf.drain(0..msg_end));
                self.msg_end = None;
                let cur_error = self.msg_error.take();
                self.tagstack.clear();
                self.parttag = None;
                let _another = self.check_from_pos(0).unwrap(); // We know no message is waiting now.
                if let Some(err) = cur_error {
                    return Err(err);
                }
                Ok(Some(msg))
            }
            None => Ok(None),
        }
    }

    fn check_from_pos(&mut self, start_pos: usize) -> Result<bool, MsgPushError> {
        if self.msg_error.is_some() || self.msg_end.is_some() {
            return Err(MsgPushError::MessageWaiting);
        }
        let mut pos = start_pos;
        while let Some(offset) = self.buf[pos..]
            .iter()
            .position(|&c| c == b'<' || c == b'\n' || c == b'>')
        {
            let c = self.buf[pos + offset];
            if c == b'\n' {
                if self.tagstack.len() == 0 {
                    self.msg_end = Some(pos + offset + 1);
                    return Ok(true);
                }
            } else if c == b'<' {
                match TAG_REGEX.captures(&self.buf[pos + offset..]) {
                    Some(captures) => {
                        let (_a, [close, tag, end]) = captures.extract();
                        if end == b"" {
                            self.parttag = Some(pos + offset);
                            return Ok(false);
                        } else {
                            if close == b"/" {
                                match self.tagstack.pop() {
                                    Some(old_tag) => {
                                        if old_tag.0 != tag {
                                            self.msg_error =
                                                Some(MsgReceiveError::MismatchedCloseTag(
                                                    old_tag.1,
                                                    pos + offset,
                                                    String::from_utf8_lossy(&old_tag.0).to_string(),
                                                    String::from_utf8_lossy(&tag).to_string(),
                                                ));
                                            self.tagstack.clear();
                                        }
                                    }
                                    None => {
                                        self.msg_error = Some(MsgReceiveError::UnexpectedCloseTag(
                                            pos + offset,
                                            String::from_utf8_lossy(&tag).to_string(),
                                        ));
                                        self.tagstack.clear();
                                    }
                                }
                            } else {
                                if self.msg_error.is_none() {
                                    self.tagstack.push((tag.to_vec(), pos + offset));
                                }
                            }
                        }
                    }
                    None => {}
                }
            }
            pos += offset + 1;
        }

        Ok(false)
    }

    pub fn push_data<'a>(&mut self, data: &'a [u8]) -> Result<bool, MsgPushError> {
        let last_pos = self.parttag.unwrap_or(self.buf.len());
        self.buf.extend_from_slice(&data);
        self.check_from_pos(last_pos)
    }

    pub fn reset(&mut self) {
        self.buf.clear();
        self.tagstack.clear();
        self.parttag = None;
        self.msg_end = None;
    }
}

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
    pub receiver: MsgRecv,
    pub logchannels: Arc<DashMap<String, broadcast::Sender<LogMessage>>>,
    pub messagechannels: HashMap<MessageIdent, mpsc::Sender<MessageResponse>>,
    pub commandchannel: mpsc::Receiver<(MessageIdent, mpsc::Sender<MessageResponse>)>,
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
    #[error("Message push error: {0}")]
    MessagePushError(MsgPushError),
}

impl QSConnectionInner {
    async fn handle_receive(&mut self, n: usize) -> Result<(), QSConnectionError> {
        trace!("Received data: {:?}", n);
        if n == 0 {
            return Ok(());
        }
        self.receiver.push_data(&self.buf[..n]).unwrap();
        trace!("Pushed data");
        'inner: loop {
            let msg = self.receiver.try_get_msg();
            match msg {
                Ok(Some(msg)) => {
                    let msg = MessageResponse::try_from(&msg[..]);
                    trace!("Received message: {:?}", msg);
                    match msg {
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
                            if let Some(channel) = self.messagechannels.get_mut(&ident) {
                                match channel.send(MessageResponse::Next { ident }).await {
                                    Ok(_) => (),
                                    Err(e) => {
                                        trace!("Error sending message: {:?}", e);
                                    }
                                }
                            } else {
                                trace!("No channel for message ident: {:?}", ident);
                            }
                        }
                        Ok(MessageResponse::Error { ident, error }) => {
                            if let Some(channel) = self.messagechannels.get_mut(&ident) {
                                match channel.send(MessageResponse::Error { ident, error }).await {
                                    Ok(_) => (),
                                    Err(e) => {
                                        trace!("Error sending message: {:?}", e);
                                    }
                                }
                            } else {
                                trace!("No channel for message ident: {:?}", ident);
                            }
                        }
                        Ok(MessageResponse::Ok { ident, message }) => {
                            if let Some(channel) = self.messagechannels.get_mut(&ident) {
                                match channel.send(MessageResponse::Ok { ident, message }).await {
                                    Ok(_) => (),
                                    Err(e) => {
                                        trace!("Error sending message: {:?}", e);
                                    }
                                }
                            } else {
                                trace!("No channel for message ident: {:?}", ident);
                            }
                        }
                        Err(e) => {
                            panic!("Error receiving message: {:?}", e);
                        }
                    }
                }
                Err(e) => {
                    panic!("Error receiving message: {:?}", e);
                }
                Ok(None) => break 'inner Ok(()),
            }
        }
    }

    pub async fn receive(&mut self) -> Result<(), QSConnectionError> {
        loop {
            let f_msg_to_send = self.commandchannel.recv();
            let f_data_to_receive = self.stream_read.read(&mut self.buf);

            select! {
                msg = f_msg_to_send => {
                    let (msg, tx) = msg.unwrap();
                    self.messagechannels.insert(msg, tx);
                }
                n = f_data_to_receive => {
                    let n = n?;
                    trace!("Receiving data");
                    self.handle_receive(n).await?;
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
    pub commandchannel: mpsc::Sender<(MessageIdent, mpsc::Sender<MessageResponse>)>,
    pub next_ident: u32,
    stream_write: WriteHalfOptions,
    pub logchannels: Arc<DashMap<String, broadcast::Sender<LogMessage>>>,
    pub ready_message: Ready
}

impl QSConnection {
    pub async fn send_command(
        &mut self,
        command: impl Into<Command>,
    ) -> Result<mpsc::Receiver<MessageResponse>, QSConnectionError> {
        let msg = Message {
            ident: Some(MessageIdent::Number(self.next_ident)),
            command: command.into(),
        };
        let (tx, rx) = mpsc::channel(5);
        self.commandchannel
            .send((msg.ident.clone().unwrap(), tx))
            .await
            .unwrap();

        // Convert message to bytes for logging
        let mut bytes = Vec::new();
        msg.write_bytes(&mut bytes)?;
        trace!("Sending: {}", String::from_utf8_lossy(&bytes));

        self.stream_write.write_all(&bytes).await?;
        self.next_ident += 1;
        Ok(rx)
    }

    pub async fn send_command_bytes(
        &mut self,
        bytes: &[u8],
    ) -> Result<mpsc::Receiver<MessageResponse>, QSConnectionError> {
        let ident = MessageIdent::Number(self.next_ident);
        let (tx, rx) = mpsc::channel(5);
        self.commandchannel.send((ident.clone(), tx)).await.unwrap();
        let mut buf = Vec::new();
        ident.write_bytes(&mut buf)?;
        buf.push(b' ');
        buf.extend_from_slice(bytes);
        buf.push(b'\n');
        trace!("Sending: {}", String::from_utf8_lossy(&buf));
        self.stream_write.write_all(&buf).await?;
        self.next_ident += 1;
        Ok(rx)
    }

    pub async fn connect(host: &str, port: u16, connection_type: ConnectionType) -> Result<QSConnection, ConnectionError> {
        match connection_type {
            ConnectionType::SSL => Self::connect_ssl(host, port).await,
            ConnectionType::TCP => Self::connect_tcp(host, port).await,
            ConnectionType::Auto => {

                // If port is 7443, use SSL
                // If port is 7000, use TCP
                // Otherwise, try an SSL connection first, then fall back to TCP
                if port == 7443 {
                    Self::connect_ssl(host, port).await
                } else if port == 7000 {
                    Self::connect_tcp(host, port).await
                } else {
                    match Self::connect_ssl(host, port).await {
                        Ok(conn) => Ok(conn),
                        Err(_) => Self::connect_tcp(host, port).await,
                    }
                }
            }
        }
    }

    pub async fn connect_ssl(
        host: &str,
        port: u16,
    ) -> Result<QSConnection, ConnectionError> {
        let root_cert_store = RootCertStore::empty();
        let mut config = ClientConfig::builder()
            .with_root_certificates(root_cert_store)
            .with_no_client_auth();

        config
            .dangerous()
            .set_certificate_verifier(Arc::new(NoVerifier));

        let connector = TlsConnector::from(Arc::new(config));
        let stream = TcpStream::connect((host, port)).await?;

        let mut c = connector
            .connect(ServerName::try_from(host.to_string())?, stream)
            .await?;

        let (com_tx, com_rx) = mpsc::channel(100);
        let logchannels = Arc::new(DashMap::new());

        // Read ready message
        let mut b = [0; 1024];
        c.read(&mut b).await?;
        trace!("Ready message: {:?}", String::from_utf8_lossy(&b[..]));
        let msg = parser::Ready::parse(&mut &b[..]).unwrap(); // FIXME: handle error
        trace!("Ready message: {:?}", msg);

        let (r, w) = tokio::io::split(c);
        let r = ReadHalfOptions::Tls(r);
        let w = WriteHalfOptions::Tls(w);

        let mut qsi = QSConnectionInner {
            stream_read: r,
            receiver: MsgRecv::new(),
            logchannels: logchannels.clone(),
            messagechannels: HashMap::new(),
            commandchannel: com_rx,
            buf: [0; 1024],
        };

        Ok(QSConnection {
            task: tokio::spawn(async move { qsi.receive().await }),
            commandchannel: com_tx,
            next_ident: 0,
            logchannels,
            stream_write: w,
            ready_message: msg,
        })
    }

    pub async fn connect_tcp(
        host: &str,
        port: u16,
    ) -> Result<QSConnection, ConnectionError> {
        let stream = TcpStream::connect((host, port)).await?;
        
        let (com_tx, com_rx) = mpsc::channel(100);
        let logchannels = Arc::new(DashMap::new());

        // Read ready message
        let mut b = [0; 1024];
        stream.readable().await?;
        let n = stream.try_read(&mut b)?;
        trace!("Ready message: {:?}", String::from_utf8_lossy(&b[..n]));
        let msg = parser::Ready::parse(&mut &b[..]).unwrap(); // FIXME: handle error
        trace!("Ready message: {:?}", msg);

        let (r, w) = tokio::io::split(stream);
        let r = ReadHalfOptions::Tcp(r);
        let w = WriteHalfOptions::Tcp(w);

        let mut qsi = QSConnectionInner {
            stream_read: r,
            receiver: MsgRecv::new(),
            logchannels: logchannels.clone(),
            messagechannels: HashMap::new(),
            commandchannel: com_rx,
            buf: [0; 1024],
        };

        Ok(QSConnection {
            task: tokio::spawn(async move { qsi.receive().await }),
            commandchannel: com_tx,
            next_ident: 0,
            logchannels,
            stream_write: w,
            ready_message: msg,
        })
    }

    pub async fn subscribe_log(
        &mut self,
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

    pub async fn is_connected(&self) -> bool {
        self.task.is_finished()
    }
}

pub enum ConnectionType {
    SSL,
    TCP,
    Auto
}