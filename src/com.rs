use anyhow::Context;
use bstr::{BString, ByteSlice};
use dashmap::DashMap;
use log::{error, trace};
use rustls::{
    client::danger::HandshakeSignatureValid, client::danger::ServerCertVerified,
    client::danger::ServerCertVerifier, DigitallySignedStruct, Error as TLSError, SignatureScheme,
};
use rustls_pki_types::{ServerName, UnixTime};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
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

use crate::commands::{self, AccessLevel, CommandBuilder, ReceiveOkResponseError};
use crate::data::{PlateData, PlatePointData};
use crate::message_receiver::{MsgPushError, MsgReceiveError, MsgRecv};

use lazy_static::lazy_static;

#[cfg(feature = "python")]
use pyo3::exceptions::PyValueError;
#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use pyo3::ToPyErr;

lazy_static! {
    static ref BASE64: data_encoding::Encoding = {
        let mut dec = data_encoding::BASE64.specification();
        dec.ignore.push('\n');
        dec.encoding().unwrap()
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
    #[error("Message push error: {0}")]
    MessagePushError(MsgPushError),
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
        self.receiver.push_data(&self.buf[..n]).unwrap();
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
                        Ok(MessageResponse::CommandError { ident, error }) => {
                            if let Some(channel) = self.messagechannels.get_mut(&ident) {
                                match channel
                                    .send(MessageResponse::CommandError { ident, error })
                                    .await
                                {
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
                    // FIXME: check for collisions
                    msg.ident = match msg.ident {
                        Some(MessageIdent::Number(n)) => Some(MessageIdent::Number(n)),
                        Some(MessageIdent::String(s)) => Some(MessageIdent::String(s)),
                        None => {
                            let i = Some(MessageIdent::Number(self.next_ident));
                            self.next_ident += 1;
                            i
                        }
                    };
                    let mut bytes = Vec::new();
                    if msg.content.is_some() {
                        msg.write_bytes(&mut bytes).unwrap();
                        self.stream_write.write_all(&bytes).await?;
                    }
                    self.messagechannels.insert(msg.ident.unwrap(), tx);
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
}

pub struct ResponseReceiver(mpsc::Receiver<MessageResponse>);

impl ResponseReceiver {
    pub async fn recv(&mut self) -> Option<MessageResponse> {
        self.0.recv().await
    }

    /// Get the OK or error response from the machine, ignoring NEXT messages.
    pub async fn get_response(
        &mut self,
    ) -> Result<Result<OkResponse, ErrorResponse>, ReceiveOkResponseError> {
        loop {
            let msg = self
                .recv()
                .await
                .ok_or(ReceiveOkResponseError::ConnectionClosed)?;
            match msg {
                MessageResponse::Ok { ident: _, message } => {
                    return Ok(Ok(message));
                }
                MessageResponse::CommandError { ident: _, error } => {
                    return Ok(Err(error));
                }
                _ => {}
            }
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
        Ok(ResponseReceiver(rx))
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
        Ok(ResponseReceiver(rx))
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
        Ok(ResponseReceiver(rx))
    }

    pub async fn connect(
        host: &str,
        port: u16,
        connection_type: ConnectionType,
    ) -> Result<QSConnection, ConnectionError> {
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

    pub async fn connect_with_timeout(
        host: &str,
        port: u16,
        connection_type: ConnectionType,
        timeout: Duration,
    ) -> Result<QSConnection, ConnectionError> {
        select! {
            conn = Self::connect(host, port, connection_type) => conn,
            _ = tokio::time::sleep(timeout) => Err(ConnectionError::Timeout),
        }
    }

    pub async fn connect_ssl(host: &str, port: u16) -> Result<QSConnection, ConnectionError> {
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
        let m = c.read(&mut b).await?;
        if m == 0 {
            return Err(ConnectionError::Timeout); // FIXME: handle error
        }
        trace!("Ready message: {:?}", String::from_utf8_lossy(&b[..]));
        let msg = parser::Ready::parse(&mut &b[..]).unwrap(); // FIXME: handle error
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
        let msg = parser::Ready::parse(&mut &b[..]).unwrap(); // FIXME: handle error
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

    pub async fn get_filterdata_one(
        &self,
        fref: FilterDataFilename,
        run: Option<String>,
    ) -> Result<PlateData, CommandError<ErrorResponse>> {
        let run = match run {
            Some(r) => r,
            None => self.get_run_title().await?,
        };
        let path = format!("{}/apldbio/sds/filter/{}", run, fref);
        let mut reply = self.send_command_bytes(path.as_bytes().as_bstr()).await?;
        let mut reply = reply.get_response().await??;

        let x = match reply
            .args
            .pop()
            .ok_or(CommandError::InternalError(anyhow::anyhow!(
                "Not enough arguments"
            )))? {
            Value::XmlString { value, .. } => value,
            _ => {
                return Err(CommandError::InternalError(anyhow::anyhow!(
                    "Invalid response"
                )))
            }
        };
        let plate_data: PlatePointData = quick_xml::de::from_str(&x.to_str_lossy())
            .with_context(|| "PlatePointData deserialization error")
            .map_err(CommandError::InternalError)?;

        // transform into first plate data
        let plate_data =
            plate_data
                .plate_data
                .into_iter()
                .next()
                .ok_or(CommandError::InternalError(anyhow::anyhow!(
                    "No plate data returned"
                )))?;

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
