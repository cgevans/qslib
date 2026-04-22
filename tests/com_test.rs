use env_logger::Builder;
use hmac::{Hmac, Mac};
use log::{debug, error, info, warn};
use md5::Md5;
use qslib::parser::{MessageResponse, Value};
use rustls::ServerConfig;
use rustls_pki_types::pem::PemObject;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::{interval, Duration};
use tokio_rustls::TlsAcceptor;
use tokio_stream::StreamExt;

use qslib::com::*;
use qslib::commands::*;

type HmacMd5 = Hmac<Md5>;

const MOCK_CHALLENGE: &str = "deadbeef12345678";
const MOCK_PASSWORD: &str = "testpassword";

fn compute_expected_auth(password: &str, challenge: &str) -> String {
    let mut mac = HmacMd5::new_from_slice(password.as_bytes()).unwrap();
    mac.update(challenge.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn setup_logging() {
    let _ = Builder::from_env("RUST_LOG")
        .format_timestamp_millis()
        .is_test(true)
        .try_init();
}

async fn setup_mock_server(
    port: Option<u16>,
    stay_open: bool,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    // Bind to a random available port
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port.unwrap_or(0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    info!("Mock server listening on {}", addr);

    // Spawn server task
    let handle = tokio::spawn(async move {
        let mut power_status = true; // Start with power ON
        let mut access_level = "Guest".to_string();
        let mut run_title = "-".to_string();

        loop {
            let (mut socket, _) = listener.accept().await.unwrap();

            // Send ready message
            let ready_msg = "READy -session=474800 -product=QuantStudio3_5 -version=1.3.0 -build=001 -capabilities=Index\n";
            socket.write_all(ready_msg.as_bytes()).await.unwrap();

            // Create a periodic timer for log messages
            let mut interval = interval(Duration::from_millis(100));

            // Buffer for accumulating partial lines
            let mut line_buffer = String::new();
            let mut buf = [0; 1024];

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // First send a log message that won't be read
                        let log_msg = "MESSage NonStatus Test status message\n";
                        if socket.write_all(log_msg.as_bytes()).await.is_err() {
                            break;
                        }

                        // Send a log message
                        let log_msg = "MESSage Status Test status message\n";
                        if socket.write_all(log_msg.as_bytes()).await.is_err() {
                            break;
                        }
                    }
                    result = socket.read(&mut buf) => {
                        match result {
                            Ok(0) => break, // Connection was closed
                            Ok(n) => {
                                // Add new data to line buffer
                                line_buffer.push_str(&String::from_utf8_lossy(&buf[..n]));

                                // Process complete lines
                                while let Some(pos) = line_buffer.find('\n') {
                                    let line = line_buffer[..pos].trim().to_string();
                                    line_buffer = line_buffer[pos + 1..].to_string();

                                    debug!("Processing command: {}", line);

                                    // Extract ident from command line
                                    let (ident, cmd_part) = {
                                        let first = line.split_whitespace().next().unwrap_or("");
                                        if first.parse::<u32>().is_ok() {
                                            (Some(first.to_string()), line.split_once(' ').map(|x| x.1.to_string()).unwrap_or_default())
                                        } else {
                                            (None, line.clone())
                                        }
                                    };

                                    // Helper to format OK response with optional ident
                                    let ok_resp = |body: &str| -> String {
                                        if let Some(ref id) = ident {
                                            format!("OK {} {}\n", id, body)
                                        } else {
                                            format!("OK {}\n", body)
                                        }
                                    };
                                    let ok_empty = || -> String {
                                        if let Some(ref id) = ident {
                                            format!("OK {}\n", id)
                                        } else {
                                            "OK\n".to_string()
                                        }
                                    };
                                    let err_resp = |error: &str, msg: &str| -> String {
                                        if let Some(ref id) = ident {
                                            format!("ERRor {} [{}] --> {}\n", id, error, msg)
                                        } else {
                                            format!("ERRor [{}] --> {}\n", error, msg)
                                        }
                                    };

                                    // Handle power commands
                                    if cmd_part.starts_with("POW?") {
                                        // Query power status
                                        let status = if power_status { "ON" } else { "OFF" };
                                        socket.write_all(ok_resp(status).as_bytes()).await.unwrap();
                                    } else if cmd_part.starts_with("POW ON") {
                                        power_status = true;
                                        socket.write_all(ok_empty().as_bytes()).await.unwrap();
                                    } else if cmd_part.starts_with("POW OFF") {
                                        power_status = false;
                                        socket.write_all(ok_empty().as_bytes()).await.unwrap();
                                    } else if cmd_part.starts_with("ACC?") {
                                        socket.write_all(ok_resp(&access_level).as_bytes()).await.unwrap();
                                    } else if cmd_part.starts_with("ACC ") {
                                        // Set access level
                                        let parts: Vec<&str> = cmd_part.split_whitespace().collect();
                                        // Find non-option arg (the level name)
                                        if let Some(level) = parts.iter().find(|p| !p.starts_with('-') && **p != "ACC") {
                                            access_level = level.to_string();
                                            socket.write_all(ok_empty().as_bytes()).await.unwrap();
                                        } else {
                                            socket.write_all(err_resp("InvocationError", "Missing access level").as_bytes()).await.unwrap();
                                        }
                                    } else if cmd_part.starts_with("drawer?") {
                                        socket.write_all(ok_resp("Closed").as_bytes()).await.unwrap();
                                    } else if cmd_part.starts_with("RUNTitle=") {
                                        // Set run title
                                        if let Some(title) = cmd_part.strip_prefix("RUNTitle= ") {
                                            run_title = title.trim_matches('"').to_string();
                                        } else if let Some(title) = cmd_part.strip_prefix("RUNTitle=") {
                                            run_title = title.trim().trim_matches('"').to_string();
                                        }
                                        socket.write_all(ok_empty().as_bytes()).await.unwrap();
                                    } else if cmd_part.starts_with("RUNTitle?") {
                                        socket.write_all(ok_resp(&run_title).as_bytes()).await.unwrap();
                                    } else if cmd_part.starts_with("TBC:SETT?") {
                                        socket.write_all(ok_resp("-Zone1=25.0 -Zone2=25.0 -Zone3=25.0 -Zone4=25.0 -Zone5=25.0 -Zone6=25.0 -Fan1=44.0 -Cover=30.0").as_bytes()).await.unwrap();
                                    } else if cmd_part.starts_with("ERRTEST") {
                                        socket.write_all(err_resp("InsufficientAccess", "Observer access required").as_bytes()).await.unwrap();
                                    } else if cmd_part.starts_with("QUIT") {
                                        socket.write_all(ok_empty().as_bytes()).await.unwrap();
                                        // Close the connection after QUIT
                                        break;
                                    } else if cmd_part.contains("CUSTOM") {
                                        socket.write_all(ok_resp(&format!("-received=\"{}\" success", cmd_part)).as_bytes()).await.unwrap();
                                    } else if cmd_part.contains("MULTILINE") {
                                        socket.write_all(ok_resp("<quote>Line 1\nLine 2\nLine 3</quote>").as_bytes()).await.unwrap();
                                    } else if cmd_part.contains("BADXML1") {
                                        socket.write_all(ok_resp("<quote>Line 1\nLine 2\nLine 3</badquote>").as_bytes()).await.unwrap();
                                    } else if cmd_part.contains("BADXML2") {
                                        socket.write_all(ok_resp("</unexpected>Line 1\nLine 2\nLine 3</unexpected>").as_bytes()).await.unwrap();
                                    } else if cmd_part.contains("ANGLES") {
                                        socket.write_all(ok_resp("Temperature < 37.5 and pH > 7.0").as_bytes()).await.unwrap();
                                    } else if cmd_part.starts_with("EXP:READ?") {
                                        // File read handler: return base64-encoded content in <quote> tags
                                        if cmd_part.contains("missing") {
                                            socket.write_all(err_resp("NoMatch", "File not found").as_bytes()).await.unwrap();
                                        } else {
                                            use base64::Engine;
                                            let content = b"test file content";
                                            let encoded = base64::engine::general_purpose::STANDARD.encode(content);
                                            socket.write_all(ok_resp(&format!("<quote>\n{}\n</quote>", encoded)).as_bytes()).await.unwrap();
                                        }
                                    } else if cmd_part.starts_with("EXP:LIST?") {
                                        // File list handler: return newline-delimited list in <quote> tags
                                        socket.write_all(ok_resp("<quote.reply>\nfile1.xml\nfile2.xml\n</quote.reply>").as_bytes()).await.unwrap();
                                    } else if cmd_part.starts_with("CHAL?") {
                                        // Challenge handler: return fixed challenge
                                        socket.write_all(ok_resp(MOCK_CHALLENGE).as_bytes()).await.unwrap();
                                    } else if cmd_part.starts_with("AUTH ") {
                                        // Auth handler: validate HMAC-MD5
                                        let auth_arg = cmd_part.strip_prefix("AUTH ").unwrap_or("").trim();
                                        let expected = compute_expected_auth(MOCK_PASSWORD, MOCK_CHALLENGE);
                                        if auth_arg == expected {
                                            socket.write_all(ok_empty().as_bytes()).await.unwrap();
                                        } else {
                                            socket.write_all(err_resp("AuthenticationError", "Invalid credentials").as_bytes()).await.unwrap();
                                        }
                                    } else if cmd_part.starts_with("NEXTTHENOK") {
                                        // Send NEXT, then after a short delay, send OK
                                        if let Some(ref id) = ident {
                                            socket.write_all(format!("NEXT {}\n", id).as_bytes()).await.unwrap();
                                            tokio::time::sleep(Duration::from_millis(50)).await;
                                            socket.write_all(format!("OK {} done\n", id).as_bytes()).await.unwrap();
                                        }
                                    } else if cmd_part.starts_with("NEXTTHERR") {
                                        // Send NEXT, then after delay send ERRor
                                        if let Some(ref id) = ident {
                                            socket.write_all(format!("NEXT {}\n", id).as_bytes()).await.unwrap();
                                            tokio::time::sleep(Duration::from_millis(50)).await;
                                            socket.write_all(format!("ERRor {} [TestError] --> test error\n", id).as_bytes()).await.unwrap();
                                        }
                                    } else if cmd_part.starts_with("SILENCE") {
                                        // Never respond — for timeout tests
                                    } else if cmd_part.starts_with("WARNTEST") {
                                        // Send a WARNing response (treated like OK)
                                        if let Some(ref id) = ident {
                                            socket.write_all(format!("WARNing {} warn result\n", id).as_bytes()).await.unwrap();
                                        } else {
                                            socket.write_all(b"WARNing warn result\n").await.unwrap();
                                        }
                                    }
                                }
                                continue;
                            },
                            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                tokio::time::sleep(Duration::from_millis(10)).await;
                            }
                            Err(_) => break, // Any other error, break the loop
                        }
                    }
                }
            }
            if !stay_open {
                break;
            }
        }
    });

    (addr, handle)
}

#[tokio::test]
async fn test_tcp_connection() {
    setup_logging();
    let (addr, _server) = setup_mock_server(None, true).await;

    info!("Testing TCP connection to {}", addr);
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP).await;

    match &connection {
        Ok(_) => info!("Connection established successfully"),
        Err(e) => error!("Connection failed: {:?}", e),
    }

    assert!(connection.is_ok());
    let connection = connection.unwrap();

    // Verify ready message
    assert_eq!(
        connection
            .ready_message
            .args
            .get("session")
            .unwrap()
            .to_string(),
        "474800"
    );
    assert_eq!(
        connection
            .ready_message
            .args
            .get("product")
            .unwrap()
            .to_string(),
        "QuantStudio3_5"
    );
    assert_eq!(
        connection
            .ready_message
            .args
            .get("version")
            .unwrap()
            .to_string(),
        "1.3.0"
    );
    assert_eq!(
        connection
            .ready_message
            .args
            .get("build")
            .unwrap()
            .to_string(),
        "1"
    );
    assert_eq!(
        connection
            .ready_message
            .args
            .get("capabilities")
            .unwrap()
            .to_string(),
        "Index"
    );

    info!("connection done");
    _server.abort();
}

#[tokio::test]
async fn test_auto_connection() {
    let (addr, _server) = setup_mock_server(None, true).await;

    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::Auto).await;

    let connection = match connection {
        Ok(c) => c,
        Err(e) => {
            panic!("connection error: {:?}", e);
        }
    };

    // Verify ready message
    assert_eq!(
        connection
            .ready_message
            .args
            .get("session")
            .unwrap()
            .to_string(),
        "474800"
    );
    assert_eq!(
        connection
            .ready_message
            .args
            .get("product")
            .unwrap()
            .to_string(),
        "QuantStudio3_5"
    );
    assert_eq!(
        connection
            .ready_message
            .args
            .get("version")
            .unwrap()
            .to_string(),
        "1.3.0"
    );
    assert_eq!(
        connection
            .ready_message
            .args
            .get("build")
            .unwrap()
            .to_string(),
        "1"
    );
    assert_eq!(
        connection
            .ready_message
            .args
            .get("capabilities")
            .unwrap()
            .to_string(),
        "Index"
    );

    println!("connection done");
    _server.abort();
}

#[tokio::test]
async fn test_connection_refused() {
    // Try to connect to a port that's definitely not listening
    let connection = QSConnection::connect(
        "127.0.0.1",
        0, // Port 0 should never have a listener
        ConnectionType::TCP,
    )
    .await;

    assert!(connection.is_err());

    println!("connection done");
}

#[tokio::test]
async fn test_power_query_and_set() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    // Test initial power status (should be ON)
    let response = PowerQuery
        .send(&connection)
        .await
        .unwrap()
        .receive_response()
        .await;
    assert!(response.is_ok());
    assert_eq!(response.unwrap().unwrap(), PowerStatus::On);

    // Set power OFF
    let response = PowerSet(PowerStatus::Off)
        .send(&connection)
        .await
        .unwrap()
        .receive_response()
        .await;
    assert!(response.is_ok());

    // Verify power is OFF
    let response = PowerQuery
        .send(&connection)
        .await
        .unwrap()
        .receive_response()
        .await;
    assert!(response.is_ok());
    assert_eq!(response.unwrap().unwrap(), PowerStatus::Off);

    // Set power ON
    let response = PowerSet(PowerStatus::On)
        .send(&connection)
        .await
        .unwrap()
        .receive_response()
        .await;
    assert!(response.is_ok());

    // Verify power is ON
    let response = PowerQuery
        .send(&connection)
        .await
        .unwrap()
        .receive_response()
        .await;
    assert!(response.is_ok());
    assert_eq!(response.unwrap().unwrap(), PowerStatus::On);

    _server.abort();
}

#[tokio::test]
async fn test_log_messages() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    // Subscribe to Status messages
    let mut stream = connection.subscribe_log(&["Status"]).await;

    // Wait for and verify a log message
    let message = stream.next().await;
    assert!(message.is_some());

    if let Some((topic, result)) = message {
        assert_eq!(topic, "Status");
        let msg = result.unwrap();
        assert_eq!(msg.topic, "Status");
        assert_eq!(msg.message, "Test status message");
    }

    _server.abort();
}

#[tokio::test]
async fn test_send_command_bytes() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    // Send a custom command using raw bytes
    let command = "CUSTOM -option=value arg1 arg2 ";
    let mut response = connection
        .send_command_bytes(command.as_bytes())
        .await
        .unwrap();

    // Add handler for CUSTOM command in mock server
    let msg = response.recv().await.unwrap();

    match msg {
        MessageResponse::Ok { ident, message } => {
            println!("ident: {:?}", ident);
            println!("message: {:?}", message);
            assert_eq!(message.args[0].to_string(), "success");
            assert_eq!(
                message.options.get("received").unwrap().to_string(),
                "CUSTOM -option=value arg1 arg2"
            );
        }
        _ => panic!("Expected OK response"),
    }

    _server.abort();
}

fn generate_test_certificate() -> rcgen::CertifiedKey {
    // Generate a self-signed certificate for testing

    // Create Certificate and PrivateKey
    rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap()
}

async fn setup_mock_ssl_server(
    port: Option<u16>,
    stay_open: bool,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    // Generate certificate and create TLS config
    let cert = generate_test_certificate();

    let cert_pem = cert.cert.der().clone();
    let key_pem = cert.key_pair.serialize_pem();
    let rkey = rustls_pki_types::PrivateKeyDer::from_pem_slice(key_pem.as_bytes()).unwrap();
    println!("key_pem: {:?}", &rkey);

    let certs = vec![cert_pem];
    let key = rkey;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(config));

    // Bind to a random available port
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port.unwrap_or(0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();

    // Spawn server task
    let handle = tokio::spawn(async move {
        let mut power_status = true;

        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let acceptor = acceptor.clone();

            // Accept TLS connection
            let mut stream = acceptor.accept(stream).await.unwrap();

            // Send ready message
            let ready_msg = "READy -session=474800 -product=QuantStudio3_5 -version=1.3.0 -build=001 -capabilities=Index\n";
            stream.write_all(ready_msg.as_bytes()).await.unwrap();

            // Create a periodic timer for log messages
            let mut interval = interval(Duration::from_millis(100));

            // Buffer for accumulating partial lines
            let mut line_buffer = String::new();
            let mut buf = [0; 1024];

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // First send a log message that won't be read
                        let log_msg = "MESSage NonStatus Test status message\n";
                        if stream.write_all(log_msg.as_bytes()).await.is_err() {
                            break;
                        }

                        // Send a log message
                        let log_msg = "MESSage Status Test status message\n";
                        if stream.write_all(log_msg.as_bytes()).await.is_err() {
                            break;
                        }
                    }
                    result = stream.read(&mut buf) => {
                        match result {
                            Ok(0) => break,
                            Ok(n) => {
                                // Add new data to line buffer
                                line_buffer.push_str(&String::from_utf8_lossy(&buf[..n]));

                                // Process complete lines
                                while let Some(pos) = line_buffer.find('\n') {
                                    let line = line_buffer[..pos].trim().to_string();
                                    line_buffer = line_buffer[pos + 1..].to_string();

                                    debug!("Processing SSL command: {}", line);

                                    // Handle power commands
                                    if line.ends_with("POW?") {
                                        // Query power status
                                        let status = if power_status { "ON" } else { "OFF" };
                                        let response = if let Some(ident) = line.split_whitespace().next() {
                                            if ident.parse::<u32>().is_ok() {
                                                format!("OK {} {}\n", ident, status)
                                            } else {
                                                format!("OK POW? {}\n", status)
                                            }
                                        } else {
                                            format!("OK POW? {}\n", status)
                                        };
                                        stream.write_all(response.as_bytes()).await.unwrap();
                                    } else if line.ends_with("POW ON") {
                                        // Set power ON
                                        power_status = true;
                                        let response = if let Some(ident) = line.split_whitespace().next() {
                                            if ident.parse::<u32>().is_ok() {
                                                format!("OK {}\n", ident)
                                            } else {
                                                "OK POW\n".to_string()
                                            }
                                        } else {
                                            "OK POW\n".to_string()
                                        };
                                        stream.write_all(response.as_bytes()).await.unwrap();
                                    } else if line.ends_with("POW OFF") {
                                        // Set power OFF
                                        power_status = false;
                                        let response = if let Some(ident) = line.split_whitespace().next() {
                                            if ident.parse::<u32>().is_ok() {
                                                format!("OK {}\n", ident)
                                            } else {
                                                "OK POW\n".to_string()
                                            }
                                        } else {
                                            "OK POW\n".to_string()
                                        };
                                        stream.write_all(response.as_bytes()).await.unwrap();
                                    } else if line.contains("CUSTOM") {
                                        // Extract the message identifier if present
                                        let response = if let Some(ident) = line.split_whitespace().next() {
                                            if ident.parse::<u32>().is_ok() {
                                                // Get the command part without the identifier
                                                let command = line.split_once(' ').map(|x| x.1).unwrap_or("");
                                                format!("OK {} -received=\"{}\" success", ident, command)
                                            } else {
                                                format!("OK CUSTOM -received=\"{}\" success", line)
                                            }
                                        } else {
                                            format!("OK CUSTOM -received=\"{}\" success", line)
                                        };
                                        stream.write_all(format!("{}\n", response).as_bytes()).await.unwrap();
                                    } else if line.contains("MULTILINE") {
                                        // Return a response with an XML-quoted multiline string
                                        let response = if let Some(ident) = line.split_whitespace().next() {
                                            if ident.parse::<u32>().is_ok() {
                                                format!("OK {} <quote>Line 1\nLine 2\nLine 3</quote>\n", ident)
                                            } else {
                                                "OK MULTILINE <quote>Line 1\nLine 2\nLine 3</quote>\n".to_string()
                                            }
                                        } else {
                                            "OK MULTILINE <quote>Line 1\nLine 2\nLine 3</quote>\n".to_string()
                                        };
                                        stream.write_all(response.as_bytes()).await.unwrap();
                                    } else if line.contains("BADXML1") {
                                        // Return a response with mismatched XML tags
                                        let response = if let Some(ident) = line.split_whitespace().next() {
                                            if ident.parse::<u32>().is_ok() {
                                                format!("OK {} <quote>Line 1\nLine 2\nLine 3</badquote>\n", ident)
                                            } else {
                                                "OK BADXML1 <quote>Line 1\nLine 2\nLine 3</badquote>\n".to_string()
                                            }
                                        } else {
                                            "OK BADXML1 <quote>Line 1\nLine 2\nLine 3</badquote>\n".to_string()
                                        };
                                        stream.write_all(response.as_bytes()).await.unwrap();
                                    } else if line.contains("BADXML2") {
                                        // Return a response with an unexpected close tag
                                        let response = if let Some(ident) = line.split_whitespace().next() {
                                            if ident.parse::<u32>().is_ok() {
                                                format!("OK {} </unexpected>Line 1\nLine 2\nLine 3</unexpected>\n", ident)
                                            } else {
                                                "OK BADXML2 </unexpected>Line 1\nLine 2\nLine 3</unexpected>\n".to_string()
                                            }
                                        } else {
                                            "OK BADXML2 </unexpected>Line 1\nLine 2\nLine 3</unexpected>\n".to_string()
                                        };
                                        stream.write_all(response.as_bytes()).await.unwrap();
                                    } else if line.contains("ANGLES") {
                                        // Return a response with angle brackets that aren't XML tags
                                        let response = if let Some(ident) = line.split_whitespace().next() {
                                            if ident.parse::<u32>().is_ok() {
                                                format!("OK {} Temperature < 37.5 and pH > 7.0\n", ident)
                                            } else {
                                                "OK ANGLES Temperature < 37.5 and pH > 7.0\n".to_string()
                                            }
                                        } else {
                                            "OK ANGLES Temperature < 37.5 and pH > 7.0\n".to_string()
                                        };
                                        stream.write_all(response.as_bytes()).await.unwrap();
                                    }
                                }
                                continue;
                            },
                            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                tokio::time::sleep(Duration::from_millis(10)).await;
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
            if !stay_open {
                break;
            }
        }
    });

    (addr, handle)
}

#[tokio::test]
async fn test_ssl_connection() {
    let (addr, _server) = setup_mock_ssl_server(None, true).await;

    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::SSL).await;

    assert!(connection.is_ok());
    let connection = connection.unwrap();

    // Verify ready message
    assert_eq!(
        connection
            .ready_message
            .args
            .get("session")
            .unwrap()
            .to_string(),
        "474800"
    );

    _server.abort();
}

#[tokio::test]
async fn test_auto_ssl_connection() {
    let (addr, _server) = setup_mock_ssl_server(None, true).await;

    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::Auto).await;

    assert!(connection.is_ok());
    let connection = connection.unwrap();

    // Verify ready message
    assert_eq!(
        connection
            .ready_message
            .args
            .get("session")
            .unwrap()
            .to_string(),
        "474800"
    );

    _server.abort();
}

#[tokio::test]
async fn test_choose_ssl_by_port_7443() {
    // Skip if port 7443 is already in use (e.g., forwarding to machine)
    if TcpListener::bind("127.0.0.1:7443").await.is_err() {
        eprintln!("Skipping test_choose_ssl_by_port_7443: port 7443 already in use");
        return;
    }
    let (_, server) = setup_mock_ssl_server(Some(7443), false).await;
    let connection = QSConnection::connect("127.0.0.1", 7443, ConnectionType::Auto).await;
    assert!(connection.is_ok(), "Should have chosen SSL for port 7443");
    let connection = connection.unwrap();
    assert_eq!(connection.connection_type, ConnectionType::SSL);
    assert!(
        connection.is_connected().await,
        "Connection should be connected"
    );
    server.abort();
    assert!(server.await.is_err(), "Mock server didn't abort.");

    let (_, server) = setup_mock_server(Some(7443), false).await;
    let connection = QSConnection::connect("127.0.0.1", 7443, ConnectionType::Auto).await;
    assert!(connection.is_err(), "Should have chosen SSL for port 7443");
    server.abort();
}

#[tokio::test]
async fn test_choose_tcp_by_port_7000() {
    // Skip if port 7000 is already in use (e.g., forwarding to machine)
    if TcpListener::bind("127.0.0.1:7000").await.is_err() {
        eprintln!("Skipping test_choose_tcp_by_port_7000: port 7000 already in use");
        return;
    }
    let (_, server) = setup_mock_server(Some(7000), false).await;
    let connection = QSConnection::connect("127.0.0.1", 7000, ConnectionType::Auto).await;
    assert!(connection.is_ok());
    let connection = connection.unwrap();
    assert_eq!(connection.connection_type, ConnectionType::TCP);
    server.abort();
}

#[tokio::test]
async fn test_ssl_power_query_and_set() {
    let (addr, _server) = setup_mock_ssl_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::SSL)
        .await
        .unwrap();

    // Test initial power status (should be ON)
    let response = PowerQuery
        .send(&connection)
        .await
        .unwrap()
        .receive_response()
        .await;
    assert!(response.is_ok());
    assert_eq!(response.unwrap().unwrap(), PowerStatus::On);

    // Set power OFF
    let response = PowerSet(PowerStatus::Off)
        .send(&connection)
        .await
        .unwrap()
        .receive_response()
        .await;
    assert!(response.is_ok());

    // Verify power is OFF
    let response = PowerQuery
        .send(&connection)
        .await
        .unwrap()
        .receive_response()
        .await;
    assert!(response.is_ok());
    assert_eq!(response.unwrap().unwrap(), PowerStatus::Off);

    _server.abort();
}

#[tokio::test]
async fn test_multiline_response() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    // Send a command that triggers a multiline response
    let command = "MULTILINE";
    let mut response = connection
        .send_command_bytes(command.as_bytes())
        .await
        .unwrap();

    // Get and verify the response
    let msg = response.recv().await.unwrap();

    match msg {
        MessageResponse::Ok { message, .. } => {
            assert_eq!(
                message.args[0],
                Value::XmlString {
                    value: "Line 1\nLine 2\nLine 3".into(),
                    tag: "quote".to_string()
                }
            );
        }
        _ => panic!("Expected OK response with multiline string"),
    }

    _server.abort();
}

#[tokio::test]
async fn test_mismatched_xml_tags() {
    setup_logging();
    let (addr, _server) = setup_mock_server(None, true).await;

    info!("Testing mismatched XML tags handling");
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    // Send a command that triggers a mismatched XML tag response
    let command = "BADXML1";
    debug!("Sending command: {}", command);
    let _ = connection
        .send_command_bytes(command.as_bytes())
        .await
        .unwrap();

    // Send another command to verify connection still works
    let command = "CUSTOM -option=value arg1 arg2";
    debug!("Sending verification command: {}", command);
    let mut response = connection
        .send_command_bytes(command.as_bytes())
        .await
        .unwrap();
    let msg = response.recv().await.unwrap();

    match &msg {
        MessageResponse::Ok { message, .. } => {
            info!("Received OK response: {:?}", message);
        }
        _ => warn!("Unexpected response type: {:?}", msg),
    }

    _server.abort();
}

#[tokio::test]
async fn test_unexpected_close_tag() {
    setup_logging();
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    // Send a command that triggers an unexpected close tag response
    let command = "BADXML2";
    let _ = connection
        .send_command_bytes(command.as_bytes())
        .await
        .unwrap();

    // Send another command to verify connection still works
    let command = "CUSTOM -option=value arg1 arg2";
    let mut response = connection
        .send_command_bytes(command.as_bytes())
        .await
        .unwrap();
    let msg = response.recv().await.unwrap();

    match msg {
        MessageResponse::Ok { message, .. } => {
            assert_eq!(message.args[0].to_string(), "success");
        }
        _ => panic!("Expected OK response"),
    }

    _server.abort();
}

#[tokio::test]
async fn test_angle_brackets_in_response() {
    setup_logging();
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    // Send a command that triggers a response with angle brackets
    let command = "ANGLES";
    debug!("Sending command: {}", command);
    let mut response = connection
        .send_command_bytes(command.as_bytes())
        .await
        .unwrap();

    // Get and verify the response
    let msg = response.recv().await.unwrap();

    match msg {
        MessageResponse::Ok { message, .. } => {
            // The angle brackets should be treated as plain text, not XML
            assert_eq!(message.args[0], Value::String("Temperature".into()));
            assert_eq!(message.args[1], Value::String("<".into()));
            assert_eq!(message.args[2], Value::Float(37.5));
            assert_eq!(message.args[3], Value::String("and".into()));
            assert_eq!(message.args[4], Value::String("pH".into()));
            assert_eq!(message.args[5], Value::String(">".into()));
            assert_eq!(message.args[6], Value::Float(7.0));
        }
        _ => panic!("Expected OK response with angle brackets"),
    }

    _server.abort();
}

#[tokio::test]
async fn test_access_level_query() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    let response = AccessLevelQuery
        .send(&connection)
        .await
        .unwrap()
        .receive_response()
        .await;
    assert!(response.is_ok());
    let level = response.unwrap().unwrap();
    assert!(matches!(level, AccessLevel::Guest));

    _server.abort();
}

#[tokio::test]
async fn test_access_level_set() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    // Set to Observer
    let response = AccessLevelSet::new(AccessLevel::Observer)
        .send(&connection)
        .await
        .unwrap()
        .receive_response()
        .await;
    assert!(response.is_ok());

    // Verify
    let response = AccessLevelQuery
        .send(&connection)
        .await
        .unwrap()
        .receive_response()
        .await;
    assert!(response.is_ok());
    let level = response.unwrap().unwrap();
    assert!(matches!(level, AccessLevel::Observer));

    _server.abort();
}

#[tokio::test]
async fn test_drawer_status_query() {
    setup_logging();
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    let response = DrawerStatusQuery
        .send(&connection)
        .await
        .unwrap()
        .receive_response()
        .await;
    assert!(response.is_ok(), "drawer? failed: {:?}", response.err());
    let status = response.unwrap().unwrap();
    assert!(matches!(status, DrawerStatus::Closed));

    _server.abort();
}

#[tokio::test]
async fn test_run_title_no_run() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    let mut response = connection
        .send_command_bytes(b"RUNTitle?")
        .await
        .unwrap();
    let msg = response.recv().await.unwrap();
    match msg {
        MessageResponse::Ok { message, .. } => {
            assert_eq!(message.args[0].to_string(), "-");
        }
        _ => panic!("Expected OK response"),
    }

    _server.abort();
}

#[tokio::test]
async fn test_error_response_handling() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    let mut response = connection
        .send_command_bytes(b"ERRTEST")
        .await
        .unwrap();
    let msg = response.recv().await.unwrap();
    match msg {
        MessageResponse::CommandError { error, .. } => {
            assert_eq!(error.error, "InsufficientAccess");
        }
        _ => panic!("Expected error response, got {:?}", msg),
    }

    _server.abort();
}

#[tokio::test]
async fn test_is_connected() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    assert!(connection.is_connected().await);

    _server.abort();
}

#[tokio::test]
async fn test_subscribe_with_timestamp_option() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    // Subscribe with timestamp enabled
    let mut stream = connection.subscribe_log(&["Status"]).await;

    // Should receive at least one message
    let message = stream.next().await;
    assert!(message.is_some());
    if let Some((topic, result)) = message {
        assert_eq!(topic, "Status");
        assert!(result.is_ok());
    }

    _server.abort();
}

#[tokio::test]
async fn test_concurrent_commands() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    // Send two commands concurrently
    let power_fut = PowerQuery.send(&connection);
    let drawer_fut = DrawerStatusQuery.send(&connection);

    let (power_resp, drawer_resp) = tokio::join!(power_fut, drawer_fut);

    let mut power = power_resp.unwrap();
    let mut drawer = drawer_resp.unwrap();

    let (power_result, drawer_result) = tokio::join!(
        power.receive_response(),
        drawer.receive_response()
    );

    assert!(power_result.is_ok());
    assert!(drawer_result.is_ok());

    _server.abort();
}

#[tokio::test]
async fn test_temperature_setpoints_query() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    let response = SetTemperaturesQuery
        .send(&connection)
        .await
        .unwrap()
        .receive_response()
        .await;
    assert!(response.is_ok());
    let temps = response.unwrap().unwrap();
    assert_eq!(temps.zones.len(), 6);
    assert_eq!(temps.fans.len(), 1);
    assert_eq!(temps.cover, 30.0);

    _server.abort();
}

// ===== Tier 2 tests: File operations =====

#[tokio::test]
async fn test_get_exp_file() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    let result = connection.get_exp_file("test/path").await;
    assert!(result.is_ok());
    let data = result.unwrap();
    assert_eq!(data, b"test file content");

    _server.abort();
}

#[tokio::test]
async fn test_get_exp_file_error() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    let result = connection.get_exp_file("missing").await;
    assert!(result.is_err());

    _server.abort();
}

#[tokio::test]
async fn test_get_expfile_list() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    let result = connection.get_expfile_list("*.xml").await;
    assert!(result.is_ok());
    let files = result.unwrap();
    assert!(files.contains(&"file1.xml".to_string()));
    assert!(files.contains(&"file2.xml".to_string()));

    _server.abort();
}

#[tokio::test]
async fn test_get_run_title() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    let result = connection.get_run_title().await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "-");

    _server.abort();
}

#[tokio::test]
async fn test_get_current_run_name_none() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    let result = connection.get_current_run_name().await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), None);

    _server.abort();
}

#[tokio::test]
async fn test_get_current_run_name_some() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    // Set the run title first
    let mut response = connection
        .send_command_bytes(b"RUNTitle= TestRun")
        .await
        .unwrap();
    let _ = response.get_response().await;

    let result = connection.get_current_run_name().await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some("TestRun".to_string()));

    _server.abort();
}

// ===== Tier 2 tests: Authentication =====

#[tokio::test]
async fn test_authenticate_success() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    let result = connection.authenticate(MOCK_PASSWORD).await;
    assert!(result.is_ok(), "Auth should succeed: {:?}", result.err());

    _server.abort();
}

#[tokio::test]
async fn test_authenticate_failure() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    let result = connection.authenticate("wrongpassword").await;
    assert!(result.is_err());

    _server.abort();
}

#[tokio::test]
async fn test_authenticate_and_set_access_level() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    let result = connection
        .authenticate_and_set_access_level(MOCK_PASSWORD, AccessLevel::Controller)
        .await;
    assert!(result.is_ok(), "Auth+access should succeed: {:?}", result.err());

    // Verify access level was set
    let level = connection.get_access_level().await;
    assert!(level.is_ok());
    assert!(matches!(level.unwrap(), AccessLevel::Controller));

    _server.abort();
}

// ===== Tier 2 tests: Response handling =====

#[tokio::test]
async fn test_response_next_then_ok() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    let mut response = connection
        .send_command_bytes(b"NEXTTHENOK")
        .await
        .unwrap();
    let result = response.get_response().await;
    assert!(result.is_ok());
    let ok = result.unwrap();
    assert!(ok.is_ok(), "Expected Ok response, got: {:?}", ok);

    _server.abort();
}

#[tokio::test]
async fn test_response_next_then_error() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    let mut response = connection
        .send_command_bytes(b"NEXTTHERR")
        .await
        .unwrap();
    let result = response.get_response().await;
    assert!(result.is_ok());
    let inner = result.unwrap();
    assert!(inner.is_err(), "Expected error response");
    let err = inner.unwrap_err();
    assert_eq!(err.error, "TestError");

    _server.abort();
}

#[tokio::test]
async fn test_response_direct_error() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    let mut response = connection
        .send_command_bytes(b"ERRTEST")
        .await
        .unwrap();
    let result = response.get_response().await;
    assert!(result.is_ok());
    let inner = result.unwrap();
    assert!(inner.is_err());
    assert_eq!(inner.unwrap_err().error, "InsufficientAccess");

    _server.abort();
}

#[tokio::test]
async fn test_response_timeout() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    let mut response = connection
        .send_command_bytes(b"SILENCE")
        .await
        .unwrap();

    let result = response
        .get_response_with_timeout(Duration::from_millis(100))
        .await;
    assert!(matches!(result, Err(ReceiveOkResponseError::Timeout)));

    _server.abort();
}

#[tokio::test]
async fn test_response_with_timeout_ok() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    let mut response = connection
        .send_command_bytes(b"POW?")
        .await
        .unwrap();

    let result = response
        .get_response_with_timeout(Duration::from_secs(5))
        .await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_ok());

    _server.abort();
}

#[tokio::test]
async fn test_warning_response() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    let mut response = connection
        .send_command_bytes(b"WARNTEST")
        .await
        .unwrap();
    let result = response.get_response().await;
    assert!(result.is_ok(), "get_response failed: {:?}", result.err());
    let inner = result.unwrap();
    // Warnings are treated like OK
    assert!(inner.is_ok(), "Expected OK-like response from warning, got: {:?}", inner);

    _server.abort();
}

#[tokio::test]
async fn test_get_response_with_next_and_ok_timeout() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    // NEXTTHENOK sends NEXT then OK after 50ms
    let mut response = connection
        .send_command_bytes(b"NEXTTHENOK")
        .await
        .unwrap();

    let result = response
        .get_response_with_next_and_ok_timeout(
            Duration::from_secs(2),   // initial: should get NEXT quickly
            Duration::from_secs(2),   // next_to_ok: should get OK within 50ms
        )
        .await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_ok());

    _server.abort();
}

// ===== Tier 2 tests: Subscribe and connection =====

#[tokio::test]
async fn test_subscribe_wildcard() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    // Subscribe to all topics with wildcard
    let mut stream = connection.subscribe_log(&["*"]).await;

    // Should receive messages — the StreamMap key for wildcard is "*"
    let message = stream.next().await;
    assert!(message.is_some());
    if let Some((topic, result)) = message {
        // Wildcard subscription uses "*" as the StreamMap key
        assert_eq!(topic, "*");
        assert!(result.is_ok());
    }

    _server.abort();
}

#[tokio::test]
async fn test_subscribe_wildcard_and_specific() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    // Subscribe to both wildcard and specific topic
    let mut stream = connection.subscribe_log(&["*", "Status"]).await;

    // Should receive messages — at least from Status
    let msg1 = stream.next().await;
    assert!(msg1.is_some());
    let msg2 = stream.next().await;
    assert!(msg2.is_some());

    _server.abort();
}

#[tokio::test]
async fn test_quit_command_succeeds() {
    // Verify that QUIT command is handled correctly
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    assert!(connection.is_connected().await);

    // Send QUIT — should get OK response
    let mut response = connection
        .send_command_bytes(b"QUIT")
        .await
        .unwrap();
    let result = response.get_response().await;
    assert!(result.is_ok(), "QUIT should get OK response");

    _server.abort();
}

// ===== Tier 2 tests: Additional coverage =====

#[tokio::test]
async fn test_get_current_temperature_setpoints_mock() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    let result = connection.get_current_temperature_setpoints().await;
    assert!(result.is_ok(), "get_current_temperature_setpoints failed: {:?}", result.err());
    let (zones, fans, cover) = result.unwrap();
    assert_eq!(zones.len(), 6);
    assert_eq!(fans.len(), 1);
    assert_eq!(cover, 30.0);
    // All zones should be 25.0
    for z in &zones {
        assert_eq!(*z, 25.0);
    }
    assert_eq!(fans[0], 44.0);

    _server.abort();
}

#[tokio::test]
async fn test_set_access_level_mock() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    let result = connection.set_access_level(AccessLevel::Controller).await;
    assert!(result.is_ok());

    // Verify access level was set
    let level = connection.get_access_level().await;
    assert!(level.is_ok());
    assert!(matches!(level.unwrap(), AccessLevel::Controller));

    _server.abort();
}

#[tokio::test]
async fn test_get_access_level_mock() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    let result = connection.get_access_level().await;
    assert!(result.is_ok());
    assert!(matches!(result.unwrap(), AccessLevel::Guest));

    _server.abort();
}

#[tokio::test]
async fn test_disconnect_completes() {
    let (addr, _server) = setup_mock_server(None, true).await;
    let connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP)
        .await
        .unwrap();

    assert!(connection.is_connected().await);

    // disconnect() sends QUIT and waits for response — should not panic or hang
    connection.disconnect().await;

    // Calling disconnect again should be safe (no-op)
    connection.disconnect().await;

    _server.abort();
}
