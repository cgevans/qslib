use env_logger::Builder;
use log::{debug, error, info, warn};
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
                                        socket.write_all(response.as_bytes()).await.unwrap();
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
                                        socket.write_all(response.as_bytes()).await.unwrap();
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
                                        socket.write_all(response.as_bytes()).await.unwrap();
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
                                        socket.write_all(format!("{}\n", response).as_bytes()).await.unwrap();
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
                                        socket.write_all(response.as_bytes()).await.unwrap();
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
                                        socket.write_all(response.as_bytes()).await.unwrap();
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
                                        socket.write_all(response.as_bytes()).await.unwrap();
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
                                        socket.write_all(response.as_bytes()).await.unwrap();
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
