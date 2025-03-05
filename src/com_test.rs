use std::net::SocketAddr;
use tokio::net::TcpListener;

async fn setup_mock_server() -> (SocketAddr, tokio::task::JoinHandle<()>) {
    // Bind to a random available port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Spawn server task
    let handle = tokio::spawn(async move {
        let mut power_status = true; // Start with power ON

        loop {
            let (mut socket, _) = listener.accept().await.unwrap();

            // Send ready message
            let ready_msg = "READy -session=474800 -product=QuantStudio3_5 -version=1.3.0 -build=001 -capabilities=Index\n";
            socket.write_all(ready_msg.as_bytes()).await.unwrap();

            // Wait for connection to close instead of infinite loop
            let mut buf = [0; 1024];
            loop {
                match socket.try_read(&mut buf) {
                    Ok(0) => break, // Connection was closed
                    Ok(n) => {
                        let msg = String::from_utf8_lossy(&buf[..n]);
                        let msg = msg.trim_end();

                        // Handle power commands
                        if msg.ends_with("POW?") {
                            // Query power status
                            let status = if power_status { "ON" } else { "OFF" };
                            let response = if let Some(ident) = msg.split_whitespace().next() {
                                if ident.parse::<u32>().is_ok() {
                                    format!("OK {} {}\n", ident, status)
                                } else {
                                    format!("OK POW? {}\n", status)
                                }
                            } else {
                                format!("OK POW? {}\n", status)
                            };
                            socket.write_all(response.as_bytes()).await.unwrap();
                        } else if msg.ends_with("POW ON") {
                            // Set power ON
                            power_status = true;
                            let response = if let Some(ident) = msg.split_whitespace().next() {
                                if ident.parse::<u32>().is_ok() {
                                    format!("OK {}\n", ident)
                                } else {
                                    "OK POW\n".to_string()
                                }
                            } else {
                                "OK POW\n".to_string()
                            };
                            socket.write_all(response.as_bytes()).await.unwrap();
                        } else if msg.ends_with("POW OFF") {
                            // Set power OFF
                            power_status = false;
                            let response = if let Some(ident) = msg.split_whitespace().next() {
                                if ident.parse::<u32>().is_ok() {
                                    format!("OK {}\n", ident)
                                } else {
                                    "OK POW\n".to_string()
                                }
                            } else {
                                "OK POW\n".to_string()
                            };
                            socket.write_all(response.as_bytes()).await.unwrap();
                        }
                        continue;
                    },
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                    }
                    Err(_) => break, // Any other error, break the loop
                }
            }
        }
    });

    (addr, handle)
}

#[tokio::test]
async fn test_power_query_and_set() {
    let (addr, _server) = setup_mock_server().await;
    let mut connection = QSConnection::connect("127.0.0.1", addr.port(), ConnectionType::TCP).await.unwrap();

    // Test initial power status (should be ON)
    let response = PowerQuery.send(&mut connection).await.unwrap().recv_response().await;
    assert!(response.is_ok());
    assert_eq!(response.unwrap(), Power::On);

    // Set power OFF
    let response = PowerSet::new(false).send(&mut connection).await.unwrap().recv_response().await;
    assert!(response.is_ok());

    // Verify power is OFF
    let response = PowerQuery.send(&mut connection).await.unwrap().recv_response().await;
    assert!(response.is_ok());
    assert_eq!(response.unwrap(), Power::Off);

    // Set power ON
    let response = PowerSet::new(true).send(&mut connection).await.unwrap().recv_response().await;
    assert!(response.is_ok());

    // Verify power is ON
    let response = PowerQuery.send(&mut connection).await.unwrap().recv_response().await;
    assert!(response.is_ok());
    assert_eq!(response.unwrap(), Power::On);

    _server.abort();
}