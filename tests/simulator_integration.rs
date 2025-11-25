// SPDX-FileCopyrightText: 2021-2025 Constantine Evans <qslib@mb.costi.net>
// SPDX-License-Identifier: EUPL-1.2

//! Integration tests that require a running simulator.
//!
//! These tests are ignored by default. To run them, ensure a simulator is running:
//! - TCP: localhost:7000
//! - SSL: localhost:7443
//!
//! Run with: `cargo test --test simulator_integration -- --ignored`

use qslib::com::{ConnectionType, QSConnection};
use qslib::commands::*;
use std::time::Duration;
use tokio_stream::StreamExt;

const TCP_HOST: &str = "localhost";
const TCP_PORT: u16 = 7000;
const SSL_HOST: &str = "localhost";
const SSL_PORT: u16 = 7443;
const TEST_PASSWORD: &str = "correctpassword";

/// Helper to connect and authenticate with Observer access level.
async fn connect_authenticated(host: &str, port: u16, conn_type: ConnectionType) -> QSConnection {
    let conn = QSConnection::connect(host, port, conn_type)
        .await
        .expect("Failed to connect");
    conn.authenticate(TEST_PASSWORD)
        .await
        .expect("Failed to authenticate");
    conn.set_access_level(AccessLevel::Observer)
        .await
        .expect("Failed to set access level");
    conn
}

/// Test TCP connection to the simulator
#[tokio::test]
#[ignore]
async fn test_simulator_tcp_connection() {
    let connection = QSConnection::connect(TCP_HOST, TCP_PORT, ConnectionType::TCP).await;
    
    assert!(connection.is_ok(), "TCP connection failed: {:?}", connection.err());
    let conn = connection.unwrap();
    
    assert!(conn.is_connected().await, "Connection should be active");
    assert_eq!(conn.connection_type, ConnectionType::TCP);
    
    // Verify ready message has expected fields
    assert!(conn.ready_message.args.get("product").is_some(), "Missing product in ready message");
    assert!(conn.ready_message.args.get("version").is_some(), "Missing version in ready message");
}

/// Test SSL connection to the simulator
#[tokio::test]
#[ignore]
async fn test_simulator_ssl_connection() {
    let connection = QSConnection::connect(SSL_HOST, SSL_PORT, ConnectionType::SSL).await;
    
    assert!(connection.is_ok(), "SSL connection failed: {:?}", connection.err());
    let conn = connection.unwrap();
    
    assert!(conn.is_connected().await, "Connection should be active");
    assert_eq!(conn.connection_type, ConnectionType::SSL);
}

/// Test auto connection type detection for TCP port
#[tokio::test]
#[ignore]
async fn test_simulator_auto_tcp() {
    let connection = QSConnection::connect(TCP_HOST, TCP_PORT, ConnectionType::Auto).await;
    
    assert!(connection.is_ok(), "Auto TCP connection failed: {:?}", connection.err());
    let conn = connection.unwrap();
    assert_eq!(conn.connection_type, ConnectionType::TCP);
}

/// Test auto connection type detection for SSL port
#[tokio::test]
#[ignore]
async fn test_simulator_auto_ssl() {
    let connection = QSConnection::connect(SSL_HOST, SSL_PORT, ConnectionType::Auto).await;
    
    assert!(connection.is_ok(), "Auto SSL connection failed: {:?}", connection.err());
    let conn = connection.unwrap();
    assert_eq!(conn.connection_type, ConnectionType::SSL);
}

/// Test connection with timeout
#[tokio::test]
#[ignore]
async fn test_simulator_connection_timeout() {
    let connection = QSConnection::connect_with_timeout(
        TCP_HOST,
        TCP_PORT,
        ConnectionType::TCP,
        Duration::from_secs(10),
    ).await;
    
    assert!(connection.is_ok(), "Connection with timeout failed: {:?}", connection.err());
}

/// Test HELP? command (available at all access levels)
#[tokio::test]
#[ignore]
async fn test_simulator_help_command() {
    let connection = QSConnection::connect(TCP_HOST, TCP_PORT, ConnectionType::TCP)
        .await
        .expect("Failed to connect");
    
    let mut response = connection
        .send_command_bytes(b"HELP?")
        .await
        .expect("Failed to send command");
    
    let result = response.get_response().await;
    assert!(result.is_ok(), "HELP? command failed: {:?}", result.err());
}

/// Test power status query
#[tokio::test]
#[ignore]
async fn test_simulator_power_query() {
    let connection = connect_authenticated(TCP_HOST, TCP_PORT, ConnectionType::TCP).await;
    
    let response = PowerQuery
        .send(&connection)
        .await
        .expect("Failed to send power query")
        .receive_response()
        .await;
    
    assert!(response.is_ok(), "Power query failed: {:?}", response.err());
    let power_status = response.unwrap();
    // Should be either Ok(On) or Ok(Off)
    assert!(power_status.is_ok(), "Power query returned error: {:?}", power_status.err());
}

/// Test access level query
#[tokio::test]
#[ignore]
async fn test_simulator_access_level_query() {
    let connection = QSConnection::connect(TCP_HOST, TCP_PORT, ConnectionType::TCP)
        .await
        .expect("Failed to connect");
    
    let response = AccessLevelQuery
        .send(&connection)
        .await
        .expect("Failed to send access level query")
        .receive_response()
        .await;
    
    assert!(response.is_ok(), "Access level query failed: {:?}", response.err());
}

/// Test authentication
#[tokio::test]
#[ignore]
async fn test_simulator_authentication() {
    let connection = QSConnection::connect(TCP_HOST, TCP_PORT, ConnectionType::TCP)
        .await
        .expect("Failed to connect");
    
    // Authenticate
    let auth_result = connection.authenticate(TEST_PASSWORD).await;
    assert!(auth_result.is_ok(), "Authentication failed: {:?}", auth_result.err());
    
    // Verify we can now set higher access levels
    let set_result = connection.set_access_level(AccessLevel::Controller).await;
    assert!(set_result.is_ok(), "Failed to set Controller after auth: {:?}", set_result.err());
    
    let level = connection.get_access_level().await.expect("Failed to get access level");
    assert!(matches!(level, AccessLevel::Controller), "Expected Controller, got {:?}", level);
}

/// Test authentication with wrong password fails
#[tokio::test]
#[ignore]
async fn test_simulator_authentication_wrong_password() {
    let connection = QSConnection::connect(TCP_HOST, TCP_PORT, ConnectionType::TCP)
        .await
        .expect("Failed to connect");
    
    let auth_result = connection.authenticate("wrongpassword").await;
    assert!(auth_result.is_err(), "Authentication with wrong password should fail");
}

/// Test setting access level (without password - basic level)
#[tokio::test]
#[ignore]
async fn test_simulator_set_access_level() {
    let connection = QSConnection::connect(TCP_HOST, TCP_PORT, ConnectionType::TCP)
        .await
        .expect("Failed to connect");
    
    // Try to set access level to Observer (doesn't require password)
    let response = AccessLevelSet::level(AccessLevel::Observer)
        .send(&connection)
        .await
        .expect("Failed to send access level set")
        .receive_response()
        .await;
    
    // Observer level should be settable without password
    println!("Access level set response: {:?}", response);
    
    // Verify current access level
    let verify = AccessLevelQuery
        .send(&connection)
        .await
        .expect("Failed to send verification query")
        .receive_response()
        .await;
    
    assert!(verify.is_ok(), "Verification query failed: {:?}", verify.err());
}

/// Test setting Controller access level (requires authentication on simulator)
#[tokio::test]
#[ignore]
async fn test_simulator_controller_access() {
    let connection = QSConnection::connect(TCP_HOST, TCP_PORT, ConnectionType::TCP)
        .await
        .expect("Failed to connect");
    
    // Authenticate first
    connection.authenticate(TEST_PASSWORD)
        .await
        .expect("Failed to authenticate");
    
    // Now set controller access level
    let result = connection.set_access_level(AccessLevel::Controller).await;
    assert!(result.is_ok(), "Failed to set Controller access: {:?}", result.err());
    
    // Verify
    let level = connection.get_access_level().await.expect("Failed to get access level");
    assert!(matches!(level, AccessLevel::Controller), "Expected Controller, got {:?}", level);
}

/// Test subscribing to log messages
#[tokio::test]
#[ignore]
async fn test_simulator_log_subscription() {
    let connection = connect_authenticated(TCP_HOST, TCP_PORT, ConnectionType::TCP).await;
    
    Subscribe::topic("Temperature").send(&connection).await.unwrap();

    // Subscribe to all messages
    let mut stream = connection.subscribe_log(&["Temperature"]).await;
    
    // Wait for at least one message (with timeout)
    let timeout = tokio::time::timeout(Duration::from_secs(5), stream.next()).await;
    
    assert!(timeout.is_ok(), "Timed out waiting for log messages");
    let message = timeout.unwrap();
    assert!(message.is_some(), "Should receive at least one log message");
}

/// Test run title query when no run is active
#[tokio::test]
#[ignore]
async fn test_simulator_run_title_no_run() {
    let connection = connect_authenticated(TCP_HOST, TCP_PORT, ConnectionType::TCP).await;
    
    let result = connection.get_current_run_name().await;
    
    assert!(result.is_ok(), "Run title query failed: {:?}", result.err());
    // When no run is active, should return None or "-"
    let run_name = result.unwrap();
    // It's valid to return None when no run is active
    println!("Current run name: {:?}", run_name);
}

/// Test temperature setpoints query
#[tokio::test]
#[ignore]
async fn test_simulator_temperature_setpoints() {
    let connection = connect_authenticated(TCP_HOST, TCP_PORT, ConnectionType::TCP).await;
    
    let result = connection.get_current_temperature_setpoints().await;
    
    assert!(result.is_ok(), "Temperature setpoints query failed: {:?}", result.err());
    let (zones, fans, cover) = result.unwrap();
    
    // Should have 6 zone temperatures
    assert_eq!(zones.len(), 6, "Should have 6 zone temperatures");
    // Should have at least 1 fan temperature
    assert!(!fans.is_empty(), "Should have fan temperatures");
    // Cover temperature should be reasonable
    assert!(cover > 0.0 && cover < 200.0, "Cover temperature {} seems unreasonable", cover);
}

/// Test file listing
#[tokio::test]
#[ignore]
async fn test_simulator_file_list() {
    let connection = connect_authenticated(TCP_HOST, TCP_PORT, ConnectionType::TCP).await;
    
    let result = connection.get_expfile_list("*").await;
    
    // This might succeed or fail depending on the simulator state
    // We just want to make sure the command runs without panicking
    println!("File list result: {:?}", result);
}

/// Test multiple concurrent commands
#[tokio::test]
#[ignore]
async fn test_simulator_concurrent_commands() {
    let connection = connect_authenticated(TCP_HOST, TCP_PORT, ConnectionType::TCP).await;
    
    // Send multiple commands concurrently
    let power_fut = PowerQuery.send(&connection);
    let access_fut = AccessLevelQuery.send(&connection);
    
    let (power_resp, access_resp) = tokio::join!(power_fut, access_fut);
    
    let mut power = power_resp.expect("Power query send failed");
    let mut access = access_resp.expect("Access query send failed");
    
    let (power_result, access_result) = tokio::join!(
        power.receive_response(),
        access.receive_response()
    );
    
    assert!(power_result.is_ok(), "Power query failed: {:?}", power_result.err());
    assert!(access_result.is_ok(), "Access query failed: {:?}", access_result.err());
}

/// Test raw command bytes
#[tokio::test]
#[ignore]
async fn test_simulator_raw_command() {
    let connection = connect_authenticated(TCP_HOST, TCP_PORT, ConnectionType::TCP).await;
    
    // Send a raw RUNTitle? command
    let mut response = connection
        .send_command_bytes(b"RUNTitle?")
        .await
        .expect("Failed to send command");
    
    let result = response.get_response().await;
    assert!(result.is_ok(), "Raw command failed: {:?}", result.err());
}

/// Test SSL-specific functionality
#[tokio::test]
#[ignore]
async fn test_simulator_ssl_commands() {
    let connection = connect_authenticated(SSL_HOST, SSL_PORT, ConnectionType::SSL).await;
    
    // Test a simple command over SSL
    let response = PowerQuery
        .send(&connection)
        .await
        .expect("Failed to send command")
        .receive_response()
        .await;
    
    assert!(response.is_ok(), "SSL command failed: {:?}", response.err());
}

/// Test reconnection behavior
#[tokio::test]
#[ignore]
async fn test_simulator_reconnection() {
    // First connection
    let conn1 = QSConnection::connect(TCP_HOST, TCP_PORT, ConnectionType::TCP)
        .await
        .expect("First connection failed");
    
    assert!(conn1.is_connected().await);
    
    // Second connection while first is still active
    let conn2 = QSConnection::connect(TCP_HOST, TCP_PORT, ConnectionType::TCP)
        .await
        .expect("Second connection failed");
    
    assert!(conn2.is_connected().await);
    
    // Both should work
    let r1 = conn1.send_command_bytes(b"HELP?").await;
    let r2 = conn2.send_command_bytes(b"HELP?").await;
    
    assert!(r1.is_ok(), "First connection command failed");
    assert!(r2.is_ok(), "Second connection command failed");
}

/// Test connection to wrong port fails gracefully
#[tokio::test]
#[ignore]
async fn test_simulator_wrong_port_type() {
    // Try SSL connection to TCP port - should fail
    let result = QSConnection::connect(TCP_HOST, TCP_PORT, ConnectionType::SSL).await;
    assert!(result.is_err(), "SSL connection to TCP port should fail");
}

