//! One-shot SCPI: connect to the localhost plaintext SCPI server per request,
//! elevate to the requested access level, run a single command, return the
//! response, and drop the connection.
//!
//! A SCPI connection is a stateful session (access level, subscriptions,
//! session variables), so a fresh connection per call gives a clean, isolated
//! baseline. The cost is a few milliseconds over plaintext loopback. Genuinely
//! stateful or long-lived interactions should use the streaming tunnel instead.

use std::time::Duration;

use qslib_core::com::{CommandError, QSConnection};
use qslib_core::commands::{AccessLevel, ReceiveOkResponseError};
use qslib_core::parser::{ErrorResponse, OkResponse};

use crate::error::ServerError;
use crate::state::AppState;

/// Maximum time to establish the loopback SCPI connection.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// The result of a one-shot command: either an OK response or a SCPI-level
/// command error (which the handler renders as HTTP 400).
pub enum OneShot {
    Ok(OkResponse),
    ScpiError(ErrorResponse),
}

/// Connect, elevate, run `command`, and return `(effective_access, result)`.
pub async fn run_oneshot(
    state: &AppState,
    command: &str,
    access: AccessLevel,
    timeout_ms: u64,
) -> Result<(AccessLevel, OneShot), ServerError> {
    if access > state.max_access {
        return Err(ServerError::forbidden(format!(
            "requested access level {} exceeds --max-access {}",
            String::from(access),
            String::from(state.max_access.clone())
        )));
    }

    let conn = connect(state).await?;

    // Elevate as needed. A SCPI command error here means the level was refused.
    ensure_access(&conn, &access, state.scpi_password.as_deref())
        .await
        .map_err(|e| map_access_error(&access, e))?;

    let mut recv = conn
        .send_command_bytes(command.as_bytes())
        .await
        .map_err(|e| ServerError::unavailable(format!("failed to send SCPI command: {e}")))?;

    let out = match recv
        .get_response_with_timeout(Duration::from_millis(timeout_ms))
        .await
    {
        Ok(Ok(ok)) => OneShot::Ok(ok),
        Ok(Err(err)) => OneShot::ScpiError(err),
        Err(ReceiveOkResponseError::Timeout) => {
            return Err(ServerError::timeout("timed out waiting for SCPI response"))
        }
        Err(ReceiveOkResponseError::ConnectionClosed) => {
            return Err(ServerError::unavailable("SCPI connection closed before response"))
        }
        Err(e) => return Err(ServerError::internal(format!("SCPI response error: {e}"))),
    };

    // Drop the connection: the receive loop exits and the socket closes.
    drop(conn);
    Ok((access, out))
}

/// Establish the loopback plaintext SCPI connection, with a bounded timeout.
pub async fn connect(state: &AppState) -> Result<QSConnection, ServerError> {
    let host = state.scpi_target.ip().to_string();
    let port = state.scpi_target.port();
    match tokio::time::timeout(CONNECT_TIMEOUT, QSConnection::connect_tcp(&host, port)).await {
        Ok(Ok(conn)) => Ok(conn),
        Ok(Err(e)) => Err(ServerError::unavailable(format!(
            "cannot connect to SCPI server at {}: {e}",
            state.scpi_target
        ))),
        Err(_) => Err(ServerError::unavailable(format!(
            "timed out connecting to SCPI server at {}",
            state.scpi_target
        ))),
    }
}

async fn ensure_access(
    conn: &QSConnection,
    target: &AccessLevel,
    password: Option<&str>,
) -> Result<(), CommandError<ErrorResponse>> {
    if *target == AccessLevel::Guest {
        return Ok(());
    }
    match password {
        Some(pw) => {
            conn.authenticate_and_set_access_level(pw, target.clone())
                .await
        }
        None => conn.set_access_level(target.clone()).await,
    }
}

fn map_access_error(target: &AccessLevel, e: CommandError<ErrorResponse>) -> ServerError {
    match e {
        // The instrument refused the access level (auth required / denied).
        CommandError::CommandError(err) => ServerError::forbidden(format!(
            "access level {} denied: {err}",
            String::from(target.clone())
        )),
        CommandError::ParseResponseError(ReceiveOkResponseError::Timeout) => {
            ServerError::timeout("timed out setting SCPI access level")
        }
        other => ServerError::unavailable(format!("failed to set SCPI access level: {other}")),
    }
}
