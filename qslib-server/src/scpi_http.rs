//! `POST /scpi` — run a single SCPI command at a chosen access level.
//!
//! The command may be sent as the raw request body (default) or as a JSON
//! object `{"command", "access", "timeout_ms", "encoding"}`. Query parameters
//! `access`, `timeout_ms`, and `encoding` are accepted as alternatives.

use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;

use qslib_core::commands::AccessLevel;

use crate::error::ServerError;
use crate::scpi::{run_oneshot, OneShot};
use crate::state::AppState;

#[derive(Debug, Default, Deserialize)]
pub struct ScpiQuery {
    access: Option<String>,
    timeout_ms: Option<u64>,
    encoding: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ScpiJsonBody {
    command: String,
    access: Option<String>,
    timeout_ms: Option<u64>,
    encoding: Option<String>,
}

fn parse_access(s: &str) -> Result<AccessLevel, ServerError> {
    AccessLevel::try_from(s.to_string()).map_err(|_| {
        ServerError::bad_request(format!(
            "invalid access level `{s}` (expected Guest|Observer|Controller|Administrator|Full)"
        ))
    })
}

fn is_json(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.starts_with("application/json"))
        .unwrap_or(false)
}

pub async fn post_scpi(
    State(state): State<AppState>,
    Query(query): Query<ScpiQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ServerError> {
    let (command, access_str, timeout_ms, encoding) = if is_json(&headers) {
        let parsed: ScpiJsonBody = serde_json::from_slice(&body)
            .map_err(|e| ServerError::bad_request(format!("invalid JSON body: {e}")))?;
        (
            parsed.command,
            parsed.access.or(query.access),
            parsed.timeout_ms.or(query.timeout_ms),
            parsed.encoding.or(query.encoding),
        )
    } else {
        let cmd = std::str::from_utf8(&body)
            .map_err(|_| ServerError::bad_request("request body is not valid UTF-8"))?
            .trim()
            .to_string();
        (cmd, query.access, query.timeout_ms, query.encoding)
    };

    if command.is_empty() {
        return Err(ServerError::bad_request("empty SCPI command"));
    }
    // A one-shot command is a single line: the connection frames it with a
    // trailing newline and reads exactly one response. An interior newline
    // would inject a second, unmonitored command onto the already-elevated
    // loopback connection (bypassing the --max-access cap) and desync the
    // response stream. Reject them; multi-command / multiline-quoted sessions
    // must use the streaming tunnel.
    if command.contains(['\n', '\r']) {
        return Err(ServerError::bad_request(
            "SCPI command must be a single line (no CR/LF); use the /scpi tunnel for multi-command or multiline sessions",
        ));
    }

    let access = match access_str {
        Some(s) => parse_access(&s)?,
        None => state.default_access.clone(),
    };
    let timeout_ms = timeout_ms.unwrap_or(state.scpi_timeout_ms);
    let want_bytes = matches!(encoding.as_deref(), Some("bytes"));

    let (eff_access, result) = run_oneshot(&state, &command, access, timeout_ms).await?;

    match result {
        OneShot::ScpiError(err) => Err(ServerError::scpi(err.to_string())),
        OneShot::Ok(ok) => {
            let mut resp_headers = HeaderMap::new();
            resp_headers.insert("X-SCPI-Status", HeaderValue::from_static("OK"));
            if let Ok(v) = HeaderValue::from_str(&String::from(eff_access)) {
                resp_headers.insert("X-SCPI-Access", v);
            }
            if want_bytes {
                let mut buf = Vec::new();
                ok.write_bytes(&mut buf)
                    .map_err(|e| ServerError::internal(format!("failed to encode response: {e}")))?;
                resp_headers.insert(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/octet-stream"),
                );
                Ok((StatusCode::OK, resp_headers, buf).into_response())
            } else {
                resp_headers.insert(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("text/plain; charset=utf-8"),
                );
                Ok((StatusCode::OK, resp_headers, ok.to_string()).into_response())
            }
        }
    }
}
