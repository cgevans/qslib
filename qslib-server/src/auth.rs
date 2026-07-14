//! Bearer-token authentication middleware.

use axum::extract::State;
use axum::http::{header, Request};
use axum::middleware::Next;
use axum::response::Response;

use crate::error::ServerError;
use crate::state::AppState;

/// Constant-time-ish comparison of two byte slices. Avoids leaking the token
/// length-prefix match timing; not a hard requirement on a private link but
/// cheap to do correctly.
fn tokens_match(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Middleware that enforces `Authorization: Bearer <token>` on every request
/// when a token is configured. When no token is configured (`--no-auth`), all
/// requests pass.
pub async fn require_bearer(
    State(state): State<AppState>,
    req: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, ServerError> {
    let Some(expected) = state.token.as_deref() else {
        return Ok(next.run(req).await);
    };

    let provided = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim);

    match provided {
        Some(tok) if tokens_match(tok.as_bytes(), expected.as_bytes()) => Ok(next.run(req).await),
        _ => Err(ServerError::unauthorized("missing or invalid bearer token")),
    }
}
