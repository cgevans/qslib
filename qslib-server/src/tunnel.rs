//! Streaming SCPI tunnel: upgrade the HTTP connection and splice bytes
//! bidirectionally to the localhost plaintext SCPI server.
//!
//! qslib then speaks its normal plaintext SCPI (including async pub/sub) over
//! the tunnel, with the instrument doing no TLS. Accessible either as
//! `CONNECT /scpi` or as `GET /scpi` with an `Upgrade` header.

use axum::extract::{Request, State};
use axum::http::{header, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tracing::{debug, warn};

use crate::error::ServerError;
use crate::state::AppState;

pub async fn tunnel(
    State(state): State<AppState>,
    mut req: Request,
) -> Result<Response, ServerError> {
    let is_connect = req.method() == Method::CONNECT;

    // For a GET upgrade, require the Upgrade request header so we do not hijack
    // ordinary GETs.
    if !is_connect {
        let wants_upgrade = req
            .headers()
            .get(header::UPGRADE)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.eq_ignore_ascii_case("qslib-scpi"))
            .unwrap_or(false);
        if !wants_upgrade {
            return Err(ServerError::bad_request(
                "GET /scpi requires `Upgrade: qslib-scpi`; use POST /scpi for one-shot commands",
            ));
        }
    }

    // Bound concurrent tunnels so idle/abandoned ones cannot exhaust the
    // instrument's SCPI connection capacity. The permit is held for the whole
    // tunnel lifetime and released when the splice ends.
    let permit = state
        .tunnels
        .clone()
        .try_acquire_owned()
        .map_err(|_| ServerError::unavailable("too many concurrent SCPI tunnels"))?;

    // Fail fast if the SCPI server is unreachable, before upgrading.
    let upstream = TcpStream::connect(state.scpi_target).await.map_err(|e| {
        ServerError::unavailable(format!(
            "cannot connect to SCPI server at {}: {e}",
            state.scpi_target
        ))
    })?;

    let on_upgrade = hyper::upgrade::on(&mut req);
    let target = state.scpi_target;
    tokio::spawn(async move {
        let _permit = permit; // released when this task ends
        match on_upgrade.await {
            Ok(upgraded) => {
                let mut client = TokioIo::new(upgraded);
                let mut server = upstream;
                match tokio::io::copy_bidirectional(&mut client, &mut server).await {
                    Ok((c2s, s2c)) => {
                        debug!("scpi tunnel to {target} closed ({c2s} up, {s2c} down bytes)")
                    }
                    Err(e) => warn!("scpi tunnel to {target} error: {e}"),
                }
            }
            Err(e) => warn!("scpi tunnel upgrade failed: {e}"),
        }
    });

    if is_connect {
        // A 2xx response to CONNECT tells the client the tunnel is established.
        Ok(StatusCode::OK.into_response())
    } else {
        // GET upgrade: 101 Switching Protocols.
        Ok((
            StatusCode::SWITCHING_PROTOCOLS,
            [
                (header::CONNECTION, "upgrade"),
                (header::UPGRADE, "qslib-scpi"),
            ],
        )
            .into_response())
    }
}
