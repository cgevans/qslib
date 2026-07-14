//! Agent error type mapped to HTTP responses with JSON bodies.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

/// An error that becomes an HTTP response. The `detail` is optional extra
/// context; the `header` field, when set, is emitted as an `X-SCPI-Error`
/// header so scripting clients can distinguish SCPI command errors.
#[derive(Debug)]
pub struct AgentError {
    pub status: StatusCode,
    pub error: String,
    pub detail: Option<String>,
    pub scpi_error: bool,
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
}

impl AgentError {
    pub fn new(status: StatusCode, error: impl Into<String>) -> Self {
        Self {
            status,
            error: error.into(),
            detail: None,
            scpi_error: false,
        }
    }

    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, msg)
    }

    pub fn forbidden(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, msg)
    }

    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, msg)
    }

    pub fn unauthorized(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, msg)
    }

    pub fn timeout(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::GATEWAY_TIMEOUT, msg)
    }

    pub fn unavailable(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::SERVICE_UNAVAILABLE, msg)
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, msg)
    }

    /// A SCPI command error (client/command error): HTTP 400 with an
    /// `X-SCPI-Error` marker header.
    pub fn scpi(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            error: msg.into(),
            detail: None,
            scpi_error: true,
        }
    }
}

impl IntoResponse for AgentError {
    fn into_response(self) -> Response {
        let mut resp = (
            self.status,
            Json(ErrorBody {
                error: self.error.clone(),
                detail: self.detail,
            }),
        )
            .into_response();
        if self.scpi_error {
            if let Ok(val) = self.error.parse::<axum::http::HeaderValue>() {
                resp.headers_mut().insert("X-SCPI-Error", val);
            } else {
                resp.headers_mut()
                    .insert("X-SCPI-Error", axum::http::HeaderValue::from_static("1"));
            }
        }
        resp
    }
}
