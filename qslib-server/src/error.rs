//! Structured API errors and HTTP status mapping.

use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug)]
pub struct ServerError {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
    pub retryable: bool,
    pub outcome: &'static str,
    pub scpi_error: bool,
    pub request_id: String,
    pub details: Option<Value>,
}

#[derive(Serialize)]
struct ErrorBody {
    error: ErrorDetail,
    request_id: String,
}

#[derive(Serialize)]
struct ErrorDetail {
    code: &'static str,
    message: String,
    retryable: bool,
    outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<Value>,
}

impl ServerError {
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            retryable: false,
            outcome: "not_started",
            scpi_error: false,
            request_id: Uuid::new_v4().to_string(),
            details: None,
        }
    }

    pub fn retryable(mut self, retryable: bool) -> Self {
        self.retryable = retryable;
        self
    }

    pub fn outcome(mut self, outcome: &'static str) -> Self {
        self.outcome = outcome;
        self
    }

    pub fn details(mut self, details: Value) -> Self {
        self.details = Some(details);
        self
    }

    pub fn coded(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self::new(status, code, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", message)
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, "forbidden", message)
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid_input", message)
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "conflict", message)
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthorized", message)
    }

    pub fn timeout(message: impl Into<String>) -> Self {
        Self::new(StatusCode::GATEWAY_TIMEOUT, "deadline_exceeded", message)
            .retryable(true)
            .outcome("unknown")
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "upstream_unavailable",
            message,
        )
        .retryable(true)
    }

    pub fn queue_full() -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "queue_full",
            "semantic operation queue is full",
        )
        .retryable(true)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", message)
    }

    pub fn instrument_rejection(message: impl Into<String>) -> Self {
        let mut error = Self::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "instrument_rejected",
            message,
        );
        error.scpi_error = true;
        error
    }

    /// Isolated raw-SCPI command rejection. Kept separate from semantic
    /// instrument rejections so clients cannot mistake the endpoint contract.
    pub fn scpi(message: impl Into<String>) -> Self {
        let mut error = Self::new(StatusCode::UNPROCESSABLE_ENTITY, "scpi_error", message);
        error.scpi_error = true;
        error
    }
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let scpi_message = self.message.clone();
        let retry_after = self.code == "queue_full";
        let mut response = (
            self.status,
            Json(ErrorBody {
                error: ErrorDetail {
                    code: self.code,
                    message: self.message,
                    retryable: self.retryable,
                    outcome: self.outcome,
                    details: self.details,
                },
                request_id: self.request_id.clone(),
            }),
        )
            .into_response();
        if let Ok(value) = HeaderValue::from_str(&self.request_id) {
            response.headers_mut().insert("x-request-id", value);
        }
        if retry_after {
            response
                .headers_mut()
                .insert("retry-after", HeaderValue::from_static("1"));
        }
        if self.scpi_error {
            let value = HeaderValue::from_str(&scpi_message)
                .unwrap_or_else(|_| HeaderValue::from_static("instrument error"));
            response.headers_mut().insert("x-scpi-error", value);
        }
        response
    }
}
