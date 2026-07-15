//! `/api/v1` semantic resource handlers.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Extension, Path as AxumPath, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::Json;
use futures::Stream;
use qslib_core::commands::{StatusLedColor, StatusLedMode};
use qslib_core::protocol::ProtocolDefinition;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::auth::{require_role, Principal, Role};
use crate::dto::CapabilitiesDto;
use crate::error::ServerError;
use crate::events::{EventEnvelope, Replay};
use crate::operation::{CreateOperation, OperationRecord};
use crate::package;
use crate::service::{
    InstrumentOperation, InstrumentResult, OverwriteMode, PreflightRunInput, StartRunInput,
};
use crate::state::AppState;

pub async fn capabilities(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<CapabilitiesDto>, ServerError> {
    require_role(Extension(principal), Role::Observer)?;
    Ok(Json(CapabilitiesDto {
        api_version: "v1",
        resources: vec![
            "instrument",
            "files",
            "directories",
            "experiments",
            "runs",
            "operations",
            "events",
        ],
        file_contexts: state.contexts.keys().cloned().collect(),
        max_access: String::from(state.max_access.clone()).to_ascii_lowercase(),
        sse: true,
        raw_scpi: state.enable_raw_scpi,
        scpi_tunnel: state.enable_scpi_tunnel,
        file_writes: state.allow_file_writes,
        controls: state.allow_controls,
    }))
}

pub async fn instrument_status(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Response, ServerError> {
    require_role(Extension(principal), Role::Observer)?;
    match state.service.execute(InstrumentOperation::Status).await? {
        InstrumentResult::Status(status) => Ok(Json(status).into_response()),
        _ => Err(ServerError::internal(
            "instrument actor returned wrong response type",
        )),
    }
}

#[derive(Deserialize, serde::Serialize)]
pub struct EnabledRequest {
    enabled: bool,
}

pub async fn set_power(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<EnabledRequest>,
) -> Result<Response, ServerError> {
    require_controls(&state, principal)?;
    execute_unit(&state, InstrumentOperation::SetPower(request.enabled)).await
}

#[derive(Deserialize, serde::Serialize)]
pub struct BlockRequest {
    enabled: bool,
    target_c: Option<f64>,
}

pub async fn set_block(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<BlockRequest>,
) -> Result<Response, ServerError> {
    require_controls(&state, principal)?;
    if request.target_c.is_some_and(|value| !value.is_finite()) {
        return Err(ServerError::bad_request("target_c must be finite"));
    }
    execute_unit(
        &state,
        InstrumentOperation::SetBlock {
            enabled: request.enabled,
            target_c: request.target_c,
        },
    )
    .await
}

#[derive(Deserialize, serde::Serialize)]
pub struct IndicatorRequest {
    color: String,
    mode: String,
}

pub async fn set_indicator(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<IndicatorRequest>,
) -> Result<Response, ServerError> {
    require_controls(&state, principal)?;
    let color = StatusLedColor::try_from(request.color.as_str())
        .map_err(|_| ServerError::bad_request("invalid indicator color"))?;
    let mode = StatusLedMode::try_from(request.mode.as_str())
        .map_err(|_| ServerError::bad_request("mode must be on, blink, or off"))?;
    execute_unit(&state, InstrumentOperation::SetIndicator { color, mode }).await
}

pub async fn indicator_off(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Response, ServerError> {
    require_controls(&state, principal)?;
    execute_unit(&state, InstrumentOperation::IndicatorOff).await
}

#[derive(Deserialize, serde::Serialize)]
pub struct DrawerRequest {
    position: String,
    #[serde(default = "default_true")]
    lower_cover: bool,
    #[serde(default = "default_true")]
    verify: bool,
}

pub async fn set_drawer(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<DrawerRequest>,
) -> Result<Response, ServerError> {
    require_controls(&state, principal)?;
    let open = match request.position.to_ascii_lowercase().as_str() {
        "open" => true,
        "closed" => false,
        _ => return Err(ServerError::bad_request("position must be open or closed")),
    };
    execute_unit(
        &state,
        InstrumentOperation::Drawer {
            open,
            lower_cover: request.lower_cover,
            verify: request.verify,
        },
    )
    .await
}

#[derive(Deserialize, serde::Serialize)]
pub struct CoverRequest {
    position: String,
    #[serde(default = "default_true")]
    verify: bool,
    #[serde(default)]
    ensure_drawer: bool,
}

pub async fn set_cover(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<CoverRequest>,
) -> Result<Response, ServerError> {
    require_controls(&state, principal)?;
    if !request.position.eq_ignore_ascii_case("down") {
        return Err(ServerError::bad_request(
            "only cover position down is supported",
        ));
    }
    execute_unit(
        &state,
        InstrumentOperation::CoverDown {
            verify: request.verify,
            ensure_drawer: request.ensure_drawer,
        },
    )
    .await
}

pub async fn access_key(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
) -> Result<Response, ServerError> {
    require_controls(&state, principal.clone())?;
    enqueue_operation(
        &state,
        &principal,
        &headers,
        "access_key",
        "{}",
        InstrumentOperation::GenerateAccessKey,
    )
}

pub async fn restart(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
) -> Result<Response, ServerError> {
    require_controls(&state, principal.clone())?;
    enqueue_operation(
        &state,
        &principal,
        &headers,
        "instrument_restart",
        "{}",
        InstrumentOperation::Restart,
    )
}

pub async fn get_operation(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    AxumPath(id): AxumPath<Uuid>,
) -> Result<Json<OperationRecord>, ServerError> {
    require_role(Extension(principal), Role::Observer)?;
    state
        .operations
        .get(id)
        .map(Json)
        .ok_or_else(|| ServerError::not_found("operation not found"))
}

pub async fn events(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ServerError> {
    require_role(Extension(principal), Role::Observer)?;
    let last_id = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok());
    let mut initial = match state.events.replay_after(last_id) {
        Replay::Events(events) => VecDeque::from(events),
        Replay::Expired => {
            let snapshot = match state.service.execute(InstrumentOperation::Status).await {
                Ok(InstrumentResult::Status(status)) => json!(status),
                Err(error) => json!({"unavailable": error.message}),
                _ => Value::Null,
            };
            VecDeque::from([state.events.publish(
                "reset",
                json!({"reason": "history_expired", "status": snapshot}),
            )])
        }
    };
    let receiver = state.events.subscribe();
    let stream = futures::stream::unfold(
        EventStreamState {
            initial: std::mem::take(&mut initial),
            receiver,
            heartbeat: tokio::time::interval(Duration::from_secs(15)),
            close_after_reset: false,
        },
        |mut stream| async move {
            if let Some(envelope) = stream.initial.pop_front() {
                return Some((Ok(to_sse(envelope)), stream));
            }
            if stream.close_after_reset {
                return None;
            }
            tokio::select! {
                event = stream.receiver.recv() => match event {
                    Ok(envelope) => Some((Ok(to_sse(envelope)), stream)),
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        stream.close_after_reset = true;
                        Some((Ok(Event::default().event("reset").data("{\"reason\":\"subscriber_lag\"}")), stream))
                    }
                    Err(broadcast::error::RecvError::Closed) => None,
                },
                _ = stream.heartbeat.tick() => {
                    Some((Ok(Event::default().comment("heartbeat")), stream))
                }
            }
        },
    );
    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}

struct EventStreamState {
    initial: VecDeque<EventEnvelope>,
    receiver: broadcast::Receiver<EventEnvelope>,
    heartbeat: tokio::time::Interval,
    close_after_reset: bool,
}

fn to_sse(envelope: EventEnvelope) -> Event {
    Event::default()
        .id(envelope.id.to_string())
        .event(envelope.event)
        .json_data(json!({
            "timestamp": envelope.timestamp,
            "data": envelope.data,
        }))
        .expect("event envelope is serializable")
}

pub async fn list_experiments(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<Value>, ServerError> {
    require_role(Extension(principal), Role::Observer)?;
    let root = state.context_root("experiments")?;
    let mut experiments = Vec::new();
    collect_directory_names(root, &mut experiments, false)?;
    let staging = root.join(".qslib-staging");
    let mut staged = Vec::new();
    collect_directory_names(&staging, &mut staged, true)?;
    Ok(Json(json!({"experiments": experiments, "staged": staged})))
}

pub async fn get_experiment(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<Value>, ServerError> {
    require_role(Extension(principal), Role::Observer)?;
    let root = state.context_root("experiments")?;
    let working = root.join(&name).is_dir();
    let package_etag = package::package_etag(root, &name).ok();
    if !working && package_etag.is_none() {
        return Err(ServerError::not_found("experiment not found"));
    }
    Ok(Json(json!({
        "name": name,
        "working": working,
        "package_etag": package_etag,
    })))
}

pub async fn put_package(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    AxumPath(name): AxumPath<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ServerError> {
    require_role(Extension(principal), Role::Controller)?;
    if !state.allow_file_writes {
        return Err(ServerError::forbidden(
            "file writes are disabled by server policy",
        ));
    }
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if !content_type.starts_with("application/zip") {
        return Err(ServerError::bad_request(
            "Content-Type must be application/zip",
        ));
    }
    let root = state.context_root("experiments")?.to_path_buf();
    let staged = package::stage_package(root, name, body).await?;
    Ok((
        StatusCode::CREATED,
        [(header::ETAG, staged.etag.clone())],
        Json(staged),
    )
        .into_response())
}

pub async fn get_package(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    AxumPath(name): AxumPath<String>,
) -> Result<Response, ServerError> {
    require_role(Extension(principal), Role::Observer)?;
    let (bytes, etag) = package::read_package(state.context_root("experiments")?, &name)?;
    Ok((
        [
            (header::CONTENT_TYPE, "application/zip"),
            (header::ETAG, etag.as_str()),
        ],
        bytes,
    )
        .into_response())
}

pub async fn delete_package(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    AxumPath(name): AxumPath<String>,
    headers: HeaderMap,
) -> Result<StatusCode, ServerError> {
    require_role(Extension(principal), Role::Controller)?;
    if !state.allow_file_writes {
        return Err(ServerError::forbidden(
            "file writes are disabled by server policy",
        ));
    }
    let expected = headers
        .get(header::IF_MATCH)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| ServerError::bad_request("If-Match header is required"))?;
    package::delete_staged_package(state.context_root("experiments")?, &name, expected)?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn delete_experiment(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    AxumPath(name): AxumPath<String>,
) -> Result<StatusCode, ServerError> {
    require_role(Extension(principal), Role::Controller)?;
    if !state.allow_file_writes {
        return Err(ServerError::forbidden(
            "file writes are disabled by server policy",
        ));
    }
    package::validate_experiment_name(&name)?;
    execute_unit(
        &state,
        InstrumentOperation::DeleteExperiment {
            name,
            experiments_root: state.context_root("experiments")?.to_path_buf(),
        },
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
pub struct RunsQuery {
    location: Option<String>,
}

pub async fn list_runs(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(query): Query<RunsQuery>,
) -> Result<Json<Value>, ServerError> {
    require_role(Extension(principal), Role::Observer)?;
    let location = query.location.as_deref().unwrap_or("working");
    let (context, strip_eds) = match location {
        "working" => ("experiments", false),
        "completed" => ("public_run_complete", true),
        _ => {
            return Err(ServerError::bad_request(
                "location must be working or completed",
            ))
        }
    };
    let mut runs = Vec::new();
    collect_run_names(state.context_root(context)?, &mut runs, strip_eds)?;
    Ok(Json(json!({"location": location, "runs": runs})))
}

pub async fn current_run(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Response, ServerError> {
    require_role(Extension(principal), Role::Observer)?;
    match state
        .service
        .execute(InstrumentOperation::RunStatus)
        .await?
    {
        InstrumentResult::RunStatus(status) => Ok(Json(status).into_response()),
        _ => Err(ServerError::internal(
            "instrument actor returned wrong response type",
        )),
    }
}

pub async fn current_protocol(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Response, ServerError> {
    require_role(Extension(principal), Role::Observer)?;
    match state
        .service
        .execute(InstrumentOperation::RunningProtocol)
        .await?
    {
        InstrumentResult::RunningProtocol(protocol) => Ok(Json(protocol).into_response()),
        _ => Err(ServerError::internal(
            "instrument actor returned wrong response type",
        )),
    }
}

pub async fn get_run(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<Value>, ServerError> {
    require_role(Extension(principal), Role::Observer)?;
    let working = state.context_root("experiments")?.join(&name).is_dir();
    let completed = state
        .context_root("public_run_complete")?
        .join(format!("{name}.eds"))
        .is_file();
    if !working && !completed {
        return Err(
            ServerError::coded(StatusCode::NOT_FOUND, "run_not_found", "run not found")
                .details(json!({"name": name})),
        );
    }
    Ok(Json(
        json!({"name": name, "working": working, "completed": completed}),
    ))
}

#[derive(Debug, Deserialize, serde::Serialize)]
pub struct StartRequest {
    experiment: String,
    package_etag: String,
    overwrite: String,
    #[serde(default)]
    require_exclusive: bool,
    #[serde(default = "default_true")]
    require_drawer_check: bool,
}

#[derive(Debug, Deserialize)]
pub struct PreflightQuery {
    experiment: String,
    overwrite: String,
}

pub async fn preflight_run(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(query): Query<PreflightQuery>,
) -> Result<Response, ServerError> {
    require_controls(&state, principal)?;
    package::validate_experiment_name(&query.experiment)?;
    let overwrite = parse_overwrite(&query.overwrite)?;
    execute_unit(
        &state,
        InstrumentOperation::PreflightRun(PreflightRunInput {
            experiment: query.experiment,
            overwrite,
            experiments_root: state.context_root("experiments")?.to_path_buf(),
            completed_root: state.context_root("public_run_complete")?.to_path_buf(),
        }),
    )
    .await
}

pub async fn start_run(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    Json(request): Json<StartRequest>,
) -> Result<Response, ServerError> {
    require_controls(&state, principal.clone())?;
    let experiments_root = state.context_root("experiments")?.to_path_buf();
    let actual_etag = package::package_etag(&experiments_root, &request.experiment)?;
    if actual_etag != request.package_etag {
        return Err(ServerError::conflict(format!(
            "package ETag mismatch: current value is {actual_etag}"
        )));
    }
    let overwrite = parse_overwrite(&request.overwrite)?;
    let (protocol, protocol_scpi) = package::load_protocol(&experiments_root, &request.experiment)?;
    let staged_root = package::staged_path(&experiments_root, &request.experiment)?;
    let fingerprint = serde_json::to_string(&request).map_err(|error| {
        ServerError::internal(format!("cannot fingerprint start request: {error}"))
    })?;
    enqueue_operation(
        &state,
        &principal,
        &headers,
        "run_start",
        &fingerprint,
        InstrumentOperation::StartRun(StartRunInput {
            experiment: request.experiment,
            overwrite,
            require_exclusive: request.require_exclusive,
            require_drawer_check: request.require_drawer_check,
            experiments_root,
            completed_root: state.context_root("public_run_complete")?.to_path_buf(),
            staged_root,
            protocol_scpi,
            protocol_name: protocol.name,
            sample_volume: protocol.sample_volume,
            run_mode: protocol.run_mode,
        }),
    )
}

fn parse_overwrite(value: &str) -> Result<OverwriteMode, ServerError> {
    Ok(match value {
        "false" => OverwriteMode::False,
        "true" => OverwriteMode::True,
        "incomplete" => OverwriteMode::Incomplete,
        _ => {
            return Err(ServerError::bad_request(
                "overwrite must be false, true, or incomplete",
            ))
        }
    })
}

pub async fn run_action(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    AxumPath((name, action)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, ServerError> {
    require_controls(&state, principal.clone())?;
    let operation = match action.as_str() {
        "pause" => InstrumentOperation::Pause {
            name: Some(name.clone()),
        },
        "resume" => InstrumentOperation::Resume {
            name: Some(name.clone()),
        },
        "stop" => InstrumentOperation::Stop {
            name: Some(name.clone()),
        },
        "abort" => InstrumentOperation::Abort {
            name: Some(name.clone()),
        },
        "compile" => InstrumentOperation::Compile {
            name: name.clone(),
            experiments_root: state.context_root("experiments")?.to_path_buf(),
            completed_root: state.context_root("public_run_complete")?.to_path_buf(),
        },
        _ => return Err(ServerError::not_found("unknown run action")),
    };
    enqueue_operation(
        &state,
        &principal,
        &headers,
        &format!("run_{action}"),
        &format!("{name}:{action}"),
        operation,
    )
}

#[derive(Deserialize)]
pub struct ProtocolQuery {
    mode: Option<String>,
}

#[derive(Deserialize)]
pub struct ProtocolUpdateRequest {
    /// Exact protocol definition sent to InstrumentServer.
    scpi: String,
    /// Approximate Android display document. This is never used to decide what
    /// protocol the instrument should execute.
    tcprotocol_xml: String,
}

pub async fn put_protocol(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    AxumPath(name): AxumPath<String>,
    Query(query): Query<ProtocolQuery>,
    Json(request): Json<ProtocolUpdateRequest>,
) -> Result<Response, ServerError> {
    require_controls(&state, principal)?;
    let mode = query.mode.as_deref().unwrap_or("replace");
    if !matches!(mode, "replace" | "from_now") {
        return Err(ServerError::bad_request("mode must be replace or from_now"));
    }
    let protocol = ProtocolDefinition::new(request.scpi.clone())
        .map_err(|error| ServerError::bad_request(format!("invalid protocol SCPI: {error}")))?;
    package::validate_xml_document(&request.tcprotocol_xml, "tcprotocol_xml")?;
    let root = state
        .context_root("experiments")?
        .join(&name)
        .join("apldbio/sds");
    if !root.is_dir() {
        return Err(ServerError::coded(
            StatusCode::NOT_FOUND,
            "run_not_found",
            "working run not found",
        )
        .details(json!({"name": name})));
    }
    execute_unit(
        &state,
        InstrumentOperation::ReplaceProtocol {
            name: name.clone(),
            protocol,
        },
    )
    .await?;
    // Store the exact QSLib definition separately from the Android display
    // approximation. Only the former is an authoritative protocol source.
    atomic_write(
        &root.join("tcprotocol.xml"),
        request.tcprotocol_xml.as_bytes(),
    )
    .map_err(|error| error.outcome("unknown"))?;
    let escaped = request
        .scpi
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;");
    atomic_write(
        &root.join("qsl-tcprotocol.xml"),
        format!(
            "<QSTCProtocol><QSLibProtocolCommand>{escaped}</QSLibProtocolCommand></QSTCProtocol>"
        )
        .as_bytes(),
    )
    .map_err(|error| error.outcome("unknown"))?;
    Ok(Json(json!({"name": name, "mode": mode})).into_response())
}

pub async fn get_eds(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    AxumPath(name): AxumPath<String>,
) -> Result<Response, ServerError> {
    require_role(Extension(principal), Role::Observer)?;
    let path = state
        .context_root("public_run_complete")?
        .join(format!("{name}.eds"));
    let bytes = std::fs::read(path).map_err(|_| {
        ServerError::coded(StatusCode::NOT_FOUND, "run_not_found", "EDS not found")
            .details(json!({"name": name}))
    })?;
    Ok(([(header::CONTENT_TYPE, "application/zip")], bytes).into_response())
}

async fn execute_unit(
    state: &AppState,
    operation: InstrumentOperation,
) -> Result<Response, ServerError> {
    match state.service.execute(operation).await? {
        InstrumentResult::Unit => Ok(StatusCode::NO_CONTENT.into_response()),
        _ => Err(ServerError::internal(
            "instrument actor returned wrong response type",
        )),
    }
}

fn require_controls(state: &AppState, principal: Principal) -> Result<(), ServerError> {
    require_role(Extension(principal), Role::Controller)?;
    require_control_policy(state)
}

fn require_control_policy(state: &AppState) -> Result<(), ServerError> {
    if !state.allow_controls {
        return Err(ServerError::forbidden(
            "instrument controls are disabled by server policy",
        ));
    }
    Ok(())
}

fn enqueue_operation(
    state: &AppState,
    principal: &Principal,
    headers: &HeaderMap,
    kind: &str,
    fingerprint_input: &str,
    operation: InstrumentOperation,
) -> Result<Response, ServerError> {
    let key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| ServerError::bad_request("Idempotency-Key header is required"))?;
    let fingerprint = format!("{:x}", Sha256::digest(fingerprint_input.as_bytes()));
    match state.operations.create(kind, principal, key, fingerprint)? {
        CreateOperation::Existing(record) => {
            Ok((StatusCode::ACCEPTED, Json(record)).into_response())
        }
        CreateOperation::New(record) => {
            let receiver = match state.service.enqueue(operation) {
                Ok(receiver) => receiver,
                Err(error) => {
                    state
                        .operations
                        .failed(record.id, ServerError::queue_full());
                    return Err(error);
                }
            };
            let store = state.operations.clone();
            let operation_id = record.id;
            tokio::spawn(async move {
                store.running(operation_id);
                match receiver.await {
                    Ok(Ok(InstrumentResult::AccessKey(key))) => {
                        store.succeeded(operation_id, json!({"key": key}))
                    }
                    Ok(Ok(_)) => store.succeeded(operation_id, json!({})),
                    Ok(Err(error)) => store.failed(operation_id, error),
                    Err(_) => store.failed(
                        operation_id,
                        ServerError::unavailable("instrument actor dropped operation")
                            .outcome("unknown"),
                    ),
                }
            });
            Ok((StatusCode::ACCEPTED, Json(record)).into_response())
        }
    }
}

fn collect_directory_names(
    root: &std::path::Path,
    output: &mut Vec<String>,
    include_hidden: bool,
) -> Result<(), ServerError> {
    let Ok(entries) = std::fs::read_dir(root) else {
        return Ok(());
    };
    for entry in entries {
        let entry = entry.map_err(|error| {
            ServerError::internal(format!("failed to list experiments: {error}"))
        })?;
        if entry.file_type().map(|kind| kind.is_dir()).unwrap_or(false) {
            let name = entry.file_name().to_string_lossy().into_owned();
            if include_hidden || !name.starts_with('.') {
                output.push(name);
            }
        }
    }
    output.sort();
    Ok(())
}

fn collect_run_names(
    root: &std::path::Path,
    output: &mut Vec<String>,
    strip_eds: bool,
) -> Result<(), ServerError> {
    let Ok(entries) = std::fs::read_dir(root) else {
        return Ok(());
    };
    for entry in entries {
        let entry = entry
            .map_err(|error| ServerError::internal(format!("failed to list runs: {error}")))?;
        let kind = entry
            .file_type()
            .map_err(|error| ServerError::internal(format!("failed to inspect run: {error}")))?;
        let mut name = entry.file_name().to_string_lossy().into_owned();
        if strip_eds {
            if !kind.is_file() || !name.to_ascii_lowercase().ends_with(".eds") {
                continue;
            }
            name.truncate(name.len() - 4);
        } else if !kind.is_dir() || name.starts_with('.') {
            continue;
        }
        output.push(name);
    }
    output.sort();
    Ok(())
}

fn atomic_write(path: &std::path::Path, bytes: &[u8]) -> Result<(), ServerError> {
    let parent = path
        .parent()
        .ok_or_else(|| ServerError::internal("protocol path has no parent"))?;
    let temp = parent.join(format!(".qslib-protocol-{}", Uuid::new_v4()));
    std::fs::write(&temp, bytes)
        .map_err(|error| ServerError::internal(format!("failed to stage protocol: {error}")))?;
    std::fs::rename(&temp, path)
        .map_err(|error| ServerError::internal(format!("failed to replace protocol: {error}")))?;
    Ok(())
}

fn default_true() -> bool {
    true
}
