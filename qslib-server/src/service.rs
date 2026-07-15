//! Single-connection managed InstrumentServer actor.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use qslib_core::com::QSConnection;
use qslib_core::commands::{
    AbortRun, AccessLevel, AccessLevelSet, AccessState, AccessStateQuery, BlockQuery, BlockSet,
    CommandBuilder, ControlZonesQuery, CoverDown, CoverPosition, CoverPositionQuery, DrawerClose,
    DrawerOpen, DrawerStatus, DrawerStatusQuery, ExperimentCollected, ExperimentCompile,
    ExperimentNew, FileMove, MachineStatusQuery, OkParseError, PauseRun, PowerQuery, PowerSet,
    RandomKeyQuery, ReceiveNextResponseError, ReceiveOkResponseError, RemainingTimeQuery,
    RestartSystem, ResumeRun, RunStart, RunStatusQuery, RunningProtocolBodyQuery,
    RunningProtocolMetadataQuery, StatusLedColor, StatusLedMode, StatusLedOff, StatusLedQuery,
    StatusLedSet, StopRun, Subscribe,
};
use qslib_core::parser::{ErrorResponse, OkResponse};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamMap;
use tracing::{debug, info, warn};

use crate::dto::{AccessDto, InstrumentStatusDto, RunStatusDto, RunningProtocolDto};
use crate::error::ServerError;
use crate::events::EventHub;
use qslib_core::protocol::ProtocolDefinition;

const CONNECT_DEADLINE: Duration = Duration::from_secs(10);
const QUERY_DEADLINE: Duration = Duration::from_secs(10);
const CONTROL_DEADLINE: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct ServiceConfig {
    pub target: SocketAddr,
    pub password: Option<String>,
    pub max_access: AccessLevel,
    pub queue_capacity: usize,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ServiceHealth {
    pub ready: bool,
    pub generation: u64,
    pub current_access: Option<AccessDto>,
    pub last_successful_command: Option<DateTime<Utc>>,
    pub reconnect_count: u64,
}

#[derive(Debug, Clone)]
pub enum InstrumentOperation {
    Status,
    RunStatus,
    RunningProtocol,
    SetPower(bool),
    SetBlock {
        enabled: bool,
        target_c: Option<f64>,
    },
    SetIndicator {
        color: StatusLedColor,
        mode: StatusLedMode,
    },
    IndicatorOff,
    Drawer {
        open: bool,
        lower_cover: bool,
        verify: bool,
    },
    CoverDown {
        verify: bool,
        ensure_drawer: bool,
    },
    Pause {
        name: Option<String>,
    },
    Resume {
        name: Option<String>,
    },
    Stop {
        name: Option<String>,
    },
    Abort {
        name: Option<String>,
    },
    StartRun(StartRunInput),
    PreflightRun(PreflightRunInput),
    Compile {
        name: String,
        experiments_root: std::path::PathBuf,
        completed_root: std::path::PathBuf,
    },
    DeleteExperiment {
        name: String,
        experiments_root: std::path::PathBuf,
    },
    ReplaceProtocol {
        name: String,
        protocol: ProtocolDefinition,
    },
    GenerateAccessKey,
    Restart,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverwriteMode {
    False,
    True,
    Incomplete,
}

#[derive(Debug, Clone)]
pub struct StartRunInput {
    pub experiment: String,
    pub overwrite: OverwriteMode,
    pub require_exclusive: bool,
    pub require_drawer_check: bool,
    pub experiments_root: std::path::PathBuf,
    pub completed_root: std::path::PathBuf,
    pub staged_root: std::path::PathBuf,
    pub protocol_scpi: String,
    pub protocol_name: String,
    pub sample_volume: f64,
    pub run_mode: String,
}

#[derive(Debug, Clone)]
pub struct PreflightRunInput {
    pub experiment: String,
    pub overwrite: OverwriteMode,
    pub experiments_root: std::path::PathBuf,
    pub completed_root: std::path::PathBuf,
}

impl InstrumentOperation {
    fn required_access(&self) -> AccessLevel {
        match self {
            Self::Status | Self::RunStatus | Self::RunningProtocol | Self::PreflightRun(_) => {
                AccessLevel::Observer
            }
            Self::GenerateAccessKey => AccessLevel::Controller,
            Self::Restart => AccessLevel::Controller,
            _ => AccessLevel::Controller,
        }
    }

    fn exclusive(&self) -> bool {
        matches!(
            self,
            Self::Pause { .. } | Self::Resume { .. } | Self::Stop { .. } | Self::Abort { .. }
        ) || matches!(self, Self::StartRun(input) if input.require_exclusive)
    }

    fn read_only(&self) -> bool {
        matches!(
            self,
            Self::Status | Self::RunStatus | Self::RunningProtocol | Self::PreflightRun(_)
        )
    }

    fn deadline(&self) -> Duration {
        match self {
            Self::Status | Self::RunStatus | Self::RunningProtocol | Self::PreflightRun(_) => {
                QUERY_DEADLINE
            }
            Self::StartRun(_) => Duration::from_secs(120),
            Self::Compile { .. } => Duration::from_secs(10 * 60),
            _ => CONTROL_DEADLINE,
        }
    }
}

#[derive(Debug, Clone)]
pub enum InstrumentResult {
    Status(Box<InstrumentStatusDto>),
    RunStatus(RunStatusDto),
    RunningProtocol(RunningProtocolDto),
    AccessKey(String),
    Unit,
}

struct Job {
    operation: InstrumentOperation,
    response: oneshot::Sender<Result<InstrumentResult, ServerError>>,
    attempt: u8,
}

/// Cloneable queue handle. Only the actor task ever owns a [`QSConnection`].
#[derive(Clone)]
pub struct InstrumentService {
    sender: mpsc::Sender<Job>,
    health: watch::Receiver<ServiceHealth>,
    events: EventHub,
    shutdown: watch::Sender<bool>,
}

impl InstrumentService {
    pub fn spawn(config: ServiceConfig, events: EventHub) -> Self {
        let (sender, receiver) = mpsc::channel(config.queue_capacity.max(1));
        let (health_sender, health) = watch::channel(ServiceHealth::default());
        let (shutdown, shutdown_receiver) = watch::channel(false);
        tokio::spawn(run_actor(
            config,
            receiver,
            health_sender,
            events.clone(),
            shutdown_receiver,
        ));
        Self {
            sender,
            health,
            events,
            shutdown,
        }
    }

    pub fn enqueue(
        &self,
        operation: InstrumentOperation,
    ) -> Result<oneshot::Receiver<Result<InstrumentResult, ServerError>>, ServerError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .try_send(Job {
                operation,
                response,
                attempt: 0,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => ServerError::queue_full(),
                mpsc::error::TrySendError::Closed(_) => {
                    ServerError::unavailable("instrument service is shutting down")
                }
            })?;
        Ok(receiver)
    }

    pub async fn execute(
        &self,
        operation: InstrumentOperation,
    ) -> Result<InstrumentResult, ServerError> {
        self.enqueue(operation)?
            .await
            .map_err(|_| ServerError::unavailable("instrument actor dropped the operation"))?
    }

    pub fn health(&self) -> ServiceHealth {
        self.health.borrow().clone()
    }

    pub fn queue_depth(&self) -> usize {
        self.sender.max_capacity() - self.sender.capacity()
    }

    pub fn events(&self) -> &EventHub {
        &self.events
    }

    pub async fn shutdown(&self) {
        let _ = self.shutdown.send(true);
        let mut health = self.health.clone();
        let _ = tokio::time::timeout(Duration::from_secs(12), async move {
            while health.borrow().ready {
                if health.changed().await.is_err() {
                    break;
                }
            }
        })
        .await;
    }
}

struct ExecutionFailure {
    error: ServerError,
    reconnect: bool,
}

impl ExecutionFailure {
    fn transport(message: impl Into<String>) -> Self {
        Self {
            error: ServerError::unavailable(message),
            reconnect: true,
        }
    }

    fn semantic(error: ServerError) -> Self {
        Self {
            error,
            reconnect: false,
        }
    }
}

async fn run_actor(
    config: ServiceConfig,
    mut jobs: mpsc::Receiver<Job>,
    health_sender: watch::Sender<ServiceHealth>,
    events: EventHub,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut health = ServiceHealth::default();
    let mut backoff = Duration::from_millis(250);
    let mut pending: Option<Job> = None;

    loop {
        if *shutdown.borrow() {
            return;
        }
        let connection = match connect_and_initialize(&config).await {
            Ok(connection) => connection,
            Err(error) => {
                health.ready = false;
                health.current_access = None;
                let _ = health_sender.send(health.clone());
                warn!("managed SCPI connect failed: {error}");
                tokio::select! {
                    _ = tokio::time::sleep(jitter(backoff, health.reconnect_count)) => {},
                    job = jobs.recv(), if pending.is_none() => {
                        let Some(job) = job else { return };
                        pending = Some(job);
                    }
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            return;
                        }
                    }
                }
                backoff = (backoff * 2).min(Duration::from_secs(30));
                health.reconnect_count += 1;
                continue;
            }
        };

        backoff = Duration::from_millis(250);
        let mut subscriptions = subscription_streams(&connection);
        health.ready = true;
        health.generation += 1;
        health.current_access = Some(AccessDto {
            level: "observer".to_string(),
            exclusive: false,
            stealth: false,
        });
        let _ = health_sender.send(health.clone());
        info!(
            generation = health.generation,
            "managed SCPI connection ready"
        );

        let reset_snapshot = fetch_status(&connection).await.ok();
        events.publish(
            "reset",
            json!({
                "generation": health.generation,
                "connected": true,
                "status": reset_snapshot,
            }),
        );

        let mut status_cache: Option<(Instant, InstrumentStatusDto)> =
            reset_snapshot.map(|status| (Instant::now(), status));
        let mut reconnect = false;

        while !reconnect {
            let next_job = async {
                if pending.is_some() {
                    pending.take()
                } else {
                    jobs.recv().await
                }
            };
            tokio::select! {
                job = next_job => {
                    let Some(mut job) = job else {
                        shutdown_connection(connection, &mut health, &health_sender).await;
                        return;
                    };
                    let result = execute_operation(
                        &connection,
                        &config,
                        &job.operation,
                        &mut health,
                        &health_sender,
                        &mut status_cache,
                    ).await;
                    match result {
                        Ok(value) => {
                            health.last_successful_command = Some(Utc::now());
                            let _ = health_sender.send(health.clone());
                            let _ = job.response.send(Ok(value));
                        }
                        Err(failure) if failure.reconnect && job.operation.read_only() && job.attempt == 0 => {
                            job.attempt = 1;
                            pending = Some(job);
                            reconnect = true;
                        }
                        Err(failure) => {
                            let _ = job.response.send(Err(failure.error));
                            reconnect = failure.reconnect;
                        }
                    }
                }
                subscription = subscriptions.next() => {
                    match subscription {
                        Some((topic, Ok(message))) => {
                            events.publish(
                                topic.to_ascii_lowercase(),
                                json!({"instrument_timestamp": message.timestamp, "message": message.message}),
                            );
                        }
                        Some((_topic, Err(_lagged))) => {
                            reconnect = true;
                        }
                        None => reconnect = true,
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(1)) => {
                    if !connection.is_connected().await {
                        reconnect = true;
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        shutdown_connection(connection, &mut health, &health_sender).await;
                        return;
                    }
                }
            }
        }

        health.ready = false;
        health.current_access = None;
        let _ = health_sender.send(health.clone());
        events.publish(
            "connection",
            json!({"generation": health.generation, "connected": false}),
        );
        connection.close().await;
        health.reconnect_count += 1;
    }
}

async fn connect_and_initialize(config: &ServiceConfig) -> Result<QSConnection, String> {
    if config.max_access < AccessLevel::Observer {
        return Err("--max-access must permit Observer".to_string());
    }
    let host = config.target.ip().to_string();
    let connection = tokio::time::timeout(
        CONNECT_DEADLINE,
        QSConnection::connect_tcp(&host, config.target.port()),
    )
    .await
    .map_err(|_| "greeting deadline exceeded".to_string())?
    .map_err(|error| error.to_string())?;

    let access_result = match config.password.as_deref() {
        Some(password) => {
            connection
                .authenticate_and_set_access_level(password, AccessLevel::Observer)
                .await
        }
        None => connection.set_access_level(AccessLevel::Observer).await,
    };
    access_result.map_err(|error| format!("failed to establish Observer access: {error}"))?;
    let access: AccessState = run_command(&connection, AccessStateQuery, QUERY_DEADLINE)
        .await
        .map_err(|failure| failure.error.message)?;
    if access.level != AccessLevel::Observer || access.exclusive || access.stealth {
        connection.close().await;
        return Err(format!(
            "unexpected access tuple after initialization: {access:?}"
        ));
    }

    // Establish subscriptions before publishing readiness. The stream objects
    // are created by the actor loop after this acknowledgement.
    run_command(
        &connection,
        Subscribe::topics(&["Temperature", "Time", "Run", "LEDStatus"]).with_timestamp(true),
        QUERY_DEADLINE,
    )
    .await
    .map_err(|failure| failure.error.message)?;
    Ok(connection)
}

async fn execute_operation(
    connection: &QSConnection,
    config: &ServiceConfig,
    operation: &InstrumentOperation,
    health: &mut ServiceHealth,
    health_sender: &watch::Sender<ServiceHealth>,
    status_cache: &mut Option<(Instant, InstrumentStatusDto)>,
) -> Result<InstrumentResult, ExecutionFailure> {
    let required = operation.required_access();
    if required > config.max_access {
        return Err(ExecutionFailure::semantic(ServerError::forbidden(format!(
            "operation requires {} access, above configured cap {}",
            String::from(required),
            String::from(config.max_access.clone())
        ))));
    }

    if required > AccessLevel::Observer {
        set_and_verify_access(connection, required.clone(), operation.exclusive()).await?;
        health.current_access = Some(AccessDto {
            level: String::from(required.clone()).to_ascii_lowercase(),
            exclusive: operation.exclusive(),
            stealth: false,
        });
        let _ = health_sender.send(health.clone());
    }

    let result: Result<InstrumentResult, ExecutionFailure> =
        match tokio::time::timeout(operation.deadline(), async {
            match operation {
                InstrumentOperation::Status => {
                    // Explicit status requests always reflect a fresh SCPI
                    // observation. The cached snapshot is retained only for
                    // event/reset bookkeeping, never as a public query result.
                    let status = fetch_status(connection).await?;
                    *status_cache = Some((Instant::now(), status.clone()));
                    Ok(InstrumentResult::Status(Box::new(status)))
                }
                InstrumentOperation::RunStatus => {
                    let status = run_command(connection, RunStatusQuery, QUERY_DEADLINE).await?;
                    let remaining = fetch_remaining_time(connection).await?;
                    Ok(InstrumentResult::RunStatus(RunStatusDto::from_parts(
                        status, remaining,
                    )))
                }
                InstrumentOperation::RunningProtocol => {
                    let status = run_command(connection, RunStatusQuery, QUERY_DEADLINE).await?;
                    if status.name == "-" || status.state.eq_ignore_ascii_case("IDLE") {
                        return Err(ExecutionFailure::semantic(ServerError::not_found(
                            "no protocol is currently running",
                        )));
                    }
                    let metadata =
                        run_command(connection, RunningProtocolMetadataQuery, QUERY_DEADLINE)
                            .await?;
                    let body = run_command(connection, RunningProtocolBodyQuery, QUERY_DEADLINE)
                        .await?
                        .0;
                    let scpi = format!(
                        "PROT -volume={} -runmode={} {} {}",
                        metadata.sample_volume,
                        metadata.run_mode,
                        shell_words::quote(&metadata.name),
                        body
                    );
                    Ok(InstrumentResult::RunningProtocol(RunningProtocolDto {
                        name: metadata.name,
                        sample_volume: metadata.sample_volume,
                        run_mode: metadata.run_mode,
                        scpi,
                    }))
                }
                InstrumentOperation::SetPower(enabled) => {
                    run_command(
                        connection,
                        if *enabled {
                            PowerSet::on()
                        } else {
                            PowerSet::off()
                        },
                        operation.deadline(),
                    )
                    .await?;
                    *status_cache = None;
                    Ok(InstrumentResult::Unit)
                }
                InstrumentOperation::SetBlock { enabled, target_c } => {
                    run_command(
                        connection,
                        BlockSet {
                            enabled: *enabled,
                            target_c: *target_c,
                        },
                        operation.deadline(),
                    )
                    .await?;
                    *status_cache = None;
                    Ok(InstrumentResult::Unit)
                }
                InstrumentOperation::SetIndicator { color, mode } => {
                    run_command(
                        connection,
                        StatusLedSet::new(*color, *mode),
                        operation.deadline(),
                    )
                    .await?;
                    *status_cache = None;
                    Ok(InstrumentResult::Unit)
                }
                InstrumentOperation::IndicatorOff => {
                    run_command(connection, StatusLedOff, operation.deadline()).await?;
                    *status_cache = None;
                    Ok(InstrumentResult::Unit)
                }
                InstrumentOperation::Drawer {
                    open,
                    lower_cover,
                    verify,
                } => {
                    if *open {
                        run_command(connection, DrawerOpen, operation.deadline()).await?;
                    } else {
                        run_command(connection, DrawerClose, operation.deadline()).await?;
                        if *lower_cover {
                            run_command(connection, CoverDown, operation.deadline()).await?;
                        }
                    }
                    if *verify {
                        let drawer =
                            run_command(connection, DrawerStatusQuery, QUERY_DEADLINE).await?;
                        let expected = if *open {
                            DrawerStatus::Open
                        } else {
                            DrawerStatus::Closed
                        };
                        if drawer != expected {
                            return Err(ExecutionFailure::semantic(ServerError::conflict(
                                format!(
                            "drawer verification failed: expected {expected:?}, got {drawer:?}"
                        ),
                            )));
                        }
                        if !*open && *lower_cover {
                            let cover =
                                run_command(connection, CoverPositionQuery, QUERY_DEADLINE).await?;
                            if cover != CoverPosition::Down {
                                return Err(ExecutionFailure::semantic(ServerError::conflict(
                                    format!(
                                        "cover verification failed: expected down, got {cover:?}"
                                    ),
                                )));
                            }
                        }
                    }
                    *status_cache = None;
                    Ok(InstrumentResult::Unit)
                }
                InstrumentOperation::CoverDown {
                    verify,
                    ensure_drawer,
                } => {
                    if *ensure_drawer {
                        let drawer =
                            run_command(connection, DrawerStatusQuery, QUERY_DEADLINE).await?;
                        if matches!(drawer, DrawerStatus::Open | DrawerStatus::Unknown) {
                            run_command(connection, DrawerClose, operation.deadline()).await?;
                        }
                    }
                    run_command(connection, CoverDown, operation.deadline()).await?;
                    if *verify {
                        if *ensure_drawer {
                            let drawer =
                                run_command(connection, DrawerStatusQuery, QUERY_DEADLINE).await?;
                            if drawer != DrawerStatus::Closed {
                                return Err(ExecutionFailure::semantic(ServerError::conflict(
                                    format!(
                                        "drawer verification failed: expected closed, got {drawer:?}"
                                    ),
                                )));
                            }
                        }
                        let cover =
                            run_command(connection, CoverPositionQuery, QUERY_DEADLINE).await?;
                        if cover != CoverPosition::Down {
                            return Err(ExecutionFailure::semantic(ServerError::conflict(
                                format!("cover verification failed: expected down, got {cover:?}"),
                            )));
                        }
                    }
                    *status_cache = None;
                    Ok(InstrumentResult::Unit)
                }
                InstrumentOperation::Pause { name } => {
                    verify_requested_run(connection, name.as_deref()).await?;
                    run_ack_command(connection, PauseRun, operation.deadline()).await?;
                    verify_run_state(connection, name.as_deref(), |state| state.contains("PAUS"))
                        .await?;
                    Ok(InstrumentResult::Unit)
                }
                InstrumentOperation::Resume { name } => {
                    verify_requested_run(connection, name.as_deref()).await?;
                    run_ack_command(connection, ResumeRun, operation.deadline()).await?;
                    verify_run_state(connection, name.as_deref(), |state| !state.contains("PAUS"))
                        .await?;
                    Ok(InstrumentResult::Unit)
                }
                InstrumentOperation::Stop { name } => {
                    let actual = verify_requested_run(connection, name.as_deref()).await?;
                    run_ack_command(connection, StopRun(actual), operation.deadline()).await?;
                    verify_run_state(connection, name.as_deref(), |state| {
                        state.contains("STOP") || state == "IDLE"
                    })
                    .await?;
                    Ok(InstrumentResult::Unit)
                }
                InstrumentOperation::Abort { name } => {
                    let actual = verify_requested_run(connection, name.as_deref()).await?;
                    run_ack_command(connection, AbortRun(actual), operation.deadline()).await?;
                    verify_run_state(connection, name.as_deref(), |state| {
                        state == "IDLE" || state.contains("TERM")
                    })
                    .await?;
                    Ok(InstrumentResult::Unit)
                }
                InstrumentOperation::StartRun(input) => {
                    start_run(connection, input, operation.deadline()).await?;
                    *status_cache = None;
                    Ok(InstrumentResult::Unit)
                }
                InstrumentOperation::PreflightRun(input) => {
                    preflight_run(connection, input).await?;
                    Ok(InstrumentResult::Unit)
                }
                InstrumentOperation::Compile {
                    name,
                    experiments_root,
                    completed_root,
                } => {
                    let working = experiments_root.join(name);
                    if !working.is_dir() {
                        return Err(ExecutionFailure::semantic(
                            ServerError::coded(
                                StatusCode::NOT_FOUND,
                                "run_not_found",
                                "working run not found",
                            )
                            .details(json!({"name": name})),
                        ));
                    }
                    let attributes = crate::file::read_attributes(&working)
                        .map_err(ExecutionFailure::semantic)?;
                    if !attributes.contains_key("run") {
                        return Err(ExecutionFailure::semantic(
                            ServerError::coded(
                                StatusCode::NOT_FOUND,
                                "run_not_found",
                                "working directory is not marked as a run",
                            )
                            .details(json!({"name": name, "attributes": attributes})),
                        ));
                    }
                    let finished = attributes
                        .get("state")
                        .and_then(Value::as_str)
                        .is_some_and(|state| {
                            state.eq_ignore_ascii_case("Completed")
                                || state.eq_ignore_ascii_case("Terminated")
                        });
                    if !finished {
                        return Err(ExecutionFailure::semantic(
                            ServerError::coded(
                                StatusCode::CONFLICT,
                                "run_not_finished",
                                "run state is not Completed or Terminated",
                            )
                            .details(json!({"name": name, "attributes": attributes})),
                        ));
                    }
                    let collected = attributes.get("collected").is_some_and(|value| match value {
                        Value::Bool(value) => *value,
                        Value::String(value) => value.eq_ignore_ascii_case("true"),
                        _ => false,
                    });
                    if collected {
                        return Err(ExecutionFailure::semantic(
                            ServerError::coded(
                                StatusCode::CONFLICT,
                                "already_collected",
                                "run has already been collected",
                            )
                            .details(json!({"name": name, "attributes": attributes})),
                        ));
                    }
                    let generated = experiments_root.join(format!("{name}.eds"));
                    let completed = completed_root.join(format!("{name}.eds"));
                    if completed.exists() {
                        return Err(ExecutionFailure::semantic(
                            ServerError::coded(
                                StatusCode::CONFLICT,
                                "completed_exists",
                                "completed EDS already exists",
                            )
                            .details(json!({"name": name})),
                        ));
                    }

                    // EXP:RUN returns NEXT when the compilation is accepted. Release
                    // Controller while InstrumentServer performs the long-running ZIP
                    // and poll the file system without semantic-operation interleaving.
                    run_ack_command(
                        connection,
                        ExperimentCompile { name: name.clone() },
                        CONTROL_DEADLINE,
                    )
                    .await?;
                    set_and_verify_access(connection, AccessLevel::Observer, false).await?;
                    health.current_access = Some(AccessDto {
                        level: "observer".to_string(),
                        exclusive: false,
                        stealth: false,
                    });
                    let _ = health_sender.send(health.clone());

                    let compile_deadline = Instant::now() + operation.deadline();
                    while !generated.is_file() {
                        if Instant::now() >= compile_deadline {
                            return Err(ExecutionFailure::semantic(
                                ServerError::timeout("EDS compilation deadline exceeded")
                                    .outcome("unknown"),
                            ));
                        }
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }

                    set_and_verify_access(connection, AccessLevel::Controller, false).await?;
                    health.current_access = Some(AccessDto {
                        level: "controller".to_string(),
                        exclusive: false,
                        stealth: false,
                    });
                    let _ = health_sender.send(health.clone());
                    run_command(
                        connection,
                        FileMove {
                            source: format!("experiments:{name}.eds"),
                            destination: format!("public_run_complete:{name}.eds"),
                        },
                        CONTROL_DEADLINE,
                    )
                    .await?;
                    run_command(
                        connection,
                        ExperimentCollected { name: name.clone() },
                        CONTROL_DEADLINE,
                    )
                    .await?;
                    Ok(InstrumentResult::Unit)
                }
                InstrumentOperation::DeleteExperiment {
                    name,
                    experiments_root,
                } => {
                    let status = run_command(connection, RunStatusQuery, QUERY_DEADLINE).await?;
                    if status.name == *name && !status.state.eq_ignore_ascii_case("IDLE") {
                        return Err(ExecutionFailure::semantic(ServerError::conflict(
                            "cannot delete the current run",
                        )));
                    }
                    let working = experiments_root.join(name);
                    let staged = experiments_root.join(".qslib-staging").join(name);
                    if !working.exists() && !staged.exists() {
                        return Err(ExecutionFailure::semantic(ServerError::not_found(
                            "experiment not found",
                        )));
                    }
                    remove_path_checked(&working)?;
                    remove_path_checked(&staged)?;
                    Ok(InstrumentResult::Unit)
                }
                InstrumentOperation::ReplaceProtocol { name, protocol } => {
                    let status = run_command(connection, RunStatusQuery, QUERY_DEADLINE).await?;
                    if status.name != *name || status.state.eq_ignore_ascii_case("IDLE") {
                        let current_name = status.name.clone();
                        return Err(ExecutionFailure::semantic(
                            ServerError::coded(
                                StatusCode::CONFLICT,
                                "not_running",
                                format!("current run is {:?}, not {:?}", current_name, name),
                            )
                            .details(json!({
                                "requested": name,
                                "current": RunStatusDto::from(status),
                            })),
                        ));
                    }
                    run_command(connection, protocol.clone(), operation.deadline()).await?;
                    Ok(InstrumentResult::Unit)
                }
                InstrumentOperation::GenerateAccessKey => {
                    let key = run_command(connection, RandomKeyQuery, operation.deadline()).await?;
                    Ok(InstrumentResult::AccessKey(key.0))
                }
                InstrumentOperation::Restart => {
                    run_ack_command(connection, RestartSystem, operation.deadline()).await?;
                    Ok(InstrumentResult::Unit)
                }
            }
        })
        .await
        {
            Ok(result) => result,
            Err(_) => Err(ExecutionFailure {
                error: ServerError::timeout("semantic operation deadline exceeded"),
                reconnect: true,
            }),
        };

    if required > AccessLevel::Observer {
        if let Err(failure) = set_and_verify_access(connection, AccessLevel::Observer, false).await
        {
            return Err(ExecutionFailure {
                error: ServerError::unavailable(format!(
                    "operation finished but Observer access restoration failed: {}",
                    failure.error.message
                ))
                .outcome("unknown"),
                reconnect: true,
            });
        }
        health.current_access = Some(AccessDto {
            level: "observer".to_string(),
            exclusive: false,
            stealth: false,
        });
        let _ = health_sender.send(health.clone());
    }
    result
}

async fn start_run(
    connection: &QSConnection,
    input: &StartRunInput,
    deadline: Duration,
) -> Result<(), ExecutionFailure> {
    preflight_run(
        connection,
        &PreflightRunInput {
            experiment: input.experiment.clone(),
            overwrite: input.overwrite,
            experiments_root: input.experiments_root.clone(),
            completed_root: input.completed_root.clone(),
        },
    )
    .await?;

    let working = input.experiments_root.join(&input.experiment);
    let completed = input
        .completed_root
        .join(format!("{}.eds", input.experiment));

    let suffix = uuid::Uuid::new_v4();
    let working_backup = input
        .experiments_root
        .join(format!(".{}.{}.qslib-backup", input.experiment, suffix));
    let completed_backup = input
        .completed_root
        .join(format!(".{}.{}.eds.qslib-backup", input.experiment, suffix));
    if working.exists() {
        std::fs::rename(&working, &working_backup).map_err(|error| {
            ExecutionFailure::semantic(ServerError::internal(format!(
                "failed to back up working experiment: {error}"
            )))
        })?;
    }
    if completed.exists() {
        std::fs::rename(&completed, &completed_backup).map_err(|error| {
            restore_path(&working_backup, &working);
            ExecutionFailure::semantic(ServerError::internal(format!(
                "failed to back up completed run: {error}"
            )))
        })?;
    }

    let mut accepted = false;
    let result: Result<(), ExecutionFailure> = async {
        run_command(connection, PowerSet::on(), deadline).await?;
        run_command(connection, DrawerClose, deadline).await?;
        run_command(connection, CoverDown, deadline).await?;
        if input.require_drawer_check {
            let drawer = run_command(connection, DrawerStatusQuery, QUERY_DEADLINE).await?;
            let cover = run_command(connection, CoverPositionQuery, QUERY_DEADLINE).await?;
            if drawer != DrawerStatus::Closed || cover != CoverPosition::Down {
                return Err(ExecutionFailure::semantic(ServerError::conflict(format!(
                    "drawer/cover verification failed ({drawer:?}, {cover:?})"
                ))));
            }
        }
        run_command(
            connection,
            ExperimentNew {
                name: input.experiment.clone(),
                template: "ruo".to_string(),
            },
            deadline,
        )
        .await?;
        merge_staged_experiment(&working, &input.staged_root)
            .map_err(ExecutionFailure::semantic)?;
        let protocol = ProtocolDefinition::new(input.protocol_scpi.clone()).map_err(|error| {
            ExecutionFailure::semantic(ServerError::bad_request(format!(
                "invalid staged QSLib protocol: {error}"
            )))
        })?;
        run_command(connection, protocol, deadline).await?;
        run_ack_command(
            connection,
            RunStart {
                sample_volume: input.sample_volume,
                run_mode: input.run_mode.clone(),
                protocol: input.protocol_name.clone(),
                experiment: input.experiment.clone(),
            },
            deadline,
        )
        .await?;
        accepted = true;
        let verification_deadline = Instant::now() + deadline;
        loop {
            let status = run_command(connection, RunStatusQuery, QUERY_DEADLINE).await?;
            if status.name == input.experiment && !status.state.eq_ignore_ascii_case("IDLE") {
                break;
            }
            if Instant::now() >= verification_deadline {
                return Err(ExecutionFailure::semantic(
                    ServerError::timeout(format!(
                        "run start was accepted but current run did not become {:?}",
                        input.experiment
                    ))
                    .outcome("unknown"),
                ));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Ok(())
    }
    .await;

    match result {
        Ok(()) => {
            remove_path(&working_backup);
            remove_path(&completed_backup);
            Ok(())
        }
        Err(mut failure) if accepted => {
            failure.error.outcome = "unknown";
            Err(failure)
        }
        Err(failure) => {
            remove_path(&working);
            restore_path(&working_backup, &working);
            restore_path(&completed_backup, &completed);
            Err(failure)
        }
    }
}

async fn preflight_run(
    connection: &QSConnection,
    input: &PreflightRunInput,
) -> Result<(), ExecutionFailure> {
    let status = run_command(connection, RunStatusQuery, QUERY_DEADLINE).await?;
    if !status.state.eq_ignore_ascii_case("IDLE") {
        return Err(ExecutionFailure::semantic(
            ServerError::coded(
                StatusCode::CONFLICT,
                "machine_busy",
                format!("instrument is not idle (state {})", status.state),
            )
            .details(json!({"current": RunStatusDto::from(status)})),
        ));
    }

    if input.experiments_root.join(&input.experiment).exists()
        && input.overwrite == OverwriteMode::False
    {
        return Err(ExecutionFailure::semantic(
            ServerError::coded(
                StatusCode::CONFLICT,
                "working_exists",
                "working experiment already exists",
            )
            .details(json!({"name": input.experiment})),
        ));
    }
    if input
        .completed_root
        .join(format!("{}.eds", input.experiment))
        .exists()
        && input.overwrite != OverwriteMode::True
    {
        return Err(ExecutionFailure::semantic(
            ServerError::coded(
                StatusCode::CONFLICT,
                "completed_exists",
                "completed run already exists",
            )
            .details(json!({"name": input.experiment})),
        ));
    }
    Ok(())
}

fn merge_staged_experiment(
    target: &std::path::Path,
    staged: &std::path::Path,
) -> Result<(), ServerError> {
    let parent = target
        .parent()
        .ok_or_else(|| ServerError::internal("working experiment has no parent"))?;
    let merged = parent.join(format!(".qslib-merge-{}", uuid::Uuid::new_v4()));
    copy_tree(target, &merged, false)?;
    if let Err(error) = copy_tree(staged, &merged, true) {
        remove_path(&merged);
        return Err(error);
    }
    for metadata in [".qslib-package.zip", ".qslib-package.etag"] {
        remove_path(&merged.join(metadata));
    }
    let generated = parent.join(format!(".qslib-generated-{}", uuid::Uuid::new_v4()));
    std::fs::rename(target, &generated).map_err(|error| {
        ServerError::internal(format!("failed to preserve generated experiment: {error}"))
    })?;
    if let Err(error) = std::fs::rename(&merged, target) {
        let _ = std::fs::rename(&generated, target);
        remove_path(&merged);
        return Err(ServerError::internal(format!(
            "failed to install staged experiment: {error}"
        )));
    }
    remove_path(&generated);
    Ok(())
}

fn copy_tree(
    source: &std::path::Path,
    destination: &std::path::Path,
    overwrite: bool,
) -> Result<(), ServerError> {
    std::fs::create_dir_all(destination).map_err(|error| {
        ServerError::internal(format!("failed to create merged directory: {error}"))
    })?;
    for entry in std::fs::read_dir(source).map_err(|error| {
        ServerError::internal(format!("failed to read experiment directory: {error}"))
    })? {
        let entry = entry.map_err(|error| {
            ServerError::internal(format!("failed to read experiment entry: {error}"))
        })?;
        let file_type = entry.file_type().map_err(|error| {
            ServerError::internal(format!("failed to inspect experiment entry: {error}"))
        })?;
        let output = destination.join(entry.file_name());
        if file_type.is_dir() {
            copy_tree(&entry.path(), &output, overwrite)?;
        } else if file_type.is_file() {
            if output.exists() && !overwrite {
                continue;
            }
            std::fs::copy(entry.path(), output).map_err(|error| {
                ServerError::internal(format!("failed to merge experiment file: {error}"))
            })?;
        } else {
            return Err(ServerError::bad_request(
                "experiment tree contains a link or special file",
            ));
        }
    }
    Ok(())
}

fn restore_path(backup: &std::path::Path, destination: &std::path::Path) {
    if backup.exists() {
        remove_path(destination);
        let _ = std::fs::rename(backup, destination);
    }
}

fn remove_path(path: &std::path::Path) {
    if path.is_dir() {
        let _ = std::fs::remove_dir_all(path);
    } else {
        let _ = std::fs::remove_file(path);
    }
}

fn remove_path_checked(path: &std::path::Path) -> Result<(), ExecutionFailure> {
    if !path.exists() {
        return Ok(());
    }
    let result = if path.is_dir() {
        std::fs::remove_dir_all(path)
    } else {
        std::fs::remove_file(path)
    };
    result.map_err(|error| {
        ExecutionFailure::semantic(
            ServerError::internal(format!("failed to delete experiment resource: {error}"))
                .outcome("unknown"),
        )
    })
}

fn subscription_streams(
    connection: &QSConnection,
) -> StreamMap<String, BroadcastStream<qslib_core::parser::LogMessage>> {
    let mut streams = StreamMap::new();
    for topic in ["Temperature", "Time", "Run", "LEDStatus"] {
        if !connection.logchannels.contains_key(topic) {
            let (sender, _) = tokio::sync::broadcast::channel(100);
            connection.logchannels.insert(topic.to_string(), sender);
        }
        if let Some(sender) = connection.logchannels.get(topic) {
            streams.insert(topic.to_string(), BroadcastStream::new(sender.subscribe()));
        }
    }
    streams
}

async fn fetch_status(connection: &QSConnection) -> Result<InstrumentStatusDto, ExecutionFailure> {
    let machine = run_command(connection, MachineStatusQuery, QUERY_DEADLINE).await?;
    let power = run_command(connection, PowerQuery, QUERY_DEADLINE).await?;
    let block = run_command(connection, BlockQuery, QUERY_DEADLINE).await?;
    let zones = run_command(connection, ControlZonesQuery, QUERY_DEADLINE).await?;
    let indicator = run_command(connection, StatusLedQuery, QUERY_DEADLINE).await?;
    let run = run_command(connection, RunStatusQuery, QUERY_DEADLINE).await?;
    let remaining_time = fetch_remaining_time(connection).await?;
    Ok(InstrumentStatusDto::from_parts(
        machine,
        power,
        block,
        zones,
        indicator,
        run,
        remaining_time,
    ))
}

/// Remaining-time support is useful but not universal across instrument
/// software versions. A structured SCPI rejection therefore means "estimate
/// unavailable"; transport or malformed-response failures still evict the
/// managed connection as they do for every other status query.
async fn fetch_remaining_time(connection: &QSConnection) -> Result<Option<i64>, ExecutionFailure> {
    match run_command(connection, RemainingTimeQuery, QUERY_DEADLINE).await {
        Ok(remaining) => Ok(remaining.0),
        Err(failure) if !failure.reconnect => {
            debug!(
                error = %failure.error.message,
                "instrument does not provide a remaining-time estimate"
            );
            Ok(None)
        }
        Err(failure) => Err(failure),
    }
}

async fn set_and_verify_access(
    connection: &QSConnection,
    level: AccessLevel,
    exclusive: bool,
) -> Result<(), ExecutionFailure> {
    run_command(
        connection,
        AccessLevelSet::new(level.clone())
            .with_exclusive(exclusive)
            .with_stealth(false),
        CONTROL_DEADLINE,
    )
    .await?;
    let actual: AccessState = run_command(connection, AccessStateQuery, QUERY_DEADLINE).await?;
    if actual.level != level || actual.exclusive != exclusive || actual.stealth {
        return Err(ExecutionFailure::transport(format!(
            "access verification failed: requested ({level:?}, {exclusive}, false), got {actual:?}"
        )));
    }
    Ok(())
}

async fn verify_requested_run(
    connection: &QSConnection,
    requested: Option<&str>,
) -> Result<String, ExecutionFailure> {
    let status = run_command(connection, RunStatusQuery, QUERY_DEADLINE).await?;
    if status.state.eq_ignore_ascii_case("IDLE") || status.name == "-" {
        return Err(ExecutionFailure::semantic(
            ServerError::coded(
                StatusCode::CONFLICT,
                "not_running",
                "there is no current run",
            )
            .details(json!({"current": RunStatusDto::from(status)})),
        ));
    }
    if let Some(requested) = requested {
        if status.name != requested {
            let current_name = status.name.clone();
            return Err(ExecutionFailure::semantic(
                ServerError::coded(
                    StatusCode::CONFLICT,
                    "not_running",
                    format!("current run is {:?}, not {:?}", current_name, requested),
                )
                .details(json!({
                    "requested": requested,
                    "current": RunStatusDto::from(status),
                })),
            ));
        }
    }
    Ok(status.name)
}

async fn verify_run_state(
    connection: &QSConnection,
    requested: Option<&str>,
    predicate: impl Fn(&str) -> bool,
) -> Result<(), ExecutionFailure> {
    let deadline = Instant::now() + CONTROL_DEADLINE;
    loop {
        let status = run_command(connection, RunStatusQuery, QUERY_DEADLINE).await?;
        if requested
            .is_some_and(|name| status.name != name && !status.state.eq_ignore_ascii_case("IDLE"))
        {
            return Err(ExecutionFailure::semantic(ServerError::conflict(
                "current run changed while verifying operation",
            )));
        }
        if predicate(&status.state.to_ascii_uppercase()) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(ExecutionFailure::semantic(
                ServerError::timeout("run did not reach the acknowledged state").outcome("unknown"),
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn run_command<C>(
    connection: &QSConnection,
    command: C,
    deadline: Duration,
) -> Result<C::Response, ExecutionFailure>
where
    C: CommandBuilder<Error = ErrorResponse>,
    C::Response: TryFrom<OkResponse, Error = OkParseError>,
{
    let mut receiver = command.send(connection).await.map_err(|error| {
        ExecutionFailure::transport(format!("failed to submit SCPI command: {error}"))
    })?;
    let response = tokio::time::timeout(deadline, receiver.receive_response())
        .await
        .map_err(|_| ExecutionFailure::transport("SCPI command deadline exceeded"))?;
    match response {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(ExecutionFailure::semantic(
            ServerError::instrument_rejection(error.to_string()),
        )),
        Err(ReceiveOkResponseError::ConnectionClosed) => Err(ExecutionFailure::transport(
            "SCPI connection closed before response",
        )),
        Err(ReceiveOkResponseError::Timeout) => Err(ExecutionFailure::transport(
            "SCPI response deadline exceeded",
        )),
        Err(error) => Err(ExecutionFailure::transport(format!(
            "invalid SCPI response: {error}"
        ))),
    }
}

async fn run_ack_command<C>(
    connection: &QSConnection,
    command: C,
    deadline: Duration,
) -> Result<(), ExecutionFailure>
where
    C: CommandBuilder<Error = ErrorResponse, Response = ()>,
{
    let mut receiver = command.send(connection).await.map_err(|error| {
        ExecutionFailure::transport(format!("failed to submit SCPI command: {error}"))
    })?;
    match tokio::time::timeout(deadline, receiver.receive_next()).await {
        Err(_) | Ok(Err(ReceiveNextResponseError::Timeout)) => Err(ExecutionFailure::transport(
            "SCPI acknowledgement deadline exceeded",
        )),
        Ok(Err(ReceiveNextResponseError::ConnectionClosed)) => Err(ExecutionFailure::transport(
            "SCPI connection closed before acknowledgement",
        )),
        Ok(Ok(Err(error))) => Err(ExecutionFailure::semantic(
            ServerError::instrument_rejection(error.to_string()),
        )),
        Ok(Ok(Ok(()))) | Ok(Err(ReceiveNextResponseError::UnexpectedOk(_))) => Ok(()),
        Ok(Err(error)) => Err(ExecutionFailure::transport(format!(
            "invalid SCPI acknowledgement: {error}"
        ))),
    }
}

async fn shutdown_connection(
    connection: QSConnection,
    health: &mut ServiceHealth,
    health_sender: &watch::Sender<ServiceHealth>,
) {
    if set_and_verify_access(&connection, AccessLevel::Observer, false)
        .await
        .is_err()
    {
        debug!("could not verify Observer during shutdown");
    }
    health.ready = false;
    health.current_access = None;
    let _ = health_sender.send(health.clone());
    connection.close().await;
}

fn jitter(base: Duration, generation: u64) -> Duration {
    // Deterministic bounded jitter avoids a random-number dependency on the
    // old instrument target while preventing synchronized reconnect storms.
    let percent = 85 + ((generation.wrapping_mul(37).wrapping_add(11)) % 31);
    base.mul_f64(percent as f64 / 100.0)
}
