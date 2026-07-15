//! Typed client for qslib-server's optional `/api/v1` semantic API.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;

pub const DEFAULT_SERVER_PORT: u16 = 7500;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
const NEGATIVE_CACHE: Duration = Duration::from_secs(30);
type EventByteStream =
    Pin<Box<dyn futures::Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Send>>;

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("cannot reach qslib-server at {url}: {source}")]
    Unreachable {
        url: String,
        submitted: bool,
        #[source]
        source: reqwest::Error,
    },
    #[error("qslib-server returned HTTP {status}: {message}")]
    Http {
        status: u16,
        code: Option<String>,
        message: String,
        retryable: bool,
        outcome: Option<String>,
        request_id: Option<String>,
    },
    #[error("qslib-server response error: {0}")]
    Decode(String),
    #[error("{abspath} does not map to a named qslib-server file context")]
    NotUnderRoot { abspath: String },
    #[error("server mutation outcome is unknown; query {state_query}")]
    OutcomeUnknown { state_query: String },
}

#[derive(Debug, Clone, Deserialize)]
pub struct Health {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub executable_sha256: String,
    #[serde(default)]
    pub uptime_s: u64,
    #[serde(default)]
    pub ready: bool,
    #[serde(default)]
    pub generation: u64,
    #[serde(default)]
    pub reconnect_count: u64,
    #[serde(default)]
    pub queue_depth: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Capabilities {
    pub api_version: String,
    #[serde(default)]
    pub resources: Vec<String>,
    #[serde(default)]
    pub file_contexts: Vec<String>,
    pub max_access: String,
    pub sse: bool,
    pub raw_scpi: bool,
    pub scpi_tunnel: bool,
    pub file_writes: bool,
    pub controls: bool,
}

impl Capabilities {
    pub fn supports(&self, resource: &str) -> bool {
        self.resources.iter().any(|candidate| candidate == resource)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct IndicatorStatus {
    pub color: Option<String>,
    pub mode: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BlockStatus {
    pub enabled: bool,
    pub target_c: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RunStatusDto {
    pub name: String,
    pub stage: i64,
    pub stage_name: String,
    pub num_stages: i64,
    pub cycle: i64,
    pub num_cycles: i64,
    pub step: i64,
    pub point: i64,
    pub state: String,
    #[serde(default)]
    pub remaining_time_s: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct InstrumentStatus {
    pub observed_at: String,
    pub power_enabled: bool,
    pub block: BlockStatus,
    pub zone_count: usize,
    pub drawer: String,
    pub cover: String,
    pub lamp_status: String,
    pub sample_temperatures_c: Vec<f64>,
    pub block_temperatures_c: Vec<f64>,
    pub cover_temperature_c: f64,
    pub target_temperatures_c: HashMap<String, f64>,
    pub target_controlled: HashMap<String, bool>,
    pub led_temperature_c: f64,
    pub indicator: IndicatorStatus,
    pub run: RunStatusDto,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OperationRecord {
    pub id: String,
    pub kind: String,
    pub state: String,
    #[serde(default)]
    pub result: Option<serde_json::Value>,
    #[serde(default)]
    pub error: Option<serde_json::Value>,
    pub outcome: String,
}

#[derive(Debug, Clone)]
pub struct ServerEvent {
    pub id: Option<u64>,
    pub event: String,
    pub data: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ListEntry {
    pub path: String,
    pub size: u64,
    #[serde(default)]
    pub modified_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExperimentsResource {
    #[serde(default)]
    pub experiments: Vec<String>,
    #[serde(default)]
    pub staged: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExperimentResource {
    pub name: String,
    pub working: bool,
    pub package_etag: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StagedPackage {
    pub name: String,
    pub etag: String,
    pub compressed_size: usize,
    pub expanded_size: u64,
    pub entries: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RunsResource {
    pub location: String,
    #[serde(default)]
    pub runs: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RunResource {
    pub name: String,
    pub working: bool,
    pub completed: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct StartRunRequest {
    pub experiment: String,
    pub package_etag: String,
    pub overwrite: String,
    pub require_exclusive: bool,
    pub require_drawer_check: bool,
}

#[derive(Deserialize)]
struct ListResponse {
    #[serde(default)]
    files: Vec<ListEntry>,
}

#[derive(Debug, Clone)]
pub struct ServerClient {
    base_url: String,
    token: Option<String>,
    client: reqwest::Client,
    capabilities: Arc<OnceCell<Capabilities>>,
    retry_at: Arc<Mutex<Option<Instant>>>,
}

impl ServerClient {
    pub fn new(host: &str, port: u16, token: Option<String>) -> Self {
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .build()
            .expect("building qslib-server HTTP client");
        Self {
            base_url: format!("http://{host}:{port}"),
            token,
            client,
            capabilities: Arc::new(OnceCell::new()),
            retry_at: Arc::new(Mutex::new(None)),
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub async fn health(&self) -> Result<Health, ServerError> {
        self.get_json("/health").await
    }

    pub async fn available(&self) -> bool {
        matches!(self.health().await, Ok(health) if health.ready)
    }

    /// Query capabilities once, with a 30-second negative cache. Constructing
    /// a client does not probe the server.
    pub async fn capabilities(&self) -> Result<Capabilities, ServerError> {
        if let Some(capabilities) = self.capabilities.get() {
            return Ok(capabilities.clone());
        }
        if self.probe_suppressed() {
            return Err(ServerError::Decode(
                "qslib-server capability probe is negatively cached".to_string(),
            ));
        }
        match self.get_json::<Capabilities>("/api/v1/capabilities").await {
            Ok(capabilities) if capabilities.api_version == "v1" => {
                *self
                    .retry_at
                    .lock()
                    .unwrap_or_else(|value| value.into_inner()) = None;
                let _ = self.capabilities.set(capabilities.clone());
                Ok(capabilities)
            }
            Ok(capabilities) => {
                *self
                    .retry_at
                    .lock()
                    .unwrap_or_else(|value| value.into_inner()) =
                    Some(Instant::now() + NEGATIVE_CACHE);
                Err(ServerError::Decode(format!(
                    "qslib-server API {} is not compatible with v1",
                    capabilities.api_version
                )))
            }
            Err(error) => {
                *self
                    .retry_at
                    .lock()
                    .unwrap_or_else(|value| value.into_inner()) =
                    Some(Instant::now() + NEGATIVE_CACHE);
                Err(error)
            }
        }
    }

    pub async fn instrument_status(&self) -> Result<InstrumentStatus, ServerError> {
        self.get_json("/api/v1/instrument/status").await
    }

    pub async fn current_run(&self) -> Result<RunStatusDto, ServerError> {
        self.get_json("/api/v1/runs/current").await
    }

    pub async fn set_power(&self, enabled: bool) -> Result<(), ServerError> {
        self.put_json(
            "/api/v1/instrument/power",
            &serde_json::json!({"enabled": enabled}),
            "/api/v1/instrument/status",
        )
        .await
    }

    pub async fn set_block(&self, enabled: bool, target_c: Option<f64>) -> Result<(), ServerError> {
        self.put_json(
            "/api/v1/instrument/block",
            &serde_json::json!({"enabled": enabled, "target_c": target_c}),
            "/api/v1/instrument/status",
        )
        .await
    }

    pub async fn set_indicator(&self, color: &str, mode: &str) -> Result<(), ServerError> {
        self.put_json(
            "/api/v1/instrument/indicator",
            &serde_json::json!({"color": color, "mode": mode}),
            "/api/v1/instrument/status",
        )
        .await
    }

    pub async fn set_drawer(
        &self,
        position: &str,
        lower_cover: bool,
        verify: bool,
    ) -> Result<(), ServerError> {
        self.put_json(
            "/api/v1/instrument/drawer",
            &serde_json::json!({
                "position": position,
                "lower_cover": lower_cover,
                "verify": verify,
            }),
            "/api/v1/instrument/status",
        )
        .await
    }

    pub async fn set_cover(&self, position: &str, verify: bool) -> Result<(), ServerError> {
        self.put_json(
            "/api/v1/instrument/cover",
            &serde_json::json!({"position": position, "verify": verify}),
            "/api/v1/instrument/status",
        )
        .await
    }

    pub async fn list_experiments(&self) -> Result<ExperimentsResource, ServerError> {
        self.get_json("/api/v1/experiments").await
    }

    pub async fn experiment(&self, name: &str) -> Result<ExperimentResource, ServerError> {
        self.get_json(&format!("/api/v1/experiments/{}", encode_segment(name)))
            .await
    }

    pub async fn stage_package(
        &self,
        name: &str,
        package: Vec<u8>,
    ) -> Result<StagedPackage, ServerError> {
        let path = format!("/api/v1/experiments/{}/package", encode_segment(name));
        let request = self
            .authorize(self.client.put(self.url(&path)?))
            .header(reqwest::header::CONTENT_TYPE, "application/zip")
            .body(package);
        self.send_json(request, true, &path).await
    }

    pub async fn package(&self, name: &str) -> Result<Vec<u8>, ServerError> {
        let path = format!("/api/v1/experiments/{}/package", encode_segment(name));
        let response = self
            .send(self.authorize(self.client.get(self.url(&path)?)), false)
            .await?;
        response
            .bytes()
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(|error| ServerError::Decode(format!("reading experiment package: {error}")))
    }

    pub async fn delete_experiment(&self, name: &str) -> Result<(), ServerError> {
        let path = format!("/api/v1/experiments/{}", encode_segment(name));
        let request = self.authorize(self.client.delete(self.url(&path)?));
        self.send_mutation(request, &path).await?;
        Ok(())
    }

    pub async fn list_runs(&self, location: &str) -> Result<RunsResource, ServerError> {
        let mut url = self.url("/api/v1/runs")?;
        url.query_pairs_mut().append_pair("location", location);
        let request = self.authorize(self.client.get(url));
        self.send_json(request, false, "/api/v1/runs").await
    }

    pub async fn run(&self, name: &str) -> Result<RunResource, ServerError> {
        self.get_json(&format!("/api/v1/runs/{}", encode_segment(name)))
            .await
    }

    pub async fn start_run(
        &self,
        input: &StartRunRequest,
        idempotency_key: &str,
    ) -> Result<OperationRecord, ServerError> {
        let request = self
            .authorize(self.client.post(self.url("/api/v1/runs")?))
            .header("Idempotency-Key", idempotency_key)
            .json(input);
        self.send_json(request, true, "/api/v1/runs/current").await
    }

    pub async fn protocol_xml(&self, name: &str) -> Result<Vec<u8>, ServerError> {
        let path = format!("/api/v1/runs/{}/protocol", encode_segment(name));
        let response = self
            .send(self.authorize(self.client.get(self.url(&path)?)), false)
            .await?;
        response
            .bytes()
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(|error| ServerError::Decode(format!("reading protocol XML: {error}")))
    }

    pub async fn replace_protocol(
        &self,
        name: &str,
        xml: Vec<u8>,
        mode: &str,
        force: bool,
    ) -> Result<(), ServerError> {
        let mut url = self.url(&format!("/api/v1/runs/{}/protocol", encode_segment(name)))?;
        url.query_pairs_mut()
            .append_pair("mode", mode)
            .append_pair("force", if force { "true" } else { "false" });
        let path = format!("/api/v1/runs/{}/protocol", encode_segment(name));
        let request = self
            .authorize(self.client.put(url))
            .header(reqwest::header::CONTENT_TYPE, "application/xml")
            .body(xml);
        self.send_mutation(request, &path).await?;
        Ok(())
    }

    pub async fn eds(&self, name: &str) -> Result<Vec<u8>, ServerError> {
        let path = format!("/api/v1/runs/{}/eds", encode_segment(name));
        let response = self
            .send(self.authorize(self.client.get(self.url(&path)?)), false)
            .await?;
        response
            .bytes()
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(|error| ServerError::Decode(format!("reading EDS: {error}")))
    }

    pub async fn generate_access_key(
        &self,
        idempotency_key: &str,
    ) -> Result<OperationRecord, ServerError> {
        let request = self
            .authorize(
                self.client
                    .post(self.url("/api/v1/instrument/access-keys")?),
            )
            .header("Idempotency-Key", idempotency_key);
        self.send_json(request, true, "/api/v1/instrument/status")
            .await
    }

    pub async fn restart_instrument(
        &self,
        idempotency_key: &str,
    ) -> Result<OperationRecord, ServerError> {
        let request = self
            .authorize(
                self.client
                    .post(self.url("/api/v1/instrument/actions/restart")?),
            )
            .header("Idempotency-Key", idempotency_key);
        self.send_json(request, true, "/health").await
    }

    /// Administrator-only isolated raw SCPI. This never uses the managed
    /// semantic connection and exists only when the server advertises it.
    pub async fn raw_scpi(&self, command: &str) -> Result<String, ServerError> {
        let request = self
            .authorize(self.client.post(self.url("/api/v1/scpi")?))
            .json(&serde_json::json!({"command": command, "encoding": "text"}));
        let response = self
            .send_mutation(request, "/api/v1/instrument/status")
            .await?;
        response
            .text()
            .await
            .map_err(|error| ServerError::Decode(format!("reading raw SCPI response: {error}")))
    }

    pub async fn run_action(
        &self,
        name: &str,
        action: &str,
        idempotency_key: &str,
    ) -> Result<OperationRecord, ServerError> {
        let path = format!(
            "/api/v1/runs/{}/actions/{}",
            encode_segment(name),
            encode_segment(action)
        );
        let request = self
            .authorize(self.client.post(self.url(&path)?))
            .header("Idempotency-Key", idempotency_key);
        self.send_json(
            request,
            true,
            &format!("/api/v1/runs/{}", encode_segment(name)),
        )
        .await
    }

    pub async fn operation(&self, id: &str) -> Result<OperationRecord, ServerError> {
        self.get_json(&format!("/api/v1/operations/{}", encode_segment(id)))
            .await
    }

    pub async fn wait_operation(
        &self,
        id: &str,
        timeout: Duration,
    ) -> Result<OperationRecord, ServerError> {
        let deadline = Instant::now() + timeout;
        loop {
            let operation = self.operation(id).await?;
            if matches!(operation.state.as_str(), "succeeded" | "failed" | "unknown") {
                return Ok(operation);
            }
            if Instant::now() >= deadline {
                return Err(ServerError::OutcomeUnknown {
                    state_query: format!("/api/v1/operations/{id}"),
                });
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    pub async fn get_file(&self, context: &str, path: &str) -> Result<Vec<u8>, ServerError> {
        let url = self.resource_url("files", context, path)?;
        let response = self
            .send(self.authorize(self.client.get(url)), false)
            .await?;
        response
            .bytes()
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(|error| ServerError::Decode(format!("reading file response: {error}")))
    }

    pub async fn put_file(
        &self,
        context: &str,
        path: &str,
        body: Vec<u8>,
    ) -> Result<(), ServerError> {
        let url = self.resource_url("files", context, path)?;
        let request = self
            .authorize(self.client.put(url))
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(body);
        self.send_mutation(
            request,
            &format!("/api/v1/files/{context}/{}", encode_path(path)),
        )
        .await?;
        Ok(())
    }

    pub async fn list_context_dir(
        &self,
        context: &str,
        path: &str,
    ) -> Result<Vec<ListEntry>, ServerError> {
        let url = if path.is_empty() {
            self.url(&format!("/api/v1/directories/{}", encode_segment(context)))?
        } else {
            self.resource_url("directories", context, path)?
        };
        let response = self
            .send(self.authorize(self.client.get(url)), false)
            .await?;
        let bytes = response
            .bytes()
            .await
            .map_err(|error| ServerError::Decode(format!("reading directory response: {error}")))?;
        serde_json::from_slice::<ListResponse>(&bytes)
            .map(|response| response.files)
            .map_err(|error| ServerError::Decode(format!("decoding directory response: {error}")))
    }

    /// Compatibility helper for QSLib's direct file API. Resolution is local
    /// and never depends on a tunnel or a successful health probe.
    pub async fn get_abs_file(&self, absolute: &str) -> Result<Vec<u8>, ServerError> {
        let (context, relative) =
            absolute_context(absolute).ok_or_else(|| ServerError::NotUnderRoot {
                abspath: absolute.into(),
            })?;
        self.get_file(context, &relative).await
    }

    pub async fn put_abs_file(&self, absolute: &str, body: Vec<u8>) -> Result<(), ServerError> {
        let (context, relative) =
            absolute_context(absolute).ok_or_else(|| ServerError::NotUnderRoot {
                abspath: absolute.into(),
            })?;
        self.put_file(context, &relative, body).await
    }

    pub async fn list_dir(&self, absolute: &str) -> Result<Vec<ListEntry>, ServerError> {
        let (context, relative) =
            absolute_context(absolute).ok_or_else(|| ServerError::NotUnderRoot {
                abspath: absolute.into(),
            })?;
        self.list_context_dir(context, &relative).await
    }

    pub async fn event_stream(&self, last_event_id: Option<u64>) -> ServerEventStream {
        ServerEventStream {
            client: self.clone(),
            last_event_id,
            stream: None,
            bytes: Vec::new(),
        }
    }

    async fn get_json<T: for<'de> Deserialize<'de>>(&self, path: &str) -> Result<T, ServerError> {
        let request = self.authorize(self.client.get(self.url(path)?));
        self.send_json(request, false, path).await
    }

    async fn put_json<T: Serialize + ?Sized>(
        &self,
        path: &str,
        value: &T,
        state_query: &str,
    ) -> Result<(), ServerError> {
        let request = self.authorize(self.client.put(self.url(path)?)).json(value);
        self.send_mutation(request, state_query).await?;
        Ok(())
    }

    async fn send_json<T: for<'de> Deserialize<'de>>(
        &self,
        request: reqwest::RequestBuilder,
        mutation: bool,
        state_query: &str,
    ) -> Result<T, ServerError> {
        let response = if mutation {
            self.send_mutation(request, state_query).await?
        } else {
            self.send(request, false).await?
        };
        let bytes = response.bytes().await.map_err(|error| {
            if mutation {
                ServerError::OutcomeUnknown {
                    state_query: state_query.to_string(),
                }
            } else {
                ServerError::Decode(format!("reading JSON response: {error}"))
            }
        })?;
        serde_json::from_slice(&bytes).map_err(|error| {
            if mutation {
                ServerError::OutcomeUnknown {
                    state_query: state_query.to_string(),
                }
            } else {
                ServerError::Decode(format!("decoding JSON response: {error}"))
            }
        })
    }

    async fn send_mutation(
        &self,
        request: reqwest::RequestBuilder,
        state_query: &str,
    ) -> Result<reqwest::Response, ServerError> {
        match self.send(request, true).await {
            Err(ServerError::Unreachable {
                submitted: true, ..
            }) => Err(ServerError::OutcomeUnknown {
                state_query: state_query.to_string(),
            }),
            result => result,
        }
    }

    async fn send(
        &self,
        request: reqwest::RequestBuilder,
        mutation: bool,
    ) -> Result<reqwest::Response, ServerError> {
        let response = request
            .send()
            .await
            .map_err(|source| ServerError::Unreachable {
                url: self.base_url.clone(),
                submitted: mutation && !source.is_connect(),
                source,
            })?;
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let request_id = response
            .headers()
            .get("x-request-id")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let body = response.bytes().await.unwrap_or_default();
        let parsed = serde_json::from_slice::<serde_json::Value>(&body).unwrap_or_default();
        let detail = parsed.get("error").cloned().unwrap_or_default();
        Err(ServerError::Http {
            status: status.as_u16(),
            code: detail.get("code").and_then(ValueExt::string),
            message: detail
                .get("message")
                .and_then(|value| value.as_str())
                .unwrap_or_else(|| status.canonical_reason().unwrap_or("server error"))
                .to_string(),
            retryable: detail
                .get("retryable")
                .and_then(|value| value.as_bool())
                .unwrap_or(false),
            outcome: detail.get("outcome").and_then(ValueExt::string),
            request_id: parsed
                .get("request_id")
                .and_then(ValueExt::string)
                .or(request_id),
        })
    }

    fn authorize(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.token {
            Some(token) => request.bearer_auth(token),
            None => request,
        }
    }

    fn url(&self, path: &str) -> Result<reqwest::Url, ServerError> {
        reqwest::Url::parse(&format!("{}{}", self.base_url, path))
            .map_err(|error| ServerError::Decode(format!("invalid server URL: {error}")))
    }

    fn resource_url(
        &self,
        resource: &str,
        context: &str,
        path: &str,
    ) -> Result<reqwest::Url, ServerError> {
        self.url(&format!(
            "/api/v1/{}/{}/{}",
            resource,
            encode_segment(context),
            encode_path(path)
        ))
    }

    fn probe_suppressed(&self) -> bool {
        self.retry_at
            .lock()
            .unwrap_or_else(|value| value.into_inner())
            .is_some_and(|retry_at| Instant::now() < retry_at)
    }
}

pub struct ServerEventStream {
    client: ServerClient,
    last_event_id: Option<u64>,
    stream: Option<EventByteStream>,
    bytes: Vec<u8>,
}

impl ServerEventStream {
    pub async fn next(&mut self) -> Result<ServerEvent, ServerError> {
        loop {
            if let Some(event) = take_sse_event(&mut self.bytes)? {
                self.last_event_id = event.id.or(self.last_event_id);
                return Ok(event);
            }
            if self.stream.is_none() {
                let mut request = self
                    .client
                    .authorize(self.client.client.get(self.client.url("/api/v1/events")?));
                if let Some(id) = self.last_event_id {
                    request = request.header("Last-Event-ID", id.to_string());
                }
                let response = self.client.send(request, false).await?;
                self.stream = Some(Box::pin(response.bytes_stream()));
            }
            let stream = self.stream.as_mut().expect("stream initialized");
            match stream.next().await {
                Some(Ok(chunk)) => {
                    self.bytes.extend_from_slice(&chunk);
                }
                Some(Err(_)) | None => {
                    self.stream = None;
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
        }
    }
}

fn take_sse_event(buffer: &mut Vec<u8>) -> Result<Option<ServerEvent>, ServerError> {
    let Some(end) = buffer.windows(2).position(|window| window == b"\n\n") else {
        return Ok(None);
    };
    let block = String::from_utf8(buffer.drain(..end + 2).collect())
        .map_err(|error| ServerError::Decode(format!("SSE stream is not UTF-8: {error}")))?;
    if block
        .lines()
        .all(|line| line.starts_with(':') || line.is_empty())
    {
        return Ok(None);
    }
    let mut id = None;
    let mut event = "message".to_string();
    let mut data = String::new();
    for line in block.lines() {
        if let Some(value) = line.strip_prefix("id:") {
            id = value.trim().parse().ok();
        } else if let Some(value) = line.strip_prefix("event:") {
            event = value.trim().to_string();
        } else if let Some(value) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(value.trim_start());
        }
    }
    let data = serde_json::from_str(&data)
        .map_err(|error| ServerError::Decode(format!("invalid SSE JSON: {error}")))?;
    Ok(Some(ServerEvent { id, event, data }))
}

fn absolute_context(path: &str) -> Option<(&'static str, String)> {
    let roots = [
        ("/sdcard/private_run_complete", "private_run_complete"),
        ("/sdcard/public_run_complete", "public_run_complete"),
        ("/data/vendor/IS/calibrations", "calibrations"),
        ("/data/vendor/IS/experiments", "experiments"),
        ("/data/vendor/IS/templates", "templates"),
        ("/data/vendor/IS/runs", "runs"),
        ("/data/vendor/IS/logs", "logs"),
        ("/data/vendor/IS", "default"),
    ];
    roots.iter().find_map(|(root, context)| {
        if path == *root {
            Some((*context, String::new()))
        } else {
            path.strip_prefix(&format!("{root}/"))
                .map(|relative| (*context, relative.to_string()))
        }
    })
}

fn encode_segment(value: &str) -> String {
    url_encode(value, false)
}

fn encode_path(value: &str) -> String {
    value
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(|segment| url_encode(segment, false))
        .collect::<Vec<_>>()
        .join("/")
}

fn url_encode(value: &str, preserve_slash: bool) -> String {
    let mut output = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric()
            || b"-._~".contains(&byte)
            || (preserve_slash && byte == b'/')
        {
            output.push(byte as char);
        } else {
            output.push_str(&format!("%{byte:02X}"));
        }
    }
    output
}

trait ValueExt {
    fn string(&self) -> Option<String>;
}

impl ValueExt for serde_json::Value {
    fn string(&self) -> Option<String> {
        self.as_str().map(str::to_string)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absolute_paths_map_to_named_contexts() {
        assert_eq!(
            absolute_context("/data/vendor/IS/experiments/run/a.xml"),
            Some(("experiments", "run/a.xml".to_string()))
        );
        assert_eq!(
            absolute_context("/sdcard/public_run_complete/a.eds"),
            Some(("public_run_complete", "a.eds".to_string()))
        );
        assert!(absolute_context("/etc/passwd").is_none());
    }

    #[test]
    fn parses_sse_event() {
        let mut bytes = b"id: 4\nevent: run\ndata: {\"state\":\"running\"}\n\n".to_vec();
        let event = take_sse_event(&mut bytes).unwrap().unwrap();
        assert_eq!(event.id, Some(4));
        assert_eq!(event.event, "run");
    }

    #[test]
    fn run_status_remaining_time_is_additive() {
        let base = serde_json::json!({
            "name": "test",
            "stage": 1,
            "stage_name": "1",
            "num_stages": 2,
            "cycle": 3,
            "num_cycles": 40,
            "step": 2,
            "point": 0,
            "state": "running"
        });
        let without: RunStatusDto = serde_json::from_value(base.clone()).unwrap();
        assert_eq!(without.remaining_time_s, None);

        let mut with = base;
        with["remaining_time_s"] = serde_json::json!(3723);
        let with: RunStatusDto = serde_json::from_value(with).unwrap();
        assert_eq!(with.remaining_time_s, Some(3723));
    }
}
