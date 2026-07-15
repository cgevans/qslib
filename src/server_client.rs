//! Rust client for the on-instrument `qslib-server` HTTP service.
//!
//! `qslib-server` (the `qslib-server` crate) runs on the instrument and serves
//! bulk file transfer (`/file`), a recursive directory manifest (`/list`), and
//! `/health` over plain HTTP on the instrument's private link. Pulling a file
//! this way avoids the base64 encoding the InstrumentServer performs for
//! `FILE:READ?`/`EXP:READ?` over SCPI, which is the slow, load-bearing path on
//! the instrument's CPU.
//!
//! This mirrors the Python [`qslib.server.ServerClient`], and is the client
//! consumers on the qslib side (the [`crate::com_ext`] domain methods,
//! qs-monitor) use. The client lives in `qslib` rather than `qslib-core` so the
//! core protocol layer shared with `qslib-server` itself carries no HTTP client.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Deserialize;
use tokio::sync::OnceCell;

/// qslib-server's default HTTP port.
pub const DEFAULT_SERVER_PORT: u16 = 7500;

/// Fast-fail connect timeout. The server path is on the data-collection hot path,
/// so a machine that is reachable for SCPI but has the server port filtered
/// (SYNs dropped) must fail quickly and fall back to SCPI rather than block on
/// the OS default TCP connect timeout.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// Overall per-request timeout. Generous enough not to truncate a normal
/// transfer, while still bounding a stuck read.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

/// Back off after a failed `/health` probe. Without a negative-cache window,
/// qs-monitor retries the three-second connect timeout once per filter file.
const FILE_ROOT_RETRY_DELAY: Duration = Duration::from_secs(30);

/// An error contacting, or returned by, qslib-server.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    /// The server could not be reached (connection refused, reset, timeout).
    #[error("cannot reach qslib-server at {url}: {source}")]
    Unreachable {
        url: String,
        #[source]
        source: reqwest::Error,
    },
    /// The server returned a non-success HTTP status.
    #[error("qslib-server returned HTTP {status}: {message}")]
    Http {
        status: u16,
        message: String,
        detail: Option<String>,
    },
    /// The response body could not be read or parsed as expected.
    #[error("qslib-server response error: {0}")]
    Decode(String),
    /// An absolute path is not under the server's file root, so it cannot be
    /// served over `/file` (the caller should fall back to SCPI).
    #[error("{abspath} is not under qslib-server file root {root:?}")]
    NotUnderRoot {
        abspath: String,
        root: Option<String>,
    },
}

/// The `/health` document.
#[derive(Debug, Clone, Deserialize)]
pub struct Health {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub uptime_s: u64,
    #[serde(default)]
    pub scpi_ok: bool,
    /// The server's canonicalized `--file-root`, if the build reports it.
    #[serde(default)]
    pub file_root: Option<String>,
    #[serde(default)]
    pub exe_sha256: String,
}

/// One entry in a `/list` manifest: path relative to the listed directory
/// (forward-slash separated), and the file's size in bytes.
#[derive(Debug, Clone, Deserialize)]
pub struct ListEntry {
    pub path: String,
    pub size: u64,
}

#[derive(Deserialize)]
struct ListResponse {
    #[serde(default)]
    files: Vec<ListEntry>,
}

/// A client for a running `qslib-server`.
#[derive(Debug, Clone)]
pub struct ServerClient {
    base_url: String,
    token: Option<String>,
    client: reqwest::Client,
    /// Cached `file_root` from `/health`. Cached whenever `/health` answers
    /// (even when the field is absent, i.e. `None`), so an old server without
    /// the field is not re-probed on every fetch. A transport failure is not
    /// cached, so a transient outage does not permanently disable the path.
    file_root: Arc<OnceCell<Option<String>>>,
    /// Earliest time to retry `/health` after a transport/auth/decode failure.
    /// Shared by clones so one failed hot-path request suppresses the rest.
    file_root_retry_at: Arc<Mutex<Option<Instant>>>,
}

impl ServerClient {
    /// Create a client for the qslib-server reachable at `host:port`.
    ///
    /// `token` is sent as a bearer token when present; our fleet runs the server
    /// tokenless behind the VPN, so it is usually `None`.
    pub fn new(host: &str, port: u16, token: Option<String>) -> Self {
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .build()
            .expect("building reqwest client with default settings");
        Self {
            base_url: format!("http://{host}:{port}"),
            token,
            client,
            file_root: Arc::new(OnceCell::new()),
            file_root_retry_at: Arc::new(Mutex::new(None)),
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    fn get(&self, url: reqwest::Url) -> reqwest::RequestBuilder {
        let req = self.client.get(url);
        match &self.token {
            Some(t) => req.bearer_auth(t),
            None => req,
        }
    }

    fn url(&self, prefix: &str, rel: &str) -> Result<reqwest::Url, ServerError> {
        let mut url = reqwest::Url::parse(&self.base_url)
            .map_err(|e| ServerError::Decode(format!("bad base url: {e}")))?;
        {
            let mut seg = url
                .path_segments_mut()
                .map_err(|_| ServerError::Decode("base url cannot be a base".into()))?;
            // Rebuild the path from scratch so a base like `http://host:7500`
            // (whose normalized path is `/`) does not leave an empty leading
            // segment that would produce `/file//<rel>`.
            seg.clear();
            seg.push(prefix);
            for part in rel.split('/') {
                if !part.is_empty() {
                    seg.push(part);
                }
            }
        }
        Ok(url)
    }

    /// Fetch and return `/health`.
    pub async fn health(&self) -> Result<Health, ServerError> {
        let url = reqwest::Url::parse(&format!("{}/health", self.base_url))
            .map_err(|e| ServerError::Decode(format!("bad base url: {e}")))?;
        let resp = self.send(self.get(url)).await?;
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| ServerError::Decode(format!("reading /health body: {e}")))?;
        serde_json::from_slice(&bytes).map_err(|e| {
            ServerError::Decode(format!(
                "qslib-server /health returned a non-JSON body: {e}"
            ))
        })
    }

    /// Return true if qslib-server answers `/health` with the SCPI target up.
    pub async fn available(&self) -> bool {
        matches!(self.health().await, Ok(h) if h.scpi_ok)
    }

    /// The server's `file_root` (cached after the first successful probe), or
    /// `None` if the server is unreachable or predates the field.
    pub async fn file_root(&self) -> Option<String> {
        if let Some(v) = self.file_root.get() {
            return v.clone();
        }
        if self.file_root_probe_suppressed() {
            return None;
        }
        match self.health().await {
            Ok(h) => {
                *self
                    .file_root_retry_at
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
                let _ = self.file_root.set(h.file_root.clone());
                h.file_root
            }
            Err(_) => {
                *self
                    .file_root_retry_at
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                    Some(Instant::now() + FILE_ROOT_RETRY_DELAY);
                None
            }
        }
    }

    fn file_root_probe_suppressed(&self) -> bool {
        self.file_root_retry_at
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_some_and(|retry_at| Instant::now() < retry_at)
    }

    /// Fetch a file addressed relative to the server's `--file-root`.
    pub async fn get_file(&self, rel: &str) -> Result<Vec<u8>, ServerError> {
        let url = self.url("file", rel)?;
        let resp = self.send(self.get(url)).await?;
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| ServerError::Decode(format!("reading /file body: {e}")))?;
        Ok(bytes.to_vec())
    }

    /// Fetch a file by its absolute on-instrument path, translating it to a
    /// path under the server's `file_root`. Returns [`ServerError::NotUnderRoot`]
    /// when the path is outside the served root (so callers fall back to SCPI).
    pub async fn get_abs_file(&self, abspath: &str) -> Result<Vec<u8>, ServerError> {
        let root = self.file_root().await;
        let rel =
            rel_to_root(root.as_deref(), abspath).ok_or_else(|| ServerError::NotUnderRoot {
                abspath: abspath.to_string(),
                root: root.clone(),
            })?;
        self.get_file(&rel).await
    }

    /// Write `body` to the file addressed relative to the server's
    /// `--file-root` (`PUT /file`), replacing it atomically on the instrument.
    pub async fn put_file(&self, rel: &str, body: Vec<u8>) -> Result<(), ServerError> {
        let url = self.url("file", rel)?;
        let mut req = self.client.put(url).body(body);
        if let Some(t) = &self.token {
            req = req.bearer_auth(t);
        }
        self.send(req).await?;
        Ok(())
    }

    /// Write a file by its absolute on-instrument path, translating it to a path
    /// under the server's `file_root`. Returns [`ServerError::NotUnderRoot`] when
    /// the path is outside the served root (so callers fall back to SCPI).
    pub async fn put_abs_file(&self, abspath: &str, body: Vec<u8>) -> Result<(), ServerError> {
        let root = self.file_root().await;
        let rel =
            rel_to_root(root.as_deref(), abspath).ok_or_else(|| ServerError::NotUnderRoot {
                abspath: abspath.to_string(),
                root: root.clone(),
            })?;
        self.put_file(&rel, body).await
    }

    /// Return the recursive file manifest of the directory at `abspath`
    /// (`GET /list`). Entries' `path` fields are relative to `abspath`.
    pub async fn list_dir(&self, abspath: &str) -> Result<Vec<ListEntry>, ServerError> {
        let root = self.file_root().await;
        let rel =
            rel_to_root(root.as_deref(), abspath).ok_or_else(|| ServerError::NotUnderRoot {
                abspath: abspath.to_string(),
                root: root.clone(),
            })?;
        let url = self.url("list", &rel)?;
        let resp = self.send(self.get(url)).await?;
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| ServerError::Decode(format!("reading /list body: {e}")))?;
        let parsed: ListResponse = serde_json::from_slice(&bytes).map_err(|e| {
            ServerError::Decode(format!("qslib-server /list returned a non-JSON body: {e}"))
        })?;
        Ok(parsed.files)
    }

    /// Send a request, mapping transport failures to [`ServerError::Unreachable`]
    /// and non-success statuses to [`ServerError::Http`] (with the server's
    /// JSON `error`/`detail` body when present).
    async fn send(&self, req: reqwest::RequestBuilder) -> Result<reqwest::Response, ServerError> {
        let resp = req.send().await.map_err(|e| ServerError::Unreachable {
            url: self.base_url.clone(),
            source: e,
        })?;
        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }
        let body = resp.bytes().await.unwrap_or_default();
        let (message, detail) =
            parse_error_body(&body, status.canonical_reason().unwrap_or("error"));
        Err(ServerError::Http {
            status: status.as_u16(),
            message,
            detail,
        })
    }
}

/// Make `abspath` relative to `root`, or `None` if it is not under `root`
/// (mirrors the Python `ServerClient._rel_to_root`). `root` is `None` when the
/// server did not report a `file_root`.
fn rel_to_root(root: Option<&str>, abspath: &str) -> Option<String> {
    let root = root?.trim_end_matches('/');
    let ap = posix_normpath(abspath);
    if root.is_empty() {
        // file_root is "/": everything is under it.
        return Some(ap.trim_start_matches('/').to_string());
    }
    if ap == root {
        return Some(String::new());
    }
    let prefix = format!("{root}/");
    ap.strip_prefix(&prefix).map(|s| s.to_string())
}

/// A small `posixpath.normpath` for absolute instrument paths: collapse `.`,
/// resolve `..`, and squeeze repeated slashes. Only the absolute case is used.
fn posix_normpath(path: &str) -> String {
    let absolute = path.starts_with('/');
    let mut out: Vec<&str> = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if matches!(out.last(), Some(&last) if last != "..") {
                    out.pop();
                } else if !absolute {
                    out.push("..");
                }
            }
            other => out.push(other),
        }
    }
    let joined = out.join("/");
    if absolute {
        format!("/{joined}")
    } else if joined.is_empty() {
        ".".to_string()
    } else {
        joined
    }
}

fn parse_error_body(body: &[u8], default: &str) -> (String, Option<String>) {
    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(body) {
        let message = v
            .get("error")
            .and_then(|e| e.as_str())
            .unwrap_or(default)
            .to_string();
        let detail = v
            .get("detail")
            .and_then(|d| d.as_str())
            .map(|s| s.to_string());
        return (message, detail);
    }
    let text = String::from_utf8_lossy(body).trim().to_string();
    (
        if text.is_empty() {
            default.to_string()
        } else {
            text
        },
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rel_under_root() {
        assert_eq!(
            rel_to_root(
                Some("/data/vendor/IS"),
                "/data/vendor/IS/experiments/run/x.xml"
            ),
            Some("experiments/run/x.xml".to_string())
        );
    }

    #[test]
    fn rel_equal_root() {
        assert_eq!(
            rel_to_root(Some("/data/vendor/IS"), "/data/vendor/IS"),
            Some(String::new())
        );
    }

    #[test]
    fn rel_outside_root() {
        assert_eq!(rel_to_root(Some("/data/vendor/IS"), "/sdcard/other"), None);
    }

    #[test]
    fn rel_root_is_slash() {
        assert_eq!(
            rel_to_root(Some("/"), "/data/vendor/IS/x"),
            Some("data/vendor/IS/x".to_string())
        );
    }

    #[test]
    fn rel_no_root_reported() {
        assert_eq!(rel_to_root(None, "/data/vendor/IS/x"), None);
    }

    #[test]
    fn file_url_has_no_double_slash() {
        let c = ServerClient::new("host", 7500, None);
        // Absolute-style rel (leading slash) and plain rel both produce a single
        // slash after the `file` segment.
        assert_eq!(
            c.url("file", "/experiments/run/x.xml").unwrap().as_str(),
            "http://host:7500/file/experiments/run/x.xml"
        );
        assert_eq!(
            c.url("file", "experiments/run/x.xml").unwrap().as_str(),
            "http://host:7500/file/experiments/run/x.xml"
        );
        assert_eq!(
            c.url("list", "experiments/run").unwrap().as_str(),
            "http://host:7500/list/experiments/run"
        );
    }

    #[test]
    fn file_url_percent_encodes_segments() {
        let c = ServerClient::new("host", 7500, None);
        assert_eq!(
            c.url("file", "a dir/b#c.xml").unwrap().as_str(),
            "http://host:7500/file/a%20dir/b%23c.xml"
        );
    }

    #[test]
    fn empty_relative_path_targets_static_root_route() {
        let c = ServerClient::new("host", 7500, None);
        assert_eq!(c.url("list", "").unwrap().as_str(), "http://host:7500/list");
    }

    #[test]
    fn failed_file_root_probe_is_temporarily_suppressed() {
        let c = ServerClient::new("host", 7500, None);
        *c.file_root_retry_at.lock().unwrap() = Some(Instant::now() + Duration::from_secs(1));
        assert!(c.file_root_probe_suppressed());
    }

    #[test]
    fn normpath_collapses() {
        assert_eq!(posix_normpath("/data//vendor/./IS/"), "/data/vendor/IS");
        assert_eq!(posix_normpath("/data/vendor/../IS"), "/data/IS");
    }
}
