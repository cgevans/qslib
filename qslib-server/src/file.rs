//! Named-context file and directory resources under `/api/v1`.
//!
//! Paths are resolved under the configured root and canonicalized; anything
//! that escapes the root (via `..`, an absolute component, or a symlink) is
//! rejected with 403. Single-range requests are supported for resume and
//! parallel chunked downloads. `PUT` writes atomically (temp file + rename),
//! creating parent directories under the root; it is refused with 403 when the
//! server policy enables file writes.

use std::collections::{BTreeMap, HashSet};
use std::ffi::OsString;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::{Body, Bytes};
use axum::extract::{Extension, Path as AxumPath, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use glob::{MatchOptions, Pattern};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::fs::File;
use tokio_util::io::ReaderStream;

use crate::auth::{require_role, Principal, Role};
use crate::error::ServerError;
use crate::state::AppState;

/// Streaming read buffer. Large enough to keep the socket saturated on the
/// instrument's gigabit link.
const READ_BUFFER: usize = 256 * 1024;

/// Filter `rel` to a root-relative path, rejecting any non-`Normal` component:
/// `..`, an absolute root, or a Windows prefix. `CurDir` (`.`) is harmless and
/// dropped. Shared by [`resolve_under_root`] (read) and [`resolve_write_target`]
/// (write) so both reject the same set of escaping paths.
fn safe_relative(rel: &str) -> Result<PathBuf, ServerError> {
    let mut safe = PathBuf::new();
    for comp in Path::new(rel).components() {
        match comp {
            Component::Normal(c) => safe.push(c),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(ServerError::forbidden(
                    "path escapes the file root (`..`, absolute, or prefix component)",
                ));
            }
        }
    }
    Ok(safe)
}

/// Resolve `rel` (the wildcard path component) to a real file under `root`,
/// rejecting any traversal or symlink escape.
///
/// Returns the canonical path on success.
pub async fn resolve_under_root(root: &Path, rel: &str) -> Result<PathBuf, ServerError> {
    let safe = safe_relative(rel)?;

    let joined = root.join(&safe);

    // Canonicalize resolves symlinks and `.`/`..`; a missing file errors (404).
    let canonical = tokio::fs::canonicalize(&joined)
        .await
        .map_err(|_| ServerError::not_found("file not found"))?;

    // Even after component filtering, a symlink inside the tree could point
    // outside; verify the real path is still under the (canonical) root.
    if !canonical.starts_with(root) {
        return Err(ServerError::forbidden(
            "path escapes the file root via symlink",
        ));
    }
    Ok(canonical)
}

/// Resolve `rel` to a write target `(parent_dir, file_name)` under `root`,
/// creating the parent directories. Rejects traversal (`..`, absolute, prefix)
/// components up front, and rejects a parent that resolves (via a symlink)
/// outside `root`.
///
/// Unlike [`resolve_under_root`] the target file need not exist. The final path
/// component is returned separately as the name to write; the atomic rename in
/// [`put_file`] replaces any existing file or symlink at that name, so a symlink
/// occupying the name cannot redirect the write outside the (canonical) parent.
async fn resolve_write_target(root: &Path, rel: &str) -> Result<(PathBuf, OsString), ServerError> {
    let safe = safe_relative(rel)?;

    let file_name = safe
        .file_name()
        .map(|s| s.to_os_string())
        .ok_or_else(|| ServerError::bad_request("no file name in path"))?;
    let parent_rel = safe.parent().map(Path::to_path_buf).unwrap_or_default();

    // Walk one component at a time from the already-canonical root. Calling
    // create_dir_all(root.join(parent_rel)) first would follow a pre-existing
    // symlink and create directories outside the root before the later
    // canonicalization noticed the escape.
    let mut parent = root.to_path_buf();
    for comp in parent_rel.components() {
        let Component::Normal(name) = comp else {
            unreachable!("safe_relative returned a non-normal component")
        };
        let next = parent.join(name);
        let mut meta = tokio::fs::symlink_metadata(&next).await;
        if matches!(&meta, Err(e) if e.kind() == std::io::ErrorKind::NotFound) {
            match tokio::fs::create_dir(&next).await {
                Ok(()) => {}
                // Another concurrent upload may have created the same parent.
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(e) => {
                    return Err(ServerError::internal(format!(
                        "failed to create parent directory: {e}"
                    )))
                }
            }
            meta = tokio::fs::symlink_metadata(&next).await;
        }

        let meta = meta.map_err(|e| {
            ServerError::internal(format!("failed to inspect parent directory: {e}"))
        })?;
        if meta.file_type().is_symlink() {
            let canonical = tokio::fs::canonicalize(&next).await.map_err(|e| {
                ServerError::internal(format!("failed to resolve parent symlink: {e}"))
            })?;
            if !canonical.starts_with(root) {
                return Err(ServerError::forbidden(
                    "path escapes the file root via symlink",
                ));
            }
            let target_meta = tokio::fs::metadata(&canonical).await.map_err(|e| {
                ServerError::internal(format!("failed to inspect parent symlink target: {e}"))
            })?;
            if !target_meta.is_dir() {
                return Err(ServerError::bad_request(
                    "a parent path component is not a directory",
                ));
            }
            parent = canonical;
        } else if meta.is_dir() {
            parent = next;
        } else {
            return Err(ServerError::bad_request(
                "a parent path component is not a directory",
            ));
        }
    }
    Ok((parent, file_name))
}

/// A parsed, validated single byte range.
#[derive(Debug, PartialEq, Eq)]
pub enum RangeResult {
    /// No `Range` header present: serve the whole file.
    Full,
    /// A satisfiable single range `[start, end]` inclusive.
    Satisfiable { start: u64, end: u64 },
    /// The range is syntactically valid but not satisfiable for this size.
    Unsatisfiable,
    /// The `Range` header is malformed or multi-range: ignore it, serve whole.
    Ignore,
}

/// Parse an HTTP `Range` header value against a known content `size`.
///
/// Only a single byte range is supported (`bytes=start-end`, `bytes=start-`,
/// `bytes=-suffix`). Multi-range or malformed headers fall back to serving the
/// whole file.
pub fn parse_range(value: &str, size: u64) -> RangeResult {
    let Some(spec) = value.trim().strip_prefix("bytes=") else {
        return RangeResult::Ignore;
    };
    let spec = spec.trim();
    // Multi-range not supported: serve the whole file rather than mishandle it.
    if spec.contains(',') {
        return RangeResult::Ignore;
    }
    let Some((start_s, end_s)) = spec.split_once('-') else {
        return RangeResult::Ignore;
    };
    let start_s = start_s.trim();
    let end_s = end_s.trim();

    if start_s.is_empty() {
        // Suffix range: last `n` bytes.
        let n: u64 = match end_s.parse() {
            Ok(n) => n,
            Err(_) => return RangeResult::Ignore,
        };
        if n == 0 {
            return RangeResult::Unsatisfiable;
        }
        if size == 0 {
            return RangeResult::Unsatisfiable;
        }
        let start = size.saturating_sub(n);
        return RangeResult::Satisfiable {
            start,
            end: size - 1,
        };
    }

    let start: u64 = match start_s.parse() {
        Ok(s) => s,
        Err(_) => return RangeResult::Ignore,
    };
    if start >= size {
        return RangeResult::Unsatisfiable;
    }
    let end: u64 = if end_s.is_empty() {
        size - 1
    } else {
        match end_s.parse::<u64>() {
            Ok(e) => e.min(size - 1),
            Err(_) => return RangeResult::Ignore,
        }
    };
    if end < start {
        return RangeResult::Ignore;
    }
    RangeResult::Satisfiable { start, end }
}

/// Compute the strong ETag `"<size>-<mtime_ns>"`.
fn etag(size: u64, modified: Option<SystemTime>) -> String {
    let mtime_ns = modified
        .and_then(|m| m.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("\"{size}-{mtime_ns}\"")
}

pub async fn serve_file(
    State(state): State<AppState>,
    method: Method,
    AxumPath((context, rel)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, ServerError> {
    let root = state.context_root(&context)?;
    let path = resolve_under_root(root, &rel).await?;

    let meta = tokio::fs::metadata(&path)
        .await
        .map_err(|_| ServerError::not_found("file not found"))?;
    // Only serve regular files. Directories, FIFOs, sockets, and device nodes
    // are rejected: opening a FIFO read-only would block a blocking-pool thread
    // indefinitely, and device nodes have no meaningful length.
    if !meta.is_file() {
        return Err(ServerError::not_found("not a regular file"));
    }
    // Note on TOCTOU: `resolve_under_root` canonicalizes and confirms the path
    // is under the root, but `metadata`/`open` below re-access it by path and
    // follow symlinks, so a local writer racing a symlink swap on a path
    // component could still escape the root. A race-free fix needs
    // openat2(RESOLVE_BENEATH), which the instrument kernel (3.0.35) lacks. The
    // deployment threat model (isolated link-local cable, no untrusted local
    // users; the write surface, when enabled, is reachable only by the same
    // trusted clients) makes this acceptable; revisit if the qslib-server is
    // ever exposed to a host with untrusted local accounts.
    let size = meta.len();
    let modified = meta.modified().ok();

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    resp_headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    if let Ok(v) = HeaderValue::from_str(&etag(size, modified)) {
        resp_headers.insert(header::ETAG, v);
    }
    if let Some(m) = modified {
        if let Ok(v) = HeaderValue::from_str(&httpdate::fmt_http_date(m)) {
            resp_headers.insert(header::LAST_MODIFIED, v);
        }
    }

    let range = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map(|v| parse_range(v, size))
        .unwrap_or(RangeResult::Full);

    let is_head = method == Method::HEAD;

    match range {
        RangeResult::Unsatisfiable => {
            resp_headers.insert(
                header::CONTENT_RANGE,
                HeaderValue::from_str(&format!("bytes */{size}")).unwrap(),
            );
            Ok((StatusCode::RANGE_NOT_SATISFIABLE, resp_headers).into_response())
        }
        RangeResult::Full | RangeResult::Ignore => {
            resp_headers.insert(header::CONTENT_LENGTH, HeaderValue::from(size));
            if is_head {
                return Ok((StatusCode::OK, resp_headers).into_response());
            }
            let file = open(&path).await?;
            let body = Body::from_stream(ReaderStream::with_capacity(file, READ_BUFFER));
            Ok((StatusCode::OK, resp_headers, body).into_response())
        }
        RangeResult::Satisfiable { start, end } => {
            let len = end - start + 1;
            resp_headers.insert(header::CONTENT_LENGTH, HeaderValue::from(len));
            resp_headers.insert(
                header::CONTENT_RANGE,
                HeaderValue::from_str(&format!("bytes {start}-{end}/{size}")).unwrap(),
            );
            if is_head {
                return Ok((StatusCode::PARTIAL_CONTENT, resp_headers).into_response());
            }
            let mut file = open(&path).await?;
            use tokio::io::{AsyncReadExt, AsyncSeekExt};
            file.seek(std::io::SeekFrom::Start(start))
                .await
                .map_err(|e| ServerError::internal(format!("seek failed: {e}")))?;
            let limited = file.take(len);
            let body = Body::from_stream(ReaderStream::with_capacity(limited, READ_BUFFER));
            Ok((StatusCode::PARTIAL_CONTENT, resp_headers, body).into_response())
        }
    }
}

async fn open(path: &Path) -> Result<File, ServerError> {
    File::open(path)
        .await
        .map_err(|e| ServerError::internal(format!("failed to open file: {e}")))
}

/// Write the request body atomically beneath a named context root.
///
/// The body replaces the target file atomically: it is written to a temp file
/// in the same directory and then renamed into place, so a reader never sees a
/// partial file and a failed transfer leaves the previous contents intact.
/// Parent directories are created as needed. Refused with 403 when the server
/// does not enable file writes.
pub async fn put_file(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    AxumPath((context, rel)): AxumPath<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ServerError> {
    require_role(Extension(principal), Role::Controller)?;
    if !state.allow_file_writes {
        return Err(ServerError::forbidden(
            "file writes are disabled by server policy",
        ));
    }

    let root = state.context_root(&context)?.to_path_buf();
    tokio::fs::create_dir_all(&root)
        .await
        .map_err(|e| ServerError::internal(format!("failed to create context root: {e}")))?;
    let (parent, file_name) = resolve_write_target(&root, &rel).await?;
    let target = parent.join(&file_name);
    let existed = match tokio::fs::symlink_metadata(&target).await {
        Ok(_) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => {
            return Err(ServerError::internal(format!(
                "failed to inspect upload target: {error}"
            )))
        }
    };

    let mut if_match_values = headers.get_all(header::IF_MATCH).iter();
    if let Some(expected) = if_match_values.next() {
        if if_match_values.next().is_some() {
            return Err(ServerError::bad_request(
                "multiple If-Match headers are not supported",
            ));
        }
        let expected = expected
            .to_str()
            .map_err(|_| ServerError::bad_request("If-Match must be a valid HTTP ETag"))?;
        let current = match tokio::fs::metadata(&target).await {
            Ok(metadata) if metadata.is_file() => etag(metadata.len(), metadata.modified().ok()),
            _ => return Err(ServerError::conflict("If-Match target does not exist")),
        };
        if expected != "*" && expected != current {
            return Err(ServerError::conflict(format!(
                "ETag mismatch: current value is {current}"
            )));
        }
    }
    // Uniquify the temp name per upload: the pid alone collides when two
    // concurrent PUTs to the same target run in one server process, so they
    // would share a temp file and clobber each other. A process-wide counter
    // gives each in-flight write its own temp file.
    static UPLOAD_SEQ: AtomicU64 = AtomicU64::new(0);
    let tmp = parent.join(format!(
        ".{}.upload.{}.{}",
        file_name.to_string_lossy(),
        std::process::id(),
        UPLOAD_SEQ.fetch_add(1, Ordering::Relaxed)
    ));

    let size = body.len();
    if let Err(e) = tokio::fs::write(&tmp, &body).await {
        let _ = tokio::fs::remove_file(&tmp).await;
        return Err(ServerError::internal(format!(
            "failed to write uploaded file: {e}"
        )));
    }
    if let Err(e) = tokio::fs::rename(&tmp, &target).await {
        let _ = tokio::fs::remove_file(&tmp).await;
        return Err(ServerError::internal(format!(
            "failed to install uploaded file: {e}"
        )));
    }

    let metadata = tokio::fs::metadata(&target)
        .await
        .map_err(post_install_metadata_error)?;
    let new_etag = etag(metadata.len(), metadata.modified().ok());
    Ok((
        if existed {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        },
        [(header::ETAG, new_etag)],
        Json(serde_json::json!({ "context": context, "path": rel, "size": size })),
    )
        .into_response())
}

fn post_install_metadata_error(error: std::io::Error) -> ServerError {
    // The atomic rename already committed the new contents. A response-side
    // metadata failure must not invite a retry under the false claim that the
    // write was never started.
    ServerError::internal(format!("failed to inspect uploaded file: {error}")).outcome("unknown")
}

/// One InstrumentServer-compatible directory entry. Paths are relative to the
/// requested directory; callers prepend their SCPI context/path spelling.
#[derive(Debug, Clone, Serialize)]
pub struct ListEntry {
    pub path: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub size: u64,
    pub mtime: f64,
    pub atime: f64,
    pub ctime: f64,
    pub attributes: BTreeMap<String, Value>,
}

#[derive(Serialize)]
pub struct ListResponse {
    pub entries: Vec<ListEntry>,
}

#[derive(Debug, Default, Deserialize)]
pub struct ListQuery {
    pattern: Option<String>,
    #[serde(default)]
    recursive: bool,
}

pub async fn list_dir(
    State(state): State<AppState>,
    AxumPath((context, rel)): AxumPath<(String, String)>,
    Query(query): Query<ListQuery>,
) -> Result<Response, ServerError> {
    list_dir_at(&state, &context, &rel, query).await
}

pub async fn list_context_root(
    State(state): State<AppState>,
    AxumPath(context): AxumPath<String>,
    Query(query): Query<ListQuery>,
) -> Result<Response, ServerError> {
    list_dir_at(&state, &context, "", query).await
}

async fn list_dir_at(
    state: &AppState,
    context: &str,
    rel: &str,
    query: ListQuery,
) -> Result<Response, ServerError> {
    let context_root = state.context_root(context)?;
    let root = resolve_under_root(context_root, rel).await?;
    if !root.is_dir() {
        return Err(ServerError::bad_request("not a directory"));
    }

    let pattern_text = query.pattern.as_deref().unwrap_or("*");
    validate_pattern(pattern_text)?;
    let pattern = Pattern::new(pattern_text)
        .map_err(|error| ServerError::bad_request(format!("invalid glob pattern: {error}")))?;
    let walk_descendants = query.recursive || pattern_text.contains('/');
    let mut entries = BTreeMap::new();
    let mut visiting = HashSet::new();
    collect_entries(
        state,
        context,
        context_root,
        &root,
        "",
        query.recursive,
        walk_descendants,
        &pattern,
        &mut visiting,
        &mut entries,
    )?;
    Ok(Json(ListResponse {
        entries: entries.into_values().collect(),
    })
    .into_response())
}

#[allow(clippy::too_many_arguments)]
fn collect_entries(
    state: &AppState,
    context: &str,
    context_root: &Path,
    directory: &Path,
    logical_prefix: &str,
    recursive: bool,
    walk_descendants: bool,
    pattern: &Pattern,
    visiting: &mut HashSet<String>,
    output: &mut BTreeMap<String, ListEntry>,
) -> Result<(), ServerError> {
    let canonical = std::fs::canonicalize(directory)
        .map_err(|_| ServerError::not_found("directory not found"))?;
    if !canonical.starts_with(context_root) {
        return Err(ServerError::forbidden(
            "directory escapes the file root via symlink",
        ));
    }
    let visit_key = format!("{}:{}", context.to_ascii_lowercase(), canonical.display());
    if !visiting.insert(visit_key.clone()) {
        return Ok(());
    }

    let result = (|| {
        let mut children = std::fs::read_dir(&canonical)
            .map_err(|error| ServerError::internal(format!("failed to read directory: {error}")))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| {
                ServerError::internal(format!("failed to read directory entry: {error}"))
            })?;
        children.sort_by_key(|entry| entry.file_name());

        for child in children {
            let name = child.file_name().to_string_lossy().into_owned();
            if name.starts_with('.') {
                continue;
            }
            let path = child.path();
            let metadata = std::fs::metadata(&path).map_err(|error| {
                ServerError::internal(format!("failed to inspect directory entry: {error}"))
            })?;
            if child.file_type().is_ok_and(|kind| kind.is_symlink()) {
                let target = std::fs::canonicalize(&path).map_err(|error| {
                    ServerError::internal(format!("failed to resolve directory symlink: {error}"))
                })?;
                if !target.starts_with(context_root) {
                    return Err(ServerError::forbidden(
                        "directory entry escapes the file root via symlink",
                    ));
                }
            }
            if !metadata.is_file() && !metadata.is_dir() {
                continue;
            }
            let attributes = read_attributes(&path)?;
            if attributes
                .get("hidden")
                .is_some_and(|value| value == &Value::Bool(true))
            {
                continue;
            }
            let relative = join_relative(logical_prefix, &name);
            let matches = pattern.matches_with(
                &relative,
                MatchOptions {
                    case_sensitive: true,
                    require_literal_separator: !recursive,
                    require_literal_leading_dot: true,
                },
            );
            if matches && (!recursive || metadata.is_file()) {
                output.entry(relative.clone()).or_insert_with(|| {
                    entry_from_metadata(
                        relative.clone(),
                        if metadata.is_dir() { "folder" } else { "file" },
                        &metadata,
                        attributes,
                    )
                });
            }
            if metadata.is_dir() && walk_descendants {
                collect_entries(
                    state,
                    context,
                    context_root,
                    &path,
                    &relative,
                    recursive,
                    walk_descendants,
                    pattern,
                    visiting,
                    output,
                )?;
            }
        }

        let shadows = canonical.join(".shadows");
        if shadows.is_file() {
            let contents = std::fs::read_to_string(&shadows).map_err(|error| {
                ServerError::internal(format!("failed to read shadow metadata: {error}"))
            })?;
            for target in contents
                .lines()
                .map(str::trim)
                .filter(|line| !line.is_empty())
            {
                let (target_context, target_rel) = target
                    .split_once(':')
                    .map_or(("default", target), |(context, rel)| (context, rel));
                let Some(target_root) = state.contexts.get(&target_context.to_ascii_lowercase())
                else {
                    return Err(ServerError::coded(
                        StatusCode::UNPROCESSABLE_ENTITY,
                        "unsupported_shadow_target",
                        format!("shadow references unsupported context {target_context:?}"),
                    ));
                };
                let safe = safe_relative(target_rel)?;
                let target_path = target_root.join(safe);
                let target_path = std::fs::canonicalize(&target_path).map_err(|_| {
                    ServerError::coded(
                        StatusCode::UNPROCESSABLE_ENTITY,
                        "unsupported_shadow_target",
                        format!("shadow target {target:?} is unavailable"),
                    )
                })?;
                if !target_path.starts_with(target_root) || !target_path.is_dir() {
                    return Err(ServerError::coded(
                        StatusCode::UNPROCESSABLE_ENTITY,
                        "unsupported_shadow_target",
                        format!("shadow target {target:?} is not a supported directory"),
                    ));
                }
                collect_entries(
                    state,
                    target_context,
                    target_root,
                    &target_path,
                    logical_prefix,
                    recursive,
                    walk_descendants,
                    pattern,
                    visiting,
                    output,
                )?;
            }
        }
        Ok(())
    })();
    visiting.remove(&visit_key);
    result
}

fn validate_pattern(pattern: &str) -> Result<(), ServerError> {
    if pattern.starts_with('/')
        || pattern.contains('\\')
        || pattern.contains('\0')
        || pattern.split('/').any(|part| part == "..")
    {
        return Err(ServerError::forbidden(
            "glob pattern escapes the listed directory",
        ));
    }
    Ok(())
}

fn join_relative(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{prefix}/{name}")
    }
}

fn entry_from_metadata(
    path: String,
    kind: &str,
    metadata: &std::fs::Metadata,
    attributes: BTreeMap<String, Value>,
) -> ListEntry {
    let (atime, mtime, ctime) = metadata_times(metadata);
    ListEntry {
        path,
        kind: kind.to_string(),
        size: metadata.len(),
        mtime,
        atime,
        ctime,
        attributes,
    }
}

#[cfg(unix)]
fn metadata_times(metadata: &std::fs::Metadata) -> (f64, f64, f64) {
    use std::os::unix::fs::MetadataExt;
    let time = |seconds: i64, nanos: i64| seconds as f64 + nanos as f64 / 1_000_000_000.0;
    (
        time(metadata.atime(), metadata.atime_nsec()),
        time(metadata.mtime(), metadata.mtime_nsec()),
        time(metadata.ctime(), metadata.ctime_nsec()),
    )
}

#[cfg(not(unix))]
fn metadata_times(metadata: &std::fs::Metadata) -> (f64, f64, f64) {
    let seconds = |value: Result<SystemTime, std::io::Error>| {
        value
            .ok()
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_secs_f64())
            .unwrap_or(0.0)
    };
    (
        seconds(metadata.accessed()),
        seconds(metadata.modified()),
        seconds(metadata.created()),
    )
}

/// Read InstrumentServer's per-directory INI metadata. Directory attributes
/// live in section `[.]`; file attributes live in the parent `.attributes`
/// under a section named for the file.
pub(crate) fn read_attributes(path: &Path) -> Result<BTreeMap<String, Value>, ServerError> {
    let (attribute_path, wanted_section) = if path.is_dir() {
        (path.join(".attributes"), ".".to_string())
    } else {
        let Some(parent) = path.parent() else {
            return Ok(BTreeMap::new());
        };
        let Some(name) = path.file_name() else {
            return Ok(BTreeMap::new());
        };
        (
            parent.join(".attributes"),
            name.to_string_lossy().into_owned(),
        )
    };
    let contents = match std::fs::read_to_string(attribute_path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(error) => {
            return Err(ServerError::internal(format!(
                "failed to read file attributes: {error}"
            )))
        }
    };
    let mut current_section = String::new();
    let mut attributes = BTreeMap::new();
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if let Some(section) = line
            .strip_prefix('[')
            .and_then(|line| line.strip_suffix(']'))
        {
            current_section = section.trim().to_string();
            continue;
        }
        if current_section != wanted_section {
            continue;
        }
        let Some((key, raw_value)) = line.split_once('=').or_else(|| line.split_once(':')) else {
            continue;
        };
        let raw_value = raw_value.trim();
        let value = if raw_value.eq_ignore_ascii_case("true") {
            Value::Bool(true)
        } else if raw_value.eq_ignore_ascii_case("false") {
            Value::Bool(false)
        } else {
            Value::String(raw_value.to_string())
        };
        attributes.insert(key.trim().to_ascii_lowercase(), value);
    }
    Ok(attributes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;

    #[test]
    fn range_full_when_no_bytes_prefix() {
        assert_eq!(parse_range("something", 1000), RangeResult::Ignore);
    }

    #[test]
    fn range_start_end() {
        assert_eq!(
            parse_range("bytes=0-99", 1000),
            RangeResult::Satisfiable { start: 0, end: 99 }
        );
    }

    #[test]
    fn range_open_ended() {
        assert_eq!(
            parse_range("bytes=100-", 1000),
            RangeResult::Satisfiable {
                start: 100,
                end: 999
            }
        );
    }

    #[test]
    fn range_suffix() {
        assert_eq!(
            parse_range("bytes=-100", 1000),
            RangeResult::Satisfiable {
                start: 900,
                end: 999
            }
        );
    }

    #[test]
    fn range_suffix_larger_than_file() {
        assert_eq!(
            parse_range("bytes=-5000", 1000),
            RangeResult::Satisfiable { start: 0, end: 999 }
        );
    }

    #[test]
    fn range_end_clamped_to_size() {
        assert_eq!(
            parse_range("bytes=500-100000", 1000),
            RangeResult::Satisfiable {
                start: 500,
                end: 999
            }
        );
    }

    #[test]
    fn range_start_past_end_unsatisfiable() {
        assert_eq!(parse_range("bytes=1000-", 1000), RangeResult::Unsatisfiable);
        assert_eq!(
            parse_range("bytes=1500-2000", 1000),
            RangeResult::Unsatisfiable
        );
    }

    #[test]
    fn range_zero_suffix_unsatisfiable() {
        assert_eq!(parse_range("bytes=-0", 1000), RangeResult::Unsatisfiable);
    }

    #[test]
    fn range_malformed_ignored() {
        assert_eq!(parse_range("bytes=abc-def", 1000), RangeResult::Ignore);
        assert_eq!(parse_range("bytes=200-100", 1000), RangeResult::Ignore);
        assert_eq!(parse_range("bytes=0-99,200-299", 1000), RangeResult::Ignore);
    }

    #[test]
    fn range_empty_file() {
        assert_eq!(parse_range("bytes=0-", 0), RangeResult::Unsatisfiable);
    }

    #[test]
    fn etag_format() {
        let t = UNIX_EPOCH + std::time::Duration::from_nanos(123);
        assert_eq!(etag(42, Some(t)), "\"42-123\"");
        assert_eq!(etag(42, None), "\"42-0\"");
    }

    #[test]
    fn metadata_failure_after_atomic_install_has_unknown_outcome() {
        let error = post_install_metadata_error(std::io::Error::other("metadata unavailable"));
        assert_eq!(error.outcome, "unknown");
    }

    async fn tmp_root() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let root = tokio::fs::canonicalize(dir.path()).await.unwrap();
        (dir, root)
    }

    #[tokio::test]
    async fn resolve_valid_file() {
        let (_dir, root) = tmp_root().await;
        tokio::fs::write(root.join("data.bin"), b"hello")
            .await
            .unwrap();
        let p = resolve_under_root(&root, "data.bin").await.unwrap();
        assert_eq!(p, root.join("data.bin"));
    }

    #[tokio::test]
    async fn resolve_valid_nested_file() {
        let (_dir, root) = tmp_root().await;
        tokio::fs::create_dir_all(root.join("a/b")).await.unwrap();
        tokio::fs::write(root.join("a/b/c.bin"), b"x")
            .await
            .unwrap();
        let p = resolve_under_root(&root, "a/b/c.bin").await.unwrap();
        assert_eq!(p, root.join("a/b/c.bin"));
    }

    #[tokio::test]
    async fn resolve_rejects_parent_traversal() {
        let (_dir, root) = tmp_root().await;
        let e = resolve_under_root(&root, "../secret").await.unwrap_err();
        assert_eq!(e.status, StatusCode::FORBIDDEN);
        let e = resolve_under_root(&root, "a/../../secret")
            .await
            .unwrap_err();
        assert_eq!(e.status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn resolve_rejects_absolute() {
        let (_dir, root) = tmp_root().await;
        let e = resolve_under_root(&root, "/etc/passwd").await.unwrap_err();
        assert_eq!(e.status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn resolve_missing_is_not_found() {
        let (_dir, root) = tmp_root().await;
        let e = resolve_under_root(&root, "nope.bin").await.unwrap_err();
        assert_eq!(e.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn write_target_creates_parents_and_splits_name() {
        let (_dir, root) = tmp_root().await;
        let (parent, name) = resolve_write_target(&root, "a/b/c.xml").await.unwrap();
        assert_eq!(parent, root.join("a/b"));
        assert_eq!(name, std::ffi::OsStr::new("c.xml"));
        assert!(parent.is_dir());
    }

    #[tokio::test]
    async fn write_target_top_level_name() {
        let (_dir, root) = tmp_root().await;
        let (parent, name) = resolve_write_target(&root, "c.xml").await.unwrap();
        assert_eq!(parent, root);
        assert_eq!(name, std::ffi::OsStr::new("c.xml"));
    }

    #[tokio::test]
    async fn write_target_rejects_traversal_and_absolute() {
        let (_dir, root) = tmp_root().await;
        assert_eq!(
            resolve_write_target(&root, "../x")
                .await
                .unwrap_err()
                .status,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            resolve_write_target(&root, "a/../../x")
                .await
                .unwrap_err()
                .status,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            resolve_write_target(&root, "/etc/passwd")
                .await
                .unwrap_err()
                .status,
            StatusCode::FORBIDDEN
        );
    }

    #[tokio::test]
    async fn write_target_no_filename_is_bad_request() {
        let (_dir, root) = tmp_root().await;
        assert_eq!(
            resolve_write_target(&root, ".").await.unwrap_err().status,
            StatusCode::BAD_REQUEST
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn write_target_rejects_symlink_before_creating_outside_root() {
        let (_dir, root) = tmp_root().await;
        let outside = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), root.join("outside")).unwrap();

        let err = resolve_write_target(&root, "outside/new/dir/file.bin")
            .await
            .unwrap_err();
        assert_eq!(err.status, StatusCode::FORBIDDEN);
        assert!(!outside.path().join("new").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn resolve_rejects_symlink_escape() {
        let (_dir, root) = tmp_root().await;
        let outside = tempfile::tempdir().unwrap();
        tokio::fs::write(outside.path().join("secret"), b"s")
            .await
            .unwrap();
        std::os::unix::fs::symlink(outside.path().join("secret"), root.join("link")).unwrap();
        let e = resolve_under_root(&root, "link").await.unwrap_err();
        assert_eq!(e.status, StatusCode::FORBIDDEN);
    }
}
