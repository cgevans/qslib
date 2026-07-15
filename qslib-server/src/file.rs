//! Named-context file and directory resources under `/api/v1`.
//!
//! Paths are resolved under the configured root and canonicalized; anything
//! that escapes the root (via `..`, an absolute component, or a symlink) is
//! rejected with 403. Single-range requests are supported for resume and
//! parallel chunked downloads. `PUT` writes atomically (temp file + rename),
//! creating parent directories under the root; it is refused with 403 when the
//! server policy enables file writes.

use std::ffi::OsString;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::{Body, Bytes};
use axum::extract::{Extension, Path as AxumPath, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;
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

    if let Some(expected) = headers
        .get(header::IF_MATCH)
        .and_then(|value| value.to_str().ok())
    {
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
        .map_err(|e| ServerError::internal(format!("failed to inspect uploaded file: {e}")))?;
    let new_etag = etag(metadata.len(), metadata.modified().ok());
    Ok((
        StatusCode::CREATED,
        [(header::ETAG, new_etag)],
        Json(serde_json::json!({ "context": context, "path": rel, "size": size })),
    )
        .into_response())
}

/// One file in a directory response: path relative to the listed directory.
/// (forward-slash separated), and its size in bytes.
#[derive(Serialize)]
pub struct ListEntry {
    pub path: String,
    pub size: u64,
    pub modified_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Serialize)]
pub struct ListResponse {
    pub files: Vec<ListEntry>,
}

/// Recursively enumerate the regular files under a named-context directory.
/// directory, off disk, returning a JSON manifest.
///
/// This mirrors the InstrumentServer `EXP:ZIPREAD?` file set (Python
/// `os.walk(followlinks=False)` + zip of the `filelist`) so a client can pull a
/// run directory as raw files instead of a base64+deflate zip: real
/// subdirectories are descended; regular files and contained symlinks-to-files
/// are listed (including dotfiles); escaping/broken symlinks and symlinks to
/// directories are skipped. Paths are relative to the requested directory.
pub async fn list_dir(
    State(state): State<AppState>,
    AxumPath((context, rel)): AxumPath<(String, String)>,
) -> Result<Response, ServerError> {
    let root = state.context_root(&context)?;
    list_dir_at(root, &rel).await
}

pub async fn list_context_root(
    State(state): State<AppState>,
    AxumPath(context): AxumPath<String>,
) -> Result<Response, ServerError> {
    let root = state.context_root(&context)?;
    list_dir_at(root, "").await
}

async fn list_dir_at(context_root: &Path, rel: &str) -> Result<Response, ServerError> {
    let root = resolve_under_root(context_root, rel).await?;
    let meta = tokio::fs::metadata(&root)
        .await
        .map_err(|_| ServerError::not_found("directory not found"))?;
    if !meta.is_dir() {
        return Err(ServerError::bad_request("not a directory"));
    }

    let mut files: Vec<ListEntry> = Vec::new();
    // DFS with an explicit stack: (absolute dir, path prefix relative to `root`).
    let mut stack: Vec<(PathBuf, String)> = vec![(root.clone(), String::new())];
    while let Some((cur, prefix)) = stack.pop() {
        let mut rd = tokio::fs::read_dir(&cur)
            .await
            .map_err(|e| ServerError::internal(format!("failed to read directory: {e}")))?;
        while let Some(entry) = rd
            .next_entry()
            .await
            .map_err(|e| ServerError::internal(format!("failed to read directory entry: {e}")))?
        {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let rel_path = if prefix.is_empty() {
                name.into_owned()
            } else {
                format!("{prefix}/{name}")
            };
            // `file_type()` does not follow symlinks, so `is_dir()` is true only
            // for real directories — those we descend (matching `followlinks=False`).
            let ft = entry
                .file_type()
                .await
                .map_err(|e| ServerError::internal(format!("failed to stat entry: {e}")))?;
            if ft.is_dir() {
                stack.push((entry.path(), rel_path));
            } else {
                // Regular file or symlink. Follow it: a symlink-to-directory is
                // skipped (as `os.walk` does not descend it and zips no file for
                // it); a symlink-to-file or regular file is listed by target size;
                // a broken symlink is skipped.
                match tokio::fs::metadata(entry.path()).await {
                    Ok(m) if m.is_file() => {
                        if ft.is_symlink()
                            && !tokio::fs::canonicalize(entry.path())
                                .await
                                .is_ok_and(|target| target.starts_with(context_root))
                        {
                            continue;
                        }
                        files.push(ListEntry {
                            path: rel_path,
                            size: m.len(),
                            modified_at: m
                                .modified()
                                .ok()
                                .map(chrono::DateTime::<chrono::Utc>::from),
                        });
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(Json(ListResponse { files }).into_response())
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
