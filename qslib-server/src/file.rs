//! `GET`/`HEAD /file/<path…>` — bulk file transfer straight off disk.
//!
//! Paths are resolved under the configured root and canonicalized; anything
//! that escapes the root (via `..`, an absolute component, or a symlink) is
//! rejected with 403. Single-range requests are supported for resume and
//! parallel chunked downloads.

use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::{Path as AxumPath, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use tokio::fs::File;
use tokio_util::io::ReaderStream;

use crate::error::ServerError;
use crate::state::AppState;

/// Streaming read buffer. Large enough to keep the socket saturated on the
/// instrument's gigabit link.
const READ_BUFFER: usize = 256 * 1024;

/// Resolve `rel` (the wildcard path component) to a real file under `root`,
/// rejecting any traversal or symlink escape.
///
/// Returns the canonical path on success.
pub async fn resolve_under_root(root: &Path, rel: &str) -> Result<PathBuf, ServerError> {
    // Reject non-`Normal` components up front: `..`, absolute roots, Windows
    // prefixes. `CurDir` (`.`) is harmless and skipped.
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

    let joined = root.join(&safe);

    // Canonicalize resolves symlinks and `.`/`..`; a missing file errors (404).
    let canonical = tokio::fs::canonicalize(&joined)
        .await
        .map_err(|_| ServerError::not_found("file not found"))?;

    // Even after component filtering, a symlink inside the tree could point
    // outside; verify the real path is still under the (canonical) root.
    if !canonical.starts_with(root) {
        return Err(ServerError::forbidden("path escapes the file root via symlink"));
    }
    Ok(canonical)
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
    AxumPath(rel): AxumPath<String>,
    headers: HeaderMap,
) -> Result<Response, ServerError> {
    let path = resolve_under_root(&state.file_root, &rel).await?;

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
    // users, read-only remote surface) makes this acceptable; revisit if the
    // qslib-server is ever exposed to a host with untrusted local accounts.
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
        assert_eq!(parse_range("bytes=1500-2000", 1000), RangeResult::Unsatisfiable);
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
        tokio::fs::write(root.join("data.bin"), b"hello").await.unwrap();
        let p = resolve_under_root(&root, "data.bin").await.unwrap();
        assert_eq!(p, root.join("data.bin"));
    }

    #[tokio::test]
    async fn resolve_valid_nested_file() {
        let (_dir, root) = tmp_root().await;
        tokio::fs::create_dir_all(root.join("a/b")).await.unwrap();
        tokio::fs::write(root.join("a/b/c.bin"), b"x").await.unwrap();
        let p = resolve_under_root(&root, "a/b/c.bin").await.unwrap();
        assert_eq!(p, root.join("a/b/c.bin"));
    }

    #[tokio::test]
    async fn resolve_rejects_parent_traversal() {
        let (_dir, root) = tmp_root().await;
        let e = resolve_under_root(&root, "../secret").await.unwrap_err();
        assert_eq!(e.status, StatusCode::FORBIDDEN);
        let e = resolve_under_root(&root, "a/../../secret").await.unwrap_err();
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

    #[cfg(unix)]
    #[tokio::test]
    async fn resolve_rejects_symlink_escape() {
        let (_dir, root) = tmp_root().await;
        let outside = tempfile::tempdir().unwrap();
        tokio::fs::write(outside.path().join("secret"), b"s").await.unwrap();
        std::os::unix::fs::symlink(outside.path().join("secret"), root.join("link")).unwrap();
        let e = resolve_under_root(&root, "link").await.unwrap_err();
        assert_eq!(e.status, StatusCode::FORBIDDEN);
    }
}
