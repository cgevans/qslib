//! `POST /api/v1/server/upgrade` — replace the running binary and restart.
//!
//! Robustness is layered so a bad upload can never leave the instrument without
//! a server:
//!   1. The body's SHA-256 must match the client-supplied `x-qslib-sha256`.
//!   2. The bytes must start with the ELF magic.
//!   3. The new binary is written next to the current one, `chmod +x`, and
//!      **run with `--version`** — if it does not execute cleanly on this
//!      instrument (wrong arch, corrupt, missing loader) the upgrade is refused
//!      *before* anything is swapped.
//!   4. The current binary is copied to `<exe>.bak`, then the new one is moved
//!      into place with an atomic `rename` (same directory / filesystem).
//!   5. A detached watchdog (`sh`, its own session) takes over: it stops this
//!      process, launches the new binary with the same argv, and — if the new
//!      process dies within a few seconds — restores `<exe>.bak` and relaunches
//!      it. The watchdog survives this process and the triggering HTTP
//!      connection closing.
//!
//! The client confirms success by polling `/health` until `executable_sha256` equals
//! the hash it uploaded (a persistent old hash means the watchdog rolled back).
//!
//! `?dry_run=1` runs steps 1–3 and reports the result without swapping or
//! restarting — used by tests and for a safe pre-flight check.

use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{Extension, Query, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;

use crate::auth::{require_role, Principal, Role};
use crate::error::ServerError;
use crate::state::{sha256_hex, AppState};

const VERSION_CHECK_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Deserialize)]
pub struct UpgradeParams {
    /// `?dry_run=1` / `true` verifies without installing. Accepted as a string
    /// so `1`, `true`, `yes` all work (a bare `bool` would reject `1`).
    pub dry_run: Option<String>,
}

impl UpgradeParams {
    fn is_dry_run(&self) -> bool {
        matches!(
            self.dry_run
                .as_deref()
                .map(|s| s.trim().to_ascii_lowercase())
                .as_deref(),
            Some("1" | "true" | "yes" | "on")
        )
    }
}

/// Exclusive ownership of the non-dry-run upgrade lifecycle.
///
/// Failed upgrades release the claim on drop. A successful upgrade calls
/// [`UpgradeClaim::keep_until_exit`] so the flag remains set during the
/// watchdog handoff and this soon-to-exit process cannot accept another swap.
#[derive(Debug)]
struct UpgradeClaim<'a> {
    flag: &'a AtomicBool,
    release_on_drop: bool,
}

impl<'a> UpgradeClaim<'a> {
    fn acquire(flag: &'a AtomicBool) -> Result<Self, ServerError> {
        flag.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| ServerError::conflict("an upgrade is already in progress"))?;
        Ok(Self {
            flag,
            release_on_drop: true,
        })
    }

    fn keep_until_exit(&mut self) {
        self.release_on_drop = false;
    }
}

impl Drop for UpgradeClaim<'_> {
    fn drop(&mut self) {
        if self.release_on_drop {
            self.flag.store(false, Ordering::Release);
        }
    }
}

pub async fn upgrade(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(params): Query<UpgradeParams>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ServerError> {
    require_role(Extension(principal), Role::Administrator)?;
    let dry_run = params.is_dry_run();
    let exe = state.exe_path.clone();
    if exe.as_os_str().is_empty() {
        return Err(ServerError::internal(
            "cannot upgrade: the running executable path is unknown",
        ));
    }

    let expected = headers
        .get("x-qslib-sha256")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_ascii_lowercase())
        .ok_or_else(|| ServerError::bad_request("missing x-qslib-sha256 header"))?;
    if expected.len() != 64 || !expected.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(ServerError::bad_request(
            "x-qslib-sha256 must be a 64-char hex SHA-256",
        ));
    }

    if body.is_empty() {
        return Err(ServerError::bad_request("empty upgrade body"));
    }
    let received = sha256_hex(&body);
    if received != expected {
        return Err(ServerError::bad_request(format!(
            "sha256 mismatch: got {received}, expected {expected}"
        )));
    }
    if body.get(0..4) != Some(b"\x7fELF") {
        return Err(ServerError::bad_request("upload is not an ELF binary"));
    }

    // Temp-name uniqueness alone is insufficient: every real upgrade also
    // shares the executable, `.bak`, and detached watchdog. Claim the whole
    // lifecycle, and keep the claim set after success until this process exits.
    let mut upgrade_claim = if dry_run {
        None
    } else {
        Some(UpgradeClaim::acquire(&state.upgrade_in_progress)?)
    };

    // Stage the new binary next to the current one (same filesystem, so the
    // final rename is atomic). The temp name is unique per request: the pid
    // alone collides when two concurrent upgrades run in one process, so they
    // would stage over the same temp file and race the swap.
    static UPGRADE_SEQ: AtomicU64 = AtomicU64::new(0);
    let dir = exe.parent().unwrap_or_else(|| Path::new("."));
    let file_name = exe
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("qslib-server");
    let tmp = dir.join(format!(
        ".{file_name}.upgrade.{}.{}",
        std::process::id(),
        UPGRADE_SEQ.fetch_add(1, Ordering::Relaxed)
    ));

    let result = stage_and_maybe_swap(&exe, &tmp, &body, dry_run).await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&tmp).await;
    }
    result?;

    if dry_run {
        let _ = tokio::fs::remove_file(&tmp).await;
        return Ok(Json(serde_json::json!({
            "status": "verified",
            "dry_run": true,
            "sha256": received,
            "old_sha256": state.exe_sha256,
        }))
        .into_response());
    }

    // At this point the new binary is in place; hand off to the watchdog.
    if let Err(watchdog_error) = spawn_restart_watchdog(&exe, &state.restart_args) {
        return match restore_backup(&exe).await {
            Ok(()) => Err(ServerError::internal(format!(
                "failed to start restart watchdog; restored previous executable: {}",
                watchdog_error.message
            ))),
            Err(restore_error) => Err(ServerError::internal(format!(
                "failed to start restart watchdog ({}) and failed to restore previous executable ({})",
                watchdog_error.message, restore_error.message
            ))
            .outcome("unknown")),
        };
    }
    if let Some(claim) = upgrade_claim.as_mut() {
        claim.keep_until_exit();
    }

    Ok(Json(serde_json::json!({
        "status": "upgrading",
        "sha256": received,
        "old_sha256": state.exe_sha256,
        "restarting": true,
    }))
    .into_response())
}

/// Write the staged binary, verify it runs (`--version`), and — unless
/// `dry_run` — back up the current exe and atomically swap the new one in.
async fn stage_and_maybe_swap(
    exe: &Path,
    tmp: &Path,
    body: &Bytes,
    dry_run: bool,
) -> Result<(), ServerError> {
    tokio::fs::write(tmp, body)
        .await
        .map_err(|e| ServerError::internal(format!("failed to write staged binary: {e}")))?;
    tokio::fs::set_permissions(tmp, std::fs::Permissions::from_mode(0o755))
        .await
        .map_err(|e| ServerError::internal(format!("failed to chmod staged binary: {e}")))?;

    // Prove it actually runs on this instrument before committing to it.
    let version_ok = check_executable_version(tmp, VERSION_CHECK_TIMEOUT).await?;
    if !version_ok {
        return Err(ServerError::bad_request(
            "uploaded binary failed `--version` (wrong architecture or corrupt); not installed",
        ));
    }

    if dry_run {
        return Ok(());
    }

    let bak = with_suffix(exe, ".bak");
    tokio::fs::copy(exe, &bak)
        .await
        .map_err(|e| ServerError::internal(format!("failed to back up current binary: {e}")))?;
    tokio::fs::rename(tmp, exe)
        .await
        .map_err(|e| ServerError::internal(format!("failed to install new binary: {e}")))?;
    Ok(())
}

async fn check_executable_version(path: &Path, timeout: Duration) -> Result<bool, ServerError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut child = match Command::new(&path)
            .arg("--version")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        {
            Ok(child) => child,
            Err(_) => return false,
        };
        let deadline = Instant::now() + timeout;
        loop {
            match child.try_wait() {
                Ok(Some(status)) => return status.success(),
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(25));
                }
                Ok(None) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return false;
                }
                Err(_) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return false;
                }
            }
        }
    })
    .await
    .map_err(|error| ServerError::internal(format!("version pre-check join error: {error}")))
}

/// Atomically put the pre-upgrade executable back after a post-swap handoff
/// failure. The backup is on the same filesystem by construction.
async fn restore_backup(exe: &Path) -> Result<(), ServerError> {
    let bak = with_suffix(exe, ".bak");
    tokio::fs::rename(&bak, exe).await.map_err(|error| {
        ServerError::internal(format!(
            "failed to restore backup executable {:?} over {:?}: {error}",
            bak, exe
        ))
    })
}

/// Spawn a detached `sh` watchdog that stops this process, launches the new
/// binary with `args`, and rolls back to `<exe>.bak` if it dies immediately.
fn spawn_restart_watchdog(exe: &Path, args: &[String]) -> Result<(), ServerError> {
    let exe_s = exe.to_string_lossy();
    let bak_s = with_suffix(exe, ".bak").to_string_lossy().into_owned();
    let log_s = with_suffix(exe, ".log").to_string_lossy().into_owned();
    let args_q = args.iter().map(|a| shq(a)).collect::<Vec<_>>().join(" ");
    let exe_q = shq(&exe_s);
    let pid = std::process::id();

    // The 1s sleep lets the 200 response flush before we stop this process.
    let script = format!(
        "sleep 1\n\
         kill {pid} 2>/dev/null\n\
         n=0\n\
         while kill -0 {pid} 2>/dev/null && [ $n -lt 20 ]; do sleep 0.25; n=$((n+1)); done\n\
         kill -9 {pid} 2>/dev/null\n\
         sleep 0.5\n\
         {exe_q} {args_q} >>{log} 2>&1 &\n\
         np=$!\n\
         sleep 3\n\
         if kill -0 $np 2>/dev/null; then exit 0; fi\n\
         cp -f {bak} {exe_q}\n\
         exec {exe_q} {args_q} >>{log} 2>&1\n",
        log = shq(&log_s),
        bak = shq(&bak_s),
    );

    let mut cmd = Command::new("sh");
    cmd.arg("-c")
        .arg(&script)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    // Detach into its own session so it outlives this process and has no
    // controlling terminal (no SIGHUP when we exit).
    unsafe {
        cmd.pre_exec(|| {
            libc::setsid();
            Ok(())
        });
    }
    cmd.spawn()
        .map_err(|e| ServerError::internal(format!("failed to spawn restart watchdog: {e}")))?;
    Ok(())
}

/// `path` with `suffix` appended to its file name (`/a/b` + `.bak` -> `/a/b.bak`).
fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path
        .file_name()
        .map(|s| s.to_os_string())
        .unwrap_or_default();
    name.push(suffix);
    path.with_file_name(name)
}

/// POSIX single-quote a string for safe interpolation into the `sh` script.
fn shq(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(c);
        }
    }
    out.push('\'');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shq_escapes_single_quotes() {
        assert_eq!(shq("plain"), "'plain'");
        assert_eq!(shq("a b"), "'a b'");
        assert_eq!(shq("it's"), "'it'\\''s'");
    }

    #[test]
    fn with_suffix_appends_to_filename() {
        assert_eq!(
            with_suffix(Path::new("/data/qslib-server"), ".bak"),
            Path::new("/data/qslib-server.bak")
        );
        assert_eq!(
            with_suffix(Path::new("/data/qslib-server"), ".log"),
            Path::new("/data/qslib-server.log")
        );
    }

    #[test]
    fn upgrade_claim_excludes_competitors_and_releases_on_failure() {
        let flag = AtomicBool::new(false);
        {
            let _claim = UpgradeClaim::acquire(&flag).unwrap();
            let err = UpgradeClaim::acquire(&flag).unwrap_err();
            assert_eq!(err.status, axum::http::StatusCode::CONFLICT);
        }
        assert!(!flag.load(Ordering::Acquire));
        assert!(UpgradeClaim::acquire(&flag).is_ok());
    }

    #[test]
    fn successful_upgrade_claim_stays_set_until_process_exit() {
        let flag = AtomicBool::new(false);
        {
            let mut claim = UpgradeClaim::acquire(&flag).unwrap();
            claim.keep_until_exit();
        }
        assert!(flag.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn backup_restore_replaces_a_swapped_executable() {
        let directory = tempfile::tempdir().unwrap();
        let exe = directory.path().join("qslib-server");
        let bak = with_suffix(&exe, ".bak");
        tokio::fs::write(&exe, b"new").await.unwrap();
        tokio::fs::write(&bak, b"old").await.unwrap();

        restore_backup(&exe).await.unwrap();

        assert_eq!(tokio::fs::read(&exe).await.unwrap(), b"old");
        assert!(!bak.exists());
    }

    #[tokio::test]
    async fn version_precheck_terminates_a_hanging_executable() {
        let directory = tempfile::tempdir().unwrap();
        let executable = directory.path().join("hangs");
        tokio::fs::write(&executable, b"#!/bin/sh\nwhile :; do :; done\n")
            .await
            .unwrap();
        tokio::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755))
            .await
            .unwrap();

        let started = Instant::now();
        assert!(
            !check_executable_version(&executable, Duration::from_millis(50))
                .await
                .unwrap()
        );
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}
