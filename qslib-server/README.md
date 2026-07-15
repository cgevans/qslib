# qslib-server

A small, static Rust binary that runs on a QuantStudio instrument, binds **one**
port on the private eth0 link, and serves — over plain HTTP — bulk file
transfer, one-shot SCPI commands, and a streaming SCPI tunnel. It is a client of
the existing localhost plaintext SCPI server (`127.0.0.1:7000`) and a reader of
the on-disk experiment files; it does **not** modify the InstrumentServer.

Bulk transfer through qslib-server avoids the base64+TLS overhead of `FILE:READ`
over SCPI (~4.7 MB/s) and approaches disk/gigabit speed.

## HTTP API

All routes require `Authorization: Bearer <token>` unless started with
`--no-auth`. Error bodies are JSON: `{"error": "...", "detail": "..."}`.

| Route | Description |
|-------|-------------|
| `GET /health` | `{"name","version","uptime_s","scpi_ok","file_root","exe_sha256"}`; `scpi_ok` is a live TCP probe of the SCPI target; `file_root` is the canonicalized `--file-root`; `exe_sha256` is the running binary's hash (used to confirm an `/upgrade`). |
| `GET`/`HEAD /file/<path…>` | Bulk file off disk under `--file-root`. Supports `Range` (→ `206`), `ETag`, `Last-Modified`, `Accept-Ranges`. Traversal/symlink escape → `403`; missing/dir → `404`. |
| `GET /list/<dir…>` | JSON manifest `{"files":[{"path","size"},…]}` of the regular files under a directory (recursive, dotfiles included, symlinked dirs not descended — matches the `EXP:ZIPREAD?` file set). Lets a client pull a run directory as raw files instead of a base64+deflate zip. Missing/not-a-dir → `404`/`400`. |
| `POST /scpi` | Run one SCPI command. Body is the raw command or JSON `{"command","access","timeout_ms","encoding"}`; query `?access=&timeout_ms=&encoding=` also accepted. Response headers `X-SCPI-Status`, `X-SCPI-Access`. SCPI command error → `400` + `X-SCPI-Error`; access denied → `403`; timeout → `504`. |
| `GET /scpi` (`Upgrade: qslib-scpi`) or `CONNECT /scpi` | Streaming SCPI tunnel spliced to `127.0.0.1:7000`. |
| `POST /upgrade` | Replace the running binary and restart into it. Body is the new binary; `x-qslib-sha256` header must match. Verifies the hash + ELF magic + a `--version` run, then atomically swaps and restarts with rollback. `?dry_run=1` verifies only. `409`/`400` on bad upload. |

```bash
# bulk pull with resume
curl -H "Authorization: Bearer $TOK" -r 0- \
     'http://instr:7500/file/experiments/2026-07-13_run/apldbio/sds/filterdata.zip' -o out.zip

# one-shot SCPI at an access level
curl -H "Authorization: Bearer $TOK" --data 'RUNTitle?' \
     'http://instr:7500/scpi?access=Observer'
```

## Configuration

All options have `--help`. Key ones:

| Flag | Default | Meaning |
|------|---------|---------|
| `--listen` | `127.0.0.1:7500` | Bind address — **use the private eth0 IP only**, never `0.0.0.0`. |
| `--scpi-target` | `127.0.0.1:7000` | Localhost plaintext SCPI endpoint. |
| `--file-root` | `/data/vendor/IS` | Root for `/file`; requests cannot escape it. |
| `--default-access` / `--max-access` | `Observer` / `Controller` | Default and hard-capped SCPI access levels. |
| `--token` / `--token-file` / `QSLIB_SERVER_TOKEN` | — | Bearer token (auth is on by default). |
| `--no-auth` | off | Disable auth (private trusted link only). |
| `--scpi-password` / `QSLIB_SERVER_SCPI_PASSWORD` | — | Password for password-gated access levels. |
| `--log` | stderr | Write logs to a file instead of stderr. |

## Security model

- Bind the **eth0 IP only**; eth0 is an isolated link-local cable to the Windows
  box, with no routable interface. Confidentiality to remote clients is provided
  downstream (Windows-box TLS terminator + tinc VPN).
- Bearer-token auth on every route (including the tunnel) by default.
- SCPI access is still governed by the InstrumentServer; qslib-server
  additionally caps elevation at `--max-access`.
- `/file` is restricted to `--file-root`, canonicalized, with `..`/absolute and
  symlink escapes rejected. `ensure_server` deploys with `--file-root=/` so it can
  serve completed `.eds` files under `/sdcard` as well as the `/data/vendor/IS`
  experiment tree; qslib-server already runs as root and binds the eth0 link only,
  so this widens what is *readable* over the trusted cable but not who can reach it.
  Pass a narrower `file_root=` to `ensure_server` to restrict it.
- Runs as root (started via `SYST:EXEC`); keep it small and private-interface
  only. Threat model = trusted private cable + admin Windows box.

## Building

Native (for tests / the host):

```bash
cargo build -p qslib-server --release
```

Cross-compile to the instrument (static ARMv7 musl, no C deps, ~2 MB stripped):

```bash
rustup target add armv7-unknown-linux-musleabihf
# With cargo-zigbuild (recommended):
cargo zigbuild -p qslib-server --profile min-size --target armv7-unknown-linux-musleabihf
# Or, since all dependencies are pure Rust, with the bundled lld and no C toolchain:
CARGO_TARGET_ARMV7_UNKNOWN_LINUX_MUSLEABIHF_LINKER=rust-lld \
  RUSTFLAGS="-C link-self-contained=yes" \
  cargo build -p qslib-server --profile min-size --target armv7-unknown-linux-musleabihf
# → target/armv7-unknown-linux-musleabihf/min-size/qslib-server (static, stripped)
```

The `min-size` profile (`opt-level="z"`, `lto`, `codegen-units=1`, `strip`) is
defined in the workspace `Cargo.toml` and applies only to `--profile min-size`
builds. It keeps `panic="unwind"` so a stray panic drops one connection rather
than the whole root process.

## Deployment

qslib-server is started on demand, mirroring qslib's dropbear pattern. From Python:

```python
from qslib.machine import Machine
from qslib.experiment import Experiment

m = Machine("instr-host", password="…", server_token="…")   # server_port defaults to 7500
m.ensure_server(
    binary="target/armv7-unknown-linux-musleabihf/min-size/qslib-server",
    listen="169.254.217.190:7500",   # the instrument's private eth0 IP
)
data = m.read_file("experiments/…/filterdata.zip")   # fast HTTP path, falls back to SCPI
exp  = Experiment.from_machine_storage(m, "myrun")    # completed .eds pulled over HTTP
```

When `server_port` is set (default 7500), `Machine` **auto-connects its SCPI session
through the qslib-server tunnel** — the client speaks plaintext SCPI over qslib-server
(no instrument-side TLS) — and falls back to a direct SSL/TCP connection if qslib-server
is not reachable, analogous to the automatic SSL/TCP selection. Pass `server_port=None`
to disable qslib-server entirely. Once connected (or once `ensure_server` confirms it is
running), `Machine.read_file` prefers qslib-server's HTTP `/file` transfer, falling back
to `FILE:READ` over SCPI.

`read_file` resolves the SCPI file context/path to an **absolute** on-instrument path
(via the InstrumentServer `locations.ini` context map) and serves it over HTTP when it
falls under qslib-server's `--file-root`. So that completed-run `.eds` files (which live
on `/sdcard/public_run_complete`, a different mount from the `/data/vendor/IS` experiment
tree) are reachable, **`ensure_server` deploys qslib-server with `--file-root=/` by
default** — the client learns the root from `/health` and only takes the HTTP path for
paths under it, falling back to SCPI otherwise. This means `Experiment.from_machine_storage`
(and `save_run_from_storage`) pull the completed `.eds` over HTTP. Directory reads
(`Experiment.from_running` / `from_uncollected`) use `Machine.download_dir`, which
enumerates the run directory via `/list` and pulls each file raw over `/file` (no
`ZIPREAD` base64+deflate), reproducing the same on-disk tree; it falls back to SCPI
`EXP:ZIPREAD?` when qslib-server is not available.

All of this is validated end-to-end against a real instrument by
`validate_against_machine.py` (SCPI vs HTTP, byte-for-byte).

`ensure_server` streams the binary to the instrument in chunks over SCPI
(gzip + base64 via `SYST:EXEC "echo … | base64 -d"`, size/md5-verified — a
single `FILE:WRITE` is unreliable for large files on some builds), `chmod`s it,
and launches it in the background with `SYST:EXEC "nohup … &"` (root), then polls
`/health`. It is idempotent: qslib-server refuses to double-bind and exits cleanly
if the port is already taken.

qslib-server's HTTP port must be reachable from the client — on the QuantStudio
fleet the Windows box that fronts each instrument forwards it, e.g. socat
`TCP-LISTEN:7500,bind=<lab-ip> → TCP:169.254.x.x:7500` (plaintext is fine behind
the mesh VPN), persisted the same way as the existing `:7443` SCPI forwards.

## Upgrading

Once qslib-server is running, upgrade it **through itself** — no SCPI, no
base64, and it works while it is running (unlike `ensure_server`, which only
helps when nothing is listening):

```python
m.upgrade_server("target/armv7-unknown-linux-musleabihf/min-size/qslib-server")
```

`upgrade_server` uploads the binary raw to `POST /upgrade`. The server verifies
the SHA-256 (client-supplied in `x-qslib-sha256`), checks the ELF magic, and
**runs the new binary with `--version`** — if it does not execute on this
instrument (wrong arch, corrupt) the upgrade is refused before anything is
touched. It then copies the current binary to `<exe>.bak`, atomically renames
the new one into place, and hands off to a detached watchdog (`sh`, its own
session) that stops the old process, launches the new one with the same argv,
and — if the new process dies within a few seconds — restores `<exe>.bak` and
relaunches it. The client confirms success by polling `/health` until
`exe_sha256` equals the uploaded hash (a persistent old hash means it rolled
back, and `upgrade_server` raises). The listener uses `SO_REUSEADDR` so the
restart rebinds the port immediately.

`ensure_server` remains the bootstrap path for the *first* install (over SCPI);
`upgrade_server` is for every update after that.

## Windows box

Add one forward for the qslib-server port: VPN → `169.254.217.190:SERVERPORT` (plaintext
TCP is fine behind the tinc VPN + ssh; or TLS-terminate at the box like the
existing 7443 bridge), plus one inbound firewall allow.

---

*Component authored by Claude Opus 4.8, for Constantine Evans.*
