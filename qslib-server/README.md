# qslib-server

A small, static Rust binary that runs on a QuantStudio instrument, binds **one**
port on the private eth0 link, and serves — over plain HTTP — bulk file
transfer, one-shot SCPI commands, and a streaming SCPI tunnel. It is a client of
the existing localhost plaintext SCPI server (`127.0.0.1:7000`) and a reader of
the on-disk experiment files; it does **not** modify the InstrumentServer.

Rationale and measurements are in `../llm-research/data-transfer-performance.md`
and the design in `../llm-plans/instrument-agent.md`. Bulk transfer through the
agent avoids the base64+TLS overhead of `FILE:READ` over SCPI (~4.7 MB/s) and
approaches disk/gigabit speed.

## HTTP API

All routes require `Authorization: Bearer <token>` unless started with
`--no-auth`. Error bodies are JSON: `{"error": "...", "detail": "..."}`.

| Route | Description |
|-------|-------------|
| `GET /health` | `{"name","version","uptime_s","scpi_ok"}`; `scpi_ok` is a live TCP probe of the SCPI target. |
| `GET`/`HEAD /file/<path…>` | Bulk file off disk under `--file-root`. Supports `Range` (→ `206`), `ETag`, `Last-Modified`, `Accept-Ranges`. Traversal/symlink escape → `403`; missing/dir → `404`. |
| `POST /scpi` | Run one SCPI command. Body is the raw command or JSON `{"command","access","timeout_ms","encoding"}`; query `?access=&timeout_ms=&encoding=` also accepted. Response headers `X-SCPI-Status`, `X-SCPI-Access`. SCPI command error → `400` + `X-SCPI-Error`; access denied → `403`; timeout → `504`. |
| `GET /scpi` (`Upgrade: qslib-scpi`) or `CONNECT /scpi` | Streaming SCPI tunnel spliced to `127.0.0.1:7000`. |

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
- SCPI access is still governed by the InstrumentServer; the agent additionally
  caps elevation at `--max-access`.
- `/file` is restricted to `--file-root`, canonicalized, with `..`/absolute and
  symlink escapes rejected.
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
than the whole root agent.

## Deployment

The agent is started on demand, mirroring qslib's dropbear pattern. From Python:

```python
from qslib.machine import Machine

m = Machine("instr-host", password="…", agent_port=7500, agent_token="…")
m.ensure_agent(
    binary="target/armv7-unknown-linux-musleabihf/min-size/qslib-server",
    listen="169.254.217.190:7500",   # the instrument's private eth0 IP
)
data = m.get_file("experiments/…/filterdata.zip")   # fast path, falls back to SCPI
```

`ensure_agent` streams the binary to the instrument in chunks over SCPI
(gzip + base64 via `SYST:EXEC "echo … | base64 -d"`, size/md5-verified — a
single `FILE:WRITE` is unreliable for large files on some builds), `chmod`s it,
and launches it in the background with `SYST:EXEC "nohup … &"` (root), then polls
`/health`. It is idempotent: the agent refuses to double-bind and exits cleanly
if the port is already taken.

The agent's HTTP port must be reachable from the client — on the QuantStudio
fleet the Windows box that fronts each instrument forwards it, e.g. socat
`TCP-LISTEN:7500,bind=<lab-ip> → TCP:169.254.x.x:7500` (plaintext is fine behind
the mesh VPN), persisted the same way as the existing `:7443` SCPI forwards.

## Windows box

Add one forward for the agent port: VPN → `169.254.217.190:AGENTPORT` (plaintext
TCP is fine behind the tinc VPN + ssh; or TLS-terminate at the box like the
existing 7443 bridge), plus one inbound firewall allow.

---

*Component authored by Claude Opus 4.8, for Constantine Evans.*
