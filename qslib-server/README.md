# qslib-server

`qslib-server` is an optional semantic HTTP/SSE service for QuantStudio
instruments. QSLib does not require it: Python `Machine` and the Rust SCPI
client use direct SCPI by default and keep complete direct implementations.
Setting `server_port` explicitly lets automatic high-level operations prefer
this service.

The server owns one managed SCPI connection for semantic operations and
subscriptions. It normally holds non-exclusive Observer access, serializes
Controller transactions, restores the exact Observer access tuple afterward,
and reconnects when the connection becomes unusable. Optional raw SCPI and
tunnel endpoints use separate connections and never enter the managed actor.

## API

Application routes are versioned beneath `/api/v1`. `GET /health` is the only
unversioned operational route.

The main resources are:

- `GET /health` and `GET /api/v1/capabilities`;
- `GET /api/v1/events` for resumable SSE events;
- `GET /api/v1/operations/{id}` for long-running work;
- `/api/v1/instrument/*` for status and bounded hardware controls;
- `/api/v1/files/{context}/{path}` and
  `/api/v1/directories/{context}/{path}`;
- `/api/v1/experiments/*` for staged experiment packages;
- `/api/v1/runs/*` for run state, start, actions, protocols, and EDS files;
- `POST /api/v1/server/upgrade` for Administrator upgrades.

SSE IDs are opaque `<server-epoch>:<sequence>` cursors. Reconnect with the
last fully processed value in `Last-Event-ID`; do not parse or increment it.
The server replays same-epoch history in order. A cursor from another process
epoch, ahead of the live stream, or older than the 4,096-event in-memory
history receives a `reset` event containing a current instrument status
snapshot, followed by the live stream. Bare numeric IDs remain accepted for
older clients, without cross-restart detection. Capability responses advertise
`"sse_cursor_format": "epoch-sequence"` when these guarantees are available.

`GET /api/v1/runs/current/protocol` returns the exact protocol currently held
by InstrumentServer as SCPI. Protocol updates send that exact SCPI separately
from `tcprotocol_xml`; the latter is only the approximate document consumed by
the instrument's Android display. For stored packages, `qsl-tcprotocol.xml`
is preferred and `tcprotocol.xml` is used only when no lossless QSLib protocol
is present.

The named file contexts are `default`, `experiments`, `runs`, `logs`,
`templates`, `calibrations`, `public_run_complete`, and
`private_run_complete`. Arbitrary absolute filesystem paths are not exposed.
File reads retain range and ETag support; writes are atomic and require both a
Controller token and `--allow-file-writes`.

Non-idempotent run operations require `Idempotency-Key`. They return an
operation resource with `queued`, `running`, `succeeded`, `failed`, or
`unknown` state. Clients must inspect that resource after an ambiguous
transport failure instead of repeating the mutation through SCPI.

Errors have a stable shape and an `X-Request-ID` header matching the body:

```json
{
  "error": {
    "code": "instrument_busy",
    "message": "Exclusive Controller access was refused",
    "retryable": true,
    "outcome": "not_started"
  },
  "request_id": "..."
}
```

## Authentication and policy

Authentication uses a root-readable TOML file supplied by `--auth-config` or
`QSLIB_SERVER_AUTH_CONFIG`. Store SHA-256 digests, not bearer tokens:

```toml
unauthenticated_role = "controller"

[[tokens]]
name = "owner"
sha256 = "<64 lowercase hexadecimal characters>"
role = "administrator"
```

Roles are `observer`, `controller`, and `administrator`. For an explicitly
trusted private deployment, the optional top-level `unauthenticated_role`
grants requests without an Authorization header that role while retaining the
token entries for higher privileges. A supplied but invalid bearer token is
rejected rather than falling back. Omit `unauthenticated_role` to require a
valid token for every request.

When no token ACL is needed, `--no-auth` must be paired with an explicit
`--unauthenticated-role`; its default cap is Observer.

Important policy flags:

| Flag | Default | Meaning |
|------|---------|---------|
| `--listen` | `127.0.0.1:7500` | HTTP bind address. Bind only a trusted private interface. |
| `--scpi-target` | `127.0.0.1:7000` | Local InstrumentServer SCPI endpoint. |
| `--file-root` | `/data/vendor/IS` | Base used to resolve named contexts. |
| `--max-access` | `Controller` | Absolute SCPI elevation cap for semantic work. |
| `--allow-file-writes` | off | Permit Controller file/package/protocol writes. |
| `--allow-controls` | off | Permit Controller hardware and run controls. |
| `--enable-raw-scpi` | off | Add Administrator-only `POST /api/v1/scpi`. |
| `--enable-scpi-tunnel` | off | Add Administrator-only `/api/v1/scpi/tunnel`. |
| `--scpi-password` | unset | Instrument password, used once per managed connection. |

`--max-access` cannot constrain commands sent inside a transparent tunnel;
enable tunnels only for trusted administrators.

## Client use

Direct SCPI is the standard mode and performs no HTTP probe:

```python
from qslib.machine import Machine

machine = Machine("instrument.example", password="instrument-password")
status = machine.get_status()
```

The semantic service is opt-in. If it is unavailable before a read is
submitted, QSLib safely falls back to direct SCPI. A mutation that may have
reached the server is never repeated automatically.

```python
machine = Machine(
    "instrument.example",
    password="instrument-password",
    server_port=7500,
    server_token="controller-bearer-token",
)
status = machine.get_status()
```

Manual `connect()`, access contexts, `run_command*`, authentication, arbitrary
session variables, and user-created subscriptions always retain direct SCPI
session semantics.

## Build and bootstrap

Build natively for tests:

```bash
cargo build -p qslib-server --release
```

For an instrument, cross-compile the static ARM binary as appropriate for the
target, then use `Machine.ensure_server(...)`. Bootstrap remains a direct-SCPI
administrator helper: it uploads the binary and an ACL file and starts the new
server. It does not migrate historical experimental configurations.

```python
machine.ensure_server(
    binary="target/armv7-unknown-linux-musleabihf/min-size/qslib-server",
    listen="169.254.217.190:7500",
)
```

Subsequent upgrades use `Machine.upgrade_server(...)`, which uploads to
`POST /api/v1/server/upgrade`, validates the executable/hash, and uses the
watchdog rollback path.
