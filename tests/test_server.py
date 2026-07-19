# SPDX-FileCopyrightText: 2024 - 2026 Constantine Evans <qslib@mb.costi.net>
# SPDX-License-Identifier: EUPL-1.2

"""Contract tests for the optional v1 HTTP client and Python dispatch."""

from __future__ import annotations

import hashlib
import io
import os
import socket
import socketserver
import subprocess
import threading
import time
import urllib.error
from contextlib import contextmanager, nullcontext
from http.client import HTTPException
from pathlib import Path

import pytest

from qslib import AccessLevel, Experiment, Machine
from qslib.experiment import (
    AlreadyExistsCompleteError,
    AlreadyExistsWorkingError,
    MachineBusyError,
    NotRunningError,
)
from qslib.server import ServerClient, ServerError, ServerOutcomeUnknown, ServerUnavailable, _parse_sse


def _server_binary() -> Path:
    """Locate the qslib-server binary.

    The coverage recipes redirect CARGO_TARGET_DIR to cargo-llvm-cov's target
    directory, so the plain ``target/debug`` path is not enough.
    """
    override = os.environ.get("QSLIB_SERVER_BINARY")
    if override:
        return Path(override)
    target_dir = os.environ.get("CARGO_TARGET_DIR")
    if target_dir:
        candidate = Path(target_dir) / "debug" / "qslib-server"
        if candidate.exists():
            return candidate
    return Path(__file__).parent.parent / "target" / "debug" / "qslib-server"


BINARY = _server_binary()


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class _MutationResponse:
    def __init__(self, body: bytes = b"", error: BaseException | None = None):
        self.body = body
        self.error = error
        self.headers: dict[str, str] = {}

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self) -> bytes:
        if self.error is not None:
            raise self.error
        return self.body


class _ScpiHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        self.wfile.write(b"READy -session=1 -product=Test -version=1 -build=1 -capabilities=Index\n")
        self.wfile.flush()
        for raw in self.rfile:
            line = raw.decode().strip()
            first, separator, rest = line.partition(" ")
            identifier = first if first.isdigit() else None
            command = rest if identifier and separator else line
            if command == "ACC?":
                body = "-stealth=False -exclusive=False Observer"
            elif command.startswith("TBC:ControlZones?"):
                body = "6"
            elif command.startswith("POW?"):
                body = "ON"
            elif command.startswith("BLOCK?"):
                body = "ON 60"
            elif command.startswith("LED:STATus?"):
                body = "green on"
            elif command.startswith("RET ${RunTitle"):
                body = "- -1 -1 -1 -1 -1 -1 Idle"
            elif command.startswith("REMainingTime?"):
                body = "-"
            elif command.startswith("RET $(DRAWER?)"):
                body = (
                    'Closed Down off "25 25 25 25 25 25" "25 25 25 25 25 25" 30 '
                    '"-Zone1=25 -Zone2=25 -Zone3=25 -Zone4=25 -Zone5=25 -Zone6=25" '
                    '"-Zone1=False -Zone2=False -Zone3=False -Zone4=False -Zone5=False -Zone6=False" 31'
                )
            else:
                body = ""
            prefix = f"OK {identifier}" if identifier else "OK"
            self.wfile.write(f"{prefix}{' ' if body else ''}{body}\n".encode())
            self.wfile.flush()
            if command.startswith("QUIT"):
                return


@pytest.fixture
def server(tmp_path: Path):
    if not BINARY.exists():
        message = f"server binary not built ({BINARY})"
        # Skipping this whole file is easy to do by accident, which leaves the
        # HTTP path untested without any signal. CI sets this to make it loud.
        if os.environ.get("QSLIB_REQUIRE_SERVER_BINARY") == "1":
            pytest.fail(message)
        pytest.skip(message)
    for context in (
        "experiments",
        "runs",
        "logs",
        "templates",
        "calibrations",
        "public_run_complete",
        "private_run_complete",
    ):
        (tmp_path / context).mkdir()
    scpi = socketserver.ThreadingTCPServer(("127.0.0.1", 0), _ScpiHandler)
    scpi.daemon_threads = True
    thread = threading.Thread(target=scpi.serve_forever, daemon=True)
    thread.start()
    port = _free_port()
    process = subprocess.Popen(
        [
            str(BINARY),
            "--listen",
            f"127.0.0.1:{port}",
            "--scpi-target",
            f"127.0.0.1:{scpi.server_address[1]}",
            "--file-root",
            str(tmp_path),
            "--no-auth",
            "--unauthenticated-role",
            "administrator",
            "--max-access",
            "Administrator",
            "--allow-file-writes",
            "--allow-controls",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    client = ServerClient("127.0.0.1", port=port, timeout=2)
    deadline = time.monotonic() + 5
    try:
        while time.monotonic() < deadline:
            try:
                if client.health().get("ready"):
                    break
            except ServerError:
                time.sleep(0.02)
        else:
            raise TimeoutError("qslib-server actor did not become ready")
        yield client, tmp_path
    finally:
        process.terminate()
        process.wait(timeout=5)
        scpi.shutdown()
        scpi.server_close()


def test_health_capabilities_and_status(server):
    client, _root = server
    health = client.health()
    assert health["ready"] is True
    assert health["current_access"]["level"] == "observer"
    capabilities = client.capabilities()
    assert capabilities["api_version"] == "v1"
    assert capabilities["sse"] is True
    assert capabilities["sse_cursor_format"] == "epoch-sequence"
    assert capabilities["raw_scpi"] is False
    status = client.instrument_status()
    assert status["zone_count"] == 6
    assert status["run"]["state"] == "Idle"
    assert status["run"]["remaining_time_s"] is None


def test_sse_parser_preserves_opaque_cursor():
    event = _parse_sse(
        [
            "id: 4db8d4e9-87a7-4ce7-8f5f-f6718c3887e1:42",
            "event: run",
            'data: {"message":"Starting"}',
        ]
    )
    assert event is not None
    assert event["id"] == "4db8d4e9-87a7-4ce7-8f5f-f6718c3887e1:42"


def test_sse_events_surface_permanent_http_errors(monkeypatch):
    client = ServerClient("instrument")

    def fail(*_args, **_kwargs):
        raise ServerError("invalid token", status=401, code="unauthorized")

    monkeypatch.setattr(client, "_request", fail)
    with pytest.raises(ServerError, match="invalid token") as error:
        next(client.events())
    assert error.value.status == 401
    assert error.value.code == "unauthorized"


def test_sse_events_retry_retryable_server_errors(monkeypatch):
    client = ServerClient("instrument")
    attempts = 0

    class Response(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            self.close()

    def request(*_args, **_kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ServerError("temporarily unavailable", status=503, retryable=True)
        return Response(b'id: epoch:1\nevent: run\ndata: {"state":"running"}\n\n')

    monkeypatch.setattr(client, "_request", request)
    monkeypatch.setattr("qslib.server.time.sleep", lambda _seconds: None)
    event = next(client.events())
    assert attempts == 2
    assert event == {"id": "epoch:1", "event": "run", "data": {"state": "running"}}


def test_sse_events_back_off_after_clean_eof(monkeypatch):
    client = ServerClient("instrument")
    responses = iter(
        [
            b"",
            b'id: epoch:2\nevent: run\ndata: {"state":"completed"}\n\n',
        ]
    )
    sleeps: list[float] = []

    class Response(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            self.close()

    monkeypatch.setattr(client, "_request", lambda *_args, **_kwargs: Response(next(responses)))
    monkeypatch.setattr("qslib.server.time.sleep", sleeps.append)

    event = next(client.events())
    assert event["id"] == "epoch:2"
    assert sleeps == [0.25]


def test_machine_get_running_protocol_no_run_server(server):
    client, _root = server
    machine = Machine(client.host, server_port=client.port)

    with pytest.raises(ValueError, match="Nothing is currently running"):
        machine.get_running_protocol()


def test_named_file_resources_and_range(server, tmp_path: Path):
    client, root = server
    data = b"0123456789" * 100
    (root / "logs" / "messages.log").write_bytes(data)
    assert client.get_file("messages.log", context="logs") == data
    assert client.get_file("messages.log", context="logs", range_start=995) == data[995:]

    client.put_file("nested/value.bin", b"value", context="experiments")
    assert (root / "experiments" / "nested" / "value.bin").read_bytes() == b"value"
    entries = client.list_context_dir("experiments", "")
    assert entries[0]["path"] == "nested"
    assert entries[0]["type"] == "folder"
    recursive = client.list_context_dir("experiments", "", recursive=True)
    assert recursive[0]["path"] == "nested/value.bin"

    destination = tmp_path / "download"
    assert client.download_dir("/data/vendor/IS/experiments/nested", destination) == 1
    assert (destination / "value.bin").read_bytes() == b"value"

    with pytest.raises(FileNotFoundError):
        client.get_file("missing.bin", context="logs")
    with pytest.raises(FileNotFoundError):
        client.list_context_dir("logs", "missing")


def test_unversioned_routes_are_gone(server):
    client, _root = server
    with pytest.raises(ServerError) as error:
        client._json("/file/old")
    assert error.value.status == 404


def test_dead_server_and_negative_capability_cache(monkeypatch):
    client = ServerClient("127.0.0.1", port=_free_port(), timeout=0.1)
    with pytest.raises(ServerError):
        client.capabilities()
    with pytest.raises(ServerError, match="negatively cached"):
        client.capabilities()
    assert client.available() is False


def test_capabilities_do_not_negative_cache_auth_failures(monkeypatch):
    client = ServerClient("instrument", token="bad-token")
    attempts = 0

    def unauthorized(*_args, **_kwargs):
        nonlocal attempts
        attempts += 1
        raise ServerError("invalid token", status=401, code="unauthorized")

    monkeypatch.setattr(client, "_json", unauthorized)
    for _ in range(2):
        with pytest.raises(ServerError, match="invalid token") as error:
            client.capabilities()
        assert error.value.status == 401
        assert error.value.code == "unauthorized"
    assert attempts == 2


@pytest.mark.parametrize("status", [401, 403])
def test_machine_capability_auth_failure_never_falls_back_to_scpi(monkeypatch, status):
    class FakeServer:
        def capabilities(self):
            raise ServerError("server credentials rejected", status=status)

    machine = Machine("instrument", server_port=7500, server_token="wrong-token")
    monkeypatch.setattr(type(machine), "server", property(lambda _self: FakeServer()))
    monkeypatch.setattr(
        machine,
        "_direct_connection",
        lambda: (_ for _ in ()).throw(AssertionError("auth failure must not open direct SCPI")),
    )

    with pytest.raises(ServerError, match="server credentials rejected") as error:
        machine.drawer_open()
    assert error.value.status == status


def test_directory_listing_rejects_missing_or_invalid_entries(monkeypatch):
    client = ServerClient("instrument")

    monkeypatch.setattr(client, "_json", lambda *_args, **_kwargs: {"files": []})
    with pytest.raises(ServerError, match="omitted its entries list"):
        client.list_context_dir("runs")

    monkeypatch.setattr(client, "_json", lambda *_args, **_kwargs: {"entries": {}})
    with pytest.raises(ServerError, match="omitted its entries list"):
        client.list_context_dir("runs")


def test_mutation_transport_failure_is_outcome_unknown(monkeypatch):
    client = ServerClient("127.0.0.1", port=1)

    def fail(*_args, **_kwargs):
        raise urllib.error.URLError("dropped")

    monkeypatch.setattr("urllib.request.urlopen", fail)
    with pytest.raises(ServerOutcomeUnknown) as error:
        client.set_power(True)
    assert error.value.state_query == "/api/v1/instrument/status"


@pytest.mark.parametrize(
    "response_error",
    [
        OSError("response dropped"),
        HTTPException("response truncated"),
        ValueError("response closed"),
    ],
)
def test_json_mutation_response_failure_is_outcome_unknown(monkeypatch, response_error):
    client = ServerClient("instrument")
    monkeypatch.setattr(
        client,
        "_request",
        lambda *_args, **_kwargs: _MutationResponse(error=response_error),
    )

    with pytest.raises(ServerOutcomeUnknown) as error:
        client.set_power(True)
    assert error.value.state_query == "/api/v1/instrument/status"


@pytest.mark.parametrize(
    "response",
    [
        _MutationResponse(error=OSError("response dropped")),
        _MutationResponse(body=b"\xff"),
    ],
)
def test_scpi_response_failure_is_outcome_unknown(monkeypatch, response):
    client = ServerClient("instrument")
    monkeypatch.setattr(client, "_request", lambda *_args, **_kwargs: response)

    with pytest.raises(ServerOutcomeUnknown) as error:
        client.scpi("POW?")
    assert error.value.state_query == "/api/v1/instrument/status"


@pytest.mark.parametrize(
    "response",
    [
        _MutationResponse(error=OSError("response dropped")),
        _MutationResponse(body=b"{"),
        _MutationResponse(body=b"[]"),
    ],
)
def test_upgrade_response_failure_is_outcome_unknown(monkeypatch, response):
    client = ServerClient("instrument")
    monkeypatch.setattr(client, "_request", lambda *_args, **_kwargs: response)

    with pytest.raises(ServerOutcomeUnknown) as error:
        client.upgrade(b"new binary")
    assert error.value.state_query == "/health"


@pytest.mark.parametrize("delete", ["experiment", "staged-package"])
def test_delete_response_failure_is_outcome_unknown(monkeypatch, delete):
    client = ServerClient("instrument")
    monkeypatch.setattr(
        client,
        "_request",
        lambda *_args, **_kwargs: _MutationResponse(error=OSError("response dropped")),
    )

    with pytest.raises(ServerOutcomeUnknown) as error:
        if delete == "experiment":
            client.delete_experiment("active")
        else:
            client.delete_staged_package("active", '"etag"')
    assert error.value.state_query == "/api/v1/experiments/active"


def test_connection_refusal_is_known_not_submitted():
    port = _free_port()
    client = ServerClient("127.0.0.1", port=port, timeout=0.2)
    with pytest.raises(ServerUnavailable) as error:
        client.set_power(True)
    assert error.value.outcome == "not_started"


@pytest.mark.parametrize("transport_error", [urllib.error.URLError("offline"), OSError("offline")])
def test_read_transport_failures_are_server_unavailable(monkeypatch, transport_error):
    client = ServerClient("instrument")

    def fail(*_args, **_kwargs):
        raise transport_error

    monkeypatch.setattr("urllib.request.urlopen", fail)
    with pytest.raises(ServerUnavailable) as error:
        client.health()
    assert error.value.outcome == "not_started"
    assert error.value.retryable is True


def test_malformed_http_response_is_not_classified_as_unavailable(monkeypatch):
    client = ServerClient("instrument")

    def fail(*_args, **_kwargs):
        raise HTTPException("malformed response")

    monkeypatch.setattr("urllib.request.urlopen", fail)
    with pytest.raises(ServerError, match="invalid HTTP response") as error:
        client.health()
    assert not isinstance(error.value, ServerUnavailable)


def test_machine_server_status_opens_no_scpi(monkeypatch):
    class FakeServer:
        def capabilities(self):
            return {"api_version": "v1", "resources": ["instrument"], "controls": True}

        def instrument_status(self):
            return {
                "drawer": "Closed",
                "cover": "Down",
                "lamp_status": "off",
                "sample_temperatures_c": [25.0] * 6,
                "block_temperatures_c": [25.0] * 6,
                "cover_temperature_c": 30.0,
                "target_temperatures_c": {"Zone1": 25.0},
                "target_controlled": {"Zone1": False},
                "led_temperature_c": 31.0,
                "zone_count": 6,
                "indicator": {"color": "green", "mode": "on"},
                "block": {"enabled": True, "target_c": 60.0},
            }

    machine = Machine("instrument", server_port=7500)
    monkeypatch.setattr(type(machine), "server", property(lambda _self: FakeServer()))
    monkeypatch.setattr(
        machine,
        "_direct_connection",
        lambda: (_ for _ in ()).throw(AssertionError("server success must not open SCPI")),
    )
    assert machine.machine_status().drawer == "Closed"
    assert machine.get_zone_count() == 6


def test_machine_get_running_protocol_idle_uses_server_without_opening_scpi(monkeypatch):
    class FakeServer:
        def capabilities(self):
            return {"api_version": "v1", "resources": ["runs"], "controls": True}

        def current_run(self):
            return {"name": "-", "state": "idle"}

    machine = Machine("instrument", server_port=7500)
    monkeypatch.setattr(type(machine), "server", property(lambda _self: FakeServer()))
    monkeypatch.setattr(
        machine,
        "_direct_connection",
        lambda: (_ for _ in ()).throw(AssertionError("idle server result must not open SCPI")),
    )

    with pytest.raises(ValueError, match="Nothing is currently running"):
        machine.get_running_protocol()


def test_machine_get_running_protocol_uses_server_scpi_not_display_xml(monkeypatch):
    scpi = """PROT -volume=35 -runmode=standard exact_protocol <multiline.protocol>
        STAGE 1 STAGE_1 <multiline.stage>
            STEP 1 <multiline.step>
                RAMP 25
                HOLD 60
            </multiline.step>
        </multiline.stage>
    </multiline.protocol>"""

    class FakeServer:
        def capabilities(self):
            return {"api_version": "v1", "resources": ["runs"], "controls": True}

        def current_run(self):
            return {"name": "active_run", "state": "running"}

        def get_running_protocol(self):
            return {"name": "exact_protocol", "scpi": scpi}

    machine = Machine("instrument", server_port=7500)
    monkeypatch.setattr(type(machine), "server", property(lambda _self: FakeServer()))
    monkeypatch.setattr(
        machine,
        "_direct_connection",
        lambda: (_ for _ in ()).throw(AssertionError("server protocol success must not open SCPI")),
    )

    protocol = machine.get_running_protocol()

    assert protocol.name == "exact_protocol"
    assert protocol.volume == 35
    assert len(protocol.stages) == 1


def test_experiment_action_uses_server_without_opening_scpi(monkeypatch):
    actions: list[tuple[str, str]] = []

    class FakeServer:
        def capabilities(self):
            return {"api_version": "v1", "resources": ["runs"], "controls": True}

        def current_run(self):
            return {
                "name": "server-run",
                "stage": 1,
                "stage_name": "1",
                "num_stages": 1,
                "cycle": 1,
                "num_cycles": 1,
                "step": 1,
                "point": 1,
                "state": "running",
            }

        def run_action(self, name, action):
            actions.append((name, action))
            return {"id": "operation"}

        def wait_operation(self, operation, timeout=120.0):
            assert operation["id"] == "operation"
            return {"state": "succeeded"}

    machine = Machine("instrument", server_port=7500)
    monkeypatch.setattr(type(machine), "server", property(lambda _self: FakeServer()))
    monkeypatch.setattr(
        machine,
        "_direct_connection",
        lambda: (_ for _ in ()).throw(AssertionError("server action must not open SCPI")),
    )
    Experiment("server-run").pause_now(machine)
    assert actions == [("server-run", "pause")]


def test_machine_without_server_configuration_never_probes_http(monkeypatch):
    machine = Machine("instrument")
    monkeypatch.setattr(
        ServerClient,
        "capabilities",
        lambda _self: (_ for _ in ()).throw(AssertionError("unexpected HTTP probe")),
    )
    marker = RuntimeError("direct path selected")
    monkeypatch.setattr(machine, "_direct_connection", lambda: (_ for _ in ()).throw(marker))
    with pytest.raises(RuntimeError, match="direct path selected"):
        machine.machine_status()


def test_machine_upgrade_uses_v1_health_hash(monkeypatch):
    payload = b"\x7fELF" + b"payload" * 20
    expected = hashlib.sha256(payload).hexdigest()
    current = {"hash": "0" * 64}

    class FakeServer:
        def health(self):
            return {"executable_sha256": current["hash"]}

        def upgrade(self, binary):
            current["hash"] = hashlib.sha256(binary).hexdigest()
            return {"status": "upgrading"}

    machine = Machine("instrument", automatic=False, server_port=7500)
    fake = FakeServer()
    monkeypatch.setattr(type(machine), "server", property(lambda _self: fake))
    assert machine.upgrade_server(payload, poll_interval=0.001) is fake
    assert current["hash"] == expected


@pytest.mark.parametrize("remote_path", ['/data/x";reboot', "/data/it's-bad"])
def test_ensure_server_rejects_unsafe_exec_values(remote_path):
    machine = Machine("127.0.0.1", server_port=_free_port())
    with pytest.raises(ValueError, match="unsafe"):
        machine.ensure_server(
            binary=b"stub",
            listen="1.2.3.4:7500",
            remote_path=remote_path,
        )


@pytest.mark.parametrize("status", [401, 403, 404, 503])
def test_ensure_server_does_not_redeploy_over_http_response(monkeypatch, status):
    class FakeServer:
        def health(self):
            raise ServerError("existing server rejected health request", status=status)

    machine = Machine("instrument", server_port=7500, server_token="wrong-token")
    monkeypatch.setattr(type(machine), "server", property(lambda _self: FakeServer()))
    deployments: list[tuple[str, bytes]] = []
    commands: list[str] = []
    monkeypatch.setattr(machine, "_deploy_binary", lambda path, data: deployments.append((path, data)))
    monkeypatch.setattr(machine, "run_command", commands.append)

    with pytest.raises(ServerError, match="existing server rejected") as error:
        machine.ensure_server(binary=b"replacement", listen="169.254.1.2:7500")
    assert error.value.status == status
    assert deployments == []
    assert commands == []


def test_ensure_server_does_not_redeploy_after_malformed_health_response(monkeypatch):
    class FakeServer:
        def health(self):
            raise ServerError("qslib-server returned invalid JSON for /health")

    machine = Machine("instrument", server_port=7500)
    monkeypatch.setattr(type(machine), "server", property(lambda _self: FakeServer()))
    monkeypatch.setattr(
        machine,
        "_deploy_binary",
        lambda *_args: (_ for _ in ()).throw(AssertionError("responding server must not be redeployed")),
    )

    with pytest.raises(ServerError, match="invalid JSON"):
        machine.ensure_server(binary=b"replacement", listen="169.254.1.2:7500")


def test_ensure_server_waits_for_existing_server_to_be_ready(monkeypatch):
    health_responses = iter([{"ready": False}, {"ready": True}])

    class FakeServer:
        def health(self):
            return next(health_responses)

    machine = Machine("instrument", server_port=7500)
    fake = FakeServer()
    monkeypatch.setattr(type(machine), "server", property(lambda _self: fake))
    monkeypatch.setattr("qslib.machine.time.sleep", lambda _seconds: None)
    monkeypatch.setattr(
        machine,
        "_deploy_binary",
        lambda *_args: (_ for _ in ()).throw(AssertionError("existing server must not be redeployed")),
    )

    assert machine.ensure_server(binary=b"replacement", listen="169.254.1.2:7500") is fake


@pytest.mark.parametrize("health", [{"ready": False}, {}])
def test_ensure_server_refuses_to_redeploy_over_unready_server(monkeypatch, health):
    class FakeServer:
        def health(self):
            return health

    ticks = iter([0.0, 0.0, 1.0])
    machine = Machine("instrument", server_port=7500)
    monkeypatch.setattr(type(machine), "server", property(lambda _self: FakeServer()))
    monkeypatch.setattr("qslib.machine.time.monotonic", lambda: next(ticks))
    monkeypatch.setattr("qslib.machine.time.sleep", lambda _seconds: None)
    monkeypatch.setattr(
        machine,
        "_deploy_binary",
        lambda *_args: (_ for _ in ()).throw(AssertionError("unready server must not be redeployed")),
    )

    with pytest.raises(ServerError, match="refusing to redeploy"):
        machine.ensure_server(binary=b"replacement", listen="169.254.1.2:7500", timeout=0.5)


def test_ensure_server_requires_explicit_readiness_after_launch(monkeypatch):
    health_calls = 0

    class FakeServer:
        def health(self):
            nonlocal health_calls
            health_calls += 1
            if health_calls == 1:
                raise ServerUnavailable("not running")
            return {}

    ticks = iter([0.0, 0.0, 1.0])
    machine = Machine("instrument", server_port=7500)
    monkeypatch.setattr(type(machine), "server", property(lambda _self: FakeServer()))
    monkeypatch.setattr("qslib.machine.time.monotonic", lambda: next(ticks))
    monkeypatch.setattr("qslib.machine.time.sleep", lambda _seconds: None)
    monkeypatch.setattr(machine, "ensured_connection", lambda *_args, **_kwargs: nullcontext(machine))
    deployments: list[tuple[str, bytes]] = []
    monkeypatch.setattr(machine, "_deploy_binary", lambda path, data: deployments.append((path, data)))
    monkeypatch.setattr(machine, "run_command", lambda _command: None)

    with pytest.raises(ServerError, match="did not become ready after deployment"):
        machine.ensure_server(binary=b"server", listen="169.254.1.2:7500", timeout=0.5)
    assert deployments == [("/data/qslib-server", b"server")]
    assert health_calls == 2


def _capture_ensure_server(monkeypatch, machine):
    health_calls = 0

    class FakeServer:
        def health(self):
            nonlocal health_calls
            health_calls += 1
            if health_calls == 1:
                raise ServerUnavailable("not running")
            return {"ready": True}

    fake = FakeServer()
    deployments: list[tuple[str, bytes]] = []
    commands: list[str] = []
    monkeypatch.setattr(type(machine), "server", property(lambda _self: fake))
    monkeypatch.setattr(machine, "ensured_connection", lambda *_args, **_kwargs: nullcontext(machine))
    monkeypatch.setattr(machine, "_deploy_binary", lambda path, data: deployments.append((path, data)))
    monkeypatch.setattr(machine, "run_command", commands.append)
    return fake, deployments, commands


def test_ensure_server_tokenless_defaults_to_read_only_observer(monkeypatch):
    machine = Machine("instrument", server_port=7500)
    fake, deployments, commands = _capture_ensure_server(monkeypatch, machine)

    assert machine.ensure_server(binary=b"server", listen="169.254.1.2:7500") is fake
    assert deployments == [("/data/qslib-server", b"server")]
    launch = commands[-1]
    assert "--no-auth --unauthenticated-role observer" in launch
    assert "--allow-file-writes" not in launch
    assert "--allow-controls" not in launch


def test_ensure_server_token_enables_authenticated_mutations(monkeypatch):
    token = "a-high-entropy-bootstrap-token"
    machine = Machine("instrument", server_port=7500, server_token=token)
    fake, deployments, commands = _capture_ensure_server(monkeypatch, machine)

    assert machine.ensure_server(binary=b"server", listen="169.254.1.2:7500") is fake
    assert [path for path, _data in deployments] == [
        "/data/qslib-server.auth.toml",
        "/data/qslib-server",
    ]
    auth = deployments[0][1].decode()
    assert hashlib.sha256(token.encode()).hexdigest() in auth
    assert 'role = "administrator"' in auth
    assert token not in auth
    launch = commands[-1]
    assert "--auth-config /data/qslib-server.auth.toml" in launch
    assert "--allow-file-writes" in launch
    assert "--allow-controls" in launch
    assert "--no-auth" not in launch


def test_ensure_server_warns_for_elevated_unauthenticated_role(monkeypatch):
    machine = Machine("instrument", server_port=7500)
    _fake, _deployments, commands = _capture_ensure_server(monkeypatch, machine)

    with pytest.warns(RuntimeWarning, match="unauthenticated Administrator"):
        machine.ensure_server(
            binary=b"server",
            listen="169.254.1.2:7500",
            unauthenticated_role="administrator",
        )
    assert "--unauthenticated-role administrator" in commands[-1]
    assert "--allow-file-writes" not in commands[-1]
    assert "--allow-controls" not in commands[-1]


def test_wait_operation_returns_success_and_raises_structured_failures(monkeypatch):
    client = ServerClient("instrument")
    records = iter(
        [
            {"state": "running"},
            {"state": "succeeded", "result": {"key": "123456"}},
        ]
    )
    monkeypatch.setattr(client, "operation", lambda _operation_id: next(records))
    assert client.wait_operation("one", timeout=1)["result"]["key"] == "123456"

    monkeypatch.setattr(
        client,
        "operation",
        lambda _operation_id: {
            "state": "failed",
            "error": {
                "status": 409,
                "code": "working_exists",
                "message": "already there",
                "retryable": False,
                "outcome": "not_started",
                "details": {"name": "run"},
            },
        },
    )
    with pytest.raises(ServerError) as failed:
        client.wait_operation("two")
    assert failed.value.status == 409
    assert failed.value.code == "working_exists"
    assert failed.value.details == {"name": "run"}

    monkeypatch.setattr(
        client,
        "operation",
        lambda _operation_id: {
            "state": "unknown",
            "error": {"code": "deadline_exceeded", "message": "uncertain", "outcome": "unknown"},
        },
    )
    with pytest.raises(ServerOutcomeUnknown) as unknown:
        client.wait_operation("three")
    assert unknown.value.state_query.endswith("/three")


@pytest.mark.parametrize("value, expected", [(True, True), (False, False), ("ON", True), ("off", False)])
def test_power_normalization_matches_direct_and_server(monkeypatch, value, expected):
    server_values: list[bool] = []

    class FakeServer:
        def capabilities(self):
            return {"api_version": "v1", "resources": ["instrument"], "controls": True}

        def set_power(self, enabled):
            server_values.append(enabled)

    server_machine = Machine("instrument", server_port=7500)
    monkeypatch.setattr(type(server_machine), "server", property(lambda _self: FakeServer()))
    server_machine.power = value
    assert server_values == [expected]

    commands: list[str] = []
    direct = Machine("instrument")
    monkeypatch.setattr(direct, "ensured_connection", lambda *_args, **_kwargs: nullcontext(direct))
    monkeypatch.setattr(direct, "run_command", commands.append)
    direct.power = value
    assert commands == [f"POW {'on' if expected else 'off'}"]


@pytest.mark.parametrize("value", [None, 0, 1, "yes", "true", "", object()])
def test_invalid_power_values_fail_before_either_backend(monkeypatch, value):
    machine = Machine("instrument", server_port=7500)
    monkeypatch.setattr(
        machine,
        "_server_for",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("backend must not be selected")),
    )
    with pytest.raises(ValueError, match="Power value"):
        machine.power = value


def test_server_machine_enforces_local_access_cap(monkeypatch):
    calls: list[str] = []

    class FakeServer:
        def capabilities(self):
            return {"api_version": "v1", "resources": ["instrument"], "controls": True}

        def instrument_status(self):
            calls.append("read")
            return {"power_enabled": True}

        def set_power(self, _enabled):
            calls.append("write")

    fake = FakeServer()
    observer = Machine("instrument", server_port=7500, max_access_level=AccessLevel.Guest)
    monkeypatch.setattr(type(observer), "server", property(lambda _self: fake))
    with pytest.raises(ValueError, match="above maximum"):
        _ = observer.power

    controller = Machine("instrument", server_port=7500, max_access_level=AccessLevel.Observer)
    with pytest.raises(ValueError, match="above maximum"):
        controller.power = True
    assert calls == []


def test_server_status_preserves_raw_scpi_state(monkeypatch):
    class FakeServer:
        def capabilities(self):
            return {"api_version": "v1", "resources": ["runs"], "controls": True}

        def current_run(self):
            return {
                "name": "active",
                "stage": 1,
                "stage_name": "PRERUN",
                "num_stages": 2,
                "cycle": 1,
                "num_cycles": 3,
                "step": 1,
                "point": 2,
                "state": "Running",
            }

    machine = Machine("instrument", server_port=7500)
    monkeypatch.setattr(type(machine), "server", property(lambda _self: FakeServer()))
    assert machine.run_status().state == "Running"


def test_file_listing_emulates_folders_metadata_globs_and_shadows(server):
    client, root = server
    experiments = root / "experiments"
    folder = experiments / "run_a"
    nested = folder / "nested"
    nested.mkdir(parents=True)
    visible = nested / "result.xml"
    visible.write_text("result")
    (folder / "top.txt").write_text("top")
    (folder / ".secret").write_text("hidden")
    (folder / ".attributes").write_text("[.]\nstate = Completed\nrun = -\ncollected = False\nowner_note = retained\n")
    empty = experiments / "empty"
    empty.mkdir()
    trap = experiments / "trap"
    trap.mkdir()
    (trap / "escape").symlink_to(root.parent, target_is_directory=True)

    shadow = root / "logs" / "shadow"
    shadow.mkdir()
    (shadow / "shadow.txt").write_text("shadow")
    (experiments / ".shadows").write_text("logs:shadow\n")
    (shadow / ".shadows").write_text("experiments:\n")

    immediate = client.list_context_dir("experiments")
    assert [(entry["path"], entry["type"]) for entry in immediate] == [
        ("empty", "folder"),
        ("run_a", "folder"),
        ("shadow.txt", "file"),
        ("trap", "folder"),
    ]
    run_entry = next(entry for entry in immediate if entry["path"] == "run_a")
    assert run_entry["attributes"] == {
        "collected": False,
        "owner_note": "retained",
        "run": "-",
        "state": "Completed",
    }
    assert run_entry["mtime"] == pytest.approx(folder.stat().st_mtime)
    assert client.list_context_dir("experiments", "empty") == []

    # A non-recursive listing must not inspect descendants. In particular, an
    # unsafe nested symlink is irrelevant until a recursive/glob walk asks the
    # server to enter this directory.
    (trap / "escape").unlink()
    trap.rmdir()

    recursive = client.list_context_dir("experiments", recursive=True)
    assert [entry["path"] for entry in recursive] == [
        "run_a/nested/result.xml",
        "run_a/top.txt",
        "shadow.txt",
    ]
    globbed = client.list_context_dir("experiments", pattern="run_a/*/*.xml")
    assert [entry["path"] for entry in globbed] == ["run_a/nested/result.xml"]

    machine = Machine(client.host, server_port=client.port)
    verbose = machine.list_files("experiments:run_a", verbose=True)
    assert [entry["path"] for entry in verbose] == ["experiments:run_a/nested", "experiments:run_a/top.txt"]
    top = next(entry for entry in verbose if entry["path"].endswith("top.txt"))
    assert top["mtime"].timestamp() == pytest.approx((folder / "top.txt").stat().st_mtime)


def test_directory_listing_rejects_traversal_and_preserves_missing_directory(server):
    client, _root = server
    with pytest.raises(ServerError) as traversal:
        client._json("/api/v1/directories/experiments/%2E%2E?pattern=*&recursive=false")
    assert traversal.value.status in {403, 404}
    with pytest.raises(FileNotFoundError):
        client.list_context_dir("experiments", "missing")
    with pytest.raises(ServerError) as unknown:
        client.list_context_dir("unsupported")
    assert unknown.value.code == "unknown_context"


def test_fallback_false_and_unknown_write_never_open_scpi(monkeypatch):
    class FakeServer:
        def __init__(self, write_error):
            self.write_error = write_error

        def capabilities(self):
            return {
                "api_version": "v1",
                "resources": ["files"],
                "controls": True,
                "file_writes": True,
            }

        def get_abs_file(self, _path):
            raise ServerError("read failed", outcome="not_started")

        def put_abs_file(self, _path, _data):
            raise self.write_error

    machine = Machine("instrument", server_port=7500)
    fake = FakeServer(ServerOutcomeUnknown("uncertain", "/state"))
    monkeypatch.setattr(type(machine), "server", property(lambda _self: fake))
    monkeypatch.setattr(
        machine,
        "_direct_connection",
        lambda: (_ for _ in ()).throw(AssertionError("SCPI fallback was forbidden")),
    )
    with pytest.raises(ServerError, match="read failed"):
        machine.read_file("logs:value", fallback=False)
    with pytest.raises(ServerOutcomeUnknown):
        machine.write_file("logs:value", b"data")


def test_fallback_false_surfaces_capability_http_failure(monkeypatch):
    class FakeServer:
        def capabilities(self):
            raise ServerError("capability request failed")

    machine = Machine("instrument", server_port=7500)
    monkeypatch.setattr(type(machine), "server", property(lambda _self: FakeServer()))
    monkeypatch.setattr(
        machine,
        "_direct_connection",
        lambda: (_ for _ in ()).throw(AssertionError("SCPI fallback was forbidden")),
    )
    with pytest.raises(ServerError, match="capability request failed"):
        machine.read_file("logs:value", fallback=False)


@pytest.mark.parametrize("status", [401, 403])
def test_cached_capabilities_cannot_hide_resource_authorization_failure(monkeypatch, status):
    client = ServerClient("instrument")
    client._capabilities = {
        "api_version": "v1",
        "resources": ["instrument", "files"],
        "controls": True,
        "file_writes": True,
    }

    def denied(*_args, **_kwargs):
        raise ServerError("credentials were revoked", status=status, code="unauthorized", outcome="not_started")

    monkeypatch.setattr(client, "instrument_status", denied)
    monkeypatch.setattr(client, "put_abs_file", denied)
    machine = Machine("instrument", server_port=7500)
    machine._server = client
    monkeypatch.setattr(
        machine,
        "_direct_connection",
        lambda: (_ for _ in ()).throw(AssertionError("authorization failure must not open SCPI")),
    )

    with pytest.raises(ServerError, match="credentials were revoked") as read_error:
        machine.machine_status()
    assert read_error.value.status == status

    # (Text below generated by LLM)
    # Even an explicitly not-started write must not bypass a fresh policy
    # rejection merely because the capability document was cached earlier.
    with pytest.raises(ServerError, match="credentials were revoked") as write_error:
        machine.write_file("logs:value", b"data")
    assert write_error.value.status == status


@pytest.mark.parametrize("status", [401, 403])
def test_cached_capabilities_protocol_authorization_failure_never_falls_back(monkeypatch, status):
    client = ServerClient("instrument")
    client._capabilities = {
        "api_version": "v1",
        "resources": ["runs"],
        "controls": True,
    }
    monkeypatch.setattr(client, "current_run", lambda: {"name": "active", "state": "running"})

    def denied():
        raise ServerError("protocol access denied", status=status, code="forbidden")

    monkeypatch.setattr(client, "get_running_protocol", denied)
    machine = Machine("instrument", server_port=7500)
    machine._server = client
    monkeypatch.setattr(
        machine,
        "_direct_connection",
        lambda: (_ for _ in ()).throw(AssertionError("authorization failure must not open SCPI")),
    )

    with pytest.raises(ServerError, match="protocol access denied") as error:
        machine.get_running_protocol()
    assert error.value.status == status


def test_write_falls_back_only_for_known_not_started(monkeypatch):
    class FakeServer:
        def capabilities(self):
            return {
                "api_version": "v1",
                "resources": ["files"],
                "controls": True,
                "file_writes": True,
            }

        def put_abs_file(self, _path, _data):
            raise ServerError("not submitted", outcome="not_started")

    machine = Machine("instrument", server_port=7500)
    monkeypatch.setattr(type(machine), "server", property(lambda _self: FakeServer()))
    original_server_for = machine._server_for
    direct_active = False

    def select_server(*args, **kwargs):
        return None if direct_active else original_server_for(*args, **kwargs)

    @contextmanager
    def direct_context(*_args, **_kwargs):
        nonlocal direct_active
        direct_active = True
        try:
            yield machine
        finally:
            direct_active = False

    submitted: list[bytes] = []
    monkeypatch.setattr(machine, "_server_for", select_server)
    monkeypatch.setattr(machine, "ensured_connection", direct_context)
    monkeypatch.setattr(machine, "run_command_bytes", submitted.append)
    machine.write_file("logs:value", b"data")
    assert len(submitted) == 1
    assert submitted[0].startswith(b"FILE:WRITE logs:value")

    submitted.clear()
    with pytest.raises(ServerError, match="not submitted"):
        machine.write_file("logs:value", b"data", fallback=False)
    assert submitted == []


def test_context_with_existing_colon_is_normalized_once(monkeypatch):
    paths: list[str] = []

    class FakeServer:
        def capabilities(self):
            return {"api_version": "v1", "resources": ["files"], "controls": True}

        def get_abs_file(self, path):
            paths.append(path)
            return b"value"

    machine = Machine("instrument", server_port=7500)
    monkeypatch.setattr(type(machine), "server", property(lambda _self: FakeServer()))
    assert machine.read_file("value", context="public_run_complete:") == b"value"
    assert paths == ["/sdcard/public_run_complete/value"]


def test_nonverbose_recursive_listing_matches_direct_contract(monkeypatch):
    class FakeServer:
        def capabilities(self):
            return {"api_version": "v1", "resources": ["files"], "controls": True}

    machine = Machine("instrument", server_port=7500)
    monkeypatch.setattr(type(machine), "server", property(lambda _self: FakeServer()))
    with pytest.raises(NotImplementedError):
        machine.list_files("experiments:", recursive=True)


def test_key_result_and_restart_operation_are_validated(monkeypatch):
    class FakeServer:
        def __init__(self):
            self.fail_restart = False

        def capabilities(self):
            return {"api_version": "v1", "resources": ["instrument"], "controls": True}

        def generate_access_key(self):
            return {"id": "key"}

        def restart_instrument(self):
            return {"id": "restart"}

        def wait_operation(self, operation, timeout=120.0):
            if operation["id"] == "restart":
                raise ServerError("restart failed", code="instrument_rejected", outcome="not_started")
            return {"state": "succeeded", "result": {}}

    fake = FakeServer()
    machine = Machine("instrument", server_port=7500)
    monkeypatch.setattr(type(machine), "server", property(lambda _self: fake))
    monkeypatch.setattr(
        machine,
        "_direct_connection",
        lambda: (_ for _ in ()).throw(AssertionError("failed records must not fall back")),
    )
    with pytest.raises(ServerError, match="omitted key"):
        machine.generate_random_key()
    with pytest.raises(ServerError, match="restart failed"):
        machine.restart_system()


def test_compile_completed_conflict_uses_direct_exception(monkeypatch):
    class FakeServer:
        def capabilities(self):
            return {"api_version": "v1", "resources": ["runs"], "controls": True}

        def run_action(self, _name, _action):
            return {"id": "compile"}

        def wait_operation(self, _operation, timeout=120.0):
            raise ServerError("exists", code="completed_exists", outcome="not_started")

    machine = Machine("instrument", server_port=7500)
    monkeypatch.setattr(type(machine), "server", property(lambda _self: FakeServer()))
    with pytest.raises(AlreadyExistsCompleteError):
        machine.compile_eds("completed")


def test_action_race_translates_not_running(monkeypatch):
    status = {
        "name": "server-run",
        "stage_name": "1",
        "num_stages": 1,
        "cycle": 1,
        "num_cycles": 1,
        "step": 1,
        "point": 1,
        "state": "Running",
    }

    class FakeServer:
        def capabilities(self):
            return {"api_version": "v1", "resources": ["runs"], "controls": True}

        def current_run(self):
            return status

        def run_action(self, _name, _action):
            return {"id": "pause"}

        def wait_operation(self, _operation, timeout=120.0):
            raise ServerError(
                "run disappeared",
                code="not_running",
                outcome="not_started",
                details={"current": {**status, "name": "-", "state": "Idle"}},
            )

    machine = Machine("instrument", server_port=7500)
    monkeypatch.setattr(type(machine), "server", property(lambda _self: FakeServer()))
    with pytest.raises(NotRunningError):
        Experiment("server-run").pause_now(machine)


class _ExperimentServer:
    def __init__(self):
        self.preflight_error: ServerError | None = None
        self.stage_error: ServerError | None = None
        self.wait_error: ServerError | None = None
        self.deleted: list[tuple[str, str]] = []
        self.staged = 0

    def capabilities(self):
        return {
            "api_version": "v1",
            "resources": ["experiments", "runs"],
            "controls": True,
            "file_writes": True,
        }

    def preflight_run(self, _name, *, overwrite=False):
        if self.preflight_error:
            raise self.preflight_error

    def stage_package(self, _name, _package):
        self.staged += 1
        if self.stage_error:
            raise self.stage_error
        return '"etag"'

    def start_run(self, *_args, **_kwargs):
        return {"id": "operation"}

    def wait_operation(self, _operation, timeout=120.0):
        if self.wait_error:
            raise self.wait_error
        return {"state": "succeeded", "result": {}}

    def delete_staged_package(self, name, etag):
        self.deleted.append((name, etag))


@pytest.mark.parametrize(
    "code, exception",
    [
        ("machine_busy", MachineBusyError),
        ("working_exists", AlreadyExistsWorkingError),
        ("completed_exists", AlreadyExistsCompleteError),
    ],
)
def test_run_preflight_translates_without_staging(monkeypatch, code, exception):
    fake = _ExperimentServer()
    details = {
        "current": {
            "name": "other",
            "stage_name": "1",
            "num_stages": 1,
            "cycle": 1,
            "num_cycles": 1,
            "step": 1,
            "point": 1,
            "state": "Running",
        }
    }
    fake.preflight_error = ServerError("preflight", code=code, outcome="not_started", details=details)
    machine = Machine("instrument", server_port=7500)
    monkeypatch.setattr(type(machine), "server", property(lambda _self: fake))
    monkeypatch.setattr(
        machine,
        "_direct_connection",
        lambda: (_ for _ in ()).throw(AssertionError("known preflight errors must not use SCPI")),
    )
    experiment = Experiment("server_run")
    with pytest.raises(exception):
        experiment.run(machine)
    assert experiment.runstate == "INIT"
    assert fake.staged == 0


@pytest.mark.parametrize("status", [401, 403])
def test_run_preflight_authorization_failure_does_not_fall_back_to_scpi(monkeypatch, status):
    fake = _ExperimentServer()
    fake.preflight_error = ServerError(
        "run authorization revoked",
        status=status,
        code="unauthorized",
        outcome="not_started",
    )
    machine = Machine("instrument", server_port=7500)
    monkeypatch.setattr(type(machine), "server", property(lambda _self: fake))
    monkeypatch.setattr(
        machine,
        "_direct_connection",
        lambda: (_ for _ in ()).throw(AssertionError("authorization failure must not open SCPI")),
    )

    experiment = Experiment("server_run")
    with pytest.raises(ServerError, match="run authorization revoked") as error:
        experiment.run(machine)
    assert error.value.status == status
    assert experiment.runstate == "INIT"
    assert fake.staged == 0


def test_run_known_failure_restores_state_and_discards_stage(monkeypatch):
    fake = _ExperimentServer()
    fake.wait_error = ServerError("conflict", code="working_exists", outcome="not_started")
    machine = Machine("instrument", server_port=7500)
    monkeypatch.setattr(type(machine), "server", property(lambda _self: fake))
    experiment = Experiment("server_run")
    with pytest.raises(AlreadyExistsWorkingError):
        experiment.run(machine)
    assert experiment.runstate == "INIT"
    assert experiment.runstarttime is None
    assert fake.deleted == [("server_run", '"etag"')]


def test_run_unknown_start_keeps_running_state_and_stage(monkeypatch):
    fake = _ExperimentServer()
    fake.wait_error = ServerOutcomeUnknown("uncertain", "/api/v1/operations/operation")
    machine = Machine("instrument", server_port=7500)
    monkeypatch.setattr(type(machine), "server", property(lambda _self: fake))
    experiment = Experiment("server_run")
    with pytest.raises(ServerOutcomeUnknown):
        experiment.run(machine)
    assert experiment.runstate == "RUNNING"
    assert experiment.runstarttime is not None
    assert fake.deleted == []


def test_run_success_discards_staged_package(monkeypatch):
    fake = _ExperimentServer()
    machine = Machine("instrument", server_port=7500)
    monkeypatch.setattr(type(machine), "server", property(lambda _self: fake))
    experiment = Experiment("server_run")
    experiment.run(machine)
    assert experiment.runstate == "RUNNING"
    assert fake.deleted == [("server_run", '"etag"')]


def test_staging_transport_failure_restores_before_direct_fallback(monkeypatch):
    fake = _ExperimentServer()
    fake.stage_error = ServerOutcomeUnknown("upload uncertain", "/experiment")
    machine = Machine("instrument", server_port=7500)
    monkeypatch.setattr(type(machine), "server", property(lambda _self: fake))
    observed: list[str] = []

    def stop_before_direct(*_args, **_kwargs):
        observed.append(experiment.runstate)
        raise RuntimeError("direct fallback reached")

    monkeypatch.setattr(machine, "ensured_connection", stop_before_direct)
    experiment = Experiment("server_run")
    with pytest.raises(RuntimeError, match="direct fallback reached"):
        experiment.run(machine)
    assert observed == ["INIT"]
    assert experiment.runstarttime is None
