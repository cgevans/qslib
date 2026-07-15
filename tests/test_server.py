# SPDX-FileCopyrightText: 2024 - 2026 Constantine Evans <qslib@mb.costi.net>
# SPDX-License-Identifier: EUPL-1.2

"""Contract tests for the optional v1 semantic client and Python dispatch."""

from __future__ import annotations

import hashlib
import socket
import socketserver
import subprocess
import threading
import time
import urllib.error
from pathlib import Path

import pytest

from qslib import Experiment, Machine
from qslib.server import ServerClient, ServerError, ServerOutcomeUnknown, ServerUnavailable

BINARY = Path(__file__).parent.parent / "target" / "debug" / "qslib-server"


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


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
        pytest.skip(f"server binary not built ({BINARY})")
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
    assert capabilities["raw_scpi"] is False
    status = client.instrument_status()
    assert status["zone_count"] == 6
    assert status["run"]["state"] == "idle"
    assert status["run"]["remaining_time_s"] is None


def test_named_file_resources_and_range(server, tmp_path: Path):
    client, root = server
    data = b"0123456789" * 100
    (root / "logs" / "messages.log").write_bytes(data)
    assert client.get_file("messages.log", context="logs") == data
    assert client.get_file("messages.log", context="logs", range_start=995) == data[995:]

    client.put_file("nested/value.bin", b"value", context="experiments")
    assert (root / "experiments" / "nested" / "value.bin").read_bytes() == b"value"
    entries = client.list_context_dir("experiments", "")
    assert entries[0]["path"] == "nested/value.bin"

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


def test_mutation_transport_failure_is_outcome_unknown(monkeypatch):
    client = ServerClient("127.0.0.1", port=1)

    def fail(*_args, **_kwargs):
        raise urllib.error.URLError("dropped")

    monkeypatch.setattr("urllib.request.urlopen", fail)
    with pytest.raises(ServerOutcomeUnknown) as error:
        client.set_power(True)
    assert error.value.state_query == "/api/v1/instrument/status"


def test_connection_refusal_is_known_not_submitted():
    port = _free_port()
    client = ServerClient("127.0.0.1", port=port, timeout=0.2)
    with pytest.raises(ServerUnavailable) as error:
        client.set_power(True)
    assert error.value.outcome == "not_started"


def test_machine_semantic_status_opens_no_scpi(monkeypatch):
    class FakeServer:
        def capabilities(self):
            return {"api_version": "v1", "resources": ["instrument"], "controls": True}

        def instrument_status(self):
            return {
                "drawer": "closed",
                "cover": "down",
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
        lambda: (_ for _ in ()).throw(AssertionError("semantic success must not open SCPI")),
    )
    assert machine.machine_status().drawer == "Closed"
    assert machine.get_zone_count() == 6


def test_experiment_action_uses_server_without_opening_scpi(monkeypatch):
    actions: list[tuple[str, str]] = []

    class FakeServer:
        def capabilities(self):
            return {"api_version": "v1", "resources": ["runs"], "controls": True}

        def current_run(self):
            return {
                "name": "semantic-run",
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
    Experiment("semantic-run").pause_now(machine)
    assert actions == [("semantic-run", "pause")]


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


def test_ensure_server_rejects_unsafe_exec_values():
    machine = Machine("127.0.0.1", server_port=_free_port())
    with pytest.raises(ValueError, match="unsafe"):
        machine.ensure_server(
            binary=b"stub",
            listen="1.2.3.4:7500",
            remote_path='/data/x";reboot',
        )
