# SPDX-FileCopyrightText: 2024 - 2026 Constantine Evans <qslib@mb.costi.net>
#
# SPDX-License-Identifier: EUPL-1.2

"""Tests for the AgentClient against the real qslib-server binary.

These launch the compiled ``qslib-server`` agent (x86 debug build) over
loopback and exercise the HTTP client. The tests skip if the binary has not
been built (``cargo build -p qslib-server``).
"""

from __future__ import annotations

import socket
import subprocess
import time
from pathlib import Path

import pytest

from qslib.agent import AgentClient, AgentError

BINARY = Path(__file__).parent.parent / "target" / "debug" / "qslib-server"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_ready(client: AgentClient, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            client.health()
            return
        except AgentError:
            time.sleep(0.05)
    raise TimeoutError("agent did not become ready")


@pytest.fixture
def agent(tmp_path):
    """Start a qslib-server agent serving tmp_path, no auth."""
    if not BINARY.exists():
        pytest.skip(f"agent binary not built ({BINARY}); run `cargo build -p qslib-server`")
    port = _free_port()
    scpi_port = _free_port()  # nothing listening -> scpi_ok False, but /file works
    proc = subprocess.Popen(
        [
            str(BINARY),
            "--listen",
            f"127.0.0.1:{port}",
            "--file-root",
            str(tmp_path),
            "--scpi-target",
            f"127.0.0.1:{scpi_port}",
            "--no-auth",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    client = AgentClient("127.0.0.1", port=port)
    try:
        _wait_ready(client)
        yield client, tmp_path
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def test_health(agent):
    client, _ = agent
    h = client.health()
    assert h["name"] == "qslib-server"
    assert "version" in h
    assert h["scpi_ok"] is False  # no SCPI server behind it


def test_get_file_bytes(agent):
    client, root = agent
    data = b"hello agent " * 5000
    (root / "sub").mkdir()
    (root / "sub" / "data.bin").write_bytes(data)
    assert client.get_file("sub/data.bin") == data


def test_get_file_to_dest(agent, tmp_path):
    client, root = agent
    data = bytes(range(256)) * 100
    (root / "d.bin").write_bytes(data)
    dest = tmp_path / "out.bin"
    assert client.get_file("d.bin", dest=dest) is None
    assert dest.read_bytes() == data


def test_get_file_missing(agent):
    client, _ = agent
    with pytest.raises(AgentError) as exc:
        client.get_file("nope.bin")
    assert exc.value.status == 404


def test_get_file_traversal_blocked(agent):
    client, _ = agent
    with pytest.raises(AgentError) as exc:
        client.get_file("../../etc/passwd")
    assert exc.value.status in (403, 404)


def test_auth_required(tmp_path):
    if not BINARY.exists():
        pytest.skip("agent binary not built")
    port = _free_port()
    scpi_port = _free_port()
    (tmp_path / "a.bin").write_bytes(b"x")
    proc = subprocess.Popen(
        [
            str(BINARY),
            "--listen",
            f"127.0.0.1:{port}",
            "--file-root",
            str(tmp_path),
            "--scpi-target",
            f"127.0.0.1:{scpi_port}",
            "--token",
            "sekret",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        good = AgentClient("127.0.0.1", port=port, token="sekret")
        _wait_ready(good)
        assert good.get_file("a.bin") == b"x"

        bad = AgentClient("127.0.0.1", port=port, token="wrong")
        with pytest.raises(AgentError) as exc:
            bad.get_file("a.bin")
        assert exc.value.status == 401

        none = AgentClient("127.0.0.1", port=port)
        with pytest.raises(AgentError) as exc:
            none.health()
        assert exc.value.status == 401
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
