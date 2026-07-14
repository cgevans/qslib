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


def test_deploy_binary_chunk_roundtrip(monkeypatch):
    """_deploy_binary chunks (gzip+base64) must be 4-aligned and reassemble to
    the original binary, with on-device verification passing. Simulates the
    instrument's shell without a real machine."""
    import base64
    import gzip
    import hashlib
    import os
    import re
    import shlex

    from qslib.machine import Machine

    files: dict[str, bytes] = {}

    def fake_run(self, cmd):
        assert cmd.startswith('SYST:EXEC "') and cmd.endswith('"'), cmd
        inner = cmd[len('SYST:EXEC "') : -1]
        if inner.startswith("rm -f "):
            for p in shlex.split(inner[len("rm -f ") :]):
                files.pop(p, None)
        elif inner.startswith("echo -n "):
            b64, rest = inner[len("echo -n ") :].split(" | ", 1)
            path = shlex.split(rest)[-1]
            # b64decode raises if the chunk is not 4-aligned -> catches bad chunking
            files[path] = files.get(path, b"") + base64.b64decode(b64)
        elif inner.startswith("gunzip -f "):
            p = shlex.split(inner[len("gunzip -f ") :])[0]
            files[p[:-3]] = gzip.decompress(files.pop(p))
        elif inner.startswith("("):
            mo = re.search(r"md5sum (\S+) \) > (\S+)", inner)
            q, check = mo.group(1), mo.group(2)
            body = files[q]
            files[check] = f"{len(body)} {q}\n{hashlib.md5(body).hexdigest()}  {q}\n".encode()
        return ""

    def fake_read(self, path, *a, **k):
        for key, val in files.items():
            if key.endswith(path.lstrip("/")):
                return val
        raise KeyError(path)

    monkeypatch.setattr(Machine, "run_command", fake_run)
    monkeypatch.setattr(Machine, "read_file", fake_read)

    m = Machine("127.0.0.1", agent_port=1)
    data = os.urandom(150_003)  # not a multiple of the chunk size
    m._deploy_binary("/data/qslib-server", data, file_root="/froot", chunk_chars=40000)
    assert files["/data/qslib-server"] == data


def test_deploy_binary_detects_corruption(monkeypatch):
    """If the device file does not match, _deploy_binary raises."""
    import os

    from qslib.agent import AgentError
    from qslib.machine import Machine

    def fake_run(self, cmd):
        return ""

    def fake_read(self, path, *a, **k):
        return b"999 /data/qslib-server\ndeadbeef" + b"0" * 24 + b"  /data/qslib-server\n"

    monkeypatch.setattr(Machine, "run_command", fake_run)
    monkeypatch.setattr(Machine, "read_file", fake_read)
    m = Machine("127.0.0.1", agent_port=1)
    with pytest.raises(AgentError):
        m._deploy_binary("/data/qslib-server", os.urandom(1000), file_root="/froot")


def test_ensure_agent_rejects_unsafe_exec_values():
    """ensure_agent must reject values that would break out of the SCPI
    SYST:EXEC string or trigger SCPI/shell substitution."""
    from qslib.machine import Machine

    m = Machine("127.0.0.1", agent_port=_free_port())
    with pytest.raises(ValueError):
        m.ensure_agent(binary=b"stub", listen="1.2.3.4:7500", remote_path='/data/x"; rm -rf /')
    with pytest.raises(ValueError):
        m.ensure_agent(binary=b"stub", listen="$(reboot)", remote_path="/data/qslib-server")
    with pytest.raises(ValueError):
        m.ensure_agent(
            binary=b"stub",
            listen="1.2.3.4:7500",
            remote_path="/data/qslib-server",
            extra_args=("--log", "`touch /tmp/pwned`"),
        )


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
