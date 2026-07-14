# SPDX-FileCopyrightText: 2024 - 2026 Constantine Evans <qslib@mb.costi.net>
#
# SPDX-License-Identifier: EUPL-1.2

"""Tests for the ServerClient against the real qslib-server binary.

These launch the compiled ``qslib-server`` binary (x86 debug build) over
loopback and exercise the HTTP client. The tests skip if the binary has not
been built (``cargo build -p qslib-server``).
"""

from __future__ import annotations

import os
import socket
import subprocess
import time
from pathlib import Path

import pytest

from qslib.server import ServerClient, ServerError

BINARY = Path(__file__).parent.parent / "target" / "debug" / "qslib-server"


def _zipread_files(run_dir: Path) -> dict[str, bytes]:
    """The file set (relpath -> content) that InstrumentServer ``EXP:ZIPREAD?``
    would produce for ``run_dir``: ``os.walk(followlinks=False)`` filenames,
    read following symlinks (as ``zipfile.write`` does), arcnames relative to
    the run dir. This is the ground truth ``download_dir`` must reproduce."""
    out: dict[str, bytes] = {}
    for folder, _dirs, files in os.walk(run_dir, followlinks=False):
        for fn in files:
            ap = Path(folder) / fn
            out[ap.relative_to(run_dir).as_posix()] = ap.read_bytes()
    return out


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_ready(client: ServerClient, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            client.health()
            return
        except ServerError:
            time.sleep(0.05)
    raise TimeoutError("server did not become ready")


@pytest.fixture
def server(tmp_path):
    """Start qslib-server serving tmp_path, no auth."""
    if not BINARY.exists():
        pytest.skip(f"server binary not built ({BINARY}); run `cargo build -p qslib-server`")
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
    client = ServerClient("127.0.0.1", port=port)
    try:
        _wait_ready(client)
        yield client, tmp_path
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def test_health(server):
    client, root = server
    h = client.health()
    assert h["name"] == "qslib-server"
    assert "version" in h
    assert h["scpi_ok"] is False  # no SCPI server behind it
    # file_root is reported (canonicalized) so clients can serve by abs path.
    assert Path(h["file_root"]).resolve() == Path(root).resolve()
    assert Path(client.file_root).resolve() == Path(root).resolve()


def test_get_file_bytes(server):
    client, root = server
    data = b"hello server " * 5000
    (root / "sub").mkdir()
    (root / "sub" / "data.bin").write_bytes(data)
    assert client.get_file("sub/data.bin") == data


def test_get_abs_file(server):
    """get_abs_file fetches by absolute path, made relative to file_root."""
    client, root = server
    data = b"abs" * 4000
    (root / "sub").mkdir()
    (root / "sub" / "a.bin").write_bytes(data)
    # An absolute path under the (canonical) file_root must resolve and download.
    assert client.get_abs_file(str(Path(client.file_root) / "sub" / "a.bin")) == data
    # A path outside the root raises (-> caller falls back to SCPI).
    with pytest.raises(ServerError):
        client.get_abs_file("/etc/passwd")


def _make_run_tree(root: Path) -> Path:
    """A synthetic run directory covering the edge cases that distinguish the
    ZIPREAD walk from a plain listing: nested dirs, a dotfile, a symlink to a
    file (included), and a symlink to a directory (not descended)."""
    run = root / "run1"
    (run / "apldbio" / "sds").mkdir(parents=True)
    (run / "apldbio" / "sds" / "a.xml").write_bytes(b"A" * 1000)
    (run / "apldbio" / "sds" / ".hidden").write_bytes(b"dotfile")
    (run / "top.txt").write_bytes(b"T" * 50)
    (run / "sub2").mkdir()
    (run / "sub2" / "b.bin").write_bytes(bytes(range(256)) * 10)
    (run / "link_to_a").symlink_to(run / "apldbio" / "sds" / "a.xml")
    (run / "link_to_sub2").symlink_to(run / "sub2")
    return run


def test_list_dir_matches_zipread_file_set(server):
    """/list enumerates exactly the files EXP:ZIPREAD? would zip: dotfiles in,
    symlinked file in, symlinked directory not descended."""
    client, root = server
    run = _make_run_tree(root)
    abspath = str(Path(client.file_root) / "run1")

    listed = {e["path"] for e in client.list_dir(abspath)}
    assert listed == set(_zipread_files(run))
    assert "apldbio/sds/.hidden" in listed  # dotfile included
    assert "link_to_a" in listed  # symlink-to-file included
    assert not any(p.startswith("link_to_sub2") for p in listed)  # symlink-to-dir not descended


def test_download_dir_reproduces_zipread_tree(server, tmp_path):
    """download_dir writes the same tree, byte-for-byte, that extracting the
    ZIPREAD zip would produce."""
    client, root = server
    run = _make_run_tree(root)
    abspath = str(Path(client.file_root) / "run1")

    dest = tmp_path / "out"
    n = client.download_dir(abspath, dest)

    expected = _zipread_files(run)
    assert n == len(expected)
    got = {f.relative_to(dest).as_posix(): f.read_bytes() for f in dest.rglob("*") if f.is_file()}
    assert got == expected


def test_health_on_dead_port_raises_servererror():
    """A connection reset/refusal (server down) surfaces as ServerError, not a
    raw OSError, so available()/ensure_server degrade gracefully."""
    client = ServerClient("127.0.0.1", port=_free_port(), timeout=2)
    with pytest.raises(ServerError):
        client.health()
    assert client.available() is False


def test_list_dir_missing_is_404(server):
    client, root = server
    with pytest.raises(ServerError) as exc:
        client.list_dir(str(Path(client.file_root) / "does_not_exist"))
    assert exc.value.status == 404


def test_get_file_to_dest(server, tmp_path):
    client, root = server
    data = bytes(range(256)) * 100
    (root / "d.bin").write_bytes(data)
    dest = tmp_path / "out.bin"
    assert client.get_file("d.bin", dest=dest) is None
    assert dest.read_bytes() == data


def test_get_file_missing(server):
    client, _ = server
    with pytest.raises(ServerError) as exc:
        client.get_file("nope.bin")
    assert exc.value.status == 404


def test_get_file_traversal_blocked(server):
    client, _ = server
    with pytest.raises(ServerError) as exc:
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

    m = Machine("127.0.0.1", server_port=1)
    data = os.urandom(150_003)  # not a multiple of the chunk size
    m._deploy_binary("/data/qslib-server", data, chunk_chars=40000)
    assert files["/data/qslib-server"] == data


def test_deploy_binary_detects_corruption(monkeypatch):
    """If the device file does not match, _deploy_binary raises."""
    import os

    from qslib.server import ServerError
    from qslib.machine import Machine

    def fake_run(self, cmd):
        return ""

    def fake_read(self, path, *a, **k):
        return b"999 /data/qslib-server\ndeadbeef" + b"0" * 24 + b"  /data/qslib-server\n"

    monkeypatch.setattr(Machine, "run_command", fake_run)
    monkeypatch.setattr(Machine, "read_file", fake_read)
    m = Machine("127.0.0.1", server_port=1)
    with pytest.raises(ServerError):
        m._deploy_binary("/data/qslib-server", os.urandom(1000))


def test_ensure_server_rejects_unsafe_exec_values():
    """ensure_server must reject values that would break out of the SCPI
    SYST:EXEC string or trigger SCPI/shell substitution."""
    from qslib.machine import Machine

    m = Machine("127.0.0.1", server_port=_free_port())
    with pytest.raises(ValueError):
        m.ensure_server(binary=b"stub", listen="1.2.3.4:7500", remote_path='/data/x"; rm -rf /')
    with pytest.raises(ValueError):
        m.ensure_server(binary=b"stub", listen="$(reboot)", remote_path="/data/qslib-server")
    with pytest.raises(ValueError):
        m.ensure_server(
            binary=b"stub",
            listen="1.2.3.4:7500",
            remote_path="/data/qslib-server",
            extra_args=("--log", "`touch /tmp/pwned`"),
        )


def _machine_with_fake_server(monkeypatch, *, get_abs_file):
    """A non-connecting Machine whose ``server`` is a stub with the given
    ``get_abs_file(abspath)`` callable."""
    from qslib.machine import Machine
    from qslib import server as server_mod

    class FakeServer:
        def get_abs_file(self, abspath):
            return get_abs_file(abspath)

    m = Machine("127.0.0.1", automatic=False, server_port=7500)
    monkeypatch.setattr(type(m), "server", property(lambda self: FakeServer()))
    return m, server_mod


def test_read_file_prefers_server(monkeypatch):
    """When connected to qslib-server, read_file resolves the default FILE
    context to an absolute path and fetches it over HTTP, not SCPI."""
    m, _ = _machine_with_fake_server(monkeypatch, get_abs_file=lambda abspath: b"http:" + abspath.encode())
    m._prefer_server_files = True

    def boom(self, command):
        raise AssertionError("SCPI must not be used when qslib-server is preferred")

    monkeypatch.setattr(type(m), "run_command_to_bytes", boom)
    # default FILE context roots at /data/vendor/IS
    assert m.read_file("experiments/run/f.bin") == b"http:/data/vendor/IS/experiments/run/f.bin"


def test_read_file_public_run_complete_uses_server(monkeypatch):
    """The public_run_complete context (completed .eds, on /sdcard) resolves to
    its /sdcard absolute path and is fetched over HTTP."""
    seen = {}

    def grab(abspath):
        seen["abspath"] = abspath
        return b"eds-bytes"

    m, _ = _machine_with_fake_server(monkeypatch, get_abs_file=grab)
    m._prefer_server_files = True

    def boom(self, command):
        raise AssertionError("SCPI must not be used when qslib-server is preferred")

    monkeypatch.setattr(type(m), "run_command_to_bytes", boom)
    assert m.read_file("myrun.eds", context="public_run_complete") == b"eds-bytes"
    assert seen["abspath"] == "/sdcard/public_run_complete/myrun.eds"


def test_read_file_falls_back_to_scpi_on_server_error(monkeypatch):
    """A qslib-server error falls back to the SCPI path when fallback=True."""
    import base64

    def raising(abspath):
        from qslib.server import ServerError

        raise ServerError("boom", status=500)

    m, _ = _machine_with_fake_server(monkeypatch, get_abs_file=raising)
    m._prefer_server_files = True

    def fake_scpi(self, command):
        return b"<quote>\n" + base64.b64encode(b"scpi-data") + b"</quote>"

    monkeypatch.setattr(type(m), "run_command_to_bytes", fake_scpi)
    assert m.read_file("f.bin") == b"scpi-data"


def test_read_file_uses_scpi_when_not_connected_via_server(monkeypatch):
    """Without a qslib-server connection, read_file uses SCPI and never HTTP."""
    import base64

    def boom(abspath):
        raise AssertionError("HTTP must not be used when qslib-server is not preferred")

    m, _ = _machine_with_fake_server(monkeypatch, get_abs_file=boom)
    m._prefer_server_files = False

    monkeypatch.setattr(
        type(m), "run_command_to_bytes", lambda self, c: b"<quote>\n" + base64.b64encode(b"x") + b"</quote>"
    )
    assert m.read_file("f.bin") == b"x"


def test_read_file_unknown_context_uses_scpi(monkeypatch):
    """An unrecognised context (not in the locations map) forces SCPI even when
    qslib-server is preferred."""
    import base64

    def boom(abspath):
        raise AssertionError("HTTP path must not handle an unknown context")

    m, _ = _machine_with_fake_server(monkeypatch, get_abs_file=boom)
    m._prefer_server_files = True

    monkeypatch.setattr(
        type(m), "run_command_to_bytes", lambda self, c: b"<quote>\n" + base64.b64encode(b"y") + b"</quote>"
    )
    assert m.read_file("f.bin", context="usbdrive") == b"y"


def _machine_with_fake_server_dir(monkeypatch, *, download_dir):
    """A non-connecting Machine whose ``server.download_dir`` is a stub."""
    from qslib.machine import Machine

    class FakeServer:
        def download_dir(self, abspath, dest_dir, *a, **k):
            return download_dir(abspath, dest_dir)

    m = Machine("127.0.0.1", automatic=False, server_port=7500)
    monkeypatch.setattr(type(m), "server", property(lambda self: FakeServer()))
    return m


def test_machine_download_dir_resolves_exp_context(monkeypatch, tmp_path):
    """Machine.download_dir maps the EXP leaf to /data/vendor/IS/experiments and
    delegates to the server with the resolved absolute path."""
    seen = {}

    def grab(abspath, dest_dir):
        seen["abspath"] = abspath
        seen["dest"] = dest_dir
        return 3

    m = _machine_with_fake_server_dir(monkeypatch, download_dir=grab)
    m._prefer_server_files = True
    assert m.download_dir("run1", tmp_path, leaf="EXP") is True
    assert seen["abspath"] == "/data/vendor/IS/experiments/run1"


def test_machine_download_dir_not_preferred_returns_false(monkeypatch, tmp_path):
    def boom(abspath, dest_dir):
        raise AssertionError("must not touch server when not preferred")

    m = _machine_with_fake_server_dir(monkeypatch, download_dir=boom)
    m._prefer_server_files = False
    assert m.download_dir("run1", tmp_path, leaf="EXP") is False


def test_machine_download_dir_404_raises_filenotfound(monkeypatch, tmp_path):
    def missing(abspath, dest_dir):
        raise ServerError("nope", status=404)

    m = _machine_with_fake_server_dir(monkeypatch, download_dir=missing)
    m._prefer_server_files = True
    with pytest.raises(FileNotFoundError):
        m.download_dir("run1", tmp_path, leaf="EXP")


def test_machine_download_dir_other_error_returns_false(monkeypatch, tmp_path):
    def err(abspath, dest_dir):
        raise ServerError("boom", status=500)

    m = _machine_with_fake_server_dir(monkeypatch, download_dir=err)
    m._prefer_server_files = True
    assert m.download_dir("run1", tmp_path, leaf="EXP") is False


def test_auth_required(tmp_path):
    if not BINARY.exists():
        pytest.skip("server binary not built")
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
        good = ServerClient("127.0.0.1", port=port, token="sekret")
        _wait_ready(good)
        assert good.get_file("a.bin") == b"x"

        bad = ServerClient("127.0.0.1", port=port, token="wrong")
        with pytest.raises(ServerError) as exc:
            bad.get_file("a.bin")
        assert exc.value.status == 401

        none = ServerClient("127.0.0.1", port=port)
        with pytest.raises(ServerError) as exc:
            none.health()
        assert exc.value.status == 401
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
