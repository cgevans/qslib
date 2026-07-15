# SPDX-FileCopyrightText: 2024 - 2026 Constantine Evans <qslib@mb.costi.net>
#
# SPDX-License-Identifier: EUPL-1.2

"""Client for the on-instrument ``qslib-server`` HTTP service.

``qslib-server`` (see the ``qslib-server`` crate) runs on the instrument and
serves bulk file transfer, one-shot SCPI commands, and a SCPI tunnel over plain
HTTP on the instrument's private link. This module is a small, dependency-free
(stdlib ``urllib``) client for it.

qslib-server is an optional acceleration layer: bulk transfer through it avoids
the base64+TLS overhead of ``FILE:READ`` over SCPI. Everything degrades to the
normal SCPI path (:meth:`qslib.machine.Machine.read_file`) when qslib-server is
not running.
"""

from __future__ import annotations

import hashlib
import json
import os
import posixpath
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from http.client import HTTPException
from pathlib import Path
from typing import IO, Any, BinaryIO

__all__ = ["ServerClient", "ServerError"]


class ServerError(RuntimeError):
    """An error returned by, or while contacting, qslib-server."""

    def __init__(self, message: str, status: int | None = None, detail: str | None = None):
        super().__init__(message)
        self.status = status
        self.detail = detail


@dataclass
class ServerClient:
    """A client for a running ``qslib-server``.

    Parameters
    ----------
    host
        Host or IP of qslib-server (typically reached through the Windows-box
        forward / VPN, or directly on the private link).
    port
        qslib-server port (default 7500).
    token
        Bearer token, if qslib-server requires one.
    timeout
        Default request timeout in seconds.
    """

    host: str
    port: int = 7500
    token: str | None = None
    timeout: float = 30.0
    _file_root: str | None = field(default=None, init=False, repr=False, compare=False)
    _file_root_fetched: bool = field(default=False, init=False, repr=False, compare=False)

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    @property
    def file_root(self) -> str | None:
        """qslib-server's canonicalized ``--file-root``, from ``/health`` (cached).

        ``None`` if qslib-server is unreachable or predates the ``file_root``
        field in ``/health``.
        """
        if not self._file_root_fetched:
            try:
                self._file_root = self.health().get("file_root")
            except ServerError:
                # A transient outage must not permanently disable the fast path.
                # Leave the result uncached so a later call retries, matching the
                # Rust client which never caches a transport failure.
                return None
            self._file_root_fetched = True
        return self._file_root

    def _rel_to_root(self, abspath: str) -> str | None:
        """Return ``abspath`` made relative to :attr:`file_root`, or ``None`` if
        it is not under the root (so the caller falls back to SCPI)."""
        root = self.file_root
        if root is None:
            return None
        root = root.rstrip("/")
        ap = posixpath.normpath(abspath)
        if root == "":  # file_root is "/"
            return ap.lstrip("/")
        if ap == root:
            return ""
        if ap.startswith(root + "/"):
            return ap[len(root) + 1 :]
        return None

    def _headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        headers: dict[str, str] = {}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        if extra:
            headers.update(extra)
        return headers

    def _open(self, req: urllib.request.Request, timeout: float | None = None) -> Any:
        try:
            return urllib.request.urlopen(req, timeout=timeout or self.timeout)  # noqa: S310 (http to private link)
        except urllib.error.HTTPError as e:
            body = b""
            try:
                body = e.read()
            except Exception:
                pass
            message, detail = _parse_error_body(body, default=str(e))
            raise ServerError(message, status=e.code, detail=detail) from e
        except urllib.error.URLError as e:
            raise ServerError(f"cannot reach qslib-server at {self.base_url}: {e.reason}") from e
        except OSError as e:
            # Connection reset/refused/timeout mid-request (e.g. the server was
            # killed, or a forwarder RSTs when nothing is listening) arrives as a
            # raw OSError rather than a URLError. Treat it as "unavailable" so
            # health()/available()/ensure_server degrade gracefully.
            raise ServerError(f"cannot reach qslib-server at {self.base_url}: {e}") from e

    # -- endpoints ---------------------------------------------------------

    def health(self) -> dict[str, Any]:
        """Return qslib-server's ``/health`` document."""
        req = urllib.request.Request(f"{self.base_url}/health", headers=self._headers(), method="GET")
        with self._open(req) as resp:
            raw = resp.read()
        try:
            return json.loads(raw.decode())
        except (ValueError, UnicodeDecodeError) as e:
            # Not qslib-server (or a proxy error page): surface as ServerError so
            # available()/ensure_server degrade gracefully rather than raising.
            raise ServerError("qslib-server /health returned a non-JSON body") from e

    def available(self) -> bool:
        """Return True if qslib-server responds to ``/health`` (and SCPI is up)."""
        try:
            h = self.health()
        except ServerError:
            return False
        return bool(h.get("scpi_ok", False))

    def get_file(
        self,
        path: str,
        dest: str | Path | BinaryIO | None = None,
        chunk_size: int = 1 << 20,
    ) -> bytes | None:
        """Fetch a file under qslib-server's file root.

        Parameters
        ----------
        path
            Path relative to qslib-server's configured ``--file-root``.
        dest
            If given, stream the file to this path or open binary file object
            and return ``None``. If ``None``, return the file contents as bytes.
        chunk_size
            Streaming chunk size in bytes.
        """
        quoted = urllib.parse.quote(path.lstrip("/"))
        req = urllib.request.Request(f"{self.base_url}/file/{quoted}", headers=self._headers(), method="GET")
        temp_path: Path | None = None
        try:
            with self._open(req) as resp:
                if dest is None:
                    data = resp.read()
                    _validate_response_size(resp, len(data))
                    return data
                if isinstance(dest, (str, Path)):
                    final_path = Path(dest)
                    with tempfile.NamedTemporaryFile(
                        mode="wb",
                        dir=final_path.parent,
                        prefix=f".{final_path.name}.download.",
                        delete=False,
                    ) as f:
                        temp_path = Path(f.name)
                        _copy_response(resp, f, chunk_size)
                    os.replace(temp_path, final_path)
                    temp_path = None
                else:
                    out: IO[bytes] = dest
                    _copy_response(resp, out, chunk_size)
                return None
        except ServerError:
            raise
        except (OSError, HTTPException) as e:
            raise ServerError(f"qslib-server file transfer failed for {path!r}: {e}") from e
        finally:
            if temp_path is not None:
                temp_path.unlink(missing_ok=True)

    def get_abs_file(
        self,
        abspath: str,
        dest: str | Path | BinaryIO | None = None,
        chunk_size: int = 1 << 20,
    ) -> bytes | None:
        """Fetch a file by its absolute on-instrument path.

        The path is made relative to qslib-server's :attr:`file_root` and served
        via :meth:`get_file`. Raises :class:`ServerError` if the path is not
        under the root (so callers fall back to SCPI).
        """
        rel = self._rel_to_root(abspath)
        if rel is None:
            raise ServerError(f"{abspath!r} is not under qslib-server file root {self.file_root!r}")
        return self.get_file(rel, dest=dest, chunk_size=chunk_size)

    def put_file(self, path: str, data: bytes) -> None:
        """Write ``data`` to a file under qslib-server's file root (``PUT /file``).

        The file is written atomically on the instrument (temp file + rename),
        creating parent directories as needed. Raises :class:`ServerError` if the
        server is read-only (403) or the path is unsafe.
        """
        quoted = urllib.parse.quote(path.lstrip("/"))
        req = urllib.request.Request(
            f"{self.base_url}/file/{quoted}",
            data=data,
            headers=self._headers({"Content-Type": "application/octet-stream"}),
            method="PUT",
        )
        try:
            with self._open(req) as resp:
                resp.read()
        except ServerError:
            raise
        except (OSError, HTTPException) as e:
            raise ServerError(f"qslib-server file upload failed for {path!r}: {e}") from e

    def put_abs_file(self, abspath: str, data: bytes) -> None:
        """Write a file by its absolute on-instrument path.

        The path is made relative to qslib-server's :attr:`file_root` and written
        via :meth:`put_file`. Raises :class:`ServerError` if the path is not
        under the root (so callers fall back to SCPI).
        """
        rel = self._rel_to_root(abspath)
        if rel is None:
            raise ServerError(f"{abspath!r} is not under qslib-server file root {self.file_root!r}")
        self.put_file(rel, data)

    def list_dir(self, abspath: str) -> list[dict[str, Any]]:
        """Return the recursive file manifest of a directory (``GET /list``).

        Each entry is ``{"path": <relative>, "size": <bytes>}``, where ``path``
        is relative to ``abspath`` (forward-slash separated). The manifest
        matches the InstrumentServer ``EXP:ZIPREAD?`` file set (dotfiles
        included, symlinked directories not descended). Raises
        :class:`ServerError` (status 404) if the directory does not exist, or if
        ``abspath`` is not under qslib-server's :attr:`file_root`.
        """
        rel = self._rel_to_root(abspath)
        if rel is None:
            raise ServerError(f"{abspath!r} is not under qslib-server file root {self.file_root!r}")
        quoted = urllib.parse.quote(rel.lstrip("/"))
        req = urllib.request.Request(f"{self.base_url}/list/{quoted}", headers=self._headers(), method="GET")
        with self._open(req) as resp:
            raw = resp.read()
        try:
            return json.loads(raw.decode()).get("files", [])
        except (ValueError, UnicodeDecodeError) as e:
            raise ServerError("qslib-server /list returned a non-JSON body") from e

    def download_dir(self, abspath: str, dest_dir: str | Path, chunk_size: int = 1 << 20) -> int:
        """Download a directory tree rooted at ``abspath`` into ``dest_dir``.

        Enumerates the directory via :meth:`list_dir` and fetches each file raw
        (no compression), preserving the relative structure under ``dest_dir``.
        Returns the number of files written.

        Raises :class:`FileNotFoundError` if the directory itself does not exist
        (a 404 on the listing), and :class:`ServerError` (or a transport error
        such as :class:`OSError`) on any other failure, so callers can tell a
        genuinely missing directory from a transfer failure and fall back to
        SCPI accordingly. A per-file 404 (a file removed between the listing and
        its fetch) surfaces as :class:`ServerError`, not ``FileNotFoundError``.
        """
        dest = Path(dest_dir)
        try:
            manifest = self.list_dir(abspath)
        except ServerError as e:
            if e.status == 404:
                raise FileNotFoundError(abspath) from e
            raise
        n = 0
        for entry in manifest:
            rel = entry["path"]
            if rel.startswith("/") or ".." in rel.split("/"):
                raise ServerError(f"unsafe path in /list manifest: {rel!r}")
            expected_size = entry.get("size")
            if not isinstance(expected_size, int) or expected_size < 0:
                raise ServerError(f"invalid size in /list manifest for {rel!r}: {expected_size!r}")
            out = dest.joinpath(*rel.split("/"))
            out.parent.mkdir(parents=True, exist_ok=True)
            self.get_abs_file(posixpath.join(abspath, rel), dest=out, chunk_size=chunk_size)
            actual_size = out.stat().st_size
            if actual_size != expected_size:
                out.unlink(missing_ok=True)
                raise ServerError(
                    f"qslib-server file changed or was truncated during download: "
                    f"{rel!r} has {actual_size} bytes, expected {expected_size}"
                )
            n += 1
        return n

    def scpi(
        self,
        command: str,
        access: str | None = None,
        timeout_ms: int | None = None,
        encoding: str = "text",
    ) -> str | bytes:
        """Run a single SCPI command through qslib-server and return its response.

        With ``encoding="bytes"`` the raw response bytes are returned; otherwise
        the response text after ``OK`` is returned as a string.
        """
        params: dict[str, str] = {"encoding": encoding}
        if access is not None:
            params["access"] = access
        if timeout_ms is not None:
            params["timeout_ms"] = str(timeout_ms)
        query = urllib.parse.urlencode(params)
        url = f"{self.base_url}/scpi?{query}"
        req = urllib.request.Request(
            url,
            data=command.encode(),
            headers=self._headers({"Content-Type": "text/plain"}),
            method="POST",
        )
        with self._open(req, timeout=(timeout_ms / 1000.0 + 5.0) if timeout_ms is not None else None) as resp:
            data = resp.read()
        return data if encoding == "bytes" else data.decode()

    def upgrade(self, binary: bytes, *, dry_run: bool = False, timeout: float = 120.0) -> dict[str, Any]:
        """Upload a new qslib-server binary via ``POST /upgrade``.

        The server verifies the SHA-256 (sent in ``x-qslib-sha256``) and that the
        binary runs (``--version``); unless ``dry_run`` it then installs it
        atomically and restarts into it, rolling back to the previous binary if
        the new one fails to start. Returns the server's JSON response. With
        ``dry_run`` the binary is only verified, not installed.

        The connection typically drops as the server restarts; confirm the new
        build is live by polling :meth:`health` for the uploaded ``exe_sha256``
        (see :meth:`qslib.machine.Machine.upgrade_server`).
        """
        sha = hashlib.sha256(binary).hexdigest()
        url = f"{self.base_url}/upgrade" + ("?dry_run=1" if dry_run else "")
        req = urllib.request.Request(
            url,
            data=binary,
            headers=self._headers({"Content-Type": "application/octet-stream", "x-qslib-sha256": sha}),
            method="POST",
        )
        with self._open(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())


def _parse_error_body(body: bytes, default: str) -> tuple[str, str | None]:
    try:
        parsed = json.loads(body.decode())
        return parsed.get("error", default), parsed.get("detail")
    except Exception:
        text = body.decode(errors="replace").strip()
        return (text or default), None


def _validate_response_size(resp: Any, actual: int) -> None:
    """Reject a short HTTP body even when ``HTTPResponse.read(amt)`` silently
    returns EOF before satisfying ``Content-Length``."""
    raw = resp.headers.get("Content-Length")
    if raw is None:
        return
    try:
        expected = int(raw)
    except (TypeError, ValueError) as e:
        raise ServerError(f"invalid qslib-server Content-Length: {raw!r}") from e
    if actual != expected:
        raise ServerError(f"short qslib-server response: received {actual} bytes, expected {expected}")


def _copy_response(resp: Any, out: IO[bytes], chunk_size: int) -> None:
    total = 0
    while chunk := resp.read(chunk_size):
        out.write(chunk)
        total += len(chunk)
    _validate_response_size(resp, total)
