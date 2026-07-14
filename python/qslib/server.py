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

import json
import posixpath
import shutil
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
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
                self._file_root = None
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
        return bool(h.get("scpi_ok", True))

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
        with self._open(req) as resp:
            if dest is None:
                return resp.read()
            if isinstance(dest, (str, Path)):
                with open(dest, "wb") as f:
                    shutil.copyfileobj(resp, f, chunk_size)
            else:
                out: IO[bytes] = dest
                shutil.copyfileobj(resp, out, chunk_size)
            return None

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
        Returns the number of files written. Raises :class:`ServerError` on any
        failure (so callers can fall back to SCPI).
        """
        dest = Path(dest_dir)
        n = 0
        for entry in self.list_dir(abspath):
            rel = entry["path"]
            if rel.startswith("/") or ".." in rel.split("/"):
                raise ServerError(f"unsafe path in /list manifest: {rel!r}")
            out = dest.joinpath(*rel.split("/"))
            out.parent.mkdir(parents=True, exist_ok=True)
            self.get_abs_file(posixpath.join(abspath, rel), dest=out, chunk_size=chunk_size)
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
        with self._open(req, timeout=(timeout_ms / 1000.0 + 5.0) if timeout_ms else None) as resp:
            data = resp.read()
        return data if encoding == "bytes" else data.decode()


def _parse_error_body(body: bytes, default: str) -> tuple[str, str | None]:
    try:
        parsed = json.loads(body.decode())
        return parsed.get("error", default), parsed.get("detail")
    except Exception:
        text = body.decode(errors="replace").strip()
        return (text or default), None
