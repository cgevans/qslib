# SPDX-FileCopyrightText: 2024 - 2026 Constantine Evans <qslib@mb.costi.net>
#
# SPDX-License-Identifier: EUPL-1.2

"""Client for the on-instrument ``qslib-server`` HTTP agent.

The agent (see the ``qslib-server`` crate) serves bulk file transfer, one-shot
SCPI commands, and a SCPI tunnel over plain HTTP on the instrument's private
link. This module is a small, dependency-free (stdlib ``urllib``) client for it.

The agent is an optional acceleration layer: bulk transfer through it avoids the
base64+TLS overhead of ``FILE:READ`` over SCPI. Everything degrades to the
normal SCPI path (:meth:`qslib.machine.Machine.read_file`) when the agent is not
running.
"""

from __future__ import annotations

import json
import shutil
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any, BinaryIO

__all__ = ["AgentClient", "AgentError"]


class AgentError(RuntimeError):
    """An error returned by, or while contacting, the agent."""

    def __init__(self, message: str, status: int | None = None, detail: str | None = None):
        super().__init__(message)
        self.status = status
        self.detail = detail


@dataclass
class AgentClient:
    """A client for a running ``qslib-server`` agent.

    Parameters
    ----------
    host
        Host or IP of the agent (typically reached through the Windows-box
        forward / VPN, or directly on the private link).
    port
        Agent port (default 8770).
    token
        Bearer token, if the agent requires one.
    timeout
        Default request timeout in seconds.
    """

    host: str
    port: int = 8770
    token: str | None = None
    timeout: float = 30.0

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

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
            raise AgentError(message, status=e.code, detail=detail) from e
        except urllib.error.URLError as e:
            raise AgentError(f"cannot reach agent at {self.base_url}: {e.reason}") from e

    # -- endpoints ---------------------------------------------------------

    def health(self) -> dict[str, Any]:
        """Return the agent's ``/health`` document."""
        req = urllib.request.Request(f"{self.base_url}/health", headers=self._headers(), method="GET")
        with self._open(req) as resp:
            raw = resp.read()
        try:
            return json.loads(raw.decode())
        except (ValueError, UnicodeDecodeError) as e:
            # Not the agent (or a proxy error page): surface as AgentError so
            # available()/ensure_agent degrade gracefully rather than raising.
            raise AgentError("agent /health returned a non-JSON body") from e

    def available(self) -> bool:
        """Return True if the agent responds to ``/health`` (and SCPI is up)."""
        try:
            h = self.health()
        except AgentError:
            return False
        return bool(h.get("scpi_ok", True))

    def get_file(
        self,
        path: str,
        dest: str | Path | BinaryIO | None = None,
        chunk_size: int = 1 << 20,
    ) -> bytes | None:
        """Fetch a file under the agent's file root.

        Parameters
        ----------
        path
            Path relative to the agent's configured ``--file-root``.
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

    def scpi(
        self,
        command: str,
        access: str | None = None,
        timeout_ms: int | None = None,
        encoding: str = "text",
    ) -> str | bytes:
        """Run a single SCPI command through the agent and return its response.

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
