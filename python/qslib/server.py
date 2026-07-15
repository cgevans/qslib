# SPDX-FileCopyrightText: 2024 - 2026 Constantine Evans <qslib@mb.costi.net>
# SPDX-License-Identifier: EUPL-1.2

"""Typed client for qslib-server's optional ``/api/v1`` semantic API."""

from __future__ import annotations

import hashlib
import json
import os
import socket
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from http.client import HTTPException
from pathlib import Path
from typing import IO, Any, BinaryIO, Iterator
from uuid import uuid4

__all__ = ["ServerClient", "ServerError", "ServerOutcomeUnknown", "ServerUnavailable"]


class ServerError(RuntimeError):
    """A structured server, response, or transport error."""

    def __init__(
        self,
        message: str,
        status: int | None = None,
        *,
        code: str | None = None,
        retryable: bool = False,
        outcome: str | None = None,
        request_id: str | None = None,
    ):
        super().__init__(message)
        self.status = status
        self.code = code
        self.retryable = retryable
        self.outcome = outcome
        self.request_id = request_id


class ServerOutcomeUnknown(ServerError):
    """A mutation may have reached the server and must not be repeated."""

    def __init__(self, message: str, state_query: str):
        super().__init__(message, retryable=False, outcome="unknown")
        self.state_query = state_query


class ServerUnavailable(ServerError):
    """The HTTP request could not be submitted, so direct fallback is safe."""


_ABSOLUTE_CONTEXTS: tuple[tuple[str, str], ...] = (
    ("/sdcard/private_run_complete", "private_run_complete"),
    ("/sdcard/public_run_complete", "public_run_complete"),
    ("/data/vendor/IS/calibrations", "calibrations"),
    ("/data/vendor/IS/experiments", "experiments"),
    ("/data/vendor/IS/templates", "templates"),
    ("/data/vendor/IS/runs", "runs"),
    ("/data/vendor/IS/logs", "logs"),
    ("/data/vendor/IS", "default"),
)


@dataclass
class ServerClient:
    host: str
    port: int = 7500
    token: str | None = None
    timeout: float = 30.0
    _capabilities: dict[str, Any] | None = field(default=None, init=False, repr=False)
    _retry_at: float = field(default=0.0, init=False, repr=False)

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def _headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        if extra:
            headers.update(extra)
        return headers

    def _request(
        self,
        path: str,
        *,
        method: str = "GET",
        data: bytes | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
        mutation: bool = False,
        state_query: str | None = None,
    ) -> Any:
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=data,
            headers=self._headers(headers),
            method=method,
        )
        try:
            return urllib.request.urlopen(request, timeout=timeout or self.timeout)  # noqa: S310
        except urllib.error.HTTPError as error:
            body = b""
            try:
                body = error.read()
            except Exception:
                pass
            parsed = _parse_error(body, str(error))
            raise ServerError(
                parsed["message"],
                status=error.code,
                code=parsed.get("code"),
                retryable=bool(parsed.get("retryable", False)),
                outcome=parsed.get("outcome"),
                request_id=parsed.get("request_id"),
            ) from error
        except (urllib.error.URLError, OSError, HTTPException) as error:
            if mutation:
                reason = error.reason if isinstance(error, urllib.error.URLError) else error
                if isinstance(reason, (ConnectionRefusedError, socket.gaierror)):
                    raise ServerUnavailable(
                        f"cannot connect to qslib-server at {self.base_url}: {error}",
                        retryable=True,
                        outcome="not_started",
                    ) from error
                raise ServerOutcomeUnknown(
                    f"qslib-server mutation transport failed: {error}",
                    state_query or "/api/v1/instrument/status",
                ) from error
            raise ServerError(f"cannot reach qslib-server at {self.base_url}: {error}") from error

    def _json(
        self,
        path: str,
        *,
        method: str = "GET",
        value: Any = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
        mutation: bool = False,
        state_query: str | None = None,
    ) -> dict[str, Any]:
        data = None if value is None else json.dumps(value, separators=(",", ":")).encode()
        request_headers = {"Content-Type": "application/json"} if data is not None else {}
        if headers:
            request_headers.update(headers)
        with self._request(
            path,
            method=method,
            data=data,
            headers=request_headers,
            timeout=timeout,
            mutation=mutation,
            state_query=state_query,
        ) as response:
            body = response.read()
        if not body:
            return {}
        try:
            result = json.loads(body)
        except (UnicodeDecodeError, ValueError) as error:
            raise ServerError(f"qslib-server returned invalid JSON for {path}") from error
        if not isinstance(result, dict):
            raise ServerError(f"qslib-server returned a non-object JSON response for {path}")
        return result

    def health(self) -> dict[str, Any]:
        return self._json("/health")

    def available(self) -> bool:
        try:
            return bool(self.health().get("ready"))
        except ServerError:
            return False

    def capabilities(self) -> dict[str, Any]:
        """Return and cache v1 capabilities; failures are cached for 30 seconds."""
        if self._capabilities is not None:
            return self._capabilities
        if time.monotonic() < self._retry_at:
            raise ServerError("qslib-server capability probe is negatively cached")
        try:
            capabilities = self._json("/api/v1/capabilities")
        except ServerError:
            self._retry_at = time.monotonic() + 30.0
            raise
        if capabilities.get("api_version") != "v1":
            self._retry_at = time.monotonic() + 30.0
            raise ServerError("qslib-server does not advertise compatible API v1")
        self._capabilities = capabilities
        self._retry_at = 0.0
        return capabilities

    def supports(self, resource: str) -> bool:
        return resource in self.capabilities().get("resources", [])

    def instrument_status(self) -> dict[str, Any]:
        return self._json("/api/v1/instrument/status")

    def current_run(self) -> dict[str, Any]:
        return self._json("/api/v1/runs/current")

    def set_power(self, enabled: bool) -> None:
        self._json(
            "/api/v1/instrument/power",
            method="PUT",
            value={"enabled": enabled},
            mutation=True,
            state_query="/api/v1/instrument/status",
        )

    def set_block(self, enabled: bool, target_c: float | None = None) -> None:
        self._json(
            "/api/v1/instrument/block",
            method="PUT",
            value={"enabled": enabled, "target_c": target_c},
            mutation=True,
            state_query="/api/v1/instrument/status",
        )

    def set_indicator(self, color: str, mode: str = "on") -> None:
        self._json(
            "/api/v1/instrument/indicator",
            method="PUT",
            value={"color": color.lower(), "mode": mode.lower()},
            mutation=True,
            state_query="/api/v1/instrument/status",
        )

    def set_drawer(self, position: str, *, lower_cover: bool = True, verify: bool = True) -> None:
        self._json(
            "/api/v1/instrument/drawer",
            method="PUT",
            value={"position": position.lower(), "lower_cover": lower_cover, "verify": verify},
            mutation=True,
            state_query="/api/v1/instrument/status",
        )

    def set_cover(self, position: str = "down", *, verify: bool = True) -> None:
        self._json(
            "/api/v1/instrument/cover",
            method="PUT",
            value={"position": position.lower(), "verify": verify},
            mutation=True,
            state_query="/api/v1/instrument/status",
        )

    def list_experiments(self) -> dict[str, Any]:
        return self._json("/api/v1/experiments")

    def experiment(self, name: str) -> dict[str, Any]:
        return self._json(f"/api/v1/experiments/{_quote(name)}")

    def get_package(self, name: str) -> tuple[bytes, str | None]:
        with self._request(
            f"/api/v1/experiments/{_quote(name)}/package",
            headers={"Accept": "application/zip"},
        ) as response:
            return response.read(), response.headers.get("ETag")

    def delete_experiment(self, name: str) -> None:
        with self._request(
            f"/api/v1/experiments/{_quote(name)}",
            method="DELETE",
            mutation=True,
            state_query=f"/api/v1/experiments/{_quote(name)}",
        ) as response:
            response.read()

    def list_runs(self, location: str = "working") -> list[str]:
        query = urllib.parse.urlencode({"location": location})
        return [str(name) for name in self._json(f"/api/v1/runs?{query}").get("runs", [])]

    def run(self, name: str) -> dict[str, Any]:
        return self._json(f"/api/v1/runs/{_quote(name)}")

    def get_eds(self, name: str) -> bytes:
        with self._request(
            f"/api/v1/runs/{_quote(name)}/eds",
            headers={"Accept": "application/zip"},
        ) as response:
            return response.read()

    def stage_package(self, name: str, package: bytes) -> str:
        path = f"/api/v1/experiments/{_quote(name)}/package"
        with self._request(
            path,
            method="PUT",
            data=package,
            headers={"Content-Type": "application/zip"},
            timeout=130.0,
            mutation=True,
            state_query=f"/api/v1/experiments/{_quote(name)}",
        ) as response:
            response.read()
            etag = response.headers.get("ETag")
        if not etag:
            raise ServerError("qslib-server package response omitted ETag")
        return etag

    def start_run(
        self,
        experiment: str,
        package_etag: str,
        *,
        overwrite: bool | str = False,
        require_exclusive: bool = False,
        require_drawer_check: bool = True,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        return self._json(
            "/api/v1/runs",
            method="POST",
            value={
                "experiment": experiment,
                "package_etag": package_etag,
                "overwrite": str(overwrite).lower(),
                "require_exclusive": require_exclusive,
                "require_drawer_check": require_drawer_check,
            },
            headers={"Idempotency-Key": idempotency_key or str(uuid4())},
            timeout=130.0,
            mutation=True,
            state_query="/api/v1/runs/current",
        )

    def run_action(
        self,
        name: str,
        action: str,
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        return self._json(
            f"/api/v1/runs/{_quote(name)}/actions/{_quote(action)}",
            method="POST",
            headers={"Idempotency-Key": idempotency_key or str(uuid4())},
            mutation=True,
            state_query=f"/api/v1/runs/{_quote(name)}",
        )

    def get_running_protocol(self) -> dict[str, Any]:
        """Return the instrument's authoritative active protocol as SCPI."""
        return self._json("/api/v1/runs/current/protocol")

    def put_protocol(
        self,
        name: str,
        scpi: str,
        tcprotocol_xml: bytes,
        *,
        mode: str = "replace",
    ) -> dict[str, Any]:
        return self._json(
            f"/api/v1/runs/{_quote(name)}/protocol?{urllib.parse.urlencode({'mode': mode})}",
            method="PUT",
            value={"scpi": scpi, "tcprotocol_xml": tcprotocol_xml.decode("utf-8")},
            mutation=True,
            state_query="/api/v1/runs/current/protocol",
        )

    def generate_access_key(self, *, idempotency_key: str | None = None) -> dict[str, Any]:
        return self._json(
            "/api/v1/instrument/access-keys",
            method="POST",
            headers={"Idempotency-Key": idempotency_key or str(uuid4())},
            mutation=True,
            state_query="/api/v1/instrument/status",
        )

    def restart_instrument(self, *, idempotency_key: str | None = None) -> dict[str, Any]:
        return self._json(
            "/api/v1/instrument/actions/restart",
            method="POST",
            headers={"Idempotency-Key": idempotency_key or str(uuid4())},
            mutation=True,
            state_query="/health",
        )

    def operation(self, operation_id: str) -> dict[str, Any]:
        return self._json(f"/api/v1/operations/{_quote(operation_id)}")

    def wait_operation(self, operation: str | dict[str, Any], timeout: float = 120.0) -> dict[str, Any]:
        operation_id = operation if isinstance(operation, str) else str(operation["id"])
        deadline = time.monotonic() + timeout
        while True:
            current = self.operation(operation_id)
            if current.get("state") in {"succeeded", "failed", "unknown"}:
                return current
            if time.monotonic() >= deadline:
                raise ServerOutcomeUnknown(
                    f"timed out waiting for operation {operation_id}",
                    f"/api/v1/operations/{operation_id}",
                )
            time.sleep(0.25)

    def events(self, last_event_id: int | None = None) -> Iterator[dict[str, Any]]:
        """Yield SSE events, reconnecting with ``Last-Event-ID`` after loss."""
        while True:
            headers = {"Accept": "text/event-stream"}
            if last_event_id is not None:
                headers["Last-Event-ID"] = str(last_event_id)
            try:
                with self._request("/api/v1/events", headers=headers, timeout=30.0) as response:
                    block: list[str] = []
                    while raw := response.readline():
                        line = raw.decode("utf-8").rstrip("\r\n")
                        if line:
                            block.append(line)
                            continue
                        event = _parse_sse(block)
                        block.clear()
                        if event is None:
                            continue
                        if event.get("id") is not None:
                            last_event_id = int(event["id"])
                        yield event
            except (ServerError, OSError, HTTPException, UnicodeDecodeError):
                time.sleep(0.25)

    def get_file(
        self,
        path: str,
        dest: str | Path | BinaryIO | None = None,
        chunk_size: int = 1 << 20,
        *,
        context: str = "default",
        range_start: int | None = None,
    ) -> bytes | None:
        headers = {"Accept": "application/octet-stream"}
        if range_start is not None:
            headers["Range"] = f"bytes={range_start}-"
        route = f"/api/v1/files/{_quote(context)}/{_quote_path(path)}"
        temp_path: Path | None = None
        try:
            try:
                with self._request(route, headers=headers) as response:
                    if dest is None:
                        data = response.read()
                        _validate_response_size(response, len(data))
                        return data
                    if isinstance(dest, (str, Path)):
                        final_path = Path(dest)
                        with tempfile.NamedTemporaryFile(
                            mode="wb",
                            dir=final_path.parent,
                            prefix=f".{final_path.name}.download.",
                            delete=False,
                        ) as output:
                            temp_path = Path(output.name)
                            _copy_response(response, output, chunk_size)
                        os.replace(temp_path, final_path)
                        temp_path = None
                    else:
                        _copy_response(response, dest, chunk_size)
                    return None
            finally:
                if temp_path is not None:
                    temp_path.unlink(missing_ok=True)
        except ServerError as error:
            if error.status == 404:
                raise FileNotFoundError(path) from error
            raise

    def put_file(self, path: str, data: bytes, *, context: str = "default", etag: str | None = None) -> None:
        headers = {"Content-Type": "application/octet-stream"}
        if etag is not None:
            headers["If-Match"] = etag
        self._request(
            f"/api/v1/files/{_quote(context)}/{_quote_path(path)}",
            method="PUT",
            data=data,
            headers=headers,
            mutation=True,
            state_query=f"/api/v1/files/{_quote(context)}/{_quote_path(path)}",
        ).close()

    def get_abs_file(
        self,
        abspath: str,
        dest: str | Path | BinaryIO | None = None,
        chunk_size: int = 1 << 20,
        *,
        range_start: int | None = None,
    ) -> bytes | None:
        context, relative = _absolute_context(abspath)
        return self.get_file(relative, dest, chunk_size, context=context, range_start=range_start)

    def put_abs_file(self, abspath: str, data: bytes) -> None:
        context, relative = _absolute_context(abspath)
        self.put_file(relative, data, context=context)

    def list_context_dir(self, context: str, path: str = "") -> list[dict[str, Any]]:
        suffix = f"/{_quote_path(path)}" if path else ""
        try:
            return self._json(f"/api/v1/directories/{_quote(context)}{suffix}").get("files", [])
        except ServerError as error:
            if error.status == 404:
                raise FileNotFoundError(path) from error
            raise

    def list_dir(self, abspath: str) -> list[dict[str, Any]]:
        context, relative = _absolute_context(abspath)
        return self.list_context_dir(context, relative)

    def download_dir(self, abspath: str, dest_dir: str | Path, chunk_size: int = 1 << 20) -> int:
        context, root = _absolute_context(abspath)
        destination = Path(dest_dir)
        manifest = self.list_context_dir(context, root)
        for entry in manifest:
            relative = str(entry["path"])
            if relative.startswith("/") or ".." in relative.split("/"):
                raise ServerError(f"unsafe directory path {relative!r}")
            output = destination.joinpath(*relative.split("/"))
            output.parent.mkdir(parents=True, exist_ok=True)
            remote = "/".join(part for part in (root, relative) if part)
            self.get_file(remote, output, chunk_size, context=context)
            if output.stat().st_size != int(entry["size"]):
                output.unlink(missing_ok=True)
                raise ServerError(f"truncated qslib-server directory download for {relative!r}")
        return len(manifest)

    def scpi(self, command: str, access: str | None = None, timeout_ms: int | None = None) -> str:
        value: dict[str, Any] = {"command": command, "encoding": "text"}
        if access is not None:
            value["access"] = access
        if timeout_ms is not None:
            value["timeout_ms"] = timeout_ms
        with self._request(
            "/api/v1/scpi",
            method="POST",
            data=json.dumps(value).encode(),
            headers={"Content-Type": "application/json"},
            timeout=(timeout_ms or 30_000) / 1000 + 5,
            mutation=True,
            state_query="/api/v1/instrument/status",
        ) as response:
            return response.read().decode()

    def upgrade(self, binary: bytes, *, dry_run: bool = False, timeout: float = 120.0) -> dict[str, Any]:
        sha = hashlib.sha256(binary).hexdigest()
        path = "/api/v1/server/upgrade" + ("?dry_run=1" if dry_run else "")
        with self._request(
            path,
            method="POST",
            data=binary,
            headers={"Content-Type": "application/octet-stream", "x-qslib-sha256": sha},
            timeout=timeout,
            mutation=True,
            state_query="/health",
        ) as response:
            return json.loads(response.read())


def _parse_error(body: bytes, default: str) -> dict[str, Any]:
    try:
        parsed = json.loads(body)
        detail = parsed.get("error", {})
        if isinstance(detail, str):
            return {"message": detail, "request_id": parsed.get("request_id")}
        if isinstance(detail, dict):
            return {"message": detail.get("message", default), "request_id": parsed.get("request_id"), **detail}
    except Exception:
        pass
    return {"message": body.decode(errors="replace").strip() or default}


def _absolute_context(path: str) -> tuple[str, str]:
    normalized = "/" + "/".join(part for part in path.split("/") if part not in {"", ".", ".."})
    for root, context in _ABSOLUTE_CONTEXTS:
        if normalized == root:
            return context, ""
        if normalized.startswith(root + "/"):
            return context, normalized[len(root) + 1 :]
    raise ServerError(f"{path!r} does not map to a named qslib-server file context")


def _quote(value: str) -> str:
    return urllib.parse.quote(value, safe="")


def _quote_path(value: str) -> str:
    return "/".join(_quote(part) for part in value.split("/") if part)


def _parse_sse(lines: list[str]) -> dict[str, Any] | None:
    if not lines or all(line.startswith(":") for line in lines):
        return None
    event: dict[str, Any] = {"event": "message"}
    data: list[str] = []
    for line in lines:
        if line.startswith("id:"):
            event["id"] = line[3:].strip()
        elif line.startswith("event:"):
            event["event"] = line[6:].strip()
        elif line.startswith("data:"):
            data.append(line[5:].lstrip())
    event["data"] = json.loads("\n".join(data)) if data else None
    return event


def _validate_response_size(response: Any, actual: int) -> None:
    raw = response.headers.get("Content-Length")
    if raw is None:
        return
    try:
        expected = int(raw)
    except (TypeError, ValueError) as error:
        raise ServerError(f"invalid qslib-server Content-Length: {raw!r}") from error
    if actual != expected:
        raise ServerError(f"short qslib-server response: received {actual} bytes, expected {expected}")


def _copy_response(response: Any, output: IO[bytes], chunk_size: int) -> None:
    total = 0
    while chunk := response.read(chunk_size):
        output.write(chunk)
        total += len(chunk)
    _validate_response_size(response, total)
