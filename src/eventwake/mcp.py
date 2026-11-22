"""A small, dependency-free MCP client for the Events extension (SEP-3415).

Events is defined for protocol revision 2026-07-28 and later only (it relies on per-request
capabilities and ``server/discover``), so this client speaks the modern protocol only and never
falls back to the legacy ``initialize`` handshake. Transports: stdio and Streamable HTTP.
"""

from __future__ import annotations

import base64
import http.client
import json
import os
import queue
import re
import socket
import ssl
import subprocess
import tempfile
import threading
import urllib.parse
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import IO, Any, Literal

from . import __version__

PROTOCOL = "2026-07-28"
EVENTS_EXT = "io.modelcontextprotocol/events"
SUB_ID_KEY = "io.modelcontextprotocol/subscriptionId"
CLIENT_INFO = {"name": "eventwake", "version": __version__}
CLIENT_CAPS: dict[str, Any] = {"extensions": {EVENTS_EXT: {}}}
DELIVERY_MODES = ("poll", "push", "webhook")

JsonObj = dict[str, Any]

ERROR_NAMES = {
    -32601: "MethodNotFound",
    -32602: "InvalidParams",
    -32603: "InternalError",
    -32020: "HeaderMismatch",
    -32021: "MissingRequiredClientCapability",
    -32022: "UnsupportedProtocolVersion",
    -32023: "NotFound",
    -32024: "Forbidden",
    -32025: "ResourceExhausted",
    -32026: "Unsupported",
    -32027: "CallbackEndpointError",
}
NOT_FOUND, FORBIDDEN, UNSUPPORTED, HEADER_MISMATCH = -32023, -32024, -32026, -32020


class McpError(Exception):
    """A JSON-RPC error returned by the server."""

    def __init__(self, code: int, message: str, data: Any = None) -> None:
        self.code = code
        self.message = message
        self.data = data
        self.name = ERROR_NAMES.get(code, "Error")
        super().__init__(f"{self.name} ({code}): {message}")

    @classmethod
    def from_obj(cls, err: Any) -> McpError:
        err = err if isinstance(err, dict) else {}
        code = err.get("code")
        return cls(code if isinstance(code, int) else 0, str(err.get("message", "")), err.get("data"))

    @property
    def permanent(self) -> bool:
        """True when retrying the same request cannot succeed (the subscription is invalid)."""
        return self.code in (-32601, -32602, NOT_FOUND, FORBIDDEN, UNSUPPORTED, -32027)


class TransportError(Exception):
    """The server could not be reached, crashed, timed out or answered with garbage."""


class StreamClosed(TransportError):
    """The push stream ended without a final frame (EOF, connection drop, or our own close)."""


@dataclass
class Endpoint:
    """How to reach one server. ``label`` is the name the user gave it."""

    label: str
    kind: Literal["http", "stdio"]
    url: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    command: str | None = None
    args: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    cwd: str | None = None
    source: str = "command line"

    def describe(self) -> str:
        """A display string that never includes header or env values (they may hold secrets)."""
        if self.kind == "http":
            return str(self.url)
        return " ".join([str(self.command), *self.args])


@dataclass(frozen=True)
class EventType:
    name: str
    description: str
    delivery: tuple[str, ...]
    input_schema: JsonObj
    payload_schema: JsonObj
    header_params: tuple[tuple[tuple[str, ...], str], ...] = ()

    @classmethod
    def parse(cls, raw: Any) -> EventType:
        if not isinstance(raw, dict) or not isinstance(raw.get("name"), str) or not raw["name"]:
            raise ValueError("event type has no name")
        delivery = raw.get("delivery")
        if not isinstance(delivery, list) or not delivery:
            raise ValueError(f"{raw['name']}: `delivery` must be a non-empty array")
        modes = tuple(m for m in delivery if m in DELIVERY_MODES)
        schema = obj(raw.get("inputSchema"))
        payload = obj(raw.get("payloadSchema"))
        return cls(
            name=raw["name"],
            description=str(raw.get("description") or ""),
            delivery=modes,
            input_schema=schema,
            payload_schema=payload,
            header_params=tuple(header_params_of(schema)),
        )


def obj(value: Any) -> JsonObj:
    """``value`` if it is a JSON object, else an empty one."""
    return value if isinstance(value, dict) else {}


def opt_str(value: Any) -> str | None:
    return value if isinstance(value, str) else None


# --- header rules (Streamable HTTP transport, 2026-07-28) --------------------------------------

_TOKEN = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")
_SAFE_INT = 2**53 - 1
_NON_PROPERTY_KEYS = (
    "items", "prefixItems", "additionalProperties", "patternProperties", "contains",
    "oneOf", "anyOf", "allOf", "not", "if", "then", "else", "$defs", "definitions",
    "dependentSchemas", "unevaluatedProperties", "unevaluatedItems", "propertyNames",
)


def header_value(value: str) -> str:
    """Encode a header value per the Streamable HTTP "Value Encoding" rules."""
    plain = (
        value != ""
        and all(0x20 <= ord(c) <= 0x7E or c == "\t" for c in value)
        and value == value.strip()
        and not (value.startswith("=?base64?") and value.endswith("?="))
    )
    if plain:
        return value
    return "=?base64?" + base64.b64encode(value.encode("utf-8")).decode("ascii") + "?="


def decode_header_value(value: str) -> str:
    if value.startswith("=?base64?") and value.endswith("?="):
        return base64.b64decode(value[9:-2], validate=True).decode("utf-8")
    return value


def _has_annotation(node: Any) -> bool:
    if isinstance(node, dict):
        return "x-mcp-header" in node or any(_has_annotation(v) for v in node.values())
    if isinstance(node, list):
        return any(_has_annotation(v) for v in node)
    return False


def header_params_of(schema: JsonObj) -> list[tuple[tuple[str, ...], str]]:
    """Return ``[(property path, header name)]`` or raise ValueError if an annotation is invalid."""
    found: list[tuple[tuple[str, ...], str]] = []

    def walk(node: JsonObj, path: tuple[str, ...]) -> None:
        for key, sub in node.items():
            if key == "properties" and isinstance(sub, dict):
                for prop, child in sub.items():
                    if isinstance(child, dict):
                        visit(child, (*path, prop))
            elif key == "x-mcp-header":
                continue
            elif _has_annotation(sub):
                where = "/".join(path) or "(root)"
                raise ValueError(f"x-mcp-header under `{key}` at {where} is not statically reachable")

    def visit(node: JsonObj, path: tuple[str, ...]) -> None:
        if "x-mcp-header" in node:
            name = node["x-mcp-header"]
            if not isinstance(name, str) or not _TOKEN.match(name):
                raise ValueError(f"x-mcp-header {name!r} on {'.'.join(path)} is not an HTTP token")
            typ = node.get("type")
            if typ not in ("string", "integer", "boolean"):
                raise ValueError(
                    f"x-mcp-header on {'.'.join(path)} needs type string, integer or boolean, not {typ!r}"
                )
            found.append((path, name))
        walk(node, path)

    if _has_annotation(schema):
        walk(schema, ())
    lowered = [n.lower() for _, n in found]
    if len(set(lowered)) != len(lowered):
        raise ValueError("x-mcp-header values are not case-insensitively unique")
    return found


def param_headers(etype: EventType | None, arguments: JsonObj) -> dict[str, str]:
    """The ``Mcp-Param-*`` headers for ``arguments`` (value at each annotated path, if present)."""
    out: dict[str, str] = {}
    if etype is None:
        return out
    for path, name in etype.header_params:
        node: Any = arguments
        for step in path:
            if not isinstance(node, dict) or step not in node:
                node = None
                break
            node = node[step]
        if node is None:
            continue
        if isinstance(node, bool):
            text = "true" if node else "false"
        elif isinstance(node, int) or (isinstance(node, float) and node.is_integer()):
            if abs(int(node)) > _SAFE_INT:
                raise ValueError(f"argument {'.'.join(path)} is outside the safe integer range")
            text = str(int(node))
        elif isinstance(node, str):
            text = node
        else:
            raise ValueError(f"argument {'.'.join(path)} must be a string, integer or boolean")
        out[f"Mcp-Param-{name}"] = header_value(text)
    return out


# --- transports --------------------------------------------------------------------------------

_EOF: JsonObj = {"__eof__": True}


class _Stdio:
    """One server process. A reader thread routes responses by id and stream notifications by
    ``_meta[subscriptionId]``, so many concurrent ``events/stream`` requests share one stdout."""

    def __init__(self, ep: Endpoint) -> None:
        env = dict(os.environ)
        env.update(ep.env)
        self._stderr: IO[bytes] = tempfile.TemporaryFile()
        try:
            self.proc = subprocess.Popen(
                [str(ep.command), *ep.args],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=self._stderr,
                env=env,
                cwd=ep.cwd,
            )
        except OSError as e:
            raise TransportError(f"cannot start {ep.describe()}: {e}") from e
        self._queues: dict[Any, queue.Queue[JsonObj]] = {}
        self._lock = threading.Lock()
        self._write_lock = threading.Lock()
        self._closed = False
        self._reader = threading.Thread(target=self._read, daemon=True, name="eventwake-stdio")
        self._reader.start()

    def _read(self) -> None:
        assert self.proc.stdout is not None
        for line in self.proc.stdout:
            try:
                msg = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if not isinstance(msg, dict):
                continue
            method = msg.get("method")
            if isinstance(method, str):
                if "id" in msg:  # a server->client request: we offer no client features but ping
                    if method == "ping":
                        self._write({"jsonrpc": "2.0", "id": msg["id"], "result": {}})
                    else:
                        err = {"code": -32601, "message": "not supported by eventwake"}
                        self._write({"jsonrpc": "2.0", "id": msg["id"], "error": err})
                    continue
                if method.startswith("notifications/events/"):
                    params = msg.get("params")
                    meta = params.get("_meta") if isinstance(params, dict) else None
                    sid = meta.get(SUB_ID_KEY) if isinstance(meta, dict) else None
                    self._route(sid, msg)
                continue
            self._route(msg.get("id"), msg)
        with self._lock:
            self._closed = True
            for q in self._queues.values():
                q.put(_EOF)

    def _route(self, key: Any, msg: JsonObj) -> None:
        with self._lock:
            q = self._queues.get(key)
        if q is not None:
            q.put(msg)

    def _write(self, msg: JsonObj) -> None:
        assert self.proc.stdin is not None
        data = (json.dumps(msg, separators=(",", ":")) + "\n").encode("utf-8")
        with self._write_lock:
            try:
                self.proc.stdin.write(data)
                self.proc.stdin.flush()
            except (BrokenPipeError, OSError, ValueError) as e:
                raise TransportError(f"server process closed its input: {self.stderr_tail()}") from e

    def register(self, key: Any) -> queue.Queue[JsonObj]:
        q: queue.Queue[JsonObj] = queue.Queue()
        with self._lock:
            self._queues[key] = q
            if self._closed:
                q.put(_EOF)
        return q

    def unregister(self, key: Any) -> None:
        with self._lock:
            self._queues.pop(key, None)

    def stderr_tail(self, n: int = 600) -> str:
        try:
            self._stderr.seek(0)
            return self._stderr.read().decode("utf-8", errors="replace")[-n:].strip()
        except (OSError, ValueError):
            return ""

    def request(self, body: JsonObj, timeout: float) -> JsonObj:
        q = self.register(body["id"])
        try:
            self._write(body)
            try:
                msg = q.get(timeout=timeout)
            except queue.Empty:
                raise TransportError(f"no answer to {body['method']} within {timeout:g}s") from None
        finally:
            self.unregister(body["id"])
        if msg is _EOF:
            tail = self.stderr_tail()
            raise TransportError("server process exited" + (f": {tail}" if tail else ""))
        return msg

    def notify(self, body: JsonObj) -> None:
        self._write(body)

    def close(self) -> None:
        try:
            if self.proc.stdin:
                self.proc.stdin.close()
            self.proc.wait(timeout=3)
        except (subprocess.TimeoutExpired, OSError):
            self.proc.kill()
            self.proc.wait(timeout=3)
        finally:
            if self.proc.stdout:
                self.proc.stdout.close()
            self._stderr.close()


def _sse_messages(lines: Iterator[bytes]) -> Iterator[JsonObj]:
    """Yield each JSON object carried in SSE ``data:`` frames (comments and other fields skipped)."""
    data: list[str] = []
    for raw in lines:
        line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
        if line == "":
            if data:
                try:
                    msg = json.loads("\n".join(data))
                except json.JSONDecodeError:
                    msg = None
                data = []
                if isinstance(msg, dict):
                    yield msg
            continue
        if line.startswith("data:"):
            data.append(line[6:] if line[5:6] == " " else line[5:])
    if data:
        try:
            msg = json.loads("\n".join(data))
        except json.JSONDecodeError:
            return
        if isinstance(msg, dict):
            yield msg


class _HttpCall:
    """One POST to the MCP endpoint whose body may be JSON or an SSE stream."""

    def __init__(self, url: str, headers: dict[str, str], body: JsonObj, timeout: float) -> None:
        parts = urllib.parse.urlsplit(url)
        if parts.scheme not in ("http", "https") or not parts.hostname:
            raise TransportError(f"not an http(s) URL: {url}")
        path = (parts.path or "/") + (f"?{parts.query}" if parts.query else "")
        conn: http.client.HTTPConnection
        if parts.scheme == "https":
            conn = http.client.HTTPSConnection(
                parts.hostname, parts.port, timeout=timeout, context=ssl.create_default_context()
            )
        else:
            conn = http.client.HTTPConnection(parts.hostname, parts.port, timeout=timeout)
        self.conn = conn
        self.url = url
        try:
            conn.request("POST", path, body=json.dumps(body).encode("utf-8"), headers=headers)
            self.resp = conn.getresponse()
        except (OSError, http.client.HTTPException) as e:
            conn.close()
            raise TransportError(f"cannot reach {url}: {e}") from e
        self.status = self.resp.status
        self.ctype = (self.resp.getheader("Content-Type") or "").split(";")[0].strip().lower()

    def messages(self) -> Iterator[JsonObj]:
        try:
            if self.ctype == "text/event-stream":
                yield from _sse_messages(iter(self.resp.readline, b""))
                return
            raw = self.resp.read()
        except (OSError, http.client.HTTPException, ValueError) as e:
            raise StreamClosed(f"{self.url}: connection lost: {e}") from e
        text = raw.decode("utf-8", errors="replace").strip()
        if not text:
            return
        try:
            msg = json.loads(text)
        except json.JSONDecodeError:
            raise TransportError(f"{self.url}: HTTP {self.status}, body is not JSON: {text[:200]}") from None
        if isinstance(msg, dict):
            yield msg

    def abort(self) -> None:
        sock = self.conn.sock
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        self.conn.close()


# --- client ------------------------------------------------------------------------------------


@dataclass
class ServerInfo:
    capabilities: JsonObj
    name: str | None
    version: str | None
    versions: list[str]

    @property
    def events(self) -> JsonObj | None:
        ext = self.capabilities.get("extensions")
        cfg = ext.get(EVENTS_EXT) if isinstance(ext, dict) else None
        return cfg if isinstance(cfg, dict) else None


class EventStream:
    """One ``events/stream`` request. Iterate with :meth:`next`; always :meth:`close` it."""

    def __init__(self, client: McpClient, request_id: int) -> None:
        self.client = client
        self.request_id = request_id
        self._q: queue.Queue[JsonObj] = queue.Queue()
        self._call: _HttpCall | None = None
        self._thread: threading.Thread | None = None
        self._closed = False

    def _pump(self, call: _HttpCall) -> None:
        try:
            for msg in call.messages():
                self._q.put(msg)
        except Exception as e:  # noqa: BLE001
            # Aborting a chunked response from another thread makes http.client fail in arbitrary
            # internal ways (seen against a Node server: IncompleteRead, then AttributeError).
            # After our own close that is expected; otherwise it is a dropped connection.
            reason = "closed by eventwake" if self._closed else f"connection lost: {e!r}"
            self._q.put({"__eof__": True, "reason": reason})
            return
        self._q.put(_EOF)

    def next(self, timeout: float | None) -> JsonObj:
        """The next message: a ``notifications/events/*`` notification or the final response.

        Raises ``TimeoutError`` if nothing arrives within ``timeout`` seconds and ``StreamClosed``
        when the transport ends.
        """
        try:
            msg = self._q.get(timeout=timeout)
        except queue.Empty:
            raise TimeoutError from None
        if msg.get("__eof__"):
            reason = msg.get("reason") or "stream ended"
            if self.client._stdio is not None:
                tail = self.client._stdio.stderr_tail()
                reason = "server process exited" + (f": {tail}" if tail else "")
            raise StreamClosed(str(reason))
        return msg

    def close(self, reason: str = "eventwake closed the subscription") -> None:
        if self._closed:
            return
        self._closed = True
        stdio = self.client._stdio
        if stdio is not None:
            stdio.unregister(self.request_id)
            note = {
                "jsonrpc": "2.0",
                "method": "notifications/cancelled",
                "params": {"requestId": self.request_id, "reason": reason},
            }
            try:
                stdio.notify(note)
            except TransportError:
                pass
        elif self._call is not None:
            self._call.abort()


class McpClient:
    """One connection to one server. Use as a context manager. Thread-safe for concurrent calls."""

    def __init__(self, endpoint: Endpoint, timeout: float = 30.0) -> None:
        self.endpoint = endpoint
        self.timeout = timeout
        self.info: ServerInfo | None = None
        self._next_id = 0
        self._id_lock = threading.Lock()
        self._stdio: _Stdio | None = None

    def __enter__(self) -> McpClient:
        self.connect()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        if self._stdio is not None:
            self._stdio.close()
            self._stdio = None

    def _id(self) -> int:
        with self._id_lock:
            self._next_id += 1
            return self._next_id

    def _body(self, method: str, params: JsonObj | None) -> JsonObj:
        meta = {
            "io.modelcontextprotocol/protocolVersion": PROTOCOL,
            "io.modelcontextprotocol/clientInfo": CLIENT_INFO,
            "io.modelcontextprotocol/clientCapabilities": CLIENT_CAPS,
        }
        return {"jsonrpc": "2.0", "id": self._id(), "method": method,
                "params": {**(params or {}), "_meta": meta}}

    def _headers(self, method: str, params: JsonObj | None, etype: EventType | None) -> dict[str, str]:
        hdrs = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "User-Agent": f"eventwake/{__version__}",
            **self.endpoint.headers,
            "MCP-Protocol-Version": PROTOCOL,
            "Mcp-Method": method,
        }
        if params and isinstance(params.get("name"), str) and method.startswith("events/"):
            hdrs["Mcp-Name"] = header_value(params["name"])
            args = params.get("arguments")
            hdrs.update(param_headers(etype, args if isinstance(args, dict) else {}))
        return hdrs

    def _http_call(self, body: JsonObj, etype: EventType | None, timeout: float) -> _HttpCall:
        assert self.endpoint.url
        params = body.get("params")
        call = _HttpCall(
            self.endpoint.url,
            self._headers(body["method"], params if isinstance(params, dict) else None, etype),
            body,
            timeout,
        )
        if call.status in (401, 403) and call.ctype != "application/json":
            call.abort()
            raise TransportError(
                f"{self.endpoint.url} answered HTTP {call.status}: it needs credentials. "
                "Set an Authorization header for this server (OAuth login is on the roadmap)."
            )
        return call

    def connect(self) -> ServerInfo:
        ep = self.endpoint
        if ep.kind == "http":
            if not ep.url:
                raise TransportError(f"server {ep.label!r} has no url")
        else:
            if not ep.command:
                raise TransportError(f"server {ep.label!r} has no command")
            self._stdio = _Stdio(ep)
        try:
            res = self.request("server/discover")
        except McpError as e:
            if e.code == -32601:
                raise TransportError(
                    f"{ep.label}: server does not implement server/discover, so it speaks a protocol "
                    f"older than {PROTOCOL}. MCP Events needs {PROTOCOL} or later."
                ) from e
            if e.code == -32022:
                raise TransportError(f"{ep.label}: server rejected protocol {PROTOCOL}: {e}") from e
            raise
        versions = res.get("supportedVersions")
        versions = [v for v in versions if isinstance(v, str)] if isinstance(versions, list) else []
        caps = obj(res.get("capabilities"))
        meta = obj(res.get("_meta"))
        sinfo = meta.get("io.modelcontextprotocol/serverInfo")
        sinfo = sinfo if isinstance(sinfo, dict) else {}
        self.info = ServerInfo(caps, sinfo.get("name"), sinfo.get("version"), versions)
        return self.info

    def request(self, method: str, params: JsonObj | None = None, etype: EventType | None = None) -> JsonObj:
        body = self._body(method, params)
        if self._stdio is not None:
            msg = self._stdio.request(body, self.timeout)
        else:
            call = self._http_call(body, etype, self.timeout)
            try:
                found: JsonObj | None = next(
                    (m for m in call.messages() if m.get("id") == body["id"] and "method" not in m), None)
            finally:
                call.abort()
            if found is None:
                raise TransportError(f"{method}: no JSON-RPC response (HTTP {call.status})")
            msg = found
        if "error" in msg:
            raise McpError.from_obj(msg["error"])
        result = msg.get("result")
        if not isinstance(result, dict):
            raise TransportError(f"{method}: response carries neither a result object nor an error")
        rtype = result.get("resultType", "complete")
        if rtype != "complete":
            raise TransportError(f"{method}: server asked for more input (resultType {rtype!r}); not supported")
        return result

    # --- events extension ---------------------------------------------------------------------

    def require_events(self) -> JsonObj:
        if self.info is None:
            self.connect()
        assert self.info is not None
        cfg = self.info.events
        if cfg is None:
            raise TransportError(
                f"{self.endpoint.label}: server does not advertise {EVENTS_EXT} in server/discover; "
                "the spec forbids sending it events/* requests"
            )
        return cfg

    def list_events(self, max_pages: int = 50) -> tuple[list[EventType], list[str]]:
        """All event types, plus warnings for descriptors the spec says a client must exclude."""
        self.require_events()
        out: list[EventType] = []
        warnings: list[str] = []
        cursor: str | None = None
        for _ in range(max_pages):
            res = self.request("events/list", {"cursor": cursor} if cursor else None)
            events = res.get("events")
            if not isinstance(events, list):
                raise TransportError("events/list result has no `events` array")
            for raw in events:
                try:
                    et = EventType.parse(raw)
                except ValueError as e:
                    name = raw.get("name") if isinstance(raw, dict) else None
                    warnings.append(f"excluded event type {name!r}: {e}")
                    continue
                if not et.delivery:
                    warnings.append(f"excluded event type {et.name!r}: no delivery mode eventwake knows")
                    continue
                out.append(et)
            nxt = res.get("nextCursor")
            if not isinstance(nxt, str) or not nxt:
                return out, warnings
            cursor = nxt
        raise TransportError(f"events/list still paginating after {max_pages} pages")

    def poll(self, etype: EventType, arguments: JsonObj, cursor: str | None,
             max_age_ms: int | None = None, max_events: int | None = None) -> JsonObj:
        params: JsonObj = {"name": etype.name, "arguments": arguments, "cursor": cursor}
        if max_age_ms is not None and cursor is not None:
            params["maxAgeMs"] = max_age_ms
        if max_events is not None:
            params["maxEvents"] = max_events
        return self.request("events/poll", params, etype)

    def open_stream(self, etype: EventType, arguments: JsonObj, cursor: str | None,
                    max_age_ms: int | None = None, heartbeat_ms: int | None = None) -> EventStream:
        self.require_events()
        params: JsonObj = {"name": etype.name, "arguments": arguments, "cursor": cursor}
        if max_age_ms is not None and cursor is not None:
            params["maxAgeMs"] = max_age_ms
        if heartbeat_ms is not None:
            params["heartbeatMs"] = heartbeat_ms
        body = self._body("events/stream", params)
        stream = EventStream(self, body["id"])
        if self._stdio is not None:
            stream._q = self._stdio.register(body["id"])
            self._stdio.notify(body)
            return stream
        # No default request timeout on the stream (the spec says the heartbeat is the liveness
        # signal); the caller enforces 3x heartbeat with next(timeout=...).
        call = self._http_call(body, etype, self.timeout)
        if call.conn.sock is not None:
            call.conn.sock.settimeout(None)
        stream._call = call
        stream._thread = threading.Thread(target=stream._pump, args=(call,), daemon=True,
                                          name=f"eventwake-stream-{etype.name}")
        stream._thread.start()
        return stream

    def subscribe(self, etype: EventType, arguments: JsonObj, url: str, secret: str,
                  cursor: str | None, ttl_ms: int | None, max_age_ms: int | None = None,
                  no_expiry: bool = False) -> JsonObj:
        """``ttl_ms=None`` omits the suggestion (server default); ``no_expiry`` sends ``ttlMs: null``."""
        params: JsonObj = {
            "name": etype.name,
            "arguments": arguments,
            "delivery": {"mode": "webhook", "url": url, "secret": secret},
            "cursor": cursor,
        }
        if no_expiry:
            params["ttlMs"] = None
        elif ttl_ms is not None:
            params["ttlMs"] = ttl_ms
        if max_age_ms is not None and cursor is not None:
            params["maxAgeMs"] = max_age_ms
        return self.request("events/subscribe", params, etype)

    def unsubscribe(self, etype: EventType, arguments: JsonObj, url: str) -> None:
        self.request("events/unsubscribe",
                     {"name": etype.name, "arguments": arguments, "delivery": {"url": url}}, etype)
