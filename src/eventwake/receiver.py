"""The webhook endpoint: Standard Webhooks verification and the MCP Events delivery profile.

Each watch gets its own path, ``/hooks/<subscription key>``, so the receiver can route (and answer
the ``verification`` handshake) before the ``events/subscribe`` response has told it the server's
subscription ``id``. It returns ``2xx`` only after the event is durably in the state file, as the
spec's acknowledgement semantics require.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import secrets
import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .mcp import obj
from .state import Delivered, Store

JsonObj = dict[str, Any]
MAX_BODY = 256 * 1024
FRESHNESS = 300  # seconds; the spec says receivers SHOULD reject older timestamps
WELL_KNOWN = "/.well-known/mcp-webhook-receiver.json"


def new_secret() -> str:
    """A Standard Webhooks symmetric secret from a CSPRNG: whsec_ + base64(32 random bytes)."""
    return "whsec_" + base64.b64encode(secrets.token_bytes(32)).decode("ascii")


def secret_bytes(secret: str) -> bytes:
    if not secret.startswith("whsec_"):
        raise ValueError("secret must start with whsec_")
    raw = base64.b64decode(secret[6:], validate=True)
    if not 24 <= len(raw) <= 64:
        raise ValueError("secret must decode to 24-64 bytes")
    return raw


def sign(secret: str, msg_id: str, timestamp: int, body: bytes) -> str:
    mac = hmac.new(secret_bytes(secret), f"{msg_id}.{timestamp}.".encode() + body, hashlib.sha256)
    return "v1," + base64.b64encode(mac.digest()).decode("ascii")


def verify(secrets_: list[str], msg_id: str, timestamp: str, signature_header: str, body: bytes,
           now: float | None = None) -> str | None:
    """None if valid, else the reason. Any ``v1,`` signature under any current secret passes."""
    try:
        ts = int(timestamp)
    except ValueError:
        return "bad webhook-timestamp"
    if abs((now if now is not None else time.time()) - ts) > FRESHNESS:
        return "webhook-timestamp outside the 5 minute window"
    offered = []
    for part in signature_header.split(" "):
        ver, _, sig = part.partition(",")
        if ver == "v1" and sig:
            try:
                offered.append(base64.b64decode(sig, validate=True))
            except (binascii.Error, ValueError):
                continue
    if not offered:
        return "no v1 signature"
    for sec in secrets_:
        try:
            key = secret_bytes(sec)
        except (ValueError, binascii.Error):
            continue
        want = hmac.new(key, f"{msg_id}.{ts}.".encode() + body, hashlib.sha256).digest()
        if any(hmac.compare_digest(want, o) for o in offered):
            return None
    return "signature does not verify"


@dataclass
class Route:
    key: str
    server: str
    event: str
    secrets: list[str]
    sub_id: str | None = None
    on_event: Callable[[list[Delivered]], None] | None = None
    on_terminated: Callable[[JsonObj], None] | None = None
    seen_ids: OrderedDict[str, None] = field(default_factory=OrderedDict)


class Receiver:
    def __init__(self, store: Store, host: str, port: int) -> None:
        self.store = store
        self.routes: dict[str, Route] = {}
        self._lock = threading.Lock()
        receiver = self

        class Handler(BaseHTTPRequestHandler):
            server_version = "eventwake-receiver"
            sys_version = ""

            def log_message(self, *_: Any) -> None:
                pass

            def _send(self, status: int, body: JsonObj | None = None) -> None:
                data = json.dumps(body or {}).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self) -> None:  # noqa: N802
                if self.path == WELL_KNOWN:
                    self._send(200, {"receivers": ["/hooks/"]})
                elif self.path == "/healthz":
                    self._send(200, {"ok": True, "routes": len(receiver.routes)})
                else:
                    self._send(404, {"error": "not found"})

            def do_POST(self) -> None:  # noqa: N802
                status, body = receiver.handle(self.path, dict(self.headers.items()), self._body())
                self._send(status, body)

            def _body(self) -> bytes | None:
                try:
                    n = int(self.headers.get("Content-Length", "0"))
                except ValueError:
                    return None
                if n > MAX_BODY:
                    return None
                return self.rfile.read(n)

        self.httpd = ThreadingHTTPServer((host, port), Handler)
        self.httpd.daemon_threads = True
        self.port = self.httpd.server_address[1]
        self._thread = threading.Thread(target=self.httpd.serve_forever, daemon=True,
                                        name="eventwake-receiver")

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()

    def add(self, route: Route) -> None:
        with self._lock:
            self.routes[route.key] = route

    def remove(self, key: str) -> None:
        with self._lock:
            self.routes.pop(key, None)

    def handle(self, path: str, headers: dict[str, str], body: bytes | None) -> tuple[int, JsonObj]:
        h = {k.lower(): v for k, v in headers.items()}
        if body is None:
            return 413, {"error": "body too large or unreadable"}
        if not path.startswith("/hooks/"):
            return 404, {"error": "not a hook path"}
        with self._lock:
            route = self.routes.get(path[len("/hooks/"):])
        if route is None:
            return 503, {"error": "no subscription routed here yet; retry"}  # retryable, per spec
        msg_id, ts, sig = h.get("webhook-id"), h.get("webhook-timestamp"), h.get("webhook-signature")
        sub_id = h.get("x-mcp-subscription-id")
        if not (msg_id and ts and sig and sub_id):
            return 400, {"error": "missing Standard Webhooks or X-MCP-Subscription-Id headers"}
        reason = verify(route.secrets, msg_id, ts, sig, body)
        if reason is not None:
            return 401, {"error": reason}
        if route.sub_id is not None and sub_id != route.sub_id:
            return 400, {"error": "X-MCP-Subscription-Id does not match this route"}
        try:
            doc = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return 400, {"error": "body is not JSON"}
        if not isinstance(doc, dict):
            return 400, {"error": "body is not a JSON object"}
        kind = doc.get("type")
        if kind == "verification":
            challenge = doc.get("challenge")
            if not isinstance(challenge, str):
                return 400, {"error": "verification without challenge"}
            return 200, {"challenge": challenge}
        with self._lock:
            if msg_id in route.seen_ids:
                return 200, {"ok": True, "duplicate": True}
        if kind == "gap":
            cursor = doc.get("cursor")
            self.store.set_cursor(route.key, cursor if isinstance(cursor, str) else None, truncated=True)
        elif kind == "terminated":
            err = obj(doc.get("error"))
            self.store.note(route.key, terminated={"error": err, "at": time.time()})
            if route.on_terminated is not None:
                route.on_terminated(err)
        elif kind is not None:
            return 400, {"error": f"unknown control envelope type {kind!r}"}
        else:
            cursor = doc.get("cursor")
            delivered = self.store.accept(route.key, route.server, route.event, [doc],
                                          cursor if isinstance(cursor, str) else None)
            if delivered and route.on_event is not None:
                route.on_event(delivered)
        with self._lock:
            route.seen_ids[msg_id] = None
            while len(route.seen_ids) > 5000:
                route.seen_ids.popitem(last=False)
        return 200, {"ok": True}
