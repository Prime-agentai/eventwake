"""A strict MCP Events server for tests, demos and server authors (``eventwake dev-server``).

It is deliberately unforgiving: it rejects a missing or mismatched ``MCP-Protocol-Version``,
``Mcp-Method``, ``Mcp-Name`` or ``Mcp-Param-*`` header with ``HeaderMismatch``, unknown or
mistyped subscription arguments with ``InvalidParams``, malformed ``whsec_`` secrets and
non-https callbacks, and it runs the verification handshake before it delivers a webhook. So
every passing client test is also a conformance check.

Event types:

* ``ledger.entry``: durable, replayable (cursor ``L<offset>``), poll + push + webhook. Argument
  ``account`` is mirrored into ``Mcp-Param-Account``.
* ``clock.tick``: emit-only, no replay (cursor always null), push only.
* ``deploy.finished``: webhook only.

Control plane (HTTP mode, or ``--control-port`` in stdio mode): ``POST /_control`` with
``{"action": "emit", "event": "ledger.entry", "data": {...}}``, ``"terminate"``,
``"drop_streams"``, ``"revoke"`` or ``"stats"``.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import ipaddress
import json
import secrets
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import IO, Any

from . import __version__
from ._http import FastHTTPServer
from .mcp import EVENTS_EXT, PROTOCOL, SUB_ID_KEY, decode_header_value, obj, opt_str
from .receiver import sign

JsonObj = dict[str, Any]


def iso(t: float) -> str:
    return datetime.fromtimestamp(t, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class RpcError(Exception):
    def __init__(self, code: int, message: str, data: Any = None, http: int = 200) -> None:
        super().__init__(message)
        self.code, self.message, self.data, self.http = code, message, data, http

    def obj(self) -> JsonObj:
        err: JsonObj = {"code": self.code, "message": self.message}
        if self.data is not None:
            err["data"] = self.data
        return err


EVENT_TYPES: list[JsonObj] = [
    {
        "name": "ledger.entry",
        "description": "A ledger entry was posted. Durable upstream: replay by cursor.",
        "delivery": ["poll", "push", "webhook"],
        "inputSchema": {
            "type": "object",
            "properties": {
                "account": {"type": "string", "x-mcp-header": "Account",
                            "description": "Only entries for this account"},
                "min_amount": {"type": "integer", "description": "Only entries at least this large"},
            },
            "additionalProperties": False,
        },
        "payloadSchema": {
            "type": "object",
            "properties": {"entryId": {"type": "string"}, "account": {"type": "string"},
                           "amount": {"type": "integer"}, "memo": {"type": "string"}},
        },
    },
    {
        "name": "clock.tick",
        "description": "A tick, emitted live. No replay.",
        "delivery": ["push"],
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        "payloadSchema": {"type": "object", "properties": {"n": {"type": "integer"}}},
    },
    {
        "name": "deploy.finished",
        "description": "A deploy finished. Webhook delivery only.",
        "delivery": ["webhook"],
        "inputSchema": {"type": "object", "properties": {"service": {"type": "string"}},
                        "additionalProperties": False},
        "payloadSchema": {"type": "object", "properties": {"service": {"type": "string"},
                                                           "status": {"type": "string"}}},
    },
]
TYPES = {t["name"]: t for t in EVENT_TYPES}


def check_arguments(etype: JsonObj, args: Any) -> JsonObj:
    if args is None:
        args = {}
    if not isinstance(args, dict):
        raise RpcError(-32602, "arguments must be an object")
    props = etype["inputSchema"]["properties"]
    for k, v in args.items():
        if k not in props:
            raise RpcError(-32602, f"unknown argument {k!r}", {"argument": k})
        want = props[k]["type"]
        ok = (want == "string" and isinstance(v, str)) or (
            want == "integer" and isinstance(v, int) and not isinstance(v, bool))
        if not ok:
            raise RpcError(-32602, f"argument {k!r} must be {want}", {"argument": k})
    return args


def matches(etype: str, args: JsonObj, data: JsonObj) -> bool:
    if etype == "ledger.entry":
        if "account" in args and data.get("account") != args["account"]:
            return False
        if "min_amount" in args and int(data.get("amount", 0)) < args["min_amount"]:
            return False
    if etype == "deploy.finished" and "service" in args:
        return bool(data.get("service") == args["service"])
    return True


@dataclass
class WebhookSub:
    sid: str
    principal: str
    name: str
    args: JsonObj
    url: str
    secret: str
    expires: float | None
    next_offset: int
    queue: list[JsonObj] = field(default_factory=list)
    active: bool = True
    last_delivery: float | None = None
    last_error: str | None = None
    delivered: int = 0
    wake: threading.Event = field(default_factory=threading.Event)
    gone: bool = False


class Hub:
    """All server state, shared by both transports."""

    def __init__(self, *, heartbeat_ms: int = 15_000, next_poll_ms: int = 1_000, retention: int = 1000,
                 insecure_callbacks: bool = False, duplicate: bool = False, chunked: bool = False) -> None:
        self.chunked = chunked  # SSE with Transfer-Encoding: chunked, as Node/Bun servers send it
        self.heartbeat_ms = heartbeat_ms
        self.next_poll_ms = next_poll_ms
        self.retention = retention
        self.insecure_callbacks = insecure_callbacks
        self.duplicate = duplicate
        self.cond = threading.Condition()
        self.ledger: list[JsonObj] = []  # {"offset", "occ"}
        self.base = 0  # offset of ledger[0]
        self.next_offset = 0
        self.live: list[JsonObj] = []  # emit-only fan-out log (clock.tick, deploy.finished)
        self.live_seq = 0
        self.tick_n = 0
        self.epoch = 0  # bumped on drop_streams
        self.revoked: set[str] = set()
        self.terminations: list[tuple[int, str, JsonObj]] = []  # (live_seq, event, error)
        self.webhooks: dict[str, WebhookSub] = {}
        self.verified: set[tuple[str, str]] = set()
        self.stats: dict[str, int] = {}

    def count(self, key: str) -> None:
        self.stats[key] = self.stats.get(key, 0) + 1

    # --- emission ----------------------------------------------------------------------------

    def emit(self, name: str, data: JsonObj | None = None) -> JsonObj:
        if name not in TYPES:
            raise RpcError(-32023, f"no event type {name!r}", {"kind": "event"})
        now = time.time()
        occ: JsonObj
        with self.cond:
            if name == "ledger.entry":
                off = self.next_offset
                self.next_offset += 1
                d = {"entryId": f"e{off}", "account": "ops", "amount": 10, "memo": ""} | (data or {})
                occ = {"eventId": f"evt_ledger_{off}", "name": name, "timestamp": iso(now), "data": d}
                self.ledger.append({"offset": off, "occ": occ, "t": now})
                if len(self.ledger) > self.retention:
                    drop = len(self.ledger) - self.retention
                    self.ledger = self.ledger[drop:]
                    self.base = self.ledger[0]["offset"]
            else:
                if name == "clock.tick":
                    self.tick_n += 1
                    data = {"n": self.tick_n} | (data or {})
                self.live_seq += 1
                occ = {"eventId": f"evt_{name}_{self.live_seq}_{secrets.token_hex(3)}", "name": name,
                       "timestamp": iso(now), "data": data or {}}
                self.live.append({"seq": self.live_seq, "occ": occ})
                self.live = self.live[-1000:]
            for sub in self.webhooks.values():
                if sub.name == name and not sub.gone and matches(name, sub.args, occ["data"]):
                    sub.queue.append({"occ": occ, "offset": occ_offset(occ)})  # noqa
                    sub.wake.set()
            self.cond.notify_all()
            return occ

    def terminate(self, name: str, error: JsonObj) -> None:
        with self.cond:
            self.live_seq += 1
            self.terminations.append((self.live_seq, name, error))
            for sub in list(self.webhooks.values()):
                if sub.name == name:
                    sub.queue.append({"control": {"type": "terminated", "error": error}})
                    sub.gone = True
                    sub.wake.set()
            self.cond.notify_all()

    # --- replay ------------------------------------------------------------------------------

    def ledger_from(self, cursor: str | None, max_age_ms: Any, args: JsonObj,
                    limit: int) -> tuple[list[JsonObj], int, bool]:
        """Events at or after the cursor position, the next position, and whether we truncated."""
        with self.cond:
            if cursor is None:
                return [], self.next_offset, False
            start = parse_cursor(cursor)
            truncated = False
            if start < self.base:
                start, truncated = self.base, True
            if isinstance(max_age_ms, int) and not isinstance(max_age_ms, bool):
                floor_t = time.time() - max_age_ms / 1000
                floor = next((e["offset"] for e in self.ledger if e["t"] >= floor_t), self.next_offset)
                if floor > start:
                    start, truncated = floor, True
            out: list[JsonObj] = []
            pos = start
            full = False
            for e in self.ledger:
                if e["offset"] < start:
                    continue
                if len(out) >= limit:
                    full = True
                    break
                pos = e["offset"] + 1
                if matches("ledger.entry", args, e["occ"]["data"]):
                    out.append(e["occ"])
            if not full:
                pos = max(pos, self.next_offset) if start <= self.next_offset else start
            return out, pos, truncated


def parse_cursor(cursor: str) -> int:
    if not cursor.startswith("L"):
        raise RpcError(-32602, f"not a cursor this server minted: {cursor!r}")
    try:
        return int(cursor[1:])
    except ValueError:
        raise RpcError(-32602, f"not a cursor this server minted: {cursor!r}") from None


def occ_offset(occ: JsonObj) -> int | None:
    eid = str(occ.get("eventId", ""))
    return int(eid.rsplit("_", 1)[1]) if eid.startswith("evt_ledger_") else None


SECRET_PREFIX = "whsec_"


def valid_secret(secret: Any) -> bool:
    if not isinstance(secret, str) or not secret.startswith(SECRET_PREFIX):
        return False
    try:
        raw = base64.b64decode(secret[len(SECRET_PREFIX):], validate=True)
    except (binascii.Error, ValueError):
        return False
    return 24 <= len(raw) <= 64


def _loopback(host: str | None) -> bool:
    if host is None:
        return False
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


class Server:
    """Protocol logic. ``ctx`` carries transport facts: headers (HTTP) and the principal."""

    def __init__(self, hub: Hub) -> None:
        self.hub = hub

    # --- validation -------------------------------------------------------------------------

    def check_meta(self, params: JsonObj) -> None:
        meta = params.get("_meta")
        if not isinstance(meta, dict):
            raise RpcError(-32602, "request _meta is missing (2026-07-28 requires per-request metadata)")
        ver = meta.get("io.modelcontextprotocol/protocolVersion")
        if ver != PROTOCOL:
            raise RpcError(-32022, f"unsupported protocol version {ver!r}", {"supported": [PROTOCOL]}, 400)

    def check_headers(self, method: str, params: JsonObj, headers: dict[str, str] | None) -> None:
        if headers is None:
            return
        h = {k.lower(): v for k, v in headers.items()}

        def bad(msg: str) -> RpcError:
            self.hub.count("header_mismatch")
            return RpcError(-32020, f"Header mismatch: {msg}", http=400)

        meta = obj(params.get("_meta"))
        if h.get("mcp-protocol-version") != meta.get("io.modelcontextprotocol/protocolVersion"):
            raise bad("MCP-Protocol-Version does not match _meta protocolVersion")
        if h.get("mcp-method") != method:
            raise bad(f"Mcp-Method {h.get('mcp-method')!r} does not match {method!r}")
        if method in ("events/poll", "events/stream", "events/subscribe", "events/unsubscribe"):
            try:
                got = decode_header_value(h["mcp-name"]) if "mcp-name" in h else None
            except (ValueError, UnicodeDecodeError):
                raise bad("Mcp-Name is not valid base64") from None
            if got != params.get("name"):
                raise bad(f"Mcp-Name {got!r} does not match params.name {params.get('name')!r}")
            args = obj(params.get("arguments"))
            etype = TYPES.get(str(params.get("name")))
            if etype is not None:
                for prop, spec in etype["inputSchema"]["properties"].items():
                    hname = spec.get("x-mcp-header")
                    if not hname:
                        continue
                    hv = h.get(f"mcp-param-{hname.lower()}")
                    want = args.get(prop)
                    if want is None and hv is None:
                        continue
                    if hv is None or want is None or decode_header_value(hv) != str(want):
                        raise bad(f"Mcp-Param-{hname} does not match arguments.{prop}")

    def etype(self, params: JsonObj, mode: str) -> JsonObj:
        name = params.get("name")
        et = TYPES.get(name) if isinstance(name, str) else None
        if et is None:
            raise RpcError(-32023, f"no event type {name!r}", {"kind": "event"})
        if name in self.hub.revoked:
            raise RpcError(-32024, "Forbidden", {"reason": "Access revoked"})
        if mode not in et["delivery"]:
            raise RpcError(-32026, f"{name} does not offer {mode} delivery",
                           {"feature": "deliveryMode", "value": mode})
        return et

    # --- dispatch ---------------------------------------------------------------------------

    def handle(self, msg: JsonObj, headers: dict[str, str] | None, principal: str | None) -> JsonObj:
        method = msg.get("method")
        params = obj(msg.get("params"))
        assert isinstance(method, str)
        self.hub.count(method)
        self.check_meta(params)
        self.check_headers(method, params, headers)
        if method == "server/discover":
            return {
                "resultType": "complete",
                "supportedVersions": [PROTOCOL],
                "capabilities": {"extensions": {EVENTS_EXT: {"listChanged": False}}},
                "_meta": {"io.modelcontextprotocol/serverInfo":
                          {"name": "eventwake-dev-server", "version": __version__}},
            }
        if method == "events/list":
            return {"resultType": "complete", "events": EVENT_TYPES}
        if method == "events/poll":
            return self.poll(params)
        if method == "events/subscribe":
            return self.subscribe(params, principal)
        if method == "events/unsubscribe":
            return self.unsubscribe(params, principal)
        raise RpcError(-32601, f"method not found: {method}", http=404)

    def poll(self, params: JsonObj) -> JsonObj:
        et = self.etype(params, "poll")
        args = check_arguments(et, params.get("arguments"))
        max_events = params.get("maxEvents")
        limit = max_events if isinstance(max_events, int) and 0 < max_events <= 500 else 100
        cursor = params.get("cursor")
        events, pos, truncated = self.hub.ledger_from(
            cursor if isinstance(cursor, str) else None, params.get("maxAgeMs"), args, limit)
        if self.hub.duplicate and events:
            events = events + events[:1]
        has_more = pos < self.hub.next_offset and len(events) >= limit
        return {"resultType": "complete", "events": events, "cursor": f"L{pos}",
                "truncated": truncated, "hasMore": has_more, "nextPollMs": self.hub.next_poll_ms}

    # --- push --------------------------------------------------------------------------------

    def stream(self, msg: JsonObj, headers: dict[str, str] | None, send: Any, cancelled: threading.Event) -> None:
        """Run one events/stream until cancelled/terminated. ``send(obj)`` raises OSError if dead."""
        params = obj(msg.get("params"))
        rid = msg.get("id")
        meta = {SUB_ID_KEY: rid}
        hub = self.hub
        hub.count("events/stream")
        try:
            self.check_meta(params)
            self.check_headers("events/stream", params, headers)
            et = self.etype(params, "push")
            args = check_arguments(et, params.get("arguments"))
        except RpcError as e:
            send({"jsonrpc": "2.0", "id": rid, "error": e.obj()}, e.http)
            return
        name = et["name"]
        hb = params.get("heartbeatMs")
        grant = min(max(hb, 200), 60_000) if isinstance(hb, int) and not isinstance(hb, bool) else hub.heartbeat_ms
        grant = min(grant, hub.heartbeat_ms)
        replay = name == "ledger.entry"
        cursor = params.get("cursor")
        with hub.cond:
            epoch = hub.epoch
            live_pos = hub.live_seq
        if replay:
            backlog, pos, truncated = hub.ledger_from(cursor if isinstance(cursor, str) else None,
                                                      params.get("maxAgeMs"), args, 10_000)
        else:
            backlog, pos, truncated = [], 0, False
        cur = (lambda: f"L{pos}") if replay else (lambda: None)
        send({"jsonrpc": "2.0", "method": "notifications/events/active",
              "params": {"cursor": cur(), "truncated": truncated, "heartbeatMs": grant, "_meta": meta}}, 200)
        for occ in backlog:
            off = occ_offset(occ)
            pos = (off + 1) if off is not None else pos
            send({"jsonrpc": "2.0", "method": "notifications/events/event",
                  "params": occ | {"cursor": cur(), "_meta": meta}}, 200)
        last_sent = time.monotonic()
        while not cancelled.is_set():
            out: list[JsonObj] = []
            with hub.cond:
                hub.cond.wait(timeout=0.1)
                if hub.epoch != epoch:
                    return  # drop_streams: vanish without a final frame
                for seq, ename, err in hub.terminations:
                    if seq > live_pos and ename == name:
                        send({"jsonrpc": "2.0", "method": "notifications/events/terminated",
                              "params": {"error": err, "_meta": meta}}, 200)
                        return
                if replay:
                    for entry in hub.ledger:
                        if entry["offset"] >= pos:
                            pos = entry["offset"] + 1
                            if matches(name, args, entry["occ"]["data"]):
                                out.append(entry["occ"])
                    pos = max(pos, hub.next_offset) if not out else pos
                else:
                    for item in hub.live:
                        if item["seq"] > live_pos and item["occ"]["name"] == name:
                            out.append(item["occ"])
                    live_pos = hub.live_seq
            for occ in out:
                send({"jsonrpc": "2.0", "method": "notifications/events/event",
                      "params": occ | {"cursor": cur(), "_meta": meta}}, 200)
                if hub.duplicate:
                    send({"jsonrpc": "2.0", "method": "notifications/events/event",
                          "params": occ | {"cursor": cur(), "_meta": meta}}, 200)
                last_sent = time.monotonic()
            if time.monotonic() - last_sent >= grant / 1000:
                send({"jsonrpc": "2.0", "method": "notifications/events/heartbeat",
                      "params": {"cursor": cur(), "_meta": meta}}, 200)
                last_sent = time.monotonic()

    # --- webhook -----------------------------------------------------------------------------

    def _sid(self, principal: str, url: str, name: str, args: JsonObj) -> str:
        raw = json.dumps([principal, url, name, args], sort_keys=True, separators=(",", ":"))
        return "sub_" + hashlib.sha256(raw.encode()).hexdigest()[:16]

    def subscribe(self, params: JsonObj, principal: str | None) -> JsonObj:
        if principal is None:
            raise RpcError(-32024, "Forbidden: events/subscribe needs an authenticated principal")
        et = self.etype(params, "webhook")
        args = check_arguments(et, params.get("arguments"))
        delivery = params.get("delivery")
        if not isinstance(delivery, dict) or delivery.get("mode") != "webhook":
            raise RpcError(-32602, "delivery.mode must be webhook")
        url = delivery.get("url")
        parts = urllib.parse.urlsplit(url) if isinstance(url, str) else None
        if parts is None or not parts.hostname:
            raise RpcError(-32602, "delivery.url is malformed")
        if parts.scheme != "https" and not (self.hub.insecure_callbacks and _loopback(parts.hostname)):
            raise RpcError(-32602, "delivery.url must be https")
        if not valid_secret(delivery.get("secret")):
            raise RpcError(-32602, "delivery.secret must be whsec_ + base64 of 24-64 bytes")
        assert isinstance(url, str)
        secret = str(delivery["secret"])
        ttl = params.get("ttlMs", 300_000)
        if "ttlMs" in params and ttl is None:
            expires: float | None = None
        else:
            if not isinstance(ttl, int) or isinstance(ttl, bool):
                raise RpcError(-32602, "ttlMs must be an integer or null")
            expires = time.time() + min(max(ttl, 2_000), 3_600_000) / 1000
        name = et["name"]
        sid = self._sid(principal, url, name, args)
        cursor = params.get("cursor")
        hub = self.hub
        if (principal, url) not in hub.verified:
            self._verify(url, secret, sid)
            hub.verified.add((principal, url))
        with hub.cond:
            existing = hub.webhooks.get(sid)
            refresh = existing is not None and not existing.gone
            truncated = False
            if name == "ledger.entry":
                if refresh:
                    assert existing is not None
                    pos = existing.next_offset
                else:
                    backlog, pos, truncated = hub.ledger_from(
                        cursor if isinstance(cursor, str) else None, params.get("maxAgeMs"), args, 10_000)
            else:
                backlog, pos = [], 0
            if refresh:
                assert existing is not None
                existing.secret, existing.expires, existing.active = secret, expires, True
                sub = existing
            else:
                sub = WebhookSub(sid, principal, name, args, url, secret, expires, pos)
                sub.queue = [{"occ": o, "offset": occ_offset(o)} for o in backlog]
                hub.webhooks[sid] = sub
                threading.Thread(target=self._deliver_loop, args=(sub,), daemon=True).start()
            sub.wake.set()
        res: JsonObj = {"resultType": "complete", "id": sid,
                        "refreshBefore": None if expires is None else iso(expires),
                        "cursor": f"L{sub.next_offset}" if name == "ledger.entry" else None,
                        "truncated": truncated}
        if refresh:
            res["deliveryStatus"] = {"active": sub.active, "lastError": sub.last_error,
                                     "lastDeliveryAt": None if sub.last_delivery is None else iso(sub.last_delivery)}
        return res

    def unsubscribe(self, params: JsonObj, principal: str | None) -> JsonObj:
        if principal is None:
            raise RpcError(-32024, "Forbidden: events/unsubscribe needs an authenticated principal")
        et = TYPES.get(str(params.get("name")))
        if et is None:
            raise RpcError(-32023, "no such event type", {"kind": "event"})
        args = check_arguments(et, params.get("arguments"))
        delivery = obj(params.get("delivery"))
        sid = self._sid(principal, str(delivery.get("url")), et["name"], args)
        with self.hub.cond:
            sub = self.hub.webhooks.pop(sid, None)
        if sub is None:
            raise RpcError(-32023, "no such subscription", {"kind": "subscription"})
        sub.gone = True
        sub.wake.set()
        return {"resultType": "complete"}

    def _post(self, sub_url: str, secret: str, sid: str, body: JsonObj, msg_id: str) -> tuple[int, bytes]:
        raw = json.dumps(body).encode("utf-8")
        ts = int(time.time())
        req = urllib.request.Request(sub_url, data=raw, method="POST", headers={
            "Content-Type": "application/json", "webhook-id": msg_id, "webhook-timestamp": str(ts),
            "webhook-signature": sign(secret, msg_id, ts, raw), "X-MCP-Subscription-Id": sid,
        })
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, resp.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()
        except (urllib.error.URLError, OSError):
            return 0, b""

    def _verify(self, url: str, secret: str, sid: str) -> None:
        challenge = secrets.token_urlsafe(16)
        status, raw = self._post(url, secret, sid, {"type": "verification", "challenge": challenge},
                                 f"msg_verification_{secrets.token_hex(8)}")
        if status == 0:
            raise RpcError(-32027, "callback endpoint unreachable", {"reason": "connection_refused"})
        try:
            echoed = json.loads(raw).get("challenge")
        except (json.JSONDecodeError, AttributeError):
            echoed = None
        if not (200 <= status < 300) or not isinstance(echoed, str) or not secrets.compare_digest(echoed, challenge):
            raise RpcError(-32027, "callback endpoint failed verification", {"reason": "challenge_failed"})

    def _deliver_loop(self, sub: WebhookSub) -> None:
        while True:
            sub.wake.wait(timeout=0.5)
            sub.wake.clear()
            if sub.expires is not None and time.time() > sub.expires:
                with self.hub.cond:
                    self.hub.webhooks.pop(sub.sid, None)
                return
            while sub.queue:
                item = sub.queue[0]
                if "control" in item:
                    body = item["control"]
                    msg_id = f"msg_{body['type']}_{secrets.token_hex(8)}"
                else:
                    occ = item["occ"]
                    off = item.get("offset")
                    if off is not None:
                        sub.next_offset = max(sub.next_offset, off + 1)
                    body = occ | {"cursor": f"L{sub.next_offset}" if sub.name == "ledger.entry" else None}
                    msg_id = f"msg_{secrets.token_hex(12)}"
                copies = 2 if self.hub.duplicate and "control" not in item else 1
                for _ in range(copies):
                    status = 0
                    for attempt in range(4):
                        status, _ = self._post(sub.url, sub.secret, sub.sid, body, msg_id)
                        if 200 <= status < 300 or status in (410, 413):
                            break
                        time.sleep(0.2 * (2 ** attempt))
                    if 200 <= status < 300:
                        sub.last_delivery, sub.last_error = time.time(), None
                        sub.delivered += 1
                    else:
                        sub.last_error = "connection_refused" if status == 0 else (
                            "http_5xx" if status >= 500 else "http_4xx")
                sub.queue.pop(0)
            if sub.gone:
                return


# --- transports -------------------------------------------------------------------------------


def serve_stdio(hub: Hub, out: IO[str] | None = None, inp: IO[str] | None = None) -> None:
    server = Server(hub)
    out = out or sys.stdout
    inp = inp or sys.stdin
    lock = threading.Lock()
    streams: dict[Any, threading.Event] = {}

    def send(obj: JsonObj, _status: int = 200) -> None:
        with lock:
            out.write(json.dumps(obj, separators=(",", ":")) + "\n")
            out.flush()

    for line in inp:
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(msg, dict):
            continue
        method = msg.get("method")
        if method == "notifications/cancelled":
            params = obj(msg.get("params"))
            ev = streams.pop(params.get("requestId"), None)
            if ev is not None:
                ev.set()
            continue
        if not isinstance(method, str) or "id" not in msg:
            continue
        if method == "events/stream":
            ev = threading.Event()
            streams[msg["id"]] = ev
            threading.Thread(target=_guard_stream, args=(server, msg, None, send, ev), daemon=True).start()
            continue
        try:
            send({"jsonrpc": "2.0", "id": msg["id"], "result": server.handle(msg, None, "local-user")})
        except RpcError as e:
            send({"jsonrpc": "2.0", "id": msg["id"], "error": e.obj()})
    for ev in streams.values():
        ev.set()


def _guard_stream(server: Server, msg: JsonObj, headers: dict[str, str] | None, send: Any,
                  ev: threading.Event) -> None:
    try:
        server.stream(msg, headers, send, ev)
    except (OSError, ValueError):
        pass


def make_http(hub: Hub, host: str = "127.0.0.1", port: int = 0) -> ThreadingHTTPServer:
    server = Server(hub)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        server_version = "eventwake-dev-server"
        sys_version = ""

        def log_message(self, *_: Any) -> None:
            pass

        def _json(self, status: int, obj: JsonObj) -> None:
            data = json.dumps(obj).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_POST(self) -> None:  # noqa: N802
            n = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(n)
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                self._json(400, {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "parse error"}})
                return
            if self.path == "/_control":
                self._json(200, control(hub, msg))
                return
            if self.path != "/mcp":
                self._json(404, {"error": "not found"})
                return
            headers = dict(self.headers.items())
            auth = self.headers.get("Authorization", "")
            principal = auth[7:] if auth.startswith("Bearer ") and len(auth) > 7 else None
            if not isinstance(msg, dict) or "id" not in msg:
                self.send_response(202)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            if msg.get("method") == "events/stream":
                self._stream(msg, headers)
                return
            try:
                self._json(200, {"jsonrpc": "2.0", "id": msg["id"],
                                 "result": server.handle(msg, headers, principal)})
            except RpcError as e:
                self._json(e.http, {"jsonrpc": "2.0", "id": msg["id"], "error": e.obj()})

        def _stream(self, msg: JsonObj, headers: dict[str, str]) -> None:
            started = False

            def send(obj: JsonObj, status: int = 200) -> None:
                nonlocal started
                if not started and "error" in obj:
                    self._json(status, obj)
                    started = True
                    return
                if not started:
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream")
                    self.send_header("Cache-Control", "no-store")
                    if hub.chunked:
                        self.send_header("Transfer-Encoding", "chunked")
                    else:
                        self.send_header("Connection", "close")
                    self.end_headers()
                    started = True
                frame = b"data: " + json.dumps(obj).encode("utf-8") + b"\n\n"
                if hub.chunked:
                    frame = f"{len(frame):x}\r\n".encode() + frame + b"\r\n"
                self.wfile.write(frame)
                self.wfile.flush()

            self.close_connection = True
            _guard_stream(server, msg, headers, send, threading.Event())

    httpd = FastHTTPServer((host, port), Handler)
    httpd.daemon_threads = True
    return httpd


def control(hub: Hub, msg: Any) -> JsonObj:
    if not isinstance(msg, dict):
        return {"error": "expected an object"}
    action = msg.get("action")
    try:
        if action == "emit":
            n = int(msg.get("count", 1))
            occs = [hub.emit(str(msg.get("event", "ledger.entry")), msg.get("data")) for _ in range(n)]
            return {"ok": True, "emitted": [o["eventId"] for o in occs]}
        if action == "terminate":
            err = msg.get("error") or {"code": -32024, "message": "Forbidden", "data": {"reason": "Access revoked"}}
            name = str(msg.get("event", "ledger.entry"))
            if err.get("code") == -32024:
                hub.revoked.add(name)
            hub.terminate(name, err)
            return {"ok": True}
        if action == "drop_streams":
            with hub.cond:
                hub.epoch += 1
                hub.cond.notify_all()
            return {"ok": True}
        if action == "stats":
            with hub.cond:
                subs = {s.sid: {"name": s.name, "delivered": s.delivered, "active": s.active,
                                "expires": s.expires, "gone": s.gone} for s in hub.webhooks.values()}
            return {"ok": True, "stats": dict(hub.stats), "webhooks": subs, "next_offset": hub.next_offset}
    except RpcError as e:
        return {"error": e.message}
    return {"error": f"unknown action {action!r}"}


def auto_emit(hub: Hub, every: float, stop: threading.Event) -> None:
    n = 0
    while not stop.wait(every):
        n += 1
        hub.emit("ledger.entry", {"amount": 10 + n % 7, "memo": f"auto entry {n}"})
        hub.emit("clock.tick")


def run(http_port: int | None, every: float | None, heartbeat_ms: int, insecure_callbacks: bool,
        duplicate: bool, control_port: int | None, retention: int) -> int:
    hub = Hub(heartbeat_ms=heartbeat_ms, insecure_callbacks=insecure_callbacks, duplicate=duplicate,
              retention=retention)
    stop = threading.Event()
    if every:
        threading.Thread(target=auto_emit, args=(hub, every, stop), daemon=True).start()
    if http_port is not None:
        httpd = make_http(hub, "127.0.0.1", http_port)
        sys.stderr.write(f"eventwake dev-server: http://127.0.0.1:{httpd.server_address[1]}/mcp "
                         f"(control: POST /_control)\n")
        sys.stderr.flush()
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        return 0
    if control_port is not None:
        ctl = make_http(hub, "127.0.0.1", control_port)
        threading.Thread(target=ctl.serve_forever, daemon=True).start()
    serve_stdio(hub)
    stop.set()
    return 0


__all__ = ["Hub", "Server", "make_http", "serve_stdio", "control", "run"]
