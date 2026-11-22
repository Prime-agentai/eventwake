"""Run watches: one runner thread per subscription, one dispatcher feeding the sinks.

Client duties from the Events spec that live here:

* Poll: honour ``nextPollMs`` with a floor (default 1 s), drain immediately while ``hasMore``,
  pass ``maxAgeMs`` only with a non-null cursor, record ``truncated`` as a gap.
* Push: confirmation first, heartbeat advances the cursor, ``3 x heartbeatMs`` of silence on HTTP
  means dead, reconnect with the last cursor after a random jitter, ``notifications/events/error``
  is transient and ``notifications/events/terminated`` ends the subscription.
* Webhook: client-generated ``whsec_`` secret, refresh before ``refreshBefore``, record
  ``deliveryStatus``, eager ``events/unsubscribe`` on clean shutdown.
* Everywhere: ``eventId`` dedupe scoped to the subscription, cursor ``null``/absent never replayed,
  ``NotFound`` / ``Unsupported(schema_changed)`` -> fetch ``events/list`` again and resubscribe,
  ``HeaderMismatch`` -> fetch ``events/list`` again and retry with the current headers.
"""

from __future__ import annotations

import random
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .config import Config, Watch, endpoint_identity
from .mcp import (
    obj, opt_str,
    FORBIDDEN, HEADER_MISMATCH, NOT_FOUND, UNSUPPORTED, Endpoint, EventType, McpClient, McpError,
    StreamClosed, TransportError,
)
from .receiver import Receiver, Route, new_secret
from .sinks import Sink, SinkError, log
from .state import Delivered, Store, subscription_key

JsonObj = dict[str, Any]
DEFAULT_POLL_S = 30.0
HEARTBEAT_HINT_MS = 30_000


class Terminated(Exception):
    """The subscription ended for good (Forbidden, removed event type, invalid arguments...)."""


class ClientPool:
    """One McpClient per server label, shared by every watch on that server."""

    def __init__(self, timeout: float = 30.0) -> None:
        self.timeout = timeout
        self._clients: dict[str, McpClient] = {}
        self._lock = threading.Lock()

    def get(self, ep: Endpoint) -> McpClient:
        with self._lock:
            c = self._clients.get(ep.label)
            if c is None:
                c = McpClient(ep, self.timeout)
                c.connect()
                self._clients[ep.label] = c
            return c

    def drop(self, ep: Endpoint, client: McpClient) -> None:
        """Forget a broken client so the next get() reconnects (restarts a stdio server)."""
        with self._lock:
            if self._clients.get(ep.label) is client:
                del self._clients[ep.label]
        try:
            client.close()
        except OSError:
            pass

    def close(self) -> None:
        with self._lock:
            for c in self._clients.values():
                try:
                    c.close()
                except OSError:
                    pass
            self._clients.clear()


def choose_mode(watch: Watch, et: EventType, webhook_ready: bool) -> str:
    if watch.mode != "auto":
        if watch.mode not in et.delivery:
            raise Terminated(f"{watch.title}: event type offers {list(et.delivery)}, not {watch.mode!r}")
        if watch.mode == "webhook" and not webhook_ready:
            raise Terminated(f"{watch.title}: mode webhook needs [webhook] public_url in eventwake.toml")
        return watch.mode
    for mode in ("push", "poll"):
        if mode in et.delivery:
            return mode
    if "webhook" in et.delivery:
        if webhook_ready:
            return "webhook"
        raise Terminated(f"{watch.title}: this event type is webhook-only; set [webhook] public_url "
                         "(an https URL that reaches `eventwake run`'s receiver)")
    raise Terminated(f"{watch.title}: no delivery mode eventwake can use")


@dataclass
class Runner:
    watch: Watch
    endpoint: Endpoint
    store: Store
    pool: ClientPool
    wake: Callable[[], None]
    config: Config
    receiver: Receiver | None = None
    stop: threading.Event = field(default_factory=threading.Event)
    stop_cycle: threading.Event = field(default_factory=threading.Event)
    key: str = ""
    mode: str = "?"
    etype: EventType | None = None
    _thread: threading.Thread | None = None
    _first: bool = True

    def __post_init__(self) -> None:
        self.key = subscription_key(endpoint_identity(self.endpoint), self.watch.event, self.watch.arguments)

    # --- lifecycle ------------------------------------------------------------------------

    def start(self) -> None:
        # Record what this watch is before anything can fail, so `status` can always name it.
        self.store.register(self.key, self.endpoint.label, endpoint_identity(self.endpoint),
                            self.watch.event, self.watch.arguments, "?")
        self._thread = threading.Thread(target=self._main, daemon=True, name=f"watch-{self.watch.title}")
        self._thread.start()

    def join(self, timeout: float | None = None) -> None:
        if self._thread is not None:
            self._thread.join(timeout)

    def alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _sleep(self, seconds: float) -> bool:
        """Sleep unless stopped. True if we should keep going."""
        return not self.stop.wait(max(0.0, seconds))

    def _main(self) -> None:
        backoff = 1.0
        while not self.stop.is_set():
            client: McpClient | None = None
            try:
                client = self.pool.get(self.endpoint)
                self._resolve(client)
                assert self.etype is not None
                mode = choose_mode(self.watch, self.etype, self._webhook_ready())
                if mode != self.mode:
                    self.mode = mode
                    self.store.register(self.key, self.endpoint.label, endpoint_identity(self.endpoint),
                                        self.watch.event, self.watch.arguments, mode)
                    log(f"{self.watch.title}: subscribed by {mode}")
                {"poll": self._poll, "push": self._push, "webhook": self._webhook}[mode](client)
                backoff = 1.0
            except Terminated as e:
                log(f"{self.watch.title}: stopped: {e}")
                self.store.note(self.key, terminated={"reason": str(e), "at": time.time()},
                                last_error=str(e))
                return
            except McpError as e:
                if e.permanent and e.code not in (NOT_FOUND, UNSUPPORTED):
                    log(f"{self.watch.title}: stopped: {e}")
                    self.store.note(self.key, terminated={"reason": str(e), "at": time.time()},
                                    last_error=str(e))
                    return
                self._transient(f"{e}", backoff)
                backoff = min(backoff * 2, 60.0)
            except (TransportError, OSError) as e:
                if client is not None:
                    self.pool.drop(self.endpoint, client)
                self._transient(str(e), backoff)
                backoff = min(backoff * 2, 60.0)

    def _transient(self, msg: str, backoff: float) -> None:
        self.store.note(self.key, last_error=msg, last_error_at=time.time())
        if self.stop.is_set():
            return
        delay = backoff * (0.5 + random.random())  # jitter, so a server restart is not a stampede
        log(f"{self.watch.title}: {msg}; retrying in {delay:.1f}s")
        self._sleep(delay)

    def _webhook_ready(self) -> bool:
        return self.receiver is not None and self.config.webhook.public_url is not None

    def _resolve(self, client: McpClient, force: bool = False) -> None:
        if self.etype is not None and not force:
            return
        types, warnings = client.list_events()
        for w in warnings:
            log(f"{self.endpoint.label}: {w}")
        match = [t for t in types if t.name == self.watch.event]
        if not match:
            names = ", ".join(t.name for t in types) or "(none)"
            raise Terminated(f"server {self.endpoint.label!r} has no event type {self.watch.event!r}. "
                             f"It offers: {names}")
        self.etype = match[0]

    def _max_age_ms(self, cursor: str | None) -> int | None:
        if cursor is None or self.watch.max_age is None or not self._first:
            return None
        return int(self.watch.max_age * 1000)

    def _on_mcp_error(self, client: McpClient, e: McpError) -> None:
        """Rediscover on schema/name changes, then let the caller resubscribe."""
        if e.code == HEADER_MISMATCH:
            self._resolve(client, force=True)
            return
        if e.code == NOT_FOUND or (e.code == UNSUPPORTED and isinstance(e.data, dict)
                                   and e.data.get("reason") == "schema_changed"):
            self._resolve(client, force=True)  # raises Terminated if the type is really gone
            if e.code == UNSUPPORTED:
                self.store.set_cursor(self.key, None, truncated=True)
            return
        raise e

    def _accept(self, occurrences: list[JsonObj], cursor: str | None) -> None:
        clean = [{k: v for k, v in o.items() if k != "_meta"} for o in occurrences if isinstance(o, dict)]
        got = self.store.accept(self.key, self.endpoint.label, self.watch.event, clean, cursor)
        if got:
            self.wake()

    # --- poll -----------------------------------------------------------------------------

    def _poll(self, client: McpClient) -> None:
        assert self.etype is not None
        cursor = self.store.cursor(self.key)
        retried_header = False
        while not self.stop.is_set():
            try:
                res = client.poll(self.etype, self.watch.arguments, cursor, self._max_age_ms(cursor), 100)
            except McpError as e:
                if e.code == HEADER_MISMATCH and retried_header:
                    raise
                retried_header = e.code == HEADER_MISMATCH
                self._on_mcp_error(client, e)
                cursor = self.store.cursor(self.key)
                continue
            retried_header = False
            self._first = False
            events = res.get("events") if isinstance(res.get("events"), list) else []
            raw_cursor = res.get("cursor")
            cursor = raw_cursor if isinstance(raw_cursor, str) else None  # absent means null
            if res.get("truncated") is True and cursor is not None:
                self.store.set_cursor(self.key, cursor, truncated=True)
                log(f"{self.watch.title}: server reports a gap (truncated); some events were skipped")
            if events:
                self._accept(events, cursor)
            else:
                self.store.set_cursor(self.key, cursor)
            self.store.note(self.key, last_ok_at=time.time(), last_error=None)
            if res.get("hasMore") is True and events:
                continue
            nxt = res.get("nextPollMs")
            wait = nxt / 1000 if isinstance(nxt, (int, float)) and not isinstance(nxt, bool) else DEFAULT_POLL_S
            if not self._sleep(max(self.config.poll_floor, wait)):
                return

    # --- push -----------------------------------------------------------------------------

    def _push(self, client: McpClient) -> None:
        assert self.etype is not None
        cursor = self.store.cursor(self.key)
        try:
            stream = client.open_stream(self.etype, self.watch.arguments, cursor,
                                        self._max_age_ms(cursor), HEARTBEAT_HINT_MS)
        except McpError as e:
            self._on_mcp_error(client, e)
            return
        http = self.endpoint.kind == "http"
        heartbeat_s: float | None = None
        last = time.monotonic()
        confirmed = False
        try:
            while not self.stop.is_set():
                try:
                    msg = stream.next(timeout=1.0)
                except TimeoutError:
                    idle = time.monotonic() - last
                    if not confirmed and idle > client.timeout:
                        raise TransportError("no notifications/events/active within "
                                             f"{client.timeout:g}s of events/stream") from None
                    if http and heartbeat_s is not None and idle > 3 * heartbeat_s:
                        raise TransportError(f"push stream silent for {idle:.0f}s "
                                             f"(3 x heartbeat {heartbeat_s:g}s); reconnecting") from None
                    continue
                last = time.monotonic()
                method = msg.get("method")
                if method is None:  # the final frame: a result (graceful close) or an error
                    if "error" in msg:
                        err = McpError.from_obj(msg["error"])
                        self._on_mcp_error(client, err)
                        return
                    log(f"{self.watch.title}: server closed the stream; reconnecting")
                    self._sleep(random.uniform(0.2, 1.5))
                    return
                params = obj(msg.get("params"))
                if method == "notifications/events/active":
                    confirmed = True
                    self._first = False
                    hb = params.get("heartbeatMs")
                    if isinstance(hb, (int, float)) and not isinstance(hb, bool) and hb > 0:
                        heartbeat_s = hb / 1000
                    c = params.get("cursor")
                    truncated = params.get("truncated") is True and isinstance(c, str)
                    if truncated:
                        log(f"{self.watch.title}: server reports a gap (truncated); some events were skipped")
                    self.store.set_cursor(self.key, c if isinstance(c, str) else None, truncated=truncated)
                    self.store.note(self.key, last_ok_at=time.time(), last_error=None)
                elif method == "notifications/events/event":
                    c = params.get("cursor")
                    self._accept([params], c if isinstance(c, str) else None)
                elif method == "notifications/events/heartbeat":
                    c = params.get("cursor")
                    self.store.set_cursor(self.key, c if isinstance(c, str) else None)
                elif method == "notifications/events/error":
                    upstream = obj(params.get("error"))
                    detail = f"upstream: {upstream.get('message')} {upstream.get('data') or ''}".strip()
                    self.store.note(self.key, last_error=detail, last_error_at=time.time())
                elif method == "notifications/events/terminated":
                    err = McpError.from_obj(params.get("error"))
                    log(f"{self.watch.title}: server terminated the subscription: {err}")
                    self._on_mcp_error(client, err)  # rediscovers, or raises (Forbidden...)
                    return
        except StreamClosed as e:
            raise TransportError(f"push stream closed: {e}") from e
        finally:
            stream.close()

    # --- webhook --------------------------------------------------------------------------

    def _webhook(self, client: McpClient) -> None:
        assert self.etype is not None and self.receiver is not None
        spec = self.config.webhook
        assert spec.public_url is not None
        rec = self.store.sub(self.key)
        hook = obj(rec.get("webhook"))
        secret = opt_str(hook.get("secret")) or new_secret()
        url = f"{spec.public_url}/hooks/{self.key}"
        terminated: list[JsonObj] = []


        def on_terminated(err: JsonObj) -> None:
            terminated.append(err)
            self.stop_cycle.set()

        route = Route(self.key, self.endpoint.label, self.watch.event, [secret],
                      on_event=lambda _d: self.wake(), on_terminated=on_terminated)
        self.receiver.add(route)
        self.store.note(self.key, webhook={"secret": secret, "url": url, "id": hook.get("id")})
        try:
            while not self.stop.is_set():
                self.stop_cycle.clear()
                cursor = self.store.cursor(self.key)
                try:
                    res = client.subscribe(self.etype, self.watch.arguments, url, secret, cursor,
                                           None if spec.ttl is None else int(spec.ttl * 1000),
                                           self._max_age_ms(cursor), no_expiry=spec.no_expiry)
                except McpError as e:
                    self._on_mcp_error(client, e)
                    continue
                self._first = False
                sub_id = res.get("id")
                route.sub_id = sub_id if isinstance(sub_id, str) else None
                c = res.get("cursor")
                truncated = res.get("truncated") is True and isinstance(c, str)
                self.store.set_cursor(self.key, c if isinstance(c, str) else None, truncated=truncated)
                status = res.get("deliveryStatus")
                self.store.note(self.key, webhook={"secret": secret, "url": url, "id": route.sub_id,
                                                   "refresh_before": res.get("refreshBefore")},
                                delivery_status=status if isinstance(status, dict) else None,
                                last_ok_at=time.time(), last_error=None)
                wait = refresh_delay(res.get("refreshBefore"))
                # Wait for the refresh time, a stop, or a terminated envelope.
                deadline = time.monotonic() + wait
                while not self.stop.is_set() and not self.stop_cycle.is_set():
                    if self.stop.wait(min(1.0, max(0.0, deadline - time.monotonic()))):
                        break
                    if time.monotonic() >= deadline:
                        break
                if terminated:
                    err = McpError.from_obj(terminated.pop())
                    log(f"{self.watch.title}: server terminated the webhook subscription: {err}")
                    self._on_mcp_error(client, err)
        finally:
            if self.stop.is_set():
                try:
                    client.unsubscribe(self.etype, self.watch.arguments, url)
                    log(f"{self.watch.title}: unsubscribed webhook")
                except (McpError, TransportError) as e:
                    log(f"{self.watch.title}: unsubscribe failed ({e}); the server's TTL will expire it")
            self.receiver.remove(self.key)



def refresh_delay(refresh_before: Any, now: float | None = None) -> float:
    """Seconds until we should refresh: 80% of the way to the grant, at least 30 s early when the
    grant allows it. ``null`` (no expiry) still refreshes every 6 h, as the spec recommends."""
    if not isinstance(refresh_before, str):
        return 6 * 3600.0
    t = iso_epoch(refresh_before)
    if t is None:
        return 60.0
    remaining = t - (now if now is not None else time.time())
    if remaining <= 2:
        return 1.0
    return max(1.0, min(remaining * 0.8, remaining - 30) if remaining > 60 else remaining * 0.5)


class Dispatcher:
    """Moves accepted events from the state file's pending queue into sinks, in batches."""

    def __init__(self, store: Store, sinks: dict[str, Sink], titles: dict[str, str],
                 on_result: Callable[[str, list[Delivered], JsonObj], None] | None = None) -> None:
        self.store = store
        self.sinks = sinks
        self.titles = titles
        self.on_result = on_result
        self.stop = threading.Event()
        self._wake = threading.Event()
        self._paused: dict[str, float] = {}
        self._thread = threading.Thread(target=self._main, daemon=True, name="eventwake-dispatch")

    def start(self) -> None:
        self._thread.start()

    def wake(self) -> None:
        self._wake.set()

    def join(self, timeout: float | None = None) -> None:
        self._thread.join(timeout)

    def drain_once(self, now: float | None = None, force: bool = False) -> float:
        """Deliver every batch that is due. Returns seconds until the next one may be due."""
        now = time.time() if now is None else now
        pending = self.store.pending()
        by_key: dict[str, list[Delivered]] = {}
        orphans: set[int] = set()
        for d in pending:
            if d.key in self.sinks:
                by_key.setdefault(d.key, []).append(d)
            else:
                orphans.add(d.seq)
        if orphans:
            log(f"dropping {len(orphans)} pending event(s) whose watch is no longer configured")
            self.store.ack(orphans)
        next_due = 3600.0
        for key, items in by_key.items():
            if self.stop.is_set():
                break
            sink = self.sinks[key]
            if not sink.ready():
                next_due = min(next_due, 0.5)
                continue
            paused = self._paused.get(key, 0.0) - now
            if paused > 0:
                next_due = min(next_due, paused)
                continue
            age = now - _epoch(items[0].received_at, now)
            if not force and age < sink.batch_window and len(items) < sink.max_batch:
                next_due = min(next_due, sink.batch_window - age)
                continue
            slot = sink.next_slot()
            if slot > 0 and not force:
                next_due = min(next_due, slot)
                continue
            batch = items[: sink.max_batch]
            try:
                result = sink.deliver(batch)
            except SinkError as e:
                log(f"{self.titles.get(key, key)}: {e}; keeping {len(batch)} event(s) pending")
                self.store.note(key, last_sink_error=str(e))
                self._paused[key] = now + 30.0
                next_due = min(next_due, 30.0)
                continue
            self.store.ack({d.seq for d in batch})
            self.store.note(key, last_run=result | {"at": time.time()}, last_sink_error=None)
            if self.on_result is not None:
                self.on_result(key, batch, result)
            if len(items) > len(batch):
                next_due = 0.0
        return max(0.05, next_due)

    def _main(self) -> None:
        while not self.stop.is_set():
            delay = self.drain_once()
            self._wake.wait(delay)
            self._wake.clear()


def iso_epoch(iso: str) -> float | None:
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def _epoch(iso: str, default: float) -> float:
    t = iso_epoch(iso)
    return default if t is None else t
