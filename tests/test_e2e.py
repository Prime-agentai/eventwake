"""End-to-end: eventwake against the strict dev server, over HTTP (in-process) and stdio."""

from __future__ import annotations

import base64
import json
import socket
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from conftest import Collect, DevHttp, config, stdio_endpoint, wait_for

from eventwake.app import App
from eventwake.config import Watch, WebhookSpec
from eventwake.mcp import McpClient, McpError, TransportError
from eventwake.state import Store


def _app(tmp: Path, servers: dict, watches: list[Watch], sink: Collect,
         webhook: WebhookSpec | None = None) -> App:
    app = App(config(tmp, servers, watches, webhook), tmp, lambda _w, _k: sink)
    app.start()
    return app


def _sub(app: App) -> dict:
    return app.store.sub(app.runners[0].key)


def _subscribed(app: App) -> bool:
    return _sub(app).get("last_ok_at") is not None


def test_push_http_delivers_persists_and_resumes_after_restart(tmp_path: Path, dev: DevHttp) -> None:
    sink = Collect()
    watch = Watch("dev", "ledger.entry", {"account": "ops"})
    app = _app(tmp_path, {"dev": dev.endpoint()}, [watch], sink)
    try:
        assert app.runners[0].mode in ("?", "push")
        wait_for(lambda: _subscribed(app), what="subscription")
        dev.control(action="emit", count=3)
        dev.control(action="emit", data={"account": "other"})  # filtered by the server
        wait_for(lambda: len(sink.events) == 3, what="3 events")
        assert app.runners[0].mode == "push"
    finally:
        app.stop()
    assert sink.ids() == ["evt_ledger_0", "evt_ledger_1", "evt_ledger_2"]
    cursor = Store(tmp_path / "state").cursor(app.runners[0].key)
    assert cursor is not None and cursor.startswith("L")
    # Events that happen while eventwake is down are replayed from the cursor on restart.
    dev.control(action="emit", count=2)
    sink2 = Collect()
    app2 = _app(tmp_path, {"dev": dev.endpoint()}, [watch], sink2)
    try:
        wait_for(lambda: len(sink2.events) == 2, what="replayed events")
    finally:
        app2.stop()
    assert sink2.ids() == ["evt_ledger_4", "evt_ledger_5"]
    assert dev.hub.stats.get("header_mismatch", 0) == 0


def test_max_age_bounds_replay_and_reports_gap(tmp_path: Path, dev: DevHttp) -> None:
    sink = Collect()
    watch = Watch("dev", "ledger.entry", {}, max_age=0.5)
    app = _app(tmp_path, {"dev": dev.endpoint()}, [watch], sink)
    try:
        wait_for(lambda: _subscribed(app))
    finally:
        app.stop()
    dev.control(action="emit", count=4)  # old by the time we come back
    time.sleep(1.0)
    sink2 = Collect()
    app2 = _app(tmp_path, {"dev": dev.endpoint()}, [watch], sink2)
    try:
        wait_for(lambda: (_sub(app2).get("gaps") or 0) >= 1, what="gap recorded")
        dev.control(action="emit")
        wait_for(lambda: len(sink2.events) == 1)
    finally:
        app2.stop()
    assert sink2.ids() == ["evt_ledger_4"]


def test_poll_drains_backlog_with_has_more(tmp_path: Path, dev: DevHttp) -> None:
    sink = Collect()
    watch = Watch("dev", "ledger.entry", {}, mode="poll")
    app = _app(tmp_path, {"dev": dev.endpoint()}, [watch], sink)
    try:
        wait_for(lambda: _subscribed(app))
        polls_before = dev.hub.stats.get("events/poll", 0)
        dev.control(action="emit", count=250)
        wait_for(lambda: len(sink.events) == 250, what="250 events")
        polls = dev.hub.stats["events/poll"] - polls_before
    finally:
        app.stop()
    assert len(set(sink.ids())) == 250
    assert polls <= 8  # 100 per page, drained back-to-back, not one per nextPollMs


def test_duplicates_are_dropped_per_subscription(tmp_path: Path, make_dev) -> None:  # type: ignore[no-untyped-def]
    dev = make_dev(duplicate=True)
    for mode in ("push", "poll"):
        sink = Collect()
        app = _app(tmp_path / mode, {"dev": dev.endpoint()}, [Watch("dev", "ledger.entry", {}, mode=mode)], sink)
        try:
            wait_for(lambda: _subscribed(app))
            dev.control(action="emit", count=3)
            wait_for(lambda: (_sub(app).get("duplicates") or 0) >= 1, what=f"{mode} duplicates")
            time.sleep(0.3)
        finally:
            app.stop()
        assert len(sink.ids()) == len(set(sink.ids())) == 3, mode


def test_push_reconnects_after_drop_without_loss(tmp_path: Path, dev: DevHttp) -> None:
    sink = Collect()
    app = _app(tmp_path, {"dev": dev.endpoint()}, [Watch("dev", "ledger.entry", {})], sink)
    try:
        wait_for(lambda: _subscribed(app))
        dev.control(action="emit", count=2)
        wait_for(lambda: len(sink.events) == 2)
        streams = dev.hub.stats["events/stream"]
        dev.control(action="drop_streams")
        dev.control(action="emit", count=2)  # emitted while the stream is down
        wait_for(lambda: len(sink.events) == 4, timeout=20, what="events after reconnect")
        assert dev.hub.stats["events/stream"] > streams
    finally:
        app.stop()
    assert sink.ids() == [f"evt_ledger_{i}" for i in range(4)]


def test_terminated_forbidden_stops_the_watch(tmp_path: Path, dev: DevHttp) -> None:
    sink = Collect()
    app = _app(tmp_path, {"dev": dev.endpoint()}, [Watch("dev", "ledger.entry", {})], sink)
    try:
        wait_for(lambda: _subscribed(app))
        dev.control(action="terminate")
        wait_for(lambda: not app.runners[0].alive(), what="runner stop")
        assert "Forbidden" in json.dumps(_sub(app)["terminated"])
    finally:
        app.stop()


def test_unknown_event_and_unoffered_mode_stop_with_a_clear_reason(tmp_path: Path, dev: DevHttp) -> None:
    sink = Collect()
    watches = [Watch("dev", "nope", {}), Watch("dev", "clock.tick", {}, mode="poll")]
    app = _app(tmp_path, {"dev": dev.endpoint()}, watches, sink)
    try:
        wait_for(lambda: not any(r.alive() for r in app.runners))
        reasons = [str(app.store.sub(r.key)["terminated"]["reason"]) for r in app.runners]
    finally:
        app.stop()
    assert "no event type 'nope'" in reasons[0] and "ledger.entry" in reasons[0]
    assert "offers ['push']" in reasons[1]


def test_invalid_arguments_stop_with_invalid_params(tmp_path: Path, dev: DevHttp) -> None:
    sink = Collect()
    app = _app(tmp_path, {"dev": dev.endpoint()}, [Watch("dev", "ledger.entry", {"min_amount": "lots"})], sink)
    try:
        wait_for(lambda: not app.runners[0].alive())
        assert "InvalidParams" in str(_sub(app)["terminated"]["reason"])
    finally:
        app.stop()


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = int(s.getsockname()[1])
    s.close()
    return port


def test_webhook_subscribe_verify_deliver_refresh_unsubscribe(tmp_path: Path, make_dev) -> None:  # type: ignore[no-untyped-def]
    dev = make_dev(insecure_callbacks=True)
    port = _free_port()
    # The dev server accepts plain-http loopback callbacks only with --insecure-callbacks; the
    # config loader itself insists on https for public_url.
    hook = WebhookSpec("127.0.0.1", port, f"http://127.0.0.1:{port}", ttl=3.0)
    sink = Collect()
    ep = dev.endpoint(headers={"Authorization": "Bearer tenant-a"})
    app = _app(tmp_path, {"dev": ep}, [Watch("dev", "deploy.finished", {"service": "api"})], sink, hook)
    try:
        wait_for(lambda: _subscribed(app), what="webhook subscribe")
        rec = _sub(app)
        assert rec["mode"] == "webhook" and rec["webhook"]["id"].startswith("sub_")
        assert rec["webhook"]["secret"].startswith("whsec_")
        dev.control(action="emit", event="deploy.finished", data={"service": "api", "status": "ok"})
        dev.control(action="emit", event="deploy.finished", data={"service": "web", "status": "ok"})
        wait_for(lambda: len(sink.events) == 1, what="webhook event")
        refreshes = dev.hub.stats["events/subscribe"]
        wait_for(lambda: dev.hub.stats["events/subscribe"] > refreshes, timeout=8, what="TTL refresh")
        wait_for(lambda: isinstance(_sub(app).get("delivery_status"), dict), what="deliveryStatus recorded")
        assert _sub(app)["delivery_status"]["active"] is True
    finally:
        app.stop()
    assert sink.events[0].occurrence["data"]["service"] == "api"
    assert dev.hub.stats.get("events/unsubscribe") == 1
    assert dev.control(action="stats")["webhooks"] == {}


def test_webhook_needs_auth_and_reports_forbidden(tmp_path: Path, make_dev) -> None:  # type: ignore[no-untyped-def]
    dev = make_dev(insecure_callbacks=True)
    port = _free_port()
    hook = WebhookSpec("127.0.0.1", port, f"http://127.0.0.1:{port}")
    app = _app(tmp_path, {"dev": dev.endpoint()}, [Watch("dev", "deploy.finished", {})], Collect(), hook)
    try:
        wait_for(lambda: not app.runners[0].alive())
        assert "Forbidden" in str(_sub(app)["terminated"]["reason"])
    finally:
        app.stop()


def test_stdio_concurrent_streams_demux_and_null_cursor(tmp_path: Path) -> None:
    ep = stdio_endpoint("--every", "0.3", "--heartbeat-ms", "300")
    sink = Collect()
    watches = [Watch("io", "ledger.entry", {}), Watch("io", "clock.tick", {})]
    app = _app(tmp_path, {"io": ep}, watches, sink)
    try:
        wait_for(lambda: {d.event for d in sink.events} >= {"ledger.entry", "clock.tick"}
                 and len(sink.events) >= 6, timeout=15, what="both streams")
        keys = {r.watch.event: r.key for r in app.runners}
        assert app.store.cursor(keys["ledger.entry"]) is not None
        assert app.store.cursor(keys["clock.tick"]) is None  # emit-only: nothing to persist
    finally:
        app.stop()
    by_event: dict[str, list[str]] = {}
    for d in sink.events:
        by_event.setdefault(d.event, []).append(d.occurrence["eventId"])
    assert all(e.startswith("evt_ledger_") for e in by_event["ledger.entry"])
    assert all(e.startswith("evt_clock.tick_") for e in by_event["clock.tick"])


def test_stdio_server_crash_is_restarted(tmp_path: Path) -> None:
    ep = stdio_endpoint("--every", "0.2")
    sink = Collect()
    app = _app(tmp_path, {"io": ep}, [Watch("io", "ledger.entry", {})], sink)
    try:
        wait_for(lambda: len(sink.events) >= 2, timeout=15)
        client = app.pool._clients[ep.label]
        assert client._stdio is not None
        client._stdio.proc.kill()
        n = len(sink.events)
        wait_for(lambda: len(sink.events) > n + 2, timeout=20, what="events after restart")
    finally:
        app.stop()


# --- the dev server's strictness is real (negative controls) ---------------------------------


def _raw(dev: DevHttp, body: dict, headers: dict) -> tuple[int, dict]:
    req = urllib.request.Request(dev.url, data=json.dumps(body).encode(), method="POST",
                                 headers={"Content-Type": "application/json", **headers})
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


META = {"io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientInfo": {"name": "t", "version": "0"},
        "io.modelcontextprotocol/clientCapabilities": {}}


def test_dev_server_rejects_header_mistakes(dev: DevHttp) -> None:
    body = {"jsonrpc": "2.0", "id": 1, "method": "events/poll",
            "params": {"name": "ledger.entry", "arguments": {"account": "ops"}, "cursor": None, "_meta": META}}
    good = {"MCP-Protocol-Version": "2026-07-28", "Mcp-Method": "events/poll",
            "Mcp-Name": "ledger.entry", "Mcp-Param-Account": "ops"}
    assert _raw(dev, body, good)[0] == 200
    for drop in good:
        h = {k: v for k, v in good.items() if k != drop}
        status, msg = _raw(dev, body, h)
        assert (status, msg["error"]["code"]) == (400, -32020), drop
    status, msg = _raw(dev, body, good | {"Mcp-Param-Account": "other"})
    assert msg["error"]["code"] == -32020
    b64 = "=?base64?" + base64.b64encode(b"ledger.entry").decode() + "?="
    assert _raw(dev, body, good | {"Mcp-Name": b64})[0] == 200  # sentinel decoded before compare
    old = json.loads(json.dumps(body))
    old["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"] = "2025-11-25"
    status, msg = _raw(dev, old, good | {"MCP-Protocol-Version": "2025-11-25"})
    assert msg["error"]["code"] == -32022


def test_dev_server_rejects_bad_webhook_requests(make_dev) -> None:  # type: ignore[no-untyped-def]
    dev = make_dev()
    with McpClient(dev.endpoint(headers={"Authorization": "Bearer t"})) as c:
        types, _ = c.list_events()
        et = {t.name: t for t in types}["deploy.finished"]
        for url, secret, code in [("http://example.com/h", "whsec_" + base64.b64encode(b"x" * 32).decode(), -32602),
                                  ("https://example.com/h", "hunter2", -32602),
                                  ("https://example.com/h", "whsec_" + base64.b64encode(b"x" * 8).decode(), -32602),
                                  ("https://127.0.0.1:1/h", "whsec_" + base64.b64encode(b"x" * 32).decode(), -32027)]:
            try:
                c.subscribe(et, {}, url, secret, None, 60_000)
            except McpError as e:
                assert e.code == code, (url, secret, e)
            else:
                raise AssertionError(f"accepted {url} {secret}")


def test_client_refuses_servers_without_events(tmp_path: Path) -> None:
    from eventwake.mcp import Endpoint
    import sys
    script = tmp_path / "legacy.py"
    script.write_text(
        "import sys, json\n"
        "for line in sys.stdin:\n"
        "    m = json.loads(line)\n"
        "    if 'id' in m:\n"
        "        print(json.dumps({'jsonrpc':'2.0','id':m['id'],'error':{'code':-32601,'message':'no'}}), flush=True)\n")
    ep = Endpoint("legacy", "stdio", command=sys.executable, args=[str(script)])
    try:
        McpClient(ep, timeout=5).connect()
    except TransportError as e:
        assert "older than 2026-07-28" in str(e)
    else:
        raise AssertionError("connected to a legacy server")


@pytest.mark.filterwarnings("error::pytest.PytestUnhandledThreadExceptionWarning")
def test_closing_a_chunked_push_stream_is_clean(tmp_path: Path, make_dev, capfd) -> None:  # type: ignore[no-untyped-def]
    """Regression: found against github-events-mcp (Node, chunked SSE). Closing the stream from
    another thread used to kill the reader thread with an uncaught http.client exception."""
    dev = make_dev(chunked=True)
    sink = Collect()
    app = _app(tmp_path, {"dev": dev.endpoint()}, [Watch("dev", "ledger.entry", {})], sink)
    try:
        wait_for(lambda: _subscribed(app))
        dev.control(action="emit", count=2)
        wait_for(lambda: len(sink.events) == 2)
    finally:
        app.stop()
    time.sleep(0.3)
    assert "Traceback" not in capfd.readouterr().err
    assert sink.ids() == ["evt_ledger_0", "evt_ledger_1"]
