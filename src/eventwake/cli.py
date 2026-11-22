"""eventwake command line."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import threading
from pathlib import Path
from typing import Any

from . import __version__
from .config import Config, ConfigError, Watch, WebhookSpec, duration, load, resolve_server
from .mcp import McpClient, McpError, TransportError
from .sinks import ChannelSink, CommandSink, JsonlSink, PreviewSink, Sink
from .state import StateLocked, Store

TEMPLATE = '''\
# eventwake.toml: what to watch, and what to wake when it fires.
# Servers resolve by name from your Claude Code (.mcp.json, ~/.claude.json) or Codex config,
# or from a [servers.<name>] table here. See what a server emits: `eventwake ls <server>`.

# A local demo server that emits a ledger entry every 3 seconds. Delete it once you point the
# watch below at a real server.
[servers.demo]
command = "eventwake"
args = ["dev-server", "--every", "3"]

[[watch]]
server = "demo"
event = "ledger.entry"
arguments = { account = "ops" }
# mode = "auto"          # auto (push > poll > webhook) | poll | push | webhook
# max_age = "10m"        # after downtime, replay at most this much backlog

# Wake a headless agent once per batch. The prompt goes on stdin; {events} is the framed,
# untrusted payload block. Remove `run` to print JSON lines instead.
run = ["claude", "-p", "--permission-mode", "plan"]
prompt = """A ledger entry was posted. Summarise it in one line. {events}"""
batch_window = "5s"
max_runs_per_hour = 30
timeout = "10m"

# Webhook delivery (only for servers that offer it): an https URL that reaches this machine.
# [webhook]
# listen = "127.0.0.1:8787"
# public_url = "https://your-tunnel.example.com"
# ttl = "1h"
'''


def _parse_args_kv(items: list[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for item in items:
        key, sep, raw = item.partition("=")
        if not sep or not key:
            raise ConfigError(f"--arg expects key=value, got {item!r}")
        try:
            val: Any = json.loads(raw)
        except json.JSONDecodeError:
            val = raw
        # Integers, booleans and JSON objects/arrays pass through; anything else stays a string.
        out[key] = val if isinstance(val, (bool, int, dict, list)) else raw
    return out


def _config(path: str | None, cwd: Path, required: bool) -> Config:
    if path is not None:
        return load(Path(path), cwd)
    default = cwd / "eventwake.toml"
    if default.exists():
        return load(default, cwd)
    if required:
        raise ConfigError("no eventwake.toml here; create one with `eventwake init` or pass -c")
    return load(None, cwd)


def cmd_ls(ns: argparse.Namespace) -> int:
    cwd = Path.cwd()
    cfg = _config(ns.config, cwd, required=False)
    ep = resolve_server(ns.server, cfg, cwd)
    with McpClient(ep, ns.timeout) as client:
        settings = client.require_events()
        info = client.info
        assert info is not None
        types, warnings = client.list_events()
    if ns.json:
        print(json.dumps([{"name": t.name, "description": t.description, "delivery": list(t.delivery),
                           "inputSchema": t.input_schema, "payloadSchema": t.payload_schema}
                          for t in types], indent=1))
        return 0
    who = f"{info.name or ep.label} {info.version or ''}".strip()
    print(f"{ep.label}: {who} ({ep.describe()}) - {len(types)} event type(s)"
          f"{', sends list_changed' if settings.get('listChanged') else ''}")
    for t in types:
        print(f"\n  {t.name}  [{', '.join(t.delivery)}]")
        if t.description:
            print(f"    {t.description}")
        props = t.input_schema.get("properties")
        if isinstance(props, dict) and props:
            hdr = {p[0]: n for p, n in t.header_params if len(p) == 1}
            parts = []
            for k, v in props.items():
                typ = v.get("type", "?") if isinstance(v, dict) else "?"
                parts.append(f"{k}:{typ}" + (f" (header Mcp-Param-{hdr[k]})" if k in hdr else ""))
            print(f"    arguments: {', '.join(parts)}")
    for w in warnings:
        print(f"\n  warning: {w}", file=sys.stderr)
    return 0


def _run_app(cfg: Config, make_sink: Any, max_events: int | None, timeout: float) -> int:
    from .app import App

    app = App(cfg, Path.cwd(), make_sink, max_events, timeout)
    app.start()
    try:
        return app.wait()
    finally:
        app.stop()


def cmd_tail(ns: argparse.Namespace) -> int:
    cwd = Path.cwd()
    cfg = _config(ns.config, cwd, required=False)
    watch = Watch(ns.server, ns.event, _parse_args_kv(ns.arg or []), ns.mode,
                  duration(ns.max_age, "--max-age") if ns.max_age else 600.0)
    tmp = None
    if ns.state:
        cfg.state_dir = Path(ns.state)
    else:
        tmp = tempfile.TemporaryDirectory(prefix="eventwake-tail-")
        cfg.state_dir = Path(tmp.name)
    cfg.watches = [watch]
    if ns.public_url:
        cfg.webhook = WebhookSpec(public_url=ns.public_url.rstrip("/"), listen_port=ns.listen_port)
    sink = JsonlSink()
    try:
        return _run_app(cfg, lambda _w, _k: sink, ns.count, ns.timeout)
    finally:
        if tmp is not None:
            tmp.cleanup()


def cmd_run(ns: argparse.Namespace) -> int:
    cwd = Path.cwd()
    cfg = _config(ns.config, cwd, required=True)
    jsonl = JsonlSink()

    def make(w: Watch, _key: str) -> Sink:
        if w.run is None:
            return jsonl
        if ns.dry_run:
            return PreviewSink(w.run)
        return CommandSink(w.run, w.title, cfg.state_dir / "runs")

    return _run_app(cfg, make, ns.count, ns.timeout)


def cmd_channel(ns: argparse.Namespace) -> int:
    from .channel import ChannelServer

    cwd = Path.cwd()
    cfg = _config(ns.config, cwd, required=True)
    server = ChannelServer()
    reader = threading.Thread(target=server.serve, daemon=True, name="eventwake-channel")
    reader.start()

    def make(w: Watch, _key: str) -> Sink:
        return ChannelSink(server.write, server.initialized, w.prompt)

    from .app import App

    app = App(cfg, cwd, make, None, ns.timeout)
    app.start()
    try:
        while not server.closed.wait(0.5):
            pass
    finally:
        app.stop()
    return 0


def cmd_status(ns: argparse.Namespace) -> int:
    cwd = Path.cwd()
    cfg = _config(ns.config, cwd, required=False)
    store = Store(Path(ns.state) if ns.state else cfg.state_dir)
    subs = store.doc.get("subscriptions", {})
    pending = store.pending()
    if ns.json:
        safe = {k: {kk: vv for kk, vv in v.items() if kk not in ("seen", "webhook")} |
                ({"webhook": {kk: vv for kk, vv in v["webhook"].items() if kk != "secret"}}
                 if isinstance(v.get("webhook"), dict) else {})
                for k, v in subs.items()}
        print(json.dumps({"subscriptions": safe, "pending": len(pending)}, indent=1, default=str))
        return 0
    if not subs:
        print(f"no subscriptions recorded in {store.dir}")
        return 0
    counts: dict[str, int] = {}
    for d in pending:
        counts[d.key] = counts.get(d.key, 0) + 1
    for key, rec in sorted(subs.items(), key=lambda kv: str(kv[1].get("server"))):
        state = "STOPPED" if rec.get("terminated") else "ok" if not rec.get("last_error") else "retrying"
        print(f"{rec.get('server')}/{rec.get('event')} {json.dumps(rec.get('arguments', {}))}  [{rec.get('mode', '?')}] {state}")
        print(f"  events: {rec.get('events_total', 0)}  pending: {counts.get(key, 0)}  "
              f"duplicates dropped: {rec.get('duplicates', 0)}  gaps: {rec.get('gaps', 0)}"
              f"{'  dropped (queue full): ' + str(rec['dropped']) if rec.get('dropped') else ''}")
        print(f"  cursor: {rec.get('cursor')}  last event: {rec.get('last_event_at') or '-'}")
        run = rec.get("last_run")
        if isinstance(run, dict) and run.get("sink") == "run":
            print(f"  last run: exit {run.get('exit')}{' (timed out)' if run.get('timed_out') else ''}, "
                  f"{run.get('count')} event(s), {run.get('seconds')}s, log {run.get('log')}")
        if rec.get("last_error"):
            print(f"  last error: {rec['last_error']}")
        term = rec.get("terminated")
        if isinstance(term, dict):
            print(f"  stopped: {term.get('reason') or term.get('error')}")
        ds = rec.get("delivery_status")
        if isinstance(ds, dict) and (ds.get("active") is False or ds.get("lastError")):
            print(f"  webhook delivery: active={ds.get('active')} lastError={ds.get('lastError')}")
    if ns.pending:
        for d in pending:
            print(json.dumps({"subscription": d.key, "server": d.server, "event": d.event,
                              "eventId": d.occurrence.get("eventId"), "data": d.occurrence.get("data")}))
    return 0


def cmd_init(ns: argparse.Namespace) -> int:
    path = Path(ns.config or "eventwake.toml")
    if path.exists() and not ns.force:
        print(f"{path} exists; use --force to overwrite", file=sys.stderr)
        return 1
    path.write_text(TEMPLATE, encoding="utf-8")
    print(f"wrote {path}. Try: eventwake run --dry-run --count 1")
    return 0


def cmd_dev_server(ns: argparse.Namespace) -> int:
    from .devserver import run

    return run(ns.http, ns.every, ns.heartbeat_ms, ns.insecure_callbacks, ns.duplicate,
               ns.control_port, ns.retention)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="eventwake",
        description="Wake your coding agent when something happens: MCP Events (SEP-3415) for "
                    "Claude Code, Codex and any CLI agent.",
    )
    p.add_argument("--version", action="version", version=f"eventwake {__version__}")
    sub = p.add_subparsers(dest="cmd", required=True)

    def common(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("-c", "--config", help="path to eventwake.toml (default: ./eventwake.toml)")
        sp.add_argument("--timeout", type=float, default=30.0, help="per-request timeout, seconds")

    sp = sub.add_parser("ls", help="list the event types a server offers")
    sp.add_argument("server", help="server name (from eventwake.toml, .mcp.json, ~/.claude.json, Codex) or URL")
    sp.add_argument("--json", action="store_true")
    common(sp)
    sp.set_defaults(fn=cmd_ls)

    sp = sub.add_parser("tail", help="subscribe to one event type and print events as JSON lines")
    sp.add_argument("server")
    sp.add_argument("event")
    sp.add_argument("-a", "--arg", action="append", metavar="KEY=VALUE", help="subscription argument")
    sp.add_argument("--mode", choices=["auto", "poll", "push", "webhook"], default="auto")
    sp.add_argument("-n", "--count", type=int, help="exit after this many events")
    sp.add_argument("--state", help="keep cursor/dedupe state here (default: a temp dir, start from now)")
    sp.add_argument("--max-age", help="bound replay after a restart, e.g. 10m")
    sp.add_argument("--public-url", help="https URL that reaches the webhook receiver (webhook mode)")
    sp.add_argument("--listen-port", type=int, default=8787)
    common(sp)
    sp.set_defaults(fn=cmd_tail)

    sp = sub.add_parser("run", help="run the watches in eventwake.toml and wake their agents")
    sp.add_argument("--dry-run", action="store_true", help="print the prompts instead of running agents")
    sp.add_argument("-n", "--count", type=int, help="exit after this many events were handled")
    common(sp)
    sp.set_defaults(fn=cmd_run)

    sp = sub.add_parser("channel", help="act as a Claude Code channel: push events into a live session")
    common(sp)
    sp.set_defaults(fn=cmd_channel)

    sp = sub.add_parser("status", help="show cursors, counts, gaps, errors and last runs")
    sp.add_argument("--state", help="state directory (default: from eventwake.toml)")
    sp.add_argument("--pending", action="store_true", help="also print events not yet delivered")
    sp.add_argument("--json", action="store_true")
    common(sp)
    sp.set_defaults(fn=cmd_status)

    sp = sub.add_parser("init", help="write a commented eventwake.toml")
    sp.add_argument("-c", "--config")
    sp.add_argument("--force", action="store_true")
    sp.set_defaults(fn=cmd_init)

    sp = sub.add_parser("dev-server", help="a strict MCP Events server for demos and tests")
    sp.add_argument("--http", type=int, metavar="PORT", help="serve Streamable HTTP on this port (0 = any)")
    sp.add_argument("--every", type=float, help="emit a ledger entry and a tick every N seconds")
    sp.add_argument("--heartbeat-ms", type=int, default=15_000)
    sp.add_argument("--retention", type=int, default=1000, help="ledger entries kept for replay")
    sp.add_argument("--insecure-callbacks", action="store_true",
                    help="accept http://127.0.0.1 webhook callbacks (testing only)")
    sp.add_argument("--duplicate", action="store_true", help="deliver every event twice (dedupe test)")
    sp.add_argument("--control-port", type=int, help="stdio mode: also serve POST /_control here")
    sp.set_defaults(fn=cmd_dev_server)
    return p


def main(argv: list[str] | None = None) -> int:
    ns = build_parser().parse_args(argv)
    try:
        return int(ns.fn(ns))
    except (ConfigError, TransportError, McpError, StateLocked, ValueError) as e:
        print(f"eventwake: {e}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130
    except BrokenPipeError:
        return 0
