"""``eventwake.toml``: what to watch, and what to wake when it fires.

Servers resolve by name from the MCP config you already have (Claude Code local/project/user
scope, then Codex), or from an inline ``[servers.<name>]`` table, which wins.
"""

from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from .mcp import Endpoint
from .servers import discover, endpoint_from_claude, label_for_url

Mode = Literal["auto", "poll", "push", "webhook"]
JsonObj = dict[str, Any]

DEFAULT_PROMPT = """\
eventwake woke you because an MCP server reported {count} new event(s) of type `{event}` \
from server `{server}`.

{events}

Decide whether anything needs doing. If it does, do it with your normal tools, and check \
authoritative state with those tools before you act. If nothing needs doing, say so in one line."""

_DURATION = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)?\s*$")
_UNITS = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0, None: 1.0}


class ConfigError(Exception):
    pass


def duration(value: Any, what: str) -> float:
    """``"5s"``, ``"10m"``, ``"250ms"``, ``"1h"``, or a bare number of seconds -> seconds."""
    if isinstance(value, bool):
        raise ConfigError(f"{what}: expected a duration, got {value!r}")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        m = _DURATION.match(value)
        if m:
            return float(m.group(1)) * _UNITS[m.group(2)]
    raise ConfigError(f"{what}: expected a duration like '30s' or '10m', got {value!r}")


@dataclass
class RunSpec:
    argv: list[str]
    prompt: str = DEFAULT_PROMPT
    cwd: str | None = None
    timeout: float = 900.0
    batch_window: float = 5.0
    max_batch: int = 50
    max_runs_per_hour: int = 30
    env: dict[str, str] = field(default_factory=dict)


@dataclass
class Watch:
    server: str
    event: str
    arguments: JsonObj = field(default_factory=dict)
    mode: Mode = "auto"
    max_age: float | None = 600.0
    run: RunSpec | None = None
    name: str = ""
    prompt: str | None = None

    @property
    def title(self) -> str:
        return self.name or f"{self.server}/{self.event}"


@dataclass
class WebhookSpec:
    listen_host: str = "127.0.0.1"
    listen_port: int = 8787
    public_url: str | None = None
    ttl: float | None = 3600.0
    no_expiry: bool = False


@dataclass
class Config:
    path: Path | None
    state_dir: Path
    servers: dict[str, Endpoint]
    watches: list[Watch]
    webhook: WebhookSpec
    poll_floor: float = 1.0


def _span(raw: Any, what: str, *, allow_zero: bool = False) -> float:
    """A duration that must be positive (``allow_zero``: or exactly zero, e.g. no batching)."""
    seconds = duration(raw, what)
    if seconds < 0 or (seconds == 0 and not allow_zero):
        raise ConfigError(f"{what}: must be {'zero or ' if allow_zero else ''}positive, got {raw!r}")
    return seconds


def _str_list(raw: Any, what: str) -> list[str]:
    if isinstance(raw, str):
        raise ConfigError(f"{what}: use an argv list like [\"claude\", \"-p\"], not a shell string")
    if not isinstance(raw, list) or not raw or not all(isinstance(a, str) for a in raw):
        raise ConfigError(f"{what}: expected a non-empty list of strings")
    return list(raw)


def _int(raw: Any, what: str, lo: int = 1) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < lo:
        raise ConfigError(f"{what}: expected an integer >= {lo}")
    return raw


def parse_run(raw: JsonObj, where: str) -> RunSpec | None:
    if "run" not in raw:
        return None
    spec = RunSpec(argv=_str_list(raw["run"], f"{where}.run"))
    if "prompt" in raw:
        if not isinstance(raw["prompt"], str) or not raw["prompt"].strip():
            raise ConfigError(f"{where}.prompt: expected a non-empty string")
        spec.prompt = raw["prompt"]
    if "cwd" in raw:
        if not isinstance(raw["cwd"], str):
            raise ConfigError(f"{where}.cwd: expected a string")
        spec.cwd = raw["cwd"]
    if "timeout" in raw:
        spec.timeout = _span(raw["timeout"], f"{where}.timeout")
    if "batch_window" in raw:
        spec.batch_window = _span(raw["batch_window"], f"{where}.batch_window", allow_zero=True)
    if "max_batch" in raw:
        spec.max_batch = _int(raw["max_batch"], f"{where}.max_batch")
    if "max_runs_per_hour" in raw:
        spec.max_runs_per_hour = _int(raw["max_runs_per_hour"], f"{where}.max_runs_per_hour")
    if "env" in raw:
        env = raw["env"]
        if not isinstance(env, dict) or not all(isinstance(v, str) for v in env.values()):
            raise ConfigError(f"{where}.env: expected a table of strings")
        spec.env = {str(k): v for k, v in env.items()}
    return spec


_WATCH_KEYS = {"server", "event", "arguments", "mode", "max_age", "name", "run", "prompt", "cwd",
               "timeout", "batch_window", "max_batch", "max_runs_per_hour", "env"}


def parse_watch(raw: Any, i: int) -> Watch:
    where = f"watch[{i}]"
    if not isinstance(raw, dict):
        raise ConfigError(f"{where}: expected a table")
    unknown = set(raw) - _WATCH_KEYS
    if unknown:
        raise ConfigError(f"{where}: unknown key(s) {', '.join(sorted(unknown))}")
    server, event = raw.get("server"), raw.get("event")
    if not isinstance(server, str) or not server:
        raise ConfigError(f"{where}.server: required (a server name or URL)")
    if not isinstance(event, str) or not event:
        raise ConfigError(f"{where}.event: required (an event type name from `eventwake ls`)")
    args = raw.get("arguments", {})
    if not isinstance(args, dict):
        raise ConfigError(f"{where}.arguments: expected a table")
    mode = raw.get("mode", "auto")
    if mode not in ("auto", "poll", "push", "webhook"):
        raise ConfigError(f"{where}.mode: one of auto, poll, push, webhook")
    max_age: float | None = 600.0
    if "max_age" in raw:
        max_age = None if raw["max_age"] in ("none", "") else _span(raw["max_age"], f"{where}.max_age")
    name = raw.get("name", "")
    if not isinstance(name, str):
        raise ConfigError(f"{where}.name: expected a string")
    prompt = raw.get("prompt")
    if prompt is not None and (not isinstance(prompt, str) or not prompt.strip()):
        raise ConfigError(f"{where}.prompt: expected a non-empty string")
    run = parse_run(raw, where)
    if run is None and any(k in raw for k in ("cwd", "timeout", "max_runs_per_hour", "env")):
        raise ConfigError(f"{where}: cwd/timeout/max_runs_per_hour/env only apply with `run`")
    return Watch(server, event, dict(args), mode, max_age, run, name, prompt)


def parse_webhook(raw: Any) -> WebhookSpec:
    spec = WebhookSpec()
    if raw is None:
        return spec
    if not isinstance(raw, dict):
        raise ConfigError("webhook: expected a table")
    if "listen" in raw:
        listen = raw["listen"]
        host, _, port = str(listen).rpartition(":")
        if not port.isdigit() or not 1 <= int(port) <= 65535:
            raise ConfigError("webhook.listen: expected host:port with a port in 1-65535, e.g. 127.0.0.1:8787")
        spec.listen_host, spec.listen_port = host or "127.0.0.1", int(port)
    if "public_url" in raw:
        url = raw["public_url"]
        if not isinstance(url, str) or not url.startswith("https://"):
            raise ConfigError("webhook.public_url: must be an https:// URL (the spec requires TLS)")
        spec.public_url = url.rstrip("/")
    if "ttl" in raw:
        if raw["ttl"] == "never":
            spec.no_expiry, spec.ttl = True, None
        else:
            spec.ttl = _span(raw["ttl"], "webhook.ttl")
    return spec


def load(path: Path | None, cwd: Path) -> Config:
    doc: JsonObj = {}
    if path is not None:
        try:
            doc = tomllib.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            raise ConfigError(f"{path}: not found (create one with `eventwake init`)") from None
        except tomllib.TOMLDecodeError as e:
            raise ConfigError(f"{path}: {e}") from e
    base = path.parent if path is not None else cwd
    state_dir = doc.get("state_dir", ".eventwake")
    if not isinstance(state_dir, str):
        raise ConfigError("state_dir: expected a string")
    servers: dict[str, Endpoint] = {}
    inline = doc.get("servers", {})
    if not isinstance(inline, dict):
        raise ConfigError("servers: expected a table of server tables")
    for label, raw in inline.items():
        missing: list[str] = []
        made = endpoint_from_claude(label, raw, str(path or "eventwake.toml"), missing)
        if isinstance(made, str):
            raise ConfigError(f"servers.{made}")
        if missing:
            raise ConfigError(f"servers.{label}: unset environment variable(s) {', '.join(missing)}")
        if made.kind == "stdio" and made.cwd is None:
            made.cwd = str(base)
        servers[label] = made
    raw_watches = doc.get("watch", [])
    if not isinstance(raw_watches, list):
        raise ConfigError("watch: use [[watch]] tables")
    watches = [parse_watch(w, i) for i, w in enumerate(raw_watches)]
    floor = duration(doc.get("poll_floor", 1.0), "poll_floor")
    if floor < 0.1:
        raise ConfigError("poll_floor: must be at least 100ms")
    sd = Path(state_dir).expanduser()
    return Config(path, sd if sd.is_absolute() else base / sd, servers, watches,
                  parse_webhook(doc.get("webhook")), floor)


def resolve_server(name: str, cfg: Config, cwd: Path) -> Endpoint:
    """A configured name, an ``http(s)://`` URL, or a name from Claude Code / Codex config."""
    if name in cfg.servers:
        return cfg.servers[name]
    if name.startswith(("http://", "https://")):
        return Endpoint(label_for_url(name), "http", url=name)
    found = discover(cwd)
    if name in found.endpoints:
        missing = found.missing_vars.get(name)
        if missing:
            raise ConfigError(f"server {name!r}: unset environment variable(s) {', '.join(missing)}")
        return found.endpoints[name]
    known = sorted({*cfg.servers, *found.endpoints})
    hint = f" Known: {', '.join(known)}." if known else " No MCP servers are configured here."
    raise ConfigError(f"unknown server {name!r}.{hint}")


def endpoint_identity(ep: Endpoint) -> str:
    """What makes two endpoints "the same server" for cursor and dedupe purposes."""
    if ep.kind == "http":
        return f"http:{ep.url}"
    return "stdio:" + " ".join([str(ep.command), *ep.args])
