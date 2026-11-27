"""Server discovery (Claude Code / Codex config we do not control) and eventwake.toml validation.

These are failure-path tests: foreign config files are routinely malformed, and a typo in
eventwake.toml must produce a one-line error naming the key, never a traceback.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from eventwake.config import ConfigError, load, resolve_server
from eventwake.servers import (
    Discovered,
    discover,
    endpoint_from_claude,
    endpoint_from_codex,
    expand,
    label_for_url,
    slug,
)


@pytest.fixture
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """An isolated Claude + Codex home so the developer's real config never leaks into a test."""
    claude = tmp_path / "claude-home"
    codex = tmp_path / "codex-home"
    claude.mkdir()
    codex.mkdir()
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(claude))
    monkeypatch.setenv("CODEX_HOME", str(codex))
    return tmp_path


def _write_claude_json(home: Path, doc: object) -> None:
    (home / "claude-home" / ".claude.json").write_text(json.dumps(doc), encoding="utf-8")


def _project(home: Path) -> Path:
    p = home / "proj"
    p.mkdir()
    return p


# --- ${VAR} expansion ---------------------------------------------------------------------------


def test_expand_set_default_and_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EW_SET", "yes")
    monkeypatch.delenv("EW_UNSET", raising=False)
    missing: list[str] = []
    assert expand("a-${EW_SET}-b", missing) == "a-yes-b"
    assert expand("${EW_UNSET:-fallback}", missing) == "fallback"
    assert missing == []
    assert expand("x${EW_UNSET}y", missing) == "xy"  # recorded, never guessed
    assert missing == ["EW_UNSET"]


def test_expand_empty_default_is_not_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EW_UNSET", raising=False)
    missing: list[str] = []
    assert expand("${EW_UNSET:-}", missing) == ""
    assert missing == []


# --- one entry -> Endpoint, or a reason ---------------------------------------------------------


@pytest.mark.parametrize("raw, reason", [
    ("just a string", "not an object"),
    ({"type": "http"}, "http server without url"),
    ({"type": "http", "url": 5}, "http server without url"),
    ({"type": "stdio"}, "stdio server without command"),
    ({"command": ["npx", "x"]}, "stdio server without command"),
    ({"type": "sse", "url": "https://x/sse"}, "transport 'sse' is not supported"),
    ({"command": "x", "args": 5}, "args must be a list of strings"),
    ({"command": "x", "args": "--flag value"}, "args must be a list of strings"),
])
def test_claude_entry_rejections_are_reasons_not_crashes(raw: object, reason: str) -> None:
    got = endpoint_from_claude("srv", raw, "claude user scope", [])
    assert isinstance(got, str) and reason in got and got.startswith("srv (claude user scope)")


def test_claude_entry_infers_transport_and_expands_everything(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EW_TOK", "s3cret")
    monkeypatch.setenv("EW_HOST", "example.com")
    missing: list[str] = []
    http = endpoint_from_claude("h", {"url": "https://${EW_HOST}/mcp",
                                      "headers": {"Authorization": "Bearer ${EW_TOK}"}}, "s", missing)
    assert not isinstance(http, str)
    assert (http.kind, http.url, http.headers) == ("http", "https://example.com/mcp",
                                                    {"Authorization": "Bearer s3cret"})
    stdio = endpoint_from_claude("s", {"command": "npx", "args": ["-y", 5, "${EW_TOK}"],
                                       "env": {"K": "${EW_HOST}", "N": 1}}, "s", missing)
    assert not isinstance(stdio, str)
    assert (stdio.kind, stdio.command, stdio.args) == ("stdio", "npx", ["-y", "5", "s3cret"])
    assert stdio.env == {"K": "example.com", "N": "1"}
    assert missing == []


def test_claude_entry_garbage_headers_and_env_are_ignored_not_fatal() -> None:
    got = endpoint_from_claude("h", {"url": "https://x", "headers": ["not", "a", "dict"]}, "s", [])
    assert not isinstance(got, str) and got.headers == {}


def test_describe_never_leaks_header_or_env_values() -> None:
    http = endpoint_from_claude("h", {"url": "https://x/mcp", "headers": {"Authorization": "Bearer TOPSECRET"}},
                                "s", [])
    stdio = endpoint_from_claude("s", {"command": "srv", "args": ["--x"], "env": {"TOKEN": "TOPSECRET"}}, "s", [])
    assert not isinstance(http, str) and not isinstance(stdio, str)
    assert "TOPSECRET" not in http.describe() and "TOPSECRET" not in stdio.describe()


def test_codex_entry_forms(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EW_TOK", "tok")
    monkeypatch.delenv("EW_HDR", raising=False)
    missing: list[str] = []
    http = endpoint_from_codex("c", {"url": "https://x/mcp", "http_headers": {"X-A": "1"},
                                     "env_http_headers": {"X-B": "EW_HDR"},
                                     "bearer_token_env_var": "EW_TOK"}, missing)
    assert not isinstance(http, str)
    assert http.headers == {"X-A": "1", "X-B": "", "Authorization": "Bearer tok"}
    assert missing == ["EW_HDR"]
    stdio = endpoint_from_codex("c", {"command": "srv", "args": ["a", 2], "cwd": "/w", "env": {"K": "v"}}, [])
    assert not isinstance(stdio, str)
    assert (stdio.args, stdio.cwd, stdio.env) == (["a", "2"], "/w", {"K": "v"})


@pytest.mark.parametrize("raw, reason", [
    ("x", "not a table"),
    ({}, "neither url nor command"),
    ({"command": "x", "args": "a b"}, "args must be a list of strings"),
])
def test_codex_entry_rejections(raw: object, reason: str) -> None:
    got = endpoint_from_codex("c", raw, [])
    assert isinstance(got, str) and reason in got


# --- discovery across files ---------------------------------------------------------------------


def test_discover_with_no_config_anywhere(home: Path) -> None:
    got = discover(_project(home))
    assert isinstance(got, Discovered)
    assert got.endpoints == {} and got.skipped == [] and got.missing_vars == {}


def test_discover_precedence_local_then_project_then_user_then_codex(home: Path) -> None:
    proj = _project(home)
    _write_claude_json(home, {
        "projects": {str(proj.resolve()): {"mcpServers": {"a": {"url": "https://local/mcp"}}}},
        "mcpServers": {"a": {"url": "https://user/mcp"}, "b": {"url": "https://user-b/mcp"},
                       "c": {"url": "https://user-c/mcp"}},
    })
    (proj / ".mcp.json").write_text(json.dumps({"mcpServers": {
        "a": {"url": "https://project/mcp"}, "b": {"url": "https://project-b/mcp"}}}))
    (home / "codex-home" / "config.toml").write_text(
        '[mcp_servers.a]\nurl = "https://codex/mcp"\n[mcp_servers.c]\nurl = "https://codex-c/mcp"\n'
        '[mcp_servers.d]\ncommand = "codex-d"\n')
    got = discover(proj)
    urls = {k: v.url or v.command for k, v in got.endpoints.items()}
    assert urls == {"a": "https://local/mcp", "b": "https://project-b/mcp",
                    "c": "https://user-c/mcp", "d": "codex-d"}
    assert got.endpoints["a"].source == "claude local scope"
    assert got.endpoints["b"].source == "claude project .mcp.json"
    # every losing definition is reported, with who beat it
    assert "a (claude project .mcp.json): shadowed by claude local scope" in got.skipped
    assert "a (claude user scope): shadowed by claude local scope" in got.skipped
    assert "b (claude user scope): shadowed by claude project .mcp.json" in got.skipped
    assert "c (codex config.toml): shadowed by claude user scope" in got.skipped
    assert "a (codex config.toml): shadowed by claude local scope" in got.skipped


def test_discover_project_stdio_runs_in_the_project_dir(home: Path) -> None:
    proj = _project(home)
    (proj / ".mcp.json").write_text(json.dumps({"mcpServers": {"s": {"command": "srv"},
                                                              "h": {"url": "https://x/mcp"}}}))
    got = discover(proj)
    assert got.endpoints["s"].cwd == str(proj)
    assert got.endpoints["h"].cwd is None


def test_discover_reports_unset_variables_per_server(home: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EW_NOPE_A", raising=False)
    monkeypatch.delenv("EW_NOPE_B", raising=False)
    proj = _project(home)
    (proj / ".mcp.json").write_text(json.dumps({"mcpServers": {
        "needs": {"url": "https://x/${EW_NOPE_B}", "headers": {"A": "${EW_NOPE_A}", "B": "${EW_NOPE_A}"}},
        "fine": {"url": "https://y/mcp"}}}))
    got = discover(proj)
    assert got.missing_vars == {"needs": ["EW_NOPE_A", "EW_NOPE_B"]}  # sorted, de-duplicated


def test_discover_one_malformed_entry_does_not_hide_the_good_ones(home: Path) -> None:
    proj = _project(home)
    _write_claude_json(home, {"mcpServers": {
        "bad-args": {"command": "x", "args": 5},
        "bad-type": "oops",
        "good": {"url": "https://good/mcp"}}})
    got = discover(proj)
    assert list(got.endpoints) == ["good"]
    assert sorted(got.skipped) == ["bad-args (claude user scope): args must be a list of strings",
                                   "bad-type (claude user scope): not an object"]


@pytest.mark.parametrize("content", ["", "{not json", "[]", "null", '{"mcpServers": []}',
                                     '{"projects": "x", "mcpServers": 3}'])
def test_discover_tolerates_garbage_claude_json(home: Path, content: str) -> None:
    (home / "claude-home" / ".claude.json").write_text(content)
    assert discover(_project(home)).endpoints == {}


def test_discover_tolerates_garbage_mcp_json_and_codex_toml(home: Path) -> None:
    proj = _project(home)
    (proj / ".mcp.json").write_text("{nope")
    (home / "codex-home" / "config.toml").write_text("mcp_servers = [")
    assert discover(proj).endpoints == {}
    (proj / ".mcp.json").write_text('{"mcpServers": "x"}')
    (home / "codex-home" / "config.toml").write_text('mcp_servers = "x"')
    assert discover(proj).endpoints == {}


def test_discover_ignores_other_projects_local_scope(home: Path) -> None:
    proj = _project(home)
    _write_claude_json(home, {"projects": {"/somewhere/else": {"mcpServers": {"x": {"url": "https://x/mcp"}}}}})
    assert discover(proj).endpoints == {}


# --- naming ad-hoc URLs -------------------------------------------------------------------------


@pytest.mark.parametrize("url, label", [
    ("https://huggingface.co/mcp", "huggingface"),
    ("https://mcp.github.com/x", "github"),
    ("https://api.example.co.uk/mcp", "example-co"),
    ("http://localhost:8080/mcp", "localhost-8080"),
    ("http://127.0.0.1/mcp", "127-0-0-1"),
    ("http://[::1]:9000/mcp", "1-9000"),
    ("not a url", "server"),
])
def test_label_for_url(url: str, label: str) -> None:
    assert label_for_url(url) == label


def test_slug_never_returns_empty() -> None:
    assert slug("Hello, World!") == "hello-world"
    assert slug("---") == "x"
    assert slug("") == "x"


# --- resolve_server -----------------------------------------------------------------------------


def test_resolve_server_order_inline_then_url_then_discovered(home: Path) -> None:
    proj = _project(home)
    (proj / ".mcp.json").write_text(json.dumps({"mcpServers": {"found": {"url": "https://found/mcp"},
                                                              "dup": {"url": "https://discovered/mcp"}}}))
    (proj / "eventwake.toml").write_text('[servers.dup]\nurl = "https://inline/mcp"\n')
    cfg = load(proj / "eventwake.toml", proj)
    assert resolve_server("dup", cfg, proj).url == "https://inline/mcp"
    assert resolve_server("found", cfg, proj).url == "https://found/mcp"
    adhoc = resolve_server("https://huggingface.co/mcp", cfg, proj)
    assert (adhoc.label, adhoc.url) == ("huggingface", "https://huggingface.co/mcp")


def test_resolve_server_unknown_lists_what_is_known(home: Path) -> None:
    proj = _project(home)
    (proj / ".mcp.json").write_text(json.dumps({"mcpServers": {"zeta": {"url": "https://z/mcp"},
                                                              "alpha": {"url": "https://a/mcp"}}}))
    (proj / "eventwake.toml").write_text("")
    cfg = load(proj / "eventwake.toml", proj)
    with pytest.raises(ConfigError, match=r"unknown server 'nope'\. Known: alpha, zeta\."):
        resolve_server("nope", cfg, proj)


def test_resolve_server_with_nothing_configured_says_so(home: Path) -> None:
    proj = _project(home)
    (proj / "eventwake.toml").write_text("")
    cfg = load(proj / "eventwake.toml", proj)
    with pytest.raises(ConfigError, match="No MCP servers are configured here"):
        resolve_server("nope", cfg, proj)


def test_resolve_server_refuses_to_run_with_unset_variables(home: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EW_NOPE", raising=False)
    proj = _project(home)
    (proj / ".mcp.json").write_text(json.dumps({"mcpServers": {"s": {"url": "https://x/mcp",
                                                                   "headers": {"A": "${EW_NOPE}"}}}}))
    (proj / "eventwake.toml").write_text("")
    cfg = load(proj / "eventwake.toml", proj)
    with pytest.raises(ConfigError, match=r"server 's': unset environment variable\(s\) EW_NOPE"):
        resolve_server("s", cfg, proj)


# --- eventwake.toml validation: every error names the key ---------------------------------------

_WATCH = '[[watch]]\nserver = "s"\nevent = "e"\n'
_RUN = _WATCH + 'run = ["agent"]\n'


def _load(tmp_path: Path, toml: str) -> object:
    p = tmp_path / "eventwake.toml"
    p.write_text(toml)
    return load(p, tmp_path)


@pytest.mark.parametrize("toml, fragment", [
    ("servers = [", "eventwake.toml"),  # TOML syntax error names the file
    ('state_dir = 5', "state_dir: expected a string"),
    ('servers = "x"', "servers: expected a table of server tables"),
    ('watch = "x"', "watch: use [[watch]] tables"),
    ('poll_floor = 0.01', "poll_floor: must be at least 100ms"),
    ('poll_floor = "soon"', "poll_floor: expected a duration"),
    ('[servers.a]\ntype = "sse"\nurl = "https://x"', "transport 'sse' is not supported"),
    ('[servers.a]\nurl = "https://x/${EW_DEFINITELY_UNSET_VAR}"', "unset environment variable(s) EW_DEFINITELY_UNSET_VAR"),
    ('[[watch]]\nevent = "e"', "watch[0].server: required"),
    ('[[watch]]\nserver = "s"', "watch[0].event: required"),
    (_WATCH + 'surprise = 1', "watch[0]: unknown key(s) surprise"),
    (_WATCH + 'mode = "pull"', "watch[0].mode: one of auto, poll, push, webhook"),
    (_WATCH + 'arguments = "x"', "watch[0].arguments: expected a table"),
    (_WATCH + 'name = 3', "watch[0].name: expected a string"),
    (_WATCH + 'prompt = "  "', "watch[0].prompt: expected a non-empty string"),
    (_WATCH + 'timeout = "1m"', "only apply with `run`"),
    (_WATCH + 'env = { A = "1" }', "only apply with `run`"),
    (_WATCH + 'run = "claude -p"', "use an argv list"),
    (_WATCH + 'run = []', "expected a non-empty list of strings"),
    (_WATCH + 'run = ["a", 1]', "expected a non-empty list of strings"),
    (_RUN + 'cwd = 5', "watch[0].cwd: expected a string"),
    (_RUN + 'env = { A = 1 }', "watch[0].env: expected a table of strings"),
    (_RUN + 'max_batch = 0', "watch[0].max_batch: expected an integer >= 1"),
    (_RUN + 'max_batch = true', "watch[0].max_batch: expected an integer >= 1"),
    (_RUN + 'max_runs_per_hour = 1.5', "watch[0].max_runs_per_hour: expected an integer >= 1"),
    (_RUN + 'timeout = 0', "watch[0].timeout: must be positive"),
    (_RUN + 'timeout = -5', "watch[0].timeout: must be positive"),
    (_RUN + 'timeout = "-5s"', "watch[0].timeout: expected a duration"),
    (_RUN + 'timeout = true', "watch[0].timeout: expected a duration"),
    (_RUN + 'batch_window = -1', "watch[0].batch_window: must be zero or positive"),
    (_WATCH + 'max_age = 0', "watch[0].max_age: must be positive"),
    (_WATCH + 'max_age = -3', "watch[0].max_age: must be positive"),
    ('webhook = 3', "webhook: expected a table"),
    ('[webhook]\nlisten = "nonsense"', "webhook.listen: expected host:port"),
    ('[webhook]\nlisten = "127.0.0.1:99999"', "port in 1-65535"),
    ('[webhook]\nlisten = "127.0.0.1:0"', "port in 1-65535"),
    ('[webhook]\npublic_url = "http://x"', "webhook.public_url: must be an https:// URL"),
    ('[webhook]\nttl = 0', "webhook.ttl: must be positive"),
    ('[webhook]\nttl = "-1m"', "webhook.ttl: expected a duration"),
])
def test_config_errors_are_one_line_and_name_the_key(tmp_path: Path, toml: str, fragment: str) -> None:
    with pytest.raises(ConfigError) as ei:
        _load(tmp_path, toml)
    assert fragment in str(ei.value)
    assert "\n" not in str(ei.value)


def test_missing_config_file_points_at_init(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match=r"not found \(create one with `eventwake init`\)"):
        load(tmp_path / "eventwake.toml", tmp_path)


def test_config_accepts_the_documented_edges(tmp_path: Path) -> None:
    cfg = _load(tmp_path, _RUN + 'batch_window = 0\nmax_age = "none"\n'
                '[webhook]\nlisten = ":8787"\nttl = "never"\npublic_url = "https://hooks.example.com/"\n')
    assert cfg.watches[0].run.batch_window == 0  # type: ignore[attr-defined]
    assert cfg.watches[0].max_age is None  # type: ignore[attr-defined]
    assert (cfg.webhook.listen_host, cfg.webhook.listen_port) == ("127.0.0.1", 8787)  # type: ignore[attr-defined]
    assert cfg.webhook.no_expiry and cfg.webhook.ttl is None  # type: ignore[attr-defined]
    assert cfg.webhook.public_url == "https://hooks.example.com"  # type: ignore[attr-defined]


def test_state_dir_resolves_next_to_the_config_file(tmp_path: Path) -> None:
    cfg = _load(tmp_path, "")
    assert cfg.state_dir == tmp_path / ".eventwake"  # type: ignore[attr-defined]
    cfg = _load(tmp_path, f'state_dir = "{tmp_path / "abs"}"')
    assert cfg.state_dir == tmp_path / "abs"  # type: ignore[attr-defined]
