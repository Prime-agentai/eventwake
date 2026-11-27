# Changelog

## 0.1.1 (2026-11-27)

- Fixed: a server entry in someone else's `.mcp.json`, `~/.claude.json` or Codex `config.toml`
  with a non-list `args` crashed `eventwake ls`/`run` with a `TypeError` (or, for a string,
  was silently split into one argument per character). It is now skipped with a reason, like
  every other unusable entry, and the remaining servers still resolve.
- Fixed: `eventwake.toml` accepted `timeout`, `max_age` and `webhook.ttl` of zero or below,
  a negative `batch_window`, and `webhook.listen` ports outside 1-65535. They now fail at load
  with a one-line error naming the key. (`batch_window = 0` stays valid: no batching.)
- Tests: 56 -> 140. New `tests/test_discovery_config.py` covers config discovery precedence and
  shadowing, `${VAR}` expansion and unset-variable refusal, malformed foreign config files, URL
  labels, and 38 config validation failures.

## 0.1.0 (2026-11-22)

First release.

- Client for the MCP Events extension (SEP-3415) on protocol 2026-07-28: poll, push (stdio and
  Streamable HTTP SSE) and webhook delivery, with `Mcp-Name` / `Mcp-Param-*` headers.
- Durable per-subscription state: the cursor and accepted events are committed together,
  `eventId` dedupe, `maxAgeMs`-bounded replay, gap accounting.
- Webhook receiver: Standard Webhooks verification, verification handshake, gap/terminated
  envelopes, TTL refresh, `deliveryStatus`.
- Sinks: run any agent CLI per batch (`claude -p`, `codex exec`...), a Claude Code channel
  (`eventwake channel`), or JSON lines.
- Untrusted-payload framing for every prompt.
- `eventwake dev-server`: a strict Events server for tests and demos.
- Interop harness for t128n/github-events-mcp. Fixed a crash when closing a chunked SSE
  stream that it exposed.
