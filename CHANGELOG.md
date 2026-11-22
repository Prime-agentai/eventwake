# Changelog

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
