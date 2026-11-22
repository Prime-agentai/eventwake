// Interop harness: serve t128n/github-events-mcp's independent MCP Events implementation
// (packages/events, MIT) over Streamable HTTP on Node, with a fake "fake.tick" source, so
// eventwake can be tested against a server it did not write.
//
//   git clone https://github.com/t128n/github-events-mcp /tmp/gem
//   node --experimental-transform-types scripts/interop/github-events-mcp.mjs /tmp/gem 18930 400
//   eventwake tail http://127.0.0.1:18930/mcp fake.tick -n 3   # with an Authorization header
//
// Node 22.7+ runs the .ts sources directly with --experimental-transform-types. The upstream server targets Bun;
// this adapter turns node:http requests into the Fetch API Request it expects.
import http from "node:http";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [root, portArg = "18930", everyArg = "400"] = process.argv.slice(2);
if (!root) {
  console.error("usage: github-events-mcp.mjs <clone dir> [port] [tick ms]");
  process.exit(2);
}
const lib = await import(pathToFileURL(path.join(root, "packages/events/src/index.ts")).href);
const start = Date.now();
const everyMs = Number(everyArg);

const tick = lib.defineEvent({
  name: "fake.tick",
  description: "A tick every N ms (interop harness).",
  payloadSchema: { type: "object", properties: { n: { type: "integer" } } },
  poll: async (cursor, _args, ctx) => {
    const now = Math.floor((ctx.now - start) / everyMs);
    if (cursor === null || cursor === undefined) {
      return { events: [], cursor: String(now), hasMore: false, nextPollMs: everyMs };
    }
    const from = Number(cursor);
    if (!Number.isInteger(from) || from < 0 || from > now) {
      return { events: [], cursor: String(now), truncated: true, hasMore: false, nextPollMs: everyMs };
    }
    const upto = Math.min(now, from + ctx.maxEvents);
    const events = [];
    for (let n = from + 1; n <= upto; n++) {
      events.push({ eventId: `tick-${n}`, name: "fake.tick",
                    timestamp: new Date(start + n * everyMs).toISOString(), data: { n } });
    }
    return { events, cursor: String(upto), hasMore: upto < now, nextPollMs: everyMs };
  },
});

const serverInfo = { name: "github-events-mcp/events (interop)", version: "0.1.0" };
const hub = new lib.EventHub({ events: [tick], serverInfo });
const handler = lib.createEventsHandler({ getHub: () => hub, token: "interop-token", serverInfo });

http.createServer(async (req, res) => {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  const request = new Request(`http://${req.headers.host ?? "127.0.0.1"}${req.url}`, {
    method: req.method,
    headers: req.headers,
    body: req.method === "GET" || req.method === "HEAD" ? undefined : Buffer.concat(chunks),
  });
  let response;
  try {
    response = await handler(request);
  } catch (e) {
    res.writeHead(500);
    res.end(String(e));
    return;
  }
  res.writeHead(response.status, Object.fromEntries(response.headers));
  if (!response.body) {
    res.end();
    return;
  }
  const reader = response.body.getReader();
  res.on("close", () => reader.cancel().catch(() => {}));
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      res.write(value);
    }
  } catch {
    // client went away
  }
  res.end();
}).listen(Number(portArg), "127.0.0.1", () => {
  console.error(`interop server: http://127.0.0.1:${portArg}/mcp (Bearer interop-token)`);
});
