import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { extname, join, normalize } from "node:path";

const publicDir = new URL("../public/", import.meta.url).pathname;
const snapshot = "2026-10-06T06:59:06.430Z";

function json(response, value, status = 200) {
  response.writeHead(status, { "Content-Type": "application/json" });
  response.end(JSON.stringify(value));
}

const server = createServer(async (request, response) => {
  const url = new URL(request.url, "http://127.0.0.1:4173");
  if (url.pathname === "/health") return json(response, { status: "ok" });
  if (url.pathname === "/metadata") {
    return json(response, {
      coverage: {
        story_count: 12345,
        coverage_start: "2024-10-01T00:00:00Z",
        coverage_end: "2026-10-06T00:00:00Z",
        last_ingested_at: new Date().toISOString(),
      },
      snapshots: [
        { snapshot_id: 2, sequence_number: 2, timestamp: "2026-10-06T07:00:00Z" },
        { snapshot_id: 1, sequence_number: 1, timestamp: snapshot },
      ],
      snapshots_truncated: false,
    });
  }
  if (url.pathname === "/query" && request.method === "POST") {
    let body = "";
    for await (const chunk of request) body += chunk;
    const { sql } = JSON.parse(body);
    if (sql.includes("AT (TIMESTAMP")) {
      return json(response, {
        columns: [
          { name: "rank", type: "INTEGER" },
          { name: "title", type: "VARCHAR" },
          { name: "score", type: "BIGINT" },
          { name: "comment_count", type: "BIGINT" },
        ],
        rows: [[1, "An earlier front page story", 321, 87]],
        row_count: 1,
      });
    }
    return json(response, {
      columns: [
        { name: "month", type: "VARCHAR" },
        { name: "postgresql", type: "BIGINT" },
        { name: "mysql", type: "BIGINT" },
      ],
      rows: [
        ["2026-07", 19, 8],
        ["2026-08", 25, 7],
        ["2026-09", 34, 9],
      ],
      row_count: 3,
    });
  }

  const relative = url.pathname === "/" ? "index.html" : url.pathname.slice(1);
  const safePath = normalize(relative).replace(/^(\.\.(\/|\\|$))+/, "");
  try {
    const content = await readFile(join(publicDir, safePath));
    const types = { ".html": "text/html", ".css": "text/css", ".js": "text/javascript" };
    response.writeHead(200, { "Content-Type": types[extname(safePath)] || "application/octet-stream" });
    response.end(content);
  } catch {
    response.writeHead(404);
    response.end("not found");
  }
});

server.listen(4173, "127.0.0.1");
