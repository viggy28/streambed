import { DurableObject } from "cloudflare:workers";

interface Env {
  ASSETS: Fetcher;
  QUERY_CONTAINER: DurableObjectNamespace<QueryContainer>;
  QUERY_RATE_LIMITER: RateLimit;
  R2_ACCESS_KEY_ID: string;
  R2_SECRET_ACCESS_KEY: string;
  S3_BUCKET: string;
  S3_ENDPOINT: string;
  S3_PREFIX: string;
}

interface QueryResult {
  columns: Array<{ name: string; type: string }>;
  rows: unknown[][];
  row_count: number;
}

interface SnapshotResult {
  table: string;
  snapshots: Array<{
    snapshot_id: number;
    sequence_number: number;
    timestamp: string;
  }>;
  truncated: boolean;
}

const CONTAINER_NAME = "public-query";
const CONTAINER_PORT = 8080;
const INACTIVITY_TIMEOUT_MS = 60_000;
const MAX_HTTP_BODY_BYTES = 66 * 1024;
const METADATA_CACHE_SECONDS = 60;

export class QueryContainer extends DurableObject<Env> {
  private starting: Promise<void> | undefined;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    const container = ctx.container!;
    if (container.running) {
      void ctx.blockConcurrencyWhile(() =>
        container.setInactivityTimeout(INACTIVITY_TIMEOUT_MS),
      );
    }
  }

  async fetch(request: Request): Promise<Response> {
    this.starting ??= this.startAndWaitForHTTP().finally(() => {
      this.starting = undefined;
    });
    await this.starting;

    const url = new URL(request.url);
    url.protocol = "http:";
    url.host = "container";
    const forwarded = new Request(url, request);
    forwarded.headers.delete("host");
    return this.ctx.container!.getTcpPort(CONTAINER_PORT).fetch(forwarded);
  }

  private async startAndWaitForHTTP(): Promise<void> {
    const container = this.ctx.container!;
    if (!container.running) {
      container.start({
        image: container.images.query,
        instance: "lite",
        enableInternet: true,
        env: {
          AWS_ACCESS_KEY_ID: this.env.R2_ACCESS_KEY_ID,
          AWS_SECRET_ACCESS_KEY: this.env.R2_SECRET_ACCESS_KEY,
          AWS_REGION: "auto",
          AWS_EC2_METADATA_DISABLED: "true",
          STREAMBED_S3_BUCKET: this.env.S3_BUCKET,
          STREAMBED_S3_PREFIX: this.env.S3_PREFIX,
          STREAMBED_S3_ENDPOINT: this.env.S3_ENDPOINT,
          STREAMBED_S3_REGION: "auto",
          STREAMBED_HTTP_QUERY_ADDR: `:${CONTAINER_PORT}`,
          STREAMBED_QUERY_MEMORY_LIMIT_MB: "128",
        },
      });
    }
    await container.setInactivityTimeout(INACTIVITY_TIMEOUT_MS);

    const port = container.getTcpPort(CONTAINER_PORT);
    let lastError: unknown;
    for (let attempt = 0; attempt < 160; attempt++) {
      try {
        const response = await port.fetch("http://container/health", {
          signal: AbortSignal.timeout(1_000),
        });
        await response.body?.cancel();
        if (!response.ok) {
          throw new Error(`health check returned ${response.status}`);
        }
        return;
      } catch (error) {
        lastError = error;
        await scheduler.wait(250);
      }
    }
    throw new Error("query container did not become ready", { cause: lastError });
  }
}

function jsonResponse(value: unknown, status = 200, headers?: HeadersInit): Response {
  const responseHeaders = new Headers(headers);
  responseHeaders.set("Content-Type", "application/json");
  return new Response(JSON.stringify(value) + "\n", {
    status,
    headers: responseHeaders,
  });
}

function addPublicHeaders(response: Response, requestID: string, noStore = true): Response {
  const headers = new Headers(response.headers);
  headers.set("X-Content-Type-Options", "nosniff");
  headers.set("Referrer-Policy", "strict-origin-when-cross-origin");
  headers.set("X-Frame-Options", "DENY");
  headers.set(
    "Content-Security-Policy",
    "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'none'",
  );
  headers.set("X-Request-ID", requestID);
  if (noStore) headers.set("Cache-Control", "no-store");
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

function configured(env: Env): boolean {
  return Boolean(env.R2_ACCESS_KEY_ID && env.R2_SECRET_ACCESS_KEY);
}

function containerStub(env: Env): DurableObjectStub<QueryContainer> {
  return env.QUERY_CONTAINER.getByName(CONTAINER_NAME);
}

async function fetchDemoMetadata(request: Request, env: Env): Promise<Response> {
  const cache = caches.default;
  const cacheKey = new Request(new URL("/__demo_metadata_v1", request.url), { method: "GET" });
  const cached = await cache.match(cacheKey);
  if (cached) return cached;

  const container = containerStub(env);
  const coverageRequest = new Request(new URL("/query", request.url), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      sql: `SELECT
        count(*) AS story_count,
        min(created_at) AS coverage_start,
        max(created_at) AS coverage_end,
        max(ingested_at) AS last_ingested_at
      FROM stories`,
    }),
  });
  const coverageResponse = await container.fetch(coverageRequest);
  if (!coverageResponse.ok) {
    throw new Error(`coverage query returned ${coverageResponse.status}`);
  }
  const coverageResult = (await coverageResponse.json()) as QueryResult;
  const coverage: Record<string, unknown> = {};
  coverageResult.columns.forEach((column, index) => {
    coverage[column.name] = coverageResult.rows[0]?.[index] ?? null;
  });

  const snapshotsURL = new URL("/snapshots", request.url);
  snapshotsURL.searchParams.set("table", "public.front_page");
  const snapshotsResponse = await container.fetch(new Request(snapshotsURL));
  if (!snapshotsResponse.ok) {
    throw new Error(`snapshot query returned ${snapshotsResponse.status}`);
  }
  const snapshotResult = (await snapshotsResponse.json()) as SnapshotResult;

  const response = jsonResponse(
    {
      coverage,
      snapshots: snapshotResult.snapshots,
      snapshots_truncated: snapshotResult.truncated,
      generated_at: new Date().toISOString(),
    },
    200,
    { "Cache-Control": `public, max-age=${METADATA_CACHE_SECONDS}` },
  );
  await cache.put(cacheKey, response.clone());
  return response;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const requestID = request.headers.get("CF-Ray") ?? crypto.randomUUID();
    const url = new URL(request.url);

    if (request.method === "OPTIONS" && url.pathname === "/query") {
      return addPublicHeaders(
        new Response(null, {
          status: 204,
          headers: {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Max-Age": "86400",
          },
        }),
        requestID,
      );
    }

    if (request.method === "GET" && url.pathname === "/health") {
      return addPublicHeaders(jsonResponse({ status: "ok" }), requestID);
    }

    if (request.method === "GET" && url.pathname === "/metadata") {
      if (!configured(env)) {
        return addPublicHeaders(jsonResponse({ error: "query service is not configured" }, 503), requestID);
      }
      try {
        return addPublicHeaders(await fetchDemoMetadata(request, env), requestID, false);
      } catch (error) {
        console.error("demo metadata request failed", { requestID, error });
        return addPublicHeaders(
          jsonResponse({ error: "demo metadata is temporarily unavailable" }, 503),
          requestID,
        );
      }
    }

    if (request.method === "POST" && url.pathname === "/query") {
      const contentLength = Number(request.headers.get("Content-Length") ?? "0");
      if (Number.isFinite(contentLength) && contentLength > MAX_HTTP_BODY_BYTES) {
        return addPublicHeaders(jsonResponse({ error: "request body is too large" }, 413), requestID);
      }
      if (!configured(env)) {
        return addPublicHeaders(jsonResponse({ error: "query service is not configured" }, 503), requestID);
      }

      const clientKey = request.headers.get("CF-Connecting-IP") ?? "anonymous";
      const rateLimit = await env.QUERY_RATE_LIMITER.limit({ key: clientKey });
      if (!rateLimit.success) {
        return addPublicHeaders(jsonResponse({ error: "rate limit exceeded" }, 429), requestID);
      }

      try {
        const response = await containerStub(env).fetch(request);
        const withCORS = new Response(response.body, response);
        withCORS.headers.set("Access-Control-Allow-Origin", "*");
        return addPublicHeaders(withCORS, requestID);
      } catch (error) {
        console.error("query container request failed", { requestID, error });
        return addPublicHeaders(
          jsonResponse({ error: "query service is temporarily unavailable" }, 503),
          requestID,
        );
      }
    }

    if (request.method !== "GET" && request.method !== "HEAD") {
      return addPublicHeaders(jsonResponse({ error: "not found" }, 404), requestID);
    }
    const assetResponse = await env.ASSETS.fetch(request);
    return addPublicHeaders(assetResponse, requestID, false);
  },
} satisfies ExportedHandler<Env>;
