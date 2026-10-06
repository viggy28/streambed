import { DurableObject } from "cloudflare:workers";

interface Env {
  QUERY_CONTAINER: DurableObjectNamespace<QueryContainer>;
  QUERY_RATE_LIMITER: RateLimit;
  R2_ACCESS_KEY_ID: string;
  R2_SECRET_ACCESS_KEY: string;
  S3_BUCKET: string;
  S3_ENDPOINT: string;
  S3_PREFIX: string;
}

const CONTAINER_NAME = "public-query";
const CONTAINER_PORT = 8080;
const INACTIVITY_TIMEOUT_MS = 60_000;
const MAX_HTTP_BODY_BYTES = 66 * 1024;

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

function jsonResponse(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value) + "\n", {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function addPublicHeaders(response: Response, requestID: string): Response {
  const headers = new Headers(response.headers);
  headers.set("Access-Control-Allow-Origin", "*");
  headers.set("Cache-Control", "no-store");
  headers.set("X-Content-Type-Options", "nosniff");
  headers.set("X-Request-ID", requestID);
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
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
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Max-Age": "86400",
          },
        }),
        requestID,
      );
    }

    if (request.method === "GET" && url.pathname === "/") {
      return addPublicHeaders(
        jsonResponse({
          service: "Streambed Hacker News SQL demo",
          query_endpoint: "POST /query",
          request: { sql: "SELECT * FROM front_page LIMIT 10" },
        }),
        requestID,
      );
    }

    if (request.method === "GET" && url.pathname === "/health") {
      return addPublicHeaders(jsonResponse({ status: "ok" }), requestID);
    }

    if (request.method !== "POST" || url.pathname !== "/query") {
      return addPublicHeaders(jsonResponse({ error: "not found" }, 404), requestID);
    }

    const contentLength = Number(request.headers.get("Content-Length") ?? "0");
    if (Number.isFinite(contentLength) && contentLength > MAX_HTTP_BODY_BYTES) {
      return addPublicHeaders(jsonResponse({ error: "request body is too large" }, 413), requestID);
    }

    if (!env.R2_ACCESS_KEY_ID || !env.R2_SECRET_ACCESS_KEY) {
      return addPublicHeaders(
        jsonResponse({ error: "query service is not configured" }, 503),
        requestID,
      );
    }

    const clientKey = request.headers.get("CF-Connecting-IP") ?? "anonymous";
    const rateLimit = await env.QUERY_RATE_LIMITER.limit({ key: clientKey });
    if (!rateLimit.success) {
      return addPublicHeaders(jsonResponse({ error: "rate limit exceeded" }, 429), requestID);
    }

    try {
      const container = env.QUERY_CONTAINER.getByName(CONTAINER_NAME);
      const response = await container.fetch(request);
      return addPublicHeaders(response, requestID);
    } catch (error) {
      console.error("query container request failed", { requestID, error });
      return addPublicHeaders(
        jsonResponse({ error: "query service is temporarily unavailable" }, 503),
        requestID,
      );
    }
  },
} satisfies ExportedHandler<Env>;
