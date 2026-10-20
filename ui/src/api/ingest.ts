// The ingester's OTLP/HTTP endpoint, reached through the /api/ingest proxy

export class IngestError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }

  /** Worth retrying: overload, unavailable, or the proxy could not reach it */
  get retryable(): boolean {
    return this.status === 0 || this.status === 429 || this.status >= 502;
  }
}

export async function postLogs(body: string, signal?: AbortSignal): Promise<void> {
  let response: Response;
  try {
    response = await fetch("/api/ingest/v1/logs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body,
      signal,
    });
  } catch (e) {
    if (signal?.aborted) throw e;
    throw new IngestError(0, `network error: ${(e as Error).message}`);
  }
  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new IngestError(response.status, `HTTP ${response.status}${text ? `: ${text.slice(0, 200)}` : ""}`);
  }
}
