import { IngestError, postLogs } from "../api/ingest";
import type { Batch } from "./generate";

export interface Progress {
  batchesSent: number;
  recordsSent: number;
  batchesFailed: number;
  recordsFailed: number;
  retries: number;
  lastError?: string;
  elapsedMs: number;
}

export interface SendOptions {
  concurrency: number;
  maxAttempts?: number;
  signal: AbortSignal;
  onProgress: (progress: Progress) => void;
  post?: (body: string, signal: AbortSignal) => Promise<void>;
  sleep?: (ms: number, signal: AbortSignal) => Promise<void>;
}

function abortableSleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(resolve, ms);
    signal.addEventListener("abort", () => {
      clearTimeout(timer);
      reject(signal.reason);
    }, { once: true });
  });
}

/**
 * Posts every batch with `concurrency` requests in flight. Retryable failures
 * (429, 5xx from the proxy or ingester, network) back off and retry; a batch
 * that still fails is counted and the run continues. Abort stops new batches
 * and cancels in-flight ones.
 */
export async function sendBatches(batches: Iterable<Batch>, options: SendOptions): Promise<Progress> {
  const post = options.post ?? postLogs;
  const sleep = options.sleep ?? abortableSleep;
  const maxAttempts = options.maxAttempts ?? 5;
  const started = performance.now();
  const progress: Progress = { batchesSent: 0, recordsSent: 0, batchesFailed: 0, recordsFailed: 0, retries: 0, elapsedMs: 0 };
  const report = () => options.onProgress({ ...progress, elapsedMs: performance.now() - started });
  const iterator = batches[Symbol.iterator]();

  const worker = async () => {
    while (!options.signal.aborted) {
      const next = iterator.next();
      if (next.done) return;
      const batch = next.value;
      for (let attempt = 1; ; attempt++) {
        try {
          await post(batch.payload, options.signal);
          progress.batchesSent++;
          progress.recordsSent += batch.records;
          break;
        } catch (e) {
          if (options.signal.aborted) return;
          const retryable = e instanceof IngestError && e.retryable;
          progress.lastError = (e as Error).message;
          if (!retryable || attempt >= maxAttempts) {
            progress.batchesFailed++;
            progress.recordsFailed += batch.records;
            break;
          }
          progress.retries++;
          report();
          try {
            await sleep(Math.min(8000, 250 * 2 ** (attempt - 1)), options.signal);
          } catch {
            return;
          }
        }
      }
      report();
    }
  };

  await Promise.all(Array.from({ length: Math.max(1, options.concurrency) }, worker));
  progress.elapsedMs = performance.now() - started;
  return progress;
}
