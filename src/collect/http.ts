/**
 * 统一 HTTP 客户端。
 *
 * 目录、更正列表、详情以及重试请求全部经过这里，因此共同遵守：
 *  1) 可配置请求速率（滑动窗口）；
 *  2) 全局在途请求上限（信号量）；
 *  3) 临时失败指数退避重试；
 *  4) 站点返回 Retry-After 时，后续所有请求统一等待到该时刻。
 */
import { config } from "../config.js";
import { sleep } from "../util/rng.js";

export interface FetchResult {
  status: number;
  body: string;
  headers: Record<string, string>;
  /** 本次 get 实际发出的 HTTP 请求数（含退避重试），供调用方计入请求预算。 */
  attempts: number;
}

export class HttpError extends Error {
  /** 失败前实际发出的请求数（含重试）。 */
  attempts: number;

  constructor(
    message: string,
    readonly status: number | null,
    readonly retriable: boolean,
    attempts = 0,
  ) {
    super(message);
    this.name = "HttpError";
    this.attempts = attempts;
  }
}

class Semaphore {
  private used = 0;
  private waiters: Array<() => void> = [];

  constructor(private readonly capacity: number) {}

  async acquire(): Promise<void> {
    if (this.used < this.capacity) {
      this.used += 1;
      return;
    }
    await new Promise<void>((resolve) => this.waiters.push(resolve));
    this.used += 1;
  }

  release(): void {
    this.used -= 1;
    const next = this.waiters.shift();
    if (next) next();
  }
}

export class HttpFetcher {
  private semaphore = new Semaphore(config.maxInflight);
  private windowStarts: number[] = [];
  /** 全局暂停截止时间（由 Retry-After 设置）。 */
  private pausedUntil = 0;
  readonly stats = { requests: 0, retried: 0, retryAfterWaits: 0 };

  private parseRetryAfter(value: string | null): number | null {
    if (!value) return null;
    const seconds = Number(value);
    if (Number.isFinite(seconds)) return seconds * 1000;
    const date = Date.parse(value);
    if (!Number.isNaN(date)) return Math.max(0, date - Date.now());
    return null;
  }

  /** 站点要求等待：后续所有请求（含其他协程）都要等到该时刻之后。 */
  applyRetryAfter(ms: number): void {
    const until = Date.now() + ms;
    if (until > this.pausedUntil) {
      this.pausedUntil = until;
      this.stats.retryAfterWaits += 1;
    }
  }

  /** 取得一个“请求许可”：同时满足在途上限、速率窗口和全局暂停。 */
  private async permit(): Promise<void> {
    await this.semaphore.acquire();
    try {
      for (;;) {
        const now = Date.now();
        const waitPause = Math.max(0, this.pausedUntil - now);
        const cutoff = now - config.rateWindowMs;
        this.windowStarts = this.windowStarts.filter((t) => t > cutoff);
        if (this.windowStarts.length < config.rateMaxRequests && waitPause === 0) {
          this.windowStarts.push(now);
          return;
        }
        let waitMs: number;
        if (waitPause > 0) {
          waitMs = waitPause + 5;
        } else {
          // 等待窗口内最早的请求滑出
          waitMs = Math.max(10, this.windowStarts[0] + config.rateWindowMs - now);
        }
        await sleep(waitMs);
      }
    } catch (err) {
      this.semaphore.release();
      throw err;
    }
  }

  private async dispatch(url: string): Promise<Omit<FetchResult, "attempts">> {
    await this.permit();
    try {
      this.stats.requests += 1;
      const resp = await fetch(url, {
        signal: AbortSignal.timeout(config.requestTimeoutMs),
        headers: { Accept: "text/html, application/json" },
      });
      const body = await resp.text();
      const headers: Record<string, string> = {};
      resp.headers.forEach((v, k) => {
        headers[k.toLowerCase()] = v;
      });
      return { status: resp.status, body, headers };
    } finally {
      this.semaphore.release();
    }
  }

  /**
   * 带退避重试的 GET。4xx（429 除外）视为不可重试错误。
   * opts.maxAttempts 可限制本次调用的最大实际请求数（含重试）：
   * 追更轮的列表请求用它把退避重试也计入每轮预算。
   */
  async get(url: string, opts?: { maxAttempts?: number }): Promise<FetchResult> {
    const maxAttempts = Math.max(1, opts?.maxAttempts ?? config.maxRetries + 1);
    let attempt = 0;
    let dispatches = 0;
    let lastError: Error | null = null;

    while (attempt < maxAttempts) {
      attempt += 1;
      let result: Omit<FetchResult, "attempts">;
      try {
        dispatches += 1;
        result = await this.dispatch(url);
      } catch (err) {
        // 网络错误 / 超时：可重试
        lastError = err instanceof Error ? err : new Error(String(err));
        if (attempt >= maxAttempts) break;
        this.stats.retried += 1;
        await this.backoff(attempt, null);
        continue;
      }

      if (result.status === 503 || result.status === 429) {
        const raMs = this.parseRetryAfter(result.headers["retry-after"]);
        if (raMs !== null) this.applyRetryAfter(raMs);
        lastError = new HttpError(
          `临时拒绝 ${result.status}${raMs !== null ? `（Retry-After ${Math.round(raMs)}ms）` : ""}`,
          result.status,
          true,
        );
        if (attempt >= maxAttempts) break;
        this.stats.retried += 1;
        await this.backoff(attempt, raMs);
        continue;
      }
      if (result.status >= 500) {
        lastError = new HttpError(`服务端临时错误 ${result.status}`, result.status, true);
        if (attempt >= maxAttempts) break;
        this.stats.retried += 1;
        await this.backoff(attempt, null);
        continue;
      }
      if (result.status >= 400) {
        throw new HttpError(`不可重试的客户端错误 ${result.status}: ${url}`, result.status, false, dispatches);
      }
      return { ...result, attempts: dispatches };
    }
    if (lastError instanceof HttpError) {
      lastError.attempts = dispatches;
      throw lastError;
    }
    throw new HttpError(
      `${lastError?.message ?? `请求失败（已重试 ${Math.max(0, attempt - 1)} 次）`}: ${url}`,
      null,
      true,
      dispatches,
    );
  }

  private async backoff(attempt: number, retryAfterMs: number | null): Promise<void> {
    const expo = Math.min(
      config.retryMaxMs,
      config.retryBaseMs * 2 ** (attempt - 1),
    );
    const jitter = Math.random() * config.retryBaseMs;
    // Retry-After 优先级更高：至少等满它（permit() 内还会再校验全局暂停）
    const ms = Math.max(expo + jitter, retryAfterMs ?? 0);
    await sleep(ms);
  }
}
