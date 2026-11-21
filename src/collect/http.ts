/**
 * 多站统一 HTTP 客户端层。
 *
 * 目录、更正列表、详情以及重试请求全部经过这里，两站采集合并到同一服务后共同遵守：
 *  1) 两站共享一个全局在途请求上限（SharedSlotScheduler 的信号量）；
 *  2) 每个站点各自遵守自己的滑动窗口速率限制；
 *  3) 每个站点各自维护自己的 Retry-After 全局暂停时刻，互不影响；
 *  4) 临时失败按站点各自做指数退避重试。
 *
 * 公平分配语义（SharedSlotScheduler）：
 *  - 只有一个站有请求时，该站可以用满全部空闲在途名额；
 *  - 两站都在等名额时，按“轮流分配”（round-robin）放行，任一站不会饿死另一站；
 *  - 速率窗口/站点暂停只让本站点在门控处等待，不占用共享在途名额，
 *    因此一个站等待期间，另一个就绪的站仍能用满名额。
 */
import { config } from "../config.js";
import { sleep } from "../util/rng.js";

export interface FetchResult {
  status: number;
  body: string;
  headers: Record<string, string>;
}

export class HttpError extends Error {
  constructor(
    message: string,
    readonly status: number | null,
    readonly retriable: boolean,
  ) {
    super(message);
    this.name = "HttpError";
  }
}

/**
 * 共享在途名额调度器：两站所有请求共用一个容量为 capacity 的信号量。
 *
 * 关键实现：
 *  - 每个站点各自排队（per-station FIFO），而不是全局单队列；
 *  - 有名额释放时，在“当前有等待者的站点”之间按 round-robin 游标选一个放行，
 *    因此两站都在等时严格轮流；只有一个站在等时它可以连续拿走全部空闲名额；
 *  - 速率限制与站点暂停不在这里判断（见 StationFetcher.gate），保证等待中的站不占名额。
 */
export class SharedSlotScheduler {
  private used = 0;
  private waiters = new Map<string, Array<() => void>>();
  /** round-robin 游标：下一次优先从该站开始挑选等待者。 */
  private cursor = 0;
  /** 观测统计：两站合计在途的高水位（验证共享上限用）。 */
  readonly stats = { highWaterInflight: 0, totalAcquired: 0 };

  constructor(private readonly capacity: number) {}

  get inflight(): number {
    return this.used;
  }

  private stationQueue(stationId: string): Array<() => void> {
    let q = this.waiters.get(stationId);
    if (!q) {
      q = [];
      this.waiters.set(stationId, q);
    }
    return q;
  }

  /** 申请一个共享在途名额；同站内 FIFO，跨站 round-robin。 */
  async acquire(stationId: string): Promise<void> {
    if (this.used < this.capacity) {
      this.grant();
      return;
    }
    await new Promise<void>((resolve) => this.stationQueue(stationId).push(resolve));
    // 被选中放行时名额已在 release()/grant() 中预留，这里直接继续。
  }

  private grant(): void {
    this.used += 1;
    this.stats.totalAcquired += 1;
    if (this.used > this.stats.highWaterInflight) this.stats.highWaterInflight = this.used;
  }

  release(stationId: string): void {
    this.used -= 1;
    const activeStations = [...this.waiters.keys()].filter((sid) => {
      const q = this.waiters.get(sid);
      return q !== undefined && q.length > 0;
    });
    if (activeStations.length === 0 || this.used >= this.capacity) return;

    // round-robin：从游标位置开始找下一个有等待者的站。
    const start = this.cursor % activeStations.length;
    const chosen = activeStations[start];
    this.cursor = (start + 1) % activeStations.length;
    const q = this.waiters.get(chosen)!;
    const next = q.shift()!;
    this.grant(); // 把释放出的名额转交给被选中的等待者
    if (q.length === 0) this.waiters.delete(chosen);
    next();
  }
}

/**
 * 单个站点的 HTTP 客户端。
 * 每个站点拥有：独立速率窗口、独立 Retry-After 暂停、独立退避统计；
 * 在途名额则向两站共享的 SharedSlotScheduler 申请。
 */
export class StationFetcher {
  private windowStarts: number[] = [];
  /** 本站全局暂停截止时间（由本站 Retry-After 设置），只影响本站请求。 */
  private pausedUntil = 0;
  readonly stats = { requests: 0, retried: 0, retryAfterWaits: 0 };
  /**
   * 收到 Retry-After（503/429）并完成计数后的同步回调。
   * 采集器用它把等待/重试计数立即落盘，保证即使下一瞬间被 SIGKILL，
   * 站点等待证据也不丢失。
   */
  onRetryAfter: (() => Promise<void> | void) | null = null;

  constructor(
    readonly stationId: string,
    private readonly baseUrl: string,
    private readonly scheduler: SharedSlotScheduler,
    private readonly rateMaxRequests: number = config.rateMaxRequests,
    private readonly rateWindowMs: number = config.rateWindowMs,
  ) {}

  private parseRetryAfter(value: string | null): number | null {
    if (!value) return null;
    const seconds = Number(value);
    if (Number.isFinite(seconds)) return seconds * 1000;
    const date = Date.parse(value);
    if (!Number.isNaN(date)) return Math.max(0, date - Date.now());
    return null;
  }

  /** 本站点要求等待：只暂停本站后续请求，不影响另一站。 */
  applyRetryAfter(ms: number): void {
    const until = Date.now() + ms;
    if (until > this.pausedUntil) {
      this.pausedUntil = until;
      this.stats.retryAfterWaits += 1;
    }
  }

  isPaused(): boolean {
    return Date.now() < this.pausedUntil;
  }

  /**
   * 门控：在不持有共享在途名额的前提下，等待本站速率窗口腾出位置、
   * 并等过本站的 Retry-After 暂停时刻。等待期间不占共享名额。
   * 返回 true 表示现在可以申请名额发请求。
   */
  private async gate(): Promise<void> {
    for (;;) {
      const now = Date.now();
      const waitPause = Math.max(0, this.pausedUntil - now);
      const cutoff = now - this.rateWindowMs;
      this.windowStarts = this.windowStarts.filter((t) => t > cutoff);
      if (this.windowStarts.length < this.rateMaxRequests && waitPause === 0) {
        this.windowStarts.push(now);
        return;
      }
      let waitMs: number;
      if (waitPause > 0) {
        waitMs = waitPause + 5;
      } else {
        // 等待窗口内最早的请求滑出
        waitMs = Math.max(10, this.windowStarts[0] + this.rateWindowMs - now);
      }
      await sleep(waitMs);
    }
  }

  private async dispatch(url: string): Promise<FetchResult> {
    // 先过本站门控（速率/暂停，不占共享名额），再申请两站共享的在途名额。
    await this.gate();
    await this.scheduler.acquire(this.stationId);
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
      this.scheduler.release(this.stationId);
    }
  }

  /** 带退避重试的 GET。4xx（429 除外）视为不可重试错误。 */
  async get(pathOrUrl: string): Promise<FetchResult> {
    const url = pathOrUrl.startsWith("http")
      ? pathOrUrl
      : `${this.baseUrl}${pathOrUrl.startsWith("/") ? pathOrUrl : `/${pathOrUrl}`}`;
    let attempt = 0;
    let lastError: Error | null = null;

    while (attempt <= config.maxRetries) {
      attempt += 1;
      let result: FetchResult;
      try {
        result = await this.dispatch(url);
      } catch (err) {
        // 网络错误 / 超时：可重试
        lastError = err instanceof Error ? err : new Error(String(err));
        if (attempt > config.maxRetries) break;
        this.stats.retried += 1;
        await this.backoff(attempt, null);
        continue;
      }

      if (result.status === 503 || result.status === 429) {
        const raMs = this.parseRetryAfter(result.headers["retry-after"]);
        if (raMs !== null) this.applyRetryAfter(raMs);
        lastError = new HttpError(
          `[${this.stationId}] 临时拒绝 ${result.status}${raMs !== null ? `（Retry-After ${Math.round(raMs)}ms）` : ""}`,
          result.status,
          true,
        );
        if (attempt > config.maxRetries) break;
        this.stats.retried += 1;
        // 同步回调：把本站等待/重试计数立即落盘（SIGKILL 也不丢证据）
        if (this.onRetryAfter) await Promise.resolve(this.onRetryAfter());
        await this.backoff(attempt, raMs);
        continue;
      }
      if (result.status >= 500) {
        lastError = new HttpError(`[${this.stationId}] 服务端临时错误 ${result.status}`, result.status, true);
        if (attempt > config.maxRetries) break;
        this.stats.retried += 1;
        await this.backoff(attempt, null);
        continue;
      }
      if (result.status >= 400) {
        throw new HttpError(`不可重试的客户端错误 ${result.status}: ${url}`, result.status, false);
      }
      return result;
    }
    throw lastError ?? new HttpError(`请求失败（已重试 ${attempt - 1} 次）: ${url}`, null, true);
  }

  private async backoff(attempt: number, retryAfterMs: number | null): Promise<void> {
    const expo = Math.min(
      config.retryMaxMs,
      config.retryBaseMs * 2 ** (attempt - 1),
    );
    const jitter = Math.random() * config.retryBaseMs;
    // Retry-After 优先级更高：至少等满它（gate() 内还会再校验本站暂停）
    const ms = Math.max(expo + jitter, retryAfterMs ?? 0);
    await sleep(ms);
  }
}
