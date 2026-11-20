/**
 * 详情核实调度器（固定窗口 + 持久化）。
 *
 * 关键约束：
 *  - 内存中最多同时容纳 config.schedulerWindow（硬上限 64）个详情地址，
 *    排队、执行、等待复查/退避都算在内；积压上万条时按批从库里取。
 *  - 每个详情地址（岩芯）一行调度记录，落 DuckDB；强杀重启后仍按已安排的
 *    复查时间与退避时间等待，不会提前发请求。
 *  - 退避/等待中的地址在窗口已满且仍有新地址待安排时，会让出窗口槽位
 *    （换出后保留原时刻存入 detail_deferred），保证追更登记的新修订能用
 *    空闲额度核实。
 *  - 同一地址上的多个待核实修订共用一次复查请求；所有请求仍经过统一
 *    HttpFetcher 的站点限额与全局 Retry-After。
 */
import { config } from "../config.js";
import { Database, IdentificationRow, ScheduleRow, timestampMs } from "../db.js";
import { HttpFetcher, HttpError } from "./http.js";
import { parseDetail } from "./parse.js";
import { sleep } from "../util/rng.js";

export interface SchedulerStats {
  /** 当前窗口内地址数（排队 + 执行 + 等待），上限 64。 */
  resident: number;
  queued: number;
  running: number;
  waiting: number;
  /** 被换出窗口、仍按原时刻等待的地址数（积压的一部分）。 */
  deferred: number;
  /** 仍待核实但尚未进入窗口的地址数。 */
  backlog: number;
  /** 库中还有待核实修订的详情地址总数。 */
  pendingCores: number;
}

export class DetailScheduler {
  private loopPromise: Promise<void> | null = null;
  private stopped = false;
  private wakeUp: (() => void) | null = null;
  private active = new Set<string>();
  private lastHousekeeping = 0;

  constructor(
    private db: Database,
    private fetcher: HttpFetcher,
    private baseUrl: string,
  ) {}

  private log(msg: string): void {
    console.log(`[scheduler] ${new Date().toISOString()} ${msg}`);
  }

  get windowSize(): number {
    return Math.min(64, Math.max(1, config.schedulerWindow));
  }

  /** 启动后台调度循环；重复调用无副作用。 */
  async start(): Promise<void> {
    if (this.loopPromise) return;
    this.stopped = false;
    // 上次进程被强杀时残留的 RUNNING：请求已不存在，立即重新排队；
    // WAITING（已安排的复查/退避）与换出窗口的延迟地址原样保留，继续等到原时刻。
    const recovery = await this.db.recoverSchedulesAfterRestart();
    if (recovery.requeued > 0 || recovery.waiting > 0) {
      this.log(
        `重启恢复：${recovery.requeued} 个在途地址重新排队，` +
          `${recovery.waiting} 个已安排复查/退避的地址继续等待原定时刻`,
      );
    }
    // 升级或调小窗口后，先把窗口压回上限以内再开始巡检，
    // 保证任何时刻（含重启刚完成时）内存窗口都不超过上限。
    const pruned = await this.db.enforceWindow(this.windowSize);
    if (pruned > 0) this.log(`窗口超过上限，已让出 ${pruned} 个地址（等待中的保留原时刻）`);
    // 先同步完成一次巡检再对外返回，避免启动初期窗口为空或超限的过渡态。
    await this.tick();
    this.loopPromise = this.loop().catch((err) => {
      this.log(`调度循环异常：${err instanceof Error ? err.message : String(err)}`);
      this.loopPromise = null;
    });
  }

  async stop(): Promise<void> {
    this.stopped = true;
    this.notify();
    const deadline = Date.now() + 3000;
    while (this.active.size > 0 && Date.now() < deadline) await sleep(50);
  }

  private wakeRequested = false;

  /** 有新修订登记时唤醒调度循环（避免丢失“安装定时器前”的唤醒）。 */
  notify(): void {
    this.wakeRequested = true;
    const wake = this.wakeUp;
    this.wakeUp = null;
    if (wake) wake();
  }

  private wakeableSleep(ms: number): Promise<void> {
    if (this.wakeRequested) {
      this.wakeRequested = false;
      return Promise.resolve();
    }
    if (ms <= 0) return Promise.resolve();
    return new Promise<void>((resolve) => {
      const timer = setTimeout(() => {
        this.wakeUp = null;
        resolve();
      }, ms);
      this.wakeUp = () => {
        clearTimeout(timer);
        this.wakeRequested = false;
        resolve();
      };
    });
  }

  private async loop(): Promise<void> {
    while (!this.stopped) {
      try {
        await this.tick();
      } catch (err) {
        this.log(`巡检出错：${err instanceof Error ? err.message : String(err)}`);
      }
      const wait = await this.nextWakeMs();
      await this.wakeableSleep(wait);
    }
  }

  /** 一次巡检：恢复等待、补充窗口、执行到期地址。 */
  private async tick(): Promise<void> {
    const now = Date.now();
    // 清理已核实地址等全表检查较重，节流执行（不影响请求时序）
    if (now - this.lastHousekeeping >= Math.max(500, config.schedulerTickMs * 5)) {
      this.lastHousekeeping = now;
      await this.db.pruneSchedules();
    }
    await this.db.requeueDueWaiting();

    const resident = await this.db.residentScheduleCount();
    let free = this.windowSize - resident;
    // 任何情况下都不让窗口超限（例如重启恢复、并发巡检的过渡态）
    if (free < 0) {
      await this.db.enforceWindow(this.windowSize);
      free = 0;
    }

    // 已到点的延迟地址优先回到窗口：它们代表“已安排的复查/退避”到期，
    // 必须优先于源源不断的积压，才能保证不提前、也不无限推迟。
    const dueDeferred = await this.db.dueDeferred(Math.max(1, config.evictionBudget));
    for (const row of dueDeferred) {
      if (free <= 0) {
        // 窗口已满：让“还要等最久”的等待地址让位，已到点的安排必须先执行
        const evicted = await this.db.evictFarthestWaitingBatch(1);
        if (evicted === 0) break;
        free += 1;
      }
      await this.db.admitDeferredRow(row);
      free -= 1;
    }

    if (free > 0) {
      const candidates = await this.db.freshScheduleCandidates(free);
      await this.db.admitFreshBatch(candidates.map((c) => c.core_id));
    } else if (dueDeferred.length === 0) {
      // 窗口已满而仍有未安排的积压：只换出 WAITING（已安排复查/退避、尚未到点）
      // 的地址，给新地址让位。换出保留原时刻到 detail_deferred，到点再回来。
      // 新地址按发布序号从新到旧取（追更登记的新修订优先拿到空闲额度）。
      const candidates = await this.db.freshScheduleCandidates(config.evictionBudget);
      if (candidates.length > 0) {
        const evicted = await this.db.evictFarthestWaitingBatch(candidates.length);
        // 只补入真正腾出的槽位，保证窗口任何时刻都不超限
        if (evicted > 0) {
          await this.db.admitFreshBatch(candidates.slice(0, evicted).map((c) => c.core_id));
        }
      }
    }

    await this.launchDue();
  }

  /** 一次性取出本轮可认领的地址，按“已安排优先”排序依次启动。 */
  private async launchDue(): Promise<void> {
    const limit = Math.max(1, config.detailConcurrency);
    for (;;) {
      const capacity = limit - this.active.size;
      if (this.stopped || capacity <= 0) return;
      const rows = await this.db.claimQueuedBatch(capacity);
      if (rows.length === 0) return;
      for (const row of rows) void this.runCore(row.core_id);
      if (rows.length < capacity) return;
    }
  }

  /** 计算下一次需要醒来的等待时长。 */
  private async nextWakeMs(): Promise<number> {
    let wait = config.schedulerTickMs;
    const pause = this.fetcher.pausedFor();
    if (pause > 0) wait = Math.max(wait, Math.min(pause + 10, 2000));

    const rows = await this.db.residentSchedules();
    if (rows.length > 0) {
      const now = Date.now();
      let earliest = Number.POSITIVE_INFINITY;
      let hasFuture = false;
      for (const row of rows) {
        if (row.state === "RUNNING") continue;
        hasFuture = true;
        earliest = Math.min(earliest, timestampMs(row.next_attempt_at));
      }
      if (hasFuture) {
        const delta = earliest - now;
        if (delta > 0) wait = Math.max(wait, Math.min(delta, 1000));
      }
    }
    return wait;
  }

  // ---------------- 单个详情地址 ----------------

  private buildUrl(coreId: string): string {
    return `${this.baseUrl}/core/${coreId}`;
  }

  private recheckDelayMs(attempts: number): number {
    const expo = Math.min(
      config.detailRecheckMaxMs,
      config.detailRecheckBaseMs * 2 ** Math.max(0, attempts - 1),
    );
    return expo;
  }

  private backoffDelayMs(attempts: number, retryAfterMs: number | null): number {
    const expo = Math.min(
      config.detailBackoffMaxMs,
      config.detailBackoffBaseMs * 2 ** Math.max(0, attempts - 1),
    );
    return Math.max(expo, retryAfterMs ?? 0, this.fetcher.pausedFor());
  }

  private async runCore(coreId: string): Promise<void> {
    this.active.add(coreId);
    const url = this.buildUrl(coreId);
    try {
      let res;
      try {
        res = await this.fetcher.fetchOnce(url);
      } catch (err) {
        await this.onFetchFailure(coreId, url, err);
        return;
      }
      await this.onFetchSuccess(coreId, url, res.body);
    } catch (err) {
      // 解析/入库异常也按失败退避，避免地址永久卡在 RUNNING
      this.log(`处理 ${coreId} 出错：${err instanceof Error ? err.message : String(err)}`);
      await this.onFetchFailure(coreId, url, err);
    } finally {
      this.active.delete(coreId);
      this.notify();
    }
  }

  private async onFetchSuccess(coreId: string, url: string, body: string): Promise<void> {
    const detail = parseDetail(body, coreId);
    await this.db.recordUrl(url, "DETAIL", "DONE", null);

    const pending: IdentificationRow[] = await this.db.listPendingForCore(coreId);
    const byRevision = new Map(detail.history.map((h) => [h.revision, h]));
    let stale = 0;

    // 同一地址上的多个待核实修订共用这次复查；各自按 revision 判定。
    for (const row of pending) {
      const match = byRevision.get(row.revision);
      if (match) {
        await this.db.markVerified(coreId, row.revision, match);
        this.log(
          `核实通过：${coreId} r${row.revision}（岩性 ${match.classification}，核实人 ${match.verifier}）`,
        );
      } else {
        stale += 1;
        await this.db.markStillStale(coreId, row.revision, detail.currentRevision);
      }
    }

    const remaining = await this.db.listPendingForCore(coreId);
    if (remaining.length === 0) {
      await this.db.removeSchedule(coreId);
      return;
    }

    const schedule = await this.db.getSchedule(coreId);
    const attempts = Number(schedule?.attempts ?? 0) + 1;
    const delay = this.recheckDelayMs(attempts);
    await this.db.scheduleRecheck(
      coreId,
      Date.now() + delay,
      `详情页仍显示旧修订 r${detail.currentRevision}，${stale} 条待复查`,
    );
  }

  private async onFetchFailure(coreId: string, url: string, err: unknown): Promise<void> {
    const message = err instanceof Error ? err.message : String(err);
    const retryAfterMs = err instanceof HttpError ? err.retryAfterMs : null;
    await this.db.recordUrl(url, "DETAIL", "FAILED", message).catch(() => undefined);

    const pending = await this.db.listPendingForCore(coreId);
    for (const row of pending) await this.db.recordAttempt(row.core_id, row.revision, message);

    const schedule = await this.db.getSchedule(coreId);
    const attempts = Number(schedule?.attempts ?? 0) + 1;
    const delay = this.backoffDelayMs(attempts, retryAfterMs);
    await this.db.scheduleBackoff(coreId, Date.now() + delay, message);
    this.log(`详情抓取失败 ${coreId}：${message}（${Math.round(delay)}ms 后重试）`);
  }

  // ---------------- 对外等待 ----------------

  /**
   * 等待所有待核实修订完成（或被停止/超时打断）。
   * 全量采集与增量追更正都用它把“空闲额度”用满。
   */
  async drainPending(options: { timeoutMs?: number; shouldStop?: () => boolean } = {}): Promise<boolean> {
    this.notify();
    const deadline = options.timeoutMs === undefined ? Infinity : Date.now() + options.timeoutMs;
    for (;;) {
      if (this.stopped) return false;
      if (options.shouldStop?.()) return false;
      const counts = await this.db.countByStatus();
      if (counts.pending === 0) return true;
      if (Date.now() >= deadline) return false;
      // 确保积压不会因为循环休眠而停滞
      this.notify();
      await sleep(Math.min(config.schedulerTickMs, 250));
    }
  }

  async stats(): Promise<SchedulerStats> {
    const counts = await this.db.scheduleStateCounts();
    const deferred = await this.db.deferredScheduleCount();
    const pendingCores = await this.db.countPendingCores();
    const resident = counts.queued + counts.running + counts.waiting;
    return {
      resident,
      queued: counts.queued,
      running: counts.running,
      waiting: counts.waiting,
      deferred,
      backlog: Math.max(0, pendingCores - resident - deferred),
      pendingCores,
    };
  }

  /** 调度明细（含已安排的复查/退避时刻），用于 /schedule 与离线断言。 */
  async snapshotSchedule(coreId?: string): Promise<{
    now: number;
    windowSize: number;
    resident: unknown[];
    deferred: unknown[];
  }> {
    const residentRows = await this.db.residentSchedules();
    const deferredRows = await this.db.deferredSchedules();
    const filter = <T extends { core_id: string }>(rows: T[]): T[] =>
      coreId ? rows.filter((r) => r.core_id === coreId) : rows;
    return {
      now: Date.now(),
      windowSize: this.windowSize,
      resident: filter(residentRows).map((r) => ({
        coreId: r.core_id,
        state: r.state,
        reason: r.reason,
        attempts: Number(r.attempts),
        nextAttemptAt: timestampMs(r.next_attempt_at),
        lastError: r.last_error,
      })),
      deferred: filter(deferredRows).map((r) => ({
        coreId: r.core_id,
        state: r.state,
        reason: r.reason,
        attempts: Number(r.attempts),
        nextAttemptAt: timestampMs(r.next_attempt_at),
        lastError: r.last_error,
      })),
    };
  }
}
