/**
 * 采集编排器。
 *
 * 流程：
 *  全量：分页抓取目录（重复链接去重合并）→ 按发布顺序抓更正列表（登记全部修订）
 *        → 并发拉详情核实待核实修订（详情仍旧修订则保留 PENDING 等待复查）
 *        → 全部核实后全量完成；之后周期性增量追更正。
 *  增量：重扫更正列表发现新修订 → 复查所有 PENDING（含上次遗留的详情滞后修订）
 *        → 只把“从上次进度开始连续核实”的发布范围计入增量进度。
 *
 * 所有进度状态写入 DuckDB；进程被强制结束后重启，依据库内状态继续，
 * 已保存的核实结果原样沿用。
 */
import { config } from "../config.js";
import { Database } from "../db.js";
import { HttpFetcher } from "./http.js";
import { parseCatalog, parseCorrections, CatalogLink, CorrectionItem } from "./parse.js";
import { DetailScheduler, SchedulerStats } from "./scheduler.js";
import { sleep } from "../util/rng.js";

export interface ProgressSnapshot {
  phase: "IDLE" | "FULL" | "INCREMENTAL";
  running: boolean;
  runId: number | null;
  catalog: {
    cursor: number;
    totalPages: number | null;
    done: boolean;
  };
  corrections: {
    cursor: number;
    totalPages: number | null;
    totalPublished: number | null;
  };
  details: {
    total: number;
    verified: number;
    pending: number;
  };
  /** 详情调度器状态：内存窗口（≤64）、延迟等待、积压。 */
  schedule: SchedulerStats;
  /** 从上次进度开始连续核实到的最大发布序号（增量进度口径）。 */
  contiguousVerifiedSeq: number;
  fullCompleted: boolean;
  fullCompletedAt: string | null;
  lastCatchupAt: string | null;
  http: { requests: number; retried: number; retryAfterWaits: number };
  lastError: string | null;
  updatedAt: string;
}

export class Collector {
  private running = false;
  private phase: ProgressSnapshot["phase"] = "IDLE";
  private runId: number | null = null;
  private lastError: string | null = null;
  private totalPublished: number | null = null;
  private stopRequested = false;
  private catchupTimer: NodeJS.Timeout | null = null;

  constructor(
    private db: Database,
    private fetcher: HttpFetcher,
    private baseUrl: string,
    private scheduler: DetailScheduler,
  ) {}

  async start(autoFull: boolean): Promise<void> {
    // 进程被强制结束后重启：把上一轮残留的 RUNNING 运行标记为 INTERRUPTED。
    // 断点保留在 run_state 中，随后会新建运行续跑；正常完成的运行已是终态，不受影响。
    await this.db.all(
      `UPDATE runs SET status = 'INTERRUPTED', finished_at = now(),
                       last_error = COALESCE(last_error, '进程被强制结束，重启后续跑')
        WHERE status = 'RUNNING'`,
    );
    // 先启动详情调度器：已安排的复查/退避时刻从库里恢复，重启后照旧遵守。
    await this.scheduler.start();
    const state = await this.db.getRunState();
    if (state.full_completed) {
      this.log(`检测到全量已完成（连续核实序号 ${state.last_contiguous_verified_seq}），进入增量模式`);
      this.scheduleCatchup(200);
      return;
    }
    if (autoFull) {
      void this.runFullForever();
    }
  }

  /** 请求停止（用于优雅关闭；被 kill -9 时由重启恢复兜底）。 */
  async shutdown(): Promise<void> {
    this.stopRequested = true;
    if (this.catchupTimer) clearTimeout(this.catchupTimer);
    await this.scheduler.stop();
    // 等待当前运行循环让出
    const deadline = Date.now() + 3000;
    while (this.running && Date.now() < deadline) await sleep(50);
  }

  private log(msg: string): void {
    console.log(`[collector] ${new Date().toISOString()} ${msg}`);
  }

  private buildUrl(path: string): string {
    return `${this.baseUrl}${path.startsWith("/") ? path : `/${path}`}`;
  }

  // ---------------- 全量 ----------------

  async runFullForever(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      this.runId = await this.db.startRun("FULL");
      this.phase = "FULL";
      this.lastError = null;
      this.log(`全量采集开始（run #${this.runId}，支持中断后重启续跑）`);

      await this.crawlCatalog();
      await this.crawlCorrections();
      // 详情核实交给后台调度器：积压时按固定窗口（≤64 个地址）分批推进，
      // 退避/等待复查中的地址让出窗口，空闲额度用于其他地址。
      // 积压未清空期间仍周期性重扫更正列表：追更登记的新修订用空闲额度核实。
      this.log("目录与更正列表登记完成，开始按调度窗口核实详情");
      let lastRescan = Date.now();
      let drained = false;
      while (!this.stopRequested) {
        drained = await this.scheduler.drainPending({
          timeoutMs: config.catchupIntervalMs,
          shouldStop: () => this.stopRequested,
        });
        if (drained) break;
        if (Date.now() - lastRescan >= config.catchupIntervalMs) {
          lastRescan = Date.now();
          const { newestSeq } = await this.crawlCorrections(true);
          await this.db.saveRunState({ last_catchup_at: new Date().toISOString() });
          this.log(`积压核实中追更正完成，站点已发布至 ${newestSeq}，待核实 ${(await this.db.countByStatus()).pending} 条`);
        }
      }
      if (!drained) {
        await this.db.finishRun(this.runId!, "INTERRUPTED", "收到停止信号");
        this.log("全量采集被中断，进度已保存，重启后续跑");
        return;
      }

      const contiguous = await this.recomputeContiguous(0);
      const state = await this.db.getRunState();
      await this.db.saveRunState({
        full_completed: true,
        full_completed_at: new Date().toISOString(),
        last_contiguous_verified_seq: contiguous,
      });
      await this.db.finishRun(this.runId!, "COMPLETED", null);
      this.log(`全量采集完成：共 ${state.last_contiguous_verified_seq || contiguous} 个发布序号已连续核实`);
      this.scheduleCatchup(config.catchupIntervalMs);
    } catch (err) {
      this.lastError = err instanceof Error ? err.message : String(err);
      this.log(`全量采集出错：${this.lastError}（状态已保存，重启可续跑）`);
      if (this.runId !== null) await this.db.finishRun(this.runId!, "FAILED", this.lastError);
      // 出错后延迟自愈重启，避免崩溃退出
      setTimeout(() => void this.runFullForever(), 3000);
    } finally {
      this.running = false;
      this.runId = null;
      this.phase = "IDLE";
    }
  }

  // ---------------- 目录 ----------------

  private async crawlCatalog(): Promise<void> {
    const state = await this.db.getRunState();
    let page = state.catalog_page_cursor + 1;
    let totalPages = state.catalog_total_pages;
    const seen = new Set<string>();
    let dupCount = 0;

    while (!this.stopRequested) {
      if (totalPages !== null && page > totalPages) break;
      const url = this.buildUrl(`/catalog?page=${page}`);
      if (await this.db.hasFetchedUrl(url)) {
        page += 1;
        if (totalPages !== null && page > totalPages) break;
        continue;
      }
      const res = await this.fetcher.get(url);
      const parsed = parseCatalog(res.body, page);
      totalPages = parsed.totalPages;

      for (const link of parsed.links) {
        if (seen.has(link.detailUrl)) {
          dupCount += 1;
          continue; // 同一详情地址的重复链接合并
        }
        seen.add(link.detailUrl);
        await this.registerCatalogLink(link);
      }
      await this.db.recordUrl(url, "CATALOG", "DONE", null);
      await this.db.saveRunState({
        catalog_page_cursor: page,
        catalog_total_pages: totalPages,
      });
      this.log(`目录第 ${page}/${totalPages} 页完成（本页链接 ${parsed.links.length} 条，累计重复合并 ${dupCount} 条）`);
      page += 1;
      if (page > totalPages) break;
    }
    this.log(`目录抓取完成：去重后详情链接 ${seen.size} 个，重复链接合并 ${dupCount} 条`);
  }

  private async registerCatalogLink(link: CatalogLink): Promise<void> {
    // 目录先发现时发布序号未知（0），待更正列表公布后回填
    await this.db.upsertPublishedRevision({
      coreId: link.coreId,
      revision: link.revision,
      publishedSeq: 0,
      publishedAt: link.updatedAt || new Date(0).toISOString(),
      detailReadySeq: null,
    });
  }

  // ---------------- 更正列表 ----------------

  /**
   * 抓取更正列表。
   * @param rescan false（全量）：从持久化游标续跑，进程被杀后不重复翻页；
   *               true（增量/重扫）：从第 1 页开始全量重扫，upsert 幂等去重，
   *               这样全量期间发布的更正也一定能被后续增量接上，且能适应页数增长。
   */
  private async crawlCorrections(
    rescan = false,
  ): Promise<{ newestSeq: number; totalPages: number | null }> {
    const state = await this.db.getRunState();
    let page = rescan ? 1 : state.correction_page_cursor + 1;
    let totalPages = rescan ? null : state.correction_total_pages;
    let newestSeq = state.last_contiguous_verified_seq;

    while (!this.stopRequested) {
      if (!rescan && totalPages !== null && page > totalPages) break;
      const url = this.buildUrl(`/corrections?page=${page}`);
      const res = await this.fetcher.get(url);
      const parsed = parseCorrections(res.body);
      totalPages = parsed.totalPages;
      this.totalPublished = parsed.totalPublished;

      const items = parsed.items;
      newestSeq = items.reduce((m, it) => Math.max(m, it.publishedSeq), newestSeq);

      await this.registerCorrectionsBatch(items);
      await this.db.recordUrl(url, "CORRECTIONS", "DONE", null);
      await this.db.saveRunState({
        correction_page_cursor: page,
        correction_total_pages: totalPages,
      });
      // 新登记的修订立即参与调度（追更用空闲额度核实，等待中的地址让出窗口）
      this.scheduler.notify();
      this.log(
        `${rescan ? "重扫" : "全量"}更正列表第 ${page}/${totalPages} 页完成` +
          `（本页 ${parsed.items.length} 条，已发布总数 ${parsed.totalPublished}）`,
      );
      page += 1;
      if (page > totalPages) break;
    }
    return { newestSeq, totalPages };
  }

  private async registerCorrectionsBatch(items: CorrectionItem[]): Promise<void> {
    if (items.length === 0) return;
    const existing = new Set(
      (
        await this.db.all<{ core_id: string; revision: number }>(
          `SELECT core_id, revision FROM identifications
            WHERE core_id IN (${items.map(() => "?").join(",")})`,
          [...new Set(items.map((i) => i.coreId))],
        )
      ).map((r) => `${r.core_id}\u0000${Number(r.revision)}`),
    );
    await this.db.upsertPublishedRevisionsBatch(
      items.map((item) => ({
        coreId: item.coreId,
        revision: item.revision,
        publishedSeq: item.publishedSeq,
        publishedAt: item.publishedAt,
        detailReadySeq: null,
      })),
    );
    for (const item of items) {
      if (!existing.has(`${item.coreId}\u0000${item.revision}`)) {
        this.log(`发现新发布修订：${item.coreId} r${item.revision}（发布序号 ${item.publishedSeq}，${item.note}）`);
      }
    }
  }

  // ---------------- 详情核实 ----------------
  //
  // 详情请求与调度完全交给 DetailScheduler：
  //  - 同一详情地址的多个待核实修订共用一次复查（按 revision 各自判定）；
  //  - 内存窗口 ≤64 个地址，退避/等待复查中的地址让出槽位；
  //  - 所有请求仍经统一 HttpFetcher（站点限额 + Retry-After）。

  // ---------------- 增量 ----------------

  private scheduleCatchup(delayMs: number): void {
    if (this.catchupTimer) clearTimeout(this.catchupTimer);
    this.catchupTimer = setTimeout(() => {
      void this.runCatchup();
    }, delayMs);
  }

  async runCatchup(): Promise<void> {
    if (this.running) {
      this.scheduleCatchup(500);
      return;
    }
    this.running = true;
    this.phase = "INCREMENTAL";
    const runId = await this.db.startRun("INCREMENTAL");
    this.runId = runId;
    try {
      const before = await this.db.getRunState();
      const baseline = before.last_contiguous_verified_seq;
      this.log(`增量追更正开始（上次连续核实序号 ${baseline}）`);

      // 重扫更正列表：全量期间发布的更正也会在这里登记并接上
      const { newestSeq } = await this.crawlCorrections(true);

      // 复查全部待核实修订（含详情滞后遗留 + 新发布修订）。
      // 调度器只用空闲额度推进：积压再多也按固定窗口分批，退避中的地址不会抢额度。
      const drained = await this.scheduler.drainPending({
        timeoutMs: config.catchupDrainBudgetMs,
        shouldStop: () => this.stopRequested,
      });
      if (!drained) {
        this.log("增量复查未能在本次窗口内清零，剩余待核实修订留待下一轮（已安排的复查/退避时间保留）");
      }

      const contiguous = await this.recomputeContiguous(baseline);
      await this.db.saveRunState({
        last_contiguous_verified_seq: contiguous,
        last_catchup_at: new Date().toISOString(),
      });
      await this.db.finishRun(runId, "COMPLETED", null);
      this.log(
        `增量完成：本次新连续核实范围 (${baseline} → ${contiguous}]，` +
          `站点已发布至 ${newestSeq}，待核实 ${(await this.db.countByStatus()).pending} 条`,
      );
    } catch (err) {
      this.lastError = err instanceof Error ? err.message : String(err);
      this.log(`增量出错：${this.lastError}`);
      await this.db.finishRun(runId, "FAILED", this.lastError);
    } finally {
      this.running = false;
      this.runId = null;
      this.phase = "IDLE";
      this.scheduleCatchup(config.catchupIntervalMs);
    }
  }

  /**
   * 计算从 baseline 开始连续核实到的最大发布序号。
   * 只计入序号连续且对应修订全部 VERIFIED 的范围；遇到缺口即停止。
   */
  private async recomputeContiguous(baseline: number): Promise<number> {
    const rows = await this.db.all<{ published_seq: number; status: string }>(
      `SELECT published_seq, status FROM identifications
        WHERE published_seq > 0
        ORDER BY published_seq`,
    );
    const verifiedSeqs = new Set(rows.filter((r) => r.status === "VERIFIED").map((r) => Number(r.published_seq)));
    let cursor = baseline;
    for (;;) {
      const next = cursor + 1;
      if (verifiedSeqs.has(next)) cursor = next;
      else break;
    }
    return cursor;
  }

  // ---------------- 进度快照 ----------------

  snapshotSchedule(coreId?: string) {
    return this.scheduler.snapshotSchedule(coreId);
  }

  async snapshot(): Promise<ProgressSnapshot> {
    const state = await this.db.getRunState();
    const counts = await this.db.countByStatus();
    const schedule = await this.scheduler.stats();
    return {
      phase: this.phase,
      running: this.running,
      runId: this.runId,
      catalog: {
        cursor: Number(state.catalog_page_cursor),
        totalPages: state.catalog_total_pages === null ? null : Number(state.catalog_total_pages),
        done:
          state.catalog_total_pages !== null &&
          Number(state.catalog_page_cursor) >= Number(state.catalog_total_pages),
      },
      corrections: {
        cursor: Number(state.correction_page_cursor),
        totalPages:
          state.correction_total_pages === null ? null : Number(state.correction_total_pages),
        totalPublished: this.totalPublished,
      },
      details: {
        total: counts.verified + counts.pending,
        verified: counts.verified,
        pending: counts.pending,
      },
      schedule,
      contiguousVerifiedSeq: Number(state.last_contiguous_verified_seq),
      fullCompleted: state.full_completed,
      fullCompletedAt: state.full_completed_at,
      lastCatchupAt: state.last_catchup_at,
      http: { ...this.fetcher.stats },
      lastError: this.lastError,
      updatedAt: new Date().toISOString(),
    };
  }
}
