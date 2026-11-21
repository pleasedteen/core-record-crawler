/**
 * 采集编排器（单站实例；两站各跑一个，共享同一个 DuckDB 与共享在途调度器）。
 *
 * 流程：
 *  全量：分页抓取目录（重复链接去重合并）→ 按发布顺序抓更正列表（登记全部修订）
 *        → 并发拉详情核实待核实修订（详情仍旧修订则保留 PENDING 等待复查）
 *        → 全部核实后全量完成；之后周期性增量追更正。
 *  增量：重扫更正列表发现新修订 → 复查所有 PENDING（含上次遗留的详情滞后修订）
 *        → 只把“从上次进度开始连续核实”的发布范围计入增量进度。
 *
 * 所有进度状态按站写入 DuckDB；进程被强制结束后重启，各站依据库内各自断点继续，
 * 已保存的核实结果原样沿用。两站之间通过 SharedSlotScheduler 共享在途上限，
 * 速率限制与 Retry-After 等待则由各自的 StationFetcher 独立维护。
 */
import { config } from "../config.js";
import { Database, IdentificationRow } from "../db.js";
import { StationFetcher, HttpError } from "./http.js";
import { parseCatalog, parseCorrections, parseDetail, CatalogLink, CorrectionItem } from "./parse.js";
import { sleep } from "../util/rng.js";

export interface ProgressSnapshot {
  stationId: string;
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
  /** 从上次进度开始连续核实到的最大发布序号（增量进度口径）。 */
  contiguousVerifiedSeq: number;
  fullCompleted: boolean;
  fullCompletedAt: string | null;
  lastCatchupAt: string | null;
  http: { requests: number; retried: number; retryAfterWaits: number };
  /** 本站是否正处于 Retry-After 等待中。 */
  paused: boolean;
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
  /** 采集运行期间周期性把 HTTP 统计刷盘，保证被 SIGKILL 时计数最多丢一个刷盘间隔。 */
  private statsFlushTimer: NodeJS.Timeout | null = null;
  /** 进程启动前历次运行累计的 HTTP 计数（从 run_state 恢复）；本进程计数在 fetcher 中。 */
  private statsBase = { requests: 0, retried: 0, retryAfterWaits: 0 };
  readonly stationId: string;

  constructor(
    private db: Database,
    private fetcher: StationFetcher,
  ) {
    this.stationId = fetcher.stationId;
    // 收到 Retry-After 立即把累计统计刷入断点表，站点等待证据跨 SIGKILL 不丢
    this.fetcher.onRetryAfter = () =>
      this.flushHttpStats().catch((err) => this.log(`HTTP 统计刷盘失败（忽略）：${String(err)}`));
  }

  async start(autoFull: boolean): Promise<void> {
    // 进程被强制结束后重启：把该站上一轮残留的 RUNNING 运行标记为 INTERRUPTED。
    // 断点保留在 run_state 中，随后会新建运行续跑；正常完成的运行已是终态，不受影响。
    await this.db.markInterruptedRuns(this.stationId);
    const state = await this.db.getRunState(this.stationId);
    // 重启后恢复内存态：已发布总数与跨重启累计的 HTTP 统计都在 run_state 里
    this.totalPublished = state.correction_total_published;
    this.statsBase = {
      requests: Number(state.http_requests ?? 0),
      retried: Number(state.http_retried ?? 0),
      retryAfterWaits: Number(state.http_retry_after_waits ?? 0),
    };
    if (state.full_completed) {
      this.log(`检测到全量已完成（连续核实序号 ${state.last_contiguous_verified_seq}），进入增量模式`);
      this.scheduleCatchup(200);
      return;
    }
    if (autoFull) {
      void this.runFullForever();
    }
  }

  /** 本站累计 HTTP 统计（启动前基线 + 当前进程实时计数）。 */
  private cumulativeHttp(): { requests: number; retried: number; retryAfterWaits: number } {
    return {
      requests: this.statsBase.requests + this.fetcher.stats.requests,
      retried: this.statsBase.retried + this.fetcher.stats.retried,
      retryAfterWaits: this.statsBase.retryAfterWaits + this.fetcher.stats.retryAfterWaits,
    };
  }

  /**
   * 把本站累计 HTTP 计数（历史基线 + 本进程实时）刷到 run_state（断点表），
   * 这样即使被 SIGKILL，请求/重试/Retry-After 等待计数也不丢失。
   */
  private async flushHttpStats(extra?: Partial<import("../db.js").RunStateRow>): Promise<void> {
    await this.db.saveRunState(this.stationId, {
      http_requests: this.statsBase.requests + this.fetcher.stats.requests,
      http_retried: this.statsBase.retried + this.fetcher.stats.retried,
      http_retry_after_waits: this.statsBase.retryAfterWaits + this.fetcher.stats.retryAfterWaits,
      ...extra,
    });
  }

  /** 请求停止（用于优雅关闭；被 kill -9 时由重启恢复兜底）。 */
  async shutdown(): Promise<void> {
    this.stopRequested = true;
    if (this.catchupTimer) clearTimeout(this.catchupTimer);
    this.stopStatsFlush();
    // 等待当前运行循环让出
    const deadline = Date.now() + 3000;
    while (this.running && Date.now() < deadline) await sleep(50);
  }

  /** 启动周期性 HTTP 统计刷盘（运行期间持续，SIGKILL 也只丢一个间隔的数据）。 */
  private startStatsFlush(): void {
    this.stopStatsFlush();
    this.statsFlushTimer = setInterval(() => {
      void this.flushHttpStats().catch(() => undefined);
    }, 400);
  }

  private stopStatsFlush(): void {
    if (this.statsFlushTimer) {
      clearInterval(this.statsFlushTimer);
      this.statsFlushTimer = null;
    }
  }

  private log(msg: string): void {
    console.log(`[collector:${this.stationId}] ${new Date().toISOString()} ${msg}`);
  }

  // ---------------- 全量 ----------------

  async runFullForever(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      this.runId = await this.db.startRun(this.stationId, "FULL");
      this.phase = "FULL";
      this.lastError = null;
      this.startStatsFlush();
      this.log(`全量采集开始（run #${this.runId}，支持中断后重启续跑）`);

      await this.crawlCatalog();
      await this.crawlCorrections();
      // 详情可能滞后：多轮复查直到全部核实（每轮之间站点时钟会推进或重试退避）
      let pass = 0;
      while (!this.stopRequested) {
        pass += 1;
        const pendingBefore = await this.db.listPending(this.stationId);
        if (pendingBefore.length === 0) break;
        this.log(`详情核实第 ${pass} 轮，待核实 ${pendingBefore.length} 条`);
        await this.verifyPending(pendingBefore);
        // 详情轮的重试/等待计数也随断点落盘，被 SIGKILL 后不丢失
        await this.flushHttpStats();
        const still = await this.db.listPending(this.stationId);
        if (still.length === 0) break;
        this.log(`仍有 ${still.length} 条详情滞后，等待站点详情更新后复查`);
        await sleep(500);
      }

      if (this.stopRequested) {
        await this.db.finishRun(this.stationId, this.runId!, "INTERRUPTED", "收到停止信号", this.cumulativeHttp());
        this.log("全量采集被中断，进度已保存，重启后续跑");
        return;
      }

      const contiguous = await this.recomputeContiguous(0);
      const state = await this.db.getRunState(this.stationId);
      await this.flushHttpStats({
        full_completed: true,
        full_completed_at: new Date().toISOString(),
        last_contiguous_verified_seq: contiguous,
      });
      await this.db.finishRun(this.stationId, this.runId!, "COMPLETED", null, this.cumulativeHttp());
      this.log(`全量采集完成：共 ${state.last_contiguous_verified_seq || contiguous} 个发布序号已连续核实`);
      this.scheduleCatchup(config.catchupIntervalMs);
    } catch (err) {
      this.lastError = err instanceof Error ? err.message : String(err);
      this.log(`全量采集出错：${this.lastError}（状态已保存，重启可续跑）`);
      if (this.runId !== null) await this.db.finishRun(this.stationId, this.runId!, "FAILED", this.lastError, this.cumulativeHttp());
      // 出错后延迟自愈重启，避免崩溃退出
      setTimeout(() => void this.runFullForever(), 3000);
    } finally {
      this.stopStatsFlush();
      this.running = false;
      this.runId = null;
      this.phase = "IDLE";
    }
  }

  // ---------------- 目录 ----------------

  private async crawlCatalog(): Promise<void> {
    const state = await this.db.getRunState(this.stationId);
    let page = state.catalog_page_cursor + 1;
    let totalPages = state.catalog_total_pages;
    const seen = new Set<string>();
    let dupCount = 0;

    // 重启续跑时目录可能已全部抓完：游标已到末页则直接结束，避免再请求越界页。
    if (totalPages !== null && page > totalPages) {
      this.log(`目录断点已在末页（${state.catalog_page_cursor}/${totalPages}），跳过目录阶段`);
      return;
    }

    while (!this.stopRequested) {
      const url = `/catalog?page=${page}`;
      if (await this.db.hasFetchedUrl(this.stationId, url)) {
        page += 1;
        if (totalPages !== null && page > totalPages) break;
        continue;
      }
      let res;
      try {
        res = await this.fetcher.get(url);
      } catch (err) {
        // 越界页 404（旧断点/站点页数收缩）：视为目录自然结束，不阻断续跑
        if (err instanceof HttpError && err.status === 404) {
          this.log(`目录第 ${page} 页返回 404，目录到此结束（已知总页数 ${totalPages ?? "?"}）`);
          if (totalPages !== null) {
            await this.flushHttpStats({
              catalog_page_cursor: totalPages,
              catalog_total_pages: totalPages,
            });
          }
          break;
        }
        throw err;
      }
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
      await this.db.recordUrl(this.stationId, url, "CATALOG", "DONE", null);
      await this.flushHttpStats({
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
      stationId: this.stationId,
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
    const state = await this.db.getRunState(this.stationId);
    let page = rescan ? 1 : state.correction_page_cursor + 1;
    let totalPages = rescan ? null : state.correction_total_pages;
    let newestSeq = state.last_contiguous_verified_seq;

    // 续跑时更正列表可能已全部抓完：游标已到末页则无需再翻
    if (!rescan && totalPages !== null && page > totalPages) {
      this.log(`更正列表断点已在末页（${state.correction_page_cursor}/${totalPages}），跳过翻页`);
      return { newestSeq, totalPages };
    }

    while (!this.stopRequested) {
      const url = `/corrections?page=${page}`;
      let res;
      try {
        res = await this.fetcher.get(url);
      } catch (err) {
        if (!rescan && err instanceof HttpError && err.status === 404) {
          this.log(`更正列表第 ${page} 页返回 404，列表到此结束（已知总页数 ${totalPages ?? "?"}）`);
          if (totalPages !== null) {
            await this.flushHttpStats({
              correction_page_cursor: totalPages,
              correction_total_pages: totalPages,
            });
          }
          break;
        }
        throw err;
      }
      const parsed = parseCorrections(res.body);
      totalPages = parsed.totalPages;
      this.totalPublished = parsed.totalPublished;

      for (const item of parsed.items) await this.registerCorrection(item);
      const maxSeq = parsed.items.reduce((m, it) => Math.max(m, it.publishedSeq), newestSeq);
      newestSeq = maxSeq;

      await this.db.recordUrl(this.stationId, url, "CORRECTIONS", "DONE", null);
      await this.flushHttpStats({
        correction_page_cursor: page,
        correction_total_pages: totalPages,
        correction_total_published: parsed.totalPublished,
      });
      this.log(
        `${rescan ? "重扫" : "全量"}更正列表第 ${page}/${totalPages} 页完成` +
          `（本页 ${parsed.items.length} 条，已发布总数 ${parsed.totalPublished}）`,
      );
      page += 1;
      if (page > totalPages) break;
    }
    return { newestSeq, totalPages };
  }

  private async registerCorrection(item: CorrectionItem): Promise<void> {
    const result = await this.db.upsertPublishedRevision({
      stationId: this.stationId,
      coreId: item.coreId,
      revision: item.revision,
      publishedSeq: item.publishedSeq,
      publishedAt: item.publishedAt,
      detailReadySeq: null,
    });
    if (result === "inserted") {
      this.log(`发现新发布修订：${item.coreId} r${item.revision}（发布序号 ${item.publishedSeq}，${item.note}）`);
    }
  }

  // ---------------- 详情核实 ----------------

  /**
   * 并发核实待核实修订。
   * 同一详情地址的多个待核实修订共享同一次响应：
   * 同一待核实修订只合并采集一次，后续修订仍会触发复查（按 revision 各自判定）。
   * 并发度受两站共享的在途上限约束（SharedSlotScheduler），本站不单独开 worker 上限。
   */
  private async verifyPending(pending: IdentificationRow[]): Promise<void> {
    const inFlight = new Map<string, Promise<void>>();
    const workers = Math.max(1, config.maxInflight);
    let idx = 0;

    const worker = async (): Promise<void> => {
      for (;;) {
        const myIdx = idx++;
        if (myIdx >= pending.length || this.stopRequested) return;
        const row = pending[myIdx];
        const url = `/core/${row.core_id}`;

        let job = inFlight.get(url);
        if (!job) {
          job = this.fetchAndVerify(url, row.core_id);
          inFlight.set(url, job);
          try {
            await job;
          } finally {
            inFlight.delete(url);
          }
        } else {
          // 同一详情地址已有在途请求：等待其完成后再依据各自修订复查
          await job.catch(() => undefined);
          await this.evaluateOne(row);
        }
      }
    };

    await Promise.all(Array.from({ length: workers }, () => worker()));
  }

  /** 拉取一次详情，并对该岩芯所有 PENDING 修订逐一判定。 */
  private async fetchAndVerify(url: string, coreId: string): Promise<void> {
    try {
      const res = await this.fetcher.get(url);
      const detail = parseDetail(res.body, coreId);
      await this.db.recordUrl(this.stationId, url, "DETAIL", "DONE", null);
      const byRevision = new Map(detail.history.map((h) => [h.revision, h]));
      const pendingRows = (await this.db.listPending(this.stationId)).filter((r) => r.core_id === coreId);
      for (const row of pendingRows) {
        const match = byRevision.get(row.revision);
        if (match) {
          // 该修订已在详情页（含修订历史）生效 → 核实通过，立即可查
          await this.db.markVerified(this.stationId, coreId, row.revision, match);
          this.log(
            `核实通过：${coreId} r${row.revision}（岩性 ${match.classification}，核实人 ${match.verifier}）`,
          );
        } else if (detail.currentRevision < row.revision) {
          // 列表已公布更正、详情尚未生效 → 保持 PENDING，后续复查
          await this.db.markStillStale(this.stationId, coreId, row.revision, detail.currentRevision);
        } else {
          // 当前修订号更大但历史中缺该条（异常数据）：记录并留待复查
          await this.db.markStillStale(this.stationId, coreId, row.revision, detail.currentRevision);
        }
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      await this.db.recordUrl(this.stationId, url, "DETAIL", "FAILED", message).catch(() => undefined);
      const pendingRows = (await this.db.listPending(this.stationId)).filter((r) => r.core_id === coreId);
      for (const row of pendingRows) await this.db.recordAttempt(this.stationId, row.core_id, row.revision, message);
      this.lastError = message;
      this.log(`详情抓取失败 ${coreId}：${message}（将在后续轮次重试）`);
    }
  }

  /** 复用在途详情响应后的二次复查入口（同一 URL 不重复发请求的场景之外兜底）。 */
  private async evaluateOne(row: IdentificationRow): Promise<void> {
    const fresh = await this.db.getIdentification(this.stationId, row.core_id, row.revision);
    if (fresh && fresh.status === "PENDING") {
      const url = `/core/${row.core_id}`;
      await this.fetchAndVerify(url, row.core_id);
    }
  }

  // ---------------- 增量 ----------------

  private scheduleCatchup(delayMs: number = config.catchupIntervalMs): void {
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
    const runId = await this.db.startRun(this.stationId, "INCREMENTAL");
    this.runId = runId;
    this.startStatsFlush();
    try {
      const before = await this.db.getRunState(this.stationId);
      const baseline = before.last_contiguous_verified_seq;
      this.log(`增量追更正开始（上次连续核实序号 ${baseline}）`);

      // 重扫更正列表：全量期间发布的更正也会在这里登记并接上
      const { newestSeq } = await this.crawlCorrections(true);

      // 复查全部待核实修订（含详情滞后遗留 + 新发布修订）
      let pass = 0;
      while (!this.stopRequested && pass < 3) {
        pass += 1;
        const pending = await this.db.listPending(this.stationId);
        if (pending.length === 0) break;
        this.log(`增量复查第 ${pass} 轮，待核实 ${pending.length} 条`);
        await this.verifyPending(pending);
        await this.flushHttpStats();
      }

      const contiguous = await this.recomputeContiguous(baseline);
      await this.flushHttpStats({
        last_contiguous_verified_seq: contiguous,
        last_catchup_at: new Date().toISOString(),
      });
      await this.db.finishRun(this.stationId, runId, "COMPLETED", null, this.cumulativeHttp());
      this.log(
        `增量完成：本次新连续核实范围 (${baseline} → ${contiguous}]，` +
          `站点已发布至 ${newestSeq}，待核实 ${(await this.db.countByStatus(this.stationId)).pending} 条`,
      );
    } catch (err) {
      this.lastError = err instanceof Error ? err.message : String(err);
      this.log(`增量出错：${this.lastError}`);
      await this.db.finishRun(this.stationId, runId, "FAILED", this.lastError, this.cumulativeHttp());
    } finally {
      this.stopStatsFlush();
      this.running = false;
      this.runId = null;
      this.phase = "IDLE";
      this.scheduleCatchup();
    }
  }

  /**
   * 计算从 baseline 开始连续核实到的最大发布序号。
   * 只计入序号连续且对应修订全部 VERIFIED 的范围；遇到缺口即停止。
   */
  private async recomputeContiguous(baseline: number): Promise<number> {
    const rows = await this.db.all<{ published_seq: number; status: string }>(
      `SELECT published_seq, status FROM identifications
        WHERE station_id = ? AND published_seq > 0
        ORDER BY published_seq`,
      [this.stationId],
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

  async snapshot(): Promise<ProgressSnapshot> {
    const state = await this.db.getRunState(this.stationId);
    const counts = await this.db.countByStatus(this.stationId);
    return {
      stationId: this.stationId,
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
      contiguousVerifiedSeq: Number(state.last_contiguous_verified_seq),
      fullCompleted: state.full_completed,
      fullCompletedAt: state.full_completed_at,
      lastCatchupAt: state.last_catchup_at,
      http: {
        requests: this.statsBase.requests + this.fetcher.stats.requests,
        retried: this.statsBase.retried + this.fetcher.stats.retried,
        retryAfterWaits: this.statsBase.retryAfterWaits + this.fetcher.stats.retryAfterWaits,
      },
      paused: this.fetcher.isPaused(),
      lastError: this.lastError,
      updatedAt: new Date().toISOString(),
    };
  }
}
