/**
 * 采集编排器。
 *
 * 流程：
 *  全量：分页抓取目录（重复链接去重合并）→ 按发布顺序抓更正列表，
 *        但只覆盖「启动时已发布的范围」（首次抓第 1 页时快照 totalPublished 落库，
 *        强杀重启后沿用同一范围，不随站点后续公布扩大）→ 对待核实修订核实一轮：
 *        详情已生效的立即核实可查，仍旧修订的保留 PENDING——【不等待详情生效】，
 *        全量就此完成并交接给周期增量。
 *  增量：周期性从第 1 页重扫更正列表 → 全量期间及之后新公布的更正
 *        （即使追加到原来的末页）都被登记接上 → 复查所有 PENDING（含详情滞后遗留）
 *        → 只把“从上次进度开始连续核实”的发布范围计入增量进度。
 *
 * 所有进度状态写入 DuckDB；进程被强制结束后重启，依据库内状态继续，
 * 已保存的核实结果原样沿用。
 */
import { config } from "../config.js";
import { Database, IdentificationRow } from "../db.js";
import { HttpFetcher, HttpError } from "./http.js";
import { parseCatalog, parseCorrections, parseDetail, CatalogLink, CorrectionItem } from "./parse.js";
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
    /** 已核实但发布序号在连续核实进度之外（连续进度等前面缺口补齐）。 */
    verifiedBeyondContiguous: number;
  };
  /** 全量锁定的“启动时已发布”最大序号（断点落库，重启沿用）。 */
  fullTargetSeq: number | null;
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
  ) {}

  async start(autoFull: boolean): Promise<void> {
    // 进程被强制结束后重启：把上一轮残留的 RUNNING 运行标记为 INTERRUPTED。
    // 断点保留在 run_state 中，随后会新建运行续跑；正常完成的运行已是终态，不受影响。
    await this.db.all(
      `UPDATE runs SET status = 'INTERRUPTED', finished_at = now(),
                       last_error = COALESCE(last_error, '进程被强制结束，重启后续跑')
        WHERE status = 'RUNNING'`,
    );
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
      this.log(`全量采集开始（run #${this.runId}）：只覆盖启动时已发布范围，滞后详情交周期增量`);

      await this.crawlCatalog();
      await this.crawlCorrections();
      // 对待核实修订核实一轮：详情已生效的立即核实可查；仍旧修订的保留 PENDING。
      // 不在此等待站点详情生效——全量只交接当前一轮结果，
      // 滞后生效的详情与全量期间新公布的更正都由周期增量接上。
      const pending = await this.db.listPending();
      if (pending.length > 0) {
        this.log(`详情核实一轮：待核实 ${pending.length} 条（滞后生效的不再原地等待）`);
        await this.verifyPending(pending);
      }

      if (this.stopRequested) {
        await this.db.finishRun(this.runId!, "INTERRUPTED", "收到停止信号");
        this.log("全量采集被中断，进度已保存，重启后续跑");
        return;
      }

      const contiguous = await this.recomputeContiguous(0);
      const left = await this.db.countByStatus();
      await this.db.saveRunState({
        full_completed: true,
        full_completed_at: new Date().toISOString(),
        last_contiguous_verified_seq: contiguous,
      });
      await this.db.finishRun(this.runId!, "COMPLETED", null);
      this.log(
        `全量采集完成：连续核实至序号 ${contiguous}，` +
          `剩余待核实 ${left.pending} 条（详情滞后或后公布）交由周期增量复查`,
      );
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
      if (totalPages !== null && page > totalPages) break; // 断点续跑：已知页抓完即停
      const url = this.buildUrl(`/catalog?page=${page}`);
      if (await this.db.hasFetchedUrl(url)) {
        page += 1;
        continue;
      }
      let res;
      try {
        res = await this.fetcher.get(url);
      } catch (err) {
        // 站点目录尚未公布到该页（内嵌站点重启后会重新分批公布）：视为目录结束
        if (err instanceof HttpError && err.status === 404) {
          this.log(`目录第 ${page} 页暂未公布，结束目录翻页`);
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
      await this.db.recordUrl(url, "CATALOG", "DONE", null);
      await this.db.saveRunState({
        catalog_page_cursor: page,
        catalog_total_pages: totalPages,
      });
      this.log(`目录第 ${page}/${totalPages} 页完成（本页链接 ${parsed.links.length} 条，累计重复合并 ${dupCount} 条）`);
      page += 1;
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
   * @param rescan false（全量）：只覆盖「启动时已发布的范围」——首次抓第 1 页时
   *               快照 totalPublished 存入 run_state.full_target_seq，强杀重启后沿用
   *               同一范围；范围之外新公布的更正（即使追加到原来的末页）不在此登记，
   *               留给周期增量接上；
   *               true（增量/重扫）：从第 1 页开始全量重扫，upsert 幂等去重，
   *               接住全部后公布的修订，且能适应页数增长。
   */
  private async crawlCorrections(
    rescan = false,
  ): Promise<{ newestSeq: number; totalPages: number | null }> {
    const state = await this.db.getRunState();
    let target = rescan ? null : state.full_target_seq;
    let page = rescan ? 1 : state.correction_page_cursor + 1;
    let totalPages = rescan ? null : state.correction_total_pages;
    let newestSeq = rescan
      ? state.last_contiguous_verified_seq
      : await this.db.maxRegisteredSeq();

    while (!this.stopRequested) {
      if (!rescan && target !== null && newestSeq >= target) break; // 启动范围已覆盖
      if (totalPages !== null && page > totalPages) break;
      const url = this.buildUrl(`/corrections?page=${page}`);
      let res;
      try {
        res = await this.fetcher.get(url);
      } catch (err) {
        // 列表尚未公布到该页（内嵌站点重启后重新分批公布）：结束翻页，交给增量
        if (err instanceof HttpError && err.status === 404) {
          this.log(`更正列表第 ${page} 页暂未公布，结束翻页`);
          break;
        }
        throw err;
      }
      const parsed = parseCorrections(res.body);
      totalPages = parsed.totalPages;
      this.totalPublished = parsed.totalPublished;

      if (!rescan && target === null) {
        // 快照「启动时已发布的范围」并落库：之后站点再公布也不扩大本次全量
        target = parsed.totalPublished;
        await this.db.saveRunState({ full_target_seq: target });
        this.log(`全量范围快照：启动时已发布序号 ≤ ${target}，之后公布的更正交由周期增量接上`);
      }

      for (const item of parsed.items) {
        if (!rescan && target !== null && item.publishedSeq > target) continue; // 超出启动范围，留给增量
        await this.registerCorrection(item);
      }
      const pageMax = parsed.items.reduce((m, it) => Math.max(m, it.publishedSeq), 0);
      newestSeq = Math.max(newestSeq, pageMax);

      await this.db.recordUrl(url, "CORRECTIONS", "DONE", null);
      await this.db.saveRunState({
        correction_page_cursor: page,
        correction_total_pages: totalPages,
      });
      this.log(
        `${rescan ? "重扫" : "全量"}更正列表第 ${page}/${totalPages} 页完成` +
          `（本页 ${parsed.items.length} 条，站点已发布 ${parsed.totalPublished} 条）`,
      );
      page += 1;
    }
    return { newestSeq, totalPages };
  }

  private async registerCorrection(item: CorrectionItem): Promise<void> {
    const result = await this.db.upsertPublishedRevision({
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
        const url = this.buildUrl(`/core/${row.core_id}`);

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
      await this.db.recordUrl(url, "DETAIL", "DONE", null);
      const byRevision = new Map(detail.history.map((h) => [h.revision, h]));
      const pendingRows = (await this.db.listPending()).filter((r) => r.core_id === coreId);
      for (const row of pendingRows) {
        const match = byRevision.get(row.revision);
        if (match) {
          // 该修订已在详情页（含修订历史）生效 → 核实通过，立即可查
          await this.db.markVerified(coreId, row.revision, match);
          this.log(
            `核实通过：${coreId} r${row.revision}（岩性 ${match.classification}，核实人 ${match.verifier}）`,
          );
        } else if (detail.currentRevision < row.revision) {
          // 列表已公布更正、详情尚未生效 → 保持 PENDING，后续复查
          await this.db.markStillStale(coreId, row.revision, detail.currentRevision);
        } else {
          // 当前修订号更大但历史中缺该条（异常数据）：记录并留待复查
          await this.db.markStillStale(coreId, row.revision, detail.currentRevision);
        }
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      await this.db.recordUrl(url, "DETAIL", "FAILED", message).catch(() => undefined);
      const pendingRows = (await this.db.listPending()).filter((r) => r.core_id === coreId);
      for (const row of pendingRows) await this.db.recordAttempt(row.core_id, row.revision, message);
      this.lastError = message;
      this.log(`详情抓取失败 ${coreId}：${message}（将在后续轮次重试）`);
    }
  }

  /** 复用在途详情响应后的二次复查入口（同一 URL 不重复发请求的场景之外兜底）。 */
  private async evaluateOne(row: IdentificationRow): Promise<void> {
    const fresh = await this.db.getIdentification(row.core_id, row.revision);
    if (fresh && fresh.status === "PENDING") {
      const url = this.buildUrl(`/core/${row.core_id}`);
      await this.fetchAndVerify(url, row.core_id);
    }
  }

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

      // 复查全部待核实修订（含详情滞后遗留 + 新发布修订）
      let pass = 0;
      while (!this.stopRequested && pass < 3) {
        pass += 1;
        const pending = await this.db.listPending();
        if (pending.length === 0) break;
        this.log(`增量复查第 ${pass} 轮，待核实 ${pending.length} 条`);
        await this.verifyPending(pending);
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

  async snapshot(): Promise<ProgressSnapshot> {
    const state = await this.db.getRunState();
    const counts = await this.db.countByStatus();
    const contiguous = Number(state.last_contiguous_verified_seq);
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
        verifiedBeyondContiguous: await this.db.countVerifiedBeyondContiguous(contiguous),
      },
      fullTargetSeq: state.full_target_seq === null ? null : Number(state.full_target_seq),
      contiguousVerifiedSeq: contiguous,
      fullCompleted: state.full_completed,
      fullCompletedAt: state.full_completed_at,
      lastCatchupAt: state.last_catchup_at,
      http: { ...this.fetcher.stats },
      lastError: this.lastError,
      updatedAt: new Date().toISOString(),
    };
  }
}
