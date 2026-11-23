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
import { Database, ReviewPhase } from "../db.js";
import { HttpFetcher } from "./http.js";
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
  };
  /** 从上次进度开始连续核实到的最大发布序号（增量进度口径）。 */
  contiguousVerifiedSeq: number;
  fullCompleted: boolean;
  fullCompletedAt: string | null;
  lastCatchupAt: string | null;
  http: { requests: number; retried: number; retryAfterWaits: number };
  /** 当前复查轮（强杀续跑同一轮）及逐地址采集进度。 */
  review: {
    openRoundId: number | null;
    phase: ReviewPhase | null;
    coresTotal: number;
    coresDone: number;
    coresFailed: number;
    restarts: number;
    resumeDoneSkips: number;
    resumeSkips: number;
    sharedDetailResponses: number;
  };
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
  /** 进程内统计：续跑原轮时跳过的、上轮已采集（DONE）的地址数。 */
  private resumeSkips = 0;
  /** 进程内统计：同址多个待核实修订共享同一次详情采集的命中次数。 */
  private sharedDetailResponses = 0;

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
      this.log(`全量采集开始（run #${this.runId}，支持中断后重启续跑）`);

      await this.crawlCatalog();
      await this.crawlCorrections();
      // 详情可能滞后：一轮复查中每个待核实详情地址只采集一次；
      // 详情仍旧的修订保留 PENDING，多轮复查直到全部核实（轮间站点时钟会推进或重试退避）。
      let pass = 0;
      while (!this.stopRequested) {
        pass += 1;
        const pendingBefore = await this.db.listPending();
        if (pendingBefore.length === 0) break;
        this.log(`详情核实第 ${pass} 轮，待核实 ${pendingBefore.length} 条`);
        await this.runReviewSweep("FULL");
        const still = await this.db.listPending();
        if (still.length === 0) break;
        this.log(`仍有 ${still.length} 条详情滞后，等待站点详情更新后复查`);
        await sleep(500);
      }

      if (this.stopRequested) {
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
    // 同一详情地址可挂多个修订：按「地址 + 修订号」去重，
    // 完全重复的链接合并，同地址的不同修订各自登记，不再漏登。
    const seenRevisions = new Set<string>();
    const seenUrls = new Set<string>();
    let dupCount = 0;

    while (!this.stopRequested) {
      const url = this.buildUrl(`/catalog?page=${page}`);
      // 已持久化总页数时先做边界判断：续跑时内存 seen 已丢失，
      // 不能依赖 URL 去重表决定结束，否则会越过末页请求不存在的页（404 不可重试）。
      if (totalPages !== null && page > totalPages) break;
      if (await this.db.hasFetchedUrl(url)) {
        page += 1;
        if (totalPages !== null && page > totalPages) break;
        continue;
      }
      const res = await this.fetcher.get(url);
      const parsed = parseCatalog(res.body, page);
      totalPages = parsed.totalPages;

      for (const link of parsed.links) {
        if (seenUrls.has(link.detailUrl)) dupCount += 1; // 同一详情地址的重复链接合并统计
        seenUrls.add(link.detailUrl);
        const revisionKey = `${link.detailUrl}#r${link.revision}`;
        if (seenRevisions.has(revisionKey)) continue; // 完全相同的「地址+修订」重复条目
        seenRevisions.add(revisionKey);
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
    this.log(
      `目录抓取完成：去重后详情链接 ${seenUrls.size} 个、修订条目 ${seenRevisions.size} 条，重复链接合并 ${dupCount} 条`,
    );
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
      const url = this.buildUrl(`/corrections?page=${page}`);
      // 非重扫模式下用持久化总页数兜底边界，避免续跑越过末页触发 404。
      if (!rescan && totalPages !== null && page > totalPages) break;
      const res = await this.fetcher.get(url);
      const parsed = parseCorrections(res.body);
      totalPages = parsed.totalPages;
      this.totalPublished = parsed.totalPublished;

      for (const item of parsed.items) await this.registerCorrection(item);
      const maxSeq = parsed.items.reduce((m, it) => Math.max(m, it.publishedSeq), newestSeq);
      newestSeq = maxSeq;
      this.totalPublished = parsed.totalPublished;

      await this.db.recordUrl(url, "CORRECTIONS", "DONE", null);
      await this.db.saveRunState({
        correction_page_cursor: page,
        correction_total_pages: totalPages,
        total_published: parsed.totalPublished,
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

  // ---------------- 详情核实（按复查轮组织） ----------------

  /**
   * 执行一次复查轮（一次扫描）。
   *
   * 不变量：
   *  - 每个有待核实修订的详情地址在本轮只发起一次详情采集（同址多修订共享同一份响应）；
   *  - 详情仍旧（缺少待核实修订）时该地址本轮标记 DONE 但不再补发，修订留待后续复查；
   *  - 临时失败照常重试（HTTP 层退避），仍失败则保留 FAILED 条目，后续轮次再抓；
   *  - 轮次与逐地址状态全部落库：强杀后重启恢复同一个 OPEN 轮次继续，
   *    已 DONE 的地址本轮不再请求。
   */
  private async runReviewSweep(phase: ReviewPhase): Promise<void> {
    const runId = this.runId;
    if (runId === null) throw new Error("复查轮必须在运行上下文中执行");

    const pendingCoreIds = await this.db.listPendingCoreIds();
    if (pendingCoreIds.length === 0) return;

    // 复用未结束的同阶段轮次（强杀后续跑原轮）；否则新建一轮。
    let round = await this.db.getOpenReviewRound(phase);
    if (round) {
      if (round.run_id !== runId) await this.db.reattachReviewRound(round.id, runId);
      // 运行中新登记的同址修订：补登条目；已是 DONE 的地址不会回退，本轮不再请求。
      for (const coreId of pendingCoreIds) await this.db.ensureReviewCoreEntry(round.id, coreId);
      // 续跑核算：轮次内处于 DONE 的地址即“上轮已采集、本轮不再请求”的地址。
      const counts = await this.db.countReviewCoreEntries(round.id);
      if (counts.done > 0) {
        this.resumeSkips += counts.done;
        await this.db.addResumeDoneSkips(round.id, counts.done);
        this.log(
          `继续复查轮 #${round.id}（已重启 ${round.restarts} 次，` +
            `跳过已采集 DONE 地址 ${counts.done} 个，剩余待采集 ${counts.failed + counts.pending} 个）`,
        );
      } else {
        this.log(`继续复查轮 #${round.id}（已重启 ${round.restarts} 次）`);
      }
    } else {
      const id = await this.db.createReviewRound(phase, runId, pendingCoreIds);
      round = await this.db.getReviewRound(id);
      this.log(`新建复查轮 #${id}：待核实详情地址 ${pendingCoreIds.length} 个，每地址本轮只采集一次`);
    }
    const roundId = round!.id;

    // 仅取出本轮尚未成功采集的地址（DONE 不返回，续跑时天然跳过）。
    const entries = await this.db.listReviewCoreEntries(roundId);

    const workers = Math.max(1, config.maxInflight);
    let idx = 0;
    const worker = async (): Promise<void> => {
      for (;;) {
        const myIdx = idx++;
        if (myIdx >= entries.length || this.stopRequested) return;
        const entry = entries[myIdx];
        await this.fetchAndVerifyOne(roundId, entry.core_id);
      }
    };
    await Promise.all(Array.from({ length: workers }, () => worker()));

    if (this.stopRequested) {
      // 保持轮次 OPEN：重启后继续原轮，已保存 DONE 的地址本轮不再请求。
      this.log(`复查轮 #${roundId} 因停止信号暂停，轮次状态已保存，重启后续跑原轮`);
      return;
    }

    const finalRound = await this.db.getReviewRound(roundId);
    const pendingAfter = await this.db.listPendingCoreIds();
    this.log(
      `复查轮 #${roundId} 结束：地址 ${finalRound?.cores_total ?? entries.length} 个，` +
        `成功采集 ${finalRound?.cores_done ?? 0} 个、临时失败 ${finalRound?.cores_failed ?? 0} 个，` +
        `仍待核实修订覆盖地址 ${pendingAfter.length} 个（留待后续复查）`,
    );
    await this.db.closeReviewRound(roundId);
  }

  /**
   * 拉取某岩芯详情一次，并对该岩芯当前所有 PENDING 修订逐一判定。
   * 成功（含详情仍旧）→ 轮次条目 DONE，本轮不再补发；
   * 临时失败 → 轮次条目 FAILED，HTTP 层已做指数退避，后续轮次照常重试。
   */
  private async fetchAndVerifyOne(roundId: number, coreId: string): Promise<void> {
    const url = this.buildUrl(`/core/${coreId}`);
    const entry = await this.db.getReviewCoreEntry(roundId, coreId);
    if (entry?.status === "DONE") return; // 双保险：同址本轮绝不重复采集

    const pendingBefore = await this.db.listPending();
    const pendingRows = pendingBefore.filter((r) => r.core_id === coreId);
    if (pendingRows.length > 1) this.sharedDetailResponses += pendingRows.length - 1;

    try {
      await this.db.incrementDetailRequests(roundId, coreId);
      const res = await this.fetcher.get(url);
      const detail = parseDetail(res.body, coreId);
      await this.db.recordUrl(url, "DETAIL", "DONE", null);
      const byRevision = new Map(detail.history.map((h) => [h.revision, h]));
      for (const row of pendingRows) {
        const match = byRevision.get(row.revision);
        if (match) {
          // 该修订已在详情页（含修订历史）生效 → 核实通过，立即可查
          await this.db.markVerified(coreId, row.revision, match);
          this.log(
            `核实通过：${coreId} r${row.revision}（岩性 ${match.classification}，核实人 ${match.verifier}）`,
          );
        } else {
          // 列表已公布但旧响应里缺少该修订（详情仍旧/尚未生效）：
          // 保持 PENDING，本轮不再补发，留给后续复查轮。
          await this.db.markStillStale(coreId, row.revision, detail.currentRevision);
        }
      }
      // 无论核实通过还是仍旧：该地址本轮采集已完成，不再重复请求。
      await this.db.markReviewCoreDone(
        roundId,
        coreId,
        detail.currentRevision,
        detail.currentPublishedSeq,
      );
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      await this.db.recordUrl(url, "DETAIL", "FAILED", message).catch(() => undefined);
      for (const row of pendingRows) await this.db.recordAttempt(row.core_id, row.revision, message);
      await this.db.markReviewCoreFailed(roundId, coreId, message).catch(() => undefined);
      this.lastError = message;
      this.log(`详情抓取失败 ${coreId}：${message}（临时失败，后续轮次照常重试）`);
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

      // 复查全部待核实修订（含详情滞后遗留 + 新发布修订）：
      // 每轮对同址待核实修订只发起一次详情采集，最多三轮扫描。
      let pass = 0;
      while (!this.stopRequested && pass < 3) {
        pass += 1;
        const pending = await this.db.listPending();
        if (pending.length === 0) break;
        this.log(`增量复查第 ${pass} 轮，待核实 ${pending.length} 条`);
        await this.runReviewSweep("INCREMENTAL");
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
    const openRounds = await this.db.listOpenReviewRounds();
    // 同一时刻只可能有一个阶段在跑；若残留多个 OPEN 轮，取最新的一个展示。
    const openRound = openRounds[openRounds.length - 1] ?? null;
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
        // 优先用进程内最新值；强杀重启后内存丢失则回落到落库值。
        totalPublished: this.totalPublished ?? state.total_published,
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
      http: { ...this.fetcher.stats },
      review: {
        openRoundId: openRound ? openRound.id : null,
        phase: openRound ? openRound.phase : null,
        coresTotal: openRound ? openRound.cores_total : 0,
        coresDone: openRound ? openRound.cores_done : 0,
        coresFailed: openRound ? openRound.cores_failed : 0,
        restarts: openRound ? openRound.restarts : 0,
        resumeDoneSkips: openRound ? openRound.resume_done_skips : 0,
        resumeSkips: this.resumeSkips,
        sharedDetailResponses: this.sharedDetailResponses,
      },
      lastError: this.lastError,
      updatedAt: new Date().toISOString(),
    };
  }
}
