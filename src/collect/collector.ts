/**
 * 采集编排器。
 *
 * 流程：
 *  全量：分页抓取目录（重复链接去重合并）→ 按发布顺序翻页抓更正列表（登记全部修订；
 *        站点以 HTTP 200 返回别页正文时按页码识别为错页，本页留缺、正文仍登记）
 *        → 并发拉详情核实待核实修订（详情仍旧修订则保留 PENDING 等待复查）
 *        → 已登记修订全部核实后全量完成；之后周期性增量追更正。
 *  增量：每轮按可配置的列表请求预算（重试计入）检查更正列表——预算内旧缺页
 *        与末页都有检查机会；缺页恢复、末页追加都在此接上
 *        → 复查所有 PENDING（含上次遗留的详情滞后修订）
 *        → 只把“从上次进度开始连续核实”的发布范围计入增量进度；
 *          缺口之后的有效更正核实后立即可查，但连续进度停在缺口前。
 *
 * 所有进度状态写入 DuckDB（含更正列表逐页收妥状态）；进程被强制结束后重启，
 * 依据库内状态继续：已收妥的完整非尾页不重读，缺页与末页由追更轮继续，
 * 已保存的核实结果原样沿用。
 */
import { config } from "../config.js";
import { Database, IdentificationRow } from "../db.js";
import { HttpFetcher, HttpError } from "./http.js";
import {
  parseCatalog,
  parseCorrections,
  parseDetail,
  CatalogLink,
  CorrectionItem,
  CorrectionsPage,
} from "./parse.js";
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
    /** 尚未完整收妥的更正列表页（旧缺页；末页未收妥时也在内）。 */
    missingPages: number[];
    /** 每轮追更的列表请求预算（重试计入）。 */
    listBudget: number;
    /** 当前运行已发生的更正列表请求数（含重试）。 */
    listRequestsThisRun: number;
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
  /** 当前运行内更正列表的实际请求数（含重试），用于预算核算与运行记录。 */
  private runListRequests = 0;

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
      this.runListRequests = 0;
      this.log(`全量采集开始（run #${this.runId}，支持中断后重启续跑）`);

      await this.crawlCatalog();
      await this.crawlCorrectionsFull();
      // 详情可能滞后：多轮复查直到全部核实（每轮之间站点时钟会推进或重试退避）
      let pass = 0;
      while (!this.stopRequested) {
        pass += 1;
        const pendingBefore = await this.db.listPending();
        if (pendingBefore.length === 0) break;
        this.log(`详情核实第 ${pass} 轮，待核实 ${pendingBefore.length} 条`);
        await this.verifyPending(pendingBefore);
        const still = await this.db.listPending();
        if (still.length === 0) break;
        this.log(`仍有 ${still.length} 条详情滞后，等待站点详情更新后复查`);
        await sleep(500);
      }

      if (this.stopRequested) {
        await this.db.finishRun(this.runId!, "INTERRUPTED", "收到停止信号", this.runListRequests);
        this.log("全量采集被中断，进度已保存，重启后续跑");
        return;
      }

      const contiguous = await this.recomputeContiguous(0);
      await this.db.saveRunState({
        full_completed: true,
        full_completed_at: new Date().toISOString(),
        last_contiguous_verified_seq: contiguous,
      });
      await this.db.finishRun(this.runId!, "COMPLETED", null, this.runListRequests);
      this.log(
        `全量采集完成：连续核实至发布序号 ${contiguous}` +
          `${contiguous < (this.totalPublished ?? 0) ? `（更正列表尚有缺页 ${await this.describeMissingPages()}，由追更轮补齐）` : ""}`,
      );
      this.scheduleCatchup(config.catchupIntervalMs);
    } catch (err) {
      this.lastError = err instanceof Error ? err.message : String(err);
      this.log(`全量采集出错：${this.lastError}（状态已保存，重启可续跑）`);
      if (this.runId !== null) await this.db.finishRun(this.runId!, "FAILED", this.lastError, this.runListRequests);
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
   * 抓取一页更正列表（只发请求与解析，不落库）。
   * 站点偶尔以 HTTP 200 返回别页正文：matched=false 时 parsed 是别页内容。
   * maxAttempts 用于追更轮把退避重试也计入每轮列表请求预算。
   */
  private async fetchCorrectionsPage(
    page: number,
    maxAttempts?: number,
  ): Promise<{
    attempts: number;
    parsed: CorrectionsPage | null;
    matched: boolean;
    error: string | null;
  }> {
    const url = this.buildUrl(`/corrections?page=${page}`);
    let attempts = 0;
    try {
      const res = await this.fetcher.get(url, maxAttempts !== undefined ? { maxAttempts } : undefined);
      attempts = res.attempts;
      let parsed: CorrectionsPage;
      try {
        parsed = parseCorrections(res.body);
      } catch (err) {
        return {
          attempts,
          parsed: null,
          matched: false,
          error: `更正列表第 ${page} 页正文解析失败：${err instanceof Error ? err.message : String(err)}`,
        };
      }
      return { attempts, parsed, matched: parsed.page === page, error: null };
    } catch (err) {
      if (err instanceof HttpError && err.attempts > 0) attempts = err.attempts;
      if (attempts === 0) attempts = 1;
      return {
        attempts,
        parsed: null,
        matched: false,
        error: err instanceof Error ? err.message : String(err),
      };
    }
  }

  /**
   * 登记一页更正列表正文（无论是否错页响应，正文本身都是有效数据）：
   * 登记全部条目、把正文对应页标记为已收妥、记录总页数变化。
   * 总页数变大时旧末页可能又追加了条目，置回待补（除非本次正文就是旧末页）。
   */
  private async ingestCorrectionsPayload(
    parsed: CorrectionsPage,
    ctx: { totalPages: number | null },
  ): Promise<void> {
    this.totalPublished = parsed.totalPublished;
    if (
      ctx.totalPages !== null &&
      parsed.totalPages > ctx.totalPages &&
      parsed.page !== ctx.totalPages
    ) {
      await this.db.invalidateCorrectionPage(ctx.totalPages);
    }
    if (parsed.totalPages > (ctx.totalPages ?? 0)) ctx.totalPages = parsed.totalPages;

    for (const item of parsed.items) await this.registerCorrection(item);
    await this.db.markCorrectionPageComplete(parsed.page, parsed.items.length);
    await this.db.recordUrl(this.buildUrl(`/corrections?page=${parsed.page}`), "CORRECTIONS", "DONE", null);
  }

  /** 当前未完整收妥的更正列表页（含未收妥的末页）。 */
  private async listMissingPages(totalPages: number | null): Promise<number[]> {
    if (totalPages === null || totalPages <= 0) return [];
    const complete = new Set(await this.db.listCompleteCorrectionPages());
    const missing: number[] = [];
    for (let p = 1; p <= totalPages; p++) if (!complete.has(p)) missing.push(p);
    return missing;
  }

  private async describeMissingPages(): Promise<string> {
    const state = await this.db.getRunState();
    const missing = await this.listMissingPages(
      state.correction_total_pages === null ? null : Number(state.correction_total_pages),
    );
    return missing.length > 0 ? missing.join(",") : "无";
  }

  /**
   * 全量走查更正列表：从持久化游标续跑，进程被杀后不重复翻页；
   * 已收妥的页（含错页响应顺带收妥的页）直接跳过不重读。
   * 错页页留缺（游标照常前进），由后续追更轮在预算内恢复。
   */
  private async crawlCorrectionsFull(): Promise<void> {
    const state = await this.db.getRunState();
    let page = state.correction_page_cursor + 1;
    const ctx = { totalPages: state.correction_total_pages };
    const completePages = new Set(await this.db.listCompleteCorrectionPages());

    while (!this.stopRequested) {
      if (ctx.totalPages !== null && page > ctx.totalPages) break;
      if (completePages.has(page)) {
        this.log(`全量：更正列表第 ${page} 页已收妥，跳过重读`);
        page += 1;
        continue;
      }
      const r = await this.fetchCorrectionsPage(page);
      this.runListRequests += r.attempts;
      if (!r.parsed) {
        // 与既有行为一致：硬失败让整轮失败并自愈重跑（游标已持久化，重启续跑）
        throw new Error(`更正列表第 ${page} 页抓取失败：${r.error}`);
      }
      await this.ingestCorrectionsPayload(r.parsed, ctx);
      completePages.add(r.parsed.page);

      if (!r.matched) {
        // 错页：正文仍登记，但请求页留缺待补
        await this.db.recordUrl(
          this.buildUrl(`/corrections?page=${page}`),
          "CORRECTIONS",
          "FAILED",
          `错页：站点返回第 ${r.parsed.page} 页正文`,
        );
        await this.db.recordCorrectionPageAttempt(page, `请求第 ${page} 页，站点返回第 ${r.parsed.page} 页正文`);
        this.log(
          `全量：更正列表第 ${page} 页错页（站点返回第 ${r.parsed.page} 页正文），本页留缺，待追更轮补齐`,
        );
      } else {
        this.log(
          `全量：更正列表第 ${page}/${ctx.totalPages} 页完成` +
            `（本页 ${r.parsed.items.length} 条，已发布总数 ${r.parsed.totalPublished}）`,
        );
      }
      await this.db.saveRunState({
        correction_page_cursor: page,
        correction_total_pages: ctx.totalPages,
      });
      page += 1;
    }
  }

  /**
   * 追更轮的预算制更正列表检查。
   *
   * 每轮预算 catchupListBudget 个列表请求（重试按实际请求数计入）；
   * 轮内计划保证旧缺页与末页都有检查机会：缺页恢复与末页追加都在这里接上。
   * 预算为 1 时按轮次奇偶在缺页与末页之间轮流。未收妥页排回队尾，
   * 预算有剩余时本轮内重试；预算耗尽则留到下一轮。
   */
  private async crawlCorrectionsBudgeted(): Promise<void> {
    const budget = Math.max(1, config.catchupListBudget);
    const state = await this.db.getRunState();
    const ctx = { totalPages: state.correction_total_pages };
    if (ctx.totalPages === null || ctx.totalPages <= 0) {
      // 防御性兜底：全量尚未走到更正列表就被中断，先顺序走查一遍
      await this.crawlCorrectionsFull();
      return;
    }

    const tail = ctx.totalPages;
    const oldMissing = (await this.listMissingPages(ctx.totalPages)).filter((p) => p !== tail);
    const parity = (await this.db.countRunsByMode("INCREMENTAL")) % 2;
    const queue: number[] = [];
    if (oldMissing.length === 0) {
      queue.push(tail); // 无缺页：每轮仍复查末页，末页追加在此接上
    } else if (parity === 0) {
      queue.push(oldMissing[0], tail, ...oldMissing.slice(1));
    } else {
      queue.push(tail, ...oldMissing);
    }

    let spent = 0;
    const completedThisRound = new Set<number>();
    while (!this.stopRequested && queue.length > 0 && spent < budget) {
      const page = queue.shift()!;
      const r = await this.fetchCorrectionsPage(page, budget - spent);
      spent += r.attempts;
      this.runListRequests += r.attempts;

      if (!r.parsed) {
        await this.db.recordCorrectionPageAttempt(page, r.error ?? "未知错误");
        this.log(`追更：更正列表第 ${page} 页抓取失败：${r.error}（本轮列表请求 ${spent}/${budget}）`);
        queue.push(page);
        continue;
      }

      const knownBefore = ctx.totalPages ?? 0;
      await this.ingestCorrectionsPayload(r.parsed, ctx);
      completedThisRound.add(r.parsed.page);
      if ((ctx.totalPages ?? 0) > knownBefore) {
        // 末页追加：新页排入检查计划（本轮已收妥的除外）；被置回待补的旧末页也重新排队
        for (let p = knownBefore + 1; p <= (ctx.totalPages ?? 0); p++) {
          if (!queue.includes(p) && !completedThisRound.has(p)) queue.push(p);
        }
        if (
          r.parsed.page !== knownBefore &&
          knownBefore > 0 &&
          !queue.includes(knownBefore) &&
          !completedThisRound.has(knownBefore)
        ) {
          queue.push(knownBefore);
        }
      }

      if (!r.matched) {
        // 错页：正文已登记，请求页仍缺，排回队尾预算内重试
        await this.db.recordUrl(
          this.buildUrl(`/corrections?page=${page}`),
          "CORRECTIONS",
          "FAILED",
          `错页：站点返回第 ${r.parsed.page} 页正文`,
        );
        await this.db.recordCorrectionPageAttempt(page, `请求第 ${page} 页，站点返回第 ${r.parsed.page} 页正文`);
        this.log(
          `追更：更正列表第 ${page} 页错页（站点返回第 ${r.parsed.page} 页正文），本页留缺` +
            `（本轮列表请求 ${spent}/${budget}）`,
        );
        queue.push(page);
        continue;
      }
      this.log(
        `追更：更正列表第 ${page}/${ctx.totalPages} 页完成` +
          `（本页 ${r.parsed.items.length} 条，本轮列表请求 ${spent}/${budget}）`,
      );
    }

    await this.db.saveRunState({ correction_total_pages: ctx.totalPages });
    const remaining = await this.listMissingPages(ctx.totalPages);
    if (remaining.length > 0) {
      this.log(`追更：本轮预算 ${budget} 用完，仍缺页 ${remaining.join(",")}，下一轮继续`);
    }
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
    this.runListRequests = 0;
    try {
      const before = await this.db.getRunState();
      const baseline = before.last_contiguous_verified_seq;
      this.log(`增量追更正开始（上次连续核实序号 ${baseline}，本轮列表预算 ${Math.max(1, config.catchupListBudget)}）`);

      // 预算制检查更正列表：旧缺页恢复 + 末页追加都在此接上；
      // 全量期间发布的更正也会经缺页/末页检查登记并接上
      await this.crawlCorrectionsBudgeted();

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
      await this.db.finishRun(runId, "COMPLETED", null, this.runListRequests);
      this.log(
        `增量完成：本次新连续核实范围 (${baseline} → ${contiguous}]，` +
          `站点已发布至 ${this.totalPublished ?? "?"}，待核实 ${(await this.db.countByStatus()).pending} 条，` +
          `本轮列表请求 ${this.runListRequests}/${Math.max(1, config.catchupListBudget)}`,
      );
    } catch (err) {
      this.lastError = err instanceof Error ? err.message : String(err);
      this.log(`增量出错：${this.lastError}`);
      await this.db.finishRun(runId, "FAILED", this.lastError, this.runListRequests);
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
    const totalPages = state.correction_total_pages === null ? null : Number(state.correction_total_pages);
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
        totalPages,
        totalPublished: this.totalPublished,
        missingPages: await this.listMissingPages(totalPages),
        listBudget: Math.max(1, config.catchupListBudget),
        listRequestsThisRun: this.runListRequests,
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
      lastError: this.lastError,
      updatedAt: new Date().toISOString(),
    };
  }
}
