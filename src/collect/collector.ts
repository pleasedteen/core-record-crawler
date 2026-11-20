/**
 * 采集编排器。
 *
 * 流程：
 *  全量：分页抓取目录（重复链接去重合并）→ 按发布顺序抓更正列表（登记全部公告，
 *        PUBLISH/WITHDRAW 各占一个发布序号）→ 并发拉详情核实待核实修订
 *        （详情仍旧修订则保留 PENDING 等待复查；撤回的修订不再核实）
 *        → 全部公告处置完毕（VERIFIED 或 WITHDRAWN）后全量完成；之后周期性增量追更正。
 *  增量：重扫更正列表发现新公告（含撤回公告与同号重发）→ 复查所有 PENDING
 *        （含上次遗留的详情滞后修订）→ 连续进度遇到未处置序号（含撤回缺口）即停止。
 *
 * 所有进度状态写入 DuckDB；进程被强制结束后重启，依据库内状态继续，
 * 已保存的核实/撤回结果原样沿用。
 */
import { config } from "../config.js";
import { Database, IdentificationRow } from "../db.js";
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
    withdrawn: number;
  };
  /** 更正公告台账：发布数、撤回数、已处置序号数（原发布与撤回都计入）。 */
  announcements: {
    total: number;
    publishes: number;
    withdraws: number;
    disposed: number;
  };
  /** 从上次进度开始连续推进到的最大发布序号（撤回占号但不在撤回处推进）。 */
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
      this.log(`检测到全量已完成（连续进度序号 ${state.last_contiguous_verified_seq}），进入增量模式`);
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

  /** 所有已发布公告是否均已处置：撤回公告天然已处置，发布须 VERIFIED 或 WITHDRAWN。 */
  private async allDisposed(): Promise<boolean> {
    const rows = await this.db.listEventDispositions();
    return rows.every((r) => r.kind === "WITHDRAW" || r.status === "VERIFIED" || r.status === "WITHDRAWN");
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
      // 详情可能滞后：多轮复查直到全部公告处置完毕（每轮之间站点时钟会推进或重试退避）
      let pass = 0;
      while (!this.stopRequested) {
        pass += 1;
        const pendingBefore = await this.db.listPending();
        if (pendingBefore.length === 0 && (await this.allDisposed())) break;
        if (pendingBefore.length > 0) {
          this.log(`详情核实第 ${pass} 轮，待核实 ${pendingBefore.length} 条`);
          await this.verifyPending(pendingBefore);
        }
        const still = await this.db.listPending();
        if (still.length === 0 && (await this.allDisposed())) break;
        this.log(`仍有 ${still.length} 条详情滞后或公告未处置，等待站点详情更新后复查`);
        await sleep(500);
      }

      if (this.stopRequested) {
        await this.db.finishRun(this.runId!, "INTERRUPTED", "收到停止信号");
        this.log("全量采集被中断，进度已保存，重启后续跑");
        return;
      }

      const contiguous = await this.recomputeContiguous(0);
      await this.db.saveRunState({
        full_completed: true,
        full_completed_at: new Date().toISOString(),
        last_contiguous_verified_seq: contiguous,
      });
      await this.db.finishRun(this.runId!, "COMPLETED", null);
      this.log(`全量采集完成：连续进度推进至发布序号 ${contiguous}（撤回占号，缺口前停止）`);
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
    // 目录先发现时发布序号未知（0），待更正列表公布后回填；目录不区分发布批次，按 issue=1 登记
    await this.db.upsertPublishedRevision({
      coreId: link.coreId,
      revision: link.revision,
      issue: 1,
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
   *               这样全量期间发布的更正/撤回也一定能被后续增量接上，且能适应页数增长。
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
      const res = await this.fetcher.get(url);
      const parsed = parseCorrections(res.body);
      totalPages = parsed.totalPages;
      // 重扫时页数可能增长（运行时撤回/重发会追加公告），取各页口径的最大值
      this.totalPublished = Math.max(this.totalPublished ?? 0, parsed.totalPublished);

      for (const item of parsed.items) await this.registerCorrection(item);
      const maxSeq = parsed.items.reduce((m, it) => Math.max(m, it.publishedSeq), newestSeq);
      newestSeq = maxSeq;

      await this.db.recordUrl(url, "CORRECTIONS", "DONE", null);
      await this.db.saveRunState({
        correction_page_cursor: page,
        correction_total_pages: totalPages,
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
    // 公告先入库：PUBLISH 与 WITHDRAW 都占发布序号，重扫幂等
    await this.db.recordAnnouncement({
      kind: item.kind,
      publishedSeq: item.publishedSeq,
      coreId: item.coreId,
      revision: item.revision,
      issue: item.issue,
      withdrawnIssue: item.withdrawnIssue,
      publishedAt: item.publishedAt,
      note: item.note,
    });

    if (item.kind === "WITHDRAW") {
      const issue = item.withdrawnIssue ?? 1;
      const outcome = await this.db.markWithdrawn({
        coreId: item.coreId,
        revision: item.revision,
        issue,
        withdrawSeq: item.publishedSeq,
      });
      this.log(
        `撤回公告入库（序号 ${item.publishedSeq}）：${item.coreId} r${item.revision}#${issue}` +
          `${outcome === "missing" ? "（原发布尚未登记，后续重扫补齐状态）" : " 已标记 WITHDRAWN"}`,
      );
      return;
    }

    // PUBLISH（含撤回后沿用原修订号重发，issue 区分批次）
    const issue = item.issue ?? 1;
    const result = await this.db.upsertPublishedRevision({
      coreId: item.coreId,
      revision: item.revision,
      issue,
      publishedSeq: item.publishedSeq,
      publishedAt: item.publishedAt,
      detailReadySeq: null,
    });
    if (result === "inserted") {
      this.log(
        `发现新发布：${item.coreId} r${item.revision}#${issue}` +
          `（发布序号 ${item.publishedSeq}${issue > 1 ? "，同号重发" : ""}）`,
      );
    }
  }

  // ---------------- 详情核实 ----------------

  /**
   * 并发核实待核实修订。
   * 同一详情地址的多个待核实修订共享同一次响应：
   * 同一待核实修订只合并采集一次，后续修订/重发仍会触发复查（按 revision+issue 各自判定）。
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
          // 同一详情地址已有在途请求：等待其完成后再依据各自修订/批次复查
          await job.catch(() => undefined);
          await this.evaluateOne(row);
        }
      }
    };

    await Promise.all(Array.from({ length: workers }, () => worker()));
  }

  /** 拉取一次详情，并对该岩芯所有 PENDING 修订逐一判定（按 revision+issue 匹配）。 */
  private async fetchAndVerify(url: string, coreId: string): Promise<void> {
    try {
      const res = await this.fetcher.get(url);
      const detail = parseDetail(res.body, coreId);
      await this.db.recordUrl(url, "DETAIL", "DONE", null);
      const keyOf = (revision: number, issue: number) => `${revision}#${issue}`;
      const byKey = new Map(detail.history.map((h) => [keyOf(h.revision, h.issue), h]));
      const pendingRows = (await this.db.listPending()).filter((r) => r.core_id === coreId);
      for (const row of pendingRows) {
        const match = byKey.get(keyOf(row.revision, row.issue));
        if (match) {
          // 该批次已在详情页（含修订历史）生效 → 核实通过，立即可查
          await this.db.markVerified(coreId, row.revision, row.issue, match);
          this.log(
            `核实通过：${coreId} r${row.revision}#${row.issue}（岩性 ${match.classification}，核实人 ${match.verifier}）`,
          );
        } else {
          // 列表已公布（或同号重发已公布）、详情尚未生效 → 保持 PENDING，后续复查。
          // 旧详情仍展示期间不切换，等本次发布在详情页核实后才切换。
          await this.db.markStillStale(
            coreId,
            row.revision,
            row.issue,
            detail.currentRevision,
            detail.currentIssue,
          );
        }
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      await this.db.recordUrl(url, "DETAIL", "FAILED", message).catch(() => undefined);
      const pendingRows = (await this.db.listPending()).filter((r) => r.core_id === coreId);
      for (const row of pendingRows) {
        await this.db.recordAttempt(row.core_id, row.revision, row.issue, message);
      }
      this.lastError = message;
      this.log(`详情抓取失败 ${coreId}：${message}（将在后续轮次重试）`);
    }
  }

  /** 复用在途详情响应后的二次复查入口（同一 URL 不重复发请求的场景之外兜底）。 */
  private async evaluateOne(row: IdentificationRow): Promise<void> {
    const fresh = await this.db.getIdentification(row.core_id, row.revision, row.issue);
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
      this.log(`增量追更正开始（上次连续进度序号 ${baseline}）`);

      // 重扫更正列表：全量期间发布的更正/撤回也会在这里登记并接上
      const { newestSeq } = await this.crawlCorrections(true);

      // 复查全部待核实修订（含详情滞后遗留 + 新发布/同号重发）
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
      const counts = await this.db.countByStatus();
      this.log(
        `增量完成：连续进度 (${baseline} → ${contiguous}]，` +
          `站点已发布至 ${newestSeq}，待核实 ${counts.pending} 条，已撤回 ${counts.withdrawn} 条`,
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
   * 计算从 baseline 开始连续推进到的最大发布序号。
   * 每个序号都必须已处置：WITHDRAW 公告天然已处置；PUBLISH 对应行须 VERIFIED/WITHDRAWN。
   * 撤回公告占用序号，但若其本身落在缺口处，连续进度不会在撤回处推进——仍停在缺口前。
   * 序号只许前进不许回退（撤回不回拨连续进度）。
   */
  private async recomputeContiguous(baseline: number): Promise<number> {
    const rows = await this.db.listEventDispositions();
    const disposed = new Set(
      rows
        .filter(
          (r) =>
            r.kind === "WITHDRAW" || r.status === "VERIFIED" || r.status === "WITHDRAWN",
        )
        .map((r) => Number(r.published_seq)),
    );
    let cursor = baseline;
    for (;;) {
      const next = cursor + 1;
      if (disposed.has(next)) cursor = next;
      else break;
    }
    return cursor;
  }

  // ---------------- 进度快照 ----------------

  async snapshot(): Promise<ProgressSnapshot> {
    const state = await this.db.getRunState();
    const counts = await this.db.countByStatus();
    const announcements = await this.db.countAnnouncements();
    const dispositions = await this.db.listEventDispositions();
    const disposed = dispositions.filter(
      (r) => r.kind === "WITHDRAW" || r.status === "VERIFIED" || r.status === "WITHDRAWN",
    ).length;
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
        // 以公告台账为持久化口径：运行时撤回/重发追加公告后，重启也能拿到真实已发布总数
        totalPublished: announcements.total > 0 ? announcements.total : this.totalPublished,
      },
      details: {
        total: counts.verified + counts.pending + counts.withdrawn,
        verified: counts.verified,
        pending: counts.pending,
        withdrawn: counts.withdrawn,
      },
      announcements: {
        total: announcements.total,
        publishes: announcements.publishes,
        withdraws: announcements.withdraws,
        disposed,
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
