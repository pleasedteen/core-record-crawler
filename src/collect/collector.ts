/**
 * 采集编排器（统一运行管理）。
 *
 * 三类触发——启动自动（STARTUP）、定时调度（SCHEDULED）、手工接口（MANUAL）——
 * 全部走同一个受理入口 requestRun()：同步占位 → runs 落 RUNNING 记录 → 执行采集
 * → closeRun 在单事务内同时写入完成进度与运行终态。
 *  1) 手工触发：runs 记录插入成功后才返回 202，故返回时运行历史已可查到本次记录；
 *  2) 并发竞争：同步占位保证同一时刻只有一个入口能开始采集，其余得到 409；
 *  3) 等待站点（Retry-After / 详情滞后复查）期间不占任何库写锁，查询接口照常可用；
 *  4) 强杀恢复：全部断点与核实结果落 DuckDB，重启把残留 RUNNING 标记为 INTERRUPTED，
 *     已核实结果原样沿用，新运行从断点继续；
 *  5) 进度与结案同事务：全量完成标志/增量连续序号与 runs 终态同时生效，不会撕裂。
 *
 * 采集流程：
 *  全量：分页抓取目录（重复链接去重合并）→ 按发布顺序抓更正列表（登记全部修订）
 *        → 并发拉详情核实待核实修订（详情仍旧修订则保留 PENDING 等待复查）
 *        → 全部核实后全量完成；之后周期性增量追更正。
 *  增量：重扫更正列表发现新修订 → 复查所有 PENDING（含上次遗留的详情滞后修订）
 *        → 只把“从上次进度开始连续核实”的发布范围计入增量进度。
 */
import { config } from "../config.js";
import { Database, IdentificationRow, RunTrigger } from "../db.js";
import { HttpFetcher } from "./http.js";
import { parseCatalog, parseCorrections, parseDetail, CatalogLink, CorrectionItem } from "./parse.js";
import { sleep } from "../util/rng.js";

export type RunMode = "FULL" | "INCREMENTAL";

export type TriggerResult =
  | { accepted: true; runId: number; mode: RunMode }
  | { accepted: false; reason: "busy" | "stopping"; activeRunId: number | null };

export interface ProgressSnapshot {
  phase: "IDLE" | "FULL" | "INCREMENTAL";
  running: boolean;
  runId: number | null;
  /** 当前运行的触发来源：STARTUP / SCHEDULED / MANUAL（空闲时为 null）。 */
  trigger: RunTrigger | null;
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
  lastError: string | null;
  updatedAt: string;
}

interface ActiveRun {
  id: number;
  mode: RunMode;
  trigger: RunTrigger;
  startedAt: number;
}

export class Collector {
  /** 运行槽位：同步读写，保证并发触发只有一个入口能占位成功。 */
  private active: ActiveRun | null = null;
  private lastError: string | null = null;
  private totalPublished: number | null = null;
  private stopRequested = false;
  private catchupTimer: NodeJS.Timeout | null = null;

  constructor(
    private db: Database,
    private fetcher: HttpFetcher,
    private baseUrl: string,
  ) {}

  // ---------------- 统一运行入口（启动 / 定时 / 手工共用） ----------------

  /**
   * 受理一次采集运行。三类触发共用本方法，行为一致：
   *  - 同步检查并占位：在第一个 await 之前完成，并发请求只有一个能通过
   *    （Node 单线程 + 同步代码不可交错，这是“竞争时只有一个入口能开始采集”的保证）；
   *  - 先向 runs 插入 RUNNING 记录（手工 202 因此在返回前已可在运行历史查到）；
   *  - 实际采集异步执行，本方法在“记录落库”后即返回。
   */
  requestRun(mode: RunMode, trigger: RunTrigger): Promise<TriggerResult> {
    // —— 以下全部为同步代码：并发请求无论何时到达，都不可能同时通过占位检查 ——
    if (this.stopRequested) {
      return Promise.resolve({ accepted: false, reason: "stopping", activeRunId: this.active?.id ?? null });
    }
    if (this.active) {
      return Promise.resolve({ accepted: false, reason: "busy", activeRunId: this.active.id });
    }
    // 先占槽位（runId 落库后回填），保证后续到达的请求同步看到“已有运行”
    const token: ActiveRun = { id: 0, mode, trigger, startedAt: Date.now() };
    this.active = token;
    // —— 首个 await 之后槽位已属于本次受理 ——
    return this.acceptRun(token);
  }

  private async acceptRun(token: ActiveRun): Promise<TriggerResult> {
    const { mode, trigger } = token;
    try {
      // 先落库再返回：调用方收到受理结果时，/runs 已能查到本次 RUNNING 记录
      const runId = await this.db.startRun(mode, trigger);
      token.id = runId;
    } catch (err) {
      // 落库失败时释放槽位（仅当槽位仍属于本次受理），交由调用方按失败处理
      if (this.active === token) this.active = null;
      throw err;
    }
    this.lastError = null;
    this.log(`运行受理：${mode}（run #${token.id}，触发来源 ${trigger}）`);
    void this.executeRun(token);
    return { accepted: true, runId: token.id, mode };
  }

  /** 运行执行主体：结束（完成/失败/中断）后统一走 closeRun 结案并恢复调度。 */
  private async executeRun(active: ActiveRun): Promise<void> {
    const { id: runId, mode, trigger } = active;
    let terminalStatus: "COMPLETED" | "FAILED" | "INTERRUPTED" = "COMPLETED";
    let terminalError: string | null = null;
    let statePatch: Parameters<Database["closeRun"]>[3] = {};
    let scheduleNextCatchup = false;

    try {
      if (mode === "FULL") {
        const result = await this.runFullLoop();
        if (this.stopRequested) {
          terminalStatus = "INTERRUPTED";
          terminalError = "收到停止信号";
        } else {
          terminalStatus = "COMPLETED";
          statePatch = result.statePatch;
          scheduleNextCatchup = true;
        }
      } else {
        const result = await this.runIncrementalLoop();
        if (this.stopRequested) {
          terminalStatus = "INTERRUPTED";
          terminalError = "收到停止信号";
        } else {
          terminalStatus = "COMPLETED";
          statePatch = result.statePatch;
        }
      }
    } catch (err) {
      terminalStatus = "FAILED";
      terminalError = err instanceof Error ? err.message : String(err);
      this.lastError = terminalError;
      this.log(`运行出错（run #${runId}）：${terminalError}（断点已保存，重启可续跑）`);
    }

    try {
      // 进度与结案在同一事务提交：同时生效，外部观察不到中间态
      await this.db.closeRun(runId, terminalStatus, terminalError, statePatch);
    } catch (closeErr) {
      // 结案事务本身失败：记录保留 RUNNING，重启恢复时判为 INTERRUPTED，不丢断点
      this.lastError = closeErr instanceof Error ? closeErr.message : String(closeErr);
      this.log(`结案事务失败（run #${runId}）：${this.lastError}（重启后按 INTERRUPTED 恢复）`);
    }

    // 仅当槽位仍属于本次运行时释放，避免与重启/重入路径串扰
    if (this.active === active) this.active = null;
    this.log(`运行结案：run #${runId} → ${terminalStatus}`);

    if (this.stopRequested) return;

    // 保留既有调度规则：
    //  - 全量完成/失败后按周期安排增量；全量失败额外 3s 自愈重启全量；
    //  - 增量每次结束（含失败）后按周期重排，忙碌时 500ms 后重试。
    if (mode === "FULL") {
      if (terminalStatus === "FAILED") {
        setTimeout(() => void this.requestRun("FULL", "STARTUP"), 3000);
      }
      this.scheduleCatchup(config.catchupIntervalMs);
    } else if (scheduleNextCatchup || terminalStatus === "FAILED" || trigger === "SCHEDULED") {
      this.scheduleCatchup(config.catchupIntervalMs);
    }
  }

  async start(autoFull: boolean): Promise<void> {
    // 进程被强制结束后重启：把上一轮残留的 RUNNING 运行标记为 INTERRUPTED。
    // 断点保留在 run_state 中，随后会新建运行续跑；正常完成的运行已是终态，不受影响。
    await this.db.all(
      `UPDATE runs SET status = 'INTERRUPTED', finished_at = now(),
                       last_error = COALESCE(last_error, '进程被强制结束，重启后续跑')
        WHERE status = 'RUNNING'`,
    );
    await this.db.saveRunState({ active_run_id: null, mode: null });
    const state = await this.db.getRunState();
    if (state.full_completed) {
      this.log(`检测到全量已完成（连续核实序号 ${state.last_contiguous_verified_seq}），进入增量模式`);
      this.scheduleCatchup(200);
      return;
    }
    if (autoFull) {
      await this.requestRun("FULL", "STARTUP");
    }
  }

  /** 请求停止（用于优雅关闭；被 kill -9 时由重启恢复兜底）。 */
  async shutdown(): Promise<void> {
    this.stopRequested = true;
    if (this.catchupTimer) clearTimeout(this.catchupTimer);
    // 等待当前运行循环让出
    const deadline = Date.now() + 3000;
    while (this.active && Date.now() < deadline) await sleep(50);
  }

  private log(msg: string): void {
    console.log(`[collector] ${new Date().toISOString()} ${msg}`);
  }

  private buildUrl(path: string): string {
    return `${this.baseUrl}${path.startsWith("/") ? path : `/${path}`}`;
  }

  // ---------------- 全量 ----------------

  /**
   * 全量采集循环。正常结束时返回需要与结案同事务提交的进度补丁；
   * 等待站点（Retry-After、详情滞后复查）期间只做 HTTP 等待，查询接口不受影响。
   */
  private async runFullLoop(): Promise<{ statePatch: Partial<import("../db.js").RunStateRow> }> {
    this.log(`全量采集开始（run #${this.active!.id}，支持中断后重启续跑）`);

    await this.crawlCatalog();
    await this.crawlCorrections();
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
      this.log("仍有详情滞后，等待站点详情更新后复查（查询接口照常可用）");
      await sleep(500);
    }
    if (this.stopRequested) return { statePatch: {} };

    const contiguous = await this.recomputeContiguous(0);
    const state = await this.db.getRunState();
    this.log(`全量采集完成：共 ${state.last_contiguous_verified_seq || contiguous} 个发布序号已连续核实`);
    return {
      statePatch: {
        full_completed: true,
        full_completed_at: new Date().toISOString(),
        last_contiguous_verified_seq: contiguous,
      },
    };
  }

  // ---------------- 目录 ----------------

  private async crawlCatalog(): Promise<void> {
    const state = await this.db.getRunState();
    let page = state.catalog_page_cursor + 1;
    let totalPages = state.catalog_total_pages;
    const seen = new Set<string>();
    let dupCount = 0;

    while (!this.stopRequested) {
      // 续跑边界：上一轮已抓完全部目录页（含被杀重启）时直接结束，不再请求越界页码
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
      // 续跑边界：上一轮已抓完全部更正页（含被杀重启）时直接结束，避免请求越界页码
      if (!rescan && totalPages !== null && page > totalPages) break;
      const url = this.buildUrl(`/corrections?page=${page}`);
      const res = await this.fetcher.get(url);
      const parsed = parseCorrections(res.body);
      totalPages = parsed.totalPages;
      this.totalPublished = parsed.totalPublished;

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
    if (this.stopRequested) return;
    if (this.catchupTimer) clearTimeout(this.catchupTimer);
    this.catchupTimer = setTimeout(() => {
      // 统一入口受理：定时与手工/启动完全一致；忙碌时短延迟重试（既有规则）
      this.requestRun("INCREMENTAL", "SCHEDULED").then((result) => {
        if (!result.accepted && result.reason === "busy") this.scheduleCatchup(500);
      });
    }, delayMs);
  }

  /** 增量追更正循环；进度补丁与结案在同一事务提交。 */
  private async runIncrementalLoop(): Promise<{ statePatch: Partial<import("../db.js").RunStateRow> }> {
    const before = await this.db.getRunState();
    const baseline = before.last_contiguous_verified_seq;
    this.log(`增量追更正开始（run #${this.active!.id}，上次连续核实序号 ${baseline}）`);

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
    if (this.stopRequested) return { statePatch: {} };

    const contiguous = await this.recomputeContiguous(baseline);
    this.log(
      `增量完成：本次新连续核实范围 (${baseline} → ${contiguous}]，` +
        `站点已发布至 ${newestSeq}，待核实 ${(await this.db.countByStatus()).pending} 条`,
    );
    return {
      statePatch: {
        last_contiguous_verified_seq: contiguous,
        last_catchup_at: new Date().toISOString(),
      },
    };
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
    return {
      phase: this.active ? this.active.mode : "IDLE",
      running: this.active !== null,
      runId: this.active?.id ?? null,
      trigger: this.active?.trigger ?? null,
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
      http: { ...this.fetcher.stats },
      lastError: this.lastError,
      updatedAt: new Date().toISOString(),
    };
  }
}
