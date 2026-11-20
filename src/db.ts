/**
 * DuckDB 嵌入式存储层。
 *
 * 设计要点：
 *  - 单连接 + 互斥锁串行化所有 SQL，避免嵌入式库的并发写锁问题；
 *  - 采集状态全部落库：进程被杀后重启可凭库内状态继续；
 *  - 同一 (core_id, revision) 为主键，天然支撑“同一待核实修订只合并采集一次”；
 *  - 核实结果与待核实状态分字段管理，核实后立即可查。
 */
import duckdb from "duckdb";
import fs from "node:fs";
import nodePath from "node:path";
import { config } from "./config.js";

export type IdentStatus = "PENDING" | "VERIFIED";
export type UrlKind = "CATALOG" | "CORRECTIONS" | "DETAIL";
export type UrlStatus = "DONE" | "FAILED";

export interface IdentificationRow {
  core_id: string;
  revision: number;
  published_seq: number;
  published_at: string;
  detail_ready_seq: number | null;
  classification: string | null;
  formation: string | null;
  depth_top: number | null;
  depth_bottom: number | null;
  verifier: string | null;
  summary: string | null;
  status: IdentStatus;
  attempts: number;
  last_error: string | null;
  updated_at: string;
}

export interface ScheduleRow {
  core_id: string;
  /** QUEUED（可请求）/ RUNNING（在执行）/ WAITING（等待复查或退避） */
  state: "QUEUED" | "RUNNING" | "WAITING";
  /** WAITING 的原因：RECHECK（详情滞后）/ BACKOFF（请求失败退避） */
  reason: "RECHECK" | "BACKOFF" | null;
  attempts: number;
  /** 下次允许请求的时间（退避/复查截止）。强杀重启后据此继续等待。 */
  next_attempt_at: string | Date;
  last_error: string | null;
  updated_at: string | Date;
}

/** 把 DuckDB 返回的 TIMESTAMP（Date 或字符串）统一转成毫秒。 */
export function timestampMs(value: string | Date | null | undefined): number {
  if (value === null || value === undefined) return 0;
  if (value instanceof Date) return value.getTime();
  const parsed = Date.parse(value);
  return Number.isNaN(parsed) ? 0 : parsed;
}


export interface RunStateRow {
  active_run_id: number | null;
  mode: string | null;
  catalog_page_cursor: number;
  catalog_total_pages: number | null;
  correction_page_cursor: number;
  correction_total_pages: number | null;
  last_contiguous_verified_seq: number;
  full_completed: boolean;
  full_completed_at: string | null;
  last_catchup_at: string | null;
  updated_at: string;
}

export interface RunRow {
  id: number;
  mode: string;
  started_at: string;
  finished_at: string | null;
  status: string;
  catalog_pages_done: number;
  catalog_total_pages: number | null;
  correction_pages_done: number;
  correction_total_pages: number | null;
  details_total: number;
  details_verified: number;
  details_pending: number;
  details_failed: number;
  contiguous_verified_seq: number;
  last_error: string | null;
}

/** 极简异步互斥锁，保证同一连接上的语句顺序执行。 */
class Mutex {
  private tail: Promise<unknown> = Promise.resolve();
  run<T>(fn: () => Promise<T>): Promise<T> {
    const next = this.tail.then(fn, fn);
    this.tail = next.catch(() => undefined);
    return next as Promise<T>;
  }
}

const DDL = `
CREATE TABLE IF NOT EXISTS identifications (
  core_id          VARCHAR   NOT NULL,
  revision         INTEGER   NOT NULL,
  published_seq    INTEGER    NOT NULL,
  published_at     TIMESTAMP NOT NULL,
  detail_ready_seq INTEGER,
  classification   VARCHAR,
  formation        VARCHAR,
  depth_top        DOUBLE,
  depth_bottom     DOUBLE,
  verifier         VARCHAR,
  summary          VARCHAR,
  status           VARCHAR   NOT NULL DEFAULT 'PENDING',
  attempts         INTEGER   NOT NULL DEFAULT 0,
  last_error       VARCHAR,
  updated_at       TIMESTAMP NOT NULL,
  PRIMARY KEY (core_id, revision)
);

CREATE TABLE IF NOT EXISTS fetch_urls (
  url         VARCHAR   NOT NULL PRIMARY KEY,
  kind        VARCHAR   NOT NULL,
  status      VARCHAR   NOT NULL DEFAULT 'DONE',
  attempts    INTEGER   NOT NULL DEFAULT 1,
  last_error  VARCHAR,
  fetched_at  TIMESTAMP NOT NULL
);

CREATE SEQUENCE IF NOT EXISTS runs_id_seq START 1;
CREATE TABLE IF NOT EXISTS runs (
  id                     INTEGER    NOT NULL PRIMARY KEY
                           DEFAULT CAST(nextval('runs_id_seq') AS INTEGER),
  mode                   VARCHAR   NOT NULL,
  started_at             TIMESTAMP NOT NULL,
  finished_at            TIMESTAMP,
  status                 VARCHAR   NOT NULL,
  catalog_pages_done     INTEGER    NOT NULL DEFAULT 0,
  catalog_total_pages    INTEGER,
  correction_pages_done  INTEGER    NOT NULL DEFAULT 0,
  correction_total_pages INTEGER,
  details_total          INTEGER    NOT NULL DEFAULT 0,
  details_verified       INTEGER    NOT NULL DEFAULT 0,
  details_pending        INTEGER    NOT NULL DEFAULT 0,
  details_failed         INTEGER    NOT NULL DEFAULT 0,
  contiguous_verified_seq INTEGER   NOT NULL DEFAULT 0,
  last_error             VARCHAR
);

CREATE TABLE IF NOT EXISTS run_state (
  id                            INTEGER NOT NULL PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  active_run_id                 INTEGER,
  mode                          VARCHAR,
  catalog_page_cursor           INTEGER NOT NULL DEFAULT 0,
  catalog_total_pages           INTEGER,
  correction_page_cursor        INTEGER NOT NULL DEFAULT 0,
  correction_total_pages        INTEGER,
  last_contiguous_verified_seq  INTEGER NOT NULL DEFAULT 0,
  full_completed                BOOLEAN NOT NULL DEFAULT false,
  full_completed_at             TIMESTAMP,
  last_catchup_at               TIMESTAMP,
  updated_at                    TIMESTAMP NOT NULL
);

-- 详情地址调度表：只保存当前内存窗口内的地址（调度器确保不超过 64 条）。
-- 进程被强制结束后，重启据此继续遵守原有的复查与退避时间。
CREATE TABLE IF NOT EXISTS detail_schedule (
  core_id          VARCHAR   NOT NULL PRIMARY KEY,
  state            VARCHAR   NOT NULL DEFAULT 'QUEUED',
  reason           VARCHAR,
  attempts         INTEGER   NOT NULL DEFAULT 0,
  next_attempt_at  TIMESTAMP NOT NULL,
  last_error       VARCHAR,
  updated_at       TIMESTAMP NOT NULL
);

-- 积压时被暂时换出内存窗口的等待中地址：保留原复查/退避时刻，到点再入窗口。
CREATE TABLE IF NOT EXISTS detail_deferred (
  core_id          VARCHAR   NOT NULL PRIMARY KEY,
  reason           VARCHAR,
  attempts         INTEGER   NOT NULL DEFAULT 0,
  next_attempt_at  TIMESTAMP NOT NULL,
  last_error       VARCHAR,
  updated_at       TIMESTAMP NOT NULL
);
`;

export class Database {
  private db: duckdb.Database;
  private mutex = new Mutex();

  private constructor(db: duckdb.Database) {
    this.db = db;
  }

  static async open(filePath: string): Promise<Database> {
    fs.mkdirSync(nodePath.dirname(filePath), { recursive: true });
    const db = await new Promise<duckdb.Database>((resolve, reject) => {
      const inst = new duckdb.Database(filePath, (err: Error | null) =>
        err ? reject(err) : resolve(inst),
      );
    });
    const wrapped = new Database(db);
    for (const stmt of DDL.split(";").map((s) => s.trim()).filter(Boolean)) {
      await wrapped.exec(stmt);
    }
    await wrapped.exec(
      `INSERT INTO run_state (id, updated_at) VALUES (1, now())
       ON CONFLICT (id) DO NOTHING`,
    );
    return wrapped;
  }

  /**
   * duckdb 1.4.x Node 驱动既不能绑定 null/undefined，也不能在零占位符语句上传空数组。
   * 这里把空值就地改写为类型化 NULL 字面量，仅对非空值保留占位符绑定。
   * 调用方传入的都是内部受控值（编号、枚举、时间戳），无注入面。
   */
  private bind(sql: string, params: unknown[]): { sql: string; params: unknown[] } {
    const kept: unknown[] = [];
    let i = 0;
    const rewritten = sql.replace(/\?/g, () => {
      const v = params[i++];
      if (v === null || v === undefined) {
        return "NULL";
      }
      kept.push(v);
      return "?";
    });
    return { sql: rewritten, params: kept };
  }

  private exec(sql: string, params: unknown[] = []): Promise<void> {
    const { sql: q, params: p } = this.bind(sql, params);
    return this.mutex.run(
      () =>
        new Promise<void>((resolve, reject) => {
          const cb = (err: Error | null) => (err ? reject(err) : resolve());
          // duckdb 驱动签名为 all(sql, ...params, callback)：参数需展开传入
          if (p.length === 0) this.db.all(q, cb);
          else (this.db.all as unknown as (...a: unknown[]) => void)(q, ...p, cb);
        }),
    );
  }

  all<T>(sql: string, params: unknown[] = []): Promise<T[]> {
    const { sql: q, params: p } = this.bind(sql, params);
    return this.mutex.run(
      () =>
        new Promise<T[]>((resolve, reject) => {
          const cb = (err: Error | null, rows: unknown[]) =>
            err ? reject(err) : resolve(rows as T[]);
          if (p.length === 0) this.db.all(q, cb);
          else (this.db.all as unknown as (...a: unknown[]) => void)(q, ...p, cb);
        }),
    );
  }

  async one<T>(sql: string, params: unknown[] = []): Promise<T | null> {
    const rows = await this.all<T>(sql, params);
    return rows[0] ?? null;
  }

  // ---------- 鉴定记录 ----------

  /** 登记一条已发布修订；已存在时不覆盖核实状态（同一待核实修订只合并一次）。 */
  async upsertPublishedRevision(input: {
    coreId: string;
    revision: number;
    publishedSeq: number;
    publishedAt: string;
    detailReadySeq: number | null;
  }): Promise<"inserted" | "existing"> {
    const existing = await this.one<IdentificationRow>(
      `SELECT * FROM identifications WHERE core_id = ? AND revision = ?`,
      [input.coreId, input.revision],
    );
    if (existing) {
      // 目录先发现时发布序号未知（0），更正列表公布后回填，不动核实状态。
      if (existing.published_seq <= 0 && input.publishedSeq > 0) {
        await this.exec(
          `UPDATE identifications
              SET published_seq = ?, published_at = ?, updated_at = now()
            WHERE core_id = ? AND revision = ?`,
          [input.publishedSeq, input.publishedAt, input.coreId, input.revision],
        );
      }
      return "existing";
    }
    await this.exec(
      `INSERT INTO identifications
        (core_id, revision, published_seq, published_at, detail_ready_seq, status, attempts, updated_at)
       VALUES (?, ?, ?, ?, ?, 'PENDING', 0, now())`,
      [
        input.coreId,
        input.revision,
        input.publishedSeq,
        input.publishedAt,
        input.detailReadySeq,
      ],
    );
    return "inserted";
  }

  async getIdentification(coreId: string, revision: number): Promise<IdentificationRow | null> {
    return this.one<IdentificationRow>(
      `SELECT * FROM identifications WHERE core_id = ? AND revision = ?`,
      [coreId, revision],
    );
  }

  /**
   * 批量登记已发布修订（更正列表分页整页写入）。
   * 已存在的行不覆盖核实状态；目录先发现（published_seq = 0）时回填发布序号。
   * 同一页内重复键先去重，避免 MERGE 源内重复。
   */
  async upsertPublishedRevisionsBatch(
    inputs: Array<{
      coreId: string;
      revision: number;
      publishedSeq: number;
      publishedAt: string;
      detailReadySeq: number | null;
    }>,
  ): Promise<number> {
    const unique = new Map<string, (typeof inputs)[number]>();
    for (const input of inputs) {
      const key = `${input.coreId}\u0000${input.revision}`;
      if (!unique.has(key)) unique.set(key, input);
    }
    const rows = [...unique.values()];
    if (rows.length === 0) return 0;

    let inserted = 0;
    const CHUNK = 400;
    for (let start = 0; start < rows.length; start += CHUNK) {
      const chunk = rows.slice(start, start + CHUNK);
      const values: string[] = [];
      const params: unknown[] = [];
      for (const row of chunk) {
        values.push("(?, ?, ?, ?)");
        params.push(row.coreId, row.revision, row.publishedSeq, row.publishedAt);
      }
      await this.exec(
        `MERGE INTO identifications AS t
         USING (VALUES ${values.join(", ")}) AS s(core_id, revision, published_seq, published_at)
         ON t.core_id = s.core_id AND t.revision = s.revision
         WHEN MATCHED AND t.published_seq <= 0 AND s.published_seq > 0 THEN
           UPDATE SET published_seq = s.published_seq, published_at = s.published_at,
                      updated_at = now()
         WHEN NOT MATCHED THEN
           INSERT (core_id, revision, published_seq, published_at, detail_ready_seq,
                   status, attempts, updated_at)
           VALUES (s.core_id, s.revision, s.published_seq, s.published_at, NULL, 'PENDING', 0, now())`,
        params,
      );
      inserted += chunk.length;
    }
    return inserted;
  }

  /** 记录一次详情抓取尝试。 */
  async recordAttempt(coreId: string, revision: number, error: string | null): Promise<void> {
    await this.exec(
      `UPDATE identifications
         SET attempts = attempts + 1,
             last_error = ?,
             updated_at = now()
       WHERE core_id = ? AND revision = ?`,
      [error, coreId, revision],
    );
  }

  /** 详情页内容与待核实修订一致：标记为已核实。 */
  async markVerified(
    coreId: string,
    revision: number,
    detail: {
      classification: string;
      formation: string;
      depthTop: number;
      depthBottom: number;
      verifier: string;
      summary: string;
    },
  ): Promise<void> {
    await this.exec(
      `UPDATE identifications
         SET status = 'VERIFIED',
             classification = ?, formation = ?, depth_top = ?, depth_bottom = ?,
             verifier = ?, summary = ?, last_error = NULL, updated_at = now()
       WHERE core_id = ? AND revision = ?`,
      [
        detail.classification,
        detail.formation,
        detail.depthTop,
        detail.depthBottom,
        detail.verifier,
        detail.summary,
        coreId,
        revision,
      ],
    );
  }

  /** 详情页仍旧修订：保持 PENDING 并记录原因，等待后续复查。 */
  async markStillStale(coreId: string, revision: number, shownRevision: number): Promise<void> {
    await this.exec(
      `UPDATE identifications
         SET last_error = ?, updated_at = now()
       WHERE core_id = ? AND revision = ?`,
      [`详情页仍显示旧修订 r${shownRevision}，等待复查`, coreId, revision],
    );
  }

  async listPending(): Promise<IdentificationRow[]> {
    return this.all<IdentificationRow>(
      `SELECT * FROM identifications WHERE status = 'PENDING' ORDER BY published_seq, core_id, revision`,
    );
  }

  /** 某岩芯当前全部待核实修订（同一详情地址共用一次复查）。 */
  async listPendingForCore(coreId: string): Promise<IdentificationRow[]> {
    return this.all<IdentificationRow>(
      `SELECT * FROM identifications
        WHERE core_id = ? AND status = 'PENDING'
        ORDER BY revision`,
      [coreId],
    );
  }

  async countPendingCores(): Promise<number> {
    const row = await this.one<{ n: number }>(
      `SELECT CAST(COUNT(DISTINCT core_id) AS INTEGER) AS n
         FROM identifications WHERE status = 'PENDING'`,
    );
    return Number(row?.n ?? 0);
  }

  // ---------- 详情调度表（固定窗口 + 持久化） ----------
  //
  // detail_schedule 只保存“内存窗口”内的地址（数量由调度器钳制在 64 以内）。
  // 积压的其它地址仍留在 identifications 中，按需要分批进入窗口；
  // 被窗口暂时换出的等待中地址落在 detail_deferred，保留原复查/退避时刻，
  // 因此进程被强杀重启后也不会提前请求。

  async residentSchedules(): Promise<ScheduleRow[]> {
    return this.all<ScheduleRow>(
      `SELECT * FROM detail_schedule
        ORDER BY CASE state WHEN 'RUNNING' THEN 0 WHEN 'QUEUED' THEN 1 ELSE 2 END,
                 next_attempt_at, core_id`,
    );
  }

  async residentScheduleCount(): Promise<number> {
    // 注意：DuckDB 对非空 PRIMARY KEY 的表用 COUNT(*) 会走较慢的计划，
    // COUNT(core_id) 语义相同且快两个数量级（调度器巡检是热路径）。
    const row = await this.one<{ n: number }>(
      `SELECT CAST(COUNT(core_id) AS INTEGER) AS n FROM detail_schedule`,
    );
    return Number(row?.n ?? 0);
  }

  async scheduleStateCounts(): Promise<{
    queued: number;
    running: number;
    waiting: number;
  }> {
    const row = await this.one<{
      queued: number;
      running: number;
      waiting: number;
    }>(
      `SELECT CAST(COUNT(core_id) FILTER (WHERE state = 'QUEUED')  AS INTEGER) AS queued,
              CAST(COUNT(core_id) FILTER (WHERE state = 'RUNNING') AS INTEGER) AS running,
              CAST(COUNT(core_id) FILTER (WHERE state = 'WAITING') AS INTEGER) AS waiting
         FROM detail_schedule`,
    );
    return {
      queued: Number(row?.queued ?? 0),
      running: Number(row?.running ?? 0),
      waiting: Number(row?.waiting ?? 0),
    };
  }

  async deferredSchedules(): Promise<ScheduleRow[]> {
    const rows = await this.all<{
      core_id: string;
      reason: ScheduleRow["reason"];
      attempts: number;
      next_attempt_at: string | Date;
      last_error: string | null;
      updated_at: string | Date;
    }>(`SELECT * FROM detail_deferred ORDER BY next_attempt_at, core_id`);
    return rows.map((r) => ({ ...r, state: "WAITING" as const }));
  }

  async deferredScheduleCount(): Promise<number> {
    const row = await this.one<{ n: number }>(
      `SELECT CAST(COUNT(core_id) AS INTEGER) AS n FROM detail_deferred`,
    );
    return Number(row?.n ?? 0);
  }

  /**
   * 批量认领到点地址：单条 UPDATE ... RETURNING，避免逐条写模型开销。
   * 排序规则：
   *   1) 已安排过复查/退避的地址（attempts > 0）优先——它们代表“已承诺的时刻”，
   *      不能被源源不断的新积压挤掉；
   *   2) 其次按原定等待时刻、地址稳定排序。
   */
  async claimQueuedBatch(limit: number): Promise<ScheduleRow[]> {
    if (limit <= 0) return [];
    return this.all<ScheduleRow>(
      `UPDATE detail_schedule
          SET state = 'RUNNING', reason = NULL, updated_at = now()
        WHERE core_id IN (
          SELECT core_id FROM detail_schedule
           WHERE state = 'QUEUED' AND next_attempt_at <= now()
           ORDER BY (attempts > 0) DESC, next_attempt_at, core_id
           LIMIT ?
        )
      RETURNING *`,
      [limit],
    );
  }

  /** 等待中的地址到点后重新入队。 */
  async requeueDueWaiting(): Promise<number> {
    const rows = await this.all<{ core_id: string }>(
      `UPDATE detail_schedule
          SET state = 'QUEUED', updated_at = now()
        WHERE state = 'WAITING' AND next_attempt_at <= now()
      RETURNING core_id`,
    );
    return rows.length;
  }

  /**
   * 重启恢复：残留的 RUNNING 表示强杀时正在执行的请求，立即重新排队即可
   * （它原本的 next_attempt_at 已到点，重试不违反任何已安排的等待）。
   * WAITING（已安排复查/退避）与 detail_deferred 原样保留，重启后继续等到原时刻。
   */
  async recoverSchedulesAfterRestart(): Promise<{ requeued: number; waiting: number }> {
    const requeued = await this.all<{ core_id: string }>(
      `UPDATE detail_schedule
          SET state = 'QUEUED', reason = NULL, next_attempt_at = now(), updated_at = now()
        WHERE state = 'RUNNING'
      RETURNING core_id`,
    );
    const waiting = await this.one<{ n: number }>(
      `SELECT CAST(COUNT(core_id) AS INTEGER) AS n
         FROM detail_schedule WHERE state = 'WAITING'`,
    );
    return { requeued: requeued.length, waiting: Number(waiting?.n ?? 0) };
  }

  /** 详情仍滞后：持久化复查时刻。 */
  async scheduleRecheck(coreId: string, nextAtMs: number, error: string | null): Promise<void> {
    await this.exec(
      `UPDATE detail_schedule
          SET state = 'WAITING', reason = 'RECHECK', attempts = attempts + 1,
              next_attempt_at = ?, last_error = ?, updated_at = now()
        WHERE core_id = ?`,
      [new Date(nextAtMs).toISOString(), error, coreId],
    );
  }

  /** 请求失败：持久化指数退避时刻。 */
  async scheduleBackoff(coreId: string, nextAtMs: number, error: string | null): Promise<void> {
    await this.exec(
      `UPDATE detail_schedule
          SET state = 'WAITING', reason = 'BACKOFF', attempts = attempts + 1,
              next_attempt_at = ?, last_error = ?, updated_at = now()
        WHERE core_id = ?`,
      [new Date(nextAtMs).toISOString(), error, coreId],
    );
  }

  /** 地址全部核实完成：移出窗口，同时清理延迟表。 */
  async removeSchedule(coreId: string): Promise<void> {
    await this.exec(`DELETE FROM detail_schedule WHERE core_id = ?`, [coreId]);
    await this.exec(`DELETE FROM detail_deferred WHERE core_id = ?`, [coreId]);
  }

  /** 已到点的延迟地址（按原定时刻排序）。 */
  async dueDeferred(limit: number): Promise<ScheduleRow[]> {
    const rows = await this.all<{
      core_id: string;
      reason: ScheduleRow["reason"];
      attempts: number;
      next_attempt_at: string | Date;
      last_error: string | null;
      updated_at: string | Date;
    }>(
      `SELECT * FROM detail_deferred
        WHERE next_attempt_at <= now()
        ORDER BY next_attempt_at, core_id
        LIMIT ?`,
      [limit],
    );
    return rows.map((r) => ({ ...r, state: "WAITING" as const }));
  }

  /** 把一条延迟地址移回窗口（保留尝试次数与原定时刻）。 */
  async admitDeferredRow(row: {
    core_id: string;
    reason: ScheduleRow["reason"];
    attempts: number;
    next_attempt_at: string | Date;
    last_error: string | null;
  }): Promise<void> {
    const nowIso = new Date().toISOString();
    await this.exec(
      `INSERT INTO detail_schedule (core_id, state, reason, attempts, next_attempt_at, last_error, updated_at)
       VALUES (?, 'QUEUED', ?, ?, ?, ?, ?)
       ON CONFLICT (core_id) DO NOTHING`,
      [
        row.core_id,
        row.reason,
        row.attempts,
        new Date(timestampMs(row.next_attempt_at)).toISOString(),
        row.last_error,
        nowIso,
      ],
    );
    await this.exec(`DELETE FROM detail_deferred WHERE core_id = ?`, [row.core_id]);
  }

  /**
   * 从未安排过的待核实地址里取候选：新发布的修订优先（追更用空闲额度核实），
   * 其余按发布序号从新到旧，保证积压再多也不会饿死刚公布的更正。
   */
  async freshScheduleCandidates(
    limit: number,
  ): Promise<Array<{ core_id: string; priority: number }>> {
    return this.all<{ core_id: string; priority: number }>(
      `SELECT i.core_id, CAST(MIN(i.published_seq) AS INTEGER) AS priority
         FROM identifications i
        WHERE i.status = 'PENDING'
          AND NOT EXISTS (SELECT 1 FROM detail_schedule s WHERE s.core_id = i.core_id)
          AND NOT EXISTS (SELECT 1 FROM detail_deferred d WHERE d.core_id = i.core_id)
        GROUP BY i.core_id
        ORDER BY priority DESC, i.core_id
        LIMIT ?`,
      [limit],
    );
  }

  /** 批量写入窗口（单条语句，减少万级积压下的写放大）。 */
  async admitFreshBatch(coreIds: string[]): Promise<void> {
    if (coreIds.length === 0) return;
    const nowIso = new Date().toISOString();
    const values = coreIds.map(() => "(?, 'QUEUED', NULL, 0, ?, NULL, ?)").join(", ");
    const params: unknown[] = [];
    for (const coreId of coreIds) params.push(coreId, nowIso, nowIso);
    await this.exec(
      `INSERT INTO detail_schedule (core_id, state, reason, attempts, next_attempt_at, last_error, updated_at)
       VALUES ${values}
       ON CONFLICT (core_id) DO NOTHING`,
      params,
    );
  }

  /**
   * 一次换出多条“最久之后才需要复查”的等待地址到 detail_deferred，保留原时刻。
   * 用单条语句完成，积压时避免逐条往返。
   */
  async evictFarthestWaitingBatch(limit: number): Promise<number> {
    if (limit <= 0) return 0;
    const victims = await this.all<{
      core_id: string;
      reason: ScheduleRow["reason"];
      attempts: number;
      next_attempt_at: string | Date;
      last_error: string | null;
    }>(
      `SELECT core_id, reason, attempts, next_attempt_at, last_error
         FROM detail_schedule
        WHERE state = 'WAITING'
        ORDER BY epoch_ms(next_attempt_at) DESC, core_id
        LIMIT ?`,
      [limit],
    );
    if (victims.length === 0) return 0;
    const values = victims.map(() => "(?, ?, ?, ?, ?, now())").join(", ");
    const params: unknown[] = [];
    for (const v of victims) {
      params.push(v.core_id, v.reason, v.attempts, new Date(timestampMs(v.next_attempt_at)).toISOString(), v.last_error);
    }
    await this.exec(
      `INSERT INTO detail_deferred (core_id, reason, attempts, next_attempt_at, last_error, updated_at)
       VALUES ${values}
       ON CONFLICT (core_id) DO UPDATE SET
         reason = excluded.reason,
         attempts = excluded.attempts,
         next_attempt_at = excluded.next_attempt_at,
         last_error = excluded.last_error,
         updated_at = now()`,
      params,
    );
    await this.exec(
      `DELETE FROM detail_schedule WHERE core_id IN (${victims.map(() => "?").join(", ")})`,
      victims.map((v) => v.core_id),
    );
    return victims.length;
  }

  /** 清理已不再有待核实修订的调度/延迟行。 */
  async pruneSchedules(): Promise<void> {
    await this.exec(
      `DELETE FROM detail_schedule
        WHERE core_id NOT IN (SELECT DISTINCT core_id FROM identifications WHERE status = 'PENDING')`,
    );
    await this.exec(
      `DELETE FROM detail_deferred
        WHERE core_id NOT IN (SELECT DISTINCT core_id FROM identifications WHERE status = 'PENDING')`,
    );
  }

  /**
   * 把窗口压回上限以内：超出部分按“已安排优先、等待最久优先”保留，其余
   * 等待中的地址换出到 detail_deferred（保留原时刻），排队中的地址退回积压。
   * 只在下限被下调或升级后首次运行时可能用到；正常情况下窗口不会超限。
   */
  async enforceWindow(windowSize: number): Promise<number> {
    const resident = await this.residentScheduleCount();
    if (resident <= windowSize) return 0;
    let excess = resident - windowSize;
    let removed = 0;

    // 1) 先换出等待中的地址（保留其复查/退避时刻）
    const evicted = await this.evictFarthestWaitingBatch(excess);
    removed += evicted;
    excess -= evicted;

    // 2) 仍超限：把最晚入队、且未执行过的排队地址退回积压
    if (excess > 0) {
      const rows = await this.all<{ core_id: string }>(
        `SELECT core_id FROM detail_schedule
          WHERE state = 'QUEUED' AND attempts = 0
          ORDER BY epoch_ms(next_attempt_at) DESC, core_id DESC
          LIMIT ?`,
        [excess],
      );
      if (rows.length > 0) {
        await this.exec(
          `DELETE FROM detail_schedule WHERE core_id IN (${rows.map(() => "?").join(", ")})`,
          rows.map((r) => r.core_id),
        );
        removed += rows.length;
      }
    }
    return removed;
  }

  async getSchedule(coreId: string): Promise<ScheduleRow | null> {
    return this.one<ScheduleRow>(`SELECT * FROM detail_schedule WHERE core_id = ?`, [coreId]);
  }

  async countByStatus(): Promise<{ verified: number; pending: number }> {
    // 显式 CAST 为 INTEGER：duckdb 1.4.x Node 绑定无法序列化 BIGINT
    const row = await this.one<{ verified: number; pending: number }>(
      `SELECT
         CAST(COUNT(*) FILTER (WHERE status = 'VERIFIED') AS INTEGER) AS verified,
         CAST(COUNT(*) FILTER (WHERE status = 'PENDING')  AS INTEGER) AS pending
       FROM identifications`,
    );
    return { verified: Number(row?.verified ?? 0), pending: Number(row?.pending ?? 0) };
  }

  async getLatestVerified(coreId: string): Promise<IdentificationRow | null> {
    return this.one<IdentificationRow>(
      `SELECT * FROM identifications
        WHERE core_id = ? AND status = 'VERIFIED'
        ORDER BY revision DESC LIMIT 1`,
      [coreId],
    );
  }

  async listVerified(limit: number, offset: number): Promise<IdentificationRow[]> {
    return this.all<IdentificationRow>(
      `SELECT i.* FROM identifications i
        WHERE i.revision = (
          SELECT MAX(revision) FROM identifications
           WHERE core_id = i.core_id AND status = 'VERIFIED'
        )
        ORDER BY i.core_id
        LIMIT ? OFFSET ?`,
      [limit, offset],
    );
  }

  async countVerifiedCores(): Promise<number> {
    const row = await this.one<{ n: number }>(
      `SELECT CAST(COUNT(DISTINCT core_id) AS INTEGER) AS n
         FROM identifications WHERE status = 'VERIFIED'`,
    );
    return Number(row?.n ?? 0);
  }

  // ---------- URL 去重（重复链接合并） ----------

  async hasFetchedUrl(url: string): Promise<boolean> {
    const row = await this.one<{ url: string }>(
      `SELECT url FROM fetch_urls WHERE url = ? AND status = 'DONE'`,
      [url],
    );
    return row !== null;
  }

  async recordUrl(url: string, kind: UrlKind, status: UrlStatus, error: string | null): Promise<void> {
    await this.exec(
      `INSERT INTO fetch_urls (url, kind, status, attempts, last_error, fetched_at)
       VALUES (?, ?, ?, 1, ?, now())
       ON CONFLICT (url) DO UPDATE SET
         status = excluded.status,
         attempts = fetch_urls.attempts + 1,
         last_error = excluded.last_error,
         fetched_at = now()`,
      [url, kind, status, error],
    );
  }

  // ---------- 运行与断点状态 ----------

  async getRunState(): Promise<RunStateRow> {
    const row = await this.one<RunStateRow>(`SELECT * FROM run_state WHERE id = 1`);
    if (!row) throw new Error("run_state 未初始化");
    return row;
  }

  async saveRunState(patch: Partial<RunStateRow>): Promise<void> {
    const cur = await this.getRunState();
    const merged: RunStateRow = { ...cur, ...patch, updated_at: new Date().toISOString() };
    await this.exec(
      `UPDATE run_state SET
         active_run_id = ?, mode = ?, catalog_page_cursor = ?, catalog_total_pages = ?,
         correction_page_cursor = ?, correction_total_pages = ?,
         last_contiguous_verified_seq = ?, full_completed = ?, full_completed_at = ?,
         last_catchup_at = ?, updated_at = ?
       WHERE id = 1`,
      [
        merged.active_run_id,
        merged.mode,
        merged.catalog_page_cursor,
        merged.catalog_total_pages,
        merged.correction_page_cursor,
        merged.correction_total_pages,
        merged.last_contiguous_verified_seq,
        merged.full_completed,
        merged.full_completed_at,
        merged.last_catchup_at,
        merged.updated_at,
      ],
    );
  }

  async startRun(mode: string): Promise<number> {
    // 显式取 INTEGER 序列值再插入：跨重启唯一递增，且避开 BIGINT/RETURNING 的驱动缺陷
    const seqRow = await this.one<{ n: number }>(
      `SELECT CAST(nextval('runs_id_seq') AS INTEGER) AS n`,
    );
    const id = Number(seqRow?.n ?? 1);
    await this.exec(
      `INSERT INTO runs (id, mode, started_at, status) VALUES (?, ?, now(), 'RUNNING')`,
      [id, mode],
    );
    await this.saveRunState({ active_run_id: id, mode });
    return id;
  }

  async finishRun(runId: number, status: string, error: string | null): Promise<void> {
    const counts = await this.one<{
      details_total: number;
      details_verified: number;
      details_pending: number;
    }>(
      `SELECT CAST(COUNT(*) AS INTEGER) AS details_total,
              CAST(COUNT(*) FILTER (WHERE status='VERIFIED') AS INTEGER) AS details_verified,
              CAST(COUNT(*) FILTER (WHERE status='PENDING')  AS INTEGER) AS details_pending
         FROM identifications`,
    );
    const state = await this.getRunState();
    await this.exec(
      `UPDATE runs SET
         finished_at = now(), status = ?,
         catalog_pages_done = ?, catalog_total_pages = ?,
         correction_pages_done = ?, correction_total_pages = ?,
         details_total = ?, details_verified = ?, details_pending = ?,
         details_failed = (SELECT CAST(COUNT(*) AS INTEGER) FROM identifications
                            WHERE status='PENDING' AND attempts > 0),
         contiguous_verified_seq = ?, last_error = ?
       WHERE id = ?`,
      [
        status,
        state.catalog_page_cursor,
        state.catalog_total_pages,
        state.correction_page_cursor,
        state.correction_total_pages,
        Number(counts?.details_total ?? 0),
        Number(counts?.details_verified ?? 0),
        Number(counts?.details_pending ?? 0),
        state.last_contiguous_verified_seq,
        error,
        runId,
      ],
    );
    await this.saveRunState({ active_run_id: null, mode: null });
  }

  async listRuns(limit = 20): Promise<RunRow[]> {
    return this.all<RunRow>(`SELECT * FROM runs ORDER BY id DESC LIMIT ?`, [limit]);
  }

  async getRun(runId: number): Promise<RunRow | null> {
    return this.one<RunRow>(`SELECT * FROM runs WHERE id = ?`, [runId]);
  }
}

export function openDatabase(): Promise<Database> {
  return Database.open(config.dbPath);
}
