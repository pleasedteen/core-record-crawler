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

export interface RunStateRow {
  active_run_id: number | null;
  mode: string | null;
  catalog_page_cursor: number;
  catalog_total_pages: number | null;
  correction_page_cursor: number;
  correction_total_pages: number | null;
  /** 全量锁定的“启动时已发布”最大序号；快照后落库，强杀重启后沿用，不随站点后续公布扩大 */
  full_target_seq: number | null;
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
  full_target_seq               INTEGER,
  last_contiguous_verified_seq  INTEGER NOT NULL DEFAULT 0,
  full_completed                BOOLEAN NOT NULL DEFAULT false,
  full_completed_at             TIMESTAMP,
  last_catchup_at               TIMESTAMP,
  updated_at                    TIMESTAMP NOT NULL
);

-- 兼容旧库：补充全量目标序号快照列
ALTER TABLE run_state ADD COLUMN IF NOT EXISTS full_target_seq INTEGER;
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

  /** 已登记修订的最大发布序号（全量判断“启动范围是否已覆盖”用）。 */
  async maxRegisteredSeq(): Promise<number> {
    const row = await this.one<{ n: number }>(
      `SELECT CAST(COALESCE(MAX(published_seq), 0) AS INTEGER) AS n FROM identifications`,
    );
    return Number(row?.n ?? 0);
  }

  /** 已核实但发布序号在连续核实进度之外的修订数（连续进度等前面缺口补齐）。 */
  async countVerifiedBeyondContiguous(contiguousSeq: number): Promise<number> {
    const row = await this.one<{ n: number }>(
      `SELECT CAST(COUNT(*) AS INTEGER) AS n FROM identifications
        WHERE status = 'VERIFIED' AND published_seq > ?`,
      [contiguousSeq],
    );
    return Number(row?.n ?? 0);
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
         correction_page_cursor = ?, correction_total_pages = ?, full_target_seq = ?,
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
        merged.full_target_seq,
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
