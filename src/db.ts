/**
 * DuckDB 嵌入式存储层（多站合并版）。
 *
 * 设计要点：
 *  - 单连接 + 互斥锁串行化所有 SQL，避免嵌入式库的并发写锁问题；
 *  - 两个采集站合并写入同一个 DuckDB 文件，所有业务表带 station_id 列、
 *    主键以 station_id 打头：同号岩芯在两站的鉴定内容物理隔离、互不覆盖；
 *  - 采集状态全部落库且按站独立：任一进程被杀后重启，各站凭各自断点继续；
 *  - 同一 (station_id, core_id, revision) 为主键，天然支撑“同一待核实修订只合并采集一次”；
 *  - 兼容旧库：检测到无 station_id 的单站旧表时自动迁移，旧数据归入站点 "default"，
 *    原有单站库与调用方式完全继续可用。
 */
import duckdb from "duckdb";
import fs from "node:fs";
import nodePath from "node:path";
import { config } from "./config.js";

export type IdentStatus = "PENDING" | "VERIFIED";
export type UrlKind = "CATALOG" | "CORRECTIONS" | "DETAIL";
export type UrlStatus = "DONE" | "FAILED";

export interface IdentificationRow {
  station_id: string;
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
  station_id: string;
  active_run_id: number | null;
  mode: string | null;
  catalog_page_cursor: number;
  catalog_total_pages: number | null;
  correction_page_cursor: number;
  correction_total_pages: number | null;
  correction_total_published: number | null;
  last_contiguous_verified_seq: number;
  full_completed: boolean;
  full_completed_at: string | null;
  last_catchup_at: string | null;
  http_requests: number;
  http_retried: number;
  http_retry_after_waits: number;
  updated_at: string;
}

export interface RunRow {
  id: number;
  station_id: string;
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
  http_requests: number;
  http_retried: number;
  http_retry_after_waits: number;
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

/** 多站新库 DDL（station_id 打头的复合主键保证同号岩芯跨站隔离）。 */
const DDL = `
CREATE TABLE IF NOT EXISTS identifications (
  station_id       VARCHAR   NOT NULL,
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
  PRIMARY KEY (station_id, core_id, revision)
);

CREATE TABLE IF NOT EXISTS fetch_urls (
  station_id  VARCHAR   NOT NULL,
  url         VARCHAR   NOT NULL,
  kind        VARCHAR   NOT NULL,
  status      VARCHAR   NOT NULL DEFAULT 'DONE',
  attempts    INTEGER   NOT NULL DEFAULT 1,
  last_error  VARCHAR,
  fetched_at  TIMESTAMP NOT NULL,
  PRIMARY KEY (station_id, url)
);

CREATE SEQUENCE IF NOT EXISTS runs_id_seq START 1;
CREATE TABLE IF NOT EXISTS runs (
  id                     INTEGER    NOT NULL PRIMARY KEY
                           DEFAULT CAST(nextval('runs_id_seq') AS INTEGER),
  station_id             VARCHAR    NOT NULL,
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
  http_requests          INTEGER    NOT NULL DEFAULT 0,
  http_retried           INTEGER    NOT NULL DEFAULT 0,
  http_retry_after_waits INTEGER    NOT NULL DEFAULT 0,
  last_error             VARCHAR
);

CREATE TABLE IF NOT EXISTS run_state (
  station_id                    VARCHAR   NOT NULL PRIMARY KEY,
  active_run_id                 INTEGER,
  mode                          VARCHAR,
  catalog_page_cursor           INTEGER NOT NULL DEFAULT 0,
  catalog_total_pages           INTEGER,
  correction_page_cursor        INTEGER NOT NULL DEFAULT 0,
  correction_total_pages        INTEGER,
  correction_total_published    INTEGER,
  last_contiguous_verified_seq  INTEGER NOT NULL DEFAULT 0,
  full_completed                BOOLEAN NOT NULL DEFAULT false,
  full_completed_at             TIMESTAMP,
  last_catchup_at               TIMESTAMP,
  http_requests                 INTEGER NOT NULL DEFAULT 0,
  http_retried                  INTEGER NOT NULL DEFAULT 0,
  http_retry_after_waits        INTEGER NOT NULL DEFAULT 0,
  updated_at                    TIMESTAMP NOT NULL
);
`;

/**
 * 旧版单站库迁移 DDL。
 * 旧表没有 station_id：加列回填 'default' 后整表重建为新主键结构，
 * 运行历史 id 与全部核实结果原样保留。
 */
function migrationStatements(): string[] {
  return [
    // identifications：旧主键 (core_id, revision) → 新主键 (station_id, core_id, revision)
    `ALTER TABLE identifications ADD COLUMN station_id VARCHAR`,
    `UPDATE identifications SET station_id = 'default'`,
    `CREATE TABLE identifications_new (
       station_id VARCHAR NOT NULL, core_id VARCHAR NOT NULL, revision INTEGER NOT NULL,
       published_seq INTEGER NOT NULL, published_at TIMESTAMP NOT NULL, detail_ready_seq INTEGER,
       classification VARCHAR, formation VARCHAR, depth_top DOUBLE, depth_bottom DOUBLE,
       verifier VARCHAR, summary VARCHAR, status VARCHAR NOT NULL DEFAULT 'PENDING',
       attempts INTEGER NOT NULL DEFAULT 0, last_error VARCHAR, updated_at TIMESTAMP NOT NULL,
       PRIMARY KEY (station_id, core_id, revision))`,
    `INSERT INTO identifications_new
       SELECT station_id, core_id, revision, published_seq, published_at, detail_ready_seq,
              classification, formation, depth_top, depth_bottom, verifier, summary, status,
              attempts, last_error, updated_at FROM identifications`,
    `DROP TABLE identifications`,
    `ALTER TABLE identifications_new RENAME TO identifications`,
    // fetch_urls：旧主键 url → 新主键 (station_id, url)
    `ALTER TABLE fetch_urls ADD COLUMN station_id VARCHAR`,
    `UPDATE fetch_urls SET station_id = 'default'`,
    `CREATE TABLE fetch_urls_new (
       station_id VARCHAR NOT NULL, url VARCHAR NOT NULL, kind VARCHAR NOT NULL,
       status VARCHAR NOT NULL DEFAULT 'DONE', attempts INTEGER NOT NULL DEFAULT 1,
       last_error VARCHAR, fetched_at TIMESTAMP NOT NULL,
       PRIMARY KEY (station_id, url))`,
    `INSERT INTO fetch_urls_new
       SELECT station_id, url, kind, status, attempts, last_error, fetched_at FROM fetch_urls`,
    `DROP TABLE fetch_urls`,
    `ALTER TABLE fetch_urls_new RENAME TO fetch_urls`,
    // runs：主键 id 不变，仅加站点列
    `ALTER TABLE runs ADD COLUMN station_id VARCHAR`,
    `UPDATE runs SET station_id = 'default'`,
    // run_state：旧表单行 id=1 → 按 station_id 主键重建
    `CREATE TABLE run_state_new (
       station_id VARCHAR NOT NULL PRIMARY KEY, active_run_id INTEGER, mode VARCHAR,
       catalog_page_cursor INTEGER NOT NULL DEFAULT 0, catalog_total_pages INTEGER,
       correction_page_cursor INTEGER NOT NULL DEFAULT 0, correction_total_pages INTEGER,
       last_contiguous_verified_seq INTEGER NOT NULL DEFAULT 0,
       full_completed BOOLEAN NOT NULL DEFAULT false, full_completed_at TIMESTAMP,
       last_catchup_at TIMESTAMP, updated_at TIMESTAMP NOT NULL)`,
    `INSERT INTO run_state_new
       SELECT 'default', active_run_id, mode, catalog_page_cursor, catalog_total_pages,
              correction_page_cursor, correction_total_pages, last_contiguous_verified_seq,
              full_completed, full_completed_at, last_catchup_at, updated_at FROM run_state`,
    `DROP TABLE run_state`,
    `ALTER TABLE run_state_new RENAME TO run_state`,
  ];
}

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

    // 旧版单站库检测：identifications 存在但没有 station_id 列 → 先迁移
    if (await wrapped.isLegacySchema()) {
      console.log("[db] 检测到单站旧版库结构，自动迁移为多站结构（旧数据归入站点 default）");
      for (const stmt of migrationStatements()) await wrapped.exec(stmt);
    }

    for (const stmt of DDL.split(";").map((s) => s.trim()).filter(Boolean)) {
      await wrapped.exec(stmt);
    }
    // 旧库（迁移后）与早期多站库可能缺少新增列：幂等补齐。
    // 注意：该 DuckDB 版本的 ALTER ADD 不支持 NOT NULL 约束，用 DEFAULT 0 的可空列补齐；
    // 全新库仍由上方 DDL 创建为 NOT NULL。
    for (const col of ["http_requests", "http_retried", "http_retry_after_waits"]) {
      await wrapped.exec(`ALTER TABLE runs ADD COLUMN IF NOT EXISTS ${col} INTEGER DEFAULT 0`);
    }
    for (const col of ["correction_total_published", "http_requests", "http_retried", "http_retry_after_waits"]) {
      await wrapped.exec(`ALTER TABLE run_state ADD COLUMN IF NOT EXISTS ${col} INTEGER`);
    }
    // 每个配置站点一行独立断点
    for (const station of config.stations) {
      await wrapped.exec(
        `INSERT INTO run_state (station_id, updated_at) VALUES (?, now())
         ON CONFLICT (station_id) DO NOTHING`,
        [station.id],
      );
    }
    return wrapped;
  }

  private async isLegacySchema(): Promise<boolean> {
    const row = await this.one<{ n: number }>(
      `SELECT CAST(COUNT(*) AS INTEGER) AS n FROM information_schema.columns
        WHERE table_name = 'identifications' AND column_name = 'station_id'`,
    );
    const tableRow = await this.one<{ n: number }>(
      `SELECT CAST(COUNT(*) AS INTEGER) AS n FROM information_schema.tables
        WHERE table_name = 'identifications'`,
    );
    return Number(tableRow?.n ?? 0) > 0 && Number(row?.n ?? 0) === 0;
  }

  async close(): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      this.db.close((err: Error | null) => (err ? reject(err) : resolve()));
    });
  }

  /**
   * duckdb 1.4.x Node 驱动既不能绑定 null/undefined，也不能在零占位符语句上传空数组。
   * 这里把空值就地改写为类型化 NULL 字面量，仅对非空值保留占位符绑定。
   * 调用方传入的都是内部受控值（站点/编号、枚举、时间戳），无注入面。
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

  // ---------- 站点 ----------

  async listStations(): Promise<string[]> {
    const rows = await this.all<{ station_id: string }>(
      `SELECT station_id FROM run_state ORDER BY station_id`,
    );
    return rows.map((r) => r.station_id);
  }

  // ---------- 鉴定记录（按站隔离） ----------

  /** 登记一条已发布修订；已存在时不覆盖核实状态（同一待核实修订只合并一次）。 */
  async upsertPublishedRevision(input: {
    stationId: string;
    coreId: string;
    revision: number;
    publishedSeq: number;
    publishedAt: string;
    detailReadySeq: number | null;
  }): Promise<"inserted" | "existing"> {
    const existing = await this.one<IdentificationRow>(
      `SELECT * FROM identifications WHERE station_id = ? AND core_id = ? AND revision = ?`,
      [input.stationId, input.coreId, input.revision],
    );
    if (existing) {
      // 目录先发现时发布序号未知（0），更正列表公布后回填，不动核实状态。
      if (existing.published_seq <= 0 && input.publishedSeq > 0) {
        await this.exec(
          `UPDATE identifications
              SET published_seq = ?, published_at = ?, updated_at = now()
            WHERE station_id = ? AND core_id = ? AND revision = ?`,
          [input.publishedSeq, input.publishedAt, input.stationId, input.coreId, input.revision],
        );
      }
      return "existing";
    }
    await this.exec(
      `INSERT INTO identifications
        (station_id, core_id, revision, published_seq, published_at, detail_ready_seq, status, attempts, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, 'PENDING', 0, now())`,
      [
        input.stationId,
        input.coreId,
        input.revision,
        input.publishedSeq,
        input.publishedAt,
        input.detailReadySeq,
      ],
    );
    return "inserted";
  }

  async getIdentification(
    stationId: string,
    coreId: string,
    revision: number,
  ): Promise<IdentificationRow | null> {
    return this.one<IdentificationRow>(
      `SELECT * FROM identifications WHERE station_id = ? AND core_id = ? AND revision = ?`,
      [stationId, coreId, revision],
    );
  }

  /** 记录一次详情抓取尝试。 */
  async recordAttempt(
    stationId: string,
    coreId: string,
    revision: number,
    error: string | null,
  ): Promise<void> {
    await this.exec(
      `UPDATE identifications
         SET attempts = attempts + 1,
             last_error = ?,
             updated_at = now()
       WHERE station_id = ? AND core_id = ? AND revision = ?`,
      [error, stationId, coreId, revision],
    );
  }

  /** 详情页内容与待核实修订一致：标记为已核实。 */
  async markVerified(
    stationId: string,
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
       WHERE station_id = ? AND core_id = ? AND revision = ?`,
      [
        detail.classification,
        detail.formation,
        detail.depthTop,
        detail.depthBottom,
        detail.verifier,
        detail.summary,
        stationId,
        coreId,
        revision,
      ],
    );
  }

  /** 详情页仍旧修订：保持 PENDING 并记录原因，等待后续复查。 */
  async markStillStale(
    stationId: string,
    coreId: string,
    revision: number,
    shownRevision: number,
  ): Promise<void> {
    await this.exec(
      `UPDATE identifications
         SET last_error = ?, updated_at = now()
       WHERE station_id = ? AND core_id = ? AND revision = ?`,
      [`详情页仍显示旧修订 r${shownRevision}，等待复查`, stationId, coreId, revision],
    );
  }

  async listPending(stationId: string): Promise<IdentificationRow[]> {
    return this.all<IdentificationRow>(
      `SELECT * FROM identifications WHERE station_id = ? AND status = 'PENDING'
        ORDER BY published_seq, core_id, revision`,
      [stationId],
    );
  }

  async countByStatus(stationId: string): Promise<{ verified: number; pending: number }> {
    // 显式 CAST 为 INTEGER：duckdb 1.4.x Node 绑定无法序列化 BIGINT
    const row = await this.one<{ verified: number; pending: number }>(
      `SELECT
         CAST(COUNT(*) FILTER (WHERE status = 'VERIFIED') AS INTEGER) AS verified,
         CAST(COUNT(*) FILTER (WHERE status = 'PENDING')  AS INTEGER) AS pending
       FROM identifications WHERE station_id = ?`,
      [stationId],
    );
    return { verified: Number(row?.verified ?? 0), pending: Number(row?.pending ?? 0) };
  }

  async getLatestVerified(stationId: string, coreId: string): Promise<IdentificationRow | null> {
    return this.one<IdentificationRow>(
      `SELECT * FROM identifications
        WHERE station_id = ? AND core_id = ? AND status = 'VERIFIED'
        ORDER BY revision DESC LIMIT 1`,
      [stationId, coreId],
    );
  }

  async listVerified(stationId: string, limit: number, offset: number): Promise<IdentificationRow[]> {
    return this.all<IdentificationRow>(
      `SELECT i.* FROM identifications i
        WHERE i.station_id = ? AND i.revision = (
          SELECT MAX(revision) FROM identifications
           WHERE station_id = i.station_id AND core_id = i.core_id AND status = 'VERIFIED'
        )
        ORDER BY i.core_id
        LIMIT ? OFFSET ?`,
      [stationId, limit, offset],
    );
  }

  async countVerifiedCores(stationId: string): Promise<number> {
    const row = await this.one<{ n: number }>(
      `SELECT CAST(COUNT(DISTINCT core_id) AS INTEGER) AS n
         FROM identifications WHERE station_id = ? AND status = 'VERIFIED'`,
      [stationId],
    );
    return Number(row?.n ?? 0);
  }

  /** 跨站读取同一岩芯编号的全部已核实记录（验证同号内容隔离用）。 */
  async listVerifiedByCoreAcrossStations(coreId: string): Promise<IdentificationRow[]> {
    return this.all<IdentificationRow>(
      `SELECT * FROM identifications
        WHERE core_id = ? AND status = 'VERIFIED'
        ORDER BY station_id, revision DESC`,
      [coreId],
    );
  }

  // ---------- URL 去重（重复链接合并，按站隔离） ----------

  async hasFetchedUrl(stationId: string, url: string): Promise<boolean> {
    const row = await this.one<{ url: string }>(
      `SELECT url FROM fetch_urls WHERE station_id = ? AND url = ? AND status = 'DONE'`,
      [stationId, url],
    );
    return row !== null;
  }

  async recordUrl(
    stationId: string,
    url: string,
    kind: UrlKind,
    status: UrlStatus,
    error: string | null,
  ): Promise<void> {
    await this.exec(
      `INSERT INTO fetch_urls (station_id, url, kind, status, attempts, last_error, fetched_at)
       VALUES (?, ?, ?, ?, 1, ?, now())
       ON CONFLICT (station_id, url) DO UPDATE SET
         status = excluded.status,
         attempts = fetch_urls.attempts + 1,
         last_error = excluded.last_error,
         fetched_at = now()`,
      [stationId, url, kind, status, error],
    );
  }

  // ---------- 运行与断点状态（每站独立） ----------

  async getRunState(stationId: string): Promise<RunStateRow> {
    const row = await this.one<RunStateRow>(
      `SELECT * FROM run_state WHERE station_id = ?`,
      [stationId],
    );
    if (!row) throw new Error(`run_state 未初始化: station=${stationId}`);
    return row;
  }

  async saveRunState(stationId: string, patch: Partial<RunStateRow>): Promise<void> {
    const cur = await this.getRunState(stationId);
    const merged: RunStateRow = { ...cur, ...patch, updated_at: new Date().toISOString() };
    await this.exec(
      `UPDATE run_state SET
         active_run_id = ?, mode = ?, catalog_page_cursor = ?, catalog_total_pages = ?,
         correction_page_cursor = ?, correction_total_pages = ?, correction_total_published = ?,
         last_contiguous_verified_seq = ?, full_completed = ?, full_completed_at = ?,
         last_catchup_at = ?, http_requests = ?, http_retried = ?, http_retry_after_waits = ?,
         updated_at = ?
       WHERE station_id = ?`,
      [
        merged.active_run_id,
        merged.mode,
        merged.catalog_page_cursor,
        merged.catalog_total_pages,
        merged.correction_page_cursor,
        merged.correction_total_pages,
        merged.correction_total_published,
        merged.last_contiguous_verified_seq,
        merged.full_completed,
        merged.full_completed_at,
        merged.last_catchup_at,
        merged.http_requests,
        merged.http_retried,
        merged.http_retry_after_waits,
        merged.updated_at,
        stationId,
      ],
    );
  }

  async startRun(stationId: string, mode: string): Promise<number> {
    // 显式取 INTEGER 序列值再插入：跨重启唯一递增，且避开 BIGINT/RETURNING 的驱动缺陷
    const seqRow = await this.one<{ n: number }>(
      `SELECT CAST(nextval('runs_id_seq') AS INTEGER) AS n`,
    );
    const id = Number(seqRow?.n ?? 1);
    await this.exec(
      `INSERT INTO runs (id, station_id, mode, started_at, status)
       VALUES (?, ?, ?, now(), 'RUNNING')`,
      [id, stationId, mode],
    );
    await this.saveRunState(stationId, { active_run_id: id, mode });
    return id;
  }

  /** 重启恢复：把该站残留的 RUNNING 运行标记为 INTERRUPTED（各站独立处理）。 */
  async markInterruptedRuns(stationId: string): Promise<void> {
    await this.exec(
      `UPDATE runs SET status = 'INTERRUPTED', finished_at = now(),
                       last_error = COALESCE(last_error, '进程被强制结束，重启后续跑')
        WHERE station_id = ? AND status = 'RUNNING'`,
      [stationId],
    );
  }

  async finishRun(
    stationId: string,
    runId: number,
    status: string,
    error: string | null,
    httpStats?: { requests: number; retried: number; retryAfterWaits: number },
  ): Promise<void> {
    const counts = await this.one<{
      details_total: number;
      details_verified: number;
      details_pending: number;
    }>(
      `SELECT CAST(COUNT(*) AS INTEGER) AS details_total,
              CAST(COUNT(*) FILTER (WHERE status='VERIFIED') AS INTEGER) AS details_verified,
              CAST(COUNT(*) FILTER (WHERE status='PENDING')  AS INTEGER) AS details_pending
         FROM identifications WHERE station_id = ?`,
      [stationId],
    );
    const state = await this.getRunState(stationId);
    await this.exec(
      `UPDATE runs SET
         finished_at = now(), status = ?,
         catalog_pages_done = ?, catalog_total_pages = ?,
         correction_pages_done = ?, correction_total_pages = ?,
         details_total = ?, details_verified = ?, details_pending = ?,
         details_failed = (SELECT CAST(COUNT(*) AS INTEGER) FROM identifications
                            WHERE station_id = ? AND status='PENDING' AND attempts > 0),
         contiguous_verified_seq = ?,
         http_requests = ?, http_retried = ?, http_retry_after_waits = ?,
         last_error = ?
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
        stationId,
        state.last_contiguous_verified_seq,
        httpStats?.requests ?? 0,
        httpStats?.retried ?? 0,
        httpStats?.retryAfterWaits ?? 0,
        error,
        runId,
      ],
    );
    await this.saveRunState(stationId, { active_run_id: null, mode: null });
  }

  async listRuns(stationId: string | null, limit = 20): Promise<RunRow[]> {
    if (stationId === null) {
      return this.all<RunRow>(`SELECT * FROM runs ORDER BY id DESC LIMIT ?`, [limit]);
    }
    return this.all<RunRow>(
      `SELECT * FROM runs WHERE station_id = ? ORDER BY id DESC LIMIT ?`,
      [stationId, limit],
    );
  }

  async getRun(stationId: string | null, runId: number): Promise<RunRow | null> {
    if (stationId === null) {
      return this.one<RunRow>(`SELECT * FROM runs WHERE id = ?`, [runId]);
    }
    return this.one<RunRow>(`SELECT * FROM runs WHERE station_id = ? AND id = ?`, [stationId, runId]);
  }
}

export function openDatabase(): Promise<Database> {
  return Database.open(config.dbPath);
}
