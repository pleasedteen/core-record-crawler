/**
 * DuckDB 嵌入式存储层。
 *
 * 设计要点：
 *  - 单连接 + 互斥锁串行化所有 SQL，避免嵌入式库的并发写锁问题；
 *  - 采集状态全部落库：进程被杀后重启可凭库内状态继续；
 *  - 同一 (core_id, revision, issue) 为主键：撤回后允许沿用原修订号重发，
 *    issue（发布批次）区分原始发布、撤回与同号重发，三者重启后仍可辨别；
 *  - announcements 表是更正列表的入库台账：PUBLISH 与 WITHDRAW 各占一个发布序号；
 *  - 核实结果、撤回状态与待核实状态分字段管理，核实/撤回后立即可查。
 */
import duckdb from "duckdb";
import fs from "node:fs";
import nodePath from "node:path";
import { config } from "./config.js";

export type IdentStatus = "PENDING" | "VERIFIED" | "WITHDRAWN";
export type UrlKind = "CATALOG" | "CORRECTIONS" | "DETAIL";
export type UrlStatus = "DONE" | "FAILED";
export type AnnouncementKind = "PUBLISH" | "WITHDRAW";

export interface IdentificationRow {
  core_id: string;
  revision: number;
  /** 发布批次：1=原始发布，>1=撤回后沿用原修订号重发。 */
  issue: number;
  published_seq: number;
  published_at: string;
  detail_ready_seq: number | null;
  /** 撤回该发布的公告序号；null 表示未被撤回。 */
  withdraw_seq: number | null;
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
  details_withdrawn: number;
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
  issue            INTEGER   NOT NULL DEFAULT 1,
  published_seq    INTEGER   NOT NULL,
  published_at     TIMESTAMP NOT NULL,
  detail_ready_seq INTEGER,
  withdraw_seq     INTEGER,
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
  PRIMARY KEY (core_id, revision, issue)
);

CREATE TABLE IF NOT EXISTS announcements (
  published_seq    INTEGER   NOT NULL PRIMARY KEY,
  kind             VARCHAR   NOT NULL,
  core_id          VARCHAR   NOT NULL,
  revision         INTEGER   NOT NULL,
  issue            INTEGER,
  withdrawn_issue  INTEGER,
  published_at     TIMESTAMP NOT NULL,
  note             VARCHAR,
  created_at       TIMESTAMP NOT NULL
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
  details_withdrawn      INTEGER    NOT NULL DEFAULT 0,
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
  correction_total_pages       INTEGER,
  last_contiguous_verified_seq  INTEGER NOT NULL DEFAULT 0,
  full_completed                BOOLEAN NOT NULL DEFAULT false,
  full_completed_at             TIMESTAMP,
  last_catchup_at               TIMESTAMP,
  updated_at                    TIMESTAMP NOT NULL
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
    await wrapped.migrate();
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
   * 旧库迁移：早期版本 identifications 以 (core_id, revision) 为主键且无 issue/withdraw_seq。
   * 重建为新主键 (core_id, revision, issue)，历史数据全部归为 issue=1（原始发布），
   * 并补齐 announcements 台账，保证“沿用旧库重启后仍能区分各状态”。
   */
  private async migrate(): Promise<void> {
    const exists = await this.one<{ n: number }>(
      `SELECT CAST(COUNT(*) AS INTEGER) AS n FROM information_schema.tables
        WHERE table_name = 'identifications'`,
    );
    if (!exists || Number(exists.n) === 0) return;
    const cols = await this.all<{ column_name: string }>(
      `SELECT column_name FROM information_schema.columns
        WHERE table_name = 'identifications'`,
    );
    const names = new Set(cols.map((c) => String(c.column_name).toLowerCase()));
    if (names.has("issue") && names.has("withdraw_seq")) return;

    await this.exec(`ALTER TABLE identifications RENAME TO identifications_legacy`);
    await this.exec(
      `CREATE TABLE identifications (
         core_id          VARCHAR   NOT NULL,
         revision         INTEGER   NOT NULL,
         issue            INTEGER   NOT NULL DEFAULT 1,
         published_seq    INTEGER   NOT NULL,
         published_at     TIMESTAMP NOT NULL,
         detail_ready_seq INTEGER,
         withdraw_seq     INTEGER,
         classification   VARCHAR,
         formation        VARCHAR,
         depth_top        DOUBLE,
         depth_bottom     DOUBLE,
         verifier         VARCHAR,
         summary          VARCHAR,
         status           VARCHAR   NOT NULL DEFAULT 'PENDING',
         attempts         INTEGER    NOT NULL DEFAULT 0,
         last_error       VARCHAR,
         updated_at       TIMESTAMP NOT NULL,
         PRIMARY KEY (core_id, revision, issue)
       )`,
    );
    await this.exec(
      `INSERT INTO identifications
         (core_id, revision, issue, published_seq, published_at, detail_ready_seq,
          withdraw_seq, classification, formation, depth_top, depth_bottom, verifier,
          summary, status, attempts, last_error, updated_at)
       SELECT core_id, revision, 1, published_seq, published_at, detail_ready_seq,
              NULL, classification, formation, depth_top, depth_bottom, verifier,
              summary, status, attempts, last_error, updated_at
         FROM identifications_legacy`,
    );
    await this.exec(`DROP TABLE identifications_legacy`);
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

  // ---------- 更正公告台账 ----------

  /** 登记一条更正列表事件（PUBLISH / WITHDRAW 都占发布序号），重扫幂等。 */
  async recordAnnouncement(input: {
    kind: AnnouncementKind;
    publishedSeq: number;
    coreId: string;
    revision: number;
    issue: number | null;
    withdrawnIssue: number | null;
    publishedAt: string;
    note: string;
  }): Promise<void> {
    await this.exec(
      `INSERT INTO announcements
         (published_seq, kind, core_id, revision, issue, withdrawn_issue, published_at, note, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, now())
       ON CONFLICT (published_seq) DO NOTHING`,
      [
        input.publishedSeq,
        input.kind,
        input.coreId,
        input.revision,
        input.issue,
        input.withdrawnIssue,
        input.publishedAt,
        input.note,
      ],
    );
  }

  async countAnnouncements(): Promise<{ total: number; publishes: number; withdraws: number }> {
    const row = await this.one<{ total: number; publishes: number; withdraws: number }>(
      `SELECT CAST(COUNT(*) AS INTEGER) AS total,
              CAST(COUNT(*) FILTER (WHERE kind = 'PUBLISH') AS INTEGER) AS publishes,
              CAST(COUNT(*) FILTER (WHERE kind = 'WITHDRAW') AS INTEGER) AS withdraws
         FROM announcements`,
    );
    return {
      total: Number(row?.total ?? 0),
      publishes: Number(row?.publishes ?? 0),
      withdraws: Number(row?.withdraws ?? 0),
    };
  }

  /**
   * 每个发布序号的处置情况（公告 LEFT JOIN 对应鉴定行）。
   * WITHDRAW 公告本身即代表该序号已处置；PUBLISH 看其鉴定行状态。
   */
  async listEventDispositions(): Promise<
    { published_seq: number; kind: string; status: string }[]
  > {
    return this.all(
      `SELECT a.published_seq, a.kind, COALESCE(i.status, '') AS status
         FROM announcements a
         LEFT JOIN identifications i
           ON i.core_id = a.core_id AND i.revision = a.revision
          AND i.issue = COALESCE(a.issue, a.withdrawn_issue)
        ORDER BY a.published_seq`,
    );
  }

  // ---------- 鉴定记录 ----------

  /**
   * 登记一条已发布修订（含同号重发，以 issue 区分批次）；
   * 已存在时不覆盖核实/撤回状态（同一待核实修订只合并一次）。
   */
  async upsertPublishedRevision(input: {
    coreId: string;
    revision: number;
    issue: number;
    publishedSeq: number;
    publishedAt: string;
    detailReadySeq: number | null;
  }): Promise<"inserted" | "existing"> {
    const existing = await this.getIdentification(input.coreId, input.revision, input.issue);
    if (existing) {
      // 目录先发现时发布序号未知（0），更正列表公布后回填，不动核实/撤回状态。
      if (existing.published_seq <= 0 && input.publishedSeq > 0) {
        await this.exec(
          `UPDATE identifications
              SET published_seq = ?, published_at = ?, updated_at = now()
            WHERE core_id = ? AND revision = ? AND issue = ?`,
          [
            input.publishedSeq,
            input.publishedAt,
            input.coreId,
            input.revision,
            input.issue,
          ],
        );
      }
      return "existing";
    }
    await this.exec(
      `INSERT INTO identifications
        (core_id, revision, issue, published_seq, published_at, detail_ready_seq,
         withdraw_seq, status, attempts, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, NULL, 'PENDING', 0, now())`,
      [
        input.coreId,
        input.revision,
        input.issue,
        input.publishedSeq,
        input.publishedAt,
        input.detailReadySeq,
      ],
    );
    return "inserted";
  }

  /** 撤回公告入库：把对应批次标记为 WITHDRAWN（已核实与否都可撤回），并记录撤回序号。 */
  async markWithdrawn(input: {
    coreId: string;
    revision: number;
    issue: number;
    withdrawSeq: number;
  }): Promise<"withdrawn" | "missing"> {
    const existing = await this.getIdentification(input.coreId, input.revision, input.issue);
    if (!existing) return "missing";
    await this.exec(
      `UPDATE identifications
         SET status = 'WITHDRAWN', withdraw_seq = ?, last_error = NULL, updated_at = now()
       WHERE core_id = ? AND revision = ? AND issue = ?`,
      [input.withdrawSeq, input.coreId, input.revision, input.issue],
    );
    return "withdrawn";
  }

  async getIdentification(
    coreId: string,
    revision: number,
    issue: number,
  ): Promise<IdentificationRow | null> {
    return this.one<IdentificationRow>(
      `SELECT * FROM identifications WHERE core_id = ? AND revision = ? AND issue = ?`,
      [coreId, revision, issue],
    );
  }

  /** 记录一次详情抓取尝试。 */
  async recordAttempt(coreId: string, revision: number, issue: number, error: string | null): Promise<void> {
    await this.exec(
      `UPDATE identifications
         SET attempts = attempts + 1,
             last_error = ?,
             updated_at = now()
       WHERE core_id = ? AND revision = ? AND issue = ?`,
      [error, coreId, revision, issue],
    );
  }

  /** 详情页内容与待核实修订一致：标记为已核实。撤回状态不可被核实覆盖。 */
  async markVerified(
    coreId: string,
    revision: number,
    issue: number,
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
       WHERE core_id = ? AND revision = ? AND issue = ? AND status <> 'WITHDRAWN'`,
      [
        detail.classification,
        detail.formation,
        detail.depthTop,
        detail.depthBottom,
        detail.verifier,
        detail.summary,
        coreId,
        revision,
        issue,
      ],
    );
  }

  /** 详情页仍旧修订：保持 PENDING 并记录原因，等待后续复查。 */
  async markStillStale(
    coreId: string,
    revision: number,
    issue: number,
    shownRevision: number,
    shownIssue: number,
  ): Promise<void> {
    await this.exec(
      `UPDATE identifications
         SET last_error = ?, updated_at = now()
       WHERE core_id = ? AND revision = ? AND issue = ? AND status = 'PENDING'`,
      [
        `详情页仍显示旧修订 r${shownRevision}#${shownIssue}，等待复查`,
        coreId,
        revision,
        issue,
      ],
    );
  }

  async listPending(): Promise<IdentificationRow[]> {
    return this.all<IdentificationRow>(
      `SELECT * FROM identifications WHERE status = 'PENDING'
        ORDER BY published_seq, core_id, revision, issue`,
    );
  }

  async countByStatus(): Promise<{ verified: number; pending: number; withdrawn: number }> {
    // 显式 CAST 为 INTEGER：duckdb 1.4.x Node 绑定无法序列化 BIGINT
    const row = await this.one<{ verified: number; pending: number; withdrawn: number }>(
      `SELECT
         CAST(COUNT(*) FILTER (WHERE status = 'VERIFIED')  AS INTEGER) AS verified,
         CAST(COUNT(*) FILTER (WHERE status = 'PENDING')   AS INTEGER) AS pending,
         CAST(COUNT(*) FILTER (WHERE status = 'WITHDRAWN') AS INTEGER) AS withdrawn
       FROM identifications`,
    );
    return {
      verified: Number(row?.verified ?? 0),
      pending: Number(row?.pending ?? 0),
      withdrawn: Number(row?.withdrawn ?? 0),
    };
  }

  /**
   * 最新“仍有效”的已核实修订：撤回行不是 VERIFIED，自然回退到上一仍有效版本；
   * 同修订号重发核实后按 issue 取最新批次。
   */
  async getLatestVerified(coreId: string): Promise<IdentificationRow | null> {
    return this.one<IdentificationRow>(
      `SELECT * FROM identifications
        WHERE core_id = ? AND status = 'VERIFIED'
        ORDER BY revision DESC, issue DESC LIMIT 1`,
      [coreId],
    );
  }

  async listVerified(limit: number, offset: number): Promise<IdentificationRow[]> {
    return this.all<IdentificationRow>(
      `WITH ranked AS (
         SELECT i.*, ROW_NUMBER() OVER (
           PARTITION BY core_id ORDER BY revision DESC, issue DESC
         ) AS rn
         FROM identifications i WHERE status = 'VERIFIED'
       )
       SELECT * FROM ranked WHERE rn = 1
        ORDER BY core_id
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
      details_withdrawn: number;
    }>(
      `SELECT CAST(COUNT(*) AS INTEGER) AS details_total,
              CAST(COUNT(*) FILTER (WHERE status='VERIFIED')  AS INTEGER) AS details_verified,
              CAST(COUNT(*) FILTER (WHERE status='PENDING')   AS INTEGER) AS details_pending,
              CAST(COUNT(*) FILTER (WHERE status='WITHDRAWN') AS INTEGER) AS details_withdrawn
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
         details_withdrawn = ?,
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
        Number(counts?.details_withdrawn ?? 0),
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
