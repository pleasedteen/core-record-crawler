/**
 * 构造一个“旧版单站库”（无 station_id 列的表结构 + 若干已核实数据），
 * 用于验证新服务启动时自动迁移、且原有单站库与调用方式继续可用。
 *
 * 仅被离线验证脚本调用：tsx scripts/lib/make-legacy-db.ts <dbPath>
 */
import duckdb from "duckdb";
import fs from "node:fs";
import path from "node:path";

const dbPath = process.argv[2];
if (!dbPath) {
  console.error("用法: make-legacy-db.ts <dbPath>");
  process.exit(2);
}
fs.mkdirSync(path.dirname(dbPath), { recursive: true });
fs.rmSync(dbPath, { force: true });
fs.rmSync(`${dbPath}.wal`, { force: true });

const db = new duckdb.Database(dbPath);
const exec = (sql: string): Promise<void> =>
  new Promise((resolve, reject) => db.all(sql, (err: Error | null) => (err ? reject(err) : resolve())));

await exec(`
CREATE TABLE identifications (
  core_id VARCHAR NOT NULL, revision INTEGER NOT NULL, published_seq INTEGER NOT NULL,
  published_at TIMESTAMP NOT NULL, detail_ready_seq INTEGER,
  classification VARCHAR, formation VARCHAR, depth_top DOUBLE, depth_bottom DOUBLE,
  verifier VARCHAR, summary VARCHAR, status VARCHAR NOT NULL DEFAULT 'PENDING',
  attempts INTEGER NOT NULL DEFAULT 0, last_error VARCHAR, updated_at TIMESTAMP NOT NULL,
  PRIMARY KEY (core_id, revision)
)`);
await exec(`
CREATE TABLE fetch_urls (
  url VARCHAR NOT NULL PRIMARY KEY, kind VARCHAR NOT NULL, status VARCHAR NOT NULL DEFAULT 'DONE',
  attempts INTEGER NOT NULL DEFAULT 1, last_error VARCHAR, fetched_at TIMESTAMP NOT NULL
)`);
await exec(`CREATE SEQUENCE runs_id_seq START 1`);
await exec(`
CREATE TABLE runs (
  id INTEGER NOT NULL PRIMARY KEY DEFAULT CAST(nextval('runs_id_seq') AS INTEGER),
  mode VARCHAR NOT NULL, started_at TIMESTAMP NOT NULL, finished_at TIMESTAMP, status VARCHAR NOT NULL,
  catalog_pages_done INTEGER NOT NULL DEFAULT 0, catalog_total_pages INTEGER,
  correction_pages_done INTEGER NOT NULL DEFAULT 0, correction_total_pages INTEGER,
  details_total INTEGER NOT NULL DEFAULT 0, details_verified INTEGER NOT NULL DEFAULT 0,
  details_pending INTEGER NOT NULL DEFAULT 0, details_failed INTEGER NOT NULL DEFAULT 0,
  contiguous_verified_seq INTEGER NOT NULL DEFAULT 0, last_error VARCHAR
)`);
await exec(`
CREATE TABLE run_state (
  id INTEGER NOT NULL PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  active_run_id INTEGER, mode VARCHAR, catalog_page_cursor INTEGER NOT NULL DEFAULT 0,
  catalog_total_pages INTEGER, correction_page_cursor INTEGER NOT NULL DEFAULT 0,
  correction_total_pages INTEGER, last_contiguous_verified_seq INTEGER NOT NULL DEFAULT 0,
  full_completed BOOLEAN NOT NULL DEFAULT false, full_completed_at TIMESTAMP,
  last_catchup_at TIMESTAMP, updated_at TIMESTAMP NOT NULL
)`);
// 旧库内容：一条已核实鉴定 + 一条运行记录 + 已完成全量的断点
await exec(`
INSERT INTO identifications
  (core_id, revision, published_seq, published_at, classification, formation,
   depth_top, depth_bottom, verifier, summary, status, attempts, updated_at)
VALUES
  ('RC-0001', 1, 1, '2026-10-01 08:00:00', '旧库砂岩', '青山组', 10, 100, '张核实',
   '旧库迁移验证数据', 'VERIFIED', 1, now()),
  ('RC-0002', 1, 2, '2026-10-02 08:00:00', '旧库泥岩', '白河组', 20, 200, '李核实',
   '旧库迁移验证数据二', 'PENDING', 0, now())`);
await exec(`INSERT INTO fetch_urls (url, kind, status, attempts, fetched_at)
            VALUES ('/core/RC-0001', 'DETAIL', 'DONE', 1, now())`);
await exec(`INSERT INTO runs (mode, started_at, finished_at, status, details_total, details_verified,
                               details_pending, contiguous_verified_seq)
            VALUES ('FULL', now(), now(), 'COMPLETED', 2, 1, 1, 1)`);
await exec(`INSERT INTO run_state (id, full_completed, full_completed_at, last_contiguous_verified_seq, updated_at)
            VALUES (1, true, now(), 1, now())`);

await new Promise<void>((resolve, reject) => db.close((err: Error | null) => (err ? reject(err) : resolve())));
console.log(`[legacy-db] 旧版单站库已生成: ${dbPath}`);
