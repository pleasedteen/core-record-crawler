/**
 * 积压场景主程序（仅用于 `npm run verify:backlog`）。
 *
 * 与 `npm run serve` 不同，这里不内嵌馆藏站点、不做目录抓取：
 *  - 启动时把 11k+ 积压详情地址直接写入 DuckDB（等同“已登记待核实”）；
 *  - 只启动详情调度器 + 本地 HTTP 查询/进度接口；
 *  - 由验证脚本通过 POST /collect/catchup 触发追更、通过模拟站点发布新修订。
 *
 * 这样积压时序完全由调度器决定，便于离线断言“退避让出、追更/空闲额度核实、
 * 窗口 ≤64、强杀重启后仍遵守已安排的复查/退避时间”。
 */
import fs from "node:fs";
import path from "node:path";
import { config } from "../src/config.js";
import { openDatabase } from "../src/db.js";
import { HttpFetcher } from "../src/collect/http.js";
import { DetailScheduler } from "../src/collect/scheduler.js";
import { Collector } from "../src/collect/collector.js";
import { startApiServer } from "../src/api.js";

function readSeed(): Array<{ coreId: string; revision: number; publishedSeq: number }> {
  const seedPath = process.env.BACKLOG_SEED_FILE;
  if (!seedPath) return [];
  return JSON.parse(fs.readFileSync(seedPath, "utf8")) as Array<{
    coreId: string;
    revision: number;
    publishedSeq: number;
  }>;
}

async function main(): Promise<void> {
  fs.mkdirSync(path.dirname(config.dbPath), { recursive: true });
  const db = await openDatabase();

  // 一次性写入积压种子（幂等：已存在则忽略，不覆盖已核实状态）
  const seed = readSeed();
  if (seed.length > 0) {
    const existing = await db.one<{ n: number }>(
      `SELECT CAST(COUNT(*) AS INTEGER) AS n FROM identifications`,
    );
    if (Number(existing?.n ?? 0) === 0) {
      const t0 = Date.now();
      const chunk = 800;
      for (let i = 0; i < seed.length; i += chunk) {
        await db.upsertPublishedRevisionsBatch(
          seed.slice(i, i + chunk).map((r) => ({
            coreId: r.coreId,
            revision: r.revision,
            publishedSeq: r.publishedSeq,
            publishedAt: "2026-10-02T08:00:00.000Z",
            detailReadySeq: null,
          })),
        );
      }
      console.log(`[backlog] 已写入积压种子 ${seed.length} 条（${Date.now() - t0}ms）`);
    }
  }

  const baseUrl = process.env.MOCK_BASE_URL ?? `http://127.0.0.1:${config.mockPort}`;
  const fetcher = new HttpFetcher();
  const scheduler = new DetailScheduler(db, fetcher, baseUrl);
  const collector = new Collector(db, fetcher, baseUrl, scheduler);
  const apiServer = startApiServer(db, collector);

  // AUTO_FULL=0：只启动调度器与追更，不做目录/更正列表全量抓取
  await collector.start(false);

  const shutdown = async (signal: string): Promise<void> => {
    console.log(`\n[backlog] 收到 ${signal}，优雅停止`);
    await collector.shutdown();
    apiServer.close();
    process.exit(0);
  };
  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));
}

main().catch((err) => {
  console.error("[backlog] 启动失败：", err);
  process.exit(1);
});
