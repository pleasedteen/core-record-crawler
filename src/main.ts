/**
 * 服务入口：
 *  - 默认在进程内同时启动离线模拟站点（EMBED_MOCK=1）与采集/查询服务，便于离线运行；
 *  - EMBED_MOCK=0 时只启动采集服务，目标站点由 MOCK_BASE_URL 指定。
 *
 * 启动后自动执行全量采集；全量完成后按 CATCHUP_INTERVAL_MS 周期增量追更正。
 * 进程被强制结束后重启：从 DuckDB 中读取断点继续，已核实结果原样沿用。
 */
import fs from "node:fs";
import path from "node:path";
import { config } from "./config.js";
import { openDatabase } from "./db.js";
import { HttpFetcher } from "./collect/http.js";
import { DetailScheduler } from "./collect/scheduler.js";
import { Collector } from "./collect/collector.js";
import { startApiServer } from "./api.js";
import { MockArchiveSite } from "./mock/site.js";

function embedMockEnabled(): boolean {
  return (process.env.EMBED_MOCK ?? "1") !== "0";
}

async function main(): Promise<void> {
  fs.mkdirSync(path.dirname(config.dbPath), { recursive: true });

  let mockServer: ReturnType<MockArchiveSite["createServer"]> | null = null;
  let baseUrl = config.mockBaseUrl;

  if (embedMockEnabled()) {
    const site = new MockArchiveSite();
    mockServer = site.createServer();
    await new Promise<void>((resolve) => mockServer!.listen(config.mockPort, "127.0.0.1", resolve));
    console.log(
      `[mock] 内嵌离线模拟站点已启动: http://127.0.0.1:${config.mockPort} ` +
        `（已发布修订 ${site.totalPublished} 条，详情生效时钟初始 ${site.detailClock}）`,
    );
    // 默认指向内嵌站点；显式设置 MOCK_BASE_URL 时以外部站点为准
    if (!process.env.MOCK_BASE_URL) baseUrl = `http://127.0.0.1:${config.mockPort}`;
  }

  const db = await openDatabase();
  const fetcher = new HttpFetcher();
  const scheduler = new DetailScheduler(db, fetcher, baseUrl);
  const collector = new Collector(db, fetcher, baseUrl, scheduler);
  const apiServer = startApiServer(db, collector);

  await collector.start(config.autoFull);

  const shutdown = async (signal: string): Promise<void> => {
    console.log(`\n[main] 收到 ${signal}，优雅停止中（强制 kill 后重启亦可续跑）`);
    await collector.shutdown();
    apiServer.close();
    mockServer?.close();
    process.exit(0);
  };
  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));
}

main().catch((err) => {
  console.error("[main] 启动失败：", err);
  process.exit(1);
});
