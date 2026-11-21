/**
 * 服务入口（多站合并版）：
 *  - 单站模式（默认）：进程内嵌一个离线模拟站点 + 采集/查询服务，沿用全部旧行为；
 *  - 两站模式：STATIONS="id1=url1,id2=url2" 指定两个采集目标，合并写入同一个 DuckDB；
 *    EMBED_MOCK=1 时可用 MOCK_STATIONS="id1:seed1,id2:seed2" 在同进程内嵌两个模拟站点，
 *    端口依次取 MOCK_PORT、MOCK_PORT+1。
 *
 * 两站各自一个 Collector（独立断点/限速/Retry-After 等待），
 * 共用一个 SharedSlotScheduler（共享在途上限、round-robin 公平分配）与一个 DuckDB 连接。
 * 进程被强制结束后重启：各站从 DuckDB 中读取各自断点继续，已核实结果原样沿用。
 */
import fs from "node:fs";
import path from "node:path";
import { config, StationConfig } from "./config.js";
import { openDatabase } from "./db.js";
import { SharedSlotScheduler, StationFetcher } from "./collect/http.js";
import { Collector } from "./collect/collector.js";
import { startApiServer } from "./api.js";
import { MockArchiveSite } from "./mock/site.js";

function embedMockEnabled(): boolean {
  return (process.env.EMBED_MOCK ?? "1") !== "0";
}

/**
 * 解析内嵌模拟站点种子：MOCK_STATIONS="id1:seed1,id2:seed2"。
 * 未配置时：单站给经典种子；多站按序号给不同种子，保证同号岩芯内容不同。
 */
function resolveMockSeeds(stations: StationConfig[]): Map<string, number> {
  const seeds = new Map<string, number>();
  const raw = process.env.MOCK_STATIONS?.trim();
  if (raw) {
    for (const part of raw.split(",")) {
      const item = part.trim();
      if (!item) continue;
      const [id, seedRaw] = item.split(":");
      const seed = Number(seedRaw);
      if (!id || !Number.isFinite(seed)) {
        throw new Error(`MOCK_STATIONS 条目格式应为 id:seed，收到: ${item}`);
      }
      seeds.set(id.trim(), seed);
    }
  }
  stations.forEach((station, idx) => {
    if (!seeds.has(station.id)) {
      seeds.set(station.id, stations.length === 1 ? 20261118 : 20261118 + (idx + 1) * 7919);
    }
  });
  return seeds;
}

async function main(): Promise<void> {
  fs.mkdirSync(path.dirname(config.dbPath), { recursive: true });

  const mockServers: ReturnType<MockArchiveSite["createServer"]>[] = [];

  if (embedMockEnabled()) {
    const seeds = resolveMockSeeds(config.stations);
    // 确定性测试可关闭随机故障注入（MOCK_RANDOM_FAULTS=0），改用 /__admin/force-503 精确控制
    const randomFaults = (process.env.MOCK_RANDOM_FAULTS ?? "1") !== "0";
    for (const [idx, station] of config.stations.entries()) {
      const seed = seeds.get(station.id) ?? 20261118;
      const site = new MockArchiveSite({ seed, randomFaults });
      const server = site.createServer();
      const port = config.mockPort + idx;
      await new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(port, "127.0.0.1", resolve);
      });
      mockServers.push(server);
      console.log(
        `[mock:${station.id}] 内嵌离线模拟站点已启动: http://127.0.0.1:${port} ` +
          `（种子 ${seed}，已发布修订 ${site.totalPublished} 条，详情生效时钟初始 ${site.detailClock}）`,
      );
    }
  }

  const db = await openDatabase();
  // 两站共享一个在途调度器：全局上限 + round-robin 公平分配
  const scheduler = new SharedSlotScheduler(config.maxInflight);
  const collectors = new Map<string, Collector>();

  for (const [idx, station] of config.stations.entries()) {
    // baseUrl 解析：显式外部 URL 优先；留空时使用内嵌模拟站点端口
    let baseUrl = station.baseUrl;
    if (!baseUrl) {
      if (!embedMockEnabled()) {
        throw new Error(`站点 ${station.id} 未配置 baseUrl：请用 STATIONS="id=http://..." 或启用 EMBED_MOCK`);
      }
      baseUrl = `http://127.0.0.1:${config.mockPort + idx}`;
    }
    const fetcher = new StationFetcher(
      station.id,
      baseUrl,
      scheduler,
      station.rateMaxRequests,
      station.rateWindowMs,
    );
    const collector = new Collector(db, fetcher);
    collectors.set(station.id, collector);
  }

  const apiServer = startApiServer(db, collectors, scheduler);

  for (const collector of collectors.values()) {
    await collector.start(config.autoFull);
  }

  const shutdown = async (signal: string): Promise<void> => {
    console.log(`\n[main] 收到 ${signal}，优雅停止中（强制 kill 后重启亦可续跑）`);
    await Promise.all([...collectors.values()].map((c) => c.shutdown()));
    apiServer.close();
    for (const server of mockServers) server.close();
    await db.close().catch(() => undefined);
    process.exit(0);
  };
  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));
}

main().catch((err) => {
  console.error("[main] 启动失败：", err);
  process.exit(1);
});
