/**
 * 本地 HTTP 接口：查询已核实鉴定 + 采集进度 + 手工触发采集（多站版）。
 *
 * 路由分两层：
 *  - 原有单站路径（/cores、/progress、/stats、/runs、/collect/* …）保持不变，
 *    绑定到配置中的第一个站点（单站模式即站点 default），旧调用方式零改动可用；
 *  - 新增按站路径 /stations/:stationId/... 显式指定站点；
 *  - /stations 列出全部站点与两站共享在途调度器的观测统计；
 *  - /compare-cores/:coreId 跨站对比同号岩芯的已核实内容（内容隔离证据）。
 */
import http from "node:http";
import { URL } from "node:url";
import { Database, IdentificationRow } from "./db.js";
import { Collector, ProgressSnapshot } from "./collect/collector.js";
import { SharedSlotScheduler } from "./collect/http.js";
import { config } from "./config.js";

const TOKEN_RE = /^[A-Za-z0-9\-_]+$/;

function json(res: http.ServerResponse, status: number, payload: unknown): void {
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(payload, (_k, v) => (typeof v === "bigint" ? Number(v) : v), 2));
}

function readBody(req: http.IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", (c) => {
      data += c;
      if (data.length > 1e6) reject(new Error("请求体过大"));
    });
    req.on("end", () => resolve(data));
    req.on("error", reject);
  });
}

function corePayload(row: IdentificationRow): Record<string, unknown> {
  return {
    stationId: row.station_id,
    coreId: row.core_id,
    revision: row.revision,
    publishedSeq: Number(row.published_seq),
    publishedAt: row.published_at,
    verifiedIdentification: {
      classification: row.classification,
      formation: row.formation,
      depthTop: row.depth_top,
      depthBottom: row.depth_bottom,
      verifier: row.verifier,
      summary: row.summary,
    },
    status: row.status,
    attempts: row.attempts,
  };
}

export function createApiServer(
  db: Database,
  collectors: Map<string, Collector>,
  scheduler: SharedSlotScheduler,
): http.Server {
  const defaultStationId = config.stations[0].id;

  const requireStation = (res: http.ServerResponse, stationId: string): Collector | null => {
    const c = collectors.get(stationId);
    if (!c) {
      json(res, 404, { error: `未知站点: ${stationId}`, stations: [...collectors.keys()] });
      return null;
    }
    return c;
  };

  const handleCore = async (res: http.ServerResponse, stationId: string, coreId: string): Promise<void> => {
    if (!TOKEN_RE.test(coreId)) return json(res, 400, { error: "岩芯编号格式不合法" });
    const row = await db.getLatestVerified(stationId, coreId);
    if (!row) {
      return json(res, 404, {
        stationId,
        coreId,
        found: false,
        message: "该岩芯暂无已核实鉴定（可能尚未采集或仍在核实中）",
      });
    }
    return json(res, 200, corePayload(row));
  };

  const handleCoresList = async (
    res: http.ServerResponse,
    stationId: string,
    url: URL,
  ): Promise<void> => {
    const limit = Math.min(200, Math.max(1, Number(url.searchParams.get("limit") ?? "50")));
    const offset = Math.max(0, Number(url.searchParams.get("offset") ?? "0"));
    const rows = await db.listVerified(stationId, limit, offset);
    const total = await db.countVerifiedCores(stationId);
    return json(res, 200, {
      stationId,
      total,
      limit,
      offset,
      items: rows.map((row) => ({
        stationId: row.station_id,
        coreId: row.core_id,
        revision: row.revision,
        publishedSeq: Number(row.published_seq),
        classification: row.classification,
        formation: row.formation,
        verifier: row.verifier,
      })),
    });
  };

  const handleIdentifications = async (
    res: http.ServerResponse,
    stationId: string,
    coreId: string,
  ): Promise<void> => {
    if (!TOKEN_RE.test(coreId)) return json(res, 400, { error: "岩芯编号格式不合法" });
    const rows = await db.all<IdentificationRow>(
      `SELECT * FROM identifications WHERE station_id = ? AND core_id = ? ORDER BY revision`,
      [stationId, coreId],
    );
    if (rows.length === 0) return json(res, 404, { stationId, coreId, found: false });
    return json(res, 200, {
      stationId,
      coreId,
      revisions: rows.map((r) => ({
        revision: r.revision,
        publishedSeq: Number(r.published_seq),
        status: r.status,
        classification: r.classification,
        verifier: r.verifier,
        attempts: r.attempts,
        lastError: r.last_error,
        updatedAt: r.updated_at,
      })),
    });
  };

  const handleStats = async (res: http.ServerResponse, stationId: string): Promise<void> => {
    const urlKinds = await db.all<{ kind: string; n: number; attempts_sum: number }>(
      `SELECT kind,
              CAST(COUNT(*) AS INTEGER) AS n,
              CAST(COALESCE(SUM(attempts), 0) AS INTEGER) AS attempts_sum
         FROM fetch_urls WHERE station_id = ? GROUP BY kind ORDER BY kind`,
      [stationId],
    );
    const runStatus = await db.all<{ status: string; n: number }>(
      `SELECT status, CAST(COUNT(*) AS INTEGER) AS n FROM runs
        WHERE station_id = ? GROUP BY status ORDER BY status`,
      [stationId],
    );
    const counts = await db.countByStatus(stationId);
    return json(res, 200, {
      stationId,
      identifications: {
        total: counts.verified + counts.pending,
        verified: counts.verified,
        pending: counts.pending,
      },
      fetchUrls: urlKinds.map((r) => ({
        kind: r.kind,
        distinctUrls: Number(r.n),
        totalAttempts: Number(r.attempts_sum),
      })),
      runs: runStatus.map((r) => ({ status: r.status, count: Number(r.n) })),
    });
  };

  const handleProgress = async (res: http.ServerResponse, collector: Collector): Promise<void> => {
    const snap = await collector.snapshot();
    const state = await db.getRunState(collector.stationId);
    return json(res, 200, {
      ...snap,
      state: {
        catalogPageCursor: Number(state.catalog_page_cursor),
        catalogTotalPages:
          state.catalog_total_pages === null ? null : Number(state.catalog_total_pages),
        correctionPageCursor: Number(state.correction_page_cursor),
        correctionTotalPages:
          state.correction_total_pages === null ? null : Number(state.correction_total_pages),
      },
    });
  };

  const handleTrigger = async (
    res: http.ServerResponse,
    collector: Collector,
    kind: "FULL" | "INCREMENTAL",
  ): Promise<void> => {
    const snap: ProgressSnapshot = await collector.snapshot();
    if (snap.running) return json(res, 409, { error: "该站已有采集任务在运行", progress: snap });
    if (kind === "FULL") {
      void collector.runFullForever();
      return json(res, 202, { stationId: collector.stationId, triggered: "FULL", message: "全量采集已触发" });
    }
    void collector.runCatchup();
    return json(res, 202, { stationId: collector.stationId, triggered: "INCREMENTAL", message: "增量追更正已触发" });
  };

  return http.createServer(async (req, res) => {
    const url = new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost"}`);
    const p = url.pathname;

    try {
      if (p === "/health" && req.method === "GET") {
        return json(res, 200, { ok: true, time: new Date().toISOString(), stations: [...collectors.keys()] });
      }

      // 站点列表 + 两站共享在途调度器观测
      if (p === "/stations" && req.method === "GET") {
        const snapshots = await Promise.all([...collectors.values()].map((c) => c.snapshot()));
        return json(res, 200, {
          defaultStationId,
          sharedInflight: {
            capacity: config.maxInflight,
            current: scheduler.inflight,
            highWaterInflight: scheduler.stats.highWaterInflight,
            totalAcquired: scheduler.stats.totalAcquired,
          },
          stations: snapshots,
        });
      }

      // 跨站对比同号岩芯：每个站一条已核实记录，内容不同即证明同号隔离
      const compareMatch = p.match(/^\/compare-cores\/([A-Za-z0-9\-_]+)$/);
      if (compareMatch && req.method === "GET") {
        const coreId = compareMatch[1];
        const rows = await db.listVerifiedByCoreAcrossStations(coreId);
        const grouped = new Map<string, IdentificationRow>();
        for (const row of rows) if (!grouped.has(row.station_id)) grouped.set(row.station_id, row);
        return json(res, 200, {
          coreId,
          stationCount: grouped.size,
          records: [...grouped.values()].map((row) => corePayload(row)),
        });
      }

      // ---------- 按站新路径 /stations/:stationId/... ----------
      const stationMatch = p.match(/^\/stations\/([A-Za-z0-9\-_]+)(\/.*)?$/);
      if (stationMatch) {
        const stationId = stationMatch[1];
        const sub = stationMatch[2] ?? "/";
        const collector = requireStation(res, stationId);
        if (!collector) return;

        if (sub === "/progress" && req.method === "GET") return handleProgress(res, collector);
        if (sub === "/stats" && req.method === "GET") return handleStats(res, stationId);

        const coreM = sub.match(/^\/cores\/([A-Za-z0-9\-_]+)$/);
        if (coreM && req.method === "GET") return handleCore(res, stationId, coreM[1]);
        if (sub === "/cores" && req.method === "GET") return handleCoresList(res, stationId, url);

        const idM = sub.match(/^\/identifications\/([A-Za-z0-9\-_]+)$/);
        if (idM && req.method === "GET") return handleIdentifications(res, stationId, idM[1]);

        if (sub === "/runs" && req.method === "GET") {
          return json(res, 200, { stationId, runs: await db.listRuns(stationId, 30) });
        }
        const runM = sub.match(/^\/runs\/(\d+)$/);
        if (runM && req.method === "GET") {
          const row = await db.getRun(stationId, Number(runM[1]));
          return row ? json(res, 200, row) : json(res, 404, { error: "运行记录不存在" });
        }
        if (sub === "/collect/run-full" && req.method === "POST") {
          await readBody(req).catch(() => "");
          return handleTrigger(res, collector, "FULL");
        }
        if (sub === "/collect/catchup" && req.method === "POST") {
          await readBody(req).catch(() => "");
          return handleTrigger(res, collector, "INCREMENTAL");
        }
        return json(res, 404, { error: "未找到站点子接口", stationId, sub });
      }

      // ---------- 原有单站路径（绑定默认站点，行为与旧版一致） ----------
      const coreMatch = p.match(/^\/cores\/([A-Za-z0-9\-_]+)$/);
      if (coreMatch && req.method === "GET") return handleCore(res, defaultStationId, coreMatch[1]);

      if (p === "/cores" && req.method === "GET") return handleCoresList(res, defaultStationId, url);

      const idMatch = p.match(/^\/identifications\/([A-Za-z0-9\-_]+)$/);
      if (idMatch && req.method === "GET") {
        return handleIdentifications(res, defaultStationId, idMatch[1]);
      }

      if (p === "/progress" && req.method === "GET") {
        const collector = collectors.get(defaultStationId)!;
        return handleProgress(res, collector);
      }

      if (p === "/runs" && req.method === "GET") {
        return json(res, 200, { stationId: defaultStationId, runs: await db.listRuns(defaultStationId, 30) });
      }
      const runMatch = p.match(/^\/runs\/(\d+)$/);
      if (runMatch && req.method === "GET") {
        const row = await db.getRun(defaultStationId, Number(runMatch[1]));
        return row ? json(res, 200, row) : json(res, 404, { error: "运行记录不存在" });
      }

      if (p === "/stats" && req.method === "GET") return handleStats(res, defaultStationId);

      if (p === "/collect/run-full" && req.method === "POST") {
        await readBody(req).catch(() => "");
        return handleTrigger(res, collectors.get(defaultStationId)!, "FULL");
      }
      if (p === "/collect/catchup" && req.method === "POST") {
        await readBody(req).catch(() => "");
        return handleTrigger(res, collectors.get(defaultStationId)!, "INCREMENTAL");
      }

      return json(res, 404, {
        error: "未找到接口",
        endpoints: [
          "GET /health",
          "GET /stations",
          "GET /stations/:stationId/progress",
          "GET /stations/:stationId/cores/:coreId",
          "GET /stations/:stationId/cores?limit=&offset=",
          "GET /stations/:stationId/identifications/:coreId",
          "GET /stations/:stationId/stats",
          "GET /stations/:stationId/runs",
          "POST /stations/:stationId/collect/run-full",
          "POST /stations/:stationId/collect/catchup",
          "GET /compare-cores/:coreId",
          "GET /cores/:coreId （默认站点，兼容旧调用）",
          "GET /progress （默认站点，兼容旧调用）",
        ],
      });
    } catch (err) {
      return json(res, 500, { error: err instanceof Error ? err.message : String(err) });
    }
  });
}

export function startApiServer(
  db: Database,
  collectors: Map<string, Collector>,
  scheduler: SharedSlotScheduler,
): http.Server {
  const server = createApiServer(db, collectors, scheduler);
  server.listen(config.httpPort, "127.0.0.1", () => {
    console.log(`[api] 本地查询/进度接口已启动: http://127.0.0.1:${config.httpPort}`);
  });
  return server;
}
