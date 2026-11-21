/**
 * 本地 HTTP 接口：查询已核实鉴定 + 采集进度 + 手工触发采集。
 */
import http from "node:http";
import { URL } from "node:url";
import { Database } from "./db.js";
import { Collector } from "./collect/collector.js";
import { config } from "./config.js";

const CORE_ID_RE = /^[A-Za-z0-9\-]+$/;

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

export function createApiServer(db: Database, collector: Collector): http.Server {
  return http.createServer(async (req, res) => {
    const url = new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost"}`);
    const p = url.pathname;

    try {
      if (p === "/health" && req.method === "GET") {
        return json(res, 200, { ok: true, time: new Date().toISOString() });
      }

      // 按岩芯编号查询最新已核实鉴定
      const coreMatch = p.match(/^\/cores\/([A-Za-z0-9\-]+)$/);
      if (coreMatch && req.method === "GET") {
        const coreId = coreMatch[1];
        if (!CORE_ID_RE.test(coreId)) return json(res, 400, { error: "岩芯编号格式不合法" });
        const row = await db.getLatestVerified(coreId);
        if (!row) {
          return json(res, 404, {
            coreId,
            found: false,
            message: "该岩芯暂无已核实鉴定（可能尚未采集或仍在核实中）",
          });
        }
        return json(res, 200, {
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
        });
      }

      // 已核实岩芯列表（分页）
      if (p === "/cores" && req.method === "GET") {
        const limit = Math.min(200, Math.max(1, Number(url.searchParams.get("limit") ?? "50")));
        const offset = Math.max(0, Number(url.searchParams.get("offset") ?? "0"));
        const rows = await db.listVerified(limit, offset);
        const total = await db.countVerifiedCores();
        return json(res, 200, {
          total,
          limit,
          offset,
          items: rows.map((row) => ({
            coreId: row.core_id,
            revision: row.revision,
            publishedSeq: Number(row.published_seq),
            classification: row.classification,
            formation: row.formation,
            verifier: row.verifier,
          })),
        });
      }

      // 某岩芯的全部修订及核实状态（同一详情地址上的多个修订）
      const idMatch = p.match(/^\/identifications\/([A-Za-z0-9\-]+)$/);
      if (idMatch && req.method === "GET") {
        const coreId = idMatch[1];
        if (!CORE_ID_RE.test(coreId)) return json(res, 400, { error: "岩芯编号格式不合法" });
        const rows = await db.all<import("./db.js").IdentificationRow>(
          `SELECT * FROM identifications WHERE core_id = ? ORDER BY revision`,
          [coreId],
        );
        if (rows.length === 0) return json(res, 404, { coreId, found: false });
        return json(res, 200, {
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
      }

      // 采集进度
      if (p === "/progress" && req.method === "GET") {
        const snap = await collector.snapshot();
        const state = await db.getRunState();
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
      }

      // 运行历史
      if (p === "/runs" && req.method === "GET") {
        return json(res, 200, { runs: await db.listRuns(30) });
      }
      const runMatch = p.match(/^\/runs\/(\d+)$/);
      if (runMatch && req.method === "GET") {
        const row = await db.getRun(Number(runMatch[1]));
        return row ? json(res, 200, row) : json(res, 404, { error: "运行记录不存在" });
      }

      // 汇总统计（验证用：URL 去重、重试、核实计数、更正列表逐页请求次数）
      if (p === "/stats" && req.method === "GET") {
        const urlKinds = await db.all<{ kind: string; n: number; attempts_sum: number }>(
          `SELECT kind,
                  CAST(COUNT(*) AS INTEGER) AS n,
                  CAST(COALESCE(SUM(attempts), 0) AS INTEGER) AS attempts_sum
             FROM fetch_urls GROUP BY kind ORDER BY kind`,
        );
        const correctionPageUrls = await db.all<{ url: string; attempts: number; status: string }>(
          `SELECT url, CAST(attempts AS INTEGER) AS attempts, status
             FROM fetch_urls WHERE kind = 'CORRECTIONS' ORDER BY url`,
        );
        const runStatus = await db.all<{ status: string; n: number }>(
          `SELECT status, CAST(COUNT(*) AS INTEGER) AS n FROM runs GROUP BY status ORDER BY status`,
        );
        const counts = await db.countByStatus();
        return json(res, 200, {
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
          correctionPageUrls: correctionPageUrls.map((r) => ({
            url: r.url,
            attempts: Number(r.attempts),
            status: r.status,
          })),
          runs: runStatus.map((r) => ({ status: r.status, count: Number(r.n) })),
        });
      }

      // 手工触发：全量 / 增量
      if (p === "/collect/run-full" && req.method === "POST") {
        await readBody(req).catch(() => "");
        const snap = await collector.snapshot();
        if (snap.running) return json(res, 409, { error: "已有采集任务在运行", progress: snap });
        void collector.runFullForever();
        return json(res, 202, { triggered: "FULL", message: "全量采集已触发" });
      }
      if (p === "/collect/catchup" && req.method === "POST") {
        await readBody(req).catch(() => "");
        const snap = await collector.snapshot();
        if (snap.running) return json(res, 409, { error: "已有采集任务在运行", progress: snap });
        void collector.runCatchup();
        return json(res, 202, { triggered: "INCREMENTAL", message: "增量追更正已触发" });
      }

      return json(res, 404, {
        error: "未找到接口",
        endpoints: [
          "GET /health",
          "GET /cores/:coreId",
          "GET /cores?limit=&offset=",
          "GET /identifications/:coreId",
          "GET /progress",
          "GET /runs",
          "GET /runs/:id",
          "GET /stats",
          "POST /collect/run-full",
          "POST /collect/catchup",
        ],
      });
    } catch (err) {
      return json(res, 500, { error: err instanceof Error ? err.message : String(err) });
    }
  });
}

export function startApiServer(db: Database, collector: Collector): http.Server {
  const server = createApiServer(db, collector);
  server.listen(config.httpPort, "127.0.0.1", () => {
    console.log(`[api] 本地查询/进度接口已启动: http://127.0.0.1:${config.httpPort}`);
  });
  return server;
}
