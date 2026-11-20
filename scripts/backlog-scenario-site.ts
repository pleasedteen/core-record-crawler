/**
 * 积压场景专用离线站点（仅用于 `npm run verify:backlog`）。
 *
 * 与 README 里的“模拟馆藏站点”分开实现，避免影响默认端到端验证：
 *  - 11,600 个积压详情地址（BG-xxxxx），详情立即可用；
 *  - 40 个“详情滞后”地址（ST-xxxxx），r1 已生效、r2 已发布但详情暂不展示，
 *    由 /__admin/reveal 统一切换为已生效；
 *  - 6 个“前两次 503 + Retry-After”地址（FL-xxxxx），用于验证退避让出额度；
 *  - 5 个追更新地址（NC-xxxxx），只出现在 /corrections 列表里；
 *  - 记录每个非管理请求的时刻、类型、地址与状态码，供时序断言。
 */
import http from "node:http";

export interface CoreRevisionFixture {
  coreId: string;
  revision: number;
  publishedSeq: number;
  classification: string;
  formation: string;
  depthTop: number;
  depthBottom: number;
  verifier: string;
  summary: string;
}

export interface RequestLogEntry {
  t: number;
  kind: "catalog" | "corrections" | "detail";
  coreId: string | null;
  status: number;
  path: string;
}

const CLASSIFICATIONS = ["砂岩", "泥岩", "灰岩", "页岩", "花岗岩", "玄武岩", "白云岩", "砾岩"];
const FORMATIONS = ["青山组", "白河组", "龙泉组", "丹霞组", "苍海组", "石林组"];
const VERIFIERS = ["张核实", "李核实", "王核实", "赵核实"];

function pad(n: number, width: number): string {
  return String(n).padStart(width, "0");
}

function makeRevision(
  coreId: string,
  revision: number,
  publishedSeq: number,
): CoreRevisionFixture {
  const i = publishedSeq % 997;
  return {
    coreId,
    revision,
    publishedSeq,
    classification: CLASSIFICATIONS[i % CLASSIFICATIONS.length],
    formation: FORMATIONS[i % FORMATIONS.length],
    depthTop: i % 500,
    depthBottom: (i % 500) + 40,
    verifier: VERIFIERS[i % VERIFIERS.length],
    summary: `岩芯 ${coreId} 第 ${revision} 次修订`,
  };
}

export interface ScenarioOptions {
  backlogCores: number;
  staleCores: number;
  flakyCores: number;
  newCores: number;
  /** 每个 FL 地址前多少次请求返回 503。 */
  flakyTimes: number;
  flakyRetryAfterSec: number;
}

export class BacklogScenarioSite {
  private cores = new Map<string, CoreRevisionFixture[]>();
  private newCorrections: CoreRevisionFixture[] = [];
  private revealed = false;
  private flakyHits = new Map<string, number>();
  /** 每个地址“还欠多少次 503”；缺省时退回 opts.flakyTimes。 */
  private flakyQuota = new Map<string, number>();
  private flakyRetryAfterOverride = new Map<string, number>();
  private pauseNextMs: number | null = null;
  /** 最近一次“暂停所有请求”的 503 应答时刻与要求等待时长。 */
  private pauseServedAt = 0;
  private pauseServedMs = 0;
  private log: RequestLogEntry[] = [];
  readonly opts: ScenarioOptions;
  readonly backlogIds: string[] = [];
  readonly staleIds: string[] = [];
  readonly flakyIds: string[] = [];
  readonly newIds: string[] = [];

  constructor(opts: Partial<ScenarioOptions> = {}) {
    this.opts = {
      backlogCores: 11_600,
      staleCores: 40,
      flakyCores: 6,
      newCores: 5,
      flakyTimes: 2,
      flakyRetryAfterSec: 0.45,
      ...opts,
    };
    this.seed();
  }

  private seed(): void {
    const { backlogCores, staleCores, flakyCores, newCores } = this.opts;
    let seq = 0;
    for (let i = 1; i <= backlogCores; i++) {
      const coreId = `BG-${pad(i, 5)}`;
      seq += 1;
      this.cores.set(coreId, [makeRevision(coreId, 1, seq)]);
      this.backlogIds.push(coreId);
    }
    for (let i = 1; i <= staleCores; i++) {
      const coreId = `ST-${pad(i, 4)}`;
      const r1 = makeRevision(coreId, 1, 300_000 + i);
      const r2 = makeRevision(coreId, 2, 400_000 + i);
      this.cores.set(coreId, [r1, r2]);
      this.staleIds.push(coreId);
    }
    for (let i = 1; i <= flakyCores; i++) {
      const coreId = `FL-${pad(i, 4)}`;
      this.cores.set(coreId, [makeRevision(coreId, 1, 500_000 + i)]);
      this.flakyIds.push(coreId);
    }
    for (let i = 1; i <= newCores; i++) {
      const coreId = `NC-${pad(i, 4)}`;
      const rev = makeRevision(coreId, 1, 600_000 + i);
      this.cores.set(coreId, [rev]);
      this.newCorrections.push(rev);
      this.newIds.push(coreId);
    }
  }

  /** 种子数据：供验证脚本直接写入 DuckDB（积压上万条，逐条走站点太慢）。 */
  seedRows(): Array<{ coreId: string; revision: number; publishedSeq: number }> {
    const rows: Array<{ coreId: string; revision: number; publishedSeq: number }> = [];
    for (const coreId of this.backlogIds) {
      rows.push({ coreId, revision: 1, publishedSeq: this.cores.get(coreId)![0].publishedSeq });
    }
    for (const coreId of this.staleIds) {
      for (const rev of this.cores.get(coreId)!) {
        rows.push({ coreId, revision: rev.revision, publishedSeq: rev.publishedSeq });
      }
    }
    for (const coreId of this.flakyIds) {
      rows.push({ coreId, revision: 1, publishedSeq: this.cores.get(coreId)![0].publishedSeq });
    }
    return rows;
  }

  get totalPublished(): number {
    return 600_000 + this.opts.newCores;
  }

  private renderDetail(coreId: string): string | null {
    const list = this.cores.get(coreId);
    if (!list) return null;
    // ST 地址在 reveal 之前只展示 r1（新修订已公布、详情仍旧修订）
    const visible = list.filter((r) => {
      if (r.revision === 1) return true;
      return this.revealed && this.staleIds.includes(coreId);
    });
    if (visible.length === 0) return null;
    const shown = visible[visible.length - 1];
    const history = visible
      .map(
        (r) => `    <dl class="identification" data-revision="${r.revision}" data-published-seq="${r.publishedSeq}">
      <dt>岩性分类</dt><dd class="classification">${r.classification}</dd>
      <dt>所属地层</dt><dd class="formation">${r.formation}</dd>
      <dt>井段(米)</dt><dd class="depth">${r.depthTop} - ${r.depthBottom}</dd>
      <dt>核实人</dt><dd class="verifier">${r.verifier}</dd>
      <dt>发布时间</dt><dd class="published-at">2026-10-12T09:30:00.000Z</dd>
      <dt>鉴定说明</dt><dd class="summary">${r.summary}</dd>
    </dl>`,
      )
      .join("\n");
    return `<!doctype html>
<html lang="zh-CN">
<head><meta charset="utf-8"><title>岩芯 ${coreId} 详情</title></head>
<body>
  <h1>岩芯 ${coreId} 详情</h1>
  <!-- core-id: ${coreId} -->
  <!-- current-revision: ${shown.revision} -->
  <!-- current-published-seq: ${shown.publishedSeq} -->
  <section class="history">
${history}
  </section>
</body>
</html>`;
  }

  private renderCorrections(): string {
    return JSON.stringify({
      page: 1,
      totalPages: 1,
      pageSize: 20,
      totalPublished: this.totalPublished,
      items: this.newCorrections.map((r) => ({
        coreId: r.coreId,
        revision: r.revision,
        publishedSeq: r.publishedSeq,
        publishedAt: "2026-10-20T09:30:00.000Z",
        detailUrl: `/core/${r.coreId}`,
        note: "鉴定更正",
      })),
    });
  }

  private record(kind: RequestLogEntry["kind"], path: string, coreId: string | null, status: number): void {
    this.log.push({ t: Date.now(), kind, coreId, status, path });
  }

  createServer(): http.Server {
    return http.createServer((req, res) => {
      const url = new URL(req.url ?? "/", "http://localhost");
      const send = (status: number, body: string, type: string, extra?: Record<string, string>) => {
        res.writeHead(status, { "Content-Type": `${type}; charset=utf-8`, ...extra });
        res.end(body);
      };

      // 管理接口不进入请求日志，也不受故障/暂停影响
      if (url.pathname === "/__admin/reveal") {
        this.revealed = true;
        return send(200, JSON.stringify({ revealed: true }), "application/json");
      }
      if (url.pathname === "/__admin/pause-next") {
        this.pauseNextMs = Math.max(1, Number(url.searchParams.get("ms") ?? "500") || 500);
        return send(200, JSON.stringify({ armed: this.pauseNextMs }), "application/json");
      }
      // 让指定地址“再失败 n 次、每次 Retry-After = retryAfterSec”
      if (url.pathname === "/__admin/flaky") {
        const coreId = url.searchParams.get("coreId") ?? "";
        const times = Math.max(0, Number(url.searchParams.get("times") ?? "3") || 0);
        const retryAfterSec = Math.max(0.05, Number(url.searchParams.get("retryAfterSec") ?? "1") || 1);
        if (!this.cores.has(coreId)) {
          return send(404, JSON.stringify({ error: "岩芯不存在" }), "application/json");
        }
        this.flakyQuota.set(coreId, times);
        this.flakyRetryAfterOverride.set(coreId, retryAfterSec);
        return send(200, JSON.stringify({ coreId, times, retryAfterSec }), "application/json");
      }
      if (url.pathname === "/__admin/log") {
        const since = Number(url.searchParams.get("since") ?? "0") || 0;
        const kind = url.searchParams.get("kind");
        const entries = this.log.filter((e) => e.t >= since && (!kind || e.kind === kind));
        return send(200, JSON.stringify({ now: Date.now(), entries }), "application/json");
      }
      if (url.pathname === "/__admin/reset-log") {
        this.log = [];
        return send(200, JSON.stringify({ ok: true }), "application/json");
      }
      if (url.pathname === "/__admin/state") {
        return send(
          200,
          JSON.stringify({
            backlog: this.backlogIds.length,
            stale: this.staleIds.length,
            flaky: this.flakyIds.length,
            newCores: this.newIds.length,
            revealed: this.revealed,
            totalPublished: this.totalPublished,
            pauseServedAt: this.pauseServedAt,
            pauseServedMs: this.pauseServedMs,
          }),
          "application/json",
        );
      }

      // 一次性 503 + Retry-After（模拟站点要求全局等待）
      if (this.pauseNextMs !== null) {
        const ms = this.pauseNextMs;
        this.pauseNextMs = null;
        this.pauseServedAt = Date.now();
        this.pauseServedMs = ms;
        this.record("detail", url.pathname, url.pathname.startsWith("/core/") ? url.pathname.slice(6) : null, 503);
        return send(503, "站点临时繁忙（暂停所有请求）", "text/plain", {
          "Retry-After": String(ms / 1000),
        });
      }

      if (url.pathname === "/catalog" || url.pathname === "/") {
        this.record("catalog", url.pathname, null, 200);
        return send(
          200,
          `<!doctype html><html><body><p class="pager" data-page="1" data-total-pages="1" data-total-cores="0">第 1 / 1 页</p><ul></ul></body></html>`,
          "text/html",
        );
      }

      if (url.pathname === "/corrections") {
        this.record("corrections", url.pathname, null, 200);
        return send(200, this.renderCorrections(), "application/json");
      }

      const m = url.pathname.match(/^\/core\/([A-Za-z0-9\-]+)$/);
      if (m) {
        const coreId = m[1];
        if (!this.cores.has(coreId)) {
          this.record("detail", url.pathname, coreId, 404);
          return send(404, "岩芯不存在", "text/plain");
        }
        const hits = (this.flakyHits.get(coreId) ?? 0) + 1;
        this.flakyHits.set(coreId, hits);
        const quota = this.flakyQuota.get(coreId);
        const shouldFail =
          quota !== undefined
            ? quota > 0
            : this.flakyIds.includes(coreId) && hits <= this.opts.flakyTimes;
        if (shouldFail) {
          if (quota !== undefined) this.flakyQuota.set(coreId, quota - 1);
          this.record("detail", url.pathname, coreId, 503);
          return send(503, "站点临时繁忙", "text/plain", {
            "Retry-After": String(
              this.flakyRetryAfterOverride.get(coreId) ?? this.opts.flakyRetryAfterSec,
            ),
          });
        }
        this.record("detail", url.pathname, coreId, 200);
        return send(200, this.renderDetail(coreId) ?? "", "text/html");
      }

      send(404, "未找到", "text/plain");
    });
  }
}
