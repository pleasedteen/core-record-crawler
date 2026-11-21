/**
 * 离线模拟馆藏站点。
 *
 * 特性（对应需求）：
 *  - 分页 HTML 馆藏目录：/catalog?page=N，含重复链接，页头/页脚标明总页数
 *  - JSON 更正列表：/corrections?page=N，按发布顺序翻页
 *  - 岩芯详情页：/core/:id，同一岩芯多个修订共用同一地址
 *  - 修订发布后，详情页可能暂时仍显示旧修订（详情延迟生效）
 *  - 早期鉴定可能在“当天”即被更正（相同日期、不同 revision）
 *  - 临时拒绝：503 + Retry-After，以及随机 500
 *
 * 数据在站点启动时生成并保持稳定，保证可重复离线验证。
 */
import http from "node:http";
import { config } from "../config.js";
import { hashSeed, mulberry32, pick } from "../util/rng.js";

export interface CoreRevision {
  coreId: string;
  revision: number;
  publishedAt: string; // ISO 时间（更正列表发布顺序键）
  publishedSeq: number; // 全局单调发布序号
  detailReadySeq: number; // 详情页从哪个发布序号起展示该修订
  classification: string;
  formation: string;
  depthTop: number;
  depthBottom: number;
  verifier: string;
  summary: string;
}

const PAGE_SIZE = 12;
const CORRECTION_PAGE_SIZE = 10;
const NUM_CORES = 40;
const CLASSIFICATIONS = ["砂岩", "泥岩", "灰岩", "页岩", "花岗岩", "玄武岩", "白云岩", "砾岩"];
const FORMATIONS = ["青山组", "白河组", "龙泉组", "丹霞组", "苍海组", "石林组"];
const VERIFIERS = ["张核实", "李核实", "王核实", "赵核实"];

function pad(n: number, width: number): string {
  return String(n).padStart(width, "0");
}

function buildRevisions(seed: number): Map<string, CoreRevision[]> {
  const rng = mulberry32(seed);
  const cores = new Map<string, CoreRevision[]>();

  // 1) 生成每个岩芯的初始鉴定（发布序号 1..N）
  let seq = 0;
  for (let i = 1; i <= NUM_CORES; i++) {
    const coreId = `RC-${pad(i, 4)}`;
    seq += 1;
    const day = 1 + Math.floor(rng() * 20);
    const rev: CoreRevision = {
      coreId,
      revision: 1,
      publishedAt: `2026-10-${pad(day, 2)}T08:00:00.000Z`,
      publishedSeq: seq,
      detailReadySeq: seq, // 初始鉴定详情立即可用
      classification: pick(rng, CLASSIFICATIONS),
      formation: pick(rng, FORMATIONS),
      depthTop: Math.round(rng() * 500),
      depthBottom: 0,
      verifier: pick(rng, VERIFIERS),
      summary: `岩芯 ${coreId} 初始岩性鉴定`,
    };
    rev.depthBottom = rev.depthTop + 20 + Math.floor(rng() * 180);
    cores.set(coreId, [rev]);
  }

  // 2) 生成更正：部分岩芯有 1~2 个后续修订；部分“当天更正”；详情延迟生效
  let sameDay = 0;
  for (const [coreId, list] of cores) {
    const extra = rng() < 0.55 ? 1 : rng() < 0.35 ? 2 : 0;
    for (let k = 1; k <= extra; k++) {
      seq += 1;
      const prev = list[list.length - 1];
      const sameDayCorrection = rng() < 0.35;
      let publishedAt: string;
      if (sameDayCorrection && sameDay < 6) {
        // 与上一修订同一天发布（当天更正）
        publishedAt = prev.publishedAt.replace("T08:00:00.000Z", `T${pad(10 + k, 2)}:15:00.000Z`);
        sameDay += 1;
      } else {
        const d = new Date(prev.publishedAt);
        d.setUTCDate(d.getUTCDate() + 1 + Math.floor(rng() * 4));
        d.setUTCHours(9, 30, 0, 0);
        publishedAt = d.toISOString();
      }
      // 详情页滞后 0~3 个发布序号才更新（列表已公布、详情仍旧修订）
      const lag = rng() < 0.45 ? 1 + Math.floor(rng() * 3) : 0;
      const rev: CoreRevision = {
        coreId,
        revision: prev.revision + 1,
        publishedAt,
        publishedSeq: seq,
        detailReadySeq: seq + lag,
        classification: pick(rng, CLASSIFICATIONS),
        formation: prev.formation,
        depthTop: prev.depthTop,
        depthBottom: prev.depthBottom,
        verifier: pick(rng, VERIFIERS),
        summary: `岩芯 ${coreId} 第 ${prev.revision + 1} 次修订（更正前次鉴定）`,
      };
      list.push(rev);
    }
  }

  // 按发布时间排序每个岩芯的修订（同日更正也有序），并归一化详情生效序号
  const globalMaxSeq = seq;
  for (const list of cores.values()) {
    list.sort((a, b) => a.publishedSeq - b.publishedSeq);
    // 截断到全局最大发布序号：保留“相对发布滞后生效”，同时保证每个修订
    // 在站点时钟走到最大值时都能在详情页出现（不会出现永远无法生效的修订）。
    for (const rev of list) {
      rev.detailReadySeq = Math.min(rev.detailReadySeq, globalMaxSeq);
    }
  }
  return cores;
}

export interface MockSiteOptions {
  /** 数据集确定性种子：不同种子让两个站对同号岩芯产出不同鉴定内容（验证内容隔离）。 */
  seed?: number;
  /** 随机故障注入开关（默认开启；确定性等待测试可关闭后改用 force-503）。 */
  randomFaults?: boolean;
}

export class MockArchiveSite {
  private cores: Map<string, CoreRevision[]>;
  private allRevisions: CoreRevision[];
  private rng = mulberry32(987654321);
  private requestCount = 0;
  private readonly randomFaults: boolean;
  /** 强制下一次（或接下来 N 次）采集请求返回 503 + Retry-After，确定性验证站点等待。 */
  private forced503Remaining = 0;
  private forcedRetryAfterSec = 1;
  /** 采集类请求的实时在途数与历史高水位（验证两站共享在途上限用；管理接口不计入）。 */
  private inflightNow = 0;
  private inflightPeak = 0;
  readonly totalPublished: number;
  /**
   * 站点时钟（发布序号口径）：更正列表一次性全部公布，
   * 但详情页只展示 detailReadySeq <= clock 的修订，模拟“列表已更正、详情仍旧修订”。
   */
  private clock: number;
  readonly initialClock: number;

  constructor(opts: MockSiteOptions = {}) {
    const seed = opts.seed ?? 20261118;
    this.randomFaults = opts.randomFaults ?? true;
    this.cores = buildRevisions(seed);
    this.allRevisions = [...this.cores.values()]
      .flat()
      .sort((a, b) => a.publishedSeq - b.publishedSeq);
    this.totalPublished = this.allRevisions.length;
    // 初始只“生效”前 20 个发布序号对应的详情，其余陆续生效
    this.initialClock = 20;
    this.clock = this.initialClock;
  }

  /** 站点当前“已发布”的最大序号（更正列表口径）。 */
  get currentMaxSeq(): number {
    return this.totalPublished;
  }

  /** 详情页生效时钟。 */
  get detailClock(): number {
    return this.clock;
  }

  /** 推进详情生效时钟，返回推进后的值。 */
  advanceClock(step = 6): number {
    this.clock = Math.min(this.totalPublished, this.clock + step);
    return this.clock;
  }

  /**
   * 临时故障注入：约 12% 请求 503（带 Retry-After），约 6% 请求 500。
   * 用请求计数做确定性伪随机，重试时计数继续推进，最终一定能成功。
   * 强制 503（管理接口设置）优先级最高，用于确定性验证“站点等待时间”。
   */
  private transientFault(): { status: number; retryAfter?: number } | null {
    if (this.forced503Remaining > 0) {
      this.forced503Remaining -= 1;
      return { status: 503, retryAfter: this.forcedRetryAfterSec };
    }
    if (!this.randomFaults) {
      this.requestCount += 1;
      return null;
    }
    this.requestCount += 1;
    const r = this.rng();
    if (r < 0.12) {
      const wait = 0.4 + this.rng() * 0.8;
      return { status: 503, retryAfter: Number(wait.toFixed(2)) };
    }
    if (r < 0.18) return { status: 500 };
    return null;
  }

  /** 让接下来 count 个采集请求确定性地返回 503 + Retry-After。 */
  forceNext503(count = 1, retryAfterSec = 1): void {
    this.forced503Remaining = count;
    this.forcedRetryAfterSec = retryAfterSec;
  }

  private catalogPages(): CoreRevision[][] {
    // 目录只展示每个岩芯的“当前最新已发布修订”的链接，分页并刻意制造重复链接
    const latest = [...this.cores.values()].map((l) => l[l.length - 1]);
    latest.sort((a, b) => a.coreId.localeCompare(b.coreId));
    const pages: CoreRevision[][] = [];
    for (let i = 0; i < latest.length; i += PAGE_SIZE) pages.push(latest.slice(i, i + PAGE_SIZE));
    return pages;
  }

  private renderCatalog(page: number): string {
    const pages = this.catalogPages();
    const idx = page - 1;
    const items = pages[idx] ?? [];
    const rows: string[] = [];
    for (const rev of items) {
      rows.push(
        `      <li><a href="/core/${rev.coreId}">${rev.coreId}</a> <span class="rev">修订 r${rev.revision}</span> <span class="updated">更新于 ${rev.publishedAt}</span></li>`,
      );
      // 重复链接：约 1/3 的条目在目录中再出现一次（同一详情地址、同一修订）
      if (this.rng() < 0.33) {
        rows.push(
          `      <li class="dup"><a href="/core/${rev.coreId}">${rev.coreId}</a> <span class="rev">修订 r${rev.revision}</span> <span class="updated">更新于 ${rev.publishedAt}</span></li>`,
        );
      }
    }
    const prevLink = page > 1 ? `<a rel="prev" href="/catalog?page=${page - 1}">上一页</a>` : "";
    const nextLink =
      page < pages.length ? `<a rel="next" href="/catalog?page=${page + 1}">下一页</a>` : "";
    return `<!doctype html>
<html lang="zh-CN">
<head><meta charset="utf-8"><title>馆藏目录 第${page}页</title></head>
<body>
  <h1>区域岩芯资料馆 · 馆藏目录</h1>
  <p class="pager" data-page="${page}" data-total-pages="${pages.length}" data-total-cores="${NUM_CORES}">第 ${page} / ${pages.length} 页</p>
  <ul>
${rows.join("\n")}
  </ul>
  <nav>${prevLink} ${nextLink}</nav>
</body>
</html>`;
  }

  private correctionsPage(page: number): { revisions: CoreRevision[]; page: number; totalPages: number } {
    const totalPages = Math.max(1, Math.ceil(this.allRevisions.length / CORRECTION_PAGE_SIZE));
    const start = (page - 1) * CORRECTION_PAGE_SIZE;
    return { revisions: this.allRevisions.slice(start, start + CORRECTION_PAGE_SIZE), page, totalPages };
  }

  private renderCorrections(page: number): string {
    const { revisions, totalPages } = this.correctionsPage(page);
    const payload = {
      page,
      totalPages,
      pageSize: CORRECTION_PAGE_SIZE,
      totalPublished: this.totalPublished,
      items: revisions.map((r) => ({
        coreId: r.coreId,
        revision: r.revision,
        publishedSeq: r.publishedSeq,
        publishedAt: r.publishedAt,
        detailUrl: `/core/${r.coreId}`,
        note: r.revision === 1 ? "初始鉴定" : "鉴定更正",
      })),
    };
    return JSON.stringify(payload, null, 2);
  }

  /**
   * 详情页：
   *  - 主区块展示“当前生效的最新修订”（新修订公布后可能暂时仍是旧修订）；
   *  - 修订历史列出所有已生效修订（同一详情地址承载该岩芯的多个修订，各自可被核实）；
   *  - 尚未到 detailReadySeq 的修订不出现在页面上，等待复查。
   */
  private renderDetail(coreId: string): string | null {
    const list = this.cores.get(coreId);
    if (!list) return null;
    const visibleRevisions = list.filter((r) => r.detailReadySeq <= this.clock);
    if (visibleRevisions.length === 0) return null;
    const shown = visibleRevisions[visibleRevisions.length - 1];
    const latest = list[list.length - 1];
    const pendingNewer = latest.publishedSeq > shown.publishedSeq;

    const history = visibleRevisions
      .map(
        (r) => `    <li class="revision" data-revision="${r.revision}" data-published-seq="${r.publishedSeq}">
      <dl class="identification" data-revision="${r.revision}" data-published-seq="${r.publishedSeq}">
        <dt>岩性分类</dt><dd class="classification">${r.classification}</dd>
        <dt>所属地层</dt><dd class="formation">${r.formation}</dd>
        <dt>井段(米)</dt><dd class="depth">${r.depthTop} - ${r.depthBottom}</dd>
        <dt>核实人</dt><dd class="verifier">${r.verifier}</dd>
        <dt>发布时间</dt><dd class="published-at">${r.publishedAt}</dd>
        <dt>鉴定说明</dt><dd class="summary">${r.summary}</dd>
      </dl>
    </li>`,
      )
      .join("\n");

    return `<!doctype html>
<html lang="zh-CN">
<head><meta charset="utf-8"><title>岩芯 ${coreId} 详情</title></head>
<body>
  <h1>岩芯 ${coreId}</h1>
  <!-- core-id: ${coreId} -->
  <!-- current-revision: ${shown.revision} -->
  <!-- current-published-seq: ${shown.publishedSeq} -->
  <section class="current">
  <dl class="identification current-identification" data-revision="${shown.revision}" data-published-seq="${shown.publishedSeq}">
    <dt>岩性分类</dt><dd class="classification">${shown.classification}</dd>
    <dt>所属地层</dt><dd class="formation">${shown.formation}</dd>
    <dt>井段(米)</dt><dd class="depth">${shown.depthTop} - ${shown.depthBottom}</dd>
    <dt>核实人</dt><dd class="verifier">${shown.verifier}</dd>
    <dt>发布时间</dt><dd class="published-at">${shown.publishedAt}</dd>
    <dt>鉴定说明</dt><dd class="summary">${shown.summary}</dd>
  </dl>
  <p class="status" data-pending-newer="${pendingNewer ? "1" : "0"}">${
      pendingNewer ? "新修订已公布，详情页更新中，请稍后复查" : "当前展示最新核实修订"
    }</p>
  </section>
  <section class="history">
    <h2>修订历史（已生效 ${visibleRevisions.length} 条）</h2>
    <ul class="revisions">
${history}
    </ul>
  </section>
</body>
</html>`;
  }

  createServer(): http.Server {
    return http.createServer((req, res) => {
      const url = new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost"}`);
      const send = (status: number, body: string, type: string, extra?: Record<string, string>) => {
        res.writeHead(status, { "Content-Type": `${type}; charset=utf-8`, ...extra });
        res.end(body);
      };

      // 管理接口（离线验证用）：不受故障注入影响、不计入采集在途
      if (url.pathname.startsWith("/__admin/")) {
        if (url.pathname === "/__admin/advance-clock") {
          const step = Number(url.searchParams.get("step") ?? "6") || 6;
          return send(
            200,
            JSON.stringify({ clock: this.advanceClock(step), totalPublished: this.totalPublished }),
            "application/json",
          );
        }
        if (url.pathname === "/__admin/reset-clock") {
          this.clock = this.initialClock;
          return send(
            200,
            JSON.stringify({ clock: this.clock, totalPublished: this.totalPublished }),
            "application/json",
          );
        }
        if (url.pathname === "/__admin/force-503") {
          const count = Number(url.searchParams.get("count") ?? "1") || 1;
          const retryAfter = Number(url.searchParams.get("retryAfter") ?? "1") || 1;
          this.forceNext503(count, retryAfter);
          return send(
            200,
            JSON.stringify({ forced503Remaining: this.forced503Remaining, retryAfter }),
            "application/json",
          );
        }
        if (url.pathname === "/__admin/state") {
          return send(
            200,
            JSON.stringify({
              clock: this.clock,
              totalPublished: this.totalPublished,
              inflightNow: this.inflightNow,
              inflightPeak: this.inflightPeak,
              expectedFinalState: this.expectedFinalState,
            }),
            "application/json",
          );
        }
        return send(404, "未找到管理接口", "text/plain");
      }

      // 采集类请求：计入在途观测，再做故障注入
      this.inflightNow += 1;
      if (this.inflightNow > this.inflightPeak) this.inflightPeak = this.inflightNow;
      const done = (): void => {
        this.inflightNow -= 1;
      };
      res.on("finish", done);
      res.on("close", done);

      const fault = this.transientFault();
      if (fault) {
        if (fault.status === 503) {
          return send(503, `服务临时繁忙，请 ${fault.retryAfter} 秒后重试`, "text/plain", {
            "Retry-After": String(fault.retryAfter),
          });
        }
        return send(500, "站点内部临时错误", "text/plain");
      }

      if (url.pathname === "/catalog" || url.pathname === "/") {
        const page = Math.max(1, Number(url.searchParams.get("page") ?? "1") || 1);
        const pages = this.catalogPages();
        if (page > pages.length) return send(404, "目录页不存在", "text/plain");
        return send(200, this.renderCatalog(page), "text/html");
      }

      if (url.pathname === "/corrections") {
        const page = Math.max(1, Number(url.searchParams.get("page") ?? "1") || 1);
        const { totalPages } = this.correctionsPage(page);
        if (page > totalPages) return send(404, "更正列表页不存在", "text/plain");
        return send(200, this.renderCorrections(page), "application/json");
      }

      const m = url.pathname.match(/^\/core\/([A-Za-z0-9\-]+)$/);
      if (m) {
        const html = this.renderDetail(m[1]);
        if (html === null) return send(404, "岩芯不存在", "text/plain");
        return send(200, html, "text/html");
      }

      send(404, "未找到", "text/plain");
    });
  }

  /** 测试/验证辅助：返回数据集中所有修订的期望终态。 */
  get expectedFinalState(): { coreId: string; latestRevision: number; totalRevisions: number }[] {
    return [...this.cores.entries()]
      .map(([coreId, list]) => ({
        coreId,
        latestRevision: list[list.length - 1].revision,
        totalRevisions: list.length,
      }))
      .sort((a, b) => a.coreId.localeCompare(b.coreId));
  }
}

// 直接启动：npm run mock
const invokedPath = process.argv[1] ?? "";
if (invokedPath.endsWith("site.ts") || invokedPath.endsWith("site.js")) {
  const site = new MockArchiveSite();
  const server = site.createServer();
  server.listen(config.mockPort, "127.0.0.1", () => {
    console.log(
      `[mock] 离线模拟馆藏站点已启动: http://127.0.0.1:${config.mockPort} ` +
        `(岩芯 ${NUM_CORES} 个，已发布修订 ${site.totalPublished} 条)`,
    );
  });
}
