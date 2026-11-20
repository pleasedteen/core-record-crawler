/**
 * 离线模拟馆藏站点。
 *
 * 特性（对应需求）：
 *  - 分页 HTML 馆藏目录：/catalog?page=N，含重复链接，页头/页脚标明总页数
 *  - JSON 更正列表：/corrections?page=N，按发布顺序翻页，每条占一个全局发布序号
 *    * PUBLISH：发布/重发某岩芯某修订（撤回后允许沿用原修订号重发，issue 区分）
 *    * WITHDRAW：撤回公告，同样占用发布序号
 *  - 岩芯详情页：/core/:id，同一岩芯多个修订共用同一地址
 *  - 修订发布后，详情页可能暂时仍显示旧修订（详情延迟生效）
 *  - 撤回后详情页切回上一仍有效修订；重发核实前详情仍展示旧内容，核实后才切换
 *  - 临时拒绝：503 + Retry-After，以及随机 500
 *
 * 数据在站点启动时生成并保持稳定，保证可重复离线验证。
 */
import http from "node:http";
import { config } from "../config.js";
import { hashSeed, mulberry32, pick } from "../util/rng.js";

export type RevisionState = "ACTIVE" | "WITHDRAWN";
export type EventKind = "PUBLISH" | "WITHDRAW";

export interface CoreRevision {
  coreId: string;
  revision: number;
  /** 同修订号的发布批次：原始发布为 1，撤回后沿用原修订号重发递增。 */
  issue: number;
  publishedAt: string; // ISO 时间（更正列表发布顺序键）
  publishedSeq: number; // 本次发布（或重发）所占的全局发布序号
  detailReadySeq: number; // 详情页从哪个发布序号起展示该次发布
  /** 撤回该次发布的公告序号；null 表示当前仍有效。 */
  withdrawnSeq: number | null;
  state: RevisionState;
  classification: string;
  formation: string;
  depthTop: number;
  depthBottom: number;
  verifier: string;
  summary: string;
}

export interface CorrectionEvent {
  kind: EventKind;
  publishedSeq: number;
  publishedAt: string;
  coreId: string;
  /** PUBLISH：本次发布的修订号；WITHDRAW：被撤回的修订号。 */
  revision: number;
  /** PUBLISH：发布批次（1=原始发布，>1=同号重发）；WITHDRAW 为 null。 */
  issue: number | null;
  /** WITHDRAW：被撤回的发布批次；PUBLISH 为 null。 */
  withdrawnIssue: number | null;
  detailUrl: string;
  note: string;
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

function isoTime(day: string, hour: number, minute: number): string {
  return day.replace("T08:00:00.000Z", `T${pad(hour, 2)}:${pad(minute, 2)}:00.000Z`);
}

function buildDataset(): {
  cores: Map<string, CoreRevision[]>;
  events: CorrectionEvent[];
} {
  const rng = mulberry32(20261118);
  const cores = new Map<string, CoreRevision[]>();
  const events: CorrectionEvent[] = [];
  let seq = 0;
  let sameDay = 0;

  const pushPublish = (
    coreId: string,
    revision: number,
    issue: number,
    publishedAt: string,
    detailReadySeq: number,
    payload: Omit<
      CoreRevision,
      "coreId" | "revision" | "issue" | "publishedAt" | "publishedSeq" | "detailReadySeq" | "withdrawnSeq" | "state"
    >,
  ): CoreRevision => {
    seq += 1;
    const rev: CoreRevision = {
      coreId,
      revision,
      issue,
      publishedAt,
      publishedSeq: seq,
      detailReadySeq,
      withdrawnSeq: null,
      state: "ACTIVE",
      ...payload,
    };
    const list = cores.get(coreId) ?? [];
    list.push(rev);
    cores.set(coreId, list);
    events.push({
      kind: "PUBLISH",
      publishedSeq: seq,
      publishedAt,
      coreId,
      revision,
      issue,
      withdrawnIssue: null,
      detailUrl: `/core/${coreId}`,
      note: revision === 1 ? "初始鉴定" : issue > 1 ? `撤回后沿用 r${revision} 重发` : "鉴定更正",
    });
    return rev;
  };

  const pushWithdraw = (coreId: string, revision: number, issue: number, publishedAt: string): void => {
    seq += 1;
    const target = cores.get(coreId)?.find((r) => r.revision === revision && r.issue === issue);
    if (!target) throw new Error(`数据集构造失败：找不到 ${coreId} r${revision}#${issue}`);
    target.state = "WITHDRAWN";
    target.withdrawnSeq = seq;
    events.push({
      kind: "WITHDRAW",
      publishedSeq: seq,
      publishedAt,
      coreId,
      revision,
      issue: null,
      withdrawnIssue: issue,
      detailUrl: `/core/${coreId}`,
      note: `撤回公告：撤回 ${coreId} r${revision}（第 ${issue} 次发布）`,
    });
  };

  // 1) 每个岩芯的初始鉴定（发布序号 1..N）
  for (let i = 1; i <= NUM_CORES; i++) {
    const coreId = `RC-${pad(i, 4)}`;
    const day = 1 + Math.floor(rng() * 20);
    const depthTop = Math.round(rng() * 500);
    pushPublish(
      coreId,
      1,
      1,
      `2026-10-${pad(day, 2)}T08:00:00.000Z`,
      0, // 占位，事件全部生成后统一归一化
      {
        classification: pick(rng, CLASSIFICATIONS),
        formation: pick(rng, FORMATIONS),
        depthTop,
        depthBottom: depthTop + 20 + Math.floor(rng() * 180),
        verifier: pick(rng, VERIFIERS),
        summary: `岩芯 ${coreId} 初始岩性鉴定`,
      },
    );
  }

  // 2) 常规更正：部分岩芯有 1~2 个后续修订；部分“当天更正”；详情延迟生效
  for (const [coreId, list] of cores) {
    const extra = rng() < 0.55 ? 1 : rng() < 0.35 ? 2 : 0;
    for (let k = 1; k <= extra; k++) {
      const prev = list[list.length - 1];
      const sameDayCorrection = rng() < 0.35;
      let publishedAt: string;
      if (sameDayCorrection && sameDay < 6) {
        publishedAt = isoTime(prev.publishedAt, 10 + k, 15);
        sameDay += 1;
      } else {
        const d = new Date(prev.publishedAt);
        d.setUTCDate(d.getUTCDate() + 1 + Math.floor(rng() * 4));
        d.setUTCHours(9, 30, 0, 0);
        publishedAt = d.toISOString();
      }
      // 详情页滞后 0~3 个发布序号才更新（列表已公布、详情仍旧修订）
      const lag = rng() < 0.45 ? 1 + Math.floor(rng() * 3) : 0;
      pushPublish(
        coreId,
        prev.revision + 1,
        1,
        publishedAt,
        0,
        {
          classification: pick(rng, CLASSIFICATIONS),
          formation: prev.formation,
          depthTop: prev.depthTop,
          depthBottom: prev.depthBottom,
          verifier: pick(rng, VERIFIERS),
          summary: `岩芯 ${coreId} 第 ${prev.revision + 1} 次修订（更正前次鉴定）`,
        },
      ).detailReadySeq = seq + lag;
    }
  }

  // 3) 静态撤回场景（确定性，重启后数据集一致）
  // 3a) RC-0007：撤回 r2 后沿用原修订号重发（r2#2），重发详情滞后，制造“回退→核实后切换”
  {
    const coreId = "RC-0007";
    const list = cores.get(coreId)!;
    const r2 = list.find((r) => r.revision === 2);
    if (r2) {
      const wd = new Date(r2.publishedAt);
      wd.setUTCDate(wd.getUTCDate() + 2);
      wd.setUTCHours(11, 0, 0, 0);
      pushWithdraw(coreId, 2, 1, wd.toISOString());
      const rd = new Date(wd);
      rd.setUTCDate(rd.getUTCDate() + 1);
      rd.setUTCHours(9, 0, 0, 0);
      pushPublish(
        coreId,
        2,
        2,
        rd.toISOString(),
        seq + 4, // 详情滞后 4 个发布序号，重发核实前详情仍展示 r1
        {
          classification: pick(rng, CLASSIFICATIONS),
          formation: r2.formation,
          depthTop: r2.depthTop,
          depthBottom: r2.depthBottom,
          verifier: pick(rng, VERIFIERS),
          summary: `岩芯 ${coreId} 第 2 次修订（撤回后沿用原修订号重发）`,
        },
      );
    }
  }

  // 3b) RC-0013：撤回其最新有效发布且不重发（多修订则回退到旧版本；仅初始鉴定则查询 404）
  {
    const coreId = "RC-0013";
    const list = cores.get(coreId)!;
    const target = list[list.length - 1];
    const wd = new Date(target.publishedAt);
    wd.setUTCDate(wd.getUTCDate() + 3);
    wd.setUTCHours(14, 30, 0, 0);
    pushWithdraw(coreId, target.revision, target.issue, wd.toISOString());
  }

  // 3c) RC-0021：撤回较新修订但保留旧版本，查询回退到仍有效的最新已核实版本
  {
    const coreId = "RC-0021";
    const list = cores.get(coreId)!;
    const newer = list[list.length - 1];
    const older = list[list.length - 2];
    if (newer && older) {
      const wd = new Date(newer.publishedAt);
      wd.setUTCDate(wd.getUTCDate() + 2);
      wd.setUTCHours(16, 0, 0, 0);
      pushWithdraw(coreId, newer.revision, newer.issue, wd.toISOString());
    }
  }

  // 按发布时间排序每个岩芯的修订链，并归一化详情生效序号
  const globalMaxSeq = seq;
  for (const list of cores.values()) {
    list.sort((a, b) => a.publishedSeq - b.publishedSeq);
    for (const rev of list) {
      rev.detailReadySeq = rev.detailReadySeq <= 0 ? rev.publishedSeq : Math.min(rev.detailReadySeq, globalMaxSeq);
    }
  }
  events.sort((a, b) => a.publishedSeq - b.publishedSeq);
  return { cores, events };
}

export class MockArchiveSite {
  private cores: Map<string, CoreRevision[]>;
  private allRevisions: CoreRevision[];
  private events: CorrectionEvent[];
  private rng = mulberry32(987654321);
  private requestCount = 0;
  readonly totalPublished: number;
  /** 被管理端按住、暂不在详情页生效的重发（key: coreId/revision#issue）。 */
  private heldReissues = new Set<string>();
  /**
   * 站点时钟（发布序号口径）：更正列表一次性全部公布，
   * 但详情页只展示 detailReadySeq <= clock 的修订，模拟“列表已更正、详情仍旧修订”。
   */
  private clock: number;
  readonly initialClock: number;

  constructor() {
    const dataset = buildDataset();
    this.cores = dataset.cores;
    this.events = dataset.events;
    this.allRevisions = [...this.cores.values()].flat().sort((a, b) => a.publishedSeq - b.publishedSeq);
    this.totalPublished = this.events.length;
    // 初始只“生效”前 20 个发布序号对应的详情，其余陆续生效
    this.initialClock = 20;
    this.clock = this.initialClock;
  }

  /** 站点当前“已发布”的最大序号（更正列表口径，含撤回公告）。 */
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
   */
  private transientFault(): { status: number; retryAfter?: number } | null {
    this.requestCount += 1;
    const r = this.rng();
    if (r < 0.12) {
      const wait = 0.4 + this.rng() * 0.8;
      return { status: 503, retryAfter: Number(wait.toFixed(2)) };
    }
    if (r < 0.18) return { status: 500 };
    return null;
  }

  /** 详情页当前可见的修订：已到生效时钟、未撤回、未被管理端按住。 */
  private visibleRevisions(coreId: string): CoreRevision[] {
    return (this.cores.get(coreId) ?? []).filter(
      (r) =>
        r.detailReadySeq <= this.clock &&
        r.state === "ACTIVE" &&
        !this.heldReissues.has(`${coreId}/${r.revision}#${r.issue}`),
    );
  }

  private catalogPages(): CoreRevision[][] {
    // 目录只展示每个岩芯“当前最新一次发布”的链接，分页并刻意制造重复链接
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

  private correctionsPage(page: number): { events: CorrectionEvent[]; page: number; totalPages: number } {
    const totalPages = Math.max(1, Math.ceil(this.events.length / CORRECTION_PAGE_SIZE));
    const start = (page - 1) * CORRECTION_PAGE_SIZE;
    return { events: this.events.slice(start, start + CORRECTION_PAGE_SIZE), page, totalPages };
  }

  private renderCorrections(page: number): string {
    const { events, totalPages } = this.correctionsPage(page);
    const payload = {
      page,
      totalPages,
      pageSize: CORRECTION_PAGE_SIZE,
      totalPublished: this.totalPublished,
      items: events.map((e) => ({
        kind: e.kind,
        coreId: e.coreId,
        revision: e.revision,
        issue: e.issue,
        withdrawnIssue: e.withdrawnIssue,
        publishedSeq: e.publishedSeq,
        publishedAt: e.publishedAt,
        detailUrl: e.detailUrl,
        note: e.note,
      })),
    };
    return JSON.stringify(payload, null, 2);
  }

  /**
   * 详情页：
   *  - 主区块展示“当前生效的最新修订”（撤回后回退到上一仍有效修订；重发核实前仍旧内容）；
   *  - 修订历史列出所有当前可见（未撤回、已生效）的发布，同一修订号只展示最新有效批次；
   *  - 尚未到 detailReadySeq、被撤回或被按住的发布不出现在页面上，等待复查。
   */
  private renderDetail(coreId: string): string | null {
    const list = this.cores.get(coreId);
    if (!list) return null;
    const visible = this.visibleRevisions(coreId);
    if (visible.length === 0) return null;
    // 同一修订号只保留最新有效批次（旧批次已被撤回）
    const byRevision = new Map<number, CoreRevision>();
    for (const r of visible) byRevision.set(r.revision, r);
    const visibleRevisions = [...byRevision.values()].sort((a, b) => a.revision - b.revision);
    const shown = visibleRevisions[visibleRevisions.length - 1];
    const latest = list[list.length - 1];
    const pendingNewer = latest.publishedSeq > shown.publishedSeq;

    const history = visibleRevisions
      .map(
        (r) => `    <li class="revision" data-revision="${r.revision}" data-issue="${r.issue}" data-published-seq="${r.publishedSeq}">
      <dl class="identification" data-revision="${r.revision}" data-issue="${r.issue}" data-published-seq="${r.publishedSeq}">
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
  <!-- current-issue: ${shown.issue} -->
  <!-- current-published-seq: ${shown.publishedSeq} -->
  <section class="current">
  <dl class="identification current-identification" data-revision="${shown.revision}" data-issue="${shown.issue}" data-published-seq="${shown.publishedSeq}">
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
  <h2>修订历史（当前有效 ${visibleRevisions.length} 条）</h2>
  <ul class="revisions">
${history}
    </ul>
  </section>
</body>
</html>`;
  }

  // ---------- 管理端：运行时撤回 / 同号重发（离线验证用） ----------

  private nextSeq(): number {
    return this.events.length + 1;
  }

  /** 运行时撤回某岩芯某修订的当前有效批次。 */
  private adminWithdraw(coreId: string, revision: number): CorrectionEvent {
    const list = this.cores.get(coreId);
    if (!list) throw new Error("岩芯不存在");
    let target: CoreRevision | undefined;
    for (let i = list.length - 1; i >= 0; i--) {
      if (list[i].revision === revision && list[i].state === "ACTIVE") {
        target = list[i];
        break;
      }
    }
    if (!target) throw new Error(`没有可撤回的有效发布：${coreId} r${revision}`);
    const seq = this.nextSeq();
    target.state = "WITHDRAWN";
    target.withdrawnSeq = seq;
    const event: CorrectionEvent = {
      kind: "WITHDRAW",
      publishedSeq: seq,
      publishedAt: new Date().toISOString(),
      coreId,
      revision,
      issue: null,
      withdrawnIssue: target.issue,
      detailUrl: `/core/${coreId}`,
      note: `撤回公告：撤回 ${coreId} r${revision}（第 ${target.issue} 次发布）`,
    };
    this.events.push(event);
    return event;
  }

  /** 运行时沿用原修订号重发；默认按住详情（held=1），核实前详情仍旧内容。 */
  private adminReissue(coreId: string, revision: number, held: boolean): { event: CorrectionEvent; held: boolean } {
    const list = this.cores.get(coreId);
    if (!list) throw new Error("岩芯不存在");
    const prior = list.filter((r) => r.revision === revision);
    const last = prior[prior.length - 1];
    if (!last) throw new Error(`原修订不存在，无法同号重发：${coreId} r${revision}`);
    const seq = this.nextSeq();
    const issue = prior.length + 1;
    const rev: CoreRevision = {
      coreId,
      revision,
      issue,
      publishedAt: new Date().toISOString(),
      publishedSeq: seq,
      detailReadySeq: this.totalPublished + 1, // 管理端释放后才生效
      withdrawnSeq: null,
      state: "ACTIVE",
      classification: pick(mulberry32(hashSeed(`${coreId}-${revision}-${issue}-${seq}`)), CLASSIFICATIONS),
      formation: last.formation,
      depthTop: last.depthTop,
      depthBottom: last.depthBottom,
      verifier: pick(mulberry32(hashSeed(`${coreId}-v-${issue}-${seq}`)), VERIFIERS),
      summary: `岩芯 ${coreId} 第 ${revision} 次修订（运行时撤回后沿用原修订号重发）`,
    };
    list.push(rev);
    list.sort((a, b) => a.publishedSeq - b.publishedSeq);
    this.allRevisions.push(rev);
    if (held) this.heldReissues.add(`${coreId}/${revision}#${issue}`);
    const event: CorrectionEvent = {
      kind: "PUBLISH",
      publishedSeq: seq,
      publishedAt: rev.publishedAt,
      coreId,
      revision,
      issue,
      withdrawnIssue: null,
      detailUrl: `/core/${coreId}`,
      note: `撤回后沿用 r${revision} 重发`,
    };
    this.events.push(event);
    return { event, held };
  }

  /** 释放被按住的重发，使其在详情页生效。 */
  private adminRelease(coreId: string, revision: number): { released: boolean } {
    const list = this.cores.get(coreId) ?? [];
    let released = false;
    for (const r of list) {
      const key = `${coreId}/${r.revision}#${r.issue}`;
      if (r.revision === revision && this.heldReissues.has(key)) {
        this.heldReissues.delete(key);
        r.detailReadySeq = Math.min(r.publishedSeq, this.clock || r.publishedSeq);
        released = true;
      }
    }
    return { released };
  }

  createServer(): http.Server {
    return http.createServer((req, res) => {
      const url = new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost"}`);
      const send = (status: number, body: string, type: string, extra?: Record<string, string>) => {
        res.writeHead(status, { "Content-Type": `${type}; charset=utf-8`, ...extra });
        res.end(body);
      };
      const sendJson = (status: number, payload: unknown) =>
        send(status, JSON.stringify(payload, null, 2), "application/json");

      const fault = this.transientFault();
      if (fault) {
        if (fault.status === 503) {
          send(503, `服务临时繁忙，请 ${fault.retryAfter} 秒后重试`, "text/plain", {
            "Retry-After": String(fault.retryAfter),
          });
        } else {
          send(500, "站点内部临时错误", "text/plain");
        }
        return;
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

      // 管理接口（离线验证用）：推进/重置详情生效时钟
      if (url.pathname === "/__admin/advance-clock") {
        const step = Number(url.searchParams.get("step") ?? "6") || 6;
        return sendJson(200, { clock: this.advanceClock(step), totalPublished: this.totalPublished });
      }
      if (url.pathname === "/__admin/reset-clock") {
        this.clock = this.initialClock;
        return sendJson(200, { clock: this.clock, totalPublished: this.totalPublished });
      }

      // 管理接口（离线验证用）：运行时撤回 → 同号重发（按住）→ 释放
      if (url.pathname === "/__admin/withdraw") {
        const coreId = url.searchParams.get("coreId") ?? "";
        const revision = Number(url.searchParams.get("revision") ?? "0");
        try {
          const event = this.adminWithdraw(coreId, revision);
          return sendJson(200, { ok: true, event, totalPublished: this.totalPublished });
        } catch (err) {
          return sendJson(400, { ok: false, error: (err as Error).message });
        }
      }
      if (url.pathname === "/__admin/reissue") {
        const coreId = url.searchParams.get("coreId") ?? "";
        const revision = Number(url.searchParams.get("revision") ?? "0");
        const held = (url.searchParams.get("held") ?? "1") !== "0";
        try {
          const result = this.adminReissue(coreId, revision, held);
          return sendJson(200, { ok: true, ...result, totalPublished: this.totalPublished });
        } catch (err) {
          return sendJson(400, { ok: false, error: (err as Error).message });
        }
      }
      if (url.pathname === "/__admin/release") {
        const coreId = url.searchParams.get("coreId") ?? "";
        const revision = Number(url.searchParams.get("revision") ?? "0");
        return sendJson(200, { ok: true, ...this.adminRelease(coreId, revision), totalPublished: this.totalPublished });
      }

      if (url.pathname === "/__admin/state") {
        return sendJson(200, {
          clock: this.clock,
          totalPublished: this.totalPublished,
          events: this.events.map((e) => ({
            kind: e.kind,
            publishedSeq: e.publishedSeq,
            coreId: e.coreId,
            revision: e.revision,
            issue: e.issue,
            withdrawnIssue: e.withdrawnIssue,
          })),
          expectedFinalState: this.expectedFinalState,
        });
      }

      // 管理接口（离线验证用）：查看某岩芯的完整修订/批次链
      const coreAdmin = url.pathname.match(/^\/__admin\/core\/([A-Za-z0-9\-]+)$/);
      if (coreAdmin) {
        const list = this.cores.get(coreAdmin[1]);
        if (!list) return sendJson(404, { ok: false, error: "岩芯不存在" });
        return sendJson(200, {
          ok: true,
          coreId: coreAdmin[1],
          clock: this.clock,
          revisions: list.map((r) => ({
            revision: r.revision,
            issue: r.issue,
            state: r.state,
            publishedSeq: r.publishedSeq,
            detailReadySeq: r.detailReadySeq,
            withdrawnSeq: r.withdrawnSeq,
            held: this.heldReissues.has(`${r.coreId}/${r.revision}#${r.issue}`),
          })),
        });
      }

      send(404, "未找到", "text/plain");
    });
  }

  /** 测试/验证辅助：返回数据集中每个岩芯的期望终态（按当前仍有效修订计算）。 */
  get expectedFinalState(): {
    coreId: string;
    latestRevision: number;
    latestIssue: number;
    latestPublishedSeq: number;
    totalRevisions: number;
    withdrawnRevisions: number;
  }[] {
    return [...this.cores.entries()]
      .map(([coreId, list]) => {
        const active = list.filter((r) => r.state === "ACTIVE");
        const latest = active[active.length - 1] ?? list[list.length - 1];
        return {
          coreId,
          latestRevision: latest.revision,
          latestIssue: latest.issue,
          latestPublishedSeq: latest.publishedSeq,
          totalRevisions: new Set(list.map((r) => r.revision)).size,
          withdrawnRevisions: new Set(
            list.filter((r) => r.state === "WITHDRAWN").map((r) => r.revision),
          ).size,
        };
      })
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
        `(岩芯 ${NUM_CORES} 个，更正事件 ${site.totalPublished} 条，含撤回公告)`,
    );
  });
}
