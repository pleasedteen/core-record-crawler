/**
 * 模拟站点响应解析：目录 HTML、更正 JSON、详情 HTML。
 * 解析器保持宽容：重复链接自然产生重复条目，由上层做去重合并。
 */

export interface CatalogLink {
  detailUrl: string;
  coreId: string;
  revision: number;
  updatedAt: string;
}

export interface CatalogPage {
  page: number;
  totalPages: number;
  totalCores: number;
  links: CatalogLink[];
}

export function parseCatalog(html: string, page: number): CatalogPage {
  const pager = html.match(/data-page="(\d+)"\s+data-total-pages="(\d+)"\s+data-total-cores="(\d+)"/);
  const totalPages = pager ? Number(pager[2]) : page;
  const totalCores = pager ? pager[3] : 0;
  const links: CatalogLink[] = [];
  const liRe = /<li[^>]*>([\s\S]*?)<\/li>/g;
  let m: RegExpExecArray | null;
  while ((m = liRe.exec(html)) !== null) {
    const block = m[1];
    const a = block.match(/<a href="(\/core\/([^"]+))">[^<]*<\/a>/);
    const rev = block.match(/修订 r(\d+)/);
    const upd = block.match(/更新于 ([^<]+?)\s*</);
    if (a && rev) {
      links.push({
        detailUrl: a[1],
        coreId: a[2],
        revision: Number(rev[1]),
        updatedAt: upd ? upd[1].trim() : "",
      });
    }
  }
  return { page, totalPages, totalCores: Number(totalCores), links };
}

export type CorrectionKind = "PUBLISH" | "WITHDRAW";

export interface CorrectionItem {
  kind: CorrectionKind;
  coreId: string;
  revision: number;
  /** PUBLISH 的发布批次（1=原始发布，>1=同号重发）；WITHDRAW 为 null。 */
  issue: number | null;
  /** WITHDRAW 被撤回的发布批次；PUBLISH 为 null。 */
  withdrawnIssue: number | null;
  publishedSeq: number;
  publishedAt: string;
  detailUrl: string;
  note: string;
}

export interface CorrectionsPage {
  page: number;
  totalPages: number;
  pageSize: number;
  totalPublished: number;
  items: CorrectionItem[];
}

export function parseCorrections(json: string): CorrectionsPage {
  const data = JSON.parse(json) as {
    page: number;
    totalPages: number;
    pageSize: number;
    totalPublished: number;
    items: Array<{
      kind?: CorrectionKind;
      coreId: string;
      revision: number;
      issue?: number | null;
      withdrawnIssue?: number | null;
      publishedSeq: number;
      publishedAt: string;
      detailUrl: string;
      note: string;
    }>;
  };
  return {
    page: data.page,
    totalPages: data.totalPages,
    pageSize: data.pageSize,
    totalPublished: data.totalPublished,
    items: data.items.map((it) => ({
      // 兼容旧格式：没有 kind 字段时一律视为发布
      kind: it.kind === "WITHDRAW" ? "WITHDRAW" : "PUBLISH",
      coreId: it.coreId,
      revision: it.revision,
      issue: it.issue ?? null,
      withdrawnIssue: it.withdrawnIssue ?? null,
      publishedSeq: it.publishedSeq,
      publishedAt: it.publishedAt,
      detailUrl: it.detailUrl,
      note: it.note,
    })),
  };
}

export interface CoreRevisionDetail {
  revision: number;
  /** 发布批次：同一修订号重发后递增；旧页面无该属性时按 1 处理。 */
  issue: number;
  publishedSeq: number;
  classification: string;
  formation: string;
  depthTop: number;
  depthBottom: number;
  verifier: string;
  publishedAt: string;
  summary: string;
}

export interface CoreDetail {
  coreId: string;
  /** 详情主区块当前生效的修订号（可能落后于已发布的最新修订）。 */
  currentRevision: number;
  /** 详情主区块当前生效的发布批次。 */
  currentIssue: number;
  currentPublishedSeq: number;
  pendingNewer: boolean;
  /** 详情页上当前生效的全部发布（同一详情地址承载的多个修订）。 */
  history: CoreRevisionDetail[];
}

function field(block: string, cls: string): string {
  const m = block.match(new RegExp(`<dd class="${cls}">([^<]*)</dd>`));
  return m ? m[1].trim() : "";
}

function parseRevisionBlock(block: string): CoreRevisionDetail | null {
  const revMatch = block.match(/data-revision="(\d+)"[^>]*data-published-seq="(\d+)"/);
  if (!revMatch) return null;
  const issueMatch = block.match(/data-issue="(\d+)"/);
  const depth = field(block, "depth").match(/(-?\d+(?:\.\d+)?)\s*-\s*(-?\d+(?:\.\d+)?)/);
  return {
    revision: Number(revMatch[1]),
    issue: issueMatch ? Number(issueMatch[1]) : 1,
    publishedSeq: Number(revMatch[2]),
    classification: field(block, "classification"),
    formation: field(block, "formation"),
    depthTop: depth ? Number(depth[1]) : NaN,
    depthBottom: depth ? Number(depth[2]) : NaN,
    verifier: field(block, "verifier"),
    publishedAt: field(block, "published-at"),
    summary: field(block, "summary"),
  };
}

export function parseDetail(html: string, expectedCoreId: string): CoreDetail {
  const coreId = html.match(/<!-- core-id: ([^ ]+) -->/)?.[1]
    ?? html.match(/<h1>岩芯 ([^ ]+) 详情<\/h1>/)?.[1];
  const currentRevision = Number(html.match(/<!-- current-revision: (\d+) -->/)?.[1] ?? "NaN");
  const currentIssue = Number(html.match(/<!-- current-issue: (\d+) -->/)?.[1] ?? "1");
  const currentPublishedSeq = Number(
    html.match(/<!-- current-published-seq: (\d+) -->/)?.[1] ?? "NaN",
  );
  if (!coreId || coreId !== expectedCoreId || !Number.isFinite(currentRevision)) {
    throw new Error(`详情页解析失败或岩芯编号不符: 期望 ${expectedCoreId}`);
  }
  const history: CoreRevisionDetail[] = [];
  const dlRe = /<dl class="identification[^"]*"[^>]*>[\s\S]*?<\/dl>/g;
  let m: RegExpExecArray | null;
  while ((m = dlRe.exec(html)) !== null) {
    const rev = parseRevisionBlock(m[0]);
    if (rev && !history.some((h) => h.revision === rev.revision && h.issue === rev.issue)) {
      history.push(rev);
    }
  }
  history.sort((a, b) => a.revision - b.revision || a.issue - b.issue);
  const pendingNewer = /data-pending-newer="1"/.test(html);
  return {
    coreId,
    currentRevision,
    currentIssue: Number.isFinite(currentIssue) ? currentIssue : 1,
    currentPublishedSeq: Number.isFinite(currentPublishedSeq) ? currentPublishedSeq : 0,
    pendingNewer,
    history,
  };
}
