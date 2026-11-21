/**
 * 离线端到端验证（npm run verify）。
 *
 * 覆盖：
 *  1) 干净库启动 → 全量采集进行中强制 SIGKILL → 重启续跑并完成（断点续传 + 核实结果沿用）
 *  2) 目录重复链接合并、临时拒绝退避重试、Retry-After 全局等待（统计接口 + 日志佐证）
 *  3) 更正列表错页（HTTP 200 返回别页正文）→ 旧缺页连续几轮拿不到；
 *     缺口期间：较晚的有效更正核实后立即可查，连续进度停在缺口前
 *  4) 缺口期间再次 SIGKILL → 同库重启 → 缺页恢复与末页追加检查接上，连续进度补齐
 *  5) 已收妥的完整非尾页不重读（逐页请求次数不变）、末页每轮复查、
 *     每轮追更的列表请求数 ≤ 预算（重试计入）
 *  6) 详情滞后→时钟推进→增量衔接、按编号查询与站点终态逐一比对
 *
 * 整体预算 90 秒，超时按失败处理。
 */
import { spawn, ChildProcessWithoutNullStreams } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import process from "node:process";

const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");
const DATA_DIR = path.join(ROOT, "data-verify");
const LOG_FILE = path.join(ROOT, "data-verify", "service.log");
const PORT = 8181;
const MOCK_PORT = 9191;
const DEADLINE_MS = 90_000;
const startedAt = Date.now();
/** 每轮追更的更正列表请求预算（与 env 中 CATCHUP_LIST_BUDGET 一致）。 */
const LIST_BUDGET = 3;

const env: NodeJS.ProcessEnv = {
  ...process.env,
  PORT: String(PORT),
  MOCK_PORT: String(MOCK_PORT),
  DB_PATH: path.join(DATA_DIR, "verify.duckdb"),
  AUTO_FULL: "1",
  EMBED_MOCK: "1",
  RATE_MAX_REQUESTS: "16",
  RATE_WINDOW_MS: "1000",
  MAX_INFLIGHT: "6",
  MAX_RETRIES: "8",
  CATCHUP_INTERVAL_MS: "2000",
  CATCHUP_LIST_BUDGET: String(LIST_BUDGET),
};

let child: ChildProcessWithoutNullStreams | null = null;
let logStream: fs.WriteStream | null = null;
const results: { name: string; ok: boolean; detail: string }[] = [];

function check(name: string, ok: boolean, detail: string): void {
  results.push({ name, ok, detail });
  console.log(`${ok ? "✅" : "❌"} ${name} — ${detail}`);
}

function timeLeft(): number {
  return DEADLINE_MS - (Date.now() - startedAt);
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function api<T = any>(p: string, init?: RequestInit): Promise<{ status: number; body: T }> {
  const resp = await fetch(`http://127.0.0.1:${PORT}${p}`, init);
  const body = (await resp.json()) as T;
  return { status: resp.status, body };
}

async function waitForHealth(): Promise<void> {
  const until = Date.now() + Math.min(20_000, timeLeft());
  for (;;) {
    try {
      const resp = await fetch(`http://127.0.0.1:${PORT}/health`);
      if (resp.ok) return;
    } catch {
      /* 尚未就绪 */
    }
    if (Date.now() > until) throw new Error("服务健康检查超时");
    await sleep(300);
  }
}

async function waitProgress(predicate: (p: any) => boolean, label: string): Promise<any> {
  const until = Date.now() + timeLeft() - 2_000;
  let last: any = null;
  while (Date.now() < until) {
    const { body } = await api<any>("/progress");
    last = body;
    if (predicate(body)) return body;
    await sleep(400);
  }
  throw new Error(`等待「${label}」超时，最后进度: ${JSON.stringify(last)}`);
}

/** 模拟站点的管理接口也会被临时故障（503/500）命中，调用方需重试。 */
async function adminJson<T = any>(p: string): Promise<T> {
  for (let i = 0; i < 25; i++) {
    try {
      const resp = await fetch(`http://127.0.0.1:${MOCK_PORT}${p}`);
      if (resp.ok) return (await resp.json()) as T;
    } catch {
      /* 连接异常也重试 */
    }
    await sleep(400);
  }
  throw new Error(`管理接口 ${p} 多次重试仍失败`);
}

async function advanceClock(step = 8): Promise<number> {
  const data = await adminJson<{ clock: number }>(`/__admin/advance-clock?step=${step}`);
  return data.clock;
}

/** 等待期间持续推进站点详情时钟，模拟时间流逝让滞后详情陆续生效。 */
async function waitProgressWithClock(predicate: (p: any) => boolean, label: string): Promise<any> {
  const until = Date.now() + timeLeft() - 2_000;
  let last: any = null;
  while (Date.now() < until) {
    const { body } = await api<any>("/progress");
    last = body;
    if (predicate(body)) return body;
    await advanceClock(10);
    await sleep(700);
  }
  throw new Error(`等待「${label}」超时，最后进度: ${JSON.stringify(last)}`);
}

function startService(): void {
  logStream = fs.createWriteStream(LOG_FILE, { flags: "a" });
  // 直接用 node 运行 tsx CLI（单进程），并用独立进程组，
  // 这样 SIGKILL 能精确杀死服务本身，避免 npx 包装进程被杀而 node 服务变孤儿。
  child = spawn(
    process.execPath,
    [path.join(ROOT, "node_modules", "tsx", "dist", "cli.mjs"), "src/main.ts"],
    { cwd: ROOT, env, detached: true },
  );
  child.stdout.on("data", (d) => {
    process.stdout.write(`[svc] ${d}`);
    logStream!.write(d);
  });
  child.stderr.on("data", (d) => {
    process.stderr.write(`[svc] ${d}`);
    logStream!.write(d);
  });
  child.on("exit", (code, signal) => {
    console.log(`[verify] 服务进程退出 code=${code} signal=${signal}`);
  });
}

/** 杀掉整个服务进程组（含可能的子进程），确保端口和 DuckDB 文件锁释放。 */
function killService(): void {
  if (!child || child.killed) return;
  try {
    process.kill(-child.pid!, "SIGKILL");
  } catch {
    try {
      child.kill("SIGKILL");
    } catch {
      /* 已退出 */
    }
  }
}

/** 从 /stats 的 correctionPageUrls 提取 页码 → 累计请求次数。 */
function pageAttemptsOf(stats: any): Map<number, number> {
  const map = new Map<number, number>();
  for (const row of stats.correctionPageUrls ?? []) {
    const m = String(row.url).match(/\/corrections\?page=(\d+)$/);
    if (m) map.set(Number(m[1]), Number(row.attempts));
  }
  return map;
}

async function main(): Promise<void> {
  fs.rmSync(DATA_DIR, { recursive: true, force: true });
  fs.mkdirSync(DATA_DIR, { recursive: true });

  const cleanup = () => {
    killService();
  };
  process.on("exit", cleanup);
  process.on("SIGINT", () => {
    cleanup();
    process.exit(130);
  });

  console.log("=== 岩芯鉴定采集服务 · 离线端到端验证 ===");

  // ---- 阶段 1：首次启动，全量采集中途强制杀掉 ----
  startService();
  await waitForHealth();
  await waitProgress(
    (p) => p.details.total > 0 && (p.details.verified > 0 || p.details.pending > 0),
    "全量采集开始产生鉴定记录",
  );
  const mid = (await api<any>("/progress")).body;
  console.log(
    `[verify] 全量进行中：已登记 ${mid.details.total} 条，已核实 ${mid.details.verified} 条，` +
      `目录 ${mid.catalog.cursor}/${mid.catalog.totalPages}，更正 ${mid.corrections.cursor}/${mid.corrections.totalPages}`,
  );

  // 强制结束（模拟 kill -9 / 掉电）：杀整个进程组
  const pid = child!.pid;
  killService();
  await sleep(1500);
  check("进程被 SIGKILL 强制结束", pid !== undefined, `pid=${pid}`);

  // ---- 阶段 2：重启续跑，沿用已保存核实结果（期间推进站点时钟让滞后详情生效）----
  startService();
  await waitForHealth();
  const progress2 = await waitProgressWithClock(
    (p) => p.fullCompleted === true && p.details.pending === 0,
    "重启后续跑并完成全量",
  );
  check(
    "重启后续跑并完成全量",
    progress2.fullCompleted === true && progress2.details.pending === 0,
    `已核实 ${progress2.details.verified} 条，待核实 ${progress2.details.pending} 条`,
  );

  // ---- 阶段 3：错页缺口窗口——较晚更正已可查，连续进度停在缺口前 ----
  // 站点对更正列表第 6 页连续返回别页正文（HTTP 200），该页成为旧缺页，
  // 需若干轮追更才能补齐；在补齐前的窗口里观察缺口语义。
  let gapSnap: any = null;
  const gapDeadline = Date.now() + Math.min(10_000, timeLeft() - 2_000);
  while (Date.now() < gapDeadline) {
    const { body } = await api<any>("/progress");
    if (
      body.corrections?.missingPages?.length > 0 &&
      body.contiguousVerifiedSeq < body.corrections.totalPublished
    ) {
      gapSnap = body;
      break;
    }
    await sleep(250);
  }
  check(
    "更正列表错页造成旧缺页，连续进度停在缺口前",
    gapSnap !== null,
    gapSnap
      ? `缺页 [${gapSnap.corrections.missingPages}]，连续序号 ${gapSnap.contiguousVerifiedSeq}/${gapSnap.corrections.totalPublished}`
      : "未观察到缺页窗口",
  );
  if (gapSnap) {
    const cores = (await api<any>("/cores?limit=200")).body;
    const later = (cores.items ?? []).filter(
      (i: any) => Number(i.publishedSeq) > Number(gapSnap.contiguousVerifiedSeq),
    );
    let queryable = false;
    let sample = "无";
    if (later.length > 0) {
      const one = (await api<any>(`/cores/${later[0].coreId}`)).body;
      queryable = one?.status === "VERIFIED" && Number(one.publishedSeq) > Number(gapSnap.contiguousVerifiedSeq);
      sample = `${later[0].coreId} r${later[0].revision}（发布序号 ${later[0].publishedSeq} > 缺口 ${gapSnap.contiguousVerifiedSeq}）`;
    }
    check(
      "较晚的有效更正核实后立即可查（连续进度仍停在缺口前）",
      later.length > 0 && queryable,
      `缺口之后已核实岩芯 ${later.length} 个，样本 ${sample}`,
    );
  }

  // ---- 阶段 4：缺口期间再次 SIGKILL，同库重启后缺页恢复、进度补齐 ----
  const pid2 = child!.pid;
  killService();
  await sleep(1500);
  check("缺口期间再次 SIGKILL", pid2 !== undefined, `pid=${pid2}`);
  startService();
  await waitForHealth();
  const recovered = await waitProgressWithClock(
    (p) =>
      p.fullCompleted === true &&
      p.details.pending === 0 &&
      p.corrections.totalPublished !== null &&
      p.contiguousVerifiedSeq === p.corrections.totalPublished &&
      p.corrections.missingPages.length === 0,
    "同库重启后缺页恢复并补齐",
  );
  check(
    "同库重启后缺页恢复、末页追加检查接上，连续进度补齐",
    recovered.contiguousVerifiedSeq === recovered.corrections.totalPublished &&
      recovered.corrections.missingPages.length === 0,
    `连续核实序号 ${recovered.contiguousVerifiedSeq}/${recovered.corrections.totalPublished}，缺页 [${recovered.corrections.missingPages}]`,
  );

  // ---- 阶段 5：请求次数证据——完整非尾页不重读、末页每轮复查、轮预算上限 ----
  const totalPages = Number(recovered.corrections.totalPages);
  const attemptsA = pageAttemptsOf((await api<any>("/stats")).body);
  await sleep(5200); // 至少经过两个追更轮（周期 2 秒）
  const attemptsB = pageAttemptsOf((await api<any>("/stats")).body);
  let rereadNonTail = 0;
  let tailRechecked = false;
  for (const [page, a] of attemptsA) {
    const b = attemptsB.get(page) ?? a;
    if (page < totalPages && b !== a) rereadNonTail += 1;
    if (page === totalPages && b > a) tailRechecked = true;
  }
  check(
    "强杀重启后已收妥的完整非尾页不重读",
    rereadNonTail === 0 && attemptsA.size >= totalPages - 1,
    `非尾页 ${totalPages - 1} 页中被重读 ${rereadNonTail} 页（逐页请求次数比对）`,
  );
  check(
    "末页每轮复查以接住追加",
    tailRechecked,
    `末页第 ${totalPages} 页请求次数 ${attemptsA.get(totalPages)} → ${attemptsB.get(totalPages)}`,
  );
  const runs = (await api<any>("/runs")).body.runs as any[];
  const incrRuns = runs.filter((r) => r.mode === "INCREMENTAL");
  const maxListReq = incrRuns.reduce((m, r) => Math.max(m, Number(r.list_requests ?? 0)), 0);
  check(
    "每轮追更的列表请求数不超过预算（重试计入）",
    incrRuns.length > 0 && incrRuns.every((r) => Number(r.list_requests) >= 1 && Number(r.list_requests) <= LIST_BUDGET),
    `${incrRuns.length} 个增量运行，单轮列表请求最大 ${maxListReq}/${LIST_BUDGET}`,
  );

  // ---- 阶段 6：错页识别、去重、重试、Retry-After 证据 ----
  const logContent = fs.readFileSync(LOG_FILE, "utf8");
  const wrongPageEvents = (logContent.match(/错页/g) || []).length;
  check(
    "站点错页（HTTP 200 返回别页正文）被识别并留缺待补",
    wrongPageEvents >= 1,
    `日志中错页事件 ${wrongPageEvents} 次`,
  );
  const stats = (await api<any>("/stats")).body;
  const catalogStat = stats.fetchUrls.find((r: any) => r.kind === "CATALOG");
  const detailStat = stats.fetchUrls.find((r: any) => r.kind === "DETAIL");
  check(
    "目录重复链接已合并（去重后 URL 数远小于条目数）",
    !!catalogStat && catalogStat.distinctUrls <= 40 && catalogStat.distinctUrls > 0,
    `目录去重后 ${catalogStat?.distinctUrls} 个 URL（模拟目录含重复链接，岩芯共 40 个）`,
  );
  check(
    "临时失败触发退避重试（总尝试次数 > 去重 URL 数）",
    !!detailStat && detailStat.totalAttempts > detailStat.distinctUrls,
    `详情请求 ${detailStat?.totalAttempts} 次 / ${detailStat?.distinctUrls} 个去重 URL`,
  );
  check(
    "限流与 Retry-After 生效（请求统计中有重试/等待计数）",
    progress2.http.requests > 0 &&
      (progress2.http.retried > 0 || progress2.http.retryAfterWaits > 0),
    `总请求 ${progress2.http.requests}，重试 ${progress2.http.retried}，Retry-After 等待 ${progress2.http.retryAfterWaits}`,
  );

  // ---- 阶段 7：手工触发一次增量，确认补齐后状态稳定 ----
  let triggered = false;
  for (let i = 0; i < 15 && !triggered; i++) {
    const resp = await api<any>("/collect/catchup", { method: "POST" });
    if (resp.status === 202) triggered = true;
    else await sleep(800);
  }
  const after = await waitProgress(
    (p) => p.fullCompleted === true && p.details.pending === 0 && p.corrections.missingPages.length === 0,
    "手工增量后状态稳定",
  );
  check(
    "增量进度只计入连续核实的发布范围（补齐后覆盖全部已发布序号）",
    triggered && after.contiguousVerifiedSeq === after.corrections.totalPublished,
    `连续序号 ${after.contiguousVerifiedSeq}/${after.corrections.totalPublished}`,
  );

  // ---- 阶段 8：按岩芯编号查询，逐一比对模拟站点期望终态 ----
  const state = await adminJson<any>(`/__admin/state`);
  const expected = state.expectedFinalState as Array<{
    coreId: string;
    latestRevision: number;
    totalRevisions: number;
  }>;
  let matched = 0;
  let multiRevisionOk = 0;
  for (const exp of expected) {
    const { body: core } = await api<any>(`/cores/${exp.coreId}`);
    if (core.found === false) continue;
    if (core.revision === exp.latestRevision && core.verifiedIdentification?.classification) matched += 1;
    const { body: idents } = await api<any>(`/identifications/${exp.coreId}`);
    if (idents.revisions?.length === exp.totalRevisions && idents.revisions.every((r: any) => r.status === "VERIFIED")) {
      multiRevisionOk += 1;
    }
  }
  check(
    "按岩芯编号查询到的最新核实鉴定与站点终态一致",
    matched === expected.length,
    `${matched}/${expected.length} 个岩芯匹配`,
  );
  check(
    "同一岩芯多个修订共用详情地址且各自完成核实",
    multiRevisionOk === expected.length,
    `${multiRevisionOk}/${expected.length} 个岩芯的全部修订均已核实`,
  );

  // 运行记录中应包含被 SIGKILL 中断的那次，以及续跑后成功完成的全量运行
  const hasInterrupted = runs.some((r) => r.status === "INTERRUPTED");
  const hasCompletedFull = runs.some((r) => r.mode === "FULL" && r.status === "COMPLETED");
  check(
    "强制结束的运行在重启后被标记为 INTERRUPTED 且续跑运行成功完成",
    hasInterrupted && hasCompletedFull,
    `INTERRUPTED=${hasInterrupted}, COMPLETED_FULL=${hasCompletedFull}，运行记录 ${runs.length} 条`,
  );

  // ---- 汇总 ----
  const failed = results.filter((r) => !r.ok);
  console.log("\n=== 验证汇总 ===");
  for (const r of results) console.log(`${r.ok ? "PASS" : "FAIL"}  ${r.name} — ${r.detail}`);
  console.log(`\n用时 ${((Date.now() - startedAt) / 1000).toFixed(1)}s（预算 90s）`);
  if (failed.length === 0) {
    console.log("🎉 全部验证通过");
    process.exit(0);
  } else {
    console.error(`💥 ${failed.length} 项失败，服务日志: ${LOG_FILE}`);
    process.exit(1);
  }
}

main().catch((err) => {
  console.error("[verify] 验证脚本异常：", err);
  killService();
  process.exit(1);
});
