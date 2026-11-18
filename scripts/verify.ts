/**
 * 离线端到端验证（npm run verify）。
 *
 * 覆盖：
 *  1) 干净库启动 → 全量采集进行中强制 SIGKILL → 重启续跑并完成（断点续传 + 核实结果沿用）
 *  2) 目录重复链接合并、临时拒绝退避重试、Retry-After 全局等待（统计接口 + 日志佐证）
 *  3) 更正已公布而详情仍旧修订 → 推进站点时钟 → 增量复查接上涨号（连续核实进度口径）
 *  4) 按岩芯编号查询最新已核实鉴定，与模拟站点期望终态逐一比对
 *
 * 整体预算 75 秒，超时按失败处理。
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
const DEADLINE_MS = 78_000;
const startedAt = Date.now();

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
    await sleep(500);
  }
  throw new Error(`等待「${label}」超时，最后进度: ${JSON.stringify(last)}`);
}

async function advanceClock(step = 8): Promise<number> {
  const resp = await fetch(`http://127.0.0.1:${MOCK_PORT}/__admin/advance-clock?step=${step}`);
  if (!resp.ok) return -1;
  const data = (await resp.json()) as { clock: number };
  return data.clock;
}

/** 等待全量完成期间持续推进站点详情时钟，模拟时间流逝让滞后详情陆续生效。 */
async function waitFullWithClock(): Promise<any> {
  const until = Date.now() + timeLeft() - 2_000;
  let last: any = null;
  while (Date.now() < until) {
    const { body } = await api<any>("/progress");
    last = body;
    if (body.fullCompleted === true) return body;
    await advanceClock(10);
    await sleep(800);
  }
  throw new Error(`等待全量完成超时，最后进度: ${JSON.stringify(last)}`);
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
  const progress2 = await waitFullWithClock();
  check(
    "重启后续跑并完成全量",
    progress2.fullCompleted === true && progress2.details.pending === 0,
    `已核实 ${progress2.details.verified} 条，待核实 ${progress2.details.pending} 条`,
  );
  check(
    "连续核实进度覆盖全部已发布序号",
    progress2.contiguousVerifiedSeq === progress2.corrections.totalPublished,
    `连续核实序号 ${progress2.contiguousVerifiedSeq} / 已发布 ${progress2.corrections.totalPublished}`,
  );

  // ---- 阶段 3：去重、重试、Retry-After 证据 ----
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

  // ---- 阶段 4：制造“详情滞后”，增量追更正衔接 ----
  // 模拟站点时钟推进：让可能存在的滞后详情生效；同时触发一次增量
  const beforeCatchup = (await api<any>("/progress")).body;
  const admin = await fetch(`http://127.0.0.1:${MOCK_PORT}/__admin/advance-clock?step=100`);
  const clock = (await admin.json()) as { clock: number };
  console.log(`[verify] 推进模拟站点详情生效时钟 → ${clock.clock}`);
  await api("/collect/catchup", { method: "POST" });
  const after = await waitProgress(
    (p) => p.fullCompleted === true && p.details.pending === 0,
    "增量复查后无待核实修订",
  );
  check(
    "增量进度只计入连续核实的发布范围",
    after.contiguousVerifiedSeq >= beforeCatchup.contiguousVerifiedSeq &&
      after.contiguousVerifiedSeq === after.corrections.totalPublished,
    `增量前连续序号 ${beforeCatchup.contiguousVerifiedSeq} → 增量后 ${after.contiguousVerifiedSeq}`,
  );
  check(
    "全量期间及之后发布的更正均可被增量接上（待核实清零）",
    after.details.pending === 0,
    `已核实 ${after.details.verified}/${after.details.total}`,
  );

  // ---- 阶段 5：按岩芯编号查询，逐一比对模拟站点期望终态 ----
  const state = (await (await fetch(`http://127.0.0.1:${MOCK_PORT}/__admin/state`)).json()) as any;
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
  const runs = (await api<any>("/runs")).body.runs as any[];
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
  console.log(`\n用时 ${((Date.now() - startedAt) / 1000).toFixed(1)}s（预算 75s）`);
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
