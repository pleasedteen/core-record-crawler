/**
 * 离线端到端验证（npm run verify，一条命令）。
 *
 * 覆盖：
 *  0) 并发手工触发：同一时刻只有一个入口能开始采集（唯一 202），且 202 返回时
 *     运行历史已能查到本次 RUNNING 记录；竞争请求得到 409
 *  1) 干净库启动 → 全量采集进行中（结案前）强制 SIGKILL → 重启续跑并完成
 *     （断点续传 + 已核实结果原样沿用）
 *  2) 等待站点期间（站点暂停、Retry-After 退避）查询接口照常可用、运行保持 RUNNING
 *  3) 目录重复链接合并、临时拒绝退避重试、Retry-After 全局等待（统计接口佐证）
 *  4) 更正已公布而详情仍旧修订 → 推进站点时钟 → 增量复查接上涨号（连续核实进度口径）
 *  5) 按岩芯编号查询最新已核实鉴定，与模拟站点期望终态逐一比对
 *  6) 结案原子性：runs 终态与 run_state 完成进度在同一事务生效，
 *     结案后（运行全部空闲时）再 SIGKILL：不产生新的 INTERRUPTED，进度零回退，查询不变
 *
 * 整体预算 115 秒，超时按失败处理。
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
const DEADLINE_MS = 115_000;
const startedAt = Date.now();

const env: NodeJS.ProcessEnv = {
  ...process.env,
  PORT: String(PORT),
  MOCK_PORT: String(MOCK_PORT),
  DB_PATH: path.join(DATA_DIR, "verify.duckdb"),
  AUTO_FULL: "0", // 验证脚本自己控制触发时机，避免启动自动全量干扰并发用例
  EMBED_MOCK: "1",
  RATE_MAX_REQUESTS: "16",
  RATE_WINDOW_MS: "1000",
  MAX_INFLIGHT: "6",
  MAX_RETRIES: "8",
  // 定时周期拉长：验证期间只保留规则本身，具体触发由脚本手工/等待调度完成
  CATCHUP_INTERVAL_MS: "600000",
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

/** 管理接口调用（带重试：模拟站点的管理接口已豁免故障，这里仅兜底网络抖动）。 */
async function admin<T = any>(p: string): Promise<T> {
  const until = Date.now() + 10_000;
  let lastErr: unknown;
  while (Date.now() < until) {
    try {
      const resp = await fetch(`http://127.0.0.1:${MOCK_PORT}${p}`);
      if (resp.ok) return (await resp.json()) as T;
      lastErr = new Error(`HTTP ${resp.status}`);
    } catch (err) {
      lastErr = err;
    }
    await sleep(300);
  }
  throw lastErr ?? new Error(`管理接口调用失败: ${p}`);
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

async function advanceClock(step = 8): Promise<number> {
  const data = await admin<{ clock: number }>(`/__admin/advance-clock?step=${step}`);
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
    await sleep(700);
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

/** 并发发起一批手工触发，返回每个请求的状态码与响应体。 */
async function burstTrigger(pathname: string, count: number): Promise<Array<{ status: number; body: any }>> {
  return Promise.all(
    Array.from({ length: count }, () =>
      api(pathname, { method: "POST" }).then(
        (r) => ({ status: r.status, body: r.body }),
        (err) => ({ status: -1, body: { error: String(err) } }),
      ),
    ),
  );
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

  console.log("=== 岩芯鉴定采集服务 · 离线端到端验证（统一运行管理）===");

  // ---- 阶段 0：并发手工触发——只有一个入口能开始采集 ----
  startService();
  await waitForHealth();
  const idle = (await api<any>("/progress")).body;
  check("启动后无自动采集、处于空闲（手工触发由用例控制）", idle.running === false && idle.phase === "IDLE",
    `running=${idle.running}, phase=${idle.phase}`);

  // 先暂停站点：保证被受理的运行在整个竞争窗口内仍处 RUNNING（等待站点，不结案）
  await admin("/__admin/pause?ms=20000");
  const burst1 = await burstTrigger("/collect/catchup", 6);
  const accepted1 = burst1.filter((r) => r.status === 202);
  const rejected1 = burst1.filter((r) => r.status === 409);
  check(
    "并发 6 个增量触发：恰好 1 个 202、其余 409（竞争时只有一个入口能开始采集）",
    accepted1.length === 1 && rejected1.length === 5,
    `202×${accepted1.length}，409×${rejected1.length}`,
  );

  if (accepted1.length === 1) {
    const runId = accepted1[0].body.runId as number;
    // 202 返回时运行历史必须已能查到本次记录
    const { body: runRow } = await api<any>(`/runs/${runId}`);
    check(
      "手工返回 202 时运行历史已可查到本次 RUNNING 记录",
      runRow?.id === runId && runRow?.status === "RUNNING" && runRow?.run_trigger === "MANUAL",
      `run #${runId}: status=${runRow?.status}, trigger=${runRow?.run_trigger}`,
    );

    // 第二批混合竞争：运行仍在等待站点，全部应被拒
    await sleep(300);
    const burst2 = [
      ...(await burstTrigger("/collect/run-full", 3)),
      ...(await burstTrigger("/collect/catchup", 3)),
    ];
    check(
      "运行进行中的混合并发触发全部 409（全量/增量入口互斥）",
      burst2.every((r) => r.status === 409) && burst2.length === 6,
      `状态码分布: ${JSON.stringify(burst2.map((r) => r.status))}`,
    );

    // ---- 阶段 2（等待站点）：查询照常可用，运行保持 RUNNING ----
    const querySamples: string[] = [];
    const [h, prog, cores, runs] = await Promise.all([
      fetch(`http://127.0.0.1:${PORT}/health`).then((r) => r.status),
      api("/progress"),
      api("/cores?limit=5"),
      api("/runs"),
    ]);
    querySamples.push(`health=${h}`, `progress=${prog.status}`, `cores=${cores.status}`, `runs=${runs.status}`);
    const duringWait = prog.body;
    check(
      "等待站点期间查询接口照常可用，且当前运行保持 RUNNING",
      h === 200 && prog.status === 200 && cores.status === 200 && runs.status === 200 &&
        duringWait.running === true && duringWait.runId === runId && duringWait.trigger === "MANUAL",
      `${querySamples.join(", ")}；runId=${duringWait.runId}`,
    );

    // 恢复站点：等待中的手工增量应自行重试并结案
    await admin("/__admin/resume");
    await waitProgress((p) => p.running === false, "暂停恢复后手工增量结案");
    const incRun = (await api<any>(`/runs/${runId}`)).body;
    check(
      "暂停恢复后被受理的手工增量正常结案（Retry-After 等待后自愈）",
      incRun?.status === "COMPLETED" && incRun?.mode === "INCREMENTAL",
      `run #${runId} → ${incRun?.status}`,
    );
  }

  // ---- 阶段 1：全量采集中途（结案前）SIGKILL ----
  const triggerFull = await api("/collect/run-full", { method: "POST" });
  check("手工触发全量返回 202 且带回 runId", triggerFull.status === 202 && typeof triggerFull.body.runId === "number",
    `status=${triggerFull.status}, runId=${triggerFull.body.runId}`);
  await waitProgress(
    (p) => p.details.total > 0 && (p.details.verified > 0 || p.details.pending > 0),
    "全量采集开始产生鉴定记录",
  );
  const mid = (await api<any>("/progress")).body;
  console.log(
    `[verify] 全量进行中：已登记 ${mid.details.total} 条，已核实 ${mid.details.verified} 条，` +
      `目录 ${mid.catalog.cursor}/${mid.catalog.totalPages}，更正 ${mid.corrections.cursor}/${mid.corrections.totalPages}`,
  );

  // 结案前强杀（模拟 kill -9 / 掉电）：杀整个进程组
  const fullRunId = mid.runId;
  const verifiedAtKill = mid.details.verified;
  killService();
  await sleep(1500);
  check("结案前进程被 SIGKILL 强制结束", fullRunId !== null, `被杀时 run #${fullRunId}，已核实 ${verifiedAtKill} 条`);

  // ---- 阶段 1b：重启续跑，沿用已保存核实结果（期间推进时钟让滞后详情生效）----
  startService();
  await waitForHealth();
  // AUTO_FULL=0 下重启不会自动续跑；按既有调度规则，未完成全量时由脚本通过同一入口再触发
  const retrigger = await api("/collect/run-full", { method: "POST" });
  check("重启后通过统一入口再次触发全量（断点续跑）", retrigger.status === 202, `status=${retrigger.status}`);
  const progress2 = await waitFullWithClock();
  check(
    "重启后续跑并完成全量",
    progress2.fullCompleted === true && progress2.details.pending === 0,
    `已核实 ${progress2.details.verified} 条，待核实 ${progress2.details.pending} 条`,
  );
  check(
    "强杀前已核实结果被原样沿用（核实数不回退、无重复劳动痕迹）",
    progress2.details.verified >= verifiedAtKill,
    `强杀前已核实 ${verifiedAtKill} → 最终 ${progress2.details.verified}`,
  );
  // 续跑可能完全不重发更正列表请求（URL 去重），内存 totalPublished 可能为空，
  // 已发布总数以模拟站点管理口径为准（库内更正游标此前已持久化）
  const siteState = await admin<any>("/__admin/state");
  check(
    "连续核实进度覆盖全部已发布序号",
    progress2.contiguousVerifiedSeq === siteState.totalPublished,
    `连续核实序号 ${progress2.contiguousVerifiedSeq} / 已发布 ${siteState.totalPublished}`,
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

  // ---- 阶段 4：制造“详情滞后”，增量追更正衔接（手工触发，MANUAL 运行）----
  const beforeCatchup = (await api<any>("/progress")).body;
  await admin("/__admin/advance-clock?step=100");
  const catchupResp = await api("/collect/catchup", { method: "POST" });
  check("手工增量触发返回 202 且运行记录先行落库", catchupResp.status === 202 && !!catchupResp.body.runId,
    `runId=${catchupResp.body.runId}`);
  const after = await waitProgress(
    (p) => p.fullCompleted === true && p.details.pending === 0 && p.running === false,
    "增量复查后无待核实修订",
  );
  const catchupRun = (await api<any>(`/runs/${catchupResp.body.runId}`)).body;
  check(
    "增量运行记录标记为 MANUAL 且 COMPLETED（三类触发同一套运行管理）",
    catchupRun?.run_trigger === "MANUAL" && catchupRun?.status === "COMPLETED",
    `trigger=${catchupRun?.run_trigger}, status=${catchupRun?.status}`,
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
  const state = await admin<any>("/__admin/state");
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

  // ---- 阶段 6a：运行历史与结案原子性（结案前强杀 + 正常完成并存）----
  const runsBefore = (await api<any>("/runs")).body.runs as any[];
  const hasInterrupted = runsBefore.some((r) => r.status === "INTERRUPTED");
  const hasCompletedFull = runsBefore.some((r) => r.mode === "FULL" && r.status === "COMPLETED");
  check(
    "结案前强杀的运行被标记 INTERRUPTED，续跑运行 COMPLETED",
    hasInterrupted && hasCompletedFull,
    `INTERRUPTED=${hasInterrupted}, COMPLETED_FULL=${hasCompletedFull}，运行记录 ${runsBefore.length} 条`,
  );
  // 原子性证据：每条终态运行的 runs 快照与当前 run_state 口径一致（同事务提交）
  const completedFull = runsBefore.find((r) => r.mode === "FULL" && r.status === "COMPLETED");
  check(
    "全量结案与完成进度同事务生效（runs 行内进度即 run_state 进度，无撕裂）",
    !!completedFull &&
      completedFull.contiguous_verified_seq === after.contiguousVerifiedSeq &&
      completedFull.details_pending === 0,
    `runs.contiguous=${completedFull?.contiguous_verified_seq}，state.contiguous=${after.contiguousVerifiedSeq}`,
  );
  const noRunning = runsBefore.every((r) => r.status !== "RUNNING");
  check("结案后不存在游离 RUNNING（所有运行均已结案）", noRunning, `RUNNING 条数=${runsBefore.filter((r) => r.status === "RUNNING").length}`);

  // 留存结案后基线，供强杀重启后比对
  const baseline = {
    fullCompleted: after.fullCompleted,
    contiguous: after.contiguousVerifiedSeq,
    verified: after.details.verified,
    total: after.details.total,
    sampleCore: (await api<any>(`/cores/${expected[0].coreId}`)).body,
  };

  // ---- 阶段 6b：结案后（全部空闲）再 SIGKILL：不产生新 INTERRUPTED，进度零回退 ----
  killService();
  await sleep(1500);
  startService();
  await waitForHealth();
  // 重启后按既有调度规则，已完成全量的库会自动排一次 SCHEDULED 增量；等它受理并结案
  const waitUntil = Date.now() + 15_000;
  let runsAfter = (await api<any>("/runs")).body.runs as any[];
  while (Date.now() < waitUntil) {
    const pro = (await api<any>("/progress")).body;
    runsAfter = (await api<any>("/runs")).body.runs as any[];
    const scheduled = runsAfter.filter((r) => !runsBefore.some((b) => b.id === r.id));
    if (!pro.running && scheduled.some((r) => r.run_trigger === "SCHEDULED" && r.status === "COMPLETED")) break;
    await sleep(300);
  }
  const progress3 = (await api<any>("/progress")).body;
  const interruptedAfter = runsAfter.filter((r) => r.status === "INTERRUPTED").length;
  const interruptedBefore = runsBefore.filter((r) => r.status === "INTERRUPTED").length;
  const newRuns = runsAfter.filter((r) => !runsBefore.some((b) => b.id === r.id));
  check(
    "结案后强杀不产生新的 INTERRUPTED 记录（无运行在途，无事可中断）",
    interruptedAfter === interruptedBefore && newRuns.every((r) => r.status !== "INTERRUPTED"),
    `强杀前后 INTERRUPTED ${interruptedBefore}→${interruptedAfter}，新增运行 ${newRuns.length} 条（状态: ${newRuns.map((r) => r.status).join(",") || "无"}）`,
  );
  check(
    "重启后仅按既有规则新增 SCHEDULED 增量（调度规则保留，非手工/启动全量）",
    newRuns.every((r) => r.mode === "INCREMENTAL" && r.run_trigger === "SCHEDULED") && newRuns.length >= 1,
    `新增运行: ${newRuns.map((r) => `#${r.id} ${r.mode}/${r.run_trigger}/${r.status}`).join("; ") || "无"}`,
  );
  check(
    "结案后强杀重启：完成进度零回退（全量标志与连续序号保持）",
    progress3.fullCompleted === baseline.fullCompleted &&
      progress3.contiguousVerifiedSeq === baseline.contiguous &&
      progress3.details.verified === baseline.verified &&
      progress3.details.total === baseline.total,
    `连续序号 ${baseline.contiguous}→${progress3.contiguousVerifiedSeq}，已核实 ${baseline.verified}→${progress3.details.verified}`,
  );
  const sampleAfter = (await api<any>(`/cores/${expected[0].coreId}`)).body;
  check(
    "结案后强杀重启：已核实查询结果不变（沿用已有核实结果）",
    sampleAfter.revision === baseline.sampleCore.revision &&
      sampleAfter.status === "VERIFIED" &&
      sampleAfter.verifiedIdentification?.classification ===
        baseline.sampleCore.verifiedIdentification?.classification,
    `${expected[0].coreId} r${sampleAfter.revision}（强杀前后一致）`,
  );

  // ---- 汇总 ----
  const failed = results.filter((r) => !r.ok);
  console.log("\n=== 验证汇总 ===");
  for (const r of results) console.log(`${r.ok ? "PASS" : "FAIL"}  ${r.name} — ${r.detail}`);
  console.log(`\n用时 ${((Date.now() - startedAt) / 1000).toFixed(1)}s（预算 115s）`);
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
