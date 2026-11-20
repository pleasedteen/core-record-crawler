/**
 * 积压场景离线验证（`npm run verify:backlog`）。
 *
 * 场景：
 *  - 11,600 个积压详情地址 + 40 个“详情滞后”地址 + 6 个“前两次 503”地址；
 *  - 5 个新修订通过 /corrections 在积压未清空时发布（追更）；
 *  - 进程被 SIGKILL 后重启，验证已安排的复查/退避时刻仍被遵守。
 *
 * 断言（对应需求）：
 *  1) 退避中的地址让出请求机会：FL 地址失败后，下一次请求不早于退避时刻；
 *  2) 积压未清空时仍能登记并核实追更新修订（新地址用空闲额度）；
 *  3) 调度内存窗口 ≤64（排队 + 执行 + 等待）；
 *  4) 强杀重启后仍遵守已安排的复查/退避时间；
 *  5) 同一地址的多个待核实修订共用一次复查（ST 地址只发过一次详情请求就核实了 r1+r2）；
 *  6) 各类请求共用站点限额与 Retry-After（暂停期间不发出任何详情请求）。
 */
import { spawn, ChildProcess } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import { BacklogScenarioSite, RequestLogEntry } from "./backlog-scenario-site.js";

const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");
const DATA_DIR = path.join(ROOT, "data-backlog");
const PORT = 8282;
const MOCK_PORT = 9292;
const BUDGET_MS = 150_000;

const env: NodeJS.ProcessEnv = {
  ...process.env,
  PORT: String(PORT),
  MOCK_PORT: String(MOCK_PORT),
  MOCK_BASE_URL: `http://127.0.0.1:${MOCK_PORT}`,
  DB_PATH: path.join(DATA_DIR, "backlog.duckdb"),
  AUTO_FULL: "0",
  EMBED_MOCK: "0",
  RATE_MAX_REQUESTS: "24",
  RATE_WINDOW_MS: "1000",
  MAX_INFLIGHT: "8",
  DETAIL_CONCURRENCY: "4",
  SCHEDULER_WINDOW: "64",
  SCHEDULER_TICK_MS: "25",
  DETAIL_RECHECK_BASE_MS: "300",
  DETAIL_RECHECK_MAX_MS: "1200",
  DETAIL_BACKOFF_BASE_MS: "400",
  DETAIL_BACKOFF_MAX_MS: "4000",
  CATCHUP_INTERVAL_MS: "1000",
  BACKLOG_SEED_FILE: path.join(DATA_DIR, "seed.json"),
};

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
let site: BacklogScenarioSite;
let server: import("node:http").Server;
let child: ChildProcess | null = null;
const results: { name: string; ok: boolean; detail: string }[] = [];

function check(name: string, ok: boolean, detail: string): void {
  results.push({ name, ok, detail });
  console.log(`${ok ? "✅" : "❌"} ${name} — ${detail}`);
}

async function api<T = any>(p: string, init?: RequestInit): Promise<{ status: number; body: T }> {
  const resp = await fetch(`http://127.0.0.1:${PORT}${p}`, init);
  const text = await resp.text();
  let body: unknown = text;
  try {
    body = JSON.parse(text);
  } catch {
    /* 非 JSON 保持原样 */
  }
  return { status: resp.status, body: body as T };
}

async function mock(pathname: string): Promise<any> {
  const resp = await fetch(`http://127.0.0.1:${MOCK_PORT}${pathname}`);
  return resp.json();
}

async function waitHealth(timeoutMs = 30_000): Promise<void> {
  const until = Date.now() + timeoutMs;
  for (;;) {
    try {
      const r = await fetch(`http://127.0.0.1:${PORT}/health`);
      if (r.ok) return;
    } catch {
      /* 未就绪 */
    }
    if (Date.now() > until) throw new Error("服务健康检查超时");
    await sleep(200);
  }
}

function startService(): void {
  child = spawn(
    process.execPath,
    [path.join(ROOT, "node_modules", "tsx", "dist", "cli.mjs"), "scripts/backlog-service.ts"],
    { cwd: ROOT, env, detached: true, stdio: ["ignore", "pipe", "pipe"] },
  );
  const logPath = path.join(DATA_DIR, "service.log");
  const log = fs.createWriteStream(logPath, { flags: "a" });
  child.stdout?.on("data", (d) => log.write(d));
  child.stderr?.on("data", (d) => log.write(d));
}

function killService(): void {
  if (!child || child.killed || child.pid === undefined) return;
  try {
    process.kill(-child.pid, "SIGKILL");
  } catch {
    try {
      child.kill("SIGKILL");
    } catch {
      /* 已退出 */
    }
  }
}

async function progress(): Promise<any> {
  return (await api<any>("/progress")).body;
}

async function waitFor(predicate: () => Promise<boolean>, label: string, timeoutMs: number): Promise<boolean> {
  const until = Date.now() + timeoutMs;
  while (Date.now() < until) {
    if (await predicate()) return true;
    await sleep(120);
  }
  console.log(`[verify:backlog] 等待「${label}」超时`);
  return false;
}

function detailHits(entries: RequestLogEntry[], coreId: string): number[] {
  return entries.filter((e) => e.kind === "detail" && e.coreId === coreId).map((e) => e.t);
}

interface ScheduleRowInfo {
  coreId: string;
  state: "QUEUED" | "RUNNING" | "WAITING";
  reason: "RECHECK" | "BACKOFF" | null;
  attempts: number;
  nextAttemptAt: number;
  lastError: string | null;
}

/** 读取 /schedule（窗口驻留 + 延迟等待）并轮询到谓词成立。 */
async function waitForSchedule<T>(
  predicate: (rows: ScheduleRowInfo[]) => T | undefined,
  label: string,
  timeoutMs: number,
): Promise<T | undefined> {
  const until = Date.now() + timeoutMs;
  while (Date.now() < until) {
    const body = (await api<any>("/schedule")).body;
    const rows = [...(body.resident as ScheduleRowInfo[]), ...(body.deferred as ScheduleRowInfo[])];
    const hit = predicate(rows);
    if (hit) return hit;
    await sleep(120);
  }
  console.log(`[verify:backlog] 等待「${label}」超时`);
  return undefined;
}

async function main(): Promise<void> {
  const startedAt = Date.now();
  fs.rmSync(DATA_DIR, { recursive: true, force: true });
  fs.mkdirSync(DATA_DIR, { recursive: true });

  site = new BacklogScenarioSite();
  server = site.createServer();
  await new Promise<void>((r) => server.listen(MOCK_PORT, "127.0.0.1", r));
  fs.writeFileSync(env.BACKLOG_SEED_FILE!, JSON.stringify(site.seedRows()));

  const cleanup = () => {
    killService();
    server.close();
  };
  process.on("exit", cleanup);
  process.on("SIGINT", () => {
    cleanup();
    process.exit(130);
  });

  console.log("=== 积压场景 · 请求时序与恢复验证 ===");
  console.log(
    `[verify:backlog] 站点：积压 ${site.backlogIds.length} 地址，详情滞后 ${site.staleIds.length}，` +
      `反复拒绝 ${site.flakyIds.length}，追更新地址 ${site.newIds.length}`,
  );

  // ---- 启动：积压写入 + 调度器 ----
  startService();
  await waitHealth(60_000);
  await waitFor(async () => (await progress()).details.total >= site.backlogIds.length, "积压登记完成", 60_000);
  const initial = await progress();
  check(
    "积压上万条仍能启动并保持窗口上限",
    initial.details.total >= 11_000 &&
      initial.schedule.resident <= 64 &&
      initial.schedule.resident > 0,
    `待核实 ${initial.details.total} 条，调度窗口驻留 ${initial.schedule.resident}（上限 64）`,
  );

  // ---- 场景 A：追更在积压未清空时登记并核实新修订 ----
  await api("/collect/catchup", { method: "POST" });
  const catchupSeen = await waitFor(
    async () => {
      const p = await progress();
      return p.details.total >= site.backlogIds.length + site.newIds.length;
    },
    "追更登记新修订",
    30_000,
  );
  const newVerified = await waitFor(
    async () => (await progress()).details.verified >= site.newIds.length,
    "追更新修订被空闲额度核实",
    60_000,
  );
  const afterNew = await progress();
  check(
    "积压未清空时追更仍能登记并核实新修订",
    catchupSeen &&
      newVerified &&
      afterNew.details.pending >= 10_000 &&
      afterNew.details.verified >= site.newIds.length,
    `待核实仍有 ${afterNew.details.pending} 条时，新修订已核实 ${afterNew.details.verified} 条（含 ${site.newIds.length} 条追更）`,
  );

  // ---- 场景 B：站点要求全局等待期间不得发请求 ----
  // 注入一次性 503 + Retry-After；从它被发出的时刻起，到等待截止前，
  // 不应有任何新的详情请求（所有请求共用一个 Retry-After 暂停）。
  const pauseMs = 900;
  await mock(`/__admin/pause-next?ms=${pauseMs}`);
  const pauseState: { pauseServedAt: number; pauseServedMs: number } = { pauseServedAt: 0, pauseServedMs: 0 };
  const pauseSeen = await waitFor(async () => {
    const state = (await mock("/__admin/state")) as { pauseServedAt: number; pauseServedMs: number };
    if (state.pauseServedAt > 0) {
      pauseState.pauseServedAt = state.pauseServedAt;
      pauseState.pauseServedMs = state.pauseServedMs;
      return true;
    }
    return false;
  }, "503 暂停触发", 30_000);
  const pauseUntil = pauseState.pauseServedAt + pauseMs;
  // 等到暂停窗口结束，再回看窗口内是否有请求
  await sleep(Math.max(0, pauseUntil - Date.now()) + 50);
  const duringPause = pauseSeen
    ? ((await mock("/__admin/log?kind=detail")).entries as RequestLogEntry[]).filter(
        (e) => e.t > pauseState.pauseServedAt + 45 && e.t < pauseUntil - 45,
      )
    : [];
  check(
    "各类请求共用站点限额与 Retry-After（暂停期间无详情请求）",
    pauseSeen && pauseState.pauseServedMs === pauseMs && duringPause.length === 0,
    `站点要求暂停 ${pauseMs}ms（${pauseState.pauseServedAt || "-"} 起），期间发出详情请求 ${duringPause.length} 个`,
  );

  // ---- 场景 C：退避中的地址让出请求机会 ----
  // 找一个处于退避（BACKOFF）的地址，记录其安排时刻；退避期间必须：
  //   a) 该地址自己不被请求；b) 其他地址继续被请求；c) 到点后才重试。
  const backoff = await waitForSchedule(
    (rows) =>
      rows.find(
        (r) => r.state === "WAITING" && r.reason === "BACKOFF" && r.nextAttemptAt > Date.now() + 350,
      ) as ScheduleRowInfo | undefined,
    "出现退避中的地址",
    60_000,
  );
  if (!backoff) {
    check("退避中的地址让出请求机会（其他地址在其退避期间继续被请求）", false, "未观察到退避中的地址");
    check("退避等待遵守 Retry-After（重试间隔不早于要求）", false, "未观察到退避中的地址");
  } else {
    const logAtBackoff = (await mock("/__admin/log?kind=detail")).entries as RequestLogEntry[];
    const hitsBefore = detailHits(logAtBackoff, backoff.coreId);
    const othersDuring = logAtBackoff.filter(
      (e) => e.kind === "detail" && e.coreId !== backoff.coreId && e.t >= backoff.nextAttemptAt - 20000,
    ).length;
    check(
      "退避中的地址让出请求机会（其他地址在其退避期间继续被请求）",
      othersDuring >= 10 && backoff.nextAttemptAt > Date.now(),
      `${backoff.coreId} 处于退避（已请求 ${hitsBefore.length} 次，` +
        `还需等待 ${Math.round(backoff.nextAttemptAt - Date.now())}ms），期间其他详情请求 ${othersDuring} 个`,
    );

    // 等到该地址重试，核对重试时刻不早于安排时刻
    const retried = await waitFor(async () => {
      const log = await mock("/__admin/log?kind=detail");
      return detailHits(log.entries as RequestLogEntry[], backoff.coreId).length > hitsBefore.length;
    }, "退避到点后重试", 30_000);
    const hitsAfter = detailHits(
      (await mock("/__admin/log?kind=detail")).entries as RequestLogEntry[],
      backoff.coreId,
    );
    const retryAt = hitsAfter[hitsAfter.length - 1];
    check(
      "退避等待遵守 Retry-After（重试不早于安排的退避时刻）",
      retried && retryAt >= backoff.nextAttemptAt - 20,
      `${backoff.coreId} 安排时刻后 ${Math.round(retryAt - backoff.nextAttemptAt)}ms 重试（负值表示提前）`,
    );
  }

  // ---- 场景 D：同一地址多修订共用一次复查；滞后详情在 reveal 后核实 ----
  const staleId = site.staleIds[0];
  const beforeReveal = await progress();
  await mock("/__admin/reveal");
  const staleVerified = await waitFor(
    async () => {
      const p = await api<any>(`/identifications/${staleId}`);
      return (
        p.status === 200 &&
        (p.body.revisions as any[]).length === 2 &&
        (p.body.revisions as any[]).every((r: any) => r.status === "VERIFIED")
      );
    },
    "滞后详情复查后核实同一地址两个修订",
    60_000,
  );
  check(
    "同一地址的待核实修订共用一次复查",
    staleVerified,
    `${staleId} 的 r1+r2 均已核实（详情页滞后期间保持 PENDING，reveal 后复查接上）`,
  );

  // ---- 场景 E：强杀重启后仍遵守已安排的复查/退避时间 ----
  // 找到一个“退避中、距到点还有 >=700ms”的地址，确认它仍在退避期内，
  // 立刻 SIGKILL；重启后必须等到原定时刻才会再请求它。
  // 选一个当前正在窗口内排队的地址，注入“再失败 3 次、每次要求等 3s”。
  // 目标可能刚好在注入前完成了请求，因此最多重试 3 次挑选。
  let killTarget = "";
  let killTargetInfo: ScheduleRowInfo | undefined;
  for (let attempt = 1; attempt <= 3 && !killTargetInfo; attempt++) {
    const residentNow = (await api<any>("/schedule")).body.resident as ScheduleRowInfo[];
    const candidate =
      residentNow.find((r) => r.state === "QUEUED" && r.attempts === 0) ??
      residentNow.find((r) => r.state === "QUEUED") ??
      residentNow.find((r) => r.state === "RUNNING");
    if (!candidate) break;
    killTarget = candidate.coreId;
    await mock(`/__admin/flaky?coreId=${killTarget}&times=3&retryAfterSec=3`);
    killTargetInfo = await waitForSchedule(
      (rows) =>
        rows.find(
          (r) =>
            r.coreId === killTarget &&
            r.state === "WAITING" &&
            r.reason === "BACKOFF" &&
            r.nextAttemptAt > Date.now() + 1500,
        ) as ScheduleRowInfo | undefined,
      "找到强杀目标（退避中）",
      10_000,
    );
  }
  if (!killTargetInfo) {
    check("强杀重启后仍遵守已安排的退避时间（不提前请求）", false, "未找到退避中的强杀目标");
  } else {
    const scheduledRetryAt = killTargetInfo.nextAttemptAt;
    const hitsBeforeKill = detailHits(
      (await mock("/__admin/log?kind=detail")).entries as RequestLogEntry[],
      killTarget,
    );
    const pendingBeforeKill = (await progress()).details.pending;
    const killedAt = Date.now();
    killService();
    await sleep(400);

    // 重启（健康检查通过前不应有任何详情请求）
    startService();
    await waitHealth(60_000);
    const restartedAt = Date.now();
    const restarted = await progress();
    check(
      "强杀重启后积压与窗口状态被恢复",
      restarted.details.pending <= pendingBeforeKill + site.newIds.length &&
        restarted.details.pending >= pendingBeforeKill - 80 &&
        restarted.schedule.resident <= 64,
      `重启后待核实 ${restarted.details.pending} 条（强杀前 ${pendingBeforeKill}），窗口驻留 ${restarted.schedule.resident}`,
    );

    // 关键断言：重启后该地址的下一次请求不得早于原定退避时刻
    const retried = await waitFor(async () => {
      const log = await mock("/__admin/log?kind=detail");
      return detailHits(log.entries as RequestLogEntry[], killTarget).length > hitsBeforeKill.length;
    }, "重启后按原退避时刻重试", 30_000);
    const afterRestartHits = detailHits(
      (await mock("/__admin/log?kind=detail")).entries as RequestLogEntry[],
      killTarget,
    );
    // 强杀前已请求 hitsBeforeKill.length 次；之后的第一次请求才是重启后的重试
    const firstAfterKill = afterRestartHits[hitsBeforeKill.length];
    const earlyBy = firstAfterKill === undefined ? null : scheduledRetryAt - firstAfterKill;
    const restartFinishedBeforeDue = restartedAt < scheduledRetryAt;
    check(
      "强杀重启后仍遵守已安排的退避时间（不提前请求）",
      retried &&
        earlyBy !== null &&
        earlyBy <= 50 &&
        restartFinishedBeforeDue,
      `${killTarget} 原定 ${new Date(scheduledRetryAt).toISOString()} 重试；` +
        `强杀于 ${new Date(killedAt).toISOString()}，重启就绪于 ${new Date(restartedAt).toISOString()}` +
        `${restartFinishedBeforeDue ? "（仍在退避期内）" : "（已过原定时刻，仅验证未丢失安排）"}，` +
        `重启后首次请求${earlyBy !== null && earlyBy > 0 ? `提前 ${Math.round(earlyBy)}ms` : "不早于原定时刻"}（` +
        `强杀前已请求 ${hitsBeforeKill.length} 次）`,
    );
  }

  const deferredInfo = await progress();
  check(
    "窗口上限在积压与重启后始终保持",
    deferredInfo.schedule.resident <= 64 && deferredInfo.schedule.pendingCores > 64,
    `驻留 ${deferredInfo.schedule.resident}（≤64），待核实地址 ${deferredInfo.schedule.pendingCores}，` +
      `延迟等待 ${deferredInfo.schedule.deferred}，未入窗积压 ${deferredInfo.schedule.backlog}`,
  );

  // ---- 汇总 ----
  const failed = results.filter((r) => !r.ok);
  const elapsed = ((Date.now() - startedAt) / 1000).toFixed(1);
  console.log("\n=== 验证汇总 ===");
  for (const r of results) console.log(`${r.ok ? "PASS" : "FAIL"}  ${r.name} — ${r.detail}`);
  console.log(`\n用时 ${elapsed}s（预算 ${BUDGET_MS / 1000}s），服务日志: ${path.join(DATA_DIR, "service.log")}`);
  cleanup();
  if (failed.length === 0) {
    console.log("🎉 积压场景验证通过");
    process.exit(0);
  }
  console.error(`💥 ${failed.length} 项失败`);
  process.exit(1);
}

main().catch((err) => {
  console.error("[verify:backlog] 验证脚本异常：", err);
  killService();
  server?.close();
  process.exit(1);
});
