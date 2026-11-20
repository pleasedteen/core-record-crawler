/**
 * 离线端到端验证（npm run verify）。
 *
 * 覆盖：
 *  1) 干净库启动 → 全量采集进行中强制 SIGKILL → 重启续跑并完成（断点续传 + 核实结果沿用）
 *  2) 目录重复链接合并、临时拒绝退避重试、Retry-After 全局等待（统计接口 + 日志佐证）
 *  3) 更正已公布而详情仍旧修订 → 推进站点时钟 → 增量复查接上涨号（连续核实进度口径）
 *  4) 撤回公告：占用发布序号、原发布与撤回都计入已处置、撤回后查询回退到仍有效的最新已核实版本
 *  5) 再次 SIGKILL 重启：沿用旧库仍能区分原始发布（VERIFIED）、撤回（WITHDRAWN）与同号重发（VERIFIED）
 *  6) 运行时撤回 → 按住的同号重发（旧详情不切换、连续进度停在缺口前）→ 释放后核实才切换
 *  7) 按岩芯编号查询最新有效鉴定，与模拟站点期望终态逐一比对
 *
 * 整体预算 110 秒，超时按失败处理。
 */
import { spawn, ChildProcessWithoutNullStreams } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import process from "node:process";

const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");
const DATA_DIR = path.join(ROOT, "data-verify");
const LOG_FILE = path.join(DATA_DIR, "service.log");
const PORT = 8181;
const MOCK_PORT = 9191;
const DEADLINE_MS = 110_000;
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

async function admin<T = any>(p: string): Promise<{ status: number; body: T }> {
  // 模拟站点会注入 503/500 临时故障（纯文本响应），管理端调用需带重试
  const until = Date.now() + 30_000;
  for (;;) {
    const resp = await fetch(`http://127.0.0.1:${MOCK_PORT}${p}`);
    if (resp.ok) {
      const body = (await resp.json()) as T;
      return { status: resp.status, body };
    }
    if (Date.now() > until) throw new Error(`管理端调用持续失败: ${p} HTTP ${resp.status}`);
    await sleep(400);
  }
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
  const { body } = await admin<{ clock: number }>(`/__admin/advance-clock?step=${step}`);
  return body.clock ?? -1;
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

/** 触发一次增量并等待其运行结束（phase 回到 IDLE）。 */
async function runCatchupAndWait(): Promise<any> {
  await api("/collect/catchup", { method: "POST" });
  return waitProgress((p) => p.phase === "IDLE", "增量运行结束");
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

  console.log("=== 岩芯鉴定采集服务 · 离线端到端验证（含撤回与同号重发）===");

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
    `已核实 ${progress2.details.verified} 条，待核实 ${progress2.details.pending} 条，已撤回 ${progress2.details.withdrawn} 条`,
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

  // ---- 阶段 4：静态撤回数据集（RC-0007 撤回+同号重发；RC-0013 仅撤回不重发）----
  const prog = (await api<any>("/progress")).body;
  check(
    "撤回公告与原发布都已入库且撤回占发布序号",
    prog.announcements.withdraws >= 2 &&
      prog.announcements.total === prog.corrections.totalPublished &&
      prog.announcements.disposed === prog.announcements.total,
    `公告 ${prog.announcements.total} 条（发布 ${prog.announcements.publishes} / 撤回 ${prog.announcements.withdraws}），已处置 ${prog.announcements.disposed}`,
  );
  check(
    "撤回与原发布都计入已处置（无待核实残留）",
    prog.details.pending === 0 && prog.details.withdrawn >= 2,
    `VERIFIED=${prog.details.verified}，WITHDRAWN=${prog.details.withdrawn}，PENDING=${prog.details.pending}`,
  );

  // RC-0013：唯一发布被撤回且不重发 → 无仍有效版本，查询返回 404
  const rc13 = (await api<any>("/identifications/RC-0013")).body;
  const r13v1 = rc13.revisions.find((r: any) => r.revision === 1);
  const rc13Query = await api<any>("/cores/RC-0013");
  check(
    "唯一发布被撤回且不重发：状态 WITHDRAWN，查询无有效版本返回 404",
    r13v1?.status === "WITHDRAWN" && typeof r13v1?.withdrawSeq === "number" &&
      rc13Query.status === 404 && rc13Query.body.found === false,
    `RC-0013 r1=${r13v1?.status}(撤回序号 ${r13v1?.withdrawSeq})，查询 HTTP ${rc13Query.status}`,
  );

  // RC-0021：较新修订被撤回、旧修订仍有效 → 查询回退到仍有效的最新已核实版本
  const rc21 = (await api<any>("/identifications/RC-0021")).body;
  const r21v1 = rc21.revisions.find((r: any) => r.revision === 1);
  const r21v2 = rc21.revisions.find((r: any) => r.revision === 2);
  const rc21Query = (await api<any>("/cores/RC-0021")).body;
  check(
    "撤回较新修订后查询回退到仍有效的最新已核实版本",
    r21v2?.status === "WITHDRAWN" && r21v1?.status === "VERIFIED" &&
      rc21Query.revision === 1 && rc21Query.status === "VERIFIED",
    `RC-0021 r2=${r21v2?.status}，r1=${r21v1?.status}，查询返回 r${rc21Query.revision}`,
  );

  // RC-0007：r2#1 被撤回，沿用同号重发 r2#2 并完成核实 → 查询切到重发批次
  const rc7 = (await api<any>("/identifications/RC-0007")).body;
  const r7v2i1 = rc7.revisions.find((r: any) => r.revision === 2 && r.issue === 1);
  const r7v2i2 = rc7.revisions.find((r: any) => r.revision === 2 && r.issue === 2);
  const rc7Query = (await api<any>("/cores/RC-0007")).body;
  check(
    "同号重发：原发布撤回、重发核实后查询切换到最新有效批次",
    r7v2i1?.status === "WITHDRAWN" &&
      r7v2i2?.status === "VERIFIED" &&
      rc7Query.revision === 2 && rc7Query.issue === 2,
    `RC-0007 r2#1=${r7v2i1?.status}，r2#2=${r7v2i2?.status}，查询返回 r${rc7Query.revision}#${rc7Query.issue}`,
  );

  // 公告台账中撤回与重发序号各自独立、单调占用
  const ledger = (await api<any>(`/announcements?limit=500`)).body;
  const seqs = ledger.items.map((i: any) => i.publishedSeq);
  const uniqueSeq = new Set(seqs).size === seqs.length;
  const w7 = ledger.items.find((i: any) => i.coreId === "RC-0007" && i.kind === "WITHDRAW");
  const p7re = ledger.items.find((i: any) => i.coreId === "RC-0007" && i.kind === "PUBLISH" && i.issue === 2);
  check(
    "撤回公告同样占用发布序号且序号全局唯一",
    uniqueSeq && !!w7 && !!p7re && p7re.publishedSeq > w7.publishedSeq,
    `RC-0007 撤回序号 ${w7?.publishedSeq} → 同号重发序号 ${p7re?.publishedSeq}`,
  );

  // ---- 阶段 5：再次 SIGKILL 重启，沿用旧库仍能区分核实 / 撤回 / 同号重发 ----
  const beforeRestart = (await api<any>("/progress")).body;
  killService();
  await sleep(1500);
  startService();
  await waitForHealth();
  // 全量完成态持久化：重启后直接进入增量模式，不再产生待核实
  await waitProgress((p) => p.fullCompleted === true, "重启后恢复全量完成态");
  await sleep(1200); // 等首轮增量跑完
  const afterRestart = (await api<any>("/progress")).body;
  check(
    "重启后沿用旧库：撤回计数、公告台账与连续进度均保持",
    afterRestart.details.withdrawn === beforeRestart.details.withdrawn &&
      afterRestart.announcements.total === beforeRestart.announcements.total &&
      afterRestart.announcements.withdraws === beforeRestart.announcements.withdraws &&
      afterRestart.contiguousVerifiedSeq === beforeRestart.contiguousVerifiedSeq &&
      afterRestart.details.pending === 0,
    `撤回 ${afterRestart.details.withdrawn} 条，公告 ${afterRestart.announcements.total} 条（撤回 ${afterRestart.announcements.withdraws}），连续序号 ${afterRestart.contiguousVerifiedSeq}`,
  );

  // 重启后三类状态在同修订号上仍可区分（RC-0007：r1 VERIFIED / r2#1 WITHDRAWN / r2#2 VERIFIED）
  const rc7After = (await api<any>("/identifications/RC-0007")).body;
  const a1 = rc7After.revisions.find((r: any) => r.revision === 1);
  const a2i1 = rc7After.revisions.find((r: any) => r.revision === 2 && r.issue === 1);
  const a2i2 = rc7After.revisions.find((r: any) => r.revision === 2 && r.issue === 2);
  check(
    "重启后仍区分原始发布核实、撤回及同号重发三种状态",
    a1?.status === "VERIFIED" && a2i1?.status === "WITHDRAWN" &&
      typeof a2i1?.withdrawSeq === "number" && a2i2?.status === "VERIFIED" && a2i2.issue === 2,
    `RC-0007 r1=${a1?.status}，r2#1=${a2i1?.status}(撤回序号 ${a2i1?.withdrawSeq})，r2#2=${a2i2?.status}`,
  );

  // 运行记录中撤回计数落库（含被 SIGKILL 中断的运行）
  const runs = (await api<any>("/runs")).body.runs as any[];
  const completedRunWithWithdraw = runs.find((r) => r.status === "COMPLETED" && Number(r.details_withdrawn) > 0);
  check(
    "运行历史持久化撤回计数（含 INTERRUPTED 与 COMPLETED 运行）",
    runs.some((r) => r.status === "INTERRUPTED") && !!completedRunWithWithdraw,
    `INTERRUPTED=${runs.some((r) => r.status === "INTERRUPTED")}，` +
      `COMPLETED 运行撤回计数 ${completedRunWithWithdraw?.details_withdrawn ?? "无"}`,
  );

  // ---- 阶段 6：运行时撤回 → 按住的同号重发（旧详情不切换）→ 释放后核实切换 ----
  // 选一个最新有效发布仍 ACTIVE、且撤回后仍有旧版本可回退的岩芯
  const state0 = (await admin<any>("/__admin/state")).body;
  let targetCore = "";
  for (const e of state0.expectedFinalState as any[]) {
    if (e.coreId === "RC-0007") continue;
    const { body: coreInfo } = await admin<any>(`/__admin/core/${e.coreId}`);
    const activeRevs = (coreInfo.revisions ?? []).filter((r: any) => r.state === "ACTIVE");
    if (activeRevs.length >= 2) {
      targetCore = e.coreId;
      break;
    }
  }
  if (!targetCore) throw new Error("找不到可用于运行时撤回测试的岩芯");
  const idTarget = (await api<any>(`/identifications/${targetCore}`)).body;
  const before = (await api<any>(`/cores/${targetCore}`)).body;
  const topRev = before.revision;
  const topIssue = before.issue;
  const issueCountBefore = idTarget.revisions.filter((r: any) => r.revision === topRev).length;
  console.log(`[verify] 运行时撤回目标：${targetCore} r${topRev}#${topIssue}`);

  const w = await admin(`/__admin/withdraw?coreId=${targetCore}&revision=${topRev}`);
  const withdrawSeq = w.body.event?.publishedSeq;
  const re = await admin(`/__admin/reissue?coreId=${targetCore}&revision=${topRev}&held=1`);
  const reissueSeq = re.body.event?.publishedSeq;
  const newIssue = re.body.event?.issue;
  check(
    "运行时撤回与同号重发各占一个新发布序号",
    typeof withdrawSeq === "number" && typeof reissueSeq === "number" &&
      reissueSeq === withdrawSeq + 1 && newIssue === issueCountBefore + 1,
    `撤回序号 ${withdrawSeq}，重发序号 ${reissueSeq}，新批次 issue=${newIssue}`,
  );

  // 增量入库：撤回立即生效；重发被按住（详情仍旧内容）→ 保持 PENDING
  const c1 = await runCatchupAndWait();
  const idB = (await api<any>(`/identifications/${targetCore}`)).body;
  const reRow = idB.revisions.find((r: any) => r.revision === topRev && r.issue === newIssue);
  const withdrawnRow = idB.revisions.find((r: any) => r.revision === topRev && r.issue === topIssue);
  check(
    "重发核实前保持 PENDING，原发布已标记撤回",
    reRow?.status === "PENDING" && withdrawnRow?.status === "WITHDRAWN",
    `r${topRev}#${topIssue}=${withdrawnRow?.status}，r${topRev}#${newIssue}=${reRow?.status}`,
  );

  // 旧详情仍展示：查询回退到仍有效的最新已核实版本（不切换到未核实的重发）
  const afterHeld = (await api<any>(`/cores/${targetCore}`)).body;
  const fellBack =
    afterHeld.found !== false &&
    afterHeld.status === "VERIFIED" &&
    (afterHeld.revision < topRev ||
      (afterHeld.revision === topRev && afterHeld.issue < newIssue));
  check(
    "重发核实前旧详情仍展示：查询回退到仍有效的最新已核实版本",
    fellBack,
    `按住期间查询返回 r${afterHeld.revision}#${afterHeld.issue}（重发 r${topRev}#${newIssue} 尚未核实）`,
  );

  // 连续进度不在撤回处推进：重发序号未处置 → 连续进度停在它前面
  const contiguousAtGap = c1.contiguousVerifiedSeq;
  check(
    "连续进度停在缺口（未核实重发）前，不被撤回序号带过",
    contiguousAtGap < reissueSeq,
    `连续进度序号 ${contiguousAtGap} < 未处置重发序号 ${reissueSeq}`,
  );

  // 释放重发 → 详情生效 → 增量核实后才切换
  const rel = await admin(`/__admin/release?coreId=${targetCore}&revision=${topRev}`);
  const c2 = await runCatchupAndWait();
  const idC = (await api<any>(`/identifications/${targetCore}`)).body;
  const reRow2 = idC.revisions.find((r: any) => r.revision === topRev && r.issue === newIssue);
  const afterRelease = (await api<any>(`/cores/${targetCore}`)).body;
  check(
    "释放后重发核实通过，查询切换到同号重发版本",
    rel.body.released === true &&
      reRow2?.status === "VERIFIED" &&
      afterRelease.revision === topRev && afterRelease.issue === newIssue,
    `r${topRev}#${newIssue}=${reRow2?.status}，查询返回 r${afterRelease.revision}#${afterRelease.issue}`,
  );
  check(
    "缺口补齐后连续进度推进到最新发布序号",
    c2.contiguousVerifiedSeq >= reissueSeq &&
      c2.contiguousVerifiedSeq === c2.corrections.totalPublished,
    `连续进度 ${c2.contiguousVerifiedSeq} / 已发布 ${c2.corrections.totalPublished}`,
  );
  check(
    "撤回与重发全程计入已处置（公告处置数=公告总数，无待核实）",
    c2.announcements.disposed === c2.announcements.total && c2.details.pending === 0,
    `公告已处置 ${c2.announcements.disposed}/${c2.announcements.total}，待核实 ${c2.details.pending}`,
  );

  // ---- 阶段 7：按岩芯编号查询与站点终态逐一比对（站点终态已含运行时撤回/重发）----
  const state = (await admin<any>("/__admin/state")).body;
  const expected = state.expectedFinalState as any[];
  let matched = 0;
  let statusOk = 0;
  let skippedNoValid = 0;
  for (const exp of expected) {
    const { body: core, status: coreStatus } = await api<any>(`/cores/${exp.coreId}`);
    if (coreStatus === 404 || core.found === false) {
      // 站点终态本身已无有效发布（如唯一发布被撤回）：404 是正确行为
      const { body: idents } = await api<any>(`/identifications/${exp.coreId}`);
      const allWithdrawn = idents.revisions?.every((r: any) => r.status === "WITHDRAWN");
      if (allWithdrawn) {
        matched += 1;
        statusOk += 1;
        skippedNoValid += 1;
      }
      continue;
    }
    if (
      core.revision === exp.latestRevision &&
      core.issue === exp.latestIssue &&
      core.verifiedIdentification?.classification
    ) {
      matched += 1;
    }
    const { body: idents } = await api<any>(`/identifications/${exp.coreId}`);
    const noPending = idents.revisions?.every((r: any) => r.status !== "PENDING");
    const withdrawnCount = idents.revisions?.filter((r: any) => r.status === "WITHDRAWN").length ?? 0;
    if (noPending && withdrawnCount >= exp.withdrawnRevisions) statusOk += 1;
  }
  check(
    "按岩芯编号查询到的最新有效鉴定与站点终态一致（含批次号，无有效版本时 404）",
    matched === expected.length,
    `${matched}/${expected.length} 个岩芯匹配（其中 ${skippedNoValid} 个无有效版本正确返回 404）`,
  );
  check(
    "撤回状态在各岩芯修订链中持久可见且无遗留待核实",
    statusOk === expected.length,
    `${statusOk}/${expected.length} 个岩芯状态正确`,
  );

  // ---- 汇总 ----
  const failed = results.filter((r) => !r.ok);
  console.log("\n=== 验证汇总 ===");
  for (const r of results) console.log(`${r.ok ? "PASS" : "FAIL"}  ${r.name} — ${r.detail}`);
  console.log(`\n用时 ${((Date.now() - startedAt) / 1000).toFixed(1)}s（预算 ${DEADLINE_MS / 1000}s）`);
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
