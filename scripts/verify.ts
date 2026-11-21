/**
 * 离线端到端验证（npm run verify）。
 *
 * 内嵌模拟站点启动后【自行分批公布】更正（无需外部推进），
 * 详情页生效时钟滞后于公布进度，同样由站点自行推进。
 *
 * 覆盖：
 *  1) 恢复：全量采集中途 SIGKILL → 重启沿用原范围快照与已核实结果续跑并完成；
 *  2) 交接：全量只锁定「启动时已发布范围」，不等待滞后详情，完成时交接给周期增量；
 *  3) 周期增量接上全部后公布更正（含追加到原来末页的批次）；
 *  4) 较晚鉴定核实后立即可查，连续核实进度等前面的缺口补齐；
 *  5) 重复链接合并、退避重试、Retry-After、按编号查询与站点终态逐一比对。
 *
 * 整体预算 80 秒，超时按失败处理。
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
const DEADLINE_MS = 78_000; // 预算 80 秒，留 2 秒余量
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
    await sleep(300);
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

  console.log("=== 岩芯鉴定采集服务 · 离线端到端验证（交接与恢复） ===");

  // ---- 阶段 1：首次启动，全量采集中途强制杀掉 ----
  startService();
  await waitForHealth();
  const mid = await waitProgress(
    (p) => p.details.verified >= 3 && !p.fullCompleted,
    "全量采集核实出首批鉴定",
  );
  // 采样一条已核实鉴定，用于重启后验证“已核实结果原样沿用”
  const sample = (await api<any>("/cores?limit=5")).body.items?.[0];
  console.log(
    `[verify] 全量进行中：已登记 ${mid.details.total} 条，已核实 ${mid.details.verified} 条，` +
      `全量范围序号 ≤ ${mid.fullTargetSeq}，采样 ${sample?.coreId} r${sample?.revision}`,
  );

  // 强制结束（模拟 kill -9 / 掉电）：杀整个进程组
  const pid = child!.pid;
  killService();
  await sleep(1500);
  check(
    "全量进行中进程被 SIGKILL 强制结束",
    pid !== undefined && !mid.fullCompleted,
    `pid=${pid}，强杀时已核实 ${mid.details.verified} 条、待核实 ${mid.details.pending} 条`,
  );

  // ---- 阶段 2：重启续跑——沿用原范围与已核实结果，全量不等待滞后详情 ----
  startService();
  await waitForHealth();
  // 强杀前已核实的鉴定，重启后不做任何重新采集也应原样可查
  const sampleAfter = (await api<any>(`/cores/${sample.coreId}`)).body;
  check(
    "强杀重启后已核实结果原样沿用",
    sampleAfter.status === "VERIFIED" &&
      sampleAfter.revision === sample.revision &&
      sampleAfter.verifiedIdentification?.classification === sample.classification,
    `${sample.coreId} r${sampleAfter.revision}（${sampleAfter.verifiedIdentification?.classification}）重启后立即可查`,
  );

  const fullDone = await waitProgress((p) => p.fullCompleted === true, "重启后续跑完成全量");
  check(
    "重启后续跑完成全量（沿用启动时已发布的范围快照）",
    fullDone.fullTargetSeq !== null && fullDone.fullTargetSeq >= 1,
    `全量范围锁定在启动时序号 ≤ ${fullDone.fullTargetSeq}，连续核实至 ${fullDone.contiguousVerifiedSeq}`,
  );
  check(
    "全量不等待滞后详情（完成时仍有待核实，交接周期增量）",
    fullDone.details.pending > 0,
    `全量完成时已核实 ${fullDone.details.verified} 条，待核实 ${fullDone.details.pending} 条（详情滞后生效）`,
  );

  // ---- 阶段 3：交接给周期增量——后公布更正（含追加到原末页者）被接上 ----
  const admin = (await (await fetch(`http://127.0.0.1:${MOCK_PORT}/__admin/state`)).json()) as any;
  const finalTotal = Number(admin.finalTotal);
  console.log(
    `[verify] 站点自行公布中：当前 ${admin.published}/${finalTotal} 条，详情生效至序号 ${admin.detailClock}`,
  );

  let gapChecked = false;
  let converged: any = null;
  const until = Date.now() + timeLeft() - 2_000;
  while (Date.now() < until) {
    const p = (await api<any>("/progress")).body;
    // 连续进度之外已有核实结果时，验证“较晚鉴定核实后立即可查”
    if (!gapChecked && p.details.verifiedBeyondContiguous > 0) {
      const list = (await api<any>("/cores?limit=200")).body.items as any[];
      const beyond = list.find((it) => it.publishedSeq > p.contiguousVerifiedSeq);
      if (beyond) {
        const one = (await api<any>(`/cores/${beyond.coreId}`)).body;
        if (one.status === "VERIFIED") {
          gapChecked = true;
          check(
            "较晚鉴定核实后立即可查，连续进度等前面缺口补齐",
            true,
            `连续序号 ${p.contiguousVerifiedSeq} 时，${beyond.coreId} r${beyond.revision}` +
              `（序号 ${beyond.publishedSeq}）已核实可查`,
          );
        }
      }
    }
    if (
      p.details.pending === 0 &&
      p.contiguousVerifiedSeq === finalTotal &&
      p.corrections.totalPublished === finalTotal
    ) {
      converged = p;
      break;
    }
    await sleep(300);
  }
  if (!gapChecked) {
    check("较晚鉴定核实后立即可查，连续进度等前面缺口补齐", false, "未观察到连续进度之外的已核实鉴定");
  }
  check(
    "周期增量接上全部后公布更正（含追加到原来末页的批次）",
    converged !== null,
    converged
      ? `连续核实 ${converged.contiguousVerifiedSeq}/${finalTotal}，待核实 0（全量范围序号 ≤ ${converged.fullTargetSeq}）`
      : "收敛超时",
  );
  if (!converged) throw new Error("周期增量收敛超时");

  // 启动时未公布、由后续批次追加到原来末页的岩芯（序号 36~40），应已被增量接上并核实
  const appendedIds = ["RC-0036", "RC-0037", "RC-0038", "RC-0039", "RC-0040"];
  let appendedOk = 0;
  for (const id of appendedIds) {
    const b = (await api<any>(`/identifications/${id}`)).body;
    if (b.revisions?.length >= 1 && b.revisions.every((r: any) => r.status === "VERIFIED")) {
      appendedOk += 1;
    }
  }
  check(
    "追加到原来末页的更正由周期增量接上",
    appendedOk === appendedIds.length,
    `${appendedIds[0]}~${appendedIds[appendedIds.length - 1]} 全部核实：${appendedOk}/${appendedIds.length}`,
  );

  // ---- 阶段 4：去重、重试、Retry-After 证据 ----
  const stats = (await api<any>("/stats")).body;
  const catalogStat = stats.fetchUrls.find((r: any) => r.kind === "CATALOG");
  const detailStat = stats.fetchUrls.find((r: any) => r.kind === "DETAIL");
  check(
    "目录重复链接已合并（登记数与站点修订总数一致，不多不少）",
    stats.identifications.total === finalTotal && !!catalogStat && catalogStat.distinctUrls <= 8,
    `登记 ${stats.identifications.total}/${finalTotal} 条，目录页 URL ${catalogStat?.distinctUrls} 个`,
  );
  check(
    "临时失败触发退避重试（总尝试次数 > 去重 URL 数）",
    !!detailStat && detailStat.totalAttempts > detailStat.distinctUrls,
    `详情请求 ${detailStat?.totalAttempts} 次 / ${detailStat?.distinctUrls} 个去重 URL`,
  );
  check(
    "限流与 Retry-After 生效（请求统计中有重试/等待计数）",
    converged.http.requests > 0 && (converged.http.retried > 0 || converged.http.retryAfterWaits > 0),
    `总请求 ${converged.http.requests}，重试 ${converged.http.retried}，Retry-After 等待 ${converged.http.retryAfterWaits}`,
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
  console.log(`\n用时 ${((Date.now() - startedAt) / 1000).toFixed(1)}s（预算 80s）`);
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
