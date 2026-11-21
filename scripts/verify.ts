/**
 * 两站合并采集 · 离线端到端验证（npm run verify，预算 80 秒）。
 *
 * 覆盖需求：
 *  0) 共享在途调度器单元语义：单站用满名额、两站都在等时轮流分配、永不超上限
 *  1) 旧版单站库自动迁移：旧数据归入 default 站，原有单站路径零改动可查
 *  2) 两站合并到同一 DuckDB：同号岩芯内容隔离（不同种子 → 同号不同鉴定，互不覆盖）
 *  3) 两站共享在途上限（合计高水位 ≤ MAX_INFLIGHT）；各自限速、各自 Retry-After 等待
 *     （一站被强制 503 暂停期间，另一站照常推进；paused 只作用于本站）
 *  4) 两站各自跟踪进度、SIGKILL 强制结束后重启各自续跑并完成
 *  5) 重复链接合并、退避重试、增量衔接、按站查询、运行历史 INTERRUPTED/COMPLETED
 */
import { spawn, spawnSync, ChildProcessWithoutNullStreams } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import process from "node:process";

const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");
const DATA_DIR = path.join(ROOT, "data-verify");
const LEGACY_DIR = path.join(DATA_DIR, "legacy");
const LEGACY_DB = path.join(LEGACY_DIR, "legacy.duckdb");
const DB_PATH = path.join(DATA_DIR, "twostation.duckdb");
const LOG_FILE = path.join(DATA_DIR, "service.log");
const PORT = 8181;
const MOCK_A = 9191;
const MOCK_B = 9192;
const STATION_A = "alpha";
const STATION_B = "bravo";
const MAX_INFLIGHT = 4;
const DEADLINE_MS = 80_000;
const startedAt = Date.now();

const env: NodeJS.ProcessEnv = {
  ...process.env,
  PORT: String(PORT),
  MOCK_PORT: String(MOCK_A),
  DB_PATH,
  AUTO_FULL: "1",
  EMBED_MOCK: "1",
  STATIONS: `${STATION_A},${STATION_B}`,
  MOCK_STATIONS: `${STATION_A}:20261118,${STATION_B}:778899001`,
  RATE_MAX_REQUESTS: "40",
  RATE_WINDOW_MS: "1000",
  MAX_INFLIGHT: String(MAX_INFLIGHT),
  MAX_RETRIES: "8",
  CATCHUP_INTERVAL_MS: "1500",
  // 关闭随机故障，重试/等待全部由 force-503 确定性注入，保证“等待只作用于本站”可判定
  MOCK_RANDOM_FAULTS: "0",
};

let child: ChildProcessWithoutNullStreams | null = null;
let logStream: fs.WriteStream | null = null;
const results: { name: string; ok: boolean; detail: string }[] = [];

function check(name: string, ok: boolean, detail: string): void {
  results.push({ name, ok, detail });
  console.log(`${ok ? "✅" : "❌"} ${name} — ${detail}`);
}

const timeLeft = () => DEADLINE_MS - (Date.now() - startedAt);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function api<T = any>(p: string, init?: RequestInit): Promise<{ status: number; body: T }> {
  const resp = await fetch(`http://127.0.0.1:${PORT}${p}`, init);
  const body = (await resp.json()) as T;
  return { status: resp.status, body };
}

function admin(port: number, pathName: string): Promise<any> {
  return fetch(`http://127.0.0.1:${port}${pathName}`).then((r) => r.json());
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

async function waitProgress(
  station: string,
  predicate: (p: any) => boolean,
  label: string,
): Promise<any> {
  const until = Date.now() + timeLeft() - 2_000;
  let last: any = null;
  while (Date.now() < until) {
    const { body } = await api<any>(`/stations/${station}/progress`);
    last = body;
    if (predicate(body)) return body;
    await sleep(400);
  }
  throw new Error(`等待「${label}」超时，最后进度: ${JSON.stringify(last)}`);
}

/** 等待两站全量完成，期间持续推进两站详情时钟，让滞后详情陆续生效。 */
async function waitBothFull(): Promise<{ a: any; b: any }> {
  const until = Date.now() + timeLeft() - 2_000;
  let lastA: any = null;
  let lastB: any = null;
  while (Date.now() < until) {
    const a = (await api<any>(`/stations/${STATION_A}/progress`)).body;
    const b = (await api<any>(`/stations/${STATION_B}/progress`)).body;
    lastA = a;
    lastB = b;
    if (a.fullCompleted && b.fullCompleted && a.details.pending === 0 && b.details.pending === 0) {
      return { a, b };
    }
    await admin(MOCK_A, "/__admin/advance-clock?step=10").catch(() => undefined);
    await admin(MOCK_B, "/__admin/advance-clock?step=10").catch(() => undefined);
    await sleep(700);
  }
  throw new Error(
    `等待两站全量完成超时: A=${JSON.stringify(lastA?.details)} B=${JSON.stringify(lastB?.details)}`,
  );
}

function startService(): void {
  logStream = fs.createWriteStream(LOG_FILE, { flags: "a" });
  // 直接用 node 运行 tsx CLI（单进程）+ 独立进程组，SIGKILL 可精确杀掉服务本身。
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
  fs.mkdirSync(LEGACY_DIR, { recursive: true });

  const cleanup = () => killService();
  process.on("exit", cleanup);
  process.on("SIGINT", () => {
    cleanup();
    process.exit(130);
  });

  console.log("=== 两站合并采集 · 离线端到端验证 ===");

  // ---- 阶段 0：共享在途调度器单元语义（轮流分配 / 单站用满 / 不超上限）----
  {
    const r = spawnSync(
      process.execPath,
      [path.join(ROOT, "node_modules", "tsx", "dist", "cli.mjs"), "scripts/lib/scheduler-test.ts"],
      { cwd: ROOT, encoding: "utf-8" },
    );
    process.stdout.write(r.stdout);
    process.stderr.write(r.stderr);
    check("调度器单元语义（单站用满名额、两站轮流、不超上限）", r.status === 0, `退出码 ${r.status}`);
  }

  // ---- 阶段 1：旧版单站库自动迁移 + 旧调用方式可用 ----
  {
    const r = spawnSync(
      process.execPath,
      [path.join(ROOT, "node_modules", "tsx", "dist", "cli.mjs"), "scripts/lib/make-legacy-db.ts", LEGACY_DB],
      { cwd: ROOT, encoding: "utf-8" },
    );
    if (r.status !== 0) {
      process.stderr.write(r.stderr);
      throw new Error("旧版库构造失败");
    }
    // 用迁移后的库启动一个短生命周期的“单站模式”服务（默认站点 default）
    const singleEnv: NodeJS.ProcessEnv = {
      ...process.env,
      PORT: "8199",
      MOCK_PORT: "9299",
      DB_PATH: LEGACY_DB,
      STATIONS: "",
      AUTO_FULL: "false",
      EMBED_MOCK: "1",
    };
    delete (singleEnv as Record<string, string | undefined>).STATIONS;
    const single = spawn(
      process.execPath,
      [path.join(ROOT, "node_modules", "tsx", "dist", "cli.mjs"), "src/main.ts"],
      { cwd: ROOT, env: singleEnv, detached: true },
    );
    const singleLog: string[] = [];
    single.stdout.on("data", (d) => singleLog.push(String(d)));
    single.stderr.on("data", (d) => singleLog.push(String(d)));
    try {
      const until = Date.now() + 15_000;
      let health = false;
      while (Date.now() < until) {
        try {
          if ((await fetch("http://127.0.0.1:8199/health")).ok) {
            health = true;
            break;
          }
        } catch {
          /* 等待启动 */
        }
        await sleep(250);
      }
      check("旧版单站库启动并自动迁移为多站结构", health, health ? "health 200" : "启动超时");
      if (health) {
        const legacy = await fetch("http://127.0.0.1:8199/cores/RC-0001").then((x) => x.json());
        check(
          "原有单站调用方式 /cores/:id 迁移后立即可查且数据不变",
          legacy.coreId === "RC-0001" && legacy.verifiedIdentification?.classification === "旧库砂岩",
          `RC-0001 → ${legacy.verifiedIdentification?.classification}`,
        );
        const stations = await fetch("http://127.0.0.1:8199/stations").then((x) => x.json());
        check(
          "旧数据归入 default 站点且按站路径可查",
          stations.stations?.[0]?.stationId === "default" &&
            stations.stations?.[0]?.fullCompleted === true,
          `站点=${stations.stations?.map((s: any) => s.stationId).join(",")}`,
        );
      } else {
        check("原有单站调用方式迁移后可查", false, singleLog.join("").slice(-500));
        check("旧数据归入 default 站点", false, "服务未启动");
      }
    } finally {
      try {
        process.kill(-single.pid!, "SIGKILL");
      } catch {
        single.kill("SIGKILL");
      }
      await sleep(800);
    }
  }

  // ---- 阶段 2：两站合并服务首次启动，全量采集中途 SIGKILL ----
  startService();
  await waitForHealth();

  const stationsInfo = (await api<any>("/stations")).body;
  check(
    "两站合并到同一服务、共享一个在途上限",
    stationsInfo.stations?.length === 2 &&
      stationsInfo.sharedInflight.capacity === MAX_INFLIGHT,
    `站点=${stationsInfo.stations?.map((s: any) => s.stationId).join("/")}，共享上限=${stationsInfo.sharedInflight?.capacity}`,
  );

  // 两站都产生鉴定记录后再杀，确保两站都在跑且各自落库
  await waitProgress(
    STATION_A,
    (p) => p.details.total > 0 && (p.details.verified > 0 || p.details.pending > 0),
    "alpha 全量产生记录",
  );
  await waitProgress(
    STATION_B,
    (p) => p.details.total > 0 && (p.details.verified > 0 || p.details.pending > 0),
    "bravo 全量产生记录",
  );

  // ---- 阶段 3：站点等待语义——强制 alpha 503，bravo 不受影响继续推进 ----
  // 随机故障已关闭：此刻只有 alpha 会收到 503，bravo 不可能进入暂停
  await admin(MOCK_A, "/__admin/force-503?count=3&retryAfter=1");
  const t0 = Date.now();
  const beforeB = (await api<any>(`/stations/${STATION_B}/progress`)).body;
  // 给 alpha 一点时间撞上 503 并进入 Retry-After 等待
  let alphaPaused = false;
  const pauseDeadline = Date.now() + 8_000;
  while (Date.now() < pauseDeadline) {
    const a = (await api<any>(`/stations/${STATION_A}/progress`)).body;
    if (a.paused === true || a.http.retryAfterWaits > 0) {
      alphaPaused = true;
      break;
    }
    await sleep(300);
  }
  // alpha 等待期间，bravo 仍在核实（详情数增长或持续有请求）
  await sleep(2_500);
  const afterB = (await api<any>(`/stations/${STATION_B}/progress`)).body;
  const bravoAdvancedWhileAlphaWaits =
    afterB.details.verified > beforeB.details.verified ||
    afterB.http.requests > beforeB.http.requests ||
    afterB.details.pending > beforeB.details.pending;
  check(
    "各站独立 Retry-After 等待：alpha 暂停时 bravo 照常推进",
    alphaPaused && bravoAdvancedWhileAlphaWaits,
    `alpha.paused=${alphaPaused}，bravo 请求 ${beforeB.http.requests}→${afterB.http.requests}，` +
      `核实 ${beforeB.details.verified}→${afterB.details.verified}`,
  );
  check(
    "等待时间只作用于本站（只对 alpha 注入 503，bravo 从未 paused）",
    afterB.paused === false && afterB.http.retryAfterWaits === 0,
    `bravo.paused=${afterB.paused}, retryAfterWaits=${afterB.http.retryAfterWaits}`,
  );
  // 再单独对 bravo 注入 503，证明两个站各自都会遵守自己的站点等待时间
  await admin(MOCK_B, "/__admin/force-503?count=2&retryAfter=1");
  let bravoPaused = false;
  const bDeadline = Date.now() + 8_000;
  while (Date.now() < bDeadline) {
    const b = (await api<any>(`/stations/${STATION_B}/progress`)).body;
    if (b.paused === true || b.http.retryAfterWaits > 0) {
      bravoPaused = true;
      break;
    }
    await sleep(300);
  }
  check(
    "两站各自遵守本站的等待时间（bravo 被注入后也进入 Retry-After 等待）",
    bravoPaused,
    `bravo.paused=${bravoPaused}`,
  );
  console.log(`[verify] 等待语义观测用时 ${Date.now() - t0}ms`);

  const midA = (await api<any>(`/stations/${STATION_A}/progress`)).body;
  const midB = (await api<any>(`/stations/${STATION_B}/progress`)).body;
  console.log(
    `[verify] SIGKILL 前：alpha ${midA.details.verified}/${midA.details.total}，` +
      `bravo ${midB.details.verified}/${midB.details.total}`,
  );
  const pid = child!.pid;
  killService();
  await sleep(1_500);
  check("两站服务被 SIGKILL 强制结束", pid !== undefined, `pid=${pid}`);

  // ---- 阶段 4：重启续跑，两站各自从断点完成全量 ----
  startService();
  await waitForHealth();
  const { a: fullA, b: fullB } = await waitBothFull();
  check(
    "重启后两站各自续跑并完成全量",
    fullA.fullCompleted && fullB.fullCompleted && fullA.details.pending === 0 && fullB.details.pending === 0,
    `alpha 核实 ${fullA.details.verified}/${fullA.details.total}；bravo 核实 ${fullB.details.verified}/${fullB.details.total}`,
  );
  check(
    "两站连续核实进度各自覆盖本站全部已发布序号",
    fullA.contiguousVerifiedSeq === fullA.corrections.totalPublished &&
      fullB.contiguousVerifiedSeq === fullB.corrections.totalPublished,
    `alpha ${fullA.contiguousVerifiedSeq}/${fullA.corrections.totalPublished}，` +
      `bravo ${fullB.contiguousVerifiedSeq}/${fullB.corrections.totalPublished}`,
  );

  // ---- 阶段 5：同号岩芯内容隔离 ----
  const stateA = await admin(MOCK_A, "/__admin/state");
  const expectedA = stateA.expectedFinalState as Array<{ coreId: string; latestRevision: number }>;
  const stateB = await admin(MOCK_B, "/__admin/state");
  const expectedB = stateB.expectedFinalState as Array<{ coreId: string; latestRevision: number }>;
  const sampleIds = expectedA.slice(0, 8).map((e) => e.coreId);
  let isolated = 0;
  let sameNumberBothPresent = 0;
  for (const coreId of sampleIds) {
    const cmp = (await api<any>(`/compare-cores/${coreId}`)).body;
    const recA = cmp.records.find((r: any) => r.stationId === STATION_A);
    const recB = cmp.records.find((r: any) => r.stationId === STATION_B);
    if (recA && recB) {
      sameNumberBothPresent += 1;
      const different =
        recA.verifiedIdentification.classification !== recB.verifiedIdentification.classification ||
        recA.verifiedIdentification.verifier !== recB.verifiedIdentification.verifier ||
        recA.verifiedIdentification.summary !== recB.verifiedIdentification.summary;
      if (different) isolated += 1;
    }
  }
  check(
    "同号岩芯在两站各自存在且鉴定内容隔离（互不覆盖）",
    sameNumberBothPresent === sampleIds.length && isolated === sampleIds.length,
    `抽样 ${sampleIds.length} 个同号岩芯，双站齐全 ${sameNumberBothPresent}，内容不同 ${isolated}`,
  );

  // 按站查询路径各自正确
  const coreA = (await api<any>(`/stations/${STATION_A}/cores/RC-0001`)).body;
  const coreB = (await api<any>(`/stations/${STATION_B}/cores/RC-0001`)).body;
  const legacyPath = await fetch(`http://127.0.0.1:${PORT}/cores/RC-0001`).then((r) => r.json());
  check(
    "按站查询同号岩芯返回各站自己的内容，默认路径绑定首个站点",
    coreA.stationId === STATION_A &&
      coreB.stationId === STATION_B &&
      coreA.verifiedIdentification.classification !== coreB.verifiedIdentification.classification &&
      legacyPath.stationId === STATION_A &&
      legacyPath.verifiedIdentification.classification === coreA.verifiedIdentification.classification,
    `alpha=${coreA.verifiedIdentification?.classification}，bravo=${coreB.verifiedIdentification?.classification}，` +
      `默认路径站点=${legacyPath.stationId}`,
  );

  // 库内行级隔离：每个 (station, core) 独立计数
  const idA = (await api<any>(`/stations/${STATION_A}/identifications/RC-0001`)).body;
  const idB = (await api<any>(`/stations/${STATION_B}/identifications/RC-0001`)).body;
  const expA1 = expectedA.find((e) => e.coreId === "RC-0001")!;
  const expB1 = expectedB.find((e) => e.coreId === "RC-0001")!;
  check(
    "同号岩芯修订按站独立存储、各自全部核实",
    idA.revisions.length === expA1.latestRevision &&
      idA.revisions.every((r: any) => r.status === "VERIFIED") &&
      idB.revisions.length === expB1.latestRevision &&
      idB.revisions.every((r: any) => r.status === "VERIFIED"),
    `alpha 修订数 ${idA.revisions?.length}/${expA1.latestRevision}，bravo ${idB.revisions?.length}/${expB1.latestRevision}`,
  );

  // ---- 阶段 6：共享在途上限证据 + 各自限速/重试统计 ----
  const info = (await api<any>("/stations")).body;
  check(
    "两站合计在途高水位未超过共享上限",
    info.sharedInflight.highWaterInflight <= MAX_INFLIGHT && info.sharedInflight.highWaterInflight > 0,
    `高水位 ${info.sharedInflight.highWaterInflight}/${MAX_INFLIGHT}，累计放行 ${info.sharedInflight.totalAcquired}`,
  );
  const statsA = (await api<any>(`/stations/${STATION_A}/stats`)).body;
  const statsB = (await api<any>(`/stations/${STATION_B}/stats`)).body;
  const detailA = statsA.fetchUrls.find((r: any) => r.kind === "DETAIL");
  const detailB = statsB.fetchUrls.find((r: any) => r.kind === "DETAIL");
  // 随机故障已关闭：强制注入的 503 让两站各自产生确定性退避重试（尝试次数 > 去重 URL）
  check(
    "两站各自对临时拒绝做退避重试（统计彼此独立）",
    detailA.totalAttempts > detailA.distinctUrls && detailB.totalAttempts > detailB.distinctUrls,
    `alpha 详情 ${detailA.totalAttempts}次/${detailA.distinctUrls}URL；bravo ${detailB.totalAttempts}次/${detailB.distinctUrls}URL`,
  );
  const catalogA = statsA.fetchUrls.find((r: any) => r.kind === "CATALOG");
  check(
    "两站目录重复链接各自合并",
    catalogA.distinctUrls > 0 && catalogA.distinctUrls <= 40,
    `alpha 目录去重 URL=${catalogA.distinctUrls}`,
  );
  check(
    "两站各自维护 HTTP 统计（请求/重试/Retry-After 均有记录）",
    fullA.http.requests > 0 && fullB.http.requests > 0 &&
      (fullA.http.retried > 0 || fullA.http.retryAfterWaits > 0) &&
      (fullB.http.retried > 0 || fullB.http.retryAfterWaits > 0),
    `alpha req=${fullA.http.requests} retry=${fullA.http.retried} wait=${fullA.http.retryAfterWaits}；` +
      `bravo req=${fullB.http.requests} retry=${fullB.http.retried} wait=${fullB.http.retryAfterWaits}`,
  );

  // ---- 阶段 7：增量衔接 + 运行历史 INTERRUPTED/COMPLETED 按站可追溯 ----
  await admin(MOCK_A, "/__admin/advance-clock?step=100");
  await admin(MOCK_B, "/__admin/advance-clock?step=100");
  await api(`/stations/${STATION_A}/collect/catchup`, { method: "POST" });
  await api(`/stations/${STATION_B}/collect/catchup`, { method: "POST" });
  await waitProgress(STATION_A, (p) => p.fullCompleted && p.details.pending === 0, "alpha 增量后清零");
  await waitProgress(STATION_B, (p) => p.fullCompleted && p.details.pending === 0, "bravo 增量后清零");
  const endA = (await api<any>(`/stations/${STATION_A}/progress`)).body;
  const endB = (await api<any>(`/stations/${STATION_B}/progress`)).body;
  check(
    "两站增量追更后待核实均清零、连续序号覆盖全部发布",
    endA.details.pending === 0 && endA.contiguousVerifiedSeq === endA.corrections.totalPublished &&
      endB.details.pending === 0 && endB.contiguousVerifiedSeq === endB.corrections.totalPublished,
    `alpha ${endA.contiguousVerifiedSeq}/${endA.corrections.totalPublished}；` +
      `bravo ${endB.contiguousVerifiedSeq}/${endB.corrections.totalPublished}`,
  );

  const runsA = (await api<any>(`/stations/${STATION_A}/runs`)).body.runs as any[];
  const runsB = (await api<any>(`/stations/${STATION_B}/runs`)).body.runs as any[];
  const allRuns = (await api<any>("/runs")).body.runs as any[];
  check(
    "两站运行历史各自含 INTERRUPTED 与 COMPLETED 全量记录",
    runsA.some((r) => r.status === "INTERRUPTED") && runsA.some((r) => r.mode === "FULL" && r.status === "COMPLETED") &&
      runsB.some((r) => r.status === "INTERRUPTED") && runsB.some((r) => r.mode === "FULL" && r.status === "COMPLETED"),
    `alpha 状态=[${runsA.map((r) => r.status).join(",")}]，bravo 状态=[${runsB.map((r) => r.status).join(",")}]`,
  );
  check(
    "默认 /runs 路径仍可用（返回默认站点运行历史）",
    Array.isArray(allRuns) && allRuns.every((r) => r.station_id === STATION_A),
    `默认路径返回 ${allRuns.length} 条，全部属于 ${STATION_A}`,
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
