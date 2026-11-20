/**
 * 全局配置：所有参数均可通过环境变量覆盖。
 */
function num(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === "") return fallback;
  const v = Number(raw);
  if (!Number.isFinite(v)) throw new Error(`环境变量 ${name} 不是合法数字: ${raw}`);
  return v;
}

function str(name: string, fallback: string): string {
  const raw = process.env[name];
  return raw === undefined || raw.trim() === "" ? fallback : raw;
}

function bool(name: string, fallback: boolean): boolean {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === "") return fallback;
  return /^(1|true|yes|on)$/i.test(raw.trim());
}

const maxInflight = num("MAX_INFLIGHT", 6);

export const config = {
  /** 本地查询/采集服务端口 */
  httpPort: num("PORT", 8080),
  /** 离线模拟馆藏站点端口 */
  mockPort: num("MOCK_PORT", 9090),
  /** 模拟站点根地址 */
  mockBaseUrl: str("MOCK_BASE_URL", `http://127.0.0.1:${num("MOCK_PORT", 9090)}`),
  /** DuckDB 数据文件 */
  dbPath: str("DB_PATH", "./data/archive.duckdb"),
  /** 全局限流：每 windowMs 毫秒最多 maxRequests 个请求（目录/列表/详情/重试共用） */
  rateMaxRequests: num("RATE_MAX_REQUESTS", 14),
  rateWindowMs: num("RATE_WINDOW_MS", 1000),
  /** 全局在途请求上限（所有类型请求共用） */
  maxInflight,
  /** 临时失败重试次数（指数退避 + 抖动） */
  maxRetries: num("MAX_RETRIES", 8),
  retryBaseMs: num("RETRY_BASE_MS", 250),
  retryMaxMs: num("RETRY_MAX_MS", 8000),
  /** 单次请求超时 */
  requestTimeoutMs: num("REQUEST_TIMEOUT_MS", 8000),
  /** 全量完成后定期追更正的间隔 */
  catchupIntervalMs: num("CATCHUP_INTERVAL_MS", 3000),
  /** 单次增量最多占用多少毫秒推进详情（积压时不会无限占住运行状态） */
  catchupDrainBudgetMs: Math.max(1, num("CATCHUP_DRAIN_BUDGET_MS", 15_000)),
  /** 服务启动后是否自动执行全量采集 */
  autoFull: bool("AUTO_FULL", true),

  // ---------------- 详情调度器 ----------------
  /**
   * 调度器内存中同时容纳的详情地址上限（排队 + 执行 + 等待复查/退避）。
   * 需求为最多 64 个，这里做硬上限钳制；积压上万条时按批从库中取。
   */
  schedulerWindow: Math.min(64, Math.max(1, num("SCHEDULER_WINDOW", 64))),
  /** 详情请求并发（小于全局在途上限，给目录/更正列表留出额度） */
  detailConcurrency: Math.max(1, num("DETAIL_CONCURRENCY", Math.max(1, maxInflight - 2))),
  /** 调度器巡检间隔 */
  schedulerTickMs: num("SCHEDULER_TICK_MS", 50),
  /** 每轮巡检最多让出窗口槽位的等待地址数（积压时防止空转） */
  evictionBudget: Math.max(1, num("SCHEDULER_EVICTION_BUDGET", 8)),
  /** 详情页仍显示旧修订时的复查间隔（按尝试次数指数增长，封顶） */
  detailRecheckBaseMs: num("DETAIL_RECHECK_BASE_MS", 1000),
  detailRecheckMaxMs: num("DETAIL_RECHECK_MAX_MS", 8000),
  /** 单个详情地址失败后的指数退避参数 */
  detailBackoffBaseMs: num("DETAIL_BACKOFF_BASE_MS", 250),
  detailBackoffMaxMs: num("DETAIL_BACKOFF_MAX_MS", 8000),
} as const;

export type AppConfig = typeof config;
