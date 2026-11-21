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
  maxInflight: num("MAX_INFLIGHT", 6),
  /** 临时失败重试次数（指数退避 + 抖动） */
  maxRetries: num("MAX_RETRIES", 8),
  retryBaseMs: num("RETRY_BASE_MS", 250),
  retryMaxMs: num("RETRY_MAX_MS", 8000),
  /** 单次请求超时 */
  requestTimeoutMs: num("REQUEST_TIMEOUT_MS", 8000),
  /** 全量完成后定期追更正的间隔 */
  catchupIntervalMs: num("CATCHUP_INTERVAL_MS", 3000),
  /** 服务启动后是否自动执行全量采集 */
  autoFull: bool("AUTO_FULL", true),
  /** 模拟站点：启动时已公布的修订数（之后按批自行公布，无需外部推进） */
  mockInitialPublished: num("MOCK_INITIAL_PUBLISHED", 35),
  /** 模拟站点：每批自行公布的修订数 / 公布间隔 */
  mockPublishBatch: num("MOCK_PUBLISH_BATCH", 6),
  mockPublishIntervalMs: num("MOCK_PUBLISH_INTERVAL_MS", 3000),
  /** 模拟站点：启动时详情已生效到的发布序号（之后自行推进，滞后于公布） */
  mockInitialDetail: num("MOCK_INITIAL_DETAIL", 20),
  mockDetailBatch: num("MOCK_DETAIL_BATCH", 5),
  mockDetailIntervalMs: num("MOCK_DETAIL_INTERVAL_MS", 2500),
} as const;

export type AppConfig = typeof config;
