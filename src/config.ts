/**
 * 全局配置：所有参数均可通过环境变量覆盖。
 *
 * 单站模式（默认，沿用原有调用方式）：
 *   MOCK_BASE_URL / MOCK_PORT 指定唯一站点，站点 id 为 "default"。
 *
 * 两站模式：
 *   STATIONS="id1=baseUrl1,id2=baseUrl2" 显式配置两个采集站点；
 *   EMBED_MOCK=1 时可再用 MOCK_STATIONS="id1:seed1,id2:seed2" 在进程内嵌两个模拟站点，
 *   端口依次取 MOCK_PORT、MOCK_PORT+1（也可用 STATION_PORTS 覆盖）。
 *
 * 速率与在途：
 *   MAX_INFLIGHT 为两站共享的全局在途上限；
 *   RATE_MAX_REQUESTS/RATE_WINDOW_MS 为每个站点各自遵守的限速，
 *   也可用 RATE_MAX_REQUESTS_<ID> / RATE_WINDOW_MS_<ID> 按站覆盖。
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

export interface StationConfig {
  id: string;
  baseUrl: string;
  /** 内嵌模拟站点时使用的确定性种子；外部站点为 null。 */
  seed: number | null;
  rateMaxRequests: number;
  rateWindowMs: number;
}

function parseStations(): StationConfig[] {
  const defaultPort = num("MOCK_PORT", 9090);
  const defaultBase = str("MOCK_BASE_URL", `http://127.0.0.1:${defaultPort}`);
  const raw = process.env.STATIONS?.trim();

  // 单站模式：完全沿用原有 MOCK_BASE_URL/MOCK_PORT 语义
  if (!raw) {
    return [
      {
        id: "default",
        baseUrl: defaultBase,
        seed: null,
        rateMaxRequests: num("RATE_MAX_REQUESTS", 14),
        rateWindowMs: num("RATE_WINDOW_MS", 1000),
      },
    ];
  }

  // 两站模式："id=baseUrl" 显式给外部地址；只写 "id" 时 baseUrl 留空，
  // 由内嵌模拟站点按 MOCK_PORT + 序号自动补齐（EMBED_MOCK=0 且留空会在启动时报错）。
  const stations: StationConfig[] = [];
  for (const part of raw.split(",")) {
    const item = part.trim();
    if (!item) continue;
    const eq = item.indexOf("=");
    const id = (eq >= 0 ? item.slice(0, eq) : item).trim();
    const baseUrl = eq >= 0 ? item.slice(eq + 1).trim() : "";
    if (!/^[A-Za-z0-9][A-Za-z0-9\-_]*$/.test(id)) {
      throw new Error(`站点 id 只允许字母数字、-、_ 且以字母数字开头: ${id}`);
    }
    stations.push({
      id,
      baseUrl,
      seed: null,
      rateMaxRequests: num(`RATE_MAX_REQUESTS_${id.toUpperCase()}`, num("RATE_MAX_REQUESTS", 14)),
      rateWindowMs: num(`RATE_WINDOW_MS_${id.toUpperCase()}`, num("RATE_WINDOW_MS", 1000)),
    });
  }
  if (stations.length === 0) throw new Error("STATIONS 已设置但未解析出任何站点");
  const ids = new Set(stations.map((s) => s.id));
  if (ids.size !== stations.length) throw new Error("STATIONS 中存在重复站点 id");
  return stations;
}

export const config = {
  /** 本地查询/采集服务端口 */
  httpPort: num("PORT", 8080),
  /** 离线模拟站点端口（单站；两站内嵌时为第一个站的端口） */
  mockPort: num("MOCK_PORT", 9090),
  /** 模拟站点根地址（单站模式使用） */
  mockBaseUrl: str("MOCK_BASE_URL", `http://127.0.0.1:${num("MOCK_PORT", 9090)}`),
  /** DuckDB 数据文件（两站合并写入同一个库） */
  dbPath: str("DB_PATH", "./data/archive.duckdb"),
  /** 采集站点列表：单站模式只有一个 id=default 的站 */
  stations: parseStations(),
  /** 每站限速默认值（滑动窗口）；按站覆盖见 StationConfig */
  rateMaxRequests: num("RATE_MAX_REQUESTS", 14),
  rateWindowMs: num("RATE_WINDOW_MS", 1000),
  /** 两站共享的全局在途请求上限（所有站点、所有类型请求共用） */
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
} as const;

export type AppConfig = typeof config;
