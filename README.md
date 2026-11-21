# 区域岩芯资料馆 · 岩芯鉴定采集服务

用 TypeScript + Node.js + 嵌入式 DuckDB 从零实现的离线采集系统：内含一个可离线运行的
模拟馆藏站点，采集服务把站点上的岩芯鉴定同步到本地 DuckDB，并通过本地 HTTP 接口提供
**按岩芯编号查询最新已核实鉴定**与**采集进度**查询。

## 1. 能力与设计要点

| 需求 | 实现方式 |
| --- | --- |
| 分页 HTML 馆藏目录 | `GET /catalog?page=N`，页头标注总页数，条目内含重复链接 |
| 按发布顺序翻页的 JSON 更正列表 | `GET /corrections?page=N`，含全局发布序号 `publishedSeq` |
| 岩芯详情页 | `GET /core/:id`，同一岩芯多个修订共用同一地址，含「当前生效修订 + 修订历史」 |
| 当年鉴定当天更正 | 数据集内含相同日期、不同 revision 的修订 |
| 列表已更正、详情暂时仍旧修订 | 站点「详情生效时钟」控制，列表先公布、详情滞后生效；采集端保留 `PENDING` 并复查 |
| 重复链接 | 目录刻意输出重复 `<li>`，采集端按详情 URL 去重合并 |
| 临时拒绝 | 站点随机返回 503（带 `Retry-After`）与 500；采集端指数退避重试 |
| 更正列表错页 | 站点偶尔以 HTTP 200 返回别页正文且同一页连续多次；采集端按页头页码识别，本页留缺待补 |
| 每轮追更列表预算 | `CATCHUP_LIST_BUDGET` 限制每轮追更的更正列表请求数，重试按实际请求数计入；预算内旧缺页与末页都有检查机会 |
| 缺页恢复与末页追加 | `correction_pages` 逐页记录收妥状态；缺页由追更轮补齐，末页每轮复查以接住追加 |
| 强杀后不重读 | 已收妥的完整非尾页重启后不再请求；错页响应顺带收妥的页也跳过 |
| 同一待核实修订只合并采集一次 | `identifications` 以 `(core_id, revision)` 为主键；同 URL 在途请求共享响应 |
| 后续修订触发复查 | 每个 revision 独立判定，新修订登记为 `PENDING` 并重新拉取详情 |
| 核实后立即可查 | 标记 `VERIFIED` 后 `/cores/:id` 立刻返回（缺口之后的较晚更正也一样） |
| 增量进度只计连续核实范围 | `last_contiguous_verified_seq` 仅在发布序号连续且全部核实时向前推进；有缺页时停在缺口前 |
| 全量期间发布的更正能被增量接上 | 追更轮检查缺页与末页，upsert 幂等，天然接住遗漏修订 |
| 强制结束后续跑 | 全部断点、逐页收妥状态与核实结果落 DuckDB；重启读取 `run_state` / `correction_pages` 继续，残留 `RUNNING` 标记为 `INTERRUPTED` |
| 统一限流 / 在途上限 | 目录、列表、详情、重试请求共用一个 `HttpFetcher`：滑动窗口速率 + 信号量在途上限 |
| Retry-After 全局等待 | 收到等待时间后设置全局 `pausedUntil`，后续所有请求统一等待 |

## 2. 目录结构

```
src/
  config.ts            全部可调参数（环境变量覆盖）
  db.ts                DuckDB 存储层：建表、断点状态、核实/查询
  api.ts               本地 HTTP 查询与进度接口
  main.ts              入口（默认内嵌模拟站点 + 采集服务）
  collect/
    http.ts            统一 HTTP 客户端：限流、在途上限、退避重试、Retry-After
    parse.ts           目录/更正/详情解析
    collector.ts       全量 + 增量 + 断点续传编排
  mock/site.ts         离线模拟馆藏站点
  util/rng.ts          确定性随机工具
scripts/verify.ts      离线端到端验证（含 SIGKILL 重启续跑）
```

## 3. 快速开始

```bash
npm install
npm run serve          # 启动：内嵌模拟站点(9090) + 采集/查询服务(8080)，自动全量采集
```

全量完成后，服务按 `CATCHUP_INTERVAL_MS`（默认 3 秒）周期性增量追更正。

仅启动模拟站点（对接外部采集器时）：

```bash
npm run mock
EMBED_MOCK=0 MOCK_BASE_URL=http://127.0.0.1:9090 npm run serve
```

## 4. 离线验证命令（一条）

```bash
npm run verify
```

依赖安装后执行，约 35 秒输出可读结果（预算 90 秒内），覆盖：
SIGKILL 强制结束→重启续跑、重复链接合并、退避重试与 Retry-After、
更正列表错页（HTTP 200 返回别页正文）→ 旧缺页连续几轮拿不到、
缺口期间较晚更正已可查而连续进度停在缺口前、
缺口期间再次 SIGKILL→同库重启→缺页恢复与末页追加接上、
已收妥完整非尾页不重读（逐页请求次数比对）、每轮追更列表请求数 ≤ 预算、
详情滞后→时钟推进→增量衔接、按编号查询与站点终态逐一比对。
全部通过打印 `🎉 全部验证通过`，退出码 0。

## 5. HTTP 接口说明

基址 `http://127.0.0.1:8080`

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| GET | `/cores/:coreId` | 按岩芯编号查询**最新已核实鉴定**（无核实结果返回 404） |
| GET | `/cores?limit=&offset=` | 已核实岩芯分页列表 |
| GET | `/identifications/:coreId` | 该岩芯全部修订及各自核实状态 |
| GET | `/progress` | 采集进度（阶段、目录/更正游标、核实计数、连续核实序号、HTTP 统计） |
| GET | `/stats` | 汇总：URL 去重数与尝试次数、各状态运行计数 |
| GET | `/runs` / `/runs/:id` | 运行历史（含 `INTERRUPTED`/`COMPLETED`/`FAILED`） |
| POST | `/collect/run-full` | 手工触发全量 |
| POST | `/collect/catchup` | 手工触发一次增量追更正 |

### `/cores/:coreId` 响应示例

```json
{
  "coreId": "RC-0009",
  "revision": 3,
  "publishedSeq": 46,
  "publishedAt": "2026-10-12T09:30:00.000Z",
  "verifiedIdentification": {
    "classification": "灰岩",
    "formation": "青山组",
    "depthTop": 120,
    "depthBottom": 305,
    "verifier": "王核实",
    "summary": "岩芯 RC-0009 第 3 次修订（更正前次鉴定）"
  },
  "status": "VERIFIED",
  "attempts": 2
}
```

### `/progress` 关键字段

- `phase`：`IDLE` / `FULL` / `INCREMENTAL`
- `catalog` / `corrections`：分页游标与总页数（断点）
- `corrections.missingPages`：尚未完整收妥的更正列表页（旧缺页）
- `corrections.listBudget` / `corrections.listRequestsThisRun`：每轮追更列表请求预算与本轮已发生请求数（含重试）
- `details`：`total` / `verified` / `pending`
- `contiguousVerifiedSeq`：从上次进度起**连续核实**到的最大发布序号（增量进度口径）
- `fullCompleted`：全量是否完成
- `http`：累计请求数、重试次数、Retry-After 等待次数

## 6. 配置项（环境变量）

| 变量 | 默认值 | 含义 |
| --- | --- | --- |
| `PORT` | 8080 | 查询/进度服务端口 |
| `MOCK_PORT` | 9090 | 模拟站点端口 |
| `MOCK_BASE_URL` | 内嵌站点 | 采集目标根地址 |
| `DB_PATH` | ./data/archive.duckdb | DuckDB 数据文件 |
| `RATE_MAX_REQUESTS` / `RATE_WINDOW_MS` | 14 / 1000 | 每窗口最大请求数 |
| `MAX_INFLIGHT` | 6 | 全局在途请求上限 |
| `MAX_RETRIES` / `RETRY_BASE_MS` / `RETRY_MAX_MS` | 8 / 250 / 8000 | 退避重试参数 |
| `REQUEST_TIMEOUT_MS` | 8000 | 单次请求超时 |
| `CATCHUP_INTERVAL_MS` | 3000 | 增量追更周期 |
| `CATCHUP_LIST_BUDGET` | 8 | 每轮追更的更正列表请求预算（重试计入；预算内旧缺页与末页都有检查机会） |
| `AUTO_FULL` | true | 启动是否自动全量 |
| `EMBED_MOCK` | 1 | 是否进程内嵌模拟站点 |

## 7. 存储模型（DuckDB）

- `identifications`：每个 `(core_id, revision)` 一行，`PENDING`/`VERIFIED` 状态 + 核实内容 + 尝试次数
- `fetch_urls`：已抓取 URL（去重与尝试次数证据）
- `correction_pages`：更正列表逐页收妥状态（`COMPLETE`/`PENDING` + 尝试次数）；缺页 = 总页数内无 `COMPLETE` 行的页
- `runs`：每次全量/增量运行记录（含中断标记、该轮更正列表请求数 `list_requests`）
- `run_state`：单行断点表（目录/更正游标、连续核实序号、全量完成标志）

> 说明：duckdb 1.4.x 的 Node 绑定无法序列化 BIGINT、不支持绑定 `null`、参数需展开传递，
> 存储层已统一处理（序号列用 INTEGER、空值内联类型化 NULL、rest 参数展开），对上层透明。
