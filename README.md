# 区域岩芯资料馆 · 岩芯鉴定采集服务

用 TypeScript + Node.js + 嵌入式 DuckDB 从零实现的离线采集系统：内含一个可离线运行的
模拟馆藏站点，采集服务把站点上的岩芯鉴定同步到本地 DuckDB，并通过本地 HTTP 接口提供
**按岩芯编号查询最新已核实鉴定**与**采集进度**查询。

## 1. 能力与设计要点

| 需求 | 实现方式 |
| --- | --- |
| 分页 HTML 馆藏目录 | `GET /catalog?page=N`，页头标注总页数，条目内含重复链接 |
| 按发布顺序翻页的 JSON 更正列表 | `GET /corrections?page=N`，含全局发布序号 `publishedSeq`；`PUBLISH`/`WITHDRAW` 两类事件，撤回公告同样占用发布序号 |
| 岩芯详情页 | `GET /core/:id`，同一岩芯多个修订/批次共用同一地址，含「当前生效修订 + 修订历史」 |
| 当年鉴定当天更正 | 数据集内含相同日期、不同 revision 的修订 |
| 列表已更正、详情暂时仍旧修订 | 站点「详情生效时钟」控制，列表先公布、详情滞后生效；采集端保留 `PENDING` 并复查 |
| 撤回公告入库、撤回占发布序号 | `announcements` 台账按 `published_seq` 记录每条公告；撤回把对应批次置 `WITHDRAWN`，原发布与撤回都计入已处置 |
| 撤回后查询回退 | `/cores/:id` 只在 `VERIFIED` 行中取最新（revision、issue 倒序），撤回后自然回退到仍有效的最新已核实版本；无有效版本返回 404 |
| 撤回后沿用原修订号重发 | `identifications` 主键含发布批次 `issue`：原始发布 issue=1，同号重发 issue 递增；重发核实前详情仍旧、查询不切换 |
| 连续进度遇缺口即停 | 公告口径逐序号判定已处置（含撤回）；撤回本身占号但不把未核实的重发缺口带过，进度停在缺口前 |
| 重复链接 | 目录刻意输出重复 `<li>`，采集端按详情 URL 去重合并 |
| 临时拒绝 | 站点随机返回 503（带 `Retry-After`）与 500；采集端指数退避重试 |
| 同一待核实修订只合并采集一次 | `identifications` 以 `(core_id, revision, issue)` 为主键；同 URL 在途请求共享响应 |
| 后续修订触发复查 | 每个 revision/issue 独立判定，新修订登记为 `PENDING` 并重新拉取详情 |
| 核实后立即可查 | 标记 `VERIFIED` 后 `/cores/:id` 立刻返回 |
| 增量进度只计连续处置范围 | `last_contiguous_verified_seq` 仅在发布序号连续且全部已处置（核实或撤回）时向前推进 |
| 全量期间发布的更正能被增量接上 | 增量从第 1 页重扫更正列表，upsert 幂等，天然接住遗漏修订/撤回 |
| 强制结束后续跑 | 全部断点、核实与撤回状态落 DuckDB；重启读取 `run_state` 继续，残留 `RUNNING` 标记为 `INTERRUPTED` |
| 旧库迁移 | 旧版 `(core_id, revision)` 主键库启动时自动重建为含 `issue`/`withdraw_seq` 的新结构，历史数据归为 issue=1 |
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

依赖安装后执行，约 25 秒输出可读结果（预算 110 秒内），覆盖：
SIGKILL 强制结束→重启续跑、重复链接合并、退避重试与 Retry-After、
详情滞后→时钟推进→增量衔接、撤回公告占号与已处置口径、撤回后查询回退/404、
同号重发核实前不切换与缺口处停止、二次重启后区分核实/撤回/重发三态、
按编号查询与站点终态逐一比对。
全部通过打印 `🎉 全部验证通过`，退出码 0。

## 5. HTTP 接口说明

基址 `http://127.0.0.1:8080`

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| GET | `/cores/:coreId` | 按岩芯编号查询**最新仍有效已核实鉴定**（撤回后回退；无有效版本返回 404） |
| GET | `/cores?limit=&offset=` | 已核实岩芯分页列表（每岩芯取最新有效批次） |
| GET | `/identifications/:coreId` | 该岩芯全部修订/批次及各自状态（`PENDING`/`VERIFIED`/`WITHDRAWN`） |
| GET | `/announcements?limit=&offset=` | 更正公告台账：发布与撤回事件各占一个发布序号 |
| GET | `/progress` | 采集进度（阶段、目录/更正游标、核实/撤回计数、公告处置数、连续进度序号、HTTP 统计） |
| GET | `/stats` | 汇总：URL 去重数与尝试次数、各状态运行计数、撤回计数 |
| GET | `/runs` / `/runs/:id` | 运行历史（含 `INTERRUPTED`/`COMPLETED`/`FAILED` 与撤回计数） |
| POST | `/collect/run-full` | 手工触发全量 |
| POST | `/collect/catchup` | 手工触发一次增量追更正 |

### 撤回与同号重发的状态语义

- 更正列表事件分两类：`PUBLISH`（发布/重发）与 `WITHDRAW`（撤回公告），二者都占用单调的
  全局 `publishedSeq`，序号全局唯一、不回收。
- 同一修订号可被多次发布：`issue`（发布批次）区分，原始发布为 1，撤回后沿用原修订号重发递增。
- 撤回把对应批次置为 `WITHDRAWN` 并记录 `withdrawSeq`；查询只看 `VERIFIED` 行，因此：
  - 撤回的是最新修订 → 回退到上一仍有效的已核实版本；全部被撤回 → 404；
  - 同号重发未核实前（详情仍旧）→ 保持 `PENDING`，查询不切换，核实通过后才切到新批次。
- 连续进度按公告序号逐格判定「已处置」（`VERIFIED`/`WITHDRAWN` 或撤回公告本身）；
  撤回占号但不会替未核实的重发补缺口，进度停在缺口前，序号只前进不回退。

### `/cores/:coreId` 响应示例

```json
{
  "coreId": "RC-0009",
  "revision": 3,
  "issue": 1,
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
- `details`：`total` / `verified` / `pending` / `withdrawn`
- `announcements`：公告台账 `total` / `publishes` / `withdraws` / `disposed`（原发布与撤回都计入已处置）
- `contiguousVerifiedSeq`：从上次进度起**连续处置**到的最大发布序号（撤回占号、缺口前停止）
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
| `AUTO_FULL` | true | 启动是否自动全量 |
| `EMBED_MOCK` | 1 | 是否进程内嵌模拟站点 |

## 7. 存储模型（DuckDB）

- `identifications`：每个 `(core_id, revision, issue)` 一行，`PENDING`/`VERIFIED`/`WITHDRAWN`
  状态 + 核实内容 + 撤回序号 `withdraw_seq` + 尝试次数；`issue` 区分原始发布与同号重发
- `announcements`：更正公告台账，按 `published_seq` 主键记录 `PUBLISH`/`WITHDRAW` 事件（撤回也占号）
- `fetch_urls`：已抓取 URL（去重与尝试次数证据）
- `runs`：每次全量/增量运行记录（含中断标记与撤回计数 `details_withdrawn`）
- `run_state`：单行断点表（目录/更正游标、连续进度序号、全量完成标志）

旧版数据库（`(core_id, revision)` 主键、无 `issue`）在首次打开时自动迁移：重建主键并把
历史行归为 issue=1，随后补齐 `announcements` 等新表，迁移结果持久、重启不重复执行。

> 说明：duckdb 1.4.x 的 Node 绑定无法序列化 BIGINT、不支持绑定 `null`、参数需展开传递，
> 存储层已统一处理（序号列用 INTEGER、空值内联类型化 NULL、rest 参数展开），对上层透明。
