# 区域岩芯资料馆 · 岩芯鉴定采集服务（两站合并版）

用 TypeScript + Node.js + 嵌入式 DuckDB 从零实现的离线采集系统：内含可离线运行的
模拟馆藏站点，采集服务把**两个站点**上的岩芯鉴定同步到**同一个 DuckDB**，并通过本地
HTTP 接口提供**按站点 + 岩芯编号查询最新已核实鉴定**、**同号岩芯跨站对比**与**按站采集进度**查询。
单站模式与旧版接口完全兼容，旧版 DuckDB 数据文件启动时自动迁移。

## 1. 能力与设计要点

| 需求 | 实现方式 |
| --- | --- |
| 两站合并到同一 DuckDB 服务 | 一个进程内嵌/对接两个站点，各跑一个 `Collector`，业务表全部带 `station_id`、主键以其打头 |
| 按站查询同号岩芯 | `/stations/:stationId/cores/:coreId`；`/compare-cores/:coreId` 一次返回两站同号内容做隔离对比 |
| 同号岩芯内容隔离 | 主键 `(station_id, core_id, revision)`：两站同编号岩芯各自独立成行，互不覆盖 |
| 两站共享在途上限 | `SharedSlotScheduler` 全局信号量，容量 `MAX_INFLIGHT`，两站所有请求共用 |
| 单站就绪时用满空闲名额 | 每站独立 FIFO 队列，无竞争时单站可连续拿完全部名额（单测覆盖） |
| 两站都在等且不限速时轮流分配 | 名额释放时在有等待者的站之间 round-robin 放行，严格交替、互不饿死（单测覆盖） |
| 各自遵守限速 | 每个 `StationFetcher` 独立滑动窗口，可用 `RATE_MAX_REQUESTS_<站点>` 按站覆盖 |
| 各自站点等待时间 | 每站独立 `pausedUntil`（Retry-After 只暂停本站），等待期间不占共享名额，另一站照常推进 |
| 分页 HTML 馆藏目录 | `GET /catalog?page=N`，页头标注总页数，条目内含重复链接 |
| 按发布顺序翻页的 JSON 更正列表 | `GET /corrections?page=N`，含全局发布序号 `publishedSeq` |
| 岩芯详情页 | `GET /core/:id`，同一岩芯多个修订共用同一地址，含「当前生效修订 + 修订历史」 |
| 当年鉴定当天更正 | 数据集内含相同日期、不同 revision 的修订 |
| 列表已更正、详情暂时仍旧修订 | 站点「详情生效时钟」控制，列表先公布、详情滞后生效；采集端保留 `PENDING` 并复查 |
| 重复链接 | 目录刻意输出重复 `<li>`，采集端按（站内）详情 URL 去重合并 |
| 临时拒绝 | 站点随机返回 503（带 `Retry-After`）与 500；采集端指数退避重试，另有可确定性注入的 `force-503` |
| 同一待核实修订只合并采集一次 | `identifications` 以 `(station_id, core_id, revision)` 为主键；同 URL 在途请求共享响应 |
| 后续修订触发复查 | 每个 revision 独立判定，新修订登记为 `PENDING` 并重新拉取详情 |
| 核实后立即可查 | 标记 `VERIFIED` 后对应站点接口立刻返回 |
| 增量进度只计连续核实范围 | `last_contiguous_verified_seq` 仅在发布序号连续且全部核实时向前推进（按站独立） |
| 全量期间发布的更正能被增量接上 | 增量从第 1 页重扫更正列表，upsert 幂等，天然接住遗漏修订 |
| 强制结束后续跑 | 全部断点、HTTP 统计与核实结果落 DuckDB；重启读 `run_state` 继续，残留 `RUNNING` 标为 `INTERRUPTED` |
| 统一限流 / 在途上限 | 目录、列表、详情、重试请求都经 `StationFetcher`：本站速率窗口 + 两站共享信号量 |
| Retry-After 站点等待 | 收到等待时间后设置本站 `pausedUntil`，只暂停该站后续请求；计数同步落盘，SIGKILL 不丢 |
| 旧单站库与旧调用继续可用 | 启动自动迁移旧表（归入站点 `default`）；旧路径 `/cores/:id` 等全部保留并绑定首个站点 |

## 2. 目录结构

```
src/
  config.ts            全部可调参数（环境变量覆盖），含 STATIONS 多站解析
  db.ts                DuckDB 存储层：多站建表、旧库自动迁移、按站断点/核实/查询
  api.ts               本地 HTTP 查询与进度接口（旧路径 + /stations/* + /compare-cores/*）
  main.ts              入口（内嵌一个或两个模拟站点 + 采集服务）
  collect/
    http.ts            SharedSlotScheduler（共享在途/轮流）+ StationFetcher（本站限速/等待/重试）
    parse.ts           目录/更正/详情解析
    collector.ts       单站全量 + 增量 + 断点续传编排（每站一个实例）
  mock/site.ts         离线模拟馆藏站点（可配种子/随机故障开关/强制 503/在途观测）
  util/rng.ts          确定性随机工具
scripts/
  verify.ts            离线端到端验证（含 SIGKILL 重启续跑，预算 80 秒）
  lib/scheduler-test.ts   共享在途调度器公平性单元测试
  lib/make-legacy-db.ts   构造旧版单站库，验证自动迁移与旧调用兼容
```

## 3. 快速开始

```bash
npm install
npm run serve          # 单站模式（默认，行为同旧版）：内嵌模拟站点(9090) + 采集/查询服务(8080)
```

两站模式（两个内嵌模拟站点，端口 9090、9091，同号岩芯内容由不同种子产生）：

```bash
STATIONS="alpha,bravo" npm run serve

# 对接两个外部站点（也可只给部分 URL，其余用内嵌站点）：
STATIONS="alpha=http://127.0.0.1:9090,bravo=http://127.0.0.1:9091" EMBED_MOCK=0 npm run serve
```

全量完成后，各站按 `CATCHUP_INTERVAL_MS`（默认 3 秒）周期性增量追更正。

仅启动模拟站点（对接外部采集器时，单站）：

```bash
npm run mock
EMBED_MOCK=0 MOCK_BASE_URL=http://127.0.0.1:9090 npm run serve
```

## 4. 离线验证命令（一条，80 秒内）

```bash
npm run verify
```

依赖安装后执行，实测约 12 秒输出可读结果（预算 80 秒），覆盖：
调度器公平性单测（单站用满名额、两站轮流、不超上限）、旧库自动迁移与旧调用兼容、
两站同号岩芯内容隔离、独立 Retry-After 等待（一站暂停另一站照常推进）、
两站共享在途上限、SIGKILL 强制结束→两站各自重启续跑、重复链接合并、退避重试、
详情滞后→时钟推进→增量衔接、按站查询与站点终态逐一比对。
全部通过打印 `🎉 全部验证通过`，退出码 0。

## 5. HTTP 接口说明

基址 `http://127.0.0.1:8080`

### 多站与跨站

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/stations` | 站点列表 + 各站进度快照 + 共享在途调度器观测（容量/当前/高水位/累计放行） |
| GET | `/stations/:stationId/progress` | 指定站采集进度 |
| GET | `/stations/:stationId/cores/:coreId` | 按站按编号查询最新已核实鉴定 |
| GET | `/stations/:stationId/cores?limit=&offset=` | 指定站已核实岩芯分页 |
| GET | `/stations/:stationId/identifications/:coreId` | 指定站该岩芯全部修订及状态 |
| GET | `/stations/:stationId/stats` | 指定站 URL 去重、尝试次数、运行计数 |
| GET | `/stations/:stationId/runs` / `/runs/:id` | 指定站运行历史 |
| POST | `/stations/:stationId/collect/run-full` | 触发指定站全量 |
| POST | `/stations/:stationId/collect/catchup` | 触发指定站一次增量 |
| GET | `/compare-cores/:coreId` | 跨站对比同号岩芯已核实内容（内容隔离证据） |

### 旧版单站路径（保留，绑定配置中的第一个站点）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查（返回站点 id 列表） |
| GET | `/cores/:coreId` | 按岩芯编号查询**默认站点**最新已核实鉴定（无结果 404） |
| GET | `/cores?limit=&offset=` | 默认站点已核实岩芯分页 |
| GET | `/identifications/:coreId` | 默认站点该岩芯全部修订及状态 |
| GET | `/progress` | 默认站点采集进度（阶段、游标、计数、连续序号、HTTP 统计） |
| GET | `/stats` | 默认站点汇总 |
| GET | `/runs` / `/runs/:id` | 默认站点运行历史（含 `INTERRUPTED`/`COMPLETED`/`FAILED`） |
| POST | `/collect/run-full` / `/collect/catchup` | 触发默认站点全量 / 增量 |

### `/stations/:id/cores/:coreId` 响应示例

```json
{
  "stationId": "alpha",
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

### `/stations` 中共享在途观测字段

- `sharedInflight.capacity`：两站共享在途上限（`MAX_INFLIGHT`）
- `sharedInflight.current` / `highWaterInflight`：当前 / 历史峰值合计在途（峰值应 ≤ capacity）
- 每个站的 `paused`：是否正处于本站 Retry-After 等待；`http` 为跨重启累计统计

## 6. 配置项（环境变量）

| 变量 | 默认值 | 含义 |
| --- | --- | --- |
| `PORT` | 8080 | 查询/进度服务端口 |
| `MOCK_PORT` | 9090 | 第一个内嵌模拟站点端口（两站时第二个为该端口 +1） |
| `STATIONS` | （空=单站） | 多站列表：`id1,id2`（内嵌）或 `id1=url1,id2=url2`（外部地址） |
| `MOCK_STATIONS` | 自动 | 内嵌站点种子：`id1:seed1,id2:seed2`，不同种子保证同号岩芯内容不同 |
| `MOCK_RANDOM_FAULTS` | 1 | 内嵌站点是否注入随机 503/500（置 0 后改用 `force-503` 确定性控制） |
| `MOCK_BASE_URL` | 内嵌站点 | 单站模式采集目标根地址 |
| `DB_PATH` | ./data/archive.duckdb | DuckDB 数据文件（两站合并写入同一个库） |
| `RATE_MAX_REQUESTS` / `RATE_WINDOW_MS` | 14 / 1000 | **每站**每窗口最大请求数（滑动窗口） |
| `RATE_MAX_REQUESTS_<ID>` / `RATE_WINDOW_MS_<ID>` | 同上 | 按站覆盖限速（站点 id 转大写） |
| `MAX_INFLIGHT` | 6 | **两站共享**的全局在途请求上限 |
| `MAX_RETRIES` / `RETRY_BASE_MS` / `RETRY_MAX_MS` | 8 / 250 / 8000 | 退避重试参数（每站独立） |
| `REQUEST_TIMEOUT_MS` | 8000 | 单次请求超时 |
| `CATCHUP_INTERVAL_MS` | 3000 | 各站增量追更周期 |
| `AUTO_FULL` | true | 启动是否自动全量 |
| `EMBED_MOCK` | 1 | 是否进程内嵌模拟站点 |

## 7. 存储模型（DuckDB，多站）

- `identifications`：每个 `(station_id, core_id, revision)` 一行，`PENDING`/`VERIFIED` + 核实内容 + 尝试次数；同号岩芯跨站物理隔离
- `fetch_urls`：按 `(station_id, url)` 记录已抓取 URL（站内去重与尝试次数证据）
- `runs`：每次全量/增量运行（含站点列、中断标记、累计 HTTP 统计）
- `run_state`：每站一行断点（目录/更正游标、已发布总数、连续核实序号、全量完成标志、累计 HTTP 统计）

**旧库自动迁移**：检测到无 `station_id` 的旧表时，自动加列回填 `default` 并重建为主键带站点的新结构，
运行历史 id、全部核实结果原样保留；迁移后旧路径接口立即可查。

> 说明：duckdb 1.4.x 的 Node 绑定无法序列化 BIGINT、不支持绑定 `null`、参数需展开传递，
> 存储层已统一处理（序号列用 INTEGER、空值内联类型化 NULL、rest 参数展开）；
> 该版本 `ALTER TABLE ADD COLUMN` 不支持 `NOT NULL`，补齐列使用 `DEFAULT 0` 的可空列。

