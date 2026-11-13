# AIS 船位连续归档系统

港口数据工程场景：本地**模拟岸站**持续产生带全局连续序号的 AIS 船位，
**采集归档服务**通过 SSE 实时流 + HTTP 历史分页两条通道完整归档，
为靠离泊分析提供无缺口轨迹；早期船位交付离线分析后，可按**全局序号清理**
在线库回收明细，同时继续提供近期轨迹。FastAPI + SQLite，Docker Compose 一键启动。

## 1. 架构与核心语义

```
                 ┌──────────────────────┐        SSE /stream（只推新记录，不补发）
                 │  模拟岸站 station:8000 │◄──────────────┐
                 │  seq 从 1 全局连续递增  │               │
                 │  记录只增不改           │        HTTP /history 分页补采
                 └──────────────────────┴──────────────┐│
                                                       ││
                                            ┌──────────▼▼───────────┐
                                            │ 采集器 collector:8001  │
                                            │  ┌──────────────────┐ │
                                            │  │ SSE 实时循环      │ │  断流指数退避重连
                                            │  │ 历史补采循环      │ │  翻页失败重试、互不影响
                                            │  │ 后台清理循环      │ │  按留存边界分批物理删除
                                            │  └────────┬─────────┘ │
                                            │     INSERT OR IGNORE  │  seq 主键去重（重叠只入一次）
                                            │  连续水位 contiguous_seq │  仅 1..w 全在库才推进
                                            │  留存边界 cleanup_boundary │  已受理最大清理截止序号，只增不减
                                            │  SQLite (WAL, 持久卷)   │
                                            └───────────────────────┘
```

**关键保证（采集容错）**

| 需求 | 实现机制 |
|---|---|
| 重叠记录只入库一次 | `positions.seq` 主键 + `INSERT OR IGNORE`，SSE/历史任一路径先到先得 |
| 补采期间新船位不漏 | 两个循环并发独立运行；SSE 不补发的部分由历史循环按持久化游标补齐 |
| 断流不丢数据 | SSE 循环断线指数退避重连；断线期间船位由历史循环补回 |
| 翻页失败不影响实时 | 历史循环仅记录错误并重试，SSE 循环完全独立 |
| 连续进度严谨 | 每次写入后从水位逐序号探测，遇缺口即停；水位持久化、单调不减 |
| 轨迹不越过缺口 | 查询以 `contiguous_seq` 为可见上界，缺口之后的记录一律不返回 |
| 重启自动接续 | 船位、水位、历史游标、错误全部在 SQLite；启动即恢复并继续补采 |
| 岸站重置可自愈 | 检测到序号代际回退时自动把补采游标回 0 重新补采（记录为已解决告警） |

**关键保证（按序号清理/回收明细）**

| 需求 | 实现机制 |
|---|---|
| 截止序号不得超过连续进度 | `POST /admin/cleanup` 校验 `cutoff_seq ≤ contiguous_seq`，越界返回 409 并留痕，状态不变 |
| 留存边界只增不减 | 边界 `cleanup_boundary = max(旧边界, cutoff)` 持久化在 meta；写锁串行受理，并发/重复清理不回退 |
| 受理即生效 | 受理后 `seq ≤ 边界` 的记录立即从轨迹、船舶列表、记录计数中排除（逻辑排除先于物理删除） |
| 轨迹注明已清理范围 | 轨迹响应带 `cleanup_boundary` 与 `cleaned_note`（如“序号 1..60 的明细已清理”） |
| 连续进度保留 | 水位不随清理回退；留存轨迹仍以边界外首个真实缺口为可见上界 |
| 单次查询同一状态 | 独立只读连接 + 快照事务，边界、明细、计数在同一快照读取 |
| 采集查询不中断 | 后台循环按 `CLEANUP_BATCH_SIZE` 小批删除；WAL 下读快照与写互不阻塞 |
| 已清理记录不回流 | 引擎内存边界拦截 + SQL 条件插入双保险；在途历史页、实时帧、回退补采、岸站重置均无法重新入库 |
| 失败/重启自动续清 | 边界与待清状态持久化；后台循环按边界扫描续删，恢复后清理错误自动关闭 |
| 旧清理不删新归档 | 每次 `/admin/reset` 归档代际 `archive_epoch` 递增；删除语句同批校验代际，代际不符一行不删 |
| 可观测 | `GET /cleanup/status` 提供边界、待删数量、代际、申请留痕与清理错误；`/errors?kind=cleanup` 过滤错误 |

## 2. 快速启动（Docker Compose）

```bash
docker compose up --build
```

- 岸站 API 文档（含故障注入接口）：http://localhost:8000/docs
- 采集器 API 文档（状态/错误/轨迹/管理）：http://localhost:8001/docs
- SQLite 数据位于命名卷 `collector-data`，容器重建不丢数据。

停止与清理：

```bash
docker compose down            # 停止，保留数据卷
docker compose down -v         # 停止并删除归档数据
```

> 采集器通过健康检查等待岸站就绪后再启动。

## 3. API 一览

### 采集器（端口 8001，演示主入口）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/status` | 已接收数（不含已清理）、连续进度、真实缺口、留存边界、待删数与两通道计数 |
| GET | `/errors?limit=&include_resolved=&kind=` | 错误查询（默认仅未恢复，可按 `sse`/`history_page`/`cleanup` 过滤） |
| GET | `/vessels` | 留存船舶清单及各自序号范围（已清理序号不计入） |
| GET | `/trajectories/{mmsi}?limit=` | 按 MMSI 读取轨迹（边界 < seq ≤ 水位，按 seq 升序，注明已清理范围） |
| POST | `/admin/cleanup` | **按全局序号清理**：body `{"cutoff_seq": N}`，N 不得超过 `contiguous_seq` |
| GET | `/cleanup/status` | 留存边界、待删数量、归档代际、最近清理申请与清理错误 |
| POST | `/admin/reset` | 清空归档并重置进度、进入新归档代际（演示用） |
| POST | `/admin/inject-gap` | 故障注入：删除指定序号，水位回退，演示轨迹截断语义 |
| POST | `/admin/rewind-backfill` | 历史游标回退到水位，重新补采缺口区间（已清理序号不会回流） |

### 模拟岸站（端口 8000）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/stream` | SSE 实时流（`text/event-stream`，只推连接后的新船位） |
| GET | `/history?cursor=&limit=` | 历史分页，返回 `seq > cursor` 的记录 |
| GET | `/station/status` | 岸站生成状态 |
| POST | `/station/stream-stop` / `stream-resume` | 模拟实时流中断 / 恢复 |
| POST | `/station/history-fail-rate` | 设置翻页失败率 `{"rate": 0.5}`（0~1） |
| POST | `/station/reset` | 岸站清空并重新生成（注意见第 6 节） |

## 4. 在 Swagger UI 中的演示流程

打开 http://localhost:8001/docs ，按顺序操作（均可点 **Try it out** 执行）：

1. **观察初始采集**：GET `/status`
   - `received_total` 增长，`contiguous_seq` 紧随其后，`complete=true`，`next_missing_seqs=[]`。
2. **看实时与补采协同**：`sse_received` 与 `history_received` 同时增长，重叠序号不产生重复。
3. **演练历史翻页失败**（在 8000 文档中）：
   - POST `/station/history-fail-rate`，body `{"rate": 1.0}`；
   - 回 8001 看 `/status`：`history_failures` 上升、`sse_connected=true`、接收仍在增长；
   - GET `/errors` 可见 `history_page` 未恢复错误；
   - 再设 `{"rate": 0.0}`，错误自动 resolved，缺口被补齐、`complete` 恢复 true。
4. **演练断流重连**：
   - POST `/station/stream-stop` → `/status` 中 `sse_connected=false`、`/errors` 出现 `sse` 错误；
   - POST `/station/stream-resume` → 自动重连（`sse_reconnects` 增加），错误恢复、数据补齐。
5. **演练缺口语义**：
   - 先 GET `/vessels` 取一个 MMSI，GET `/trajectories/{mmsi}` 看完整轨迹；
   - POST `/admin/inject-gap`，body 例如 `{"seqs": [50]}`；
   - `/status` 显示 `contiguous_seq=49`、`next_missing_seqs=[50]`；
   - 再查轨迹：所有返回点 `seq ≤ 49`，缺口之后的记录不可见；
   - POST `/admin/rewind-backfill`，数秒内补齐，轨迹恢复。
6. **演练按序号清理（回收明细）**：
   - 先 GET `/status` 记下 `contiguous_seq`（假设为 200）；
   - POST `/admin/cleanup`，body `{"cutoff_seq": 100}`：
     受理后返回 `cleanup_boundary=100`、`pending_delete=100`；
   - 立即再 GET `/status`：`received_total` 已排除前 100 条，
     `cleanup_boundary=100`，`cleanup_pending` 随后台删除逐步降到 0；
   - GET `/vessels`：各船 `min_seq` 全部 > 100；
   - GET `/trajectories/{mmsi}`：只含 `seq > 100` 的点，
     响应中 `cleanup_boundary=100`、`cleaned_note` 注明“序号 1..100 的明细已清理”；
   - GET `/cleanup/status` 观察 `pending_delete` 归零、申请 `completed_at` 落定；
   - 越界演练：再 POST `{"cutoff_seq": 999999}` 返回 409，边界不变；
   - 不回退演练：POST `{"cutoff_seq": 50}` 仍返回 200，但边界保持 100；
   - 回流演练：POST `/admin/rewind-backfill`（或在 8000 文档 POST `/station/reset`），
     历史/实时重新送达小序号，`/vessels` 中仍看不到 `seq ≤ 100` 的记录。
7. **演练清理失败与自动续清**（可选）：
   - 给采集器设置环境变量 `CLEANUP_FAIL_RATE=1.0` 后重启；
   - 再发起一次清理：GET `/cleanup/status` 中 `pending_delete` 不下降、
     `open_cleanup_error_count=1`，GET `/errors?kind=cleanup` 可见错误，期间查询正常；
   - 去掉该变量重启采集器：自动续清至 `pending_delete=0`，错误自动 resolved。
8. **错误查询**：GET `/errors?include_resolved=true` 查看完整故障史与恢复时间。

## 5. 中断重启演示

```bash
docker compose restart collector        # 重启采集器，卷中数据保留
# 或本地：
# tests/restart_test.py 会真正杀掉进程再拉起，断言进度保留与自动接续
```

重启后 `/status` 中 `received_total`、`contiguous_seq`、`history_cursor`、
`cleanup_boundary` 均为重启前的值，随后继续增长并最终 `complete=true`；
若重启时有未删完的清理批次，后台循环按持久化边界自动续清至
`/cleanup/status` 的 `pending_delete=0`。

## 6. 关于“重置”的正确姿势

- 重新开始一轮干净演练时，**两侧都要重置**（端到端测试即如此）：
  先 POST 采集器 `/admin/reset`，再 POST 岸站 `/station/reset`。
- 采集器 `/admin/reset` 会让归档代际 `archive_epoch` 递增：
  重置前已在运行的旧清理任务凭旧代际自动停手，**旧清理不会删除新归档**；
  新归档的序号即使与旧代际重合也不受旧边界影响，可重新清理。
- 若只重置岸站：采集器能检测到序号代际回退并自动回退补采游标（不会卡死），
  但已被清理的旧序号受留存边界拦截不会重新入库，其余旧代际船位因 `seq`
  主键冲突保留在库中。这是去重主键与清理边界的预期安全行为；
  需要全新数据集请使用 `/admin/reset` 清空归档。

## 7. 本地开发（无 Docker 时）

```bash
python3 -m pip install -r requirements.txt   # 建议虚拟环境
# 终端 1：岸站
uvicorn shore_station.app:app --port 8000
# 终端 2：采集器
STATION_URL=http://127.0.0.1:8000 DB_PATH=./data/collector.db \
  uvicorn collector.app:app --port 8001
```

## 8. 自动化测试

测试只通过 HTTP 驱动，对 Docker 环境与本地进程同样适用：

```bash
# 基础容错与缺口语义（31 项断言）
python3 tests/e2e_test.py
# 真正杀进程重启的接续测试
python3 tests/restart_test.py
# 清理/回收明细全语义（33 项断言，含并发清理、防回流、杀进程续清、代际隔离）
python3 tests/cleanup_test.py
```

覆盖：初始去重采集、翻页 100% 失败期间实时不中断、断流/重连补齐、
缺口注入后轨迹截断与回补、重启后进度保留与自动接续；
清理截止校验与驳回留痕、受理即排除、后台分批删除与续清、并发/重复清理
边界不回退、实时帧/历史补采/岸站重置均不回流、重置代际隔离与清理错误查询。

## 9. 目录结构

```
.
├── docker-compose.yml        # station + collector 两服务，SQLite 命名卷
├── Dockerfile                # 共用镜像，compose 中以不同 command 启动
├── requirements.txt
├── shore_station/app.py      # 模拟岸站：SSE 流、历史分页、断流/失败注入
├── collector/
│   ├── db.py                 # SQLite：去重写入、连续水位、留存边界、代际、快照只读池、清理
│   ├── ingest.py             # SSE/历史补采/后台清理三循环、退避重试、代际检测、边界拦截
│   └── app.py                # FastAPI：状态/错误/船舶/轨迹/清理/管理接口
└── tests/
    ├── e2e_test.py
    ├── restart_test.py
    └── cleanup_test.py
```

## 10. 主要环境变量

| 变量 | 默认 | 作用 |
|---|---|---|
| `GEN_INTERVAL` | 0.25 | 岸站船位生成间隔（秒） |
| `INITIAL_COUNT` | 60 | 岸站启动时预生成历史条数 |
| `HISTORY_PAGE_LIMIT` | 100 | 岸站历史分页上限 |
| `STATION_URL` | http://station:8000 | 采集器访问的岸站基址 |
| `DB_PATH` | /data/collector.db | SQLite 文件路径 |
| `HISTORY_PAGE_SIZE` | 50 | 采集器补采分页大小 |
| `SSE_READ_TIMEOUT` | 60 | SSE 读超时（秒），超时判定断流并重连 |
| `SSE_RECONNECT_MAX` / `HISTORY_RETRY_MAX` | 30 / 30 | 两类重试的最大退避（秒） |
| `CLEANUP_BATCH_SIZE` | 100 | 后台清理每批物理删除行数 |
| `CLEANUP_IDLE_INTERVAL` | 1.0 | 无待删明细时后台清理循环的轮询间隔（秒） |
| `CLEANUP_FAIL_RATE` | 0.0 | 清理故障演练注入概率（0~1，生产保持 0） |
| `READ_POOL_SIZE` | 4 | 快照只读连接池大小 |
