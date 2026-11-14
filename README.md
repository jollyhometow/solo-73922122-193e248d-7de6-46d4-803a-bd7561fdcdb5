# 采样-检测记录配对与运行批次服务

实验室数据工程师用本服务完成两件事：

1. **配对**：把**人工采样登记**与**仪器检测记录**对上号，供质控员判断哪些
   结果可以进入分析库。服务只把**所有同等最优方案中都成立**的配对列为确定
   配对，绝不任选一套最优方案冒充确定结论。
2. **运行批次**：仪器在弱网下按运行 ID **分段补传**检测记录，等整次运行
   齐全后一次性固定完整快照，再交给分析库。批次用 **SQLite** 持久保存。

## 运行批次 API（分段补传 → 发布快照）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `PUT` | `/runs/{run_id}/segments/{segment_index}` | 提交一个分段（允许乱序，同内容重传幂等） |
| `POST` | `/runs/{run_id}/publish` | 发布：齐段后一次性固定完整快照 |
| `GET` | `/runs/{run_id}` | 查询批次状态（已发布时含原快照） |

### 提交分段

- 路径：`run_id`（字母数字开头，可含 `.` `_` `-`）、`segment_index`（**从 0 起**，0–7）。
- 请求体：`total_segments`（总段数 1–8，同一运行各段必须一致）+ `records`
  （本段记录，**0–12 条**）。
- 每条记录：唯一 `id`、条码 `barcode`、UTC 时间 `timestamp`（ISO 8601，必须带
  时区）、`results`（检测项目 → 数值，1–32 项，**必须是有限数**，拒绝
  NaN/Infinity）。

```bash
curl -X PUT http://localhost:8000/runs/RUN-2026-0456/segments/0 \
  -H 'Content-Type: application/json' \
  -d '{
    "total_segments": 2,
    "records": [
      {"id": "R-001", "barcode": "BC-100", "timestamp": "2026-11-14T08:00:00Z",
       "results": {"GLU": 5.2, "K": 4.1}}
    ]
  }'
```

应答给出 `received_segments` / `missing_segments`，便于续传进度跟踪。

### 一致性与幂等语义

| 场景 | 结果 |
| --- | --- |
| 分段乱序到达 | 正常接收 |
| 同段**同内容**重传 | 200，`replay=true`，不改数据（键序、时区写法不同但逻辑相同也算同内容） |
| 同段**异内容**重传 | 409 `segment_conflict`，已收数据不变 |
| 跨段重复记录 ID | 409 `duplicate_record_id`（附冲突 ID），该段不入库 |
| 各段 `total_segments` 不一致 | 409 `total_segments_mismatch`（附已确立值） |
| 发布时缺段 | 409 `run_incomplete`（附 `missing_segments` 缺号），**保留续传资格** |
| 齐段后发布 | 200，单事务内按段号固定完整快照 |
| 发布后重试发布 | 200，**原样返回首次快照**（逐字节一致） |
| 发布后再提交分段 | 409 `run_already_published` |

并发补传与发布互斥（进程内锁 + SQLite `BEGIN IMMEDIATE` 事务），任何成功的
发布都包含全部分段，不会暴露半成品；进程重启后批次仍在（SQLite 持久化，
Compose 中挂载 named volume）。

### 发布响应（快照）

```json
{
  "run_id": "RUN-2026-0456",
  "state": "published",
  "total_segments": 2,
  "record_count": 3,
  "published_at": "2026-11-14T09:00:00.123456+00:00",
  "segments": [
    {"segment_index": 0, "records": [ ... ]},
    {"segment_index": 1, "records": [ ... ]}
  ]
}
```

## 配对 API（/match）

### 配对规则

- 输入两组记录：采样登记、检测记录，**每组最多 12 条**。
- 每条记录：组内唯一 `id`、可重复的容器条码 `barcode`、UTC 时间 `timestamp`
  （ISO 8601，必须带时区，推荐 `Z` 后缀）。
- 两条记录**条码相同且 UTC 时间差 ≤ 10 分钟（含边界）**才可配对。
- 每条记录至多参与一对。
- 优化目标（字典序）：**先使配对数最多，再使时间差总和最小**。

### 输出语义

| 状态 | 含义 | 处置建议 |
| --- | --- | --- |
| `certain` | 确定配对：在所有同等最优方案中都成立 | 可进入分析库 |
| `review` | 待核查：存在同等最优方案分歧（对象不定或时有时无） | 交质控员人工核查 |
| `unmatched` | 始终未配对：任何同等最优方案都不包含它 | 不进入分析库 |

`review` 记录附带：

- `matched_in_all_optimal`：`true` 表示"始终被配对但对象不定"，`false` 表示
  "部分最优方案中未配对"；
- `possible_partners`：在至少一个同等最优方案中可行的配对对象及时间差。

### 请求示例

```bash
curl -X POST http://localhost:8000/match \
  -H 'Content-Type: application/json' \
  -d '{
    "sampling_records": [
      {"id": "S-01", "barcode": "BC-100", "timestamp": "2026-11-14T08:00:00Z"}
    ],
    "detection_records": [
      {"id": "D-01", "barcode": "BC-100", "timestamp": "2026-11-14T08:04:00Z"}
    ]
  }'
```

## 快速开始（Docker Compose）

```bash
docker compose up --build
```

服务监听 `http://localhost:8000`：

- `GET /docs` —— Swagger UI，可直接试用：
  - `PUT /runs/{run_id}/segments/{segment_index}` 的 Examples 内置
    `segment_0_of_2` / `segment_1_of_2`：依次把路径参数设为
    `run_id=RUN-2026-0456`、`segment_index=0/1` 各提交一次，
    再 `POST /runs/RUN-2026-0456/publish` 即可看到固定快照；
  - `POST /match` 的 Examples 内置 `duplicate_barcodes`（重复条码）、
    `crossing_candidates`（交叉候选）、`equivalent_optima`（等价最优）；
- `GET /health` —— 健康检查（Compose healthcheck 使用）。

SQLite 数据库落在 named volume `runs-data`（容器内 `/srv/app/data/runs.db`），
可用 `RUNS_DB_PATH` 环境变量改路径。

## 校验规则（不满足返回 422）

配对与批次共用：

- `id`、`barcode` 非空；`timestamp` 必须带时区（带偏移量的时间会归一化为 UTC）；
- 不接受未定义的字段。

批次特有：

- 总段数 1–8、段号 0–7 且必须小于总段数、每段 0–12 条、每条 1–32 个检测项目；
- 段内记录 ID 不重复；数值必须是有限数（NaN/Infinity 返回 422）。

## 算法说明（配对）

建模为二分图最小费用最大流：左部采样记录、右部检测记录，候选边权为时间差
（毫秒整数）。持续沿最短路增广直到不可增广，即得"配对数最大、同流量下总时间差
最小"的字典序最优匹配。

确定性判定通过精确重算完成，而非启发式：

- **确定配对**：禁用该候选边后最优值（配对数, 总时间差）变差 ⟺ 它存在于所有
  同等最优方案；
- **可行对象**：强制选入该候选边（删除两端点）后仍能达到整体最优值 ⟺ 它存在于
  某个同等最优方案；
- **始终被配对**：删除该记录后最优值变差 ⟺ 它在所有同等最优方案中都被配对。

每组 ≤ 12 条记录时候选边 ≤ 144 条，重算约 200 次微型流，单次请求开销在毫秒级。
算法结果已用 3000 个随机小规模用例与暴力枚举全部匹配交叉验证一致。

## 本地开发与测试

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt pytest httpx
pytest                 # 运行测试
uvicorn app.main:app --reload
```

## 目录结构

```
app/
  main.py       # FastAPI 入口、响应组装、校验错误处理
  matching.py   # 配对核心算法（最小费用最大流 + 确定性判定）
  schemas.py    # 配对请求/响应模型
  runroutes.py  # 运行批次路由（提交分段 / 发布 / 状态查询）
  runschemas.py # 运行批次请求/响应模型
  runstore.py   # SQLite 持久化与事务逻辑（幂等、快照、并发互斥）
  examples.py   # /docs 内置演示请求
tests/          # 算法单元测试、配对 API 与运行批次 API 集成/并发/持久化测试
Dockerfile
docker-compose.yml
```
