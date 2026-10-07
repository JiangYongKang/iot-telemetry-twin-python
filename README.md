# iot-telemetry-twin-python

设备接入与数字孪生服务：纯内存实现，本机即可运行，不依赖任何外部中间件或真实凭据。

## 接入协议约定

HTTP + JSON，设备通过 `device_id` + `credential` 接入。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/devices` | 注册设备：`{device_id, credential, quota}` |
| POST | `/devices/{id}/disable` | 禁用设备 |
| POST | `/telemetry` | 遥测上报（单条） |
| POST | `/telemetry/backfill` | 断线重连后的批量补报 |
| GET | `/devices/{id}/resume?credential=…` | 查询续传位置：“我上次报到哪儿了” |
| PUT | `/devices/{id}/shadow/desired` | 设置期望状态：`{desired: {...}}` |
| GET | `/devices/{id}/shadow` | 查询设备影子（desired / reported 分离，各带版本号） |
| GET | `/devices/{id}/shadow/delta` | 查询字段级期望差异 |
| GET | `/devices/{id}/stats` | 查询遥测统计（接受/重复/乱序计数、指标和与均值） |

遥测上报报文：

```json
{
  "device_id": "dev-1",
  "credential": "…",
  "message_id": "全局唯一的消息 id（幂等键）",
  "device_ts": 1727400000.0,
  "state": {"on": true},
  "metrics": {"temp": 22.0}
}
```

- `message_id`：幂等键，由设备侧保证唯一。
- `device_ts`：设备侧时间戳（秒），是新旧判定的唯一依据。
- `state`：进入设备影子的 reported 部分；`metrics`：进入遥测统计。

### 拒绝原因（可区分，不揉成一个通用错误）

上报失败返回结构化 `reason`：

| reason | HTTP | 含义 |
| --- | --- | --- |
| `unknown_device` | 403 | 设备未注册 |
| `bad_credential` | 403 | 凭证不匹配 |
| `device_disabled` | 403 | 设备已被禁用 |
| `quota_exceeded` | 429 | 超出设备上报配额 |

成功时返回 `outcome` ∈ `applied / duplicate / stale / gap` 及可读的判定依据
`reason` 字段（`gap` 只出现在批量补报中，见下文）。

## 去重与新旧判定规则

1. **幂等去重**：`message_id` 仍在回放历史保留窗口内即判定为 `duplicate`，
   不重复计数、不重复推进影子状态、不占用配额。
2. **新旧判定**：每条消息携带设备侧时间戳 `device_ts`，服务为每台设备维护
   `last_device_ts`（已应用消息的最大设备时间戳）。仅当
   `device_ts > last_device_ts` 时消息才会被应用（`applied`）；否则判定为
   乱序旧数据（`stale`），不覆盖已有状态、不计入指标。
   判定依据会写入响应的 `reason` 字段，例如
   `device_ts=100.0 <= 已见最大 device_ts=200.0，判定为旧数据`，结论可解释、可复现。
3. **设备影子**：期望状态（desired）与上报状态（reported）分离，各自带单调
   递增版本号。

## 断线重连（本轮新增）

### 字段级期望差异：`GET /devices/{id}/shadow/delta`

设备离线期间期望状态可能被改过多次。重连后查询 delta，返回**以最新期望为准**、
逐字段列出的未对齐项：

```json
{
  "in_sync": false,
  "fields": [
    {"field": "interval", "desired_value": 60, "reported_value": null,
     "reported_present": false},
    {"field": "mode", "desired_value": "boost", "reported_value": "eco",
     "reported_present": true}
  ]
}
```

设备只把这几项补报上来即可；每对齐一个字段，差异随之缩小，全部对齐后
`in_sync=true`。`reported_present` 区分“从未上报过”与“上报的值恰为 null”。

### 续传位置：`GET /devices/{id}/resume?credential=…`

```json
{
  "last_device_ts": 200.0,       // 你上次报到这儿
  "replay_horizon": 150.0,       // 回放水位：早于等于它的历史已被回收
  "earliest_retained_ts": 151.0, // 保留窗口内现存最早的时间戳
  "retained_count": 49
}
```

设备拿 `last_device_ts` 接着往下报；`replay_horizon` 为 `null` 表示从未发生
回收，任意早的缓冲消息都能补。

### 批量补报：`POST /telemetry/backfill`

设备把断线期间攒下的一批消息一次提交（允许乱序、允许混入已送达过的、
允许批次内重复）：

```json
{
  "device_id": "dev-1",
  "credential": "…",
  "messages": [
    {"message_id": "m-4", "device_ts": 4.0, "state": {...}, "metrics": {...}},
    {"message_id": "m-3", "device_ts": 3.0, "state": {...}, "metrics": {...}}
  ]
}
```

服务按设备侧时间戳理顺后逐条判定：

- 窗口内已处理过的 id、批次内重复投递 → `duplicate`，不重复计入、不重复推进状态；
- `device_ts` 新于已见最大 → 与单条上报同语义，整条原子生效；
- 早于已见最大但越过回放水位、且窗口内查无此 id → 一定从未被应用过，
  插入历史并**回放重建**影子（`applied`）；
- `device_ts <= replay_horizon` → 该位置的历史已被回收，无法与“已应用后
  被回收”区分，判为 `gap`，批次响应 `resync_required=true`，
  **明确告知这段接不上、需要全量对齐**，不会悄悄给一份缺数据的正常结果。

补报完成后，最终的影子状态与统计，和这批数据从一开始就按顺序逐条上报
得到的结果一模一样（影子版本号是单调递增的元数据，不参与该等价）。

### 保留上限与兼容范围

回放历史是每台设备一段有界窗口，两个上限均可配置（环境变量）：

| 环境变量 | 默认 | 含义 |
| --- | --- | --- |
| `IOT_TWIN_HISTORY_MAX_MESSAGES` | 1000 | 每台设备最多保留的消息条数 |
| `IOT_TWIN_HISTORY_MAX_AGE_SECONDS` | 3600 | 每条消息最多保留的秒数（按服务端写入时刻计龄） |

超出上限从最旧的一端回收。**保留范围被超出以后的表现**：

- 已回收消息的 `message_id` 再去重会判为 `stale`（单条路径）或 `gap`
  （补报路径），不会重复计入统计；
- 补报落在已回收区段的消息得到明确的 `gap` + `resync_required=true`，
  设备应转为全量对齐（重新上报当前完整状态）；
- 回收会把被回收条目的状态折叠进基态，回放重建的结果仍然与
  “从头按顺序上报”等价，不因回收而丢失已对齐的状态。

内存占用随 `(max_messages, max_age_seconds)` 收敛：长时间运行、设备数量
增多时，每台设备的历史窗口不会越过上限持续增长。

## 并发下的一致性保证

- 每台设备一把锁：同一设备的去重判定、新旧判定、配额扣减、影子更新、
  统计累加、历史写入在同一个临界区内完成——**要么一起生效，要么一起不生效**，
  不存在半截写入；并发打满配额时，统计计数与配额消耗始终对得上。
- 实时上报与批量补报可以并发到达：补报的插入与回放重建同样在设备锁内完成，
  终态与顺序执行一致，不依赖线程先后。
- 不同设备之间互不阻塞，可并行上报。
- 认证失败发生在临界区之前，不产生任何副作用。

## 本地验证

```bash
uv sync                 # 安装依赖（含 dev 依赖 pytest）
uv run pytest -v -s --log-cli-level=INFO
```

测试覆盖：重复上报幂等、乱序到达不覆盖新状态、重连补报不重复应用、
补报回放与顺序上报的等价性、回放水位与缺口判定、保留上限与内存有界、
字段级期望差异、同一设备并发写入原子性、并发下实时+补报收敛、
并发重复投递恰好生效一次、四类拒绝原因可区分、同进程多轮重复一致，
以及 HTTP 端到端链路（含完整重连流程）。单测日志会打印每条判定
根据什么输入、按什么依据得出。

手动跑服务：

```bash
uv run python main.py   # 监听 127.0.0.1 随机端口，也可 uvicorn main:app
```
