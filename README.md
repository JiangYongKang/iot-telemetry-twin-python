# iot-telemetry-twin-python

设备接入与数字孪生服务：纯内存实现，本机即可运行，不依赖任何外部中间件或真实凭据。

## 接入协议约定

HTTP + JSON，设备通过 `device_id` + `credential` 接入。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/devices` | 注册设备：`{device_id, credential, quota}` |
| POST | `/devices/{id}/disable` | 禁用设备 |
| POST | `/telemetry` | 单条遥测上报（在线路径） |
| GET | `/devices/{id}/resume?credential=…` | **重连续传游标**：查询"我上次报到哪儿了" |
| POST | `/devices/{id}/backfill` | **断线批量补报**：按设备时间排序、去重、一次性回放 |
| GET | `/devices/{id}/shadow/delta` | **期望差异**：期望里还没跟上报对上的字段（逐字段） |
| PUT | `/devices/{id}/shadow/desired` | 设置期望状态：`{desired: {...}}` |
| GET | `/devices/{id}/shadow` | 查询设备影子（desired / reported 分离，各带版本号） |
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
- `device_ts`：设备侧时间戳（秒），是新旧判定与补报排序的唯一依据。
- `state`：进入设备影子的 reported 部分；`metrics`：进入遥测统计。

## 掉线重连对齐流程（本轮新增）

1. **查差异，只补没对上的字段**：设备重连后调
   `GET /devices/{id}/shadow/delta`，返回逐字段的未对齐清单：

   ```json
   {
     "desired_version": 3,
     "pending": [
       {"field": "mode", "desired": "auto", "reported": "manual",
        "ever_reported": true},
       {"field": "brightness", "desired": 80, "reported": null,
        "ever_reported": false}
     ],
     "aligned": ["on"],
     "in_sync": false
   }
   ```

   离线期间期望改了多少次都只以**最新期望**为准；`ever_reported`
   区分"从未上报"与"上报了但值不匹配"。设备把对应字段报上来后，
   该字段从 `pending` 移到 `aligned`，差异随之缩小、消失。

2. **问位置，接着往下报**：调 `GET /devices/{id}/resume` 得到游标
   `{last_device_ts, earliest_retained_ts, retained_count}`，
   设备从 `last_device_ts` 之后继续，不必全量重发。

3. **断线期间的消息一次补**：`POST /devices/{id}/backfill`：

   ```json
   {
     "credential": "…",
     "after_device_ts": 1000.0,
     "messages": [ {"message_id": "…", "device_ts": 900.0,
                    "state": {"mode": "auto"}, "metrics": {}} ]
   }
   ```

   服务在设备级临界区内完成：**先校验续传位置仍在保留范围内**，
   再按 `(device_ts, message_id)` 把整批排序插入，等价于这些消息
   从一开始就按顺序逐条上报：

   - 时间更早的丢失消息会被插到时间轴中间正确的位置，字段覆盖顺序
     与指标求和都以设备时间为准；
   - 已处理过的 `message_id`、以及完全重复投递的消息逐条标记
     `duplicate`，不重复计数、不重复推进影子版本、不重复占配额；
   - 补报完成后的影子 reported 与统计，和"一直在线按序上报"
     **逐字节一致**。响应逐条给出 `items[].outcome/reason`，
     并汇总 `applied / duplicates / quota_rejected`。

### 拒绝原因（可区分，不揉成一个通用错误）

| reason | HTTP | 含义 |
| --- | --- | --- |
| `unknown_device` | 403 | 设备未注册 |
| `bad_credential` | 403 | 凭证不匹配 |
| `device_disabled` | 403 | 设备已被禁用 |
| `quota_exceeded` | 429 | 超出设备上报配额 |
| `resync_required` | **409** | **续传位置已超出服务端保留范围，必须全量对齐** |

单条上报成功时返回 `outcome` ∈ `applied / duplicate / stale`；
批量补报的每条结果还可能出现 `quota_exceeded`（批次中配额已被
此前条目耗尽，该条未处理）。所有结论都带可读的 `reason` 判定依据。

## 回放历史的保留范围与"接不上"的表现（本轮新增）

支持回放/去重的历史按设备有界保留，策略可配置且有硬上限：

| 配置 | 默认 | 环境变量 |
| --- | --- | --- |
| 每台设备最多保留消息条数 | 1000 | `IOT_TWIN_HISTORY_MAX_MESSAGES` |
| 保留的设备时间跨度（秒） | 3600 | `IOT_TWIN_HISTORY_MAX_AGE_SECONDS`（置空表示只按条数） |

- 超出任一边界的**最早**记录被左裁剪，其 `message_id` 随之被遗忘；
  被裁剪消息的状态/指标会按时间顺序折叠进"压缩基"，因此影子最终状态
  与指标总和始终等价于全部历史按序上报的结果。
- 内存占用严格随上限走：每台设备的回放条目数恒为
  `≤ max_messages`，设备再多也不会无界增长。
- **兼容范围（重要）**：补报必须携带游标位置 `after_device_ts`。
  若该位置早于当前仍保留的最早记录（`earliest_retained_ts`），
  说明设备回来得太晚、中间区间已被回收——此时服务**整批拒绝**并返回
  `409 resync_required`，不产生任何部分写入，绝不会给一份缺数据的
  回放还当成正常成功。设备收到后应改用新的 `message_id`
  全量上报当前状态重新对齐（`GET …/shadow/delta` 可指导对齐哪些字段）。
  设备从未上报过（窗口为空）时不存在缺口，补报正常接受。

## 去重与新旧判定规则

1. **幂等去重**：同一设备内 `message_id` 在保留窗口内已处理过的消息
   返回 `duplicate`，不重复计数、不推进影子、不占用配额。
2. **新旧判定（在线单条路径）**：仅当 `device_ts > last_device_ts`
   时才 `applied`；否则为 `stale`，不覆盖已有状态、不计入指标。
   乱序到达但确实是丢失的更早消息，应走 `/backfill` 由服务重排插入。
3. **设备影子**：desired 与 reported 分离，各自带单调递增版本号；
   补报只按实际新生效的消息数推进 reported 版本。

## 并发下的一致性保证

- 每台设备一把锁：去重判定、续传范围校验、配额扣减、窗口插入、
  影子落地、统计累加在同一临界区内完成——**要么一起生效，要么一起
  不生效**，不存在半截写入。
- 不同设备之间互不阻塞；认证失败发生在临界区之前，无任何副作用。

## 本地验证

```bash
uv sync                                  # 安装依赖（含 pytest）
uv run pytest -v                         # 运行全部单测（日志在失败用例中可见）
uv run pytest -v -s --log-cli-level=INFO # 同时在控制台打印每条判定依据
```

测试覆盖（31 个用例，同一进程可连跑多轮结果一致）：

- 原有：重复幂等、乱序不覆盖、补报不重复应用、并发原子性、配额超限、
  拒绝原因区分、HTTP 端到端（均无回归）。
- 新增：
  - 期望差异逐字段列出、随上报缩小消失、以最新期望为准、
    区分"从未上报/值不匹配"；
  - 续传游标查询；批量补报乱序重排、夹带重复投递时与"一直按序上报"
    的影子状态、版本、指标和/计数/均值完全等价；
  - 条数上限与时间跨度上限裁剪、内存有界、裁剪后状态仍可重放；
  - 续传位置超出保留范围时整批 409、无部分写入，以及全量对齐恢复；
  - 并发用例中"计数/配额严丝合缝"与"旧数据/重复判定成立"两类结论
    分别独立断言（barrier 同步发起，不依赖线程调度顺序）；
  - 新接口的 HTTP 端到端链路与 409 行为。

手动跑服务：

```bash
uv run python main.py    # 监听 127.0.0.1 随机端口，也可 uvicorn main:app --port 8000
# 用小保留上限自测"接不上"场景：
IOT_TWIN_HISTORY_MAX_MESSAGES=5 uv run uvicorn main:app --port 8000
```

手动体验完整重连流程：

```bash
DEV=demo-1
curl -s -XPOST localhost:8000/devices -H 'content-type: application/json' \
  -d "{\"device_id\":\"$DEV\",\"credential\":\"c\",\"quota\":100}"
curl -s -XPOST localhost:8000/telemetry -H 'content-type: application/json' \
  -d "{\"device_id\":\"$DEV\",\"credential\":\"c\",\"message_id\":\"live\",\"device_ts\":50,\"state\":{\"live\":true}}"
curl -s "localhost:8000/devices/$DEV/resume?credential=c"
curl -s -XPUT localhost:8000/devices/$DEV/shadow/desired -H 'content-type: application/json' \
  -d '{"desired":{"mode":"auto"}}'
curl -s "localhost:8000/devices/$DEV/shadow/delta"
curl -s -XPOST localhost:8000/devices/$DEV/backfill -H 'content-type: application/json' \
  -d '{"credential":"c","after_device_ts":50,"messages":[{"message_id":"b1","device_ts":20,"state":{"mode":"auto"}}]}'
curl -s "localhost:8000/devices/$DEV/shadow/delta"
```
