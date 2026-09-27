# iot-telemetry-twin-python

设备接入与数字孪生服务：纯内存实现，本机即可运行，不依赖任何外部中间件或真实凭据。

## 接入协议约定

HTTP + JSON，设备通过 `device_id` + `credential` 接入。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/devices` | 注册设备：`{device_id, credential, quota}` |
| POST | `/devices/{id}/disable` | 禁用设备 |
| POST | `/telemetry` | 遥测上报 |
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

成功时返回 `outcome` ∈ `applied / duplicate / stale` 及可读的判定依据 `reason` 字段。

## 去重与新旧判定规则

1. **幂等去重**：同一设备内 `message_id` 已处理过的消息直接返回 `duplicate`，
   不重复计数、不重复推进影子状态、不占用配额。
2. **新旧判定**：每条消息携带设备侧时间戳 `device_ts`，服务为每台设备维护
   `last_device_ts`（已应用消息的最大设备时间戳）。仅当
   `device_ts > last_device_ts` 时消息才会被应用（`applied`）；否则判定为
   乱序旧数据或重连补报（`stale`），不覆盖已有状态、不计入指标。
   判定依据会写入响应的 `reason` 字段，例如
   `device_ts=100.0 <= 已见最大 device_ts=200.0，判定为旧数据`，结论可解释、可复现。
3. **设备影子**：期望状态（desired）与上报状态（reported）分离，各自带单调
   递增版本号。设备重连后的增量补报因 `device_ts` 不新于 `last_device_ts`
   而被拒绝，不会重复应用。

## 并发下的一致性保证

- 每台设备一把锁：同一设备的去重判定、新旧判定、配额扣减、影子更新、
  统计累加、去重标记在同一个临界区内完成——**要么一起生效，要么一起不生效**，
  不存在半截写入；并发打满配额时，统计计数与配额消耗始终对得上。
- 不同设备之间互不阻塞，可并行上报。
- 认证失败发生在临界区之前，不产生任何副作用。

## 本地验证

```bash
uv sync                 # 安装依赖（含 dev 依赖 pytest）
uv run pytest -v -s --log-cli-level=INFO
```

测试覆盖：重复上报幂等、乱序到达不覆盖新状态、重连补报不重复应用、
同一设备并发写入原子性、并发下配额超限拒绝、四类拒绝原因可区分，
以及一条 HTTP 端到端链路。单测日志会打印触发判定的输入与依据。

手动跑服务：

```bash
uv run python main.py   # 监听 127.0.0.1 随机端口，也可 uvicorn main:app
```
