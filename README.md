# IoT 设备接入与数字孪生服务（本地内存版）

多设备并发上报、消息乱序到达、离线后重连补报场景下，设备状态与遥测统计结论
**稳定、可复现**。纯进程内实现（FastAPI + 每设备锁），**不依赖任何外部中间件
（MQTT/Kafka/Redis/数据库）或真实凭据**，本机一条命令即可跑通。

## 1. 快速开始

```bash
uv sync                         # 安装依赖（fastapi/uvicorn，均为本地进程）
uv run uvicorn main:app --host 127.0.0.1 --port 8000
```

另开终端：

```bash
# 注册设备（quota=0 表示不限制）
curl -s -X POST http://127.0.0.1:8000/devices \
  -H 'Content-Type: application/json' \
  -d '{"device_id":"dev-1","credential":"k","quota":10}'

# 遥测上报
curl -s -X POST http://127.0.0.1:8000/devices/dev-1/telemetry \
  -H 'Content-Type: application/json' \
  -d '{"credential":"k","msg_id":"m1","device_ts_ms":1000,
       "payload":{"temp":36.5},"reported":{"state":"online","firmware":"v1"}}'
```

## 2. 接入协议约定

### 2.1 接口一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/devices` | 注册设备：`device_id`、`credential`、`enabled`、`quota`（生效条数上限，0 不限） |
| `POST` | `/devices/{id}/telemetry` | 遥测上报 |
| `GET`  | `/devices/{id}/state` | 当前设备状态：`never_seen` / `online` / `offline` |
| `GET`  | `/devices/{id}/stats` | 遥测计数：accepted / duplicate / stale / rejected |
| `GET`  | `/devices/{id}/shadow` | 设备影子全貌（desired / reported / delta / 版本 / 应用位点） |
| `PUT`  | `/devices/{id}/shadow/desired` | 下发期望状态（支持 `expected_version` 乐观锁） |
| `GET`  | `/devices/{id}/shadow/delta` | 设备重连后拉取待对齐增量 |

### 2.2 遥测报文

```json
{
  "credential": "k",
  "msg_id": "设备侧全局唯一消息ID",
  "device_ts_ms": 1716000000000,
  "payload": {"temp": 36.5},
  "reported": {"state": "online", "firmware": "v1"}
}
```

- `msg_id`：设备侧生成的**消息唯一标识**，是幂等去重的依据；
- `device_ts_ms`：**设备侧毫秒时间戳**，是新旧判定的唯一依据（不是服务器到达时间）；
- `payload`：遥测数据；`reported.state == "offline"` 时设备状态推进为 `offline`，
  其余新消息推进为 `online`；
- `reported` 中除 `state` 外的字段写入影子的 reported 区。

### 2.3 响应与失败原因（严格区分，不揉通用报错）

成功结论（HTTP 200）本身区分三种情况：

| outcome | 含义 | 是否计数/推进状态 |
| --- | --- | --- |
| `accepted` | 首次生效 | accepted_count +1，推进最新位点与影子 |
| `duplicate` | 同 `msg_id` 重复投递 | 否，仅 duplicate_count +1 |
| `stale` | 旧数据乱序到达 | 否（不覆盖新状态），仅 stale_count +1 |

拒绝统一返回 `{"error":{"code","message"}}`，code 与 HTTP 状态码一一对应：

| error.code | HTTP | 触发条件 |
| --- | --- | --- |
| `device_not_found` | 404 | 设备未注册 |
| `invalid_credentials` | 401 | 凭证不匹配 |
| `device_disabled` | 403 | 设备存在但被禁用 |
| `quota_exceeded` | 429 | accepted 条数已达 `quota` |
| `invalid_message` | 400 | 报文字段缺失/类型错误 |
| `desired_version_conflict` | 409 | 期望状态乐观版本冲突 |
| `desired_too_large` | 413 | desired 补丁超过大小上限 |

校验顺序固定为：**报文合法性 → 设备存在 → 凭证 → 启用状态 →（锁内）去重 →
新旧判定 → 配额 → 原子生效**。因此同一请求只会命中一个明确原因，例如“配额已满
时重复投递”仍返回 `duplicate` 而不是 `quota_exceeded`。

## 3. 去重与新旧判定规则（结论可解释）

1. **幂等去重**：服务端为每个设备维护已处理 `msg_id` 集合。同一条消息无论投递
   多少次（含离线补报、重连重放），只有第一次 `accepted`，之后全部 `duplicate`，
   不重复计数、不重复推进状态。响应中的 `reason` 会给出“已处理过（首次时间戳）”。
2. **新旧判定只看设备侧时间戳**：设已处理最新时间戳为 `T_last`，
   - `device_ts_ms > T_last`：新数据，允许生效；
   - `device_ts_ms <= T_last`：判为 `stale`，**旧状态不得覆盖新状态**；
   - 相等时间戳的不同消息同样判旧（严格“更新”才生效），保证结论与到达顺序无关、可复现。
   `reason` 会同时打印输入时间戳、`T_last` 及对应 `msg_id`，判定依据可解释。
3. **设备影子双重保护**：影子另记 `reported_ts_ms` 与 `last_applied_msg_id`：
   - 重连后重放同一 `msg_id`：即使绕过服务层直接重放，影子位点也会拒绝二次应用；
   - 更旧的 `reported` 永不覆盖更新的 reported；
   - `delta = desired 中与 reported 不一致的字段`，上报对齐后自动收敛，
     设备重连可通过 `/shadow/delta` 只拉增量。

## 4. 并发下的一致性保证

- 每个设备一把独立的可重入锁（`threading.RLock`），设备之间互不阻塞；
- 同一设备的 **去重判定、新旧判定、配额扣减、计数自增、最新位点、设备状态、
  影子更新全部在同一临界区完成**——要么一起生效，要么都不发生，不会出现
  “计数加了但状态没更新”的半截写入；
- 同一消息被 N 个线程同时投递时，**恰好 1 个 `accepted`、N-1 个 `duplicate`**，
  最终 accepted 计数恒为 1（测试以 32 线程 + Barrier 同时冲击验证）；
- accepted 的 `count_after` 在并发下严格连续 `1..n`，数目对得上。

## 5. 本地验证方式（无需外部服务/真实凭据）

```bash
# 1) 全部单测：重复、乱序、同设备并发写入、超额拒绝、错误码区分、影子补报
uv run python -m unittest discover -s tests -v

# 2) 本机端到端演示（含 32 线程并发同消息、重连补报、各类拒绝）
uv run python scripts/local_demo.py

# 3) 真实 HTTP 服务 + curl（见第 1 节）
uv run uvicorn main:app --host 127.0.0.1 --port 8000
```

测试与演示运行时会输出形如下面的**判定日志**，打印触发判定的输入与依据：

```
INFO iot.ingest | 接受上报 | 输入 device_id=dev-1 msg_id=m1 device_ts_ms=1000 | 判定=accepted 依据=首次生效... | 生效后 accepted=1 state=online
INFO iot.ingest | 重复上报 | 输入 device_id=dev-1 msg_id=m1 device_ts_ms=1000 | 判定=duplicate 依据=重复投递: msg_id=m1 已处理过（首次 device_ts_ms=1000）...
INFO iot.ingest | 乱序上报 | 输入 device_id=dev-1 msg_id=m0 device_ts_ms=900 | 判定=stale 依据=乱序旧数据: 900 <= 已处理最新 1000(msg_id=m1)，拒绝覆盖
WARNING iot.ingest | 拒绝上报 | 输入 ... | 判定 code=quota_exceeded 依据=超出设备配额...
```

### 代码结构

```
app/
  errors.py    # ErrorCode 与 ServiceError（失败原因可区分的单一来源）
  models.py    # Device / TelemetryMessage / IngestResult / TelemetryStats / DeviceShadow
  store.py     # InMemoryStore：设备注册表、每设备 RLock、运行态
  shadow.py    # ShadowService：desired/reported 分离、delta、补报位点、乐观版本
  service.py   # TelemetryService：校验顺序、去重、新旧、配额、原子生效
  api.py       # FastAPI 路由与统一错误响应
scripts/local_demo.py   # 本机端到端演示
tests/                  # service / shadow / api 三组单测
```

## 6. 适用范围说明

当前为**单进程内存实现**，目标是本地可运行、行为可复现与语义自洽。多实例水平
扩展时，可将 `InMemoryStore` 替换为带事务/单键原子性的外部存储、将每设备锁替换
为租约或串行分区（如按 device_id 分区的单线程消费者），上述去重、新旧、配额与
原子提交的判定顺序与错误码契约保持不变。
