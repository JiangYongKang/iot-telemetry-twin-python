"""本机端到端演示：不依赖外部中间件或真实凭据。

依次演示：注册 -> 正常上报 -> 重复投递 -> 乱序到达 -> 同消息并发（恰好一次）
-> 设备影子期望/上报与增量 -> 重连补报 -> 配额拒绝 -> 失败原因区分。

运行：uv run python scripts/local_demo.py
"""

from __future__ import annotations

import logging
import sys
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# 允许从仓库根目录直接运行：uv run python scripts/local_demo.py
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.errors import ServiceError
from app.models import TelemetryMessage
from app.service import TelemetryService
from app.shadow import ShadowService
from app.store import InMemoryStore

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger("demo")


def tmsg(mid: str, ts: int, *, device="sensor-A", cred="s3cr3t", reported=None):
    return TelemetryMessage(device, cred, mid, ts, reported=reported or {})


def expect_reject(svc, msg, label: str) -> None:
    try:
        svc.ingest(msg)
    except ServiceError as e:
        log.info("[拒绝-%s] code=%s http=%s :: %s", label, e.code.value,
                 e.status_code, e.message)


def main() -> None:
    store = InMemoryStore()
    shadows = ShadowService(store)
    svc = TelemetryService(store, shadows)

    # 1) 注册：启用设备（配额 3）与一个被禁用设备
    svc.register_device("sensor-A", "s3cr3t", quota=4)  # m1, hot, m2, m3
    svc.register_device("sensor-B", "other")
    with store.get("sensor-B").lock:
        d = store.get("sensor-B").registered
        store.get("sensor-B").registered = type(d)(d.device_id, d.credential, False, d.quota)

    # 2) 正常、重复、乱序
    log.info("-> 正常 m1@1000: %s", svc.ingest(tmsg("m1", 1000, reported={"rpm": 100})).outcome.value)
    log.info("-> 重复 m1@1000: %s", svc.ingest(tmsg("m1", 1000)).outcome.value)
    log.info("-> 乱序 m0@0900: %s", svc.ingest(tmsg("m0", 900, reported={"rpm": 1})).outcome.value)

    # 3) 同一条消息 32 线程并发：恰好一次 accepted
    n = 32
    barrier = threading.Barrier(n)
    outcomes: list[str] = []
    lock = threading.Lock()

    def fire() -> None:
        barrier.wait()
        r = svc.ingest(tmsg("hot", 1001, reported={"rpm": 200}))
        with lock:
            outcomes.append(r.outcome.value)

    # 注意：参与 barrier 的线程数必须 <= 池 worker 数，否则会死锁。
    with ThreadPoolExecutor(max_workers=n) as pool:
        for _ in range(n):
            pool.submit(fire)
    log.info("-> 并发同消息: accepted=%s duplicate=%s",
             outcomes.count("accepted"), outcomes.count("duplicate"))

    # 4) 设备影子：期望 vs 上报、delta、重连补报
    shadows.update_desired("sensor-A", {"firmware": "v2"}, None)
    log.info("-> 期望下发后 delta=%s", shadows.sync_delta("sensor-A"))
    svc.ingest(tmsg("m2", 1002, reported={"firmware": "v2"}))
    log.info("-> 上报对齐后 delta=%s", shadows.sync_delta("sensor-A"))
    replay = svc.ingest(tmsg("m2", 1002, reported={"firmware": "v2"}))
    log.info("-> 重连重放 m2: %s（%s）", replay.outcome.value, replay.reason)

    # 5) 配额：m3 生效（第 3 条），m4 被拒绝；重复/乱序仍按幂等处理
    log.info("-> m3@1003: %s", svc.ingest(tmsg("m3", 1003)).outcome.value)
    expect_reject(svc, tmsg("m4", 1004), "超额")

    # 6) 失败原因严格可区分
    expect_reject(svc, tmsg("x", 1, device="ghost"), "未注册")
    expect_reject(svc, tmsg("x", 1, cred="bad"), "凭证错误")
    expect_reject(svc, tmsg("x", 1, device="sensor-B", cred="other"), "已禁用")

    st = svc.stats("sensor-A")
    log.info("-> 最终统计: accepted=%s duplicate=%s stale=%s rejected=%s last=%s@%s",
             st.accepted_count, st.duplicate_count, st.stale_count, st.rejected_count,
             st.last_msg_id, st.last_device_ts_ms)
    assert st.accepted_count == 4  # m1 + hot + m2 + m3
    assert outcomes.count("accepted") == 1
    log.info("全部断言通过，演示结束。")


if __name__ == "__main__":
    main()
