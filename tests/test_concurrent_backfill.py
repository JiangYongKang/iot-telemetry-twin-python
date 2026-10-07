"""并发下的实时上报 + 补报回放：终态与顺序执行一致，计数与分类各自独立断言。

所有断言都只针对收敛后的终态，不依赖线程执行的先后顺序。
"""
from __future__ import annotations

import logging
import threading

from iot_twin.history import HistoryConfig, HistoryStore
from iot_twin.ingest import IngestService, Outcome
from iot_twin.models import TelemetryMessage
from iot_twin.registry import DeviceRegistry
from iot_twin.shadow import ShadowStore

log = logging.getLogger("test_concurrent_backfill")

CRED = "test-credential"


def make_service(quota: int, max_messages: int = 1000) -> tuple[IngestService, DeviceRegistry]:
    registry = DeviceRegistry()
    registry.register("dev-1", CRED, quota=quota)
    svc = IngestService(
        registry, ShadowStore(),
        HistoryStore(HistoryConfig(max_messages=max_messages,
                                   max_age_seconds=3600.0)),
    )
    return svc, registry


def msg(mid: str, ts: float) -> TelemetryMessage:
    return TelemetryMessage("dev-1", mid, ts,
                            state={"seq": int(ts)}, metrics={"v": 1.0})


def run_threads(*groups: list) -> None:
    threads = [threading.Thread(target=g) for g in groups for g in g]
    for th in threads:
        th.start()
    for th in threads:
        th.join()


def test_concurrent_live_and_backfill_converge_to_in_order() -> None:
    n = 100
    svc, registry = make_service(quota=n)
    live_ts = list(range(51, n + 1))      # 实时在线上报（设备侧顺序流）
    backfill_ts = list(range(1, 51))      # 断线攒下、重连补报

    def live_worker() -> None:
        # 设备的实时流本身按时间顺序发出，与补报流并发交错
        for ts in live_ts:
            svc.ingest("dev-1", CRED, msg(f"m-{ts}", float(ts)))

    def backfill_worker() -> None:
        svc.backfill("dev-1", CRED,
                     [msg(f"m-{ts}", float(ts)) for ts in backfill_ts])

    run_threads([live_worker], [backfill_worker])

    stats = svc.stats("dev-1")
    device = registry.get("dev-1")
    shadow = svc._shadows.get_or_create("dev-1")
    log.info("并发收敛: accepted=%d quota_used=%d last_ts=%s",
             stats.accepted_messages, device.accepted_count,
             shadow.last_device_ts)
    # 第一类断言：计数与配额严丝合缝
    assert stats.accepted_messages == n
    assert device.accepted_count == n
    assert stats.metric_sums == {"v": float(n)}
    # 终态与“从一开始按顺序逐条上报”一致
    assert shadow.last_device_ts == float(n)
    assert shadow.reported == {"seq": n}


def test_concurrent_duplicate_deliveries_apply_exactly_once() -> None:
    svc, registry = make_service(quota=10)
    n_threads = 8
    outcomes: list[Outcome] = []
    lock = threading.Lock()

    def worker() -> None:
        r = svc.ingest("dev-1", CRED, msg("same-id", 1.0))
        with lock:
            outcomes.append(r.outcome)

    run_threads(*[[worker] for _ in range(n_threads)])

    stats = svc.stats("dev-1")
    log.info("同一消息 %d 路并发投递: outcomes=%s",
             n_threads, [o.value for o in outcomes])
    # 恰好生效一次，其余全部判重——两类计数分开断言
    assert outcomes.count(Outcome.APPLIED) == 1
    assert outcomes.count(Outcome.DUPLICATE) == n_threads - 1
    assert stats.accepted_messages == 1
    assert stats.duplicate_messages == n_threads - 1
    assert registry.get("dev-1").accepted_count == 1  # 重复投递不占配额


def test_concurrent_backfill_same_batch_is_idempotent() -> None:
    """同一批补报被并发提交两次：合并效果等于只提交一次。"""
    svc, registry = make_service(quota=100)
    batch = [msg(f"m-{ts}", float(ts)) for ts in range(1, 21)]

    def worker() -> None:
        svc.backfill("dev-1", CRED, list(batch))

    run_threads([worker], [worker])

    stats = svc.stats("dev-1")
    log.info("同一批次并发补报两次: accepted=%d duplicate=%d",
             stats.accepted_messages, stats.duplicate_messages)
    assert stats.accepted_messages == 20
    assert stats.duplicate_messages == 20  # 第二批整体判重
    assert registry.get("dev-1").accepted_count == 20
    assert svc._shadows.get_or_create("dev-1").reported == {"seq": 20}
