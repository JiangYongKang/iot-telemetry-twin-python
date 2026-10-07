"""并发一致性测试：计数/配额严丝合缝，与"重复/过期判定"分别独立断言。

关键设计：
- 对到达顺序敏感的结论（互不重复的消息全部生效）走批量回放路径——
  回放内部按设备时间排序插入，结论不依赖线程被调度的先后；
- 对"是否重复/过期"的判定用 barrier 让所有线程在同一刻发起；
- 每类性质各自断言，不用同一个数字同时说明两件事。
"""
from __future__ import annotations

import logging
import threading

from iot_twin.history import HistoryStore, RetentionConfig
from iot_twin.ingest import IngestService, Outcome
from iot_twin.models import TelemetryMessage
from iot_twin.registry import DeviceRegistry
from iot_twin.shadow import ShadowStore

log = logging.getLogger("test_concurrency")


def _service(quota: int = 1000, max_messages: int = 1000) -> IngestService:
    registry = DeviceRegistry()
    registry.register("dev-1", "cred", quota=quota)
    return IngestService(
        registry, ShadowStore(),
        HistoryStore(RetentionConfig(max_messages=max_messages)),
    )


def test_concurrent_duplicates_keep_counts_and_quota_exact() -> None:
    """N 个线程同时重投同一条消息：恰一次生效，其余全部 duplicate。

    两类结论分别断言，互不借用：
    A) 计数/配额：accepted、配额占用、指标和三者各自独立核对；
    B) 判定结论：每个返回 outcome 都被收集，duplicate 次数独立核对。
    """
    n = 16
    svc = _service()
    barrier = threading.Barrier(n)
    outcomes: list[Outcome] = []
    out_lock = threading.Lock()

    def worker() -> None:
        m = TelemetryMessage("dev-1", "same-id", 1.0,
                             {"on": True}, {"temp": 5.0})
        barrier.wait()  # 所有线程同时发起，不依赖调度顺序
        r = svc.ingest("dev-1", "cred", m)
        with out_lock:
            outcomes.append(r.outcome)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # —— 结论 B：判定本身（与计数无关的独立证据）——
    applied = [o for o in outcomes if o is Outcome.APPLIED]
    duplicates = [o for o in outcomes if o is Outcome.DUPLICATE]
    log.info("并发重投同一消息 outcomes=%s", [o.value for o in outcomes])
    assert len(applied) == 1
    assert len(duplicates) == n - 1
    assert len(outcomes) == n  # 没有第三类结果混进来

    # —— 结论 A：计数与配额（独立于上面的 outcome 列表重新核对）——
    stats = svc.stats("dev-1")
    device = svc._registry.get("dev-1")
    log.info("accepted=%d duplicate=%d quota_used=%d sums=%s",
             stats.accepted_messages, stats.duplicate_messages,
             device.accepted_count, stats.metric_sums)
    assert stats.accepted_messages == 1
    assert stats.duplicate_messages == n - 1
    assert device.accepted_count == 1          # 配额独立核对
    assert stats.metric_sums == {"temp": 5.0}  # 指标独立核对
    assert stats.metric_counts == {"temp": 1}
    assert svc._shadows.get_or_create("dev-1").reported_version == 1


def test_concurrent_stale_messages_are_all_rejected_independently() -> None:
    """高水位先立住，再让多线程同时投更早的旧消息：全部 stale。

    同样分两组断言：
    B) 每条返回都确实是 STALE（过期判定的独立证据）；
    A) accepted/配额/影子三项都没有被旧数据推进。
    """
    n = 12
    svc = _service()
    # 先确立高水位。
    svc.ingest("dev-1", "cred",
               TelemetryMessage("dev-1", "seed", 100.0, {"k": "new"}))

    barrier = threading.Barrier(n)
    outcomes: list[Outcome] = []
    out_lock = threading.Lock()

    def worker(i: int) -> None:
        m = TelemetryMessage("dev-1", f"old-{i}", float(i), {"k": "old"})
        barrier.wait()
        r = svc.ingest("dev-1", "cred", m)
        with out_lock:
            outcomes.append(r.outcome)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # —— 结论 B：过期判定逐条成立 ——
    log.info("并发旧数据 outcomes=%s", sorted(o.value for o in outcomes))
    assert outcomes and all(o is Outcome.STALE for o in outcomes)
    assert len(outcomes) == n

    # —— 结论 A：计数/配额/影子未被推进（独立证据）——
    stats = svc.stats("dev-1")
    shadow = svc._shadows.get_or_create("dev-1")
    log.info("accepted=%d stale=%d quota_used=%d reported=%s last_ts=%s",
             stats.accepted_messages, stats.stale_messages,
             svc._registry.get("dev-1").accepted_count,
             shadow.reported, shadow.last_device_ts)
    assert stats.accepted_messages == 1
    assert stats.stale_messages == n
    assert svc._registry.get("dev-1").accepted_count == 1  # 旧数据不占配额
    assert shadow.reported == {"k": "new"}
    assert shadow.last_device_ts == 100.0
    assert shadow.reported_version == 1


def test_concurrent_backfills_exact_counts_regardless_of_order() -> None:
    """多线程同时回放互不重叠时间片：全部生效，计数/配额/和严丝合缝。

    回放在窗口内按设备时间插入，与线程被调度的先后无关——这正是
    "不能依赖线程刚好按某顺序执行"要求下唯一稳妥的全量生效路径。
    """
    n_threads, per = 8, 25
    total = n_threads * per
    svc = _service(quota=total + 1, max_messages=total + 10)
    # 先取游标并立一个锚点：所有时间片都在锚点之后，窗口不发生裁剪。
    svc.ingest("dev-1", "cred",
               TelemetryMessage("dev-1", "anchor", 0.5))
    anchor = svc.resume_cursor("dev-1", "cred").last_device_ts
    barrier = threading.Barrier(n_threads)
    errors: list[Exception] = []

    def worker(t: int) -> None:
        base = t * per
        batch = [
            TelemetryMessage("dev-1", f"t{t}-{i}", float(base + i + 1),
                             {"seq": base + i + 1}, {"v": 1.0})
            for i in range(per)
        ]
        barrier.wait()  # 同时发起，任意调度顺序结果一致
        try:
            r = svc.backfill("dev-1", "cred", anchor, batch)
        except Exception as e:  # noqa: BLE001
            errors.append(e)
            return
        assert r.applied == per and r.duplicates == 0

    threads = [threading.Thread(target=worker, args=(t,))
               for t in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    stats = svc.stats("dev-1")
    log.info("并发回放互不重复: accepted=%d quota_used=%d sum=%s",
             stats.accepted_messages,
             svc._registry.get("dev-1").accepted_count, stats.metric_sums)
    assert stats.accepted_messages == total + 1  # 含锚点一条
    assert stats.duplicate_messages == 0
    assert stats.stale_messages == 0
    assert svc._registry.get("dev-1").accepted_count == total + 1
    assert stats.metric_sums == {"v": float(total)}  # 锚点无指标
    assert stats.metric_counts == {"v": total}


def test_concurrent_backfills_with_shared_duplicate_id() -> None:
    """多线程同时补报，且每批夹带同一个 message_id：只生效一次。

    重复结论（outcome 列表）与计数/配额结论分别断言，
    最终状态等价于把全部消息按序合并回放。
    """
    n = 4
    svc = _service(max_messages=500)
    svc.ingest("dev-1", "cred",
               TelemetryMessage("dev-1", "anchor", 0.5))
    anchor = svc.resume_cursor("dev-1", "cred").last_device_ts
    barrier = threading.Barrier(n)
    results = []
    res_lock = threading.Lock()
    errors: list[Exception] = []

    def worker(t: int) -> None:
        base = t * 10
        batch = [
            TelemetryMessage("dev-1", f"t{t}-m{i}", float(base + i + 1),
                             {"k": base + i + 1})
            for i in range(5)
        ]
        batch.append(TelemetryMessage("dev-1", "dup", 1000.0, {"k": 1000}))
        barrier.wait()
        try:
            r = svc.backfill("dev-1", "cred", anchor, batch)
        except Exception as e:  # noqa: BLE001
            errors.append(e)
            return
        with res_lock:
            results.append(r)

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    # —— 结论 B：每批的 duplicate 数独立核对，合计恰 n-1 ——
    dup_per_batch = sorted(r.duplicates for r in results)
    log.info("各批重复数=%s 生效数=%s", dup_per_batch,
             sorted(r.applied for r in results))
    assert sorted(dup_per_batch).count(0) == 1
    assert sorted(dup_per_batch).count(1) == n - 1
    assert sum(r.duplicates for r in results) == n - 1

    # —— 结论 A：计数/配额/最终状态独立核对 ——
    stats = svc.stats("dev-1")
    shadow = svc._shadows.get_or_create("dev-1")
    log.info("并发补报夹带重复: accepted=%d duplicates=%d quota_used=%d "
             "reported=%s last_ts=%s",
             stats.accepted_messages, stats.duplicate_messages,
             svc._registry.get("dev-1").accepted_count,
             shadow.reported, shadow.last_device_ts)
    assert stats.accepted_messages == 20 + 1 + 1  # 锚点 + 4x5 + dup
    assert stats.duplicate_messages == n - 1
    assert svc._registry.get("dev-1").accepted_count == 22
    assert shadow.reported == {"k": 1000}
    assert shadow.last_device_ts == 1000.0
