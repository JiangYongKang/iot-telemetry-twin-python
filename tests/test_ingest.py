"""接入链路测试：重复上报、乱序到达、并发写入、超额拒绝、原因区分。

所有用例只依赖内存实现，不需要外部服务或真实凭据。
日志中打印触发判定的输入与依据，便于复核结论。
"""
from __future__ import annotations

import logging
import threading

import pytest

from iot_twin.errors import RejectError, RejectReason
from iot_twin.ingest import IngestService, Outcome
from iot_twin.models import TelemetryMessage
from iot_twin.registry import DeviceRegistry
from iot_twin.shadow import ShadowStore

log = logging.getLogger("test_ingest")


@pytest.fixture()
def service() -> IngestService:
    registry = DeviceRegistry()
    registry.register("dev-1", "test-credential", quota=1000)
    return IngestService(registry, ShadowStore())


def msg(mid: str, ts: float, state=None, metrics=None) -> TelemetryMessage:
    return TelemetryMessage("dev-1", mid, ts, state or {}, metrics or {})


def test_duplicate_delivery_is_idempotent(service: IngestService) -> None:
    m = msg("m-1", 100.0, state={"on": True}, metrics={"temp": 21.5})
    first = service.ingest("dev-1", "test-credential", m)
    log.info("输入 message_id=%s device_ts=%s -> %s (%s)",
             m.message_id, m.device_ts, first.outcome, first.reason)
    assert first.outcome is Outcome.APPLIED

    for i in range(3):  # 同一条消息重复投递
        again = service.ingest("dev-1", "test-credential", m)
        log.info("重复投递 #%d message_id=%s -> %s (%s)",
                 i + 1, m.message_id, again.outcome, again.reason)
        assert again.outcome is Outcome.DUPLICATE

    stats = service.stats("dev-1")
    log.info("统计: accepted=%d duplicate=%d sums=%s",
             stats.accepted_messages, stats.duplicate_messages, stats.metric_sums)
    assert stats.accepted_messages == 1  # 不重复计数
    assert stats.duplicate_messages == 3
    assert stats.metric_sums == {"temp": 21.5}
    shadow = service._shadows.get_or_create("dev-1")
    assert shadow.reported_version == 1  # 不重复推进状态


def test_out_of_order_does_not_overwrite_newer_state(service: IngestService) -> None:
    newer = msg("m-2", 200.0, state={"firmware": "2.0"})
    older = msg("m-1", 100.0, state={"firmware": "1.0"})

    r1 = service.ingest("dev-1", "test-credential", newer)
    log.info("先到新数据 device_ts=%s -> %s (%s)", newer.device_ts, r1.outcome, r1.reason)
    r2 = service.ingest("dev-1", "test-credential", older)
    log.info("乱序旧数据 device_ts=%s -> %s (%s)", older.device_ts, r2.outcome, r2.reason)

    assert r1.outcome is Outcome.APPLIED
    assert r2.outcome is Outcome.STALE
    shadow = service._shadows.get_or_create("dev-1")
    log.info("影子 reported=%s last_device_ts=%s", shadow.reported, shadow.last_device_ts)
    assert shadow.reported == {"firmware": "2.0"}  # 旧数据没盖回新状态
    assert shadow.last_device_ts == 200.0
    assert service.stats("dev-1").stale_messages == 1


def test_reconnect_backfill_not_reapplied(service: IngestService) -> None:
    """设备重连后的增量补报（同 id 或同 ts）不得重复应用。"""
    live = msg("m-9", 300.0, state={"connected": True})
    service.ingest("dev-1", "test-credential", live)
    # 离线期间缓存的补报：时间戳都早于已见最大值
    for i, ts in enumerate([100.0, 200.0, 300.0]):
        r = service.ingest("dev-1", "test-credential",
                           msg(f"backfill-{i}", ts, state={"connected": False}))
        log.info("补报 device_ts=%s -> %s (%s)", ts, r.outcome, r.reason)
        assert r.outcome is Outcome.STALE
    shadow = service._shadows.get_or_create("dev-1")
    assert shadow.reported == {"connected": True}
    assert shadow.reported_version == 1


def test_concurrent_writes_are_atomic() -> None:
    """同一设备并发上报：计数与状态更新同生同灭，数目对得上。"""
    registry = DeviceRegistry()
    n_threads, n_msgs = 8, 50
    total = n_threads * n_msgs
    registry.register("dev-1", "test-credential", quota=total)
    svc = IngestService(registry, ShadowStore())
    errors: list[Exception] = []

    def worker(t: int) -> None:
        try:
            for i in range(n_msgs):
                m = msg(f"t{t}-m{i}", ts=float(t * n_msgs + i + 1),
                        state={"seq": t * n_msgs + i + 1}, metrics={"v": 1.0})
                svc.ingest("dev-1", "test-credential", m)
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert not errors
    stats = svc.stats("dev-1")
    device = registry.get("dev-1")
    log.info("并发写入: accepted=%d quota_used=%d sum_v=%s",
             stats.accepted_messages, device.accepted_count,
             stats.metric_sums.get("v"))
    assert stats.accepted_messages == total
    assert device.accepted_count == total          # 配额计数与统计一致
    assert stats.metric_sums == {"v": float(total)}
    assert stats.metric_counts == {"v": total}
    shadow = svc._shadows.get_or_create("dev-1")
    assert shadow.last_device_ts == float(total)   # 最大时间戳来自最大消息
    assert shadow.reported == {"seq": total}


def test_concurrent_quota_exhaustion_has_no_partial_writes() -> None:
    """并发打到配额上限：要么整条生效要么整条拒绝，无半截写入。"""
    registry = DeviceRegistry()
    quota = 10
    registry.register("dev-1", "test-credential", quota=quota)
    svc = IngestService(registry, ShadowStore())
    rejected: list[RejectError] = []
    lock = threading.Lock()

    def worker(t: int) -> None:
        for i in range(10):
            try:
                svc.ingest("dev-1", "test-credential",
                           msg(f"t{t}-m{i}", ts=float(t * 10 + i + 1),
                               metrics={"v": 1.0}))
            except RejectError as e:
                with lock:
                    rejected.append(e)

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(4)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    stats = svc.stats("dev-1")
    device = registry.get("dev-1")
    log.info("配额=%d accepted=%d quota_used=%d rejected=%d reasons=%s",
             quota, stats.accepted_messages, device.accepted_count,
             len(rejected), {e.reason.value for e in rejected})
    assert stats.accepted_messages == quota
    assert device.accepted_count == quota
    assert stats.metric_sums == {"v": float(quota)}  # 计数与统计对得上
    assert len(rejected) == 40 - quota
    assert all(e.reason is RejectReason.QUOTA_EXCEEDED for e in rejected)


def test_reject_reasons_are_distinguishable() -> None:
    registry = DeviceRegistry()
    registry.register("dev-ok", "right", quota=1)
    registry.register("dev-off", "right")
    registry.disable("dev-off")
    svc = IngestService(registry, ShadowStore())

    cases = [
        ("ghost", "right", "dev-ok", RejectReason.UNKNOWN_DEVICE),
        ("dev-ok", "wrong", "dev-ok", RejectReason.BAD_CREDENTIAL),
        ("dev-off", "right", "dev-off", RejectReason.DEVICE_DISABLED),
    ]
    for device_id, cred, msg_device, expected in cases:
        m = TelemetryMessage(msg_device, "x-1", 1.0)
        with pytest.raises(RejectError) as exc_info:
            svc.ingest(device_id, cred, m)
        log.info("输入 device=%s credential=%s -> 拒绝原因 %s",
                 device_id, cred, exc_info.value.reason.value)
        assert exc_info.value.reason is expected

    # 配额：第一条成功，第二条超额
    svc.ingest("dev-ok", "right", TelemetryMessage("dev-ok", "q-1", 1.0))
    with pytest.raises(RejectError) as exc_info:
        svc.ingest("dev-ok", "right", TelemetryMessage("dev-ok", "q-2", 2.0))
    log.info("配额=1 第二条上报 -> 拒绝原因 %s", exc_info.value.reason.value)
    assert exc_info.value.reason is RejectReason.QUOTA_EXCEEDED
    # 四种原因互不相同
    assert len({r.value for r in RejectReason}) == 4
