"""回放历史保留策略测试：有界裁剪、缺口明确要求全量对齐、状态仍可重放。"""
from __future__ import annotations

import logging

import pytest

from iot_twin.errors import RejectError, RejectReason
from iot_twin.history import HistoryStore, RetentionConfig
from iot_twin.ingest import IngestService
from iot_twin.models import TelemetryMessage
from iot_twin.registry import DeviceRegistry
from iot_twin.shadow import ShadowStore

log = logging.getLogger("test_retention")


def build_service(retention: RetentionConfig, quota: int = 100_000):
    registry = DeviceRegistry()
    registry.register("dev-1", "cred", quota=quota)
    history = HistoryStore(retention)
    return IngestService(registry, ShadowStore(), history), history, registry


def m(mid: str, ts: float, state=None) -> TelemetryMessage:
    return TelemetryMessage("dev-1", mid, ts, state or {})


def test_retained_count_is_bounded_by_max_messages() -> None:
    svc, history, _ = build_service(RetentionConfig(max_messages=5))
    for i in range(1, 11):
        svc.ingest("dev-1", "cred", m(f"m{i}", float(i), {"k": i}))

    cursor = history.cursor("dev-1")
    log.info("插入 10 条、上限 5 -> retained=%d floor=%s last=%s",
             cursor.retained_count, cursor.earliest_retained_ts,
             cursor.last_device_ts)
    assert cursor.retained_count == 5  # 内存严格跟上限走
    assert cursor.earliest_retained_ts == 6.0
    assert cursor.last_device_ts == 10.0
    # 被裁剪掉的 id 已遗忘，但其状态折叠进了最终 reported。
    assert svc._shadows.get_or_create("dev-1").reported == {"k": 10}


def test_eviction_folds_state_in_time_order() -> None:
    """裁剪后 reported 仍等价于全部历史按序上报的最终状态。"""
    svc, history, _ = build_service(
        RetentionConfig(max_messages=2, max_age_seconds=None)
    )
    svc.ingest("dev-1", "cred", m("m1", 1.0, {"a": 1, "shared": "old"}))
    svc.ingest("dev-1", "cred", m("m2", 2.0, {"a": 2}))
    svc.ingest("dev-1", "cred", m("m3", 3.0, {"shared": "new"}))  # 挤掉 m1

    state = history.reported_state("dev-1")
    log.info("裁剪后重放状态=%s", state)
    assert state == {"a": 2, "shared": "new"}
    assert svc._shadows.get_or_create("dev-1").reported == state


def test_resume_within_retained_range_succeeds() -> None:
    svc, history, _ = build_service(RetentionConfig(max_messages=3))
    for i in range(1, 7):
        svc.ingest("dev-1", "cred", m(f"m{i}", float(i)))
    # floor=4.0：位置正好等于下界，区间连续，可正常补报。
    history.can_resume_from("dev-1", 4.0)
    log.info("续传位置 4.0 >= floor=4.0，允许回放")


def test_resume_beyond_retained_range_requires_resync() -> None:
    """设备回来太晚：要接的位置被回收了，必须明确要求全量对齐。"""
    svc, history, _ = build_service(RetentionConfig(max_messages=3))
    for i in range(1, 7):
        svc.ingest("dev-1", "cred", m(f"m{i}", float(i)))

    for pos in (1.0, 3.9, None):
        with pytest.raises(RejectError) as exc:
            history.can_resume_from("dev-1", pos)
        log.info("续传位置=%s floor=4.0 -> %s: %s",
                 pos, exc.value.reason.value, exc.value)
        assert exc.value.reason is RejectReason.RESYNC_REQUIRED


def test_backfill_beyond_retained_range_rejects_entire_batch() -> None:
    """缺口场景下整批拒绝：不产生任何部分写入，绝不静默残缺回放。"""
    svc, history, _ = build_service(RetentionConfig(max_messages=2))
    svc.ingest("dev-1", "cred", m("m5", 5.0))
    svc.ingest("dev-1", "cred", m("m6", 6.0))
    # floor=5.0；设备拿着更早的位置 4.0 回来补报 m5 之前的消息。
    before = history.cursor("dev-1")
    with pytest.raises(RejectError) as exc:
        svc.backfill(
            "dev-1", "cred", 4.0,
            [m("m3", 3.0), m("m4", 4.0), m("m5", 5.0), m("m7", 7.0)],
        )
    log.info("越界补报 -> %s（%s）", exc.value.reason.value, exc.value)
    assert exc.value.reason is RejectReason.RESYNC_REQUIRED
    after = history.cursor("dev-1")
    # 整批（连同时间更新的 m7）都没有落地
    assert (after.retained_count, after.last_device_ts) == (
        before.retained_count, before.last_device_ts,
    )
    assert svc.stats("dev-1").accepted_messages == 2


def test_full_resync_after_gap_restores_alignment() -> None:
    """按服务端提示全量对齐：重新上报当前全量状态后差异消失、统计自洽。"""
    svc, _history, _reg = build_service(RetentionConfig(max_messages=3))
    for i in range(1, 7):
        svc.ingest("dev-1", "cred", m(f"m{i}", float(i), {"k": i}))

    with pytest.raises(RejectError):
        svc.backfill("dev-1", "cred", 1.0, [m("lost", 1.5)])

    # 设备改用新 message_id 全量上报当前状态（时间戳必须新于 last）。
    r = svc.ingest("dev-1", "cred", m("resync", 7.0, {"k": 7}))
    assert r.outcome.value == "applied"
    assert svc._shadows.get_or_create("dev-1").reported == {"k": 7}
    log.info("全量对齐后 reported=%s",
             svc._shadows.get_or_create("dev-1").reported)


def test_max_age_span_evicts_old_entries() -> None:
    """设备时间跨度上限：早于 最新ts-跨度 的条目被裁剪。"""
    svc, history, _ = build_service(
        RetentionConfig(max_messages=100, max_age_seconds=10.0)
    )
    for ts in (1.0, 5.0, 11.0, 12.0):
        svc.ingest("dev-1", "cred", m(f"m{ts}", ts, {"k": ts}))
    cursor = history.cursor("dev-1")
    log.info("跨度=10，最新=12 -> floor=%s count=%d",
             cursor.earliest_retained_ts, cursor.retained_count)
    # horizon=2.0：ts=1.0 被裁；5.0/11.0/12.0 保留。
    assert cursor.earliest_retained_ts == 5.0
    assert cursor.retained_count == 3


def test_invalid_retention_config_rejected() -> None:
    with pytest.raises(ValueError):
        RetentionConfig(max_messages=0)
    with pytest.raises(ValueError):
        RetentionConfig(max_messages=10, max_age_seconds=0)
    log.info("非法保留参数被拒绝")


def test_evicted_id_is_forgotten_and_replayed_entry_reapplied() -> None:
    """被回收窗口遗忘的 id 再次投递会作为新消息生效（缺口的根因，
    也正因此越界位置才必须 resync 而不是静默回放）。"""
    svc, history, _ = build_service(
        RetentionConfig(max_messages=2, max_age_seconds=None)
    )
    svc.ingest("dev-1", "cred", m("old", 1.0, {"k": "old"}))
    svc.ingest("dev-1", "cred", m("mid", 2.0))
    svc.ingest("dev-1", "cred", m("new", 3.0))  # old 被挤出
    assert not history.is_known("dev-1", "old")
    log.info("old 已被回收遗忘：越界位置无法区分丢失补报与重复投递")
    # 窗口仍把 old 的状态折叠在最终结果里。
    assert history.reported_state("dev-1")["k"] == "old"
