"""重连续传能力测试：期望差异、续传游标、批量回放等价性。

日志逐条打印触发判定的输入与依据；每个用例自建独立服务，
在同一进程里连续跑多轮互不影响。
"""
from __future__ import annotations

import logging

from iot_twin.history import HistoryStore, RetentionConfig
from iot_twin.ingest import IngestService, Outcome
from iot_twin.models import TelemetryMessage
from iot_twin.registry import DeviceRegistry
from iot_twin.shadow import MISSING, ShadowStore

log = logging.getLogger("test_reconnect")


def build_service(retention: RetentionConfig | None = None, quota: int = 1000):
    registry = DeviceRegistry()
    registry.register("dev-1", "cred", quota=quota)
    return IngestService(registry, ShadowStore(),
                         HistoryStore(retention)), registry


def m(mid: str, ts: float, state=None, metrics=None) -> TelemetryMessage:
    return TelemetryMessage("dev-1", mid, ts, state or {}, metrics or {})


# ---- 期望差异 -----------------------------------------------------------

def test_desired_delta_lists_each_misaligned_field() -> None:
    svc, _ = build_service()
    svc._shadows.set_desired("dev-1", {"on": True, "brightness": 80, "mode": "auto"})

    # 只对上一个字段；其余逐字段可见，未上报与上报不匹配可区分。
    svc.ingest("dev-1", "cred", m("m1", 1.0, {"on": True, "mode": "manual"}))
    delta = svc._shadows.desired_delta("dev-1")
    log.info("delta: pending=%s aligned=%s", delta.pending, delta.aligned)

    assert set(delta.pending) == {"brightness", "mode"}
    assert delta.pending["brightness"] == (80, MISSING)   # 从未上报
    assert delta.pending["mode"] == ("auto", "manual")    # 上报了但不匹配
    assert delta.aligned == ("on",)


def test_desired_delta_shrinks_as_fields_are_reported() -> None:
    svc, _ = build_service()
    svc._shadows.set_desired("dev-1", {"a": 1, "b": 2})

    d0 = svc._shadows.desired_delta("dev-1")
    assert set(d0.pending) == {"a", "b"}

    svc.ingest("dev-1", "cred", m("m1", 1.0, {"a": 1}))
    d1 = svc._shadows.desired_delta("dev-1")
    log.info("上报 a 后 pending=%s aligned=%s", d1.pending, d1.aligned)
    assert set(d1.pending) == {"b"}
    assert d1.aligned == ("a",)

    svc.ingest("dev-1", "cred", m("m2", 2.0, {"b": 2}))
    d2 = svc._shadows.desired_delta("dev-1")
    log.info("上报 b 后 pending=%s aligned=%s", d2.pending, d2.aligned)
    assert d2.pending == {}
    assert set(d2.aligned) == {"a", "b"}


def test_desired_delta_uses_latest_desired_value() -> None:
    """离线期间期望改了多次：差异始终以最新期望为准。"""
    svc, _ = build_service()
    svc._shadows.set_desired("dev-1", {"target": 10})
    svc._shadows.set_desired("dev-1", {"target": 20})
    svc._shadows.set_desired("dev-1", {"target": 30})

    svc.ingest("dev-1", "cred", m("m1", 1.0, {"target": 10}))
    delta = svc._shadows.desired_delta("dev-1")
    log.info("期望已改为 30，上报 10 -> pending=%s", delta.pending)
    assert delta.pending == {"target": (30, 10)}

    svc.ingest("dev-1", "cred", m("m2", 2.0, {"target": 30}))
    delta = svc._shadows.desired_delta("dev-1")
    assert delta.pending == {}


# ---- 续传游标与批量回放 --------------------------------------------------

def test_resume_cursor_reports_last_position() -> None:
    svc, _ = build_service()
    svc.ingest("dev-1", "cred", m("m1", 10.0))
    svc.ingest("dev-1", "cred", m("m2", 20.0))

    cursor = svc.resume_cursor("dev-1", "cred")
    log.info("游标: last=%s floor=%s count=%d",
             cursor.last_device_ts, cursor.earliest_retained_ts,
             cursor.retained_count)
    assert cursor.last_device_ts == 20.0
    assert cursor.earliest_retained_ts == 10.0
    assert cursor.retained_count == 2


def test_backfill_equivalent_to_in_order_ingest() -> None:
    """断口补报：乱序批量 + 重复投递，结果必须等价于按序逐条上报。"""
    # 参照系：所有消息从一开始按 device_ts 升序逐条上报。
    ref, _ = build_service(RetentionConfig(max_messages=100))
    reference_msgs = [
        m("live", 50.0, {"live": True}),
        m("lost-1", 10.0, {"color": "red", "seq": 1}),
        m("lost-2", 20.0, {"color": "green", "seq": 2}),
        m("lost-3", 30.0, {"color": "blue", "seq": 3}),
    ]
    for x in sorted(reference_msgs, key=lambda x: (x.device_ts, x.message_id)):
        ref.ingest("dev-1", "cred", x)

    # 实际路径：先收到 live，设备掉线；重连后把断线期间的一批乱序补上来，
    # 其中夹带一条已经投递过的 live（完全重复投递）。
    svc, _ = build_service(RetentionConfig(max_messages=100))
    svc.ingest("dev-1", "cred", m("live", 50.0, {"live": True}))
    batch = [
        m("lost-3", 30.0, {"color": "blue", "seq": 3}),
        m("lost-1", 10.0, {"color": "red", "seq": 1}),
        m("live", 50.0, {"live": True}),   # 重复
        m("lost-2", 20.0, {"color": "green", "seq": 2}),
    ]
    result = svc.backfill("dev-1", "cred", after_device_ts=50.0, messages=batch)
    for it in result.items:
        log.info("补报 ts=%s id=%s -> %s (%s)",
                 it.device_ts, it.message_id, it.outcome.value, it.reason)

    assert result.applied == 3
    assert result.duplicates == 1
    # 状态与参照系完全一致（中间消息丢失后补回也能还原按序结果）
    sh = svc._shadows.get_or_create("dev-1")
    ref_sh = ref._shadows.get_or_create("dev-1")
    log.info("补报后 reported=%s 参照=%s", sh.reported, ref_sh.reported)
    assert sh.reported == ref_sh.reported == {
        "live": True, "color": "blue", "seq": 3,
    }
    assert sh.last_device_ts == 50.0
    st, ref_st = svc.stats("dev-1"), ref.stats("dev-1")
    assert st.accepted_messages == ref_st.accepted_messages == 4
    assert st.duplicate_messages == ref_st.duplicate_messages + 1
    assert st.metric_sums == ref_st.metric_sums


def test_backfill_with_metrics_sums_match_in_order() -> None:
    """补报的指标统计同样与按序逐条上报等价（含乱序指标求和）。"""
    ref, _ = build_service()
    for ts, v in [(1.0, 1.0), (2.0, 2.0), (3.0, 3.0), (4.0, 4.0)]:
        ref.ingest("dev-1", "cred", m(f"r{ts}", ts, metrics={"temp": v}))

    svc, _ = build_service()
    svc.ingest("dev-1", "cred", m("r4", 4.0, metrics={"temp": 4.0}))
    # 设备从 /resume 拿到 last_device_ts=4.0，从该位置续传更早的丢失消息。
    result = svc.backfill(
        "dev-1", "cred", 4.0,
        [m("r2", 2.0, metrics={"temp": 2.0}),
         m("r1", 1.0, metrics={"temp": 1.0}),
         m("r3", 3.0, metrics={"temp": 3.0})],
    )
    assert result.applied == 3
    st = svc.stats("dev-1")
    log.info("补报指标 sums=%s counts=%s", st.metric_sums, st.metric_counts)
    assert st.metric_sums == {"temp": 10.0}
    assert st.metric_counts == {"temp": 4}
    assert st.averages() == {"temp": 2.5}


def test_backfill_sorts_by_device_ts_within_batch() -> None:
    """同一批内时间更早的消息先落地：字段覆盖顺序正确。"""
    svc, _ = build_service()
    result = svc.backfill(
        "dev-1", "cred", None,
        [m("b", 3.0, {"fw": "3"}),
         m("a", 1.0, {"fw": "1"}),
         m("c", 2.0, {"fw": "2"})],
    )
    assert [i.message_id for i in result.items] == ["a", "c", "b"]
    assert svc._shadows.get_or_create("dev-1").reported == {"fw": "3"}


def test_backfill_duplicate_id_not_reapplied() -> None:
    svc, _ = build_service()
    svc.ingest("dev-1", "cred", m("m1", 1.0, {"v": 1}, {"temp": 1.0}))
    result = svc.backfill(
        "dev-1", "cred", 1.0,
        [m("m1", 1.0, {"v": 99}, {"temp": 99.0})],
    )
    assert result.applied == 0 and result.duplicates == 1
    sh = svc._shadows.get_or_create("dev-1")
    assert sh.reported == {"v": 1}
    assert sh.reported_version == 1  # 重复不推进版本
    assert svc.stats("dev-1").metric_sums == {"temp": 1.0}


def test_backfill_quota_wall_marks_remaining_items() -> None:
    """批次内配额耗尽：撞上的条目及其后条目确定性记为 quota_exceeded，
    不写入、不计数；此前条目正常生效。结论与调度顺序无关（单批已排序）。"""
    svc, reg = build_service(quota=3)
    # 先用掉 1 个配额。
    svc.ingest("dev-1", "cred", m("used", 0.5))
    result = svc.backfill(
        "dev-1", "cred", 0.5,
        [m(f"m{i}", float(i), {"k": i}) for i in range(1, 6)],
    )
    outcomes = [it.outcome for it in result.items]
    log.info("配额墙: %s applied=%d quota_rejected=%d",
             [o.value for o in outcomes], result.applied,
             result.quota_rejected)
    assert result.applied == 2
    assert result.quota_rejected == 3
    assert outcomes == [
        Outcome.APPLIED, Outcome.APPLIED,
        Outcome.QUOTA_EXCEEDED, Outcome.QUOTA_EXCEEDED, Outcome.QUOTA_EXCEEDED,
    ]
    # 计数/配额/状态只反映真正生效的两条。
    stats = svc.stats("dev-1")
    assert stats.accepted_messages == 3
    assert reg.get("dev-1").accepted_count == 3
    assert svc._shadows.get_or_create("dev-1").reported == {"k": 2}
