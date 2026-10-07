"""断线重连：续传位置查询、批量补报回放、与顺序上报的等价性、缺口判定。

等价性基准：同一批消息在参照服务上按 device_ts 升序逐条上报，
被测服务先在线收一部分、断线攒一批、重连后一次补报，
两者的最终影子状态与统计必须一模一样。
"""
from __future__ import annotations

import logging

import pytest

from iot_twin.history import HistoryConfig, HistoryStore
from iot_twin.ingest import IngestService, Outcome
from iot_twin.models import TelemetryMessage
from iot_twin.registry import DeviceRegistry
from iot_twin.shadow import ShadowStore

log = logging.getLogger("test_backfill")

CRED = "test-credential"


def make_service(max_messages: int = 1000, max_age: float = 3600.0,
                 quota: int = 1000) -> IngestService:
    registry = DeviceRegistry()
    registry.register("dev-1", CRED, quota=quota)
    return IngestService(
        registry, ShadowStore(),
        HistoryStore(HistoryConfig(max_messages=max_messages,
                                   max_age_seconds=max_age)),
    )


def msg(mid: str, ts: float, state=None, metrics=None) -> TelemetryMessage:
    return TelemetryMessage("dev-1", mid, ts, state or {}, metrics or {})


def feed_in_order(service: IngestService, msgs: list[TelemetryMessage]) -> None:
    for m in sorted(msgs, key=lambda m: m.device_ts):
        r = service.ingest("dev-1", CRED, m)
        assert r.outcome is Outcome.APPLIED


def test_resume_reports_position_and_window() -> None:
    svc = make_service()
    for i in range(1, 4):
        svc.ingest("dev-1", CRED, msg(f"m-{i}", float(i)))
    info = svc.resume("dev-1", CRED)
    log.info("续传位置: last_device_ts=%s 窗口起点=%s 窗口条数=%d",
             info.last_device_ts, info.earliest_retained_ts, info.retained_count)
    assert info.last_device_ts == 3.0       # “上次报到 3.0”
    assert info.earliest_retained_ts == 1.0
    assert info.retained_count == 3


def test_backfill_after_reconnect_equals_in_order() -> None:
    """在线收 1..3，断线攒 4..8（乱序、含已送达的 3），补报后与全顺序等价。"""
    live = [msg(f"m-{i}", float(i), state={"seq": i}, metrics={"v": float(i)})
            for i in range(1, 4)]
    buffered = [msg(f"m-{i}", float(i), state={"seq": i}, metrics={"v": float(i)})
                for i in range(3, 9)]  # 设备不确定 m-3 是否送达，一并补报
    shuffled = [buffered[i] for i in (4, 1, 3, 0, 5, 2)]  # 乱序交付

    reference = make_service()
    feed_in_order(reference, live + buffered[1:])  # m-1..m-8 顺序上报

    svc = make_service()
    feed_in_order(svc, live)  # 在线收到 m-1..m-3
    result = svc.backfill("dev-1", CRED, shuffled)
    for r in result.results:
        log.info("补报 %s -> %s (%s)", r.message_id, r.outcome.value, r.reason)
    assert not result.resync_required

    outcomes = {r.message_id: r.outcome for r in result.results}
    assert outcomes["m-3"] is Outcome.DUPLICATE  # 已算过的不重复计入
    assert all(outcomes[f"m-{i}"] is Outcome.APPLIED for i in range(4, 9))

    ref_shadow = reference._shadows.get_or_create("dev-1")
    shadow = svc._shadows.get_or_create("dev-1")
    ref_stats, stats = reference.stats("dev-1"), svc.stats("dev-1")
    log.info("等价性: reported=%s vs %s; accepted=%d vs %d; sums=%s vs %s",
             shadow.reported, ref_shadow.reported,
             stats.accepted_messages, ref_stats.accepted_messages,
             stats.metric_sums, ref_stats.metric_sums)
    assert shadow.reported == ref_shadow.reported
    assert shadow.last_device_ts == ref_shadow.last_device_ts
    assert stats.accepted_messages == ref_stats.accepted_messages
    assert stats.metric_sums == ref_stats.metric_sums
    assert stats.metric_counts == ref_stats.metric_counts


def test_backfill_older_than_live_triggers_replay_and_matches_in_order() -> None:
    """重连后先来了一条新消息，再补更早的：回放重建后仍与顺序上报等价。"""
    all_msgs = [msg(f"m-{i}", float(i), state={f"f{i}": i, "seq": i},
                    metrics={"v": 1.0})
                for i in range(1, 8)]

    reference = make_service()
    feed_in_order(reference, all_msgs)

    svc = make_service()
    feed_in_order(svc, all_msgs[:3])          # 在线 m-1..m-3
    svc.ingest("dev-1", CRED, all_msgs[6])    # 重连后先实时到了 m-7
    # 断线期间攒的 m-4..m-6 时间上都早于已见最大，走回放路径
    result = svc.backfill("dev-1", CRED,
                          [all_msgs[5], all_msgs[3], all_msgs[4]])
    assert not result.resync_required
    assert all(r.outcome is Outcome.APPLIED for r in result.results)

    ref_shadow = reference._shadows.get_or_create("dev-1")
    shadow = svc._shadows.get_or_create("dev-1")
    ref_stats, stats = reference.stats("dev-1"), svc.stats("dev-1")
    log.info("回放重建: reported=%s vs %s", shadow.reported, ref_shadow.reported)
    assert shadow.reported == ref_shadow.reported  # 含 m-4..m-6 的字段
    assert stats.accepted_messages == ref_stats.accepted_messages
    assert stats.metric_sums == ref_stats.metric_sums


def test_duplicate_and_stale_are_counted_independently() -> None:
    """重复投递与乱序旧数据是两类结论，各有独立计数与断言。"""
    svc = make_service()
    svc.ingest("dev-1", CRED, msg("m-1", 1.0, metrics={"v": 1.0}))
    svc.ingest("dev-1", CRED, msg("m-2", 2.0, metrics={"v": 1.0}))

    dup = svc.ingest("dev-1", CRED, msg("m-1", 1.0, metrics={"v": 1.0}))
    assert dup.outcome is Outcome.DUPLICATE  # 完全重复投递

    stale = svc.ingest("dev-1", CRED, msg("m-0", 0.5, metrics={"v": 1.0}))
    assert stale.outcome is Outcome.STALE    # 未见过但时间戳过旧

    stats = svc.stats("dev-1")
    log.info("accepted=%d duplicate=%d stale=%d",
             stats.accepted_messages, stats.duplicate_messages,
             stats.stale_messages)
    # 两类结论分别断言，不拿一个总数同时说明两件事
    assert stats.duplicate_messages == 1
    assert stats.stale_messages == 1
    assert stats.accepted_messages == 2
    assert stats.metric_sums == {"v": 2.0}  # 两类都没有计入指标


def test_gap_when_position_already_evicted() -> None:
    """回来得太晚：要接的位置已被回收，明确报 gap，不悄悄当正常结果。"""
    svc = make_service(max_messages=3)
    feed_in_order(svc, [msg(f"m-{i}", float(i)) for i in range(1, 6)])
    info = svc.resume("dev-1", CRED)
    log.info("窗口上限=3，当前窗口起点=%s", info.earliest_retained_ts)
    assert info.earliest_retained_ts == 3.0  # m-1、m-2 已被回收

    result = svc.backfill("dev-1", CRED, [msg("m-1", 1.0), msg("m-2", 2.0)])
    for r in result.results:
        log.info("过期补报 %s -> %s (%s)", r.message_id, r.outcome.value, r.reason)
    assert result.resync_required
    assert all(r.outcome is Outcome.GAP for r in result.results)
    stats = svc.stats("dev-1")
    # gap 消息不计入任何统计、不推进状态
    assert stats.accepted_messages == 5
    assert stats.duplicate_messages == 0
    assert svc._shadows.get_or_create("dev-1").reported_version == 5


def test_backfill_respects_quota_atomically() -> None:
    """补报中途打满配额：已生效的保持生效，剩余的明确拒绝。"""
    svc = make_service(quota=3)
    from iot_twin.errors import RejectError, RejectReason
    svc.ingest("dev-1", CRED, msg("m-1", 1.0))
    with pytest.raises(RejectError) as exc_info:
        svc.backfill("dev-1", CRED,
                     [msg(f"m-{i}", float(i)) for i in range(2, 6)])
    log.info("配额=3，补报 4 条 -> %s", exc_info.value.reason.value)
    assert exc_info.value.reason is RejectReason.QUOTA_EXCEEDED
    stats = svc.stats("dev-1")
    assert stats.accepted_messages == 3  # 恰好打满，无半截写入


def test_scenario_is_repeatable_in_same_process() -> None:
    """同一场景在同一进程里连跑几轮，结果逐轮一致、互不影响。"""
    snapshots = []
    for round_no in range(3):
        svc = make_service()
        feed_in_order(svc, [msg(f"m-{i}", float(i), state={"seq": i},
                                  metrics={"v": 1.0}) for i in range(1, 4)])
        svc.backfill("dev-1", CRED,
                     [msg(f"m-{i}", float(i), state={"seq": i},
                          metrics={"v": 1.0}) for i in (5, 4, 6)])
        shadow = svc._shadows.get_or_create("dev-1")
        stats = svc.stats("dev-1")
        snapshot = (dict(shadow.reported), stats.accepted_messages,
                    dict(stats.metric_sums))
        log.info("第 %d 轮: reported=%s accepted=%d",
                 round_no + 1, snapshot[0], snapshot[1])
        snapshots.append(snapshot)
    assert snapshots[0] == snapshots[1] == snapshots[2]


def test_backfill_before_window_start_is_safe_when_nothing_evicted() -> None:
    """从未发生回收时，早于窗口起点的补报可安全回放，不误判为缺口。"""
    svc = make_service()
    # 重连后实时流先到了 10..15，窗口起点是 10
    feed_in_order(svc, [msg(f"m-{i}", float(i), state={"seq": i},
                              metrics={"v": 1.0}) for i in range(10, 16)])
    info = svc.resume("dev-1", CRED)
    log.info("窗口起点=%s 回放水位=%s", info.earliest_retained_ts,
             info.replay_horizon)
    assert info.replay_horizon is None  # 从未回收，任意早的消息都能补

    result = svc.backfill("dev-1", CRED,
                          [msg(f"m-{i}", float(i), state={"seq": i},
                               metrics={"v": 1.0}) for i in (7, 9, 8)])
    assert not result.resync_required
    assert all(r.outcome is Outcome.APPLIED for r in result.results)

    reference = make_service()
    feed_in_order(reference, [msg(f"m-{i}", float(i), state={"seq": i},
                                    metrics={"v": 1.0}) for i in range(7, 16)])
    shadow = svc._shadows.get_or_create("dev-1")
    ref_shadow = reference._shadows.get_or_create("dev-1")
    log.info("回放后 reported=%s vs 顺序参照 %s",
             shadow.reported, ref_shadow.reported)
    assert shadow.reported == ref_shadow.reported
    assert svc.stats("dev-1").accepted_messages == \
        reference.stats("dev-1").accepted_messages
