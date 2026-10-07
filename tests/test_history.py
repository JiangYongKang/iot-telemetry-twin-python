"""回放历史窗口：有序性、条数上限、按龄回收、内存有界。

日志打印每次回收/插入的输入与依据，便于复核。
"""
from __future__ import annotations

import logging

from iot_twin.history import DeviceHistory, HistoryConfig, HistoryStore
from iot_twin.models import TelemetryMessage

log = logging.getLogger("test_history")


def msg(mid: str, ts: float) -> TelemetryMessage:
    return TelemetryMessage("dev-1", mid, ts, state={"seq": ts}, metrics={"v": 1.0})


def test_append_keeps_ts_order_and_bounds() -> None:
    h = DeviceHistory(HistoryConfig(max_messages=5, max_age_seconds=1000.0))
    for i in range(1, 6):
        h.append(msg(f"m-{i}", float(i)))
    assert [e.message_id for e in h] == [f"m-{i}" for i in range(1, 6)]
    assert h.earliest_ts == 1.0 and h.latest_ts == 5.0

    h.append(msg("m-6", 6.0))  # 触发条数回收：最旧的 m-1 被淘汰
    log.info("条数上限=5，追加 m-6 后窗口=%s", [e.message_id for e in h])
    assert len(h) == 5
    assert not h.contains("m-1")  # 已回收的 id 不再可去重
    assert h.contains("m-6")
    assert h.earliest_ts == 2.0


def test_insert_places_message_in_ts_order() -> None:
    h = DeviceHistory(HistoryConfig(max_messages=10, max_age_seconds=1000.0))
    for i in (1, 2, 4, 5):
        h.append(msg(f"m-{i}", float(i)))
    h.insert(msg("m-3", 3.0))  # 落在窗口内部
    log.info("插入 device_ts=3.0 后窗口=%s",
             [(e.message_id, e.device_ts) for e in h])
    assert [e.device_ts for e in h] == [1.0, 2.0, 3.0, 4.0, 5.0]
    assert h.contains("m-3")


def test_age_based_eviction_uses_injected_clock() -> None:
    clock = {"t": 1000.0}
    h = DeviceHistory(
        HistoryConfig(max_messages=100, max_age_seconds=60.0),
        clock=lambda: clock["t"],
    )
    h.append(msg("old-1", 1.0))
    clock["t"] += 30.0
    h.append(msg("mid-1", 2.0))
    clock["t"] += 31.0  # old-1 已写入 61 秒，超过 60 秒上限
    h.append(msg("new-1", 3.0))  # 触发回收
    log.info("按龄回收后窗口=%s（上限 60 秒）", [e.message_id for e in h])
    assert not h.contains("old-1")
    assert h.contains("mid-1") and h.contains("new-1")
    assert h.earliest_ts == 2.0


def test_memory_stays_bounded_under_sustained_ingest() -> None:
    cap = 50
    h = DeviceHistory(HistoryConfig(max_messages=cap, max_age_seconds=10**9))
    for i in range(10_000):  # 长时间运行、持续写入
        h.append(msg(f"m-{i}", float(i)))
    log.info("连续写入 10000 条后窗口大小=%d（上限 %d）", len(h), cap)
    assert len(h) == cap  # 内存随上限收敛，不随写入量增长
    assert h.earliest_ts == float(10_000 - cap)


def test_store_isolates_devices() -> None:
    store = HistoryStore(HistoryConfig(max_messages=2, max_age_seconds=1000.0))
    a = store.get_or_create("dev-a")
    b = store.get_or_create("dev-b")
    assert a is store.get_or_create("dev-a")  # 同设备同一窗口
    assert a is not b
    a.append(msg("a-1", 1.0))
    assert len(b) == 0  # 设备之间互不影响
