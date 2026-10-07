"""有界回放历史：支撑断线重连后的补报去重、乱序理顺与缺口判定。

每台设备保留一段按设备侧时间戳排序的已应用消息窗口，
窗口大小与保留时长均可配置、有上限，内存占用随上限收敛。
"""
from __future__ import annotations

import bisect
import logging
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any

from .models import TelemetryMessage, now

log = logging.getLogger("iot_twin.history")


@dataclass(frozen=True)
class HistoryConfig:
    """回放历史的保留上限。

    - max_messages: 每台设备最多保留的消息条数（超出后从最旧的开始回收）。
    - max_age_seconds: 每条消息最多保留多久（按写入时的服务端时钟计龄）。
    """

    max_messages: int = 1000
    max_age_seconds: float = 3600.0


@dataclass(frozen=True)
class HistoryEntry:
    """一条已应用消息在回放历史中的留存。"""

    message_id: str
    device_ts: float
    state: dict[str, Any]
    metrics: dict[str, float]
    stored_at: float  # 服务端写入时刻，用于按龄回收


class DeviceHistory:
    """单台设备的有界有序回放窗口。

    - 窗口内条目按 device_ts 升序排列；
    - 每次写入后执行回收：先按龄淘汰，再按条数上限从最旧端淘汰；
    - 内存占用随 (max_messages, max_age_seconds) 收敛，不随运行时长增长。
    """

    def __init__(self, config: HistoryConfig, clock: Callable[[], float] = now) -> None:
        self._config = config
        self._clock = clock
        self._entries: list[HistoryEntry] = []  # 按 device_ts 升序
        self._ids: set[str] = set()
        # 已回收条目的状态折叠值：回放重建时作为窗口之前的基态，
        # 保证“窗口折叠结果”与“从头按顺序应用全部消息”一致。
        self._base_state: dict[str, Any] = {}
        # 回收水位：已被回收条目的最大 device_ts（None 表示从未回收）。
        # 回收总是切掉时间序前缀，因此“已 id 不在窗口内”且
        # “device_ts > evicted_up_to”的消息一定从未被应用过，可安全补入；
        # 反之无法与“已应用但被回收”区分，必须按缺口处理。
        self._evicted_up_to: float | None = None

    def _to_entry(self, msg: TelemetryMessage) -> HistoryEntry:
        return HistoryEntry(
            message_id=msg.message_id,
            device_ts=msg.device_ts,
            state=dict(msg.state),
            metrics=dict(msg.metrics),
            stored_at=self._clock(),
        )

    def _evict(self) -> None:
        """按龄 + 按条数回收，始终保持窗口有界。"""
        cutoff = self._clock() - self._config.max_age_seconds
        keep_from = 0
        while keep_from < len(self._entries) and (
            self._entries[keep_from].stored_at < cutoff
        ):
            keep_from += 1
        overflow = len(self._entries) - keep_from - self._config.max_messages
        if overflow > 0:
            keep_from += overflow
        if keep_from > 0:
            evicted = self._entries[:keep_from]
            self._entries = self._entries[keep_from:]
            for e in evicted:
                self._ids.discard(e.message_id)
                self._base_state.update(e.state)  # 按回收先后（即时间序）折叠进基态
            max_evicted = max(e.device_ts for e in evicted)
            if self._evicted_up_to is None or max_evicted > self._evicted_up_to:
                self._evicted_up_to = max_evicted
            log.info(
                "回收 %d 条历史（窗口剩 %d 条，上限 %d 条 / %.0f 秒）",
                len(evicted), len(self._entries),
                self._config.max_messages, self._config.max_age_seconds,
            )

    def append(self, msg: TelemetryMessage) -> None:
        """追加一条新消息（device_ts 不早于窗口末尾），并执行回收。"""
        entry = self._to_entry(msg)
        if self._entries and entry.device_ts < self._entries[-1].device_ts:
            # 调用方保证 append 只用于窗口末尾之后；否则走 insert。
            self.insert(msg)
            return
        self._entries.append(entry)
        self._ids.add(entry.message_id)
        self._evict()

    def insert(self, msg: TelemetryMessage) -> None:
        """按 device_ts 有序插入一条落在窗口内部的历史消息，并执行回收。"""
        entry = self._to_entry(msg)
        keys = [e.device_ts for e in self._entries]
        pos = bisect.bisect_right(keys, entry.device_ts)
        self._entries.insert(pos, entry)
        self._ids.add(entry.message_id)
        log.info(
            "历史消息 message_id=%r device_ts=%s 插入窗口位置 %d/%d",
            entry.message_id, entry.device_ts, pos, len(self._entries),
        )
        self._evict()

    def contains(self, message_id: str) -> bool:
        """message_id 是否仍在保留窗口内。"""
        return message_id in self._ids

    def folded_state(self) -> dict[str, Any]:
        """把基态与窗口内条目按时间序折叠成一份完整上报状态。"""
        state = dict(self._base_state)
        for e in self._entries:
            state.update(e.state)
        return state

    @property
    def earliest_ts(self) -> float | None:
        """窗口内最早的 device_ts；空窗口返回 None。"""
        return self._entries[0].device_ts if self._entries else None

    @property
    def latest_ts(self) -> float | None:
        """窗口内最晚的 device_ts；空窗口返回 None。"""
        return self._entries[-1].device_ts if self._entries else None

    @property
    def evicted_up_to(self) -> float | None:
        """回收水位：已回收条目的最大 device_ts；从未回收为 None。"""
        return self._evicted_up_to

    def __len__(self) -> int:
        return len(self._entries)

    def __iter__(self) -> Iterator[HistoryEntry]:
        return iter(list(self._entries))


class HistoryStore:
    """按设备管理回放窗口，线程安全。

    写路径的并发一致性由 IngestService 的设备级锁保证；
    这里的锁只保护字典本身的存取。
    """

    def __init__(self, config: HistoryConfig, clock: Callable[[], float] = now) -> None:
        self._config = config
        self._clock = clock
        self._lock = threading.Lock()
        self._histories: dict[str, DeviceHistory] = {}

    def get_or_create(self, device_id: str) -> DeviceHistory:
        with self._lock:
            history = self._histories.get(device_id)
            if history is None:
                history = DeviceHistory(self._config, self._clock)
                self._histories[device_id] = history
            return history
