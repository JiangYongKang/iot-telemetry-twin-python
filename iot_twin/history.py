"""重连续传支持：有界回放历史、续传游标、缺口判定。

回放历史按设备维护，保留条数与保留的设备时间跨度均可配置且有上限；
当设备请求接续的位置已被回收时，明确要求全量对齐（resync），
绝不把残缺回放当成正常结果。

本模块只维护"已应用消息"的有界窗口，所有调用均须在
IngestService 的设备级临界区内进行（本模块不自加锁）。

不变量（按 (device_ts, message_id) 升序的有序列表）：
- 幂等集合 = 窗口内消息 id；条目被裁剪的同时其 id 也被遗忘。
- 左裁剪严格按时间顺序进行，因此被裁掉的条目可折叠进"压缩基"：
  base_state 逐字段 update、指标累加，天然等价于把所有历史按序应用。
- 最终 reported = base_state 上再按序叠加窗口内条目；
  指标总和 = base 累加 + 窗口内条目累加。
  因而批量补报（含更早时间戳的丢失消息）重排后，结果与"从一开始
  就按顺序一条条上报"完全一致。
"""
from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass, field
from typing import Any

from .errors import RejectError, RejectReason


@dataclass(frozen=True)
class RetentionConfig:
    """回放历史保留策略（均为有界）。

    - max_messages: 每台设备最多保留多少条已应用消息（正整数）。
    - max_age_seconds: 保留的设备时间跨度上限（秒）：早于
      "最新 device_ts - max_age_seconds" 的条目会被裁剪；
      None 表示只按条数裁剪。
    """

    max_messages: int = 1000
    max_age_seconds: float | None = 3600.0

    def __post_init__(self) -> None:
        if self.max_messages <= 0:
            raise ValueError("max_messages 必须为正整数")
        if self.max_age_seconds is not None and self.max_age_seconds <= 0:
            raise ValueError("max_age_seconds 必须为正数或 None")


@dataclass(frozen=True)
class HistoryEntry:
    """一条已应用消息在回放窗口中的记录。"""

    message_id: str
    device_ts: float
    state: dict[str, Any] = field(default_factory=dict)
    metrics: dict[str, float] = field(default_factory=dict)

    @property
    def order_key(self) -> tuple[float, str]:
        # 相同 device_ts 时以 message_id 打破平局，保证顺序确定、可复现。
        return (self.device_ts, self.message_id)


@dataclass(frozen=True)
class ResumeCursor:
    """设备重连时询问"我上次报到哪儿了"的回答。

    - last_device_ts: 已应用的最大设备侧时间戳（从未上报为 None），
      设备从这里之后接着报。
    - earliest_retained_ts: 当前仍保留的最早一条历史的设备时间戳；
      无历史时为 None。设备本地的续传位置若早于它，就会收到
      resync_required，需要全量对齐。
    - retained_count: 当前窗口内保留的历史条数。
    """

    device_id: str
    last_device_ts: float | None
    earliest_retained_ts: float | None
    retained_count: int


@dataclass
class _Window:
    """单设备的有界窗口（调用方持锁访问）。"""

    entries: list[HistoryEntry] = field(default_factory=list)
    ids: set[str] = field(default_factory=set)
    # 已被左裁剪条目的折叠结果：按裁剪先后（即时间先后）更新/累加。
    base_state: dict[str, Any] = field(default_factory=dict)
    base_sums: dict[str, float] = field(default_factory=dict)
    base_counts: dict[str, int] = field(default_factory=dict)


class HistoryStore:
    """按设备维护有界回放窗口；窗口即幂等集合与重放事实源。"""

    def __init__(self, config: RetentionConfig | None = None) -> None:
        self._config = config or RetentionConfig()
        self._windows: dict[str, _Window] = {}

    # ---- 内部工具 -----------------------------------------------------

    def _window(self, device_id: str) -> _Window:
        return self._windows.setdefault(device_id, _Window())

    def _keys(self, w: _Window) -> list[tuple[float, str]]:
        return [e.order_key for e in w.entries]

    def _prune(self, w: _Window) -> None:
        """左裁剪：条数上限与设备时间跨度上限，折叠进压缩基。"""
        # 1) 时间跨度：保留 device_ts >= 最新 ts - span 的条目。
        if self._config.max_age_seconds is not None and w.entries:
            horizon = w.entries[-1].device_ts - self._config.max_age_seconds
            cut = bisect_left(self._keys(w), (horizon, ""))
            self._fold_evicted(w, cut)
        # 2) 条数：只保留最新的 max_messages 条。
        if len(w.entries) > self._config.max_messages:
            self._fold_evicted(w, len(w.entries) - self._config.max_messages)

    def _fold_evicted(self, w: _Window, cut: int) -> None:
        """把前 cut 条（时间上最早）折叠进压缩基并从窗口移除。"""
        if cut <= 0:
            return
        for e in w.entries[:cut]:
            # 裁剪严格按时间升序：同字段以本条（较新）覆盖压缩基（较旧）。
            w.base_state.update(e.state)
            for k, v in e.metrics.items():
                w.base_sums[k] = w.base_sums.get(k, 0.0) + v
                w.base_counts[k] = w.base_counts.get(k, 0) + 1
            w.ids.remove(e.message_id)
        del w.entries[:cut]

    # ---- 判定与写入（调用方持设备锁）----------------------------------

    def is_known(self, device_id: str, message_id: str) -> bool:
        return message_id in self._window(device_id).ids

    def last_device_ts(self, device_id: str) -> float:
        w = self._window(device_id)
        return w.entries[-1].device_ts if w.entries else float("-inf")

    def insert(self, device_id: str, entry: HistoryEntry) -> None:
        """把一条此前未见过的消息插入窗口有序位置，并执行有界裁剪。"""
        w = self._window(device_id)
        pos = bisect_left(self._keys(w), entry.order_key)
        w.entries.insert(pos, entry)
        w.ids.add(entry.message_id)
        self._prune(w)

    # ---- 重放派生 ------------------------------------------------------

    def reported_state(self, device_id: str) -> dict[str, Any]:
        """压缩基之上按序叠加窗口条目，等价于全部历史按序上报的结果。"""
        w = self._window(device_id)
        state: dict[str, Any] = dict(w.base_state)
        for e in w.entries:
            state.update(e.state)
        return state

    def metric_totals(self, device_id: str) -> tuple[dict[str, float], dict[str, int]]:
        w = self._window(device_id)
        sums = dict(w.base_sums)
        counts = dict(w.base_counts)
        for e in w.entries:
            for k, v in e.metrics.items():
                sums[k] = sums.get(k, 0.0) + v
                counts[k] = counts.get(k, 0) + 1
        return sums, counts

    # ---- 续传 ----------------------------------------------------------

    def can_resume_from(
        self, device_id: str, after_device_ts: float | None
    ) -> None:
        """续传位置仍在保留范围内则正常返回，否则抛 RESYNC_REQUIRED。

        范围之外意味着：该位置之后、现存最早历史之前可能有消息已被
        回收，其 message_id 也随之遗忘——重投时服务无法区分
        "丢失补报"与"已处理过的重复投递"，故只能要求全量对齐。
        """
        w = self._window(device_id)
        if not w.entries:
            return  # 没有任何历史，谈不上缺口
        floor = w.entries[0].device_ts
        if after_device_ts is None or after_device_ts < floor:
            raise self.resync_error(
                device_id,
                f"续传位置 after_device_ts={after_device_ts} 早于仍保留的最早 "
                f"device_ts={floor}，该区间历史已回收，无法保证回放完整，"
                "需要全量对齐",
            )

    def cursor(self, device_id: str) -> ResumeCursor:
        w = self._window(device_id)
        if not w.entries:
            return ResumeCursor(device_id, None, None, 0)
        return ResumeCursor(
            device_id=device_id,
            last_device_ts=w.entries[-1].device_ts,
            earliest_retained_ts=w.entries[0].device_ts,
            retained_count=len(w.entries),
        )

    @staticmethod
    def resync_error(device_id: str, detail: str) -> RejectError:
        return RejectError(RejectReason.RESYNC_REQUIRED, device_id, detail)
