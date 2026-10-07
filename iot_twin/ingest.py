"""遥测接入：认证、幂等去重、乱序判定、原子应用，以及重连续传/批量回放。"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from enum import Enum

from .errors import RejectError, RejectReason
from .history import HistoryEntry, HistoryStore, ResumeCursor, RetentionConfig
from .models import TelemetryMessage, TelemetryStats
from .registry import DeviceRegistry
from .shadow import ShadowStore


class Outcome(str, Enum):
    APPLIED = "applied"                # 新消息，已生效
    DUPLICATE = "duplicate"            # 重复投递，幂等忽略
    STALE = "stale"                    # 乱序旧数据，不覆盖新状态
    QUOTA_EXCEEDED = "quota_exceeded"  # 批次中配额耗尽，未处理


@dataclass(frozen=True)
class IngestResult:
    outcome: Outcome
    device_id: str
    message_id: str
    reason: str  # 可解释的判定依据


@dataclass(frozen=True)
class BatchItemResult:
    """批量补报中每一条消息各自的判定（结论一一对应，不共用一个计数）。"""

    message_id: str
    device_ts: float
    outcome: Outcome
    reason: str


@dataclass(frozen=True)
class BackfillResult:
    """一次批量补报的整体结果。"""

    device_id: str
    after_device_ts: float | None
    items: tuple[BatchItemResult, ...]
    applied: int       # 本次新生效条数
    duplicates: int    # 已算过/完全重复的条数
    quota_rejected: int  # 因配额耗尽未处理条数

    @property
    def total(self) -> int:
        return len(self.items)


class IngestService:
    """同一设备的写入在设备级锁内原子完成：计数与状态更新同生同灭。

    有界回放历史（HistoryStore）是幂等判定与状态重放的事实源：
    - 单条上报沿用"旧时间戳不覆盖新状态"；
    - 批量补报先校验续传位置未超出保留范围，再按设备侧时间排序插入，
      重复/已算过的条目跳过，最后把窗口折叠结果一次性落地到影子，
      结果与所有消息从一开始就按序逐条上报完全一致。
    """

    def __init__(
        self,
        registry: DeviceRegistry,
        shadows: ShadowStore,
        history: HistoryStore | None = None,
        retention: RetentionConfig | None = None,
    ) -> None:
        self._registry = registry
        self._shadows = shadows
        self._history = history or HistoryStore(retention)
        self._stats: dict[str, TelemetryStats] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    @property
    def history(self) -> HistoryStore:
        return self._history

    def _device_lock(self, device_id: str) -> threading.Lock:
        with self._locks_guard:
            return self._locks.setdefault(device_id, threading.Lock())

    def _refresh_metrics(self, device_id: str, stats: TelemetryStats) -> None:
        """指标和/计数以回放窗口（含压缩基）为准，任何插入顺序下都一致。"""
        sums, counts = self._history.metric_totals(device_id)
        stats.metric_sums = sums
        stats.metric_counts = counts

    def ingest(
        self, device_id: str, credential: str, msg: TelemetryMessage
    ) -> IngestResult:
        # 认证在锁外完成：失败抛 RejectError，不产生任何副作用。
        self._registry.authenticate(device_id, credential)

        lock = self._device_lock(device_id)
        with lock:
            stats = self._stats.setdefault(device_id, TelemetryStats())

            # 1) 幂等去重：同一 message_id 只生效一次（集合即有界窗口）。
            if self._history.is_known(device_id, msg.message_id):
                stats.duplicate_messages += 1
                return IngestResult(
                    Outcome.DUPLICATE, device_id, msg.message_id,
                    f"message_id={msg.message_id!r} 在保留窗口内已处理过，"
                    "幂等忽略",
                )

            # 2) 新旧判定：依据设备侧时间戳，旧数据不得盖回新状态。
            last_ts = self._history.last_device_ts(device_id)
            if msg.device_ts <= last_ts:
                stats.stale_messages += 1
                return IngestResult(
                    Outcome.STALE, device_id, msg.message_id,
                    f"device_ts={msg.device_ts} <= 已见最大 "
                    f"device_ts={last_ts}，判定为乱序旧数据",
                )

            # 3) 配额：超额拒绝，此前无任何写入，整体不生效。
            self._registry.consume_quota(device_id)

            # 4) 原子生效：进有界窗口、影子落地、统计提交，同一临界区完成。
            self._history.insert(
                device_id,
                HistoryEntry(msg.message_id, msg.device_ts,
                             dict(msg.state), dict(msg.metrics)),
            )
            self._materialize_shadow(device_id, version_bump=1)
            stats.accepted_messages += 1
            self._refresh_metrics(device_id, stats)
            return IngestResult(
                Outcome.APPLIED, device_id, msg.message_id,
                f"device_ts={msg.device_ts} 为最新，状态与统计已原子提交",
            )

    def _materialize_shadow(self, device_id: str, version_bump: int) -> None:
        """把回放窗口折叠出的最终状态一次性落地为影子 reported。"""
        final_state = self._history.reported_state(device_id)
        last_ts = self._history.last_device_ts(device_id)
        self._shadows.reset_reported(device_id, final_state, last_ts)
        if version_bump:
            self._shadows.bump_reported_version(device_id, version_bump)

    def resume_cursor(self, device_id: str, credential: str) -> ResumeCursor:
        """设备重连先问"我上次报到哪儿了"。认证后返回续传游标。"""
        self._registry.authenticate(device_id, credential)
        lock = self._device_lock(device_id)
        with lock:
            return self._history.cursor(device_id)

    def backfill(
        self,
        device_id: str,
        credential: str,
        after_device_ts: float | None,
        messages: list[TelemetryMessage],
    ) -> BackfillResult:
        """断线期间缓存的一批消息一次性补报。

        步骤（同一设备临界区内）：
        1) 先校验续传位置仍在保留范围内——已被回收则整体拒绝
           （RESYNC_REQUIRED），不做任何部分写入；
        2) 按设备侧 (device_ts, message_id) 排序，等价于按序逐条上报；
        3) 已算过/完全重复投递的条目跳过，不重复计数、不重复推进；
        4) 新条目依次插入有界窗口并各自占用一个配额；
        5) 窗口折叠结果一次性落地影子，指标以窗口全量重算。
        """
        self._registry.authenticate(device_id, credential)

        ordered = sorted(messages, key=lambda m: (m.device_ts, m.message_id))
        lock = self._device_lock(device_id)
        with lock:
            stats = self._stats.setdefault(device_id, TelemetryStats())

            # 续传位置已被回收：明确要求全量对齐，拒绝残缺回放。
            self._history.can_resume_from(device_id, after_device_ts)

            items: list[BatchItemResult] = []
            applied = duplicates = quota_rejected = 0
            quota_open = True
            for m in ordered:
                if self._history.is_known(device_id, m.message_id):
                    duplicates += 1
                    stats.duplicate_messages += 1
                    items.append(BatchItemResult(
                        m.message_id, m.device_ts, Outcome.DUPLICATE,
                        f"message_id={m.message_id!r} 在保留窗口内已处理过，"
                        "补报中跳过",
                    ))
                    continue
                try:
                    self._registry.consume_quota(device_id)
                except RejectError as e:
                    if e.reason is not RejectReason.QUOTA_EXCEEDED:
                        raise
                    # 配额墙在此条撞上：本条及其后所有未处理条目确定性
                    # 记为 quota_exceeded，不产生任何写入（与调度顺序无关）。
                    quota_open = False
                if not quota_open:
                    quota_rejected += 1
                    items.append(BatchItemResult(
                        m.message_id, m.device_ts, Outcome.QUOTA_EXCEEDED,
                        "设备上报配额已耗尽，本条未处理",
                    ))
                    continue
                self._history.insert(
                    device_id,
                    HistoryEntry(m.message_id, m.device_ts,
                                 dict(m.state), dict(m.metrics)),
                )
                applied += 1
                stats.accepted_messages += 1
                items.append(BatchItemResult(
                    m.message_id, m.device_ts, Outcome.APPLIED,
                    f"device_ts={m.device_ts} 按序补报，已插入回放窗口",
                ))

            if applied:
                # 只按实际生效的新消息推进版本；重复/拒绝不推进。
                self._materialize_shadow(device_id, version_bump=applied)
                self._refresh_metrics(device_id, stats)
            return BackfillResult(
                device_id=device_id,
                after_device_ts=after_device_ts,
                items=tuple(items),
                applied=applied,
                duplicates=duplicates,
                quota_rejected=quota_rejected,
            )

    def stats(self, device_id: str) -> TelemetryStats:
        lock = self._device_lock(device_id)
        with lock:
            stats = self._stats.setdefault(device_id, TelemetryStats())
            self._refresh_metrics(device_id, stats)
            return stats
