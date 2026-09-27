"""遥测接入：认证、幂等去重、乱序判定、原子应用。"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from enum import Enum

from .models import TelemetryMessage, TelemetryStats
from .registry import DeviceRegistry
from .shadow import ShadowStore


class Outcome(str, Enum):
    APPLIED = "applied"        # 新消息，已生效
    DUPLICATE = "duplicate"    # 重复投递，幂等忽略
    STALE = "stale"            # 乱序旧数据，不覆盖新状态


@dataclass(frozen=True)
class IngestResult:
    outcome: Outcome
    device_id: str
    message_id: str
    reason: str  # 可解释的判定依据


class IngestService:
    """同一设备的写入在设备级锁内原子完成：计数与状态更新同生同灭。"""

    def __init__(self, registry: DeviceRegistry, shadows: ShadowStore) -> None:
        self._registry = registry
        self._shadows = shadows
        self._stats: dict[str, TelemetryStats] = {}
        self._seen: dict[str, set[str]] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    def _device_lock(self, device_id: str) -> threading.Lock:
        with self._locks_guard:
            return self._locks.setdefault(device_id, threading.Lock())

    def ingest(
        self, device_id: str, credential: str, msg: TelemetryMessage
    ) -> IngestResult:
        # 认证在锁外完成：失败抛 RejectError，不产生任何副作用。
        self._registry.authenticate(device_id, credential)

        lock = self._device_lock(device_id)
        with lock:
            seen = self._seen.setdefault(device_id, set())
            stats = self._stats.setdefault(device_id, TelemetryStats())
            shadow = self._shadows.get_or_create(device_id)

            # 1) 幂等去重：同一 message_id 只生效一次。
            if msg.message_id in seen:
                stats.duplicate_messages += 1
                return IngestResult(
                    Outcome.DUPLICATE, device_id, msg.message_id,
                    f"message_id={msg.message_id!r} 已处理过，幂等忽略",
                )

            # 2) 新旧判定：依据设备侧时间戳，旧数据不得盖回新状态。
            if msg.device_ts <= shadow.last_device_ts:
                stats.stale_messages += 1
                return IngestResult(
                    Outcome.STALE, device_id, msg.message_id,
                    f"device_ts={msg.device_ts} <= 已见最大 "
                    f"device_ts={shadow.last_device_ts}，判定为旧数据",
                )

            # 3) 配额：超额拒绝，此前无任何写入，整体不生效。
            self._registry.consume_quota(device_id)

            # 4) 原子生效：影子状态、统计、去重标记在同一临界区内一起提交。
            applied = self._shadows.apply_reported(
                device_id, msg.state, msg.device_ts
            )
            assert applied, "锁内时间戳判定与影子应用必须一致"
            stats.accepted_messages += 1
            for k, v in msg.metrics.items():
                stats.metric_sums[k] = stats.metric_sums.get(k, 0.0) + v
                stats.metric_counts[k] = stats.metric_counts.get(k, 0) + 1
            seen.add(msg.message_id)
            return IngestResult(
                Outcome.APPLIED, device_id, msg.message_id,
                f"device_ts={msg.device_ts} 为最新，状态与统计已原子提交",
            )

    def stats(self, device_id: str) -> TelemetryStats:
        lock = self._device_lock(device_id)
        with lock:
            return self._stats.setdefault(device_id, TelemetryStats())
