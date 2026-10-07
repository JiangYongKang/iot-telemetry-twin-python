"""遥测接入：认证、幂等去重、乱序判定、原子应用、断线续传与补报回放。"""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from enum import Enum

from .history import HistoryConfig, HistoryStore
from .models import TelemetryMessage, TelemetryStats
from .registry import DeviceRegistry
from .shadow import ShadowStore

log = logging.getLogger("iot_twin.ingest")


class Outcome(str, Enum):
    APPLIED = "applied"        # 新消息，已生效
    DUPLICATE = "duplicate"    # 重复投递，幂等忽略
    STALE = "stale"            # 乱序旧数据，不覆盖新状态
    GAP = "gap"                # 落在保留窗口之外，无法判定，需要全量对齐


@dataclass(frozen=True)
class IngestResult:
    outcome: Outcome
    device_id: str
    message_id: str
    reason: str  # 可解释的判定依据


@dataclass(frozen=True)
class ResumeInfo:
    """设备重连时的续传位置。

    - last_device_ts: 服务已应用的最大设备侧时间戳——“你上次报到这儿”。
    - replay_horizon: 回放水位（已被回收消息的最大 device_ts）。设备缓冲
      里 device_ts 大于它的消息都可以安全补报；小于等于它的消息服务已
      无法核对，补报会被判为 gap。从未发生回收时为 None，表示任意早的
      消息都能补。
    - earliest_retained_ts: 回放历史窗口内现存最早的时间戳（空窗口 None）。
    - retained_count: 窗口内现存条数。
    """

    device_id: str
    last_device_ts: float
    replay_horizon: float | None
    earliest_retained_ts: float | None
    retained_count: int


@dataclass(frozen=True)
class BackfillResult:
    """一批补报消息的处理结果。

    results 按设备侧时间戳升序排列；resync_required 为 True 表示有消息
    落在保留窗口之外（outcome=gap），这段接不上，需要全量对齐。
    """

    device_id: str
    results: tuple[IngestResult, ...]
    resync_required: bool


class IngestService:
    """同一设备的写入在设备级锁内原子完成：计数与状态更新同生同灭。

    去重与补报核对依赖有界回放历史（HistoryStore）：内存占用随保留
    上限收敛，不随运行时长与上报量增长。
    """

    def __init__(
        self,
        registry: DeviceRegistry,
        shadows: ShadowStore,
        histories: HistoryStore | None = None,
    ) -> None:
        self._registry = registry
        self._shadows = shadows
        self._histories = histories or HistoryStore(HistoryConfig())
        self._stats: dict[str, TelemetryStats] = {}
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
            stats = self._stats.setdefault(device_id, TelemetryStats())
            shadow = self._shadows.get_or_create(device_id)
            history = self._histories.get_or_create(device_id)

            # 1) 幂等去重：message_id 仍在保留窗口内即视为重复投递。
            if history.contains(msg.message_id):
                stats.duplicate_messages += 1
                result = IngestResult(
                    Outcome.DUPLICATE, device_id, msg.message_id,
                    f"message_id={msg.message_id!r} 在保留窗口内已处理过，幂等忽略",
                )
                log.info("判定 %s：%s", result.outcome.value, result.reason)
                return result

            # 2) 新旧判定：依据设备侧时间戳，旧数据不得盖回新状态。
            if msg.device_ts <= shadow.last_device_ts:
                stats.stale_messages += 1
                result = IngestResult(
                    Outcome.STALE, device_id, msg.message_id,
                    f"device_ts={msg.device_ts} <= 已见最大 "
                    f"device_ts={shadow.last_device_ts}，判定为旧数据",
                )
                log.info("判定 %s：%s", result.outcome.value, result.reason)
                return result

            # 3) 配额：超额拒绝，此前无任何写入，整体不生效。
            self._registry.consume_quota(device_id)

            # 4) 原子生效：影子状态、统计、回放历史在同一临界区内一起提交。
            applied = self._shadows.apply_reported(
                device_id, msg.state, msg.device_ts
            )
            assert applied, "锁内时间戳判定与影子应用必须一致"
            self._accept(stats, msg)
            history.append(msg)
            result = IngestResult(
                Outcome.APPLIED, device_id, msg.message_id,
                f"device_ts={msg.device_ts} 为最新，状态与统计已原子提交",
            )
            log.info("判定 %s：%s", result.outcome.value, result.reason)
            return result

    @staticmethod
    def _accept(stats: TelemetryStats, msg: TelemetryMessage) -> None:
        stats.accepted_messages += 1
        for k, v in msg.metrics.items():
            stats.metric_sums[k] = stats.metric_sums.get(k, 0.0) + v
            stats.metric_counts[k] = stats.metric_counts.get(k, 0) + 1

    def resume(self, device_id: str, credential: str) -> ResumeInfo:
        """设备重连后查询续传位置：“我上次报到哪儿了”。"""
        self._registry.authenticate(device_id, credential)
        lock = self._device_lock(device_id)
        with lock:
            shadow = self._shadows.get_or_create(device_id)
            history = self._histories.get_or_create(device_id)
            info = ResumeInfo(
                device_id=device_id,
                last_device_ts=shadow.last_device_ts,
                replay_horizon=history.evicted_up_to,
                earliest_retained_ts=history.earliest_ts,
                retained_count=len(history),
            )
            log.info(
                "续传位置查询 device=%s last_device_ts=%s 回放水位=%s "
                "窗口起点=%s 窗口内 %d 条",
                device_id, info.last_device_ts, info.replay_horizon,
                info.earliest_retained_ts, info.retained_count,
            )
            return info

    def backfill(
        self, device_id: str, credential: str, messages: list[TelemetryMessage]
    ) -> BackfillResult:
        """补报断线期间攒下的一批消息。

        服务按设备侧时间戳理顺后逐条判定：
        - 保留窗口内已处理过的 id、以及批次内重复投递 → duplicate，不重复计入；
        - device_ts 新于已见最大 → 正常应用；
        - 落在窗口内部但从未见过 → 插入历史并回放重建影子，与从一开始就
          按顺序上报得到的状态与统计一致；
        - device_ts 不超过回放水位（该位置的历史已被回收，无法与“已应用
          后被回收”区分）→ gap，标记 resync_required，不悄悄当作正常结果。

        每条消息各自原子生效；若中途配额耗尽，抛出 QUOTA_EXCEEDED，
        已生效的保持生效，未处理的不处理。
        """
        self._registry.authenticate(device_id, credential)

        lock = self._device_lock(device_id)
        with lock:
            stats = self._stats.setdefault(device_id, TelemetryStats())
            shadow = self._shadows.get_or_create(device_id)
            history = self._histories.get_or_create(device_id)

            results: list[IngestResult] = []
            resync_required = False
            needs_rebuild = False

            # 按设备侧时间戳理顺；同一 device_ts 保持投递先后（稳定排序）。
            ordered = sorted(messages, key=lambda m: m.device_ts)
            seen_in_batch: set[str] = set()

            for msg in ordered:
                # 批次内完全重复投递：只生效一次。
                if msg.message_id in seen_in_batch:
                    stats.duplicate_messages += 1
                    results.append(IngestResult(
                        Outcome.DUPLICATE, device_id, msg.message_id,
                        f"message_id={msg.message_id!r} 在本批次内重复投递，幂等忽略",
                    ))
                    continue
                seen_in_batch.add(msg.message_id)

                # 保留窗口内已处理过：不重复计入、不重复推进状态。
                if history.contains(msg.message_id):
                    stats.duplicate_messages += 1
                    results.append(IngestResult(
                        Outcome.DUPLICATE, device_id, msg.message_id,
                        f"message_id={msg.message_id!r} 在保留窗口内已处理过，幂等忽略",
                    ))
                    continue

                if msg.device_ts > shadow.last_device_ts:
                    # 新于已见最大时间戳：与单条上报同语义，整条原子生效。
                    self._registry.consume_quota(device_id)
                    applied = self._shadows.apply_reported(
                        device_id, msg.state, msg.device_ts
                    )
                    assert applied, "锁内时间戳判定与影子应用必须一致"
                    self._accept(stats, msg)
                    history.append(msg)
                    results.append(IngestResult(
                        Outcome.APPLIED, device_id, msg.message_id,
                        f"device_ts={msg.device_ts} 为最新，状态与统计已原子提交",
                    ))
                    continue

                horizon = history.evicted_up_to
                if horizon is not None and msg.device_ts <= horizon:
                    # 该位置的历史已被回收，无法与“已应用后被回收”区分：
                    # 明确告知接不上，不悄悄给缺数据的正常结果。
                    resync_required = True
                    results.append(IngestResult(
                        Outcome.GAP, device_id, msg.message_id,
                        f"device_ts={msg.device_ts} <= 回放水位 "
                        f"replay_horizon={horizon}，该段历史已回收，"
                        f"无法核对，需要全量对齐",
                    ))
                    continue

                # 窗口内查无此 id 且越过回放水位：一定从未被应用过，
                # 插入历史，随后统一回放重建。
                self._registry.consume_quota(device_id)
                self._accept(stats, msg)
                history.insert(msg)
                needs_rebuild = True
                results.append(IngestResult(
                    Outcome.APPLIED, device_id, msg.message_id,
                    f"device_ts={msg.device_ts} 越过回放水位 "
                    f"replay_horizon={horizon} 且窗口内查无此 id，"
                    f"判定为未处理过的历史消息，已插入历史并参与回放重建",
                ))

            if needs_rebuild:
                # 回放重建：基态 + 窗口按时间序折叠，与从头按顺序上报等价。
                folded = history.folded_state()
                self._shadows.rebuild_reported(
                    device_id, folded, shadow.last_device_ts
                )
                log.info(
                    "补报触发回放重建 device=%s 窗口=%d 条 folded=%s",
                    device_id, len(history), folded,
                )

            for r in results:
                log.info("补报判定 %s：%s", r.outcome.value, r.reason)
            return BackfillResult(
                device_id=device_id,
                results=tuple(results),
                resync_required=resync_required,
            )

    def stats(self, device_id: str) -> TelemetryStats:
        lock = self._device_lock(device_id)
        with lock:
            return self._stats.setdefault(device_id, TelemetryStats())
