"""遥测服务：接入校验、幂等去重、新旧判定、配额与统计。

固定处理顺序（每一步失败原因都稳定可区分）：
  0) 消息字段合法性（msg_id / device_ts_ms）
  1) 设备是否注册
  2) 接入凭证是否正确
  3) 设备是否被禁用
  4) 进入设备临界区：
     4.1) 消息去重（msg_id 命中即 DUPLICATE，不计数不推进状态）
     4.2) 新旧判定（device_ts_ms 不严格更新即 STALE，不覆盖状态）
     4.3) 配额校验（accepted 条数达到 quota 即 QUOTA_EXCEEDED）
     4.4) 计数、最新状态、设备状态、影子在同一把锁内一起生效

乱序/重复属于“可解释结论”而不是错误，返回 IngestResult；
凭证/禁用/配额等拒绝抛出携带 ErrorCode 的 ServiceError。
"""

from __future__ import annotations

import logging
import threading

from app.errors import ErrorCode, ServiceError
from app.models import (
    Device,
    DeviceState,
    IngestOutcome,
    IngestResult,
    TelemetryMessage,
    TelemetryStats,
)
from app.shadow import ShadowService
from app.store import InMemoryStore

logger = logging.getLogger("iot.ingest")


class TelemetryService:
    def __init__(self, store: InMemoryStore, shadows: ShadowService) -> None:
        self._store = store
        self._shadows = shadows
        # 保护 rejected_count 的聚合自增无需额外锁：拒绝计数也在设备锁内更新。
        self._register_lock = threading.Lock()

    def register_device(
        self,
        device_id: str,
        credential: str,
        *,
        enabled: bool = True,
        quota: int = 0,
    ) -> Device:
        if not device_id:
            raise ServiceError(
                ErrorCode.INVALID_MESSAGE, "device_id 不能为空", status_code=400
            )
        if not credential:
            raise ServiceError(
                ErrorCode.INVALID_MESSAGE, "credential 不能为空", status_code=400
            )
        if quota < 0:
            raise ServiceError(
                ErrorCode.INVALID_MESSAGE, "quota 不能为负数", status_code=400
            )
        device = Device(
            device_id=device_id, credential=credential, enabled=enabled, quota=quota
        )
        with self._register_lock:
            return self._store.register_device(device)

    # -- 拒绝：原因可区分 -------------------------------------------------

    def _reject(
        self,
        rt,
        code: ErrorCode,
        message: str,
        *,
        status_code: int,
        device_id: str,
        msg_id: str | None,
        device_ts_ms: int | None,
    ) -> None:
        if rt is not None:
            with rt.lock:
                rt.rejected_count += 1
        logger.warning(
            "拒绝上报 | 输入 device_id=%s msg_id=%s device_ts_ms=%s | 判定 code=%s 依据=%s",
            device_id, msg_id, device_ts_ms, code.value, message,
        )
        raise ServiceError(code, message, status_code=status_code)

    # -- 主入口 -----------------------------------------------------------

    def ingest(self, msg: TelemetryMessage) -> IngestResult:
        # 0) 基本合法性：先于一切，避免脏输入污染判定。
        if not msg.msg_id:
            raise ServiceError(
                ErrorCode.INVALID_MESSAGE, "msg_id 不能为空", status_code=400
            )
        if not isinstance(msg.device_ts_ms, int) or isinstance(msg.device_ts_ms, bool):
            raise ServiceError(
                ErrorCode.INVALID_MESSAGE,
                "device_ts_ms 必须是整数毫秒时间戳",
                status_code=400,
            )
        if msg.device_ts_ms < 0:
            raise ServiceError(
                ErrorCode.INVALID_MESSAGE,
                "device_ts_ms 不能为负数",
                status_code=400,
            )

        rt = self._store.get(msg.device_id)

        # 1) 设备不存在
        if rt is None or rt.registered is None:
            self._reject(
                None,
                ErrorCode.DEVICE_NOT_FOUND,
                f"设备未注册: {msg.device_id}",
                status_code=404,
                device_id=msg.device_id,
                msg_id=msg.msg_id,
                device_ts_ms=msg.device_ts_ms,
            )

        device = rt.registered

        # 2) 凭证错误（与“禁用”“超额”明确区分）
        if msg.credential != device.credential:
            self._reject(
                rt,
                ErrorCode.INVALID_CREDENTIALS,
                f"接入凭证无效: device_id={msg.device_id}",
                status_code=401,
                device_id=msg.device_id,
                msg_id=msg.msg_id,
                device_ts_ms=msg.device_ts_ms,
            )

        # 3) 设备被禁用
        if not device.enabled:
            self._reject(
                rt,
                ErrorCode.DEVICE_DISABLED,
                f"设备已被禁用: {msg.device_id}",
                status_code=403,
                device_id=msg.device_id,
                msg_id=msg.msg_id,
                device_ts_ms=msg.device_ts_ms,
            )

        # 4) 同一设备：去重 / 新旧 / 配额 / 生效，全部在一把设备锁内。
        with rt.lock:
            # 4.1) 幂等去重：同 msg_id 重复投递直接返回同一结论，不重复计数。
            if msg.msg_id in rt.seen_msg_ids:
                rt.duplicate_count += 1
                reason = (
                    f"重复投递: msg_id={msg.msg_id} 已处理过（首次 device_ts_ms="
                    f"{rt.last_device_ts_ms}），不重复计数/不推进状态"
                )
                logger.info(
                    "重复上报 | 输入 device_id=%s msg_id=%s device_ts_ms=%s | 判定=%s 依据=%s",
                    msg.device_id, msg.msg_id, msg.device_ts_ms,
                    IngestOutcome.DUPLICATE.value, reason,
                )
                return IngestResult(
                    outcome=IngestOutcome.DUPLICATE,
                    device_id=msg.device_id,
                    msg_id=msg.msg_id,
                    device_ts_ms=msg.device_ts_ms,
                    reason=reason,
                    count_after=rt.accepted_count,
                )

            # 4.2) 新旧判定：以设备侧时间戳为唯一依据。
            #   严格更新（>）才算新；更早或相等（乱序/同刻）都不得覆盖最新状态。
            if (
                rt.last_device_ts_ms is not None
                and msg.device_ts_ms <= rt.last_device_ts_ms
            ):
                rt.stale_count += 1
                rt.seen_msg_ids.add(msg.msg_id)
                reason = (
                    f"乱序旧数据: device_ts_ms={msg.device_ts_ms} <= 已处理最新 "
                    f"{rt.last_device_ts_ms}(msg_id={rt.last_msg_id})，拒绝覆盖"
                )
                logger.info(
                    "乱序上报 | 输入 device_id=%s msg_id=%s device_ts_ms=%s | 判定=%s 依据=%s",
                    msg.device_id, msg.msg_id, msg.device_ts_ms,
                    IngestOutcome.STALE.value, reason,
                )
                return IngestResult(
                    outcome=IngestOutcome.STALE,
                    device_id=msg.device_id,
                    msg_id=msg.msg_id,
                    device_ts_ms=msg.device_ts_ms,
                    reason=reason,
                    count_after=rt.accepted_count,
                )

            # 4.3) 配额：quota>0 时只允许 quota 条真正生效的新消息。
            if device.quota and rt.accepted_count >= device.quota:
                self._reject(
                    rt,
                    ErrorCode.QUOTA_EXCEEDED,
                    f"超出设备配额: {msg.device_id} 已用 {rt.accepted_count}/{device.quota}",
                    status_code=429,
                    device_id=msg.device_id,
                    msg_id=msg.msg_id,
                    device_ts_ms=msg.device_ts_ms,
                )

            # 4.4) 原子生效点：以下写入要么一起完成，要么都不发生。
            applied, shadow_reason = self._shadows.apply_reported(rt, msg)
            rt.seen_msg_ids.add(msg.msg_id)
            rt.accepted_count += 1
            rt.last_device_ts_ms = msg.device_ts_ms
            rt.last_msg_id = msg.msg_id
            rt.last_payload = dict(msg.payload)
            reported_state = msg.reported.get("state")
            if reported_state == "offline":
                rt.state = DeviceState.OFFLINE
            else:
                rt.state = DeviceState.ONLINE

            reason = (
                f"首次生效: msg_id={msg.msg_id}, device_ts_ms={msg.device_ts_ms}; "
                f"影子: {shadow_reason}"
            )
            logger.info(
                "接受上报 | 输入 device_id=%s msg_id=%s device_ts_ms=%s | 判定=%s 依据=%s "
                "| 生效后 accepted=%s state=%s",
                msg.device_id, msg.msg_id, msg.device_ts_ms,
                IngestOutcome.ACCEPTED.value, reason,
                rt.accepted_count, rt.state.value,
            )
            return IngestResult(
                outcome=IngestOutcome.ACCEPTED,
                device_id=msg.device_id,
                msg_id=msg.msg_id,
                device_ts_ms=msg.device_ts_ms,
                reason=reason,
                count_after=rt.accepted_count,
            )

    # -- 查询 -------------------------------------------------------------

    def stats(self, device_id: str) -> TelemetryStats:
        rt = self._store.get(device_id)
        if rt is None or rt.registered is None:
            raise ServiceError(
                ErrorCode.DEVICE_NOT_FOUND, f"设备不存在: {device_id}", status_code=404
            )
        with rt.lock:
            return TelemetryStats(
                device_id=device_id,
                accepted_count=rt.accepted_count,
                duplicate_count=rt.duplicate_count,
                stale_count=rt.stale_count,
                rejected_count=rt.rejected_count,
                last_device_ts_ms=rt.last_device_ts_ms,
                last_msg_id=rt.last_msg_id,
            )

    def snapshot_state(self, device_id: str) -> DeviceState:
        rt = self._store.get(device_id)
        if rt is None or rt.registered is None:
            raise ServiceError(
                ErrorCode.DEVICE_NOT_FOUND, f"设备不存在: {device_id}", status_code=404
            )
        with rt.lock:
            return rt.state
