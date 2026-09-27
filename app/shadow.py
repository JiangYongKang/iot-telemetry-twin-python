"""设备影子服务：期望状态 / 上报状态分离、delta 计算与增量补报去重。

规则：
- desired 带单调版本号，更新支持乐观并发（expected_version）；
- reported 依据设备侧时间戳 device_ts_ms 判定新旧：严格更新才覆盖，
  旧时间戳（乱序到达）不得覆盖新状态，相等时间戳的不同消息视为过期；
- 同一条消息（msg_id）重连后再次补报不会被重复应用；
- delta = desired 中尚未被 reported 对齐的字段（值一致即视为已对齐）。
"""

from __future__ import annotations

import json
from typing import Any

from app.errors import ErrorCode, ServiceError
from app.models import DeviceShadow, TelemetryMessage
from app.store import DeviceRuntime, InMemoryStore


def _compute_delta(desired: dict[str, Any], reported: dict[str, Any]) -> dict[str, Any]:
    """期望中与上报不一致（或缺省）的字段构成增量；值一致即清除。"""
    delta: dict[str, Any] = {}
    for key, wanted in desired.items():
        if key not in reported or reported[key] != wanted:
            delta[key] = wanted
    return delta


class ShadowService:
    def __init__(self, store: InMemoryStore, max_desired_bytes: int = 16 * 1024) -> None:
        self._store = store
        self._max_desired_bytes = max_desired_bytes

    def ensure_shadow(self, rt: DeviceRuntime) -> DeviceShadow:
        """惰性创建影子（调用方须持有 rt.lock）。"""
        if rt.shadow is None:
            rt.shadow = DeviceShadow(device_id=rt.registered.device_id)
        return rt.shadow

    def get_shadow(self, device_id: str) -> DeviceShadow:
        rt = self._store.get(device_id)
        if rt is None or rt.registered is None:
            raise ServiceError(
                ErrorCode.DEVICE_NOT_FOUND, f"设备不存在: {device_id}", status_code=404
            )
        with rt.lock:
            return self.ensure_shadow(rt)

    def update_desired(
        self,
        device_id: str,
        patch: dict[str, Any],
        expected_version: int | None,
    ) -> DeviceShadow:
        if not isinstance(patch, dict):
            raise ServiceError(
                ErrorCode.INVALID_MESSAGE, "desired 补丁必须是 JSON 对象", status_code=400
            )
        size = len(json.dumps(patch, ensure_ascii=False).encode("utf-8"))
        if size > self._max_desired_bytes:
            raise ServiceError(
                ErrorCode.DESIRED_TOO_LARGE,
                f"desired 大小 {size}B 超过上限 {self._max_desired_bytes}B",
                status_code=413,
            )
        rt = self._store.get(device_id)
        if rt is None or rt.registered is None:
            raise ServiceError(
                ErrorCode.DEVICE_NOT_FOUND, f"设备不存在: {device_id}", status_code=404
            )
        with rt.lock:
            shadow = self.ensure_shadow(rt)
            # 乐观并发：客户端可携带它看到的版本号，冲突要与其他错误区分开。
            if expected_version is not None and expected_version != shadow.desired_version:
                raise ServiceError(
                    ErrorCode.DESIRED_VERSION_CONFLICT,
                    f"期望版本冲突: 携带 {expected_version}, 实际 {shadow.desired_version}",
                    status_code=409,
                )
            shadow.desired.update(patch)
            shadow.desired_version += 1
            shadow.delta = _compute_delta(shadow.desired, shadow.reported)
            return shadow

    def apply_reported(
        self, rt: DeviceRuntime, msg: TelemetryMessage
    ) -> tuple[bool, str]:
        """在已持有 rt.lock 的前提下应用一次上报。

        返回 (是否生效, 判定依据)。不生效的情形：
        1) 同一 msg_id 重复补报；
        2) device_ts_ms 不严格大于已应用的最新时间戳（乱序旧数据）。
        无 reported 字段的消息无需改动影子，按生效但空操作处理。
        """
        shadow = self.ensure_shadow(rt)

        # 1) 增量补报幂等：重连后重放同一条消息不重复应用。
        if shadow.last_applied_msg_id == msg.msg_id:
            return (
                False,
                f"重复补报: msg_id={msg.msg_id} 已应用，跳过影子更新",
            )

        # 2) 新旧判定：以设备侧时间戳为准，严格更新才允许覆盖。
        if (
            shadow.reported_ts_ms is not None
            and msg.device_ts_ms <= shadow.reported_ts_ms
        ):
            return (
                False,
                "乱序旧数据: device_ts_ms="
                f"{msg.device_ts_ms} <= 已应用 {shadow.reported_ts_ms}，不覆盖影子",
            )

        if not msg.reported:
            shadow.last_applied_msg_id = msg.msg_id
            return True, f"msg_id={msg.msg_id} 无 reported 字段，仅记录应用位点"

        shadow.reported.update(msg.reported)
        shadow.reported_ts_ms = msg.device_ts_ms
        shadow.last_applied_msg_id = msg.msg_id
        shadow.delta = _compute_delta(shadow.desired, shadow.reported)
        return (
            True,
            f"应用 reported: msg_id={msg.msg_id}, device_ts_ms={msg.device_ts_ms}",
        )

    def sync_delta(self, device_id: str) -> dict[str, Any]:
        """设备重连后拉取待对齐增量（只读，不改变影子版本与位点）。"""
        shadow = self.get_shadow(device_id)
        return dict(shadow.delta)
