"""设备影子：期望/上报状态分离，增量补报幂等，字段级期望差异。"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any

from .models import DeviceShadow


@dataclass(frozen=True)
class FieldDelta:
    """单个未对齐字段的差异明细。"""

    field: str
    desired_value: Any
    reported_value: Any  # 设备从未上报过该字段时为 None，并由 reported_present 区分
    reported_present: bool


@dataclass(frozen=True)
class ShadowDelta:
    """期望状态与上报状态的字段级差异。

    以最新期望为准：只列出 desired 中尚未被 reported 对齐的字段；
    设备把对应字段报上来之后，差异随之缩小、消失。
    """

    device_id: str
    desired_version: int
    reported_version: int
    fields: tuple[FieldDelta, ...]

    @property
    def in_sync(self) -> bool:
        return not self.fields


class ShadowStore:
    """按设备维护影子。

    写路径的并发一致性由 IngestService 的设备级锁保证；
    这里自身也加锁，保证读（如 API 查询）不会看到半截状态。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._shadows: dict[str, DeviceShadow] = {}

    def get_or_create(self, device_id: str) -> DeviceShadow:
        with self._lock:
            return self._shadows.setdefault(device_id, DeviceShadow())

    def set_desired(self, device_id: str, desired: dict[str, Any]) -> int:
        """更新期望状态并递增版本，返回新版本号。"""
        with self._lock:
            shadow = self._shadows.setdefault(device_id, DeviceShadow())
            shadow.desired.update(desired)
            shadow.desired_version += 1
            return shadow.desired_version

    def apply_reported(
        self, device_id: str, state: dict[str, Any], device_ts: float
    ) -> bool:
        """应用上报状态。

        新旧判定依据设备侧时间戳：device_ts 严格大于已见最大时间戳才应用，
        否则视为乱序旧数据或重连补报的重复增量，拒绝并返回 False。
        """
        with self._lock:
            shadow = self._shadows.setdefault(device_id, DeviceShadow())
            if device_ts <= shadow.last_device_ts:
                return False
            shadow.reported.update(state)
            shadow.reported_version += 1
            shadow.last_device_ts = device_ts
            return True

    def rebuild_reported(
        self, device_id: str, folded_state: dict[str, Any], last_device_ts: float
    ) -> None:
        """回放重建：用按设备时间顺序折叠出的状态整体替换 reported。

        仅在补报乱序历史消息时由 IngestService 在设备锁内调用。
        """
        with self._lock:
            shadow = self._shadows.setdefault(device_id, DeviceShadow())
            shadow.reported = dict(folded_state)
            shadow.reported_version += 1
            shadow.last_device_ts = last_device_ts

    def delta(self, device_id: str) -> ShadowDelta:
        """计算字段级期望差异：以最新 desired 为准，逐字段比对 reported。"""
        with self._lock:
            shadow = self._shadows.setdefault(device_id, DeviceShadow())
            fields = tuple(
                FieldDelta(
                    field=key,
                    desired_value=desired_value,
                    reported_value=shadow.reported.get(key),
                    reported_present=key in shadow.reported,
                )
                for key, desired_value in shadow.desired.items()
                if key not in shadow.reported
                or shadow.reported[key] != desired_value
            )
            return ShadowDelta(
                device_id=device_id,
                desired_version=shadow.desired_version,
                reported_version=shadow.reported_version,
                fields=fields,
            )
