"""设备影子：期望/上报状态分离，增量补报幂等。"""
from __future__ import annotations

import threading
from typing import Any

from .models import DeviceShadow


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
