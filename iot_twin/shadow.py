"""设备影子：期望/上报状态分离，增量补报幂等，期望差异可逐项查询。"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any

from .models import DeviceShadow

# 哨兵：表示某期望字段从未被上报过，区别于显式上报了 None。
MISSING = object()


@dataclass(frozen=True)
class DesiredDelta:
    """期望与上报之间尚未对齐的字段差异（逐字段给出）。

    - pending: 尚未对齐的字段：字段名 -> (期望值, 实际上报值)。
      实际上报值为 MISSING 表示该字段从未上报过。
    - aligned: 已对齐的字段名。
    差异始终以最新期望为准：设备上报后 pending 会随之缩小、消失。
    """

    device_id: str
    desired_version: int
    pending: dict[str, tuple[Any, Any]]
    aligned: tuple[str, ...]


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

    def reset_reported(
        self, device_id: str, state: dict[str, Any], last_device_ts: float
    ) -> None:
        """以"历史按序回放得到的最终结果"整体替换 reported。

        批量补报可能在时间轴中间插入此前丢失的消息，逐条 update 无法
        表达"重排"，因此由调用方（IngestService，持有设备锁）把有界
        回放窗口折叠出的最终状态交给这里一次性落地；版本号按实际产生
        变化的补报消息数推进。期望状态（desired）不受影响。
        """
        with self._lock:
            shadow = self._shadows.setdefault(device_id, DeviceShadow())
            shadow.reported = dict(state)
            shadow.last_device_ts = last_device_ts

    def bump_reported_version(self, device_id: str, by: int = 1) -> int:
        with self._lock:
            shadow = self._shadows.setdefault(device_id, DeviceShadow())
            shadow.reported_version += by
            return shadow.reported_version

    def desired_delta(self, device_id: str) -> DesiredDelta:
        """返回期望中尚未与上报对上的字段差异（逐字段，以最新期望为准）。

        只考察期望里出现过的字段；设备多报的字段不产生差异。
        期望值被更新后立即按新值重新比对；设备把对应字段报上来后，
        该字段从未对齐集合移入已对齐集合，差异随之缩小、消失。
        """
        with self._lock:
            shadow = self._shadows.setdefault(device_id, DeviceShadow())
            pending: dict[str, tuple[Any, Any]] = {}
            aligned: list[str] = []
            for key, desired_value in shadow.desired.items():
                if key not in shadow.reported:
                    pending[key] = (desired_value, MISSING)
                elif shadow.reported[key] != desired_value:
                    pending[key] = (desired_value, shadow.reported[key])
                else:
                    aligned.append(key)
            return DesiredDelta(
                device_id=device_id,
                desired_version=shadow.desired_version,
                pending=pending,
                aligned=tuple(aligned),
            )
