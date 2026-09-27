"""数据模型：设备、遥测消息、设备影子与统计。"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class DeviceStatus(str, Enum):
    ACTIVE = "active"
    DISABLED = "disabled"


@dataclass
class Device:
    """已注册设备及其接入凭证与配额。"""

    device_id: str
    credential: str
    status: DeviceStatus = DeviceStatus.ACTIVE
    quota: int = 1000  # 允许上报的最大消息数
    accepted_count: int = 0


@dataclass(frozen=True)
class TelemetryMessage:
    """一条遥测上报。

    - message_id: 幂等键，重复投递同一 id 只生效一次。
    - device_ts: 设备侧时间戳（秒），用于乱序新旧判定。
    - state: 上报状态（设备影子 reported 部分）。
    - metrics: 数值型遥测，进入统计。
    """

    device_id: str
    message_id: str
    device_ts: float
    state: dict[str, Any] = field(default_factory=dict)
    metrics: dict[str, float] = field(default_factory=dict)


@dataclass
class DeviceShadow:
    """设备影子：期望状态与上报状态分离，各自带版本号。"""

    desired: dict[str, Any] = field(default_factory=dict)
    desired_version: int = 0
    reported: dict[str, Any] = field(default_factory=dict)
    reported_version: int = 0
    last_device_ts: float = float("-inf")


@dataclass
class TelemetryStats:
    """遥测统计：只统计被接受且未重复的消息。"""

    accepted_messages: int = 0
    duplicate_messages: int = 0
    stale_messages: int = 0
    metric_sums: dict[str, float] = field(default_factory=dict)
    metric_counts: dict[str, int] = field(default_factory=dict)

    def averages(self) -> dict[str, float]:
        return {
            k: self.metric_sums[k] / self.metric_counts[k]
            for k in self.metric_sums
            if self.metric_counts.get(k)
        }


def now() -> float:
    return time.time()
