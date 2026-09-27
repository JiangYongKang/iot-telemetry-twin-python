"""领域模型：设备、遥测消息、设备影子、统计等数据结构。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class DeviceState(str, Enum):
    """设备生命周期状态（由遥测上报推进）。"""

    NEVER_SEEN = "never_seen"
    ONLINE = "online"
    OFFLINE = "offline"


@dataclass(frozen=True)
class Device:
    """设备注册信息与接入配置。"""

    device_id: str
    credential: str
    enabled: bool = True
    quota: int = 0  # 允许计入的遥测条数上限；0 视为不限制


@dataclass(frozen=True)
class TelemetryMessage:
    """一条遥测上报。

    device_ts_ms 为设备侧时间戳（毫秒），是新旧判定的唯一依据；
    msg_id 为设备侧消息唯一标识，用于幂等去重。
    """

    device_id: str
    credential: str
    msg_id: str
    device_ts_ms: int
    payload: dict[str, Any] = field(default_factory=dict)
    reported: dict[str, Any] = field(default_factory=dict)


class IngestOutcome(str, Enum):
    """上报处理结论。重复投递与过期乱序不是错误，而是明确的结论。"""

    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    STALE = "stale"


@dataclass(frozen=True)
class IngestResult:
    """上报处理结果，携带可解释的判定依据。"""

    outcome: IngestOutcome
    device_id: str
    msg_id: str
    device_ts_ms: int
    reason: str
    count_after: int


@dataclass(frozen=True)
class TelemetryStats:
    """遥测计数统计（只统计真正生效的新消息）。"""

    device_id: str
    accepted_count: int
    duplicate_count: int
    stale_count: int
    rejected_count: int
    last_device_ts_ms: int | None
    last_msg_id: str | None


@dataclass
class DeviceShadow:
    """期望状态与上报状态分离的设备影子。

    - desired：云端期望，带单调版本号；
    - reported：设备上报，更新依据设备侧时间戳；
    - delta：尚未被上报状态对齐的期望字段；
    - last_applied_msg_id：已应用增量补报的消息 id，保证重连补报不重复应用。
    """

    device_id: str
    desired: dict[str, Any] = field(default_factory=dict)
    desired_version: int = 0
    reported: dict[str, Any] = field(default_factory=dict)
    reported_ts_ms: int | None = None
    delta: dict[str, Any] = field(default_factory=dict)
    last_applied_msg_id: str | None = None
