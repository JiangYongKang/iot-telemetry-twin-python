"""可区分的拒绝原因与异常。"""
from __future__ import annotations

from enum import Enum


class RejectReason(str, Enum):
    UNKNOWN_DEVICE = "unknown_device"
    BAD_CREDENTIAL = "bad_credential"
    DEVICE_DISABLED = "device_disabled"
    QUOTA_EXCEEDED = "quota_exceeded"
    # 设备请求接续的位置已超出服务端保留范围，无法保证回放完整，
    # 必须全量对齐，不能把残缺回放当成正常结果。
    RESYNC_REQUIRED = "resync_required"


class RejectError(Exception):
    """上报被拒绝，reason 区分具体失败原因。"""

    def __init__(self, reason: RejectReason, device_id: str, detail: str = ""):
        self.reason = reason
        self.device_id = device_id
        self.detail = detail
        super().__init__(f"{reason.value}: device={device_id} {detail}".strip())
