"""错误码与服务异常：接入失败原因必须可区分。"""

from __future__ import annotations

from enum import Enum


class ErrorCode(str, Enum):
    """所有可区分的失败原因。"""

    DEVICE_NOT_FOUND = "device_not_found"
    INVALID_CREDENTIALS = "invalid_credentials"
    DEVICE_DISABLED = "device_disabled"
    QUOTA_EXCEEDED = "quota_exceeded"
    INVALID_MESSAGE = "invalid_message"
    DESIRED_VERSION_CONFLICT = "desired_version_conflict"
    DESIRED_TOO_LARGE = "desired_too_large"


class ServiceError(Exception):
    """携带稳定 error_code 的业务异常，供 API 层映射为明确的 HTTP 响应。"""

    def __init__(self, code: ErrorCode, message: str, *, status_code: int = 400):
        self.code = code
        self.message = message
        self.status_code = status_code
        super().__init__(message)
