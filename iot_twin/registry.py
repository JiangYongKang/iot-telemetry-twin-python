"""设备注册与接入凭证校验。"""
from __future__ import annotations

import threading

from .errors import RejectError, RejectReason
from .models import Device, DeviceStatus


class DeviceRegistry:
    """内存设备注册表，线程安全。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._devices: dict[str, Device] = {}

    def register(
        self, device_id: str, credential: str, quota: int = 1000
    ) -> Device:
        with self._lock:
            device = Device(device_id=device_id, credential=credential, quota=quota)
            self._devices[device_id] = device
            return device

    def disable(self, device_id: str) -> None:
        with self._lock:
            device = self._devices.get(device_id)
            if device is None:
                raise RejectError(RejectReason.UNKNOWN_DEVICE, device_id)
            device.status = DeviceStatus.DISABLED

    def get(self, device_id: str) -> Device | None:
        with self._lock:
            return self._devices.get(device_id)

    def authenticate(self, device_id: str, credential: str) -> Device:
        """校验设备存在、凭证正确、未被禁用；失败抛出带具体原因的 RejectError。"""
        with self._lock:
            device = self._devices.get(device_id)
            if device is None:
                raise RejectError(RejectReason.UNKNOWN_DEVICE, device_id)
            if device.credential != credential:
                raise RejectError(
                    RejectReason.BAD_CREDENTIAL, device_id, "credential mismatch"
                )
            if device.status is DeviceStatus.DISABLED:
                raise RejectError(RejectReason.DEVICE_DISABLED, device_id)
            return device

    def consume_quota(self, device_id: str) -> None:
        """占用一个上报配额，超额抛 QUOTA_EXCEEDED。"""
        with self._lock:
            device = self._devices.get(device_id)
            if device is None:
                raise RejectError(RejectReason.UNKNOWN_DEVICE, device_id)
            if device.accepted_count >= device.quota:
                raise RejectError(
                    RejectReason.QUOTA_EXCEEDED,
                    device_id,
                    f"quota={device.quota} reached",
                )
            device.accepted_count += 1
