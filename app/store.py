"""内存存储：设备注册表、每设备锁与运行态（计数、去重窗口、影子）。

不依赖任何外部中间件；每设备一把可重入锁（RLock），保证同一设备的
计数推进、新旧状态写入与影子更新在同一临界区内原子生效。
设备之间互不阻塞（锁按设备隔离）。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any

from app.models import Device, DeviceShadow, DeviceState


@dataclass
class DeviceRuntime:
    """单个设备的全部可变运行态（任何读写都必须持有该设备的 lock）。"""

    lock: threading.RLock = field(default_factory=threading.RLock)
    registered: Device | None = None
    state: DeviceState = DeviceState.NEVER_SEEN
    accepted_count: int = 0
    duplicate_count: int = 0
    stale_count: int = 0
    rejected_count: int = 0
    seen_msg_ids: set[str] = field(default_factory=set)
    last_device_ts_ms: int | None = None
    last_msg_id: str | None = None
    last_payload: dict[str, Any] = field(default_factory=dict)
    shadow: DeviceShadow | None = None


class InMemoryStore:
    """进程内存储：设备运行态的注册、查找与隔离锁。"""

    def __init__(self) -> None:
        self._runtimes: dict[str, DeviceRuntime] = {}
        # 仅保护“运行态字典本身”的创建；设备自身数据由各设备锁保护。
        self._registry_lock = threading.Lock()

    def register_device(self, device: Device) -> Device:
        """注册（或更新注册信息）；重复注册保留既有计数与影子，只更新配置。"""
        with self._registry_lock:
            rt = self._runtimes.get(device.device_id)
            if rt is None:
                rt = DeviceRuntime()
                self._runtimes[device.device_id] = rt
        with rt.lock:
            rt.registered = device
        return device

    def get(self, device_id: str) -> DeviceRuntime | None:
        """返回设备运行态；未注册返回 None（调用方负责转成可区分错误）。"""
        return self._runtimes.get(device_id)

    def runtime_for(self, device_id: str) -> DeviceRuntime:
        """返回已注册设备的运行态；未注册属于编程错误，直接抛出。"""
        rt = self._runtimes.get(device_id)
        if rt is None:
            raise KeyError(device_id)
        return rt

    def all_devices(self) -> list[Device]:
        return [
            rt.registered
            for rt in self._runtimes.values()
            if rt.registered is not None
        ]
