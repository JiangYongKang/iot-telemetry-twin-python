"""期望差异：离线期间多次改期望，重连后按字段补齐，差异随之缩小直至消失。"""
from __future__ import annotations

import logging

from iot_twin.shadow import ShadowStore

log = logging.getLogger("test_shadow_delta")


def test_delta_tracks_field_level_alignment() -> None:
    shadows = ShadowStore()
    # 设备离线期间，期望状态被改了好几次
    shadows.set_desired("dev-1", {"interval": 30, "mode": "eco"})
    shadows.set_desired("dev-1", {"interval": 60})           # 覆盖旧值
    shadows.set_desired("dev-1", {"ota": "v2"})

    d = shadows.delta("dev-1")
    log.info("设备尚未上报，差异字段=%s",
             [(f.field, f.desired_value, f.reported_present) for f in d.fields])
    assert not d.in_sync
    # 差异以最新期望为准：interval 是最后改的 60，不是最早的 30
    by_field = {f.field: f for f in d.fields}
    assert set(by_field) == {"interval", "mode", "ota"}
    assert by_field["interval"].desired_value == 60
    assert not by_field["interval"].reported_present  # 从未上报过，可与上报了 None 区分

    # 设备重连，先补上一个字段：差异缩小
    shadows.apply_reported("dev-1", {"interval": 60}, device_ts=1.0)
    d = shadows.delta("dev-1")
    log.info("补上 interval 后，剩余差异=%s", [f.field for f in d.fields])
    assert {f.field for f in d.fields} == {"mode", "ota"}

    # 再补上其余字段：差异消失
    shadows.apply_reported("dev-1", {"mode": "eco", "ota": "v2"}, device_ts=2.0)
    d = shadows.delta("dev-1")
    log.info("全部对齐后 in_sync=%s desired_version=%d",
             d.in_sync, d.desired_version)
    assert d.in_sync
    assert d.fields == ()

    # 上报值与期望不一致时，该字段重新出现在差异里，且能看到两侧取值
    shadows.set_desired("dev-1", {"mode": "boost"})
    d = shadows.delta("dev-1")
    by_field = {f.field: f for f in d.fields}
    log.info("期望改为 boost 后，mode 差异=%s", by_field["mode"])
    assert set(by_field) == {"mode"}
    assert by_field["mode"].desired_value == "boost"
    assert by_field["mode"].reported_value == "eco"
    assert by_field["mode"].reported_present


def test_delta_ignores_desired_fields_already_matched() -> None:
    shadows = ShadowStore()
    shadows.set_desired("dev-1", {"a": 1, "b": 2})
    shadows.apply_reported("dev-1", {"a": 1, "b": 2, "extra": "x"}, device_ts=1.0)
    d = shadows.delta("dev-1")
    log.info("reported 多出 extra 字段，差异=%s", d.fields)
    assert d.in_sync  # reported 里多出的字段不影响“期望是否对齐”
