"""设备影子测试：期望/上报分离、delta 收敛、版本乐观锁、重连增量补报幂等。"""

from __future__ import annotations

import logging
import unittest

from app.errors import ErrorCode, ServiceError
from app.models import TelemetryMessage
from app.service import TelemetryService
from app.shadow import ShadowService
from app.store import InMemoryStore

logger = logging.getLogger("test.shadow")


def build(quota: int = 0):
    store = InMemoryStore()
    shadows = ShadowService(store, max_desired_bytes=256)
    svc = TelemetryService(store, shadows)
    svc.register_device("dev-1", "k", quota=quota)
    return store, shadows, svc


def telemetry(mid: str, ts: int, reported: dict) -> TelemetryMessage:
    return TelemetryMessage("dev-1", "k", mid, ts, reported=reported)


class ShadowDeltaTest(unittest.TestCase):
    def test_desired_reported_separation_and_delta_convergence(self):
        _, shadows, _ = build()
        sh = shadows.update_desired("dev-1", {"led": "on", "fan": 2}, None)
        logger.info("[用例:delta] 写入期望 v=%s delta=%s", sh.desired_version, sh.delta)
        self.assertEqual(sh.delta, {"led": "on", "fan": 2})

        rt = shadows._store.runtime_for("dev-1")
        with rt.lock:
            ok, why = shadows.apply_reported(rt, telemetry("m1", 100, {"led": "on"}))
        logger.info("[用例:delta] 部分上报 生效=%s 依据=%s", ok, why)
        self.assertTrue(ok)
        self.assertEqual(shadows.get_shadow("dev-1").delta, {"fan": 2})

        with rt.lock:
            shadows.apply_reported(rt, telemetry("m2", 200, {"fan": 2}))
        sh = shadows.get_shadow("dev-1")
        logger.info("[用例:delta] 全部对齐后 delta=%s", sh.delta)
        self.assertEqual(sh.delta, {})
        # 期望与上报仍是两份独立数据
        self.assertEqual(sh.desired, {"led": "on", "fan": 2})
        self.assertEqual(sh.reported, {"led": "on", "fan": 2})


class ReconnectReplayTest(unittest.TestCase):
    def test_replayed_reported_after_reconnect_applied_once(self):
        store, shadows, svc = build()
        shadows.update_desired("dev-1", {"mode": "eco"}, None)
        # 首次上报应用成功
        r1 = svc.ingest(telemetry("m1", 100, {"mode": "eco"}))
        self.assertEqual(r1.outcome.value, "accepted")
        self.assertEqual(shadows.sync_delta("dev-1"), {})
        # 设备断线重连，重放同一条增量补报：服务层幂等 + 影子层位点双重保护
        r2 = svc.ingest(telemetry("m1", 100, {"mode": "eco"}))
        logger.info("[用例:重连补报] 重放 m1 outcome=%s 依据=%s", r2.outcome.value, r2.reason)
        self.assertEqual(r2.outcome.value, "duplicate")
        # 即使绕过服务层直接调影子（模拟内部重放），位点也保证不重复应用
        rt = store.runtime_for("dev-1")
        with rt.lock:
            applied_direct, why = shadows.apply_reported(rt, telemetry("m1", 100, {"mode": "eco"}))
        logger.info("[用例:重连补报] 影子直调 生效=%s 依据=%s", applied_direct, why)
        self.assertFalse(applied_direct)
        sh = shadows.get_shadow("dev-1")
        self.assertEqual(sh.reported_ts_ms, 100)
        self.assertEqual(sh.last_applied_msg_id, "m1")

    def test_stale_reported_never_overwrites_shadow(self):
        _, shadows, svc = build()
        svc.ingest(telemetry("new", 200, {"mode": "eco"}))
        r = svc.ingest(telemetry("old", 100, {"mode": "sport"}))
        logger.info("[用例:影子乱序] outcome=%s 依据=%s", r.outcome.value, r.reason)
        self.assertEqual(r.outcome.value, "stale")
        sh = shadows.get_shadow("dev-1")
        self.assertEqual(sh.reported, {"mode": "eco"})
        self.assertEqual(sh.reported_ts_ms, 200)


class DesiredVersionTest(unittest.TestCase):
    def test_optimistic_version_conflict_is_distinguishable(self):
        _, shadows, _ = build()
        shadows.update_desired("dev-1", {"a": 1}, None)  # v1
        shadows.update_desired("dev-1", {"a": 2}, expected_version=1)  # v2
        with self.assertRaises(ServiceError) as cm:
            shadows.update_desired("dev-1", {"a": 3}, expected_version=1)
        logger.info("[用例:版本冲突] code=%s 依据=%s",
                    cm.exception.code.value, cm.exception.message)
        self.assertEqual(cm.exception.code, ErrorCode.DESIRED_VERSION_CONFLICT)
        self.assertEqual(cm.exception.status_code, 409)
        self.assertEqual(shadows.get_shadow("dev-1").desired["a"], 2)

    def test_desired_too_large_rejected(self):
        _, shadows, _ = build()
        big = {"k": "x" * 300}
        with self.assertRaises(ServiceError) as cm:
            shadows.update_desired("dev-1", big, None)
        self.assertEqual(cm.exception.code, ErrorCode.DESIRED_TOO_LARGE)
        self.assertEqual(cm.exception.status_code, 413)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s | %(message)s")
    unittest.main(verbosity=2)
