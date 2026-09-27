"""服务层测试：重复上报、乱序到达、同设备并发写入、超额拒绝、失败原因可区分。

每个用例都会通过 logging 打印“触发判定的输入与依据”（见 iot.ingest logger）。
纯内存实现，不依赖外部服务或真实凭据。
"""

from __future__ import annotations

import logging
import threading
import unittest

from app.errors import ErrorCode, ServiceError
from app.models import DeviceState, TelemetryMessage
from app.service import TelemetryService
from app.shadow import ShadowService
from app.store import InMemoryStore

logger = logging.getLogger("test.service")


def make_service(quota: int = 0) -> TelemetryService:
    store = InMemoryStore()
    svc = TelemetryService(store, ShadowService(store))
    svc.register_device("dev-1", "credential-A", quota=quota)
    svc.register_device("dev-disabled", "credential-B")
    # 直接更新为禁用，模拟运维侧封禁。
    rt = store.get("dev-disabled")
    with rt.lock:
        d = rt.registered
        rt.registered = type(d)(d.device_id, d.credential, False, d.quota)
    return svc


def msg(msg_id: str, ts: int, *, device: str = "dev-1", cred: str = "credential-A",
        reported: dict | None = None) -> TelemetryMessage:
    return TelemetryMessage(device, cred, msg_id, ts, reported=reported or {})


class DuplicateTest(unittest.TestCase):
    """同一条消息重复投递：只计一次，不重复推进状态，结论可复现。"""

    def test_same_message_delivered_many_times_counts_once(self):
        svc = make_service()
        inputs = [msg("msg-1", 1000)] * 5
        outcomes = []
        for i, m in enumerate(inputs):
            logger.info("[用例:重复] 第%s次投递 输入 msg_id=%s device_ts_ms=%s",
                        i + 1, m.msg_id, m.device_ts_ms)
            r = svc.ingest(m)
            logger.info("[用例:重复] 判定 outcome=%s 依据=%s", r.outcome.value, r.reason)
            outcomes.append(r.outcome.value)
        self.assertEqual(outcomes, ["accepted"] + ["duplicate"] * 4)
        st = svc.stats("dev-1")
        logger.info("[用例:重复] 最终统计 accepted=%s duplicate=%s",
                    st.accepted_count, st.duplicate_count)
        self.assertEqual((st.accepted_count, st.duplicate_count), (1, 4))
        # 可复现：再来一遍结论不变
        self.assertEqual(svc.ingest(msg("msg-1", 1000)).outcome.value, "duplicate")
        self.assertEqual(svc.stats("dev-1").accepted_count, 1)


class OutOfOrderTest(unittest.TestCase):
    """乱序旧数据不得覆盖更新状态；新旧判定依据设备侧时间戳，结论可解释。"""

    def test_older_device_ts_does_not_overwrite(self):
        svc = make_service()
        sequence = [
            msg("m-new", 2000, reported={"state": "online", "rpm": 2000}),
            msg("m-old", 1000, reported={"state": "offline", "rpm": 1000}),
        ]
        results = []
        for m in sequence:
            logger.info("[用例:乱序] 输入 msg_id=%s device_ts_ms=%s", m.msg_id, m.device_ts_ms)
            r = svc.ingest(m)
            logger.info("[用例:乱序] 判定 outcome=%s 依据=%s", r.outcome.value, r.reason)
            results.append(r.outcome.value)
        self.assertEqual(results, ["accepted", "stale"])
        st = svc.stats("dev-1")
        self.assertEqual(st.last_msg_id, "m-new")
        self.assertEqual(st.last_device_ts_ms, 2000)
        # 旧状态不得盖回：设备仍 online，影子仍保留新值
        self.assertEqual(svc.snapshot_state("dev-1").value, "online")
        sh = svc._shadows.get_shadow("dev-1")
        self.assertEqual(sh.reported.get("rpm"), 2000)
        self.assertEqual(sh.last_applied_msg_id, "m-new")

    def test_equal_device_ts_is_not_newer(self):
        svc = make_service()
        self.assertEqual(svc.ingest(msg("a", 100)).outcome.value, "accepted")
        r = svc.ingest(msg("b", 100))
        logger.info("[用例:相等时间戳] 判定=%s 依据=%s", r.outcome.value, r.reason)
        # 同时间戳不同消息：顶层状态计数里视为乱序（不覆盖最新位点）
        self.assertEqual(r.outcome.value, "stale")
        self.assertEqual(svc.stats("dev-1").last_msg_id, "a")


class ConcurrencyTest(unittest.TestCase):
    """同一设备并发上报：计数与状态必须原子、数目对得上。"""

    def test_concurrent_unique_messages_all_counted(self):
        svc = make_service(quota=0)
        n_threads, per_thread = 20, 5
        total = n_threads * per_thread
        results: list[str] = []
        results_lock = threading.Lock()
        # 用有序闸门让消息按时间戳升序进入服务，从而把“并发原子计数”与
        # “乱序判定”两个变量隔离开；乱序行为由 OutOfOrderTest 专门覆盖。
        turn = 0
        cond = threading.Condition()

        def worker(tid: int) -> None:
            nonlocal turn
            local = []
            for i in range(per_thread):
                seq = tid * per_thread + i
                with cond:
                    while turn != seq:
                        cond.wait()
                    mid = f"t{tid}-m{i}"
                    ts = 1000 + seq
                    logger.info("[用例:并发] 线程%s 输入 msg_id=%s device_ts_ms=%s", tid, mid, ts)
                    r = svc.ingest(msg(mid, ts))
                    local.append(r.outcome.value)
                    turn += 1
                    cond.notify_all()
            with results_lock:
                results.extend(local)

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        st = svc.stats("dev-1")
        logger.info("[用例:并发] 最终 accepted=%s 期望=%s rejected=%s",
                    st.accepted_count, n_threads * per_thread, st.rejected_count)
        self.assertEqual(results.count("accepted"), total)
        self.assertEqual(st.accepted_count, total)
        self.assertEqual(st.rejected_count, 0)

    def test_concurrent_duplicates_of_same_msg_exactly_one_accepted(self):
        svc = make_service()
        n = 32
        barrier = threading.Barrier(n)
        outcomes: list[str] = []
        lock = threading.Lock()

        def fire() -> None:
            barrier.wait()  # 尽量把并发挤压到同一时刻
            r = svc.ingest(msg("hot-msg", 1234))
            with lock:
                outcomes.append(r.outcome.value)

        threads = [threading.Thread(target=fire) for _ in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        st = svc.stats("dev-1")
        logger.info("[用例:并发重复] accepted 结果数=%s 统计 accepted=%s duplicate=%s",
                    outcomes.count("accepted"), st.accepted_count, st.duplicate_count)
        self.assertEqual(outcomes.count("accepted"), 1)
        self.assertEqual(outcomes.count("duplicate"), n - 1)
        self.assertEqual((st.accepted_count, st.duplicate_count), (1, n - 1))


class QuotaAndRejectionTest(unittest.TestCase):
    """超额拒绝；凭证/禁用/未注册/超额失败原因彼此可区分。"""

    def test_quota_rejects_only_over_limit_and_duplicates_still_idempotent(self):
        svc = make_service(quota=2)
        self.assertEqual(svc.ingest(msg("q1", 1)).outcome.value, "accepted")
        self.assertEqual(svc.ingest(msg("q2", 2)).outcome.value, "accepted")
        with self.assertRaises(ServiceError) as cm:
            svc.ingest(msg("q3", 3))
        logger.info("[用例:配额] 拒绝 code=%s 依据=%s",
                    cm.exception.code.value, cm.exception.message)
        self.assertEqual(cm.exception.code, ErrorCode.QUOTA_EXCEEDED)
        self.assertEqual(cm.exception.status_code, 429)
        # 配额已满时，重复/乱序仍按幂等与新旧规则处理，不会被误判为超额
        self.assertEqual(svc.ingest(msg("q1", 1)).outcome.value, "duplicate")
        self.assertEqual(svc.ingest(msg("old", 0)).outcome.value, "stale")
        st = svc.stats("dev-1")
        self.assertEqual(st.accepted_count, 2)

    def test_rejection_reasons_are_distinguishable(self):
        svc = make_service()
        cases = [
            (msg("x", 1, device="unknown", cred="c"), ErrorCode.DEVICE_NOT_FOUND, 404),
            (msg("x", 1, cred="wrong-key"), ErrorCode.INVALID_CREDENTIALS, 401),
            (msg("x", 1, device="dev-disabled", cred="credential-B"),
             ErrorCode.DEVICE_DISABLED, 403),
        ]
        for m_in, expected_code, expected_status in cases:
            with self.subTest(code=expected_code):
                with self.assertRaises(ServiceError) as cm:
                    logger.info("[用例:拒绝区分] 输入 device=%s", m_in.device_id)
                    svc.ingest(m_in)
                logger.info("[用例:拒绝区分] 判定 code=%s status=%s 依据=%s",
                            cm.exception.code.value, cm.exception.status_code,
                            cm.exception.message)
                self.assertEqual(cm.exception.code, expected_code)
                self.assertEqual(cm.exception.status_code, expected_status)

    def test_invalid_message_shape(self):
        svc = make_service()
        with self.assertRaises(ServiceError) as cm:
            svc.ingest(TelemetryMessage("dev-1", "credential-A", "", 1))
        self.assertEqual(cm.exception.code, ErrorCode.INVALID_MESSAGE)
        with self.assertRaises(ServiceError) as cm:
            svc.ingest(TelemetryMessage("dev-1", "credential-A", "m", -1))
        self.assertEqual(cm.exception.code, ErrorCode.INVALID_MESSAGE)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    unittest.main(verbosity=2)


class MultiDeviceConcurrencyTest(unittest.TestCase):
    """多设备并发上报：锁按设备隔离，各设备计数与状态互不影响。"""

    def test_multiple_devices_concurrent_independent_counts(self):
        store = InMemoryStore()
        svc = TelemetryService(store, ShadowService(store))
        device_ids = [f"dev-{i}" for i in range(10)]
        for d in device_ids:
            svc.register_device(d, "k", quota=0)

        per_device = 20

        def worker(dev: str) -> None:
            for i in range(per_device):
                logger.info("[用例:多设备并发] 设备=%s 输入 msg_id=%s device_ts_ms=%s",
                            dev, f"{dev}-m{i}", 1000 + i)
                svc.ingest(TelemetryMessage(dev, "k", f"{dev}-m{i}", 1000 + i))

        threads = [threading.Thread(target=worker, args=(d,)) for d in device_ids]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        for d in device_ids:
            st = svc.stats(d)
            self.assertEqual(st.accepted_count, per_device, d)
            self.assertEqual(st.rejected_count, 0, d)
            self.assertEqual(svc.snapshot_state(d), DeviceState.ONLINE, d)
        logger.info("[用例:多设备并发] %s 台设备各自 %s 条，全部一致",
                    len(device_ids), per_device)
