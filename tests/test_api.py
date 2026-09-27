"""HTTP API 测试：错误响应码可区分，以及经 HTTP 并发上报的计数一致性。

使用 FastAPI TestClient + 线程池，无外部服务/真实凭据。
"""

from __future__ import annotations

import logging
import unittest
from concurrent.futures import ThreadPoolExecutor

from fastapi.testclient import TestClient

from app.api import create_app

logger = logging.getLogger("test.api")


class ApiErrorMappingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(create_app())
        self.client.post("/devices", json={"device_id": "d1", "credential": "k", "quota": 1})
        self.client.post("/devices", json={"device_id": "d2", "credential": "k2", "enabled": False})

    def test_distinguishable_failure_codes(self):
        cases = [
            ("/devices/ghost/telemetry", {"credential": "x", "msg_id": "m", "device_ts_ms": 1},
             404, "device_not_found"),
            ("/devices/d1/telemetry", {"credential": "bad", "msg_id": "m", "device_ts_ms": 1},
             401, "invalid_credentials"),
            ("/devices/d2/telemetry", {"credential": "k2", "msg_id": "m", "device_ts_ms": 1},
             403, "device_disabled"),
            ("/devices/d1/telemetry", {"credential": "k", "msg_id": "", "device_ts_ms": 1},
             400, "invalid_message"),
        ]
        for path, body, expect_status, expect_code in cases:
            with self.subTest(code=expect_code):
                r = self.client.post(path, json=body)
                logger.info("[用例:HTTP错误区分] 输入 path=%s body.msg_id=%r -> status=%s",
                            path, body.get("msg_id"), r.status_code)
                self.assertEqual(r.status_code, expect_status)
                self.assertEqual(r.json()["error"]["code"], expect_code)

    def test_full_flow_duplicate_out_of_order_quota(self):
        c = self.client
        # 第一条生效（配额 1）
        r = c.post("/devices/d1/telemetry",
                   json={"credential": "k", "msg_id": "m1", "device_ts_ms": 100,
                         "reported": {"state": "online"}})
        self.assertEqual(r.json()["outcome"], "accepted")
        # 重复
        r = c.post("/devices/d1/telemetry",
                   json={"credential": "k", "msg_id": "m1", "device_ts_ms": 100})
        self.assertEqual(r.json()["outcome"], "duplicate")
        # 乱序
        r = c.post("/devices/d1/telemetry",
                   json={"credential": "k", "msg_id": "m-old", "device_ts_ms": 50})
        self.assertEqual(r.json()["outcome"], "stale")
        # 超额：错误码而非通用失败
        r = c.post("/devices/d1/telemetry",
                   json={"credential": "k", "msg_id": "m2", "device_ts_ms": 200})
        self.assertEqual(r.status_code, 429)
        self.assertEqual(r.json()["error"]["code"], "quota_exceeded")
        stats = c.get("/devices/d1/stats").json()
        logger.info("[用例:HTTP全链路] stats=%s", stats)
        self.assertEqual(stats["accepted_count"], 1)
        self.assertEqual(stats["duplicate_count"], 1)
        self.assertEqual(stats["stale_count"], 1)
        self.assertEqual(c.get("/devices/d1/state").json()["state"], "online")

    def test_shadow_endpoints_and_conflict(self):
        c = self.client
        c.post("/devices", json={"device_id": "d3", "credential": "k"})
        r = c.put("/devices/d3/shadow/desired", json={"patch": {"led": "on"}})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["delta"], {"led": "on"})
        self.assertEqual(r.json()["desired_version"], 1)
        r = c.put("/devices/d3/shadow/desired",
                  json={"patch": {"led": "off"}, "expected_version": 99})
        self.assertEqual(r.status_code, 409)
        self.assertEqual(r.json()["error"]["code"], "desired_version_conflict")
        # 重连增量拉取
        self.assertEqual(c.get("/devices/d3/shadow/delta").json()["delta"], {"led": "on"})


class HttpConcurrencyTest(unittest.TestCase):
    def test_concurrent_http_posts_counts_are_consistent(self):
        client = TestClient(create_app())
        client.post("/devices", json={"device_id": "dc", "credential": "k", "quota": 0})

        n = 100

        def post(i: int):
            # msg_id 唯一；时间戳随提交序号递增，但线程实际执行顺序任意，
            # 因此部分消息会依据设备时间戳被判 stale（系统预期行为）。
            body = {"credential": "k", "msg_id": f"m{i}", "device_ts_ms": 1000 + i}
            r = client.post("/devices/dc/telemetry", json=body)
            return r.json()

        with ThreadPoolExecutor(max_workers=16) as pool:
            results = list(pool.map(post, range(n)))
        stats = client.get("/devices/dc/stats").json()
        n_accept = sum(1 for r in results if r["outcome"] == "accepted")
        n_stale = sum(1 for r in results if r["outcome"] == "stale")
        logger.info("[用例:HTTP并发] accepted=%s stale=%s stats=%s",
                    n_accept, n_stale, stats)
        # 没有任何拒绝；每条消息要么生效要么按新旧规则判旧，结论守恒
        self.assertEqual(n_accept + n_stale, n)
        self.assertEqual(stats["accepted_count"], n_accept)
        self.assertEqual(stats["accepted_count"] + stats["stale_count"], n)
        self.assertEqual(stats["rejected_count"], 0)
        # 生效的计数必须严格连续 1..n_accept（无丢失/无半截写入）
        counts = sorted(r["count_after"] for r in results if r["outcome"] == "accepted")
        self.assertEqual(counts, list(range(1, n_accept + 1)))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s | %(message)s")
    unittest.main(verbosity=2)
