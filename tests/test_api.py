"""HTTP 层端到端验证：本机内存链路，不依赖外部服务。"""
from __future__ import annotations

import logging

from fastapi.testclient import TestClient

from main import app

log = logging.getLogger("test_api")
client = TestClient(app)


def test_full_ingest_flow_over_http() -> None:
    assert client.post("/devices", json={
        "device_id": "api-1", "credential": "local-test-only", "quota": 5,
    }).status_code == 201

    payload = {
        "device_id": "api-1", "credential": "local-test-only",
        "message_id": "api-m1", "device_ts": 1000.0,
        "state": {"on": True}, "metrics": {"temp": 22.0},
    }
    r = client.post("/telemetry", json=payload)
    log.info("首次上报 -> %s", r.json())
    assert r.status_code == 200 and r.json()["outcome"] == "applied"

    r = client.post("/telemetry", json=payload)  # 重复投递
    log.info("重复上报 -> %s", r.json())
    assert r.json()["outcome"] == "duplicate"

    stale = dict(payload, message_id="api-m0", device_ts=500.0)
    r = client.post("/telemetry", json=stale)  # 乱序旧数据
    log.info("乱序上报 -> %s", r.json())
    assert r.json()["outcome"] == "stale"

    bad = dict(payload, credential="wrong", message_id="api-m2", device_ts=1001.0)
    r = client.post("/telemetry", json=bad)
    log.info("错误凭证 -> %s %s", r.status_code, r.json())
    assert r.status_code == 403 and r.json()["detail"]["reason"] == "bad_credential"

    shadow = client.get("/devices/api-1/shadow").json()
    log.info("影子 -> %s", shadow)
    assert shadow["reported"] == {"on": True}
    assert shadow["last_device_ts"] == 1000.0

    stats = client.get("/devices/api-1/stats").json()
    log.info("统计 -> %s", stats)
    assert stats["accepted_messages"] == 1
    assert stats["duplicate_messages"] == 1
    assert stats["stale_messages"] == 1
    assert stats["metric_averages"] == {"temp": 22.0}
