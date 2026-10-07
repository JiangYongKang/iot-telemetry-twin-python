"""HTTP 层端到端验证：本机内存链路，不依赖外部服务。

每个用例前重置内存状态：同一进程里连跑几轮，前一轮不留影响。
"""
from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

import main
from main import app

log = logging.getLogger("test_api")
client = TestClient(app)


@pytest.fixture(autouse=True)
def _fresh_state() -> None:
    main.reset_state()


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


def test_reconnect_flow_over_http() -> None:
    """重连全链路：离线改期望 -> 查差异 -> 问续传位置 -> 补报 -> 差异消失。"""
    assert client.post("/devices", json={
        "device_id": "api-2", "credential": "local-test-only", "quota": 100,
    }).status_code == 201

    # 在线上报两条（ts 1、2）
    for i in (1, 2):
        r = client.post("/telemetry", json={
            "device_id": "api-2", "credential": "local-test-only",
            "message_id": f"live-{i}", "device_ts": float(i),
            "state": {"seq": i}, "metrics": {"v": 1.0},
        })
        assert r.json()["outcome"] == "applied"

    # 设备离线期间，期望状态被改了好几次
    client.put("/devices/api-2/shadow/desired",
               json={"desired": {"interval": 30, "mode": "eco"}})
    client.put("/devices/api-2/shadow/desired", json={"desired": {"interval": 60}})

    # 重连：先问“我上次报到哪儿了”
    r = client.get("/devices/api-2/resume",
                   params={"credential": "local-test-only"})
    info = r.json()
    log.info("续传位置 -> %s", info)
    assert info["last_device_ts"] == 2.0
    assert info["replay_horizon"] is None  # 从未回收，任意早都能补

    # 查期望差异：两个字段都没对齐，能看清每个字段的期望值
    delta = client.get("/devices/api-2/shadow/delta").json()
    log.info("补报前差异 -> %s", delta)
    assert not delta["in_sync"]
    by_field = {f["field"]: f for f in delta["fields"]}
    assert set(by_field) == {"interval", "mode"}
    assert by_field["interval"]["desired_value"] == 60  # 以最新期望为准

    # 补报断线期间攒的消息：乱序、含已送达的 live-2、含批次内重复
    r = client.post("/telemetry/backfill", json={
        "device_id": "api-2", "credential": "local-test-only",
        "messages": [
            {"message_id": "buf-4", "device_ts": 4.0,
             "state": {"seq": 4, "mode": "eco"}, "metrics": {"v": 1.0}},
            {"message_id": "buf-3", "device_ts": 3.0,
             "state": {"seq": 3, "interval": 60}, "metrics": {"v": 1.0}},
            {"message_id": "live-2", "device_ts": 2.0, "state": {"seq": 2}},
            {"message_id": "buf-3", "device_ts": 3.0,
             "state": {"seq": 3, "interval": 60}, "metrics": {"v": 1.0}},
        ],
    })
    body = r.json()
    log.info("补报结果 -> %s", body)
    assert not body["resync_required"]
    outcomes = [x["outcome"] for x in body["results"]]
    assert outcomes.count("applied") == 2      # buf-3、buf-4
    assert outcomes.count("duplicate") == 2    # live-2、批次内重复的 buf-3

    # 差异随上报对齐而消失
    delta = client.get("/devices/api-2/shadow/delta").json()
    log.info("补报后差异 -> %s", delta)
    assert delta["in_sync"]

    stats = client.get("/devices/api-2/stats").json()
    log.info("统计 -> %s", stats)
    assert stats["accepted_messages"] == 4     # live-1/2 + buf-3/4
    assert stats["duplicate_messages"] == 2
    assert stats["metric_averages"] == {"v": 1.0}
