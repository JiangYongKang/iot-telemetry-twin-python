"""HTTP 端到端：续传游标、批量补报、期望差异、越界 409 全量对齐提示。"""
from __future__ import annotations

import logging
import uuid

from fastapi.testclient import TestClient

from main import app, history_store

log = logging.getLogger("test_reconnect_api")
client = TestClient(app)

H = history_store._config.max_messages


def _dev() -> str:
    return f"api-{uuid.uuid4().hex[:8]}"


def test_resume_backfill_delta_flow_over_http() -> None:
    dev = _dev()
    assert client.post("/devices", json={
        "device_id": dev, "credential": "c", "quota": 100,
    }).status_code == 201

    # 先在线上报一条，随后掉线。
    assert client.post("/telemetry", json={
        "device_id": dev, "credential": "c", "message_id": "live",
        "device_ts": 50.0, "state": {"live": True},
    }).json()["outcome"] == "applied"

    # 重连先问"我上次报到哪儿了"。
    r = client.get(f"/devices/{dev}/resume", params={"credential": "c"})
    cursor = r.json()
    log.info("游标 -> %s", cursor)
    assert r.status_code == 200
    assert cursor["last_device_ts"] == 50.0
    assert cursor["earliest_retained_ts"] == 50.0
    assert cursor["retained_count"] == 1

    # 离线期间期望改过几次；只补差异字段即可。
    client.put(f"/devices/{dev}/shadow/desired", json={"desired": {"mode": "eco"}})
    client.put(f"/devices/{dev}/shadow/desired", json={"desired": {"mode": "auto"}})
    delta = client.get(f"/devices/{dev}/shadow/delta").json()
    log.info("差异（最新期望）-> %s", delta)
    assert [p["field"] for p in delta["pending"]] == ["mode"]
    assert delta["pending"][0]["desired"] == "auto"
    assert delta["pending"][0]["ever_reported"] is False
    assert delta["in_sync"] is False

    # 断线期间攒的更早消息乱序一次补，夹一条完全重复投递。
    r = client.post(f"/devices/{dev}/backfill", json={
        "credential": "c", "after_device_ts": 50.0,
        "messages": [
            {"message_id": "lost-3", "device_ts": 30.0, "state": {"rssi": -70}},
            {"message_id": "live", "device_ts": 50.0, "state": {"live": True}},
            {"message_id": "lost-1", "device_ts": 10.0, "state": {"fw": "1"}},
            {"message_id": "lost-2", "device_ts": 20.0, "state": {"mode": "auto"}},
        ],
    }).json()
    log.info("补报 -> applied=%d duplicates=%d items=%s",
             r["applied"], r["duplicates"],
             [(i["device_ts"], i["outcome"]) for i in r["items"]])
    assert r["applied"] == 3
    assert r["duplicates"] == 1
    # 服务按 device_ts 理顺，mode 的最终值来自 ts=20 的 "auto"，差异消失。
    assert [i["message_id"] for i in r["items"]] == [
        "lost-1", "lost-2", "lost-3", "live",
    ]
    shadow = client.get(f"/devices/{dev}/shadow").json()
    assert shadow["reported"] == {
        "live": True, "fw": "1", "mode": "auto", "rssi": -70,
    }

    delta = client.get(f"/devices/{dev}/shadow/delta").json()
    assert delta["in_sync"] is True and delta["pending"] == []
    assert "mode" in delta["aligned"]


def test_backfill_beyond_retention_returns_409() -> None:
    dev = _dev()
    client.post("/devices", json={"device_id": dev, "credential": "c",
                                  "quota": H + 100})
    # 填满历史把最早记录挤出保留范围。
    for i in range(1, H + 5):
        client.post("/telemetry", json={
            "device_id": dev, "credential": "c",
            "message_id": f"m{i}", "device_ts": float(i),
        })
    floor = client.get(f"/devices/{dev}/resume",
                       params={"credential": "c"}).json()["earliest_retained_ts"]
    log.info("保留下界 floor=%s", floor)

    # 设备拿着更早的位置回来补 -> 明确 409，要求全量对齐。
    r = client.post(f"/devices/{dev}/backfill", json={
        "credential": "c", "after_device_ts": floor - 1,
        "messages": [{"message_id": "ancient", "device_ts": 0.1}],
    })
    log.info("越界补报 -> %s %s", r.status_code, r.json())
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "resync_required"


def test_resume_requires_credential() -> None:
    dev = _dev()
    client.post("/devices", json={"device_id": dev, "credential": "c"})
    r = client.get(f"/devices/{dev}/resume", params={"credential": "wrong"})
    assert r.status_code == 403
    assert r.json()["detail"]["reason"] == "bad_credential"
