"""设备接入与数字孪生服务的 HTTP 入口（内存实现，无外部依赖）。"""
from __future__ import annotations

from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from iot_twin.errors import RejectError
from iot_twin.ingest import IngestService
from iot_twin.models import TelemetryMessage
from iot_twin.registry import DeviceRegistry
from iot_twin.shadow import ShadowStore

app = FastAPI(title="iot-telemetry-twin")

registry = DeviceRegistry()
shadows = ShadowStore()
ingest_service = IngestService(registry, shadows)


class RegisterRequest(BaseModel):
    device_id: str
    credential: str
    quota: int = 1000


class TelemetryRequest(BaseModel):
    device_id: str
    credential: str
    message_id: str
    device_ts: float
    state: dict[str, Any] = Field(default_factory=dict)
    metrics: dict[str, float] = Field(default_factory=dict)


class DesiredRequest(BaseModel):
    desired: dict[str, Any]


@app.post("/devices", status_code=201)
def register_device(req: RegisterRequest) -> dict[str, Any]:
    registry.register(req.device_id, req.credential, req.quota)
    return {"device_id": req.device_id, "quota": req.quota}


@app.post("/devices/{device_id}/disable")
def disable_device(device_id: str) -> dict[str, str]:
    try:
        registry.disable(device_id)
    except RejectError as e:
        raise HTTPException(status_code=404, detail=e.reason.value)
    return {"device_id": device_id, "status": "disabled"}


@app.post("/telemetry")
def report_telemetry(req: TelemetryRequest) -> dict[str, Any]:
    msg = TelemetryMessage(
        device_id=req.device_id,
        message_id=req.message_id,
        device_ts=req.device_ts,
        state=req.state,
        metrics=req.metrics,
    )
    try:
        result = ingest_service.ingest(req.device_id, req.credential, msg)
    except RejectError as e:
        # 失败原因可区分：unknown_device / bad_credential /
        # device_disabled / quota_exceeded
        status = 403 if e.reason.value != "quota_exceeded" else 429
        raise HTTPException(
            status_code=status,
            detail={"reason": e.reason.value, "device_id": e.device_id},
        )
    return {
        "outcome": result.outcome.value,
        "message_id": result.message_id,
        "reason": result.reason,
    }


@app.put("/devices/{device_id}/shadow/desired")
def set_desired(device_id: str, req: DesiredRequest) -> dict[str, Any]:
    version = shadows.set_desired(device_id, req.desired)
    return {"device_id": device_id, "desired_version": version}


@app.get("/devices/{device_id}/shadow")
def get_shadow(device_id: str) -> dict[str, Any]:
    shadow = shadows.get_or_create(device_id)
    return {
        "device_id": device_id,
        "desired": shadow.desired,
        "desired_version": shadow.desired_version,
        "reported": shadow.reported,
        "reported_version": shadow.reported_version,
        "last_device_ts": shadow.last_device_ts,
    }


@app.get("/devices/{device_id}/stats")
def get_stats(device_id: str) -> dict[str, Any]:
    stats = ingest_service.stats(device_id)
    return {
        "device_id": device_id,
        "accepted_messages": stats.accepted_messages,
        "duplicate_messages": stats.duplicate_messages,
        "stale_messages": stats.stale_messages,
        "metric_sums": stats.metric_sums,
        "metric_averages": stats.averages(),
    }


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
