"""设备接入与数字孪生服务的 HTTP 入口（内存实现，无外部依赖）。"""
from __future__ import annotations

import os
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from iot_twin.errors import RejectError
from iot_twin.history import HistoryConfig, HistoryStore
from iot_twin.ingest import IngestService
from iot_twin.models import TelemetryMessage
from iot_twin.registry import DeviceRegistry
from iot_twin.shadow import ShadowStore

app = FastAPI(title="iot-telemetry-twin")

# 回放历史保留上限可用环境变量调整：
#   IOT_TWIN_HISTORY_MAX_MESSAGES   每台设备最多保留条数（默认 1000）
#   IOT_TWIN_HISTORY_MAX_AGE_SECONDS 每条最多保留秒数（默认 3600）
history_config = HistoryConfig(
    max_messages=int(os.environ.get("IOT_TWIN_HISTORY_MAX_MESSAGES", "1000")),
    max_age_seconds=float(
        os.environ.get("IOT_TWIN_HISTORY_MAX_AGE_SECONDS", "3600")
    ),
)

registry = DeviceRegistry()
shadows = ShadowStore()
histories = HistoryStore(history_config)
ingest_service = IngestService(registry, shadows, histories)


def reset_state() -> None:
    """清空全部内存状态并重建服务（供测试与本地自测使用）。"""
    global registry, shadows, histories, ingest_service
    registry = DeviceRegistry()
    shadows = ShadowStore()
    histories = HistoryStore(history_config)
    ingest_service = IngestService(registry, shadows, histories)


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


class BackfillMessage(BaseModel):
    message_id: str
    device_ts: float
    state: dict[str, Any] = Field(default_factory=dict)
    metrics: dict[str, float] = Field(default_factory=dict)


class BackfillRequest(BaseModel):
    device_id: str
    credential: str
    messages: list[BackfillMessage]


class DesiredRequest(BaseModel):
    desired: dict[str, Any]


def _reject_http(e: RejectError) -> HTTPException:
    # 失败原因可区分：unknown_device / bad_credential /
    # device_disabled / quota_exceeded
    status = 403 if e.reason.value != "quota_exceeded" else 429
    return HTTPException(
        status_code=status,
        detail={"reason": e.reason.value, "device_id": e.device_id},
    )


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
        raise _reject_http(e)
    return {
        "outcome": result.outcome.value,
        "message_id": result.message_id,
        "reason": result.reason,
    }


@app.post("/telemetry/backfill")
def backfill_telemetry(req: BackfillRequest) -> dict[str, Any]:
    """断线重连后的批量补报：服务按设备侧时间理顺、去重、回放。"""
    messages = [
        TelemetryMessage(
            device_id=req.device_id,
            message_id=m.message_id,
            device_ts=m.device_ts,
            state=m.state,
            metrics=m.metrics,
        )
        for m in req.messages
    ]
    try:
        result = ingest_service.backfill(req.device_id, req.credential, messages)
    except RejectError as e:
        raise _reject_http(e)
    return {
        "device_id": result.device_id,
        "resync_required": result.resync_required,
        "results": [
            {
                "message_id": r.message_id,
                "outcome": r.outcome.value,
                "reason": r.reason,
            }
            for r in result.results
        ],
    }


@app.get("/devices/{device_id}/resume")
def get_resume_position(device_id: str, credential: str) -> dict[str, Any]:
    """设备重连后查询续传位置：“我上次报到哪儿了”。"""
    try:
        info = ingest_service.resume(device_id, credential)
    except RejectError as e:
        raise _reject_http(e)
    return {
        "device_id": info.device_id,
        "last_device_ts": info.last_device_ts,
        "replay_horizon": info.replay_horizon,
        "earliest_retained_ts": info.earliest_retained_ts,
        "retained_count": info.retained_count,
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


@app.get("/devices/{device_id}/shadow/delta")
def get_shadow_delta(device_id: str) -> dict[str, Any]:
    """字段级期望差异：期望里还有哪些没跟上报对上。"""
    delta = shadows.delta(device_id)
    return {
        "device_id": delta.device_id,
        "in_sync": delta.in_sync,
        "desired_version": delta.desired_version,
        "reported_version": delta.reported_version,
        "fields": [
            {
                "field": f.field,
                "desired_value": f.desired_value,
                "reported_value": f.reported_value,
                "reported_present": f.reported_present,
            }
            for f in delta.fields
        ],
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
