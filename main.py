"""设备接入与数字孪生服务的 HTTP 入口（内存实现，无外部依赖）。"""
from __future__ import annotations

import os
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from iot_twin.errors import RejectError
from iot_twin.history import HistoryStore, RetentionConfig
from iot_twin.ingest import IngestService
from iot_twin.models import TelemetryMessage
from iot_twin.registry import DeviceRegistry
from iot_twin.shadow import MISSING, ShadowStore

app = FastAPI(title="iot-telemetry-twin")

# 回放历史保留策略：有上限、可配置。可通过环境变量调整。
_retention = RetentionConfig(
    max_messages=int(os.environ.get("IOT_TWIN_HISTORY_MAX_MESSAGES", "1000")),
    max_age_seconds=(
        float(v)
        if (v := os.environ.get("IOT_TWIN_HISTORY_MAX_AGE_SECONDS"))
        else 3600.0
    ),
)

registry = DeviceRegistry()
shadows = ShadowStore()
history_store = HistoryStore(_retention)
ingest_service = IngestService(registry, shadows, history_store)


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


class BackfillMessage(BaseModel):
    message_id: str
    device_ts: float
    state: dict[str, Any] = Field(default_factory=dict)
    metrics: dict[str, float] = Field(default_factory=dict)


class BackfillRequest(BaseModel):
    credential: str
    # 设备本地记录的续传位置（上次已确认的最大 device_ts）。
    after_device_ts: float | None = None
    messages: list[BackfillMessage] = Field(default_factory=list)


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
        raise _reject_http(e)
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


@app.get("/devices/{device_id}/shadow/delta")
def get_desired_delta(device_id: str) -> dict[str, Any]:
    """重连对齐：返回期望里还没跟上报对上的字段（逐字段给出差异）。"""
    delta = shadows.desired_delta(device_id)
    return {
        "device_id": device_id,
        "desired_version": delta.desired_version,
        "pending": [
            {
                "field": key,
                "desired": wanted,
                "reported": None if actual is MISSING else actual,
                "ever_reported": actual is not MISSING,
            }
            for key, (wanted, actual) in sorted(delta.pending.items())
        ],
        "aligned": list(delta.aligned),
        "in_sync": not delta.pending,
    }


@app.get("/devices/{device_id}/resume")
def get_resume_cursor(device_id: str, credential: str) -> dict[str, Any]:
    """设备重连先问"我上次报到哪儿了"，拿游标接着报。"""
    try:
        cursor = ingest_service.resume_cursor(device_id, credential)
    except RejectError as e:
        raise _reject_http(e)
    return {
        "device_id": device_id,
        "last_device_ts": cursor.last_device_ts,
        "earliest_retained_ts": cursor.earliest_retained_ts,
        "retained_count": cursor.retained_count,
    }


@app.post("/devices/{device_id}/backfill")
def backfill_telemetry(device_id: str, req: BackfillRequest) -> dict[str, Any]:
    """断线期间攒下的一批消息一次补报：服务按设备时间排序、去重、重放。"""
    messages = [
        TelemetryMessage(
            device_id=device_id,
            message_id=m.message_id,
            device_ts=m.device_ts,
            state=m.state,
            metrics=m.metrics,
        )
        for m in req.messages
    ]
    try:
        result = ingest_service.backfill(
            device_id, req.credential, req.after_device_ts, messages
        )
    except RejectError as e:
        # 续传位置已超出保留范围：明确 409 resync_required，需要全量对齐。
        raise _reject_http(e)
    return {
        "device_id": device_id,
        "after_device_ts": result.after_device_ts,
        "total": result.total,
        "applied": result.applied,
        "duplicates": result.duplicates,
        "quota_rejected": result.quota_rejected,
        "items": [
            {
                "message_id": it.message_id,
                "device_ts": it.device_ts,
                "outcome": it.outcome.value,
                "reason": it.reason,
            }
            for it in result.items
        ],
    }


def _reject_http(e: RejectError) -> HTTPException:
    if e.reason.value == "resync_required":
        status = 409
    elif e.reason.value == "quota_exceeded":
        status = 429
    else:
        status = 403
    return HTTPException(
        status_code=status,
        detail={
            "reason": e.reason.value,
            "device_id": e.device_id,
            "message": str(e),
        },
    )


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
