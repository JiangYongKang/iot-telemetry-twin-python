"""HTTP API：设备注册、遥测上报、状态/统计查询、设备影子。

错误响应统一为 {"error": {"code", "message"}}，code 稳定、原因可区分：
  device_not_found(404) / invalid_credentials(401) / device_disabled(403) /
  quota_exceeded(429) / invalid_message(400) / desired_version_conflict(409) /
  desired_too_large(413)
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.errors import ErrorCode, ServiceError
from app.models import TelemetryMessage
from app.service import TelemetryService
from app.shadow import ShadowService
from app.store import InMemoryStore
from pydantic import BaseModel, Field


# ---- 请求/响应模型 --------------------------------------------------------


class RegisterRequest(BaseModel):
    device_id: str = Field(min_length=1)
    credential: str = Field(min_length=1)
    enabled: bool = True
    quota: int = 0  # 0 表示不限制


class TelemetryRequest(BaseModel):
    credential: str = Field(min_length=1)
    msg_id: str = Field(min_length=1)
    device_ts_ms: int = Field(ge=0)
    payload: dict[str, Any] = Field(default_factory=dict)
    reported: dict[str, Any] = Field(default_factory=dict)


class DesiredRequest(BaseModel):
    patch: dict[str, Any]
    expected_version: int | None = None


# ---- 应用工厂 -------------------------------------------------------------


def create_app(
    service: TelemetryService | None = None,
    shadows: ShadowService | None = None,
    store: InMemoryStore | None = None,
) -> FastAPI:
    store = store or InMemoryStore()
    shadows = shadows or ShadowService(store)
    service = service or TelemetryService(store, shadows)

    app = FastAPI(title="iot-telemetry-twin", version="0.1.0")

    def _error(code: ErrorCode, message: str, status_code: int) -> JSONResponse:
        return JSONResponse(
            status_code=status_code,
            content={"error": {"code": code.value, "message": message}},
        )

    @app.exception_handler(ServiceError)
    async def _on_service_error(_: Request, exc: ServiceError) -> JSONResponse:
        return _error(exc.code, exc.message, exc.status_code)

    @app.exception_handler(RequestValidationError)
    async def _on_validation_error(
        _: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return _error(
            ErrorCode.INVALID_MESSAGE, f"请求参数不合法: {exc.errors()}", 400
        )

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/devices", status_code=201)
    async def register(req: RegisterRequest) -> dict[str, Any]:
        device = service.register_device(
            req.device_id,
            req.credential,
            enabled=req.enabled,
            quota=req.quota,
        )
        return {
            "device_id": device.device_id,
            "enabled": device.enabled,
            "quota": device.quota,
        }

    @app.post("/devices/{device_id}/telemetry")
    async def telemetry(device_id: str, req: TelemetryRequest) -> dict[str, Any]:
        result = service.ingest(
            TelemetryMessage(
                device_id=device_id,
                credential=req.credential,
                msg_id=req.msg_id,
                device_ts_ms=req.device_ts_ms,
                payload=req.payload,
                reported=req.reported,
            )
        )
        return {
            "outcome": result.outcome.value,
            "device_id": result.device_id,
            "msg_id": result.msg_id,
            "device_ts_ms": result.device_ts_ms,
            "reason": result.reason,
            "count_after": result.count_after,
        }

    @app.get("/devices/{device_id}/stats")
    async def stats(device_id: str) -> dict[str, Any]:
        st = service.stats(device_id)
        return {
            "device_id": st.device_id,
            "accepted_count": st.accepted_count,
            "duplicate_count": st.duplicate_count,
            "stale_count": st.stale_count,
            "rejected_count": st.rejected_count,
            "last_device_ts_ms": st.last_device_ts_ms,
            "last_msg_id": st.last_msg_id,
        }

    @app.get("/devices/{device_id}/state")
    async def state(device_id: str) -> dict[str, Any]:
        return {"device_id": device_id, "state": service.snapshot_state(device_id).value}

    @app.get("/devices/{device_id}/shadow")
    async def get_shadow(device_id: str) -> dict[str, Any]:
        sh = shadows.get_shadow(device_id)
        return {
            "device_id": sh.device_id,
            "desired": sh.desired,
            "desired_version": sh.desired_version,
            "reported": sh.reported,
            "reported_ts_ms": sh.reported_ts_ms,
            "delta": sh.delta,
            "last_applied_msg_id": sh.last_applied_msg_id,
        }

    @app.put("/devices/{device_id}/shadow/desired")
    async def put_desired(device_id: str, req: DesiredRequest) -> dict[str, Any]:
        sh = shadows.update_desired(device_id, req.patch, req.expected_version)
        return {
            "device_id": sh.device_id,
            "desired": sh.desired,
            "desired_version": sh.desired_version,
            "delta": sh.delta,
        }

    @app.get("/devices/{device_id}/shadow/delta")
    async def sync_delta(device_id: str) -> dict[str, Any]:
        return {"device_id": device_id, "delta": shadows.sync_delta(device_id)}

    return app
