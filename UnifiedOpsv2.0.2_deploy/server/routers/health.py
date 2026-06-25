"""
/api/health/* — REST view onto the live monitors.

Used by the SPA's WS-disconnected fallback and by smoke-test scripts;
the canonical channel for live updates is /ws/* (see routers/websocket.py).
"""
from __future__ import annotations

import time
from typing import Optional

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from services.alert_monitor   import AlertMonitor
from services.health_check    import HealthCheckMonitor

router = APIRouter(prefix="/api/health", tags=["health"])

_alert_monitor:  Optional[AlertMonitor]        = None
_health_monitor: Optional[HealthCheckMonitor]  = None
_alerts_hub_size  = lambda: 0
_health_hub_size  = lambda: 0


def configure(
    alert_monitor:  AlertMonitor,
    health_monitor: HealthCheckMonitor,
    alerts_hub_size,
    health_hub_size,
) -> None:
    global _alert_monitor, _health_monitor, _alerts_hub_size, _health_hub_size
    _alert_monitor    = alert_monitor
    _health_monitor   = health_monitor
    _alerts_hub_size  = alerts_hub_size
    _health_hub_size  = health_hub_size


@router.get("/listeners", include_in_schema=False)
async def listeners() -> JSONResponse:
    if _health_monitor is None:
        return JSONResponse({
            "ok": False, "error": "monitor not started",
            "listeners": [], "down_events": [], "infra_events": [],
        })
    return JSONResponse(_health_monitor.snapshot())


@router.get("/pipeline", include_in_schema=False)
async def pipeline() -> JSONResponse:
    if _alert_monitor is None:
        return JSONResponse({"ok": False, "error": "monitor not started"})
    snap = _alert_monitor.snapshot(limit=0)
    return JSONResponse({
        "ok":            snap["ok"],
        "as_of":         snap["as_of"],
        "buckets_total": snap["buckets_total"],
        "buckets_ok":    snap["buckets_ok"],
        "bucket_status": snap["bucket_status"],
    })


@router.get("/websocket", include_in_schema=False)
async def websocket_health() -> JSONResponse:
    return JSONResponse({
        "ok": True,
        "ts": time.time(),
        "alerts":          {"connections": _alerts_hub_size()},
        "listener_health": {"connections": _health_hub_size()},
    })
