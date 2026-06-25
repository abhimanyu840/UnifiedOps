"""
/api/alerts/* — REST view onto the AlertMonitor rolling buffer.

The same data is pushed live over /ws/alerts; the REST endpoint exists
as a fallback for clients that can't keep a WebSocket open.
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from services.alert_monitor import AlertMonitor

router = APIRouter(prefix="/api/alerts", tags=["alerts"])

_alert_monitor: Optional[AlertMonitor] = None


def configure(alert_monitor: AlertMonitor) -> None:
    global _alert_monitor
    _alert_monitor = alert_monitor


@router.get("/recent", include_in_schema=False)
async def recent(limit: int = Query(200, ge=1, le=2000)) -> JSONResponse:
    if _alert_monitor is None:
        return JSONResponse({"ok": False, "error": "monitor not started", "alerts": []})
    return JSONResponse(_alert_monitor.snapshot(limit=limit))
