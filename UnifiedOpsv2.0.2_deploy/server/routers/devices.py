"""
/api/devices/* - device inventory + per-vendor live counts.

These endpoints power the System Health Overview OEM cards:
    Total devices: <inventory>
    Alert devices: <unique array_names with alerts in range>

`/inventory` is the static map (no range filter needed); `/snapshot`
takes a range query and adds the alerting counts on top.
"""
from __future__ import annotations

import logging
from typing import List, Optional

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from services.device_inventory import DeviceService

log = logging.getLogger("unifiedops.routers.devices")
router = APIRouter(prefix="/api/devices", tags=["devices"])

_service: Optional[DeviceService] = None


def configure(service: DeviceService) -> None:
    global _service
    _service = service


def _parse_csv(raw: Optional[str]) -> Optional[List[str]]:
    if not raw:
        return None
    out = [s.strip() for s in raw.split(",") if s.strip()]
    return out or None


@router.get("/inventory", include_in_schema=False)
async def inventory() -> JSONResponse:
    if _service is None:
        return JSONResponse({"error": "service not started"}, status_code=503)
    return JSONResponse({
        "inventory": _service.inventory(),
        "totals":    _service.total_counts(),
    })


@router.get("/snapshot", include_in_schema=False)
async def snapshot(
    range:   str = Query("6h"),
    sites:   Optional[str] = Query(None),
    vendors: Optional[str] = Query(None),
) -> JSONResponse:
    if _service is None:
        return JSONResponse({"error": "service not started"}, status_code=503)
    snap = await _service.get_snapshot(range, _parse_csv(sites), _parse_csv(vendors))
    return JSONResponse(snap)
