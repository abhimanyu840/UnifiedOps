"""
/api/dashboard/* — REST aggregator endpoints consumed by the React UI.

Every card on the dashboard has a dedicated endpoint plus there's a
composite `/api/dashboard/snapshot` that returns the whole payload in
one round-trip (used on first paint + after every WS-driven refresh).
"""
from __future__ import annotations

import logging
from typing import List, Optional

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from services.dashboard import DashboardService

log = logging.getLogger("unifiedops.routers.dashboard")
router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])

# Set at app startup by `server.py`. Kept module-global because FastAPI
# `Depends` injection would clutter every route signature for what is
# effectively a singleton.
_service: Optional[DashboardService] = None


def configure(service: DashboardService) -> None:
    global _service
    _service = service


def _parse_csv(raw: Optional[str]) -> Optional[List[str]]:
    if not raw:
        return None
    out = [s.strip() for s in raw.split(",") if s.strip()]
    return out or None


@router.get("/snapshot", include_in_schema=False)
async def snapshot(
    range:   str = Query("6h",  description="Range key — 5m..30d"),
    sites:   Optional[str] = Query(None, description="CSV of site codes (CDVL,BCP,SIFY)"),
    vendors: Optional[str] = Query(None, description="CSV of vendors (hitachi,brocade,netapp,dell)"),
) -> JSONResponse:
    if _service is None:
        return JSONResponse({"error": "service not started"}, status_code=503)
    snap = await _service.get_snapshot(range, _parse_csv(sites), _parse_csv(vendors))
    return JSONResponse(snap)


@router.get("/total", include_in_schema=False)
async def total(
    range:   str = Query("6h"),
    sites:   Optional[str] = Query(None),
    vendors: Optional[str] = Query(None),
) -> JSONResponse:
    if _service is None:
        return JSONResponse({"error": "service not started"}, status_code=503)
    n = await _service.get_total(range, _parse_csv(sites), _parse_csv(vendors))
    return JSONResponse({"range": range, "total": n})


@router.get("/severity", include_in_schema=False)
async def severity(
    range:   str = Query("6h"),
    sites:   Optional[str] = Query(None),
    vendors: Optional[str] = Query(None),
) -> JSONResponse:
    if _service is None:
        return JSONResponse({"error": "service not started"}, status_code=503)
    s = await _service.get_severity(range, _parse_csv(sites), _parse_csv(vendors))
    return JSONResponse({"range": range, "severity": s})


@router.get("/categories", include_in_schema=False)
async def categories(
    range:   str = Query("6h"),
    sites:   Optional[str] = Query(None),
    vendors: Optional[str] = Query(None),
) -> JSONResponse:
    if _service is None:
        return JSONResponse({"error": "service not started"}, status_code=503)
    c = await _service.get_categories(range, _parse_csv(sites), _parse_csv(vendors))
    return JSONResponse({"range": range, "categories": c})


@router.get("/top-systems", include_in_schema=False)
async def top_systems(
    range:   str = Query("6h"),
    sites:   Optional[str] = Query(None),
    vendors: Optional[str] = Query(None),
    limit:   int = Query(50, ge=1, le=200),
) -> JSONResponse:
    if _service is None:
        return JSONResponse({"error": "service not started"}, status_code=503)
    rows = await _service.get_top_systems(range, _parse_csv(sites), _parse_csv(vendors), limit)
    return JSONResponse({"range": range, "topSystems": rows})


@router.get("/trend", include_in_schema=False)
async def trend(
    range:   str = Query("6h"),
    sites:   Optional[str] = Query(None),
    vendors: Optional[str] = Query(None),
    buckets: int = Query(25, ge=4, le=120, alias="target"),
) -> JSONResponse:
    if _service is None:
        return JSONResponse({"error": "service not started"}, status_code=503)
    rows = await _service.get_trend(range, _parse_csv(sites), _parse_csv(vendors), buckets)
    return JSONResponse({"range": range, "trend": rows})


@router.get("/recent", include_in_schema=False)
async def recent(
    range:   str = Query("6h"),
    sites:   Optional[str] = Query(None),
    vendors: Optional[str] = Query(None),
    limit:   int = Query(200, ge=1, le=2000),
) -> JSONResponse:
    if _service is None:
        return JSONResponse({"error": "service not started"}, status_code=503)
    rows = await _service.get_recent(range, _parse_csv(sites), _parse_csv(vendors), limit)
    return JSONResponse({"range": range, "recent": rows})
