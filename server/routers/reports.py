from __future__ import annotations

import logging
from typing import List, Optional

from fastapi import APIRouter, Query, Response
from services.reports import ReportService

log = logging.getLogger("unifiedops.router.reports")
router = APIRouter(prefix="/api/reports", tags=["Reports"])

_svc: Optional[ReportService] = None

def configure(svc: ReportService) -> None:
    global _svc
    _svc = svc

@router.get("/download")
async def download_report(
    range: str = Query("1d", description="Time range, e.g. 6h, 1d, 7d"),
    site: Optional[List[str]] = Query(None, description="Filter by sites (e.g. CDVL, BCP)"),
    vendor: Optional[List[str]] = Query(None, description="Filter by vendor (e.g. hitachi, brocade)"),
    format: str = Query("csv", description="Format: csv, xlsx, pdf"),
    report_type: str = Query("hardware", description="hardware or health_check"),
) -> Response:
    if _svc is None:
        return Response(status_code=503, content="ReportService not configured")
        
    content, media_type, ext = await _svc.get_multi_format_report(
        range_key=range,
        sites=site,
        vendors=vendor,
        fmt=format,
        report_type=report_type,
    )
    
    import datetime
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    name_prefix = "health_check_report" if report_type == "health_check" else "hardware_alerts"
    filename = f"unifiedops_{name_prefix}_{range}_{timestamp}.{ext}"
    
    return Response(
        content=content,
        media_type=media_type,
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        }
    )
