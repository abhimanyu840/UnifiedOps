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
        
    from services.influx_pool import InfluxQueryError

    try:
        sites_list = []
        if site:
            for s in site:
                sites_list.extend([x.strip() for x in s.split(",") if x.strip()])
        else:
            sites_list = None
            
        vendors_list = []
        if vendor:
            for v in vendor:
                vendors_list.extend([x.strip() for x in v.split(",") if x.strip()])
        else:
            vendors_list = None
        
        content, media_type, ext = await _svc.get_multi_format_report(
            range_key=range,
            sites=sites_list,
            vendors=vendors_list,
            fmt=format,
            report_type=report_type,
        )
        import os
        print(f"DEBUG ENV CDVL_REPORT_URL: {os.environ.get('HITRACK_INFLUX_BROCADE_CDVL_REPORT_URL')}", flush=True)
        print(f"DEBUG ENV BCP_REPORT_URL: {os.environ.get('HITRACK_INFLUX_BROCADE_BCP_REPORT_URL')}", flush=True)

    except InfluxQueryError as exc:
        return Response(status_code=503, content=f"InfluxDB Connection Error: {exc.reason}")
    except Exception as exc:
        return Response(status_code=500, content=f"Report Generation Error: {exc}")
    
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
