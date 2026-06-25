"""
/ws/* — WebSocket endpoints for live push.

Three channels:
  /ws/alerts           — every new alert AlertMonitor scrapes (5 s tick)
  /ws/listener-health  — full HealthCheck snapshot (10 s tick)
  /ws/dashboard        — full dashboard snapshot per subscription
                         (5 s tick + immediate push on subscribe)

Inbound protocol:
  /ws/dashboard   {"type": "subscribe", "range": "6h",
                   "sites":   ["CDVL","BCP","SIFY"],
                   "vendors": ["hitachi","brocade","netapp","dell"]}

All endpoints honour a 30 s server-side keepalive ping; clients can
also send the bare text "ping" to bump the connection.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Awaitable, Callable, List, Optional

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from services.influx_pool          import WsHub
from services.alert_monitor        import AlertMonitor
from services.health_check         import HealthCheckMonitor
from services.dashboard_broadcaster import DashboardBroadcaster

log = logging.getLogger("unifiedops.routers.websocket")
router = APIRouter()

_alerts_hub:    Optional[WsHub]                 = None
_health_hub:    Optional[WsHub]                 = None
_alert_monitor: Optional[AlertMonitor]          = None
_health_monitor:Optional[HealthCheckMonitor]    = None
_dashboard_bc:  Optional[DashboardBroadcaster]  = None


def configure(
    alerts_hub:     WsHub,
    health_hub:     WsHub,
    alert_monitor:  AlertMonitor,
    health_monitor: HealthCheckMonitor,
    dashboard_bc:   DashboardBroadcaster,
) -> None:
    global _alerts_hub, _health_hub, _alert_monitor, _health_monitor, _dashboard_bc
    _alerts_hub     = alerts_hub
    _health_hub     = health_hub
    _alert_monitor  = alert_monitor
    _health_monitor = health_monitor
    _dashboard_bc   = dashboard_bc


async def _ws_loop(
    ws:      WebSocket,
    hub:     WsHub,
    hydrate: Optional[Callable[[WebSocket], Awaitable[None]]],
) -> None:
    await ws.accept()
    await hub.register(ws)
    try:
        if hydrate is not None:
            try:
                await hydrate(ws)
            except Exception as exc:
                log.info("hub[%s] hydrate failed: %s", hub.name, exc)
        while True:
            try:
                raw = await asyncio.wait_for(ws.receive_text(), timeout=30.0)
            except asyncio.TimeoutError:
                try:
                    await ws.send_text(json.dumps({"type": "ping", "ts": time.time()}))
                except Exception:
                    break
                continue
            if raw == "ping":
                await ws.send_text(json.dumps({"type": "pong", "ts": time.time()}))
    except WebSocketDisconnect:
        log.info("ws[%s] client disconnected cleanly", hub.name)
    except Exception as exc:
        log.info("ws[%s] client closed: %s", hub.name, exc)
    finally:
        await hub.unregister(ws)


@router.websocket("/ws/alerts")
async def ws_alerts(ws: WebSocket) -> None:
    if _alerts_hub is None or _alert_monitor is None:
        await ws.close(code=1011)
        return
    await _ws_loop(ws, _alerts_hub, _alert_monitor.hydrate)


@router.websocket("/ws/listener-health")
async def ws_listener_health(ws: WebSocket) -> None:
    if _health_hub is None or _health_monitor is None:
        await ws.close(code=1011)
        return
    await _ws_loop(ws, _health_hub, _health_monitor.hydrate)


@router.websocket("/ws/dashboard")
async def ws_dashboard(ws: WebSocket) -> None:
    """Per-client dashboard channel.

    Inbound protocol:
      {"type": "subscribe",
       "range":   "<key>",
       "sites":   ["CDVL","BCP","SIFY"],
       "vendors": ["hitachi","brocade","netapp","dell"]}

    Each (re)subscribe immediately pushes one snapshot; thereafter the
    broadcaster's polling loop fans out a fresh snapshot every tick.
    """
    if _dashboard_bc is None:
        await ws.close(code=1011)
        return

    await ws.accept()
    await _dashboard_bc.register(ws)
    try:
        while True:
            try:
                raw = await asyncio.wait_for(ws.receive_text(), timeout=30.0)
            except asyncio.TimeoutError:
                try:
                    await ws.send_text(json.dumps({"type": "ping", "ts": time.time()}))
                except Exception:
                    break
                continue

            if raw == "ping":
                try:
                    await ws.send_text(json.dumps({"type": "pong", "ts": time.time()}))
                except Exception:
                    break
                continue

            try:
                msg = json.loads(raw)
            except Exception:
                continue
            if not isinstance(msg, dict):
                continue
            if msg.get("type") != "subscribe":
                continue
            await _dashboard_bc.subscribe(
                ws,
                range_key=str(msg.get("range") or "6h"),
                sites=msg.get("sites")   if isinstance(msg.get("sites"),   list) else None,
                vendors=msg.get("vendors") if isinstance(msg.get("vendors"), list) else None,
            )
    except WebSocketDisconnect:
        log.info("ws[dashboard] client disconnected cleanly")
    except Exception as exc:
        log.info("ws[dashboard] client closed: %s", exc)
    finally:
        await _dashboard_bc.unregister(ws)
