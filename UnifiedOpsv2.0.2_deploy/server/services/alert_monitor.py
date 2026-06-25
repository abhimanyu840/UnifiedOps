"""
Alert monitor.

Background asyncio loop that polls all 11 per-vendor / per-site InfluxDB
buckets every N seconds (default 5 s — matches the user's diagram) and
broadcasts incrementally-new alerts to every `/ws/alerts` subscriber.

Reads via the official `influxdb-client` package, wrapped in an
`InfluxPool` so the synchronous client API runs on a ThreadPoolExecutor
while the asyncio event loop drives the WS fanout.

Python 3.9+ compatible.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from .influx_pool import InfluxPool, InfluxQueryError, WsHub

log = logging.getLogger("unifiedops.alert_monitor")


# ---------------------------------------------------------------------------
# Bucket topology — one row per (site, vendor) deployed listener.
# ---------------------------------------------------------------------------
VENDOR_BUCKETS: List[Dict[str, str]] = [
    # ---- Hitachi ---------------------------------------------------------
    {"site": "CDVL", "vendor": "hitachi",
     "url":    os.environ.get("HITRACK_BUCKET_HITACHI_CDVL_URL",    "http://127.0.0.1:8086"),
     "token":  os.environ.get("HITRACK_BUCKET_HITACHI_CDVL_TOKEN",  "unifiedops-dev-token-cdvl"),
     "org":    os.environ.get("HITRACK_BUCKET_HITACHI_CDVL_ORG",    "HDFC"),
     "bucket": os.environ.get("HITRACK_BUCKET_HITACHI_CDVL_BUCKET", "Hitachi_CDVL_Bucket")},
    {"site": "BCP",  "vendor": "hitachi",
     "url":    os.environ.get("HITRACK_BUCKET_HITACHI_BCP_URL",     "http://127.0.0.1:8087"),
     "token":  os.environ.get("HITRACK_BUCKET_HITACHI_BCP_TOKEN",   "unifiedops-dev-token-bcp"),
     "org":    os.environ.get("HITRACK_BUCKET_HITACHI_BCP_ORG",     "HDFC"),
     "bucket": os.environ.get("HITRACK_BUCKET_HITACHI_BCP_BUCKET",  "Hitachi_BCP_Bucket")},
    {"site": "SIFY", "vendor": "hitachi",
     "url":    os.environ.get("HITRACK_BUCKET_HITACHI_SIFY_URL",    "http://127.0.0.1:8088"),
     "token":  os.environ.get("HITRACK_BUCKET_HITACHI_SIFY_TOKEN",  "unifiedops-dev-token-sify"),
     "org":    os.environ.get("HITRACK_BUCKET_HITACHI_SIFY_ORG",    "HDFC"),
     "bucket": os.environ.get("HITRACK_BUCKET_HITACHI_SIFY_BUCKET", "Hitachi_SIFY_Bucket")},
    # ---- Brocade ---------------------------------------------------------
    {"site": "CDVL", "vendor": "brocade",
     "url":    os.environ.get("HITRACK_BUCKET_BROCADE_CDVL_URL",    "http://127.0.0.1:8186"),
     "token":  os.environ.get("HITRACK_BUCKET_BROCADE_CDVL_TOKEN",  "unifiedops-dev-token-brocade-cdvl"),
     "org":    os.environ.get("HITRACK_BUCKET_BROCADE_CDVL_ORG",    "HDFC"),
     "bucket": os.environ.get("HITRACK_BUCKET_BROCADE_CDVL_BUCKET", "Brocade_CDVL_Bucket")},
    {"site": "BCP",  "vendor": "brocade",
     "url":    os.environ.get("HITRACK_BUCKET_BROCADE_BCP_URL",     "http://127.0.0.1:8187"),
     "token":  os.environ.get("HITRACK_BUCKET_BROCADE_BCP_TOKEN",   "unifiedops-dev-token-brocade-bcp"),
     "org":    os.environ.get("HITRACK_BUCKET_BROCADE_BCP_ORG",     "HDFC"),
     "bucket": os.environ.get("HITRACK_BUCKET_BROCADE_BCP_BUCKET",  "Brocade_BCP_Bucket")},
    # ---- NetApp ----------------------------------------------------------
    {"site": "CDVL", "vendor": "netapp",
     "url":    os.environ.get("HITRACK_BUCKET_NETAPP_CDVL_URL",     "http://127.0.0.1:8286"),
     "token":  os.environ.get("HITRACK_BUCKET_NETAPP_CDVL_TOKEN",   "unifiedops-dev-token-netapp-cdvl"),
     "org":    os.environ.get("HITRACK_BUCKET_NETAPP_CDVL_ORG",     "HDFC"),
     "bucket": os.environ.get("HITRACK_BUCKET_NETAPP_CDVL_BUCKET",  "NetApp_CDVL_Bucket")},
    {"site": "BCP",  "vendor": "netapp",
     "url":    os.environ.get("HITRACK_BUCKET_NETAPP_BCP_URL",      "http://127.0.0.1:8287"),
     "token":  os.environ.get("HITRACK_BUCKET_NETAPP_BCP_TOKEN",    "unifiedops-dev-token-netapp-bcp"),
     "org":    os.environ.get("HITRACK_BUCKET_NETAPP_BCP_ORG",      "HDFC"),
     "bucket": os.environ.get("HITRACK_BUCKET_NETAPP_BCP_BUCKET",   "NetApp_BCP_Bucket")},
    {"site": "SIFY", "vendor": "netapp",
     "url":    os.environ.get("HITRACK_BUCKET_NETAPP_SIFY_URL",     "http://127.0.0.1:8288"),
     "token":  os.environ.get("HITRACK_BUCKET_NETAPP_SIFY_TOKEN",   "unifiedops-dev-token-netapp-sify"),
     "org":    os.environ.get("HITRACK_BUCKET_NETAPP_SIFY_ORG",     "HDFC"),
     "bucket": os.environ.get("HITRACK_BUCKET_NETAPP_SIFY_BUCKET",  "NetApp_SIFY_Bucket")},
    # ---- Dell ------------------------------------------------------------
    {"site": "CDVL", "vendor": "dell",
     "url":    os.environ.get("HITRACK_BUCKET_DELL_CDVL_URL",       "http://127.0.0.1:8386"),
     "token":  os.environ.get("HITRACK_BUCKET_DELL_CDVL_TOKEN",     "unifiedops-dev-token-dell-cdvl"),
     "org":    os.environ.get("HITRACK_BUCKET_DELL_CDVL_ORG",       "HDFC"),
     "bucket": os.environ.get("HITRACK_BUCKET_DELL_CDVL_BUCKET",    "Dell_CDVL_Bucket")},
    {"site": "BCP",  "vendor": "dell",
     "url":    os.environ.get("HITRACK_BUCKET_DELL_BCP_URL",        "http://127.0.0.1:8387"),
     "token":  os.environ.get("HITRACK_BUCKET_DELL_BCP_TOKEN",      "unifiedops-dev-token-dell-bcp"),
     "org":    os.environ.get("HITRACK_BUCKET_DELL_BCP_ORG",        "HDFC"),
     "bucket": os.environ.get("HITRACK_BUCKET_DELL_BCP_BUCKET",     "Dell_BCP_Bucket")},
    {"site": "SIFY", "vendor": "dell",
     "url":    os.environ.get("HITRACK_BUCKET_DELL_SIFY_URL",       "http://127.0.0.1:8388"),
     "token":  os.environ.get("HITRACK_BUCKET_DELL_SIFY_TOKEN",     "unifiedops-dev-token-dell-sify"),
     "org":    os.environ.get("HITRACK_BUCKET_DELL_SIFY_ORG",       "HDFC"),
     "bucket": os.environ.get("HITRACK_BUCKET_DELL_SIFY_BUCKET",    "Dell_SIFY_Bucket")},
]


# Poll cadence — 5 s matches the user's architecture diagram.
POLL_INTERVAL_S          = max(2, int(os.environ.get("HITRACK_PIPELINE_POLL_SECS",  "5")))
INITIAL_LOOKBACK_S       = max(60, int(os.environ.get("HITRACK_ALERTS_LOOKBACK_S",  "21600")))   # 6h
PER_BUCKET_LIMIT_DEFAULT = max(10, int(os.environ.get("HITRACK_ALERTS_PER_BUCKET",   "100")))
ROLLING_BUFFER_MAX       = max(200, int(os.environ.get("HITRACK_ALERTS_BUFFER_MAX", "1000")))


_VALID_SEVERITIES = ("critical", "error", "warning", "notice", "informational")


def _normalize_severity(raw: Any) -> str:
    if not raw:
        return "informational"
    s = str(raw).strip().lower()
    if s in _VALID_SEVERITIES:
        return s
    if s in ("crit", "fatal", "alert", "emergency", "acute"):
        return "critical"
    if s in ("err", "failure", "failed", "serious"):
        return "error"
    if s in ("warn", "moderate"):
        return "warning"
    if s in ("info", "informational"):
        return "informational"
    if s in ("note", "service"):
        return "notice"
    return "notice"


# Standard syslog severity numbers (the low-order 3 bits of the PRI field).
_PRI_SEVERITY: Dict[int, str] = {
    0: "critical",      # emergency
    1: "critical",      # alert
    2: "critical",      # critical
    3: "error",
    4: "warning",
    5: "notice",
    6: "informational",
    7: "informational", # debug
}


def severity_from_body(text: Any) -> Optional[str]:
    """Parse the syslog `<PRI>` prefix from a packet body and map the
    severity bits to one of our canonical strings.

    Most NetApp / Dell listeners don't pre-extract a `severity` tag so
    the only place severity information survives is the original syslog
    PRI value at the start of the message. Walks past an optional
    `[SOURCE_IP=...]` test-mode prefix that the trap-sender prepends.

    Returns None if no PRI can be parsed (we leave the upstream default
    of "informational" alone in that case).
    """
    if not text:
        return None
    s = str(text).lstrip()
    if s.startswith("[SOURCE_IP="):
        # trap_sender.py prepends `[SOURCE_IP=10.x.x.x] ` so the
        # listener can spoof the source IP for tests; the actual syslog
        # frame starts right after it.
        cut = s.find("] ")
        if cut > 0:
            s = s[cut + 2:].lstrip()
    if not s.startswith("<"):
        return None
    end = s.find(">")
    if end < 2:
        return None
    try:
        pri = int(s[1:end])
    except (ValueError, TypeError):
        return None
    return _PRI_SEVERITY.get(pri & 0x07)


def _flux_since(bucket: str, since_iso: Optional[str], limit: int) -> str:
    """Pull alerts since `since_iso` (or the initial lookback window)."""
    if since_iso:
        range_clause = f'range(start: time(v: "{since_iso}"))'
    else:
        range_clause = f"range(start: -{INITIAL_LOOKBACK_S}s)"
    return (
        f'from(bucket: "{bucket}")\n'
        f'  |> {range_clause}\n'
        f'  |> filter(fn: (r) => r._measurement != "syslog_listener_heartbeat")\n'
        f'  |> filter(fn: (r) => r._field == "message" or r._field == "preview" '
        f'or r._field == "raw_message")\n'
        f'  |> sort(columns: ["_time"], desc: true)\n'
        f'  |> limit(n: {limit})\n'
    )


def _row_to_alert(row: Dict[str, Any], site: str, vendor: str) -> Optional[Dict[str, Any]]:
    time_iso = row.get("_time")
    if not time_iso:
        return None
    try:
        dt = datetime.fromisoformat(str(time_iso).replace("Z", "+00:00"))
    except Exception:
        return None
    ts_ms = int(dt.timestamp() * 1000)
    local_dt = dt.astimezone()

    storage = (
        row.get("array_name")
        or row.get("switch_name")
        or row.get("hostname")
        or row.get("source_ip", "-")
    )
    if storage in ("", "unknown", "-"):
        storage = row.get("source_ip", "-")

    event_text = (
        row.get("message")
        or row.get("preview")
        or row.get("raw_message")
        or row.get("_value")
        or "-"
    )
    event_text = str(event_text)
    if len(event_text) > 300:
        event_text = event_text[:297] + "..."

    # Severity precedence:
    #   1. `severity` TAG if the listener parsed one (Hitachi, Brocade)
    #   2. syslog `<PRI>` prefix in the message body (NetApp, Dell)
    #   3. fallback to informational via _normalize_severity("")
    raw_sev = row.get("severity")
    severity = _normalize_severity(raw_sev) if raw_sev else "informational"
    if not raw_sev or severity == "informational":
        body_sev = severity_from_body(event_text)
        if body_sev is not None:
            severity = body_sev

    return {
        "time":        local_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "ts":          ts_ms,
        "severity":    severity,
        "storageName": storage,
        "ip":          row.get("source_ip", "-"),
        "event":       event_text,
        "category":    row.get("trap_category") or "other",
        "location":    site,
        "vendor":      vendor,
    }


class AlertMonitor:
    """Background poller that broadcasts new alerts over WebSocket.

    State per bucket:
        - last_seen_iso  ISO-8601 timestamp of newest delivered alert
        - reachable      True iff the most recent query succeeded
        - error          last error string (None on success)

    Each tick fans out queries to every registered bucket in parallel via
    asyncio.gather + InfluxPool.query; only alerts whose `_time` is
    strictly after `last_seen_iso` are delivered.
    """

    def __init__(self, pool: InfluxPool, hub: WsHub) -> None:
        self._pool = pool
        self._hub  = hub
        self._stop = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._cursors: Dict[str, Optional[str]] = {}
        self._bucket_status: Dict[str, Dict[str, Any]] = {}
        # Rolling buffer of recent alerts so newly-connected WS clients
        # receive a hydrate snapshot without waiting for the next tick.
        self._recent: List[Dict[str, Any]] = []

        for cfg in VENDOR_BUCKETS:
            key = self._key(cfg)
            self._cursors[key] = None
            self._bucket_status[key] = {
                "site":   cfg["site"],
                "vendor": cfg["vendor"],
                "bucket": cfg["bucket"],
                "ok":     False,
                "error":  None,
                "count":  0,
                "last_check": None,
            }
            self._pool.register(
                key,
                url=cfg["url"], token=cfg["token"], org=cfg["org"],
            )

    @staticmethod
    def _key(cfg: Dict[str, str]) -> str:
        return "alert:{0}:{1}".format(cfg["site"], cfg["vendor"])

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="alert-monitor")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._stop.set()
        try:
            await asyncio.wait_for(self._task, timeout=4.0)
        except asyncio.TimeoutError:
            self._task.cancel()
        self._task = None

    async def _run(self) -> None:
        log.info(
            "AlertMonitor started — buckets=%d poll=%ds",
            len(VENDOR_BUCKETS), POLL_INTERVAL_S,
        )
        while not self._stop.is_set():
            try:
                await self._tick()
            except Exception as exc:  # pragma: no cover - defensive
                log.warning("AlertMonitor tick failed: %s", exc)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=POLL_INTERVAL_S)
            except asyncio.TimeoutError:
                continue

    async def _tick(self) -> None:
        results = await asyncio.gather(*[
            self._fetch_bucket(cfg) for cfg in VENDOR_BUCKETS
        ], return_exceptions=True)

        fresh: List[Dict[str, Any]] = []
        now = time.time()
        for cfg, result in zip(VENDOR_BUCKETS, results):
            key = self._key(cfg)
            status = self._bucket_status[key]
            status["last_check"] = now
            if isinstance(result, Exception):
                status["ok"]    = False
                status["error"] = str(result)
                status["count"] = 0
                continue
            alerts, latest_iso = result
            status["ok"]    = True
            status["error"] = None
            status["count"] = len(alerts)
            if latest_iso:
                self._cursors[key] = latest_iso
            fresh.extend(alerts)

        if fresh:
            # Newest-first when broadcasting so the React store can prepend
            # directly without a re-sort.
            fresh.sort(key=lambda a: a["ts"], reverse=True)
            self._recent = (fresh + self._recent)[:ROLLING_BUFFER_MAX]
            await self._broadcast({"type": "alerts", "alerts": fresh})
        # Health frame is sent every tick regardless so the UI knows the
        # backend is still alive (keeps the WS-disconnect modal off).
        await self._broadcast_health()

    async def _fetch_bucket(
        self, cfg: Dict[str, str],
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        key = self._key(cfg)
        flux = _flux_since(cfg["bucket"], self._cursors[key], PER_BUCKET_LIMIT_DEFAULT)
        try:
            rows = await self._pool.query(key, flux)
        except InfluxQueryError as exc:
            raise exc
        out: List[Dict[str, Any]] = []
        merged = {}
        latest_iso: Optional[str] = self._cursors[key]
        cursor = self._cursors[key]

        for row in rows:
            t = str(row.get("_time", ""))
            if cursor and t <= cursor:
                continue
            if t not in merged:
                merged[t] = dict(row)
            field_name = row.get("_field")
            if field_name:
                merged[t][field_name] = row.get("_value")

        for t, mrow in merged.items():
            alert = _row_to_alert(mrow, cfg["site"], cfg["vendor"])
            if alert is None:
                continue
            out.append(alert)
            if latest_iso is None or t > latest_iso:
                latest_iso = t
        return out, latest_iso

    # ------------------------------------------------------------------
    # Snapshot + broadcast helpers
    # ------------------------------------------------------------------
    def snapshot(self, limit: int = 200) -> Dict[str, Any]:
        recent = self._recent[:limit]
        return {
            "ok":            any(s["ok"] for s in self._bucket_status.values()),
            "as_of":         time.time(),
            "poll_interval": POLL_INTERVAL_S,
            "buckets_total": len(self._bucket_status),
            "buckets_ok":    sum(1 for s in self._bucket_status.values() if s["ok"]),
            "count":         len(recent),
            "alerts":        recent,
            "bucket_status": list(self._bucket_status.values()),
        }

    async def _broadcast(self, msg: Dict[str, Any]) -> None:
        await self._hub.broadcast(msg["type"], json.dumps(msg, default=str))

    async def _broadcast_health(self) -> None:
        msg = {
            "type":           "health",
            "as_of":          time.time(),
            "poll_interval":  POLL_INTERVAL_S,
            "buckets_total":  len(self._bucket_status),
            "buckets_ok":     sum(1 for s in self._bucket_status.values() if s["ok"]),
            "bucket_status":  list(self._bucket_status.values()),
        }
        await self._hub.broadcast(msg["type"], json.dumps(msg, default=str))

    async def hydrate(self, ws: Any) -> None:
        """Send the most recent buffer + latest health frame to a single
        newly-connected client so it has data immediately."""
        snap = self.snapshot()
        await ws.send_text(json.dumps({"type": "hydrate", **snap}, default=str))
