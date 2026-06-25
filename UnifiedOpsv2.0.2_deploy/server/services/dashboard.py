"""
Dashboard aggregator service.

On-demand REST aggregator: fans out Flux queries across every relevant
vendor bucket and merges the results into the shape the React cards need.
This is the SERVER-SIDE source of truth — the browser only ever consumes
`/api/dashboard/*` REST endpoints and `/ws/*` push frames; it never
touches InfluxDB directly.

Uses the same `InfluxPool` that AlertMonitor + HealthCheck use, so the
shared async thread pool is the only InfluxDB connection layer in the
process.

Python 3.9+ compatible.
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from .influx_pool import InfluxPool, InfluxQueryError
from .alert_monitor import VENDOR_BUCKETS, severity_from_body


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------
# `raw_message` is the canonical, full-fidelity packet we store. For the
# Recent Critical Alerts card we strip the syslog envelope so operators
# see the actual vendor event text instead of "<131>1 2026-... - - - ...".
_RFC5424_HEAD_RE = re.compile(
    r"^<\d+>\d+\s+\S+\s+\S+\s+\S+\s+\S+\s+\S+\s+"
    r"(?:-|\[[^\]]*\](?:\s*\[[^\]]*\])*)\s+"
    r"(?:\ufeff)?"
)
_RFC3164_HEAD_RE = re.compile(
    r"^<\d+>\s*\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+\S+\s+[^:\[]+(?:\[\d+\])?:\s*"
)
_PRI_RE = re.compile(r"^<\d+>")
_SOURCE_IP_PREFIX_RE = re.compile(r"^\s*\[SOURCE_IP=[^\]]+\]\s*")


def strip_syslog_header(raw: str) -> str:
    """Return just the vendor event text from a raw syslog packet.

    Order of fallbacks:
        1. strip trap_sender's [SOURCE_IP=...] dev prefix
        2. strip the RFC5424 header up to the structured-data block
        3. strip the RFC3164 header up to the process tag
        4. last-ditch: strip the leading <PRI> if everything else fails
    """
    if not raw:
        return ""
    s = str(raw)
    s = _SOURCE_IP_PREFIX_RE.sub("", s, count=1)
    m = _RFC5424_HEAD_RE.match(s)
    if m:
        return s[m.end():].strip()
    m = _RFC3164_HEAD_RE.match(s)
    if m:
        return s[m.end():].strip()
    if _PRI_RE.match(s):
        cut = s.find(">")
        if cut > 0:
            return s[cut + 1:].lstrip()
    return s.strip()

log = logging.getLogger("unifiedops.dashboard")


# ---------------------------------------------------------------------------
# Time range helpers
# ---------------------------------------------------------------------------
RANGE_DURATIONS_S: Dict[str, int] = {
    "5m":   300,
    "15m":  900,
    "30m":  1800,
    "1h":   3600,
    "3h":   10800,
    "6h":   21600,
    "12h":  43200,
    "24h":  86400,
    "1d":   86400,
    "2d":   172800,
    "3d":   259200,
    "7d":   604800,
    "15d":  1296000,
    "30d":  2592000,
}


def range_duration_s(range_key: str) -> int:
    return RANGE_DURATIONS_S.get(range_key, 21600)


def range_clause(range_key: str) -> str:
    return "start: -{0}".format(range_key)


def bucket_window(range_key: str, target: int) -> str:
    """Window string sized so the trend gets ~`target` buckets across the
    range. E.g. 5m / 10 -> '30s'; 24h / 24 -> '1h'."""
    ms = max(1000, (range_duration_s(range_key) * 1000) // max(1, target))
    if ms >= 86_400_000:
        return "{0}d".format(max(1, ms // 86_400_000))
    if ms >= 3_600_000:
        return "{0}h".format(max(1, ms // 3_600_000))
    if ms >= 60_000:
        return "{0}m".format(max(1, ms // 60_000))
    return "{0}s".format(max(1, ms // 1000))


# ---------------------------------------------------------------------------
# Severity normalization (server-side mirror of frontend's `bucketSeverity`)
# ---------------------------------------------------------------------------
_CRIT  = {"emergency", "alert", "critical", "acute"}
_ERR   = {"error", "err", "serious", "failure"}
_WARN  = {"warning", "warn", "moderate"}
_NOTE  = {"notice", "note", "service"}


def bucket_severity(raw: Any) -> str:
    if not raw:
        return "informational"
    s = str(raw).strip().lower()
    if s in _CRIT: return "critical"
    if s in _ERR:  return "error"
    if s in _WARN: return "warning"
    if s in _NOTE: return "notice"
    return "informational"


# ---------------------------------------------------------------------------
# Bucket scoping
# ---------------------------------------------------------------------------
def scoped_buckets(
    sites:   Optional[List[str]] = None,
    vendors: Optional[List[str]] = None,
) -> List[Dict[str, str]]:
    sites_u   = set(s.upper() for s in (sites or []))
    vendors_l = set(v.lower() for v in (vendors or []))
    out: List[Dict[str, str]] = []
    for cfg in VENDOR_BUCKETS:
        if sites_u   and cfg["site"]   not in sites_u:   continue
        if vendors_l and cfg["vendor"] not in vendors_l: continue
        if not cfg.get("token"): continue
        out.append(cfg)
    return out


def bucket_key(cfg: Dict[str, str]) -> str:
    return "alert:{0}:{1}".format(cfg["site"], cfg["vendor"])


# ---------------------------------------------------------------------------
# Flux queries — kept simple, every dashboard card has one
# ---------------------------------------------------------------------------
_MEASUREMENT_FILTER = 'r._measurement != "syslog_listener_heartbeat"'

# Every listener writes exactly one `raw_message` field per alert
# (Hitachi/Brocade also write `message`, NetApp/Dell also write `preview`).
# Counting on `raw_message` ALONE gives one row per alert; counting on
# multiple fields would multi-count Hitachi+Brocade by 2x and
# NetApp+Dell by 2x.
_COUNT_FIELD_FILTER = 'r._field == "raw_message"'


def _flux_total(bucket: str, range_key: str) -> str:
    """Row-level scan filtered to `raw_message` so we get exactly one
    record per stored alert. The Python caller counts the rows directly,
    matching `get_severity`'s source-of-truth and avoiding any
    `group()/count()` surprises on tiny / single-row datasets."""
    return (
        f'from(bucket: "{bucket}")\n'
        f'  |> range({range_clause(range_key)})\n'
        f'  |> filter(fn: (r) => {_MEASUREMENT_FILTER})\n'
        f'  |> filter(fn: (r) => {_COUNT_FIELD_FILTER})\n'
        f'  |> keep(columns: ["_time"])\n'
    )


def _flux_severity(bucket: str, range_key: str) -> str:
    """Row-level scan so we can apply the same body-PRI fallback as the
    Recent Critical Alerts card. Listeners that don't pre-extract a
    `severity` tag (NetApp / Dell placeholder ingest paths) would
    otherwise be silently counted as `informational`, producing a donut
    that disagrees with the table. Keep the body in `_value` for the
    PRI re-derivation in Python. Filter to `raw_message` only so we get
    one row per alert (not one per stored field)."""
    return (
        f'from(bucket: "{bucket}")\n'
        f'  |> range({range_clause(range_key)})\n'
        f'  |> filter(fn: (r) => {_MEASUREMENT_FILTER})\n'
        f'  |> filter(fn: (r) => {_COUNT_FIELD_FILTER})\n'
        f'  |> keep(columns: ["_time", "severity", "_value"])\n'
    )


def _flux_categories(bucket: str, range_key: str) -> str:
    return (
        f'from(bucket: "{bucket}")\n'
        f'  |> range({range_clause(range_key)})\n'
        f'  |> filter(fn: (r) => {_MEASUREMENT_FILTER})\n'
        f'  |> filter(fn: (r) => {_COUNT_FIELD_FILTER})\n'
        f'  |> filter(fn: (r) => exists r.trap_category '
        f'and r.trap_category != "none" and r.trap_category != "unknown")\n'
        f'  |> group(columns: ["trap_category"])\n'
        f'  |> count()\n'
        f'  |> keep(columns: ["trap_category", "_value"])\n'
    )


def _flux_top_systems(bucket: str, range_key: str) -> str:
    """Row-level scan so we can aggregate by whichever storage-name tag
    the vendor's listener actually wrote (`array_name` for Hitachi,
    `switch_name` for Brocade, `hostname` for NetApp/Dell). Falling back
    in Flux is awkward — easier to scan rows and group in Python."""
    return (
        f'from(bucket: "{bucket}")\n'
        f'  |> range({range_clause(range_key)})\n'
        f'  |> filter(fn: (r) => {_MEASUREMENT_FILTER})\n'
        f'  |> filter(fn: (r) => {_COUNT_FIELD_FILTER})\n'
        f'  |> keep(columns: ["_time", "array_name", "switch_name", "hostname", "source_ip"])\n'
    )


def _flux_trend(bucket: str, range_key: str, window: str) -> str:
    return (
        f'from(bucket: "{bucket}")\n'
        f'  |> range({range_clause(range_key)})\n'
        f'  |> filter(fn: (r) => {_MEASUREMENT_FILTER})\n'
        f'  |> filter(fn: (r) => {_COUNT_FIELD_FILTER})\n'
        f'  |> aggregateWindow(every: {window}, fn: count, createEmpty: true)\n'
        f'  |> fill(value: 0)\n'
        f'  |> keep(columns: ["_time", "_value"])\n'
    )


def _flux_recent(bucket: str, range_key: str, limit: int) -> str:
    """Pull the canonical `raw_message` row per alert. Every listener
    writes `raw_message` exactly once per alert, so this gives one row
    per alert (without the duplicate Hitachi-message / NetApp-preview
    rows that the multi-field filter used to produce)."""
    return (
        f'from(bucket: "{bucket}")\n'
        f'  |> range({range_clause(range_key)})\n'
        f'  |> filter(fn: (r) => {_MEASUREMENT_FILTER})\n'
        f'  |> filter(fn: (r) => {_COUNT_FIELD_FILTER})\n'
        f'  |> sort(columns: ["_time"], desc: true)\n'
        f'  |> limit(n: {limit})\n'
    )


# ---------------------------------------------------------------------------
# Aggregator
# ---------------------------------------------------------------------------
class DashboardService:
    """Composes the dashboard payload from per-bucket Flux queries."""

    def __init__(self, pool: InfluxPool) -> None:
        self._pool = pool

    # ---- single-card endpoints -----------------------------------------
    async def get_total(
        self, range_key: str,
        sites:   Optional[List[str]] = None,
        vendors: Optional[List[str]] = None,
    ) -> int:
        buckets = scoped_buckets(sites, vendors)
        rows = await asyncio.gather(*[
            self._safe_query(bucket_key(b), _flux_total(b["bucket"], range_key))
            for b in buckets
        ])
        return sum(len(row_set) for row_set in rows)

    async def get_severity(
        self, range_key: str,
        sites:   Optional[List[str]] = None,
        vendors: Optional[List[str]] = None,
    ) -> Dict[str, int]:
        buckets = scoped_buckets(sites, vendors)
        out = {"critical": 0, "error": 0, "warning": 0, "notice": 0, "informational": 0}
        rows = await asyncio.gather(*[
            self._safe_query(bucket_key(b), _flux_severity(b["bucket"], range_key))
            for b in buckets
        ])
        for row_set in rows:
            for r in row_set:
                raw_sev  = r.get("severity")
                severity = bucket_severity(raw_sev) if raw_sev else "informational"
                if not raw_sev or severity == "informational":
                    body_sev = severity_from_body(r.get("_value") or "")
                    if body_sev is not None:
                        severity = body_sev
                out[severity] = out[severity] + 1
        return out

    async def get_categories(
        self, range_key: str,
        sites:   Optional[List[str]] = None,
        vendors: Optional[List[str]] = None,
    ) -> Dict[str, int]:
        buckets = scoped_buckets(sites, vendors)
        out: Dict[str, int] = {}
        rows = await asyncio.gather(*[
            self._safe_query(bucket_key(b), _flux_categories(b["bucket"], range_key))
            for b in buckets
        ])
        for row_set in rows:
            for r in row_set:
                # Pass the raw `trap_category` straight through. The
                # frontend's `normalizeCategory()` handles bucketing into
                # the 12 display categories — keeping the raw key here
                # means new listener-side categories don't require a
                # backend rebuild to appear in the dashboard.
                cat = r.get("trap_category") or "other"
                out[cat] = out.get(cat, 0) + int(r.get("_value") or 0)
        return out

    async def get_top_systems(
        self, range_key: str,
        sites:   Optional[List[str]] = None,
        vendors: Optional[List[str]] = None,
        limit:   int = 50,
    ) -> List[Dict[str, Any]]:
        buckets = scoped_buckets(sites, vendors)
        per_bucket = await asyncio.gather(*[
            self._safe_query(bucket_key(b), _flux_top_systems(b["bucket"], range_key))
            for b in buckets
        ])
        agg: Dict[Tuple[str, str, str], int] = {}
        for cfg, row_set in zip(buckets, per_bucket):
            for r in row_set:
                # Same fallback order as `get_recent` so vendor mix shows up.
                name = (
                    r.get("array_name")
                    or r.get("switch_name")
                    or r.get("hostname")
                    or r.get("source_ip")
                )
                if not name or name in ("", "unknown", "-"):
                    continue
                key = (str(name), cfg["site"], cfg["vendor"])
                agg[key] = agg.get(key, 0) + 1
        out: List[Dict[str, Any]] = [
            {"name": name, "alerts": cnt, "location": site, "vendor": vendor}
            for (name, site, vendor), cnt in agg.items()
        ]
        out.sort(key=lambda x: x["alerts"], reverse=True)
        return out[:limit]

    async def get_trend(
        self, range_key: str,
        sites:   Optional[List[str]] = None,
        vendors: Optional[List[str]] = None,
        target:  int = 25,
    ) -> List[Dict[str, Any]]:
        buckets = scoped_buckets(sites, vendors)
        window  = bucket_window(range_key, target)
        rows_per_bucket = await asyncio.gather(*[
            self._safe_query(bucket_key(b), _flux_trend(b["bucket"], range_key, window))
            for b in buckets
        ])
        merged: Dict[str, int] = {}
        for rows in rows_per_bucket:
            for r in rows:
                t = r.get("_time")
                if not t:
                    continue
                merged[t] = merged.get(t, 0) + int(r.get("_value") or 0)
        ordered_times = sorted(merged.keys())
        dur_s = range_duration_s(range_key)
        out: List[Dict[str, Any]] = []
        for t in ordered_times:
            try:
                dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
            except Exception:
                continue
            local = dt.astimezone()
            if dur_s <= 86_400:
                label = local.strftime("%H:%M")
            else:
                label = "{0} {1}".format(local.strftime("%b"), local.day)
            out.append({
                "ts":    int(dt.timestamp() * 1000),
                "value": merged[t],
                "label": label,
            })
        return out

    async def get_recent(
        self, range_key: str,
        sites:   Optional[List[str]] = None,
        vendors: Optional[List[str]] = None,
        limit:   int = 200,
    ) -> List[Dict[str, Any]]:
        buckets = scoped_buckets(sites, vendors)
        per_bucket_limit = max(5, min(2000, (limit * 2) // max(1, len(buckets) or 1)))
        per_bucket = await asyncio.gather(*[
            self._safe_query(
                bucket_key(b),
                _flux_recent(b["bucket"], range_key, per_bucket_limit),
            )
            for b in buckets
        ])
        out: List[Dict[str, Any]] = []
        for cfg, rows in zip(buckets, per_bucket):
            for r in rows:
                ts = r.get("_time")
                if not ts:
                    continue
                try:
                    dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                except Exception:
                    continue
                local = dt.astimezone()
                storage = (
                    r.get("array_name")
                    or r.get("switch_name")
                    or r.get("hostname")
                    or r.get("source_ip", "-")
                )
                if storage in ("", "unknown", "-"):
                    storage = r.get("source_ip", "-")
                raw_body   = str(r.get("_value") or "")
                event_text = strip_syslog_header(raw_body) or raw_body
                if len(event_text) > 300:
                    event_text = event_text[:297] + "..."
                # Severity precedence: explicit `severity` tag → syslog
                # PRI from the raw body → "informational". Without the
                # PRI fallback every NetApp/Dell trap would land as
                # informational.
                raw_sev  = r.get("severity")
                severity = bucket_severity(raw_sev) if raw_sev else "informational"
                if not raw_sev or severity == "informational":
                    body_sev = severity_from_body(raw_body)
                    if body_sev is not None:
                        severity = body_sev
                out.append({
                    "ts":          int(dt.timestamp() * 1000),
                    "time":        local.strftime("%Y-%m-%d %H:%M:%S"),
                    "severity":    severity,
                    "storageName": storage,
                    "ip":          r.get("source_ip") or "-",
                    "event":       event_text,
                    "category":    r.get("trap_category") or "other",
                    "location":    cfg["site"],
                    "vendor":      cfg["vendor"],
                })
        out.sort(key=lambda a: a["ts"], reverse=True)
        return out[:limit]

    # ---- composite snapshot --------------------------------------------
    async def get_snapshot(
        self, range_key: str,
        sites:   Optional[List[str]] = None,
        vendors: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Return everything every dashboard card needs in one round-trip."""
        t0 = time.perf_counter()
        total, severity, categories, top_systems, trend, recent = await asyncio.gather(
            self.get_total       (range_key, sites, vendors),
            self.get_severity    (range_key, sites, vendors),
            self.get_categories  (range_key, sites, vendors),
            self.get_top_systems (range_key, sites, vendors, 50),
            self.get_trend       (range_key, sites, vendors, 25),
            self.get_recent      (range_key, sites, vendors, 200),
        )
        return {
            "range":      range_key,
            "sites":      sites   or [],
            "vendors":    vendors or [],
            "as_of":      time.time(),
            "elapsed_ms": int((time.perf_counter() - t0) * 1000),
            "total":      total,
            "severity":   severity,
            "categories": categories,
            "topSystems": top_systems,
            "trend":      trend,
            "recent":     recent,
        }

    # ---- helpers --------------------------------------------------------
    async def _safe_query(self, key: str, flux: str) -> List[Dict[str, Any]]:
        try:
            return await self._pool.query(key, flux)
        except InfluxQueryError as exc:
            log.warning("dashboard %s query failed: %s", key, exc.reason)
            return []
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("dashboard %s query crash: %s", key, exc)
            return []


async def _empty_device_snapshot(range_key: str) -> Dict[str, Any]:
    return {"range": range_key, "vendors": {}, "grand_total": 0, "grand_alerting": 0}
