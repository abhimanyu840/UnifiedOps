"""
Listener health check.

Background asyncio loop that polls every per-site heartbeat InfluxDB
bucket every N seconds (default 10 s — matches the user's diagram) and
broadcasts the full listener-state snapshot to every `/ws/listener-health`
subscriber.

Tracks two independent state machines so the React UI can distinguish:
    - "listener stopped heartbeating"           -> per-listener `down`     -> ListenerDownModal
    - "heartbeat InfluxDB itself is unreachable" -> per-site `infra_down` -> InfrastructureDownModal

Same `InfluxPool` is used as `alert_monitor.py` so InfluxDB connections
and the worker threadpool are shared across both monitors.

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

log = logging.getLogger("unifiedops.health_check")


# ---------------------------------------------------------------------------
# Topology
# ---------------------------------------------------------------------------
HEARTBEAT_PIPELINES: Dict[str, Dict[str, str]] = {
    "CDVL": {
        "url":    os.environ.get("HITRACK_HEARTBEAT_CDVL_URL",    "http://127.0.0.1:8486"),
        "token":  os.environ.get("HITRACK_HEARTBEAT_CDVL_TOKEN",  "unifiedops-dev-token-heartbeat-cdvl"),
        "org":    os.environ.get("HITRACK_HEARTBEAT_CDVL_ORG",    "HDFC"),
        "bucket": os.environ.get("HITRACK_HEARTBEAT_CDVL_BUCKET", "CDVL_Heartbeat_Bucket"),
    },
    "BCP": {
        "url":    os.environ.get("HITRACK_HEARTBEAT_BCP_URL",    "http://127.0.0.1:8487"),
        "token":  os.environ.get("HITRACK_HEARTBEAT_BCP_TOKEN",  "unifiedops-dev-token-heartbeat-bcp"),
        "org":    os.environ.get("HITRACK_HEARTBEAT_BCP_ORG",    "HDFC"),
        "bucket": os.environ.get("HITRACK_HEARTBEAT_BCP_BUCKET", "BCP_Heartbeat_Bucket"),
    },
    "SIFY": {
        "url":    os.environ.get("HITRACK_HEARTBEAT_SIFY_URL",    "http://127.0.0.1:8488"),
        "token":  os.environ.get("HITRACK_HEARTBEAT_SIFY_TOKEN",  "unifiedops-dev-token-heartbeat-sify"),
        "org":    os.environ.get("HITRACK_HEARTBEAT_SIFY_ORG",    "HDFC"),
        "bucket": os.environ.get("HITRACK_HEARTBEAT_SIFY_BUCKET", "SIFY_Heartbeat_Bucket"),
    },
}

SITE_LISTENERS: Dict[str, List[Tuple[str, str]]] = {
    "CDVL": [("Hitachi", "hitachi-cdvl"),
             ("Brocade", "brocade-cdvl"),
             ("NetApp",  "netapp-cdvl"),
             ("Dell",    "dell-cdvl")],
    "BCP":  [("Hitachi", "hitachi-bcp"),
             ("Brocade", "brocade-bcp"),
             ("NetApp",  "netapp-bcp"),
             ("Dell",    "dell-bcp")],
    "SIFY": [("Hitachi", "hitachi-sify"),
             # Brocade not deployed at SIFY per the agreed topology.
             ("NetApp",  "netapp-sify"),
             ("Dell",    "dell-sify")],
}


# --- Heartbeat cadence knobs -------------------------------------------
#
# Each listener writes a heartbeat every HEARTBEAT_INTERVAL_S seconds.
# We mark a listener "down" only when MISSED_BEATS consecutive beats are
# missed - i.e. when the most recent heartbeat is older than
# MISSED_BEATS * HEARTBEAT_INTERVAL_S + GRACE_S. The grace tolerates one
# stale poll tick / clock skew so we never raise a phantom down-modal.
#
# Defaults: 3 beats x 10 s + 5 s grace = 35 s tolerance before "down".
POLL_INTERVAL_S        = max(2, int(os.environ.get("HITRACK_LISTENER_POLL_SECS",          "10")))
HEARTBEAT_INTERVAL_S   = max(2, int(os.environ.get("HITRACK_LISTENER_HEARTBEAT_INTERVAL", "10")))
MISSED_BEATS_THRESHOLD = max(1, int(os.environ.get("HITRACK_LISTENER_MISSED_BEATS",       "3")))
DOWN_GRACE_S           = max(0, int(os.environ.get("HITRACK_LISTENER_DOWN_GRACE_S",       "5")))

# Backward-compat: if the operator pinned an absolute threshold, honour
# it; otherwise derive from beats x interval + grace.
_OVERRIDE = os.environ.get("HITRACK_LISTENER_DOWN_THRESHOLD_S")
if _OVERRIDE:
    DOWN_THRESHOLD_S = max(HEARTBEAT_INTERVAL_S, int(_OVERRIDE))
else:
    DOWN_THRESHOLD_S = MISSED_BEATS_THRESHOLD * HEARTBEAT_INTERVAL_S + DOWN_GRACE_S

LOOKBACK_WINDOW = max(2 * DOWN_THRESHOLD_S, 300)


def _flux_latest_hb(bucket: str, window_s: int) -> str:
    return (
        f'from(bucket: "{bucket}")\n'
        f'  |> range(start: -{window_s}s)\n'
        f'  |> filter(fn: (r) => r._measurement == "syslog_listener_heartbeat" '
        f'and r._field == "hb_seq")\n'
        f'  |> last()\n'
        f'  |> keep(columns: ["site", "oem", "listener", "_time", "_value"])'
    )


def _flux_latest_msg_count(bucket: str, window_s: int) -> str:
    return (
        f'from(bucket: "{bucket}")\n'
        f'  |> range(start: -{window_s}s)\n'
        f'  |> filter(fn: (r) => r._measurement == "syslog_listener_heartbeat" '
        f'and r._field == "msg_count")\n'
        f'  |> last()\n'
        f'  |> keep(columns: ["site", "oem", "listener", "_value"])'
    )


def _row_ts(row: Dict[str, Any]) -> Optional[float]:
    t = row.get("_time")
    if not t:
        return None
    try:
        return datetime.fromisoformat(str(t).replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


class HealthCheckMonitor:
    """Polls each site's heartbeat bucket; maintains per-listener +
    per-site state; pushes a full snapshot to /ws/listener-health on
    every tick (and on every WS connect, via `hydrate`).

    `alert_monitor` is optional; when supplied the monitor also surfaces
    any unreachable alert-store buckets (one per vendor x site) as
    infra events so the React infrastructure-down modal fires whenever
    ANY Influx container the system depends on goes offline - not just
    the heartbeat stores.
    """

    def __init__(
        self,
        pool: InfluxPool,
        hub: WsHub,
        *,
        alert_monitor: Optional[Any] = None,
    ) -> None:
        self._pool = pool
        self._hub  = hub
        self._alert_monitor = alert_monitor
        self._stop = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._started_at = time.time()

        # listener_id -> dict
        self._listeners: Dict[str, Dict[str, Any]] = {}
        # site -> {reachable, last_check, last_ok, error, event_key}
        self._sites: Dict[str, Dict[str, Any]] = {
            site: {
                "reachable":  None,
                "last_check": None,
                "last_ok":    None,
                "error":      None,
                "event_key":  None,
            }
            for site in HEARTBEAT_PIPELINES
        }
        # listener_id -> {key, id, site, oem, listener, down_since, raised_at, age_s}
        self._down_events: Dict[str, Dict[str, Any]] = {}
        # Heartbeat-store infra events.
        # site -> {key, component, site, since, error}
        self._infra_events: Dict[str, Dict[str, Any]] = {}
        # Alert-store infra events.
        # (site, vendor) -> {key, component, site, vendor, since, error}
        self._alert_infra: Dict[Tuple[str, str], Dict[str, Any]] = {}

        for site, cfg in HEARTBEAT_PIPELINES.items():
            key = "heartbeat:{0}".format(site)
            self._pool.register(
                key,
                url=cfg["url"], token=cfg["token"], org=cfg["org"],
            )

        # Seed all expected listeners as 'unknown' so the WS payload is
        # well-formed from the very first frame.
        for site, listeners in SITE_LISTENERS.items():
            for oem, name in listeners:
                lid = "{0}:{1}".format(site, oem)
                self._listeners[lid] = {
                    "id":         lid,
                    "site":       site,
                    "oem":        oem,
                    "listener":   name,
                    "state":      "unknown",
                    "last_seen":  None,
                    "age_s":      None,
                    "down_since": None,
                    "msg_count":  0,
                    "hb_seq":     0,
                    "event_key":  None,
                }

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="health-check-monitor")

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
            "HealthCheck started - sites=%d poll=%ds threshold>%ds "
            "(%d missed beats x %ds interval + %ds grace)",
            len(HEARTBEAT_PIPELINES), POLL_INTERVAL_S, DOWN_THRESHOLD_S,
            MISSED_BEATS_THRESHOLD, HEARTBEAT_INTERVAL_S, DOWN_GRACE_S,
        )
        while not self._stop.is_set():
            try:
                await self._tick()
                self._scan_alert_buckets()
                await self._broadcast()
            except Exception as exc:  # pragma: no cover - defensive
                log.warning("HealthCheck tick failed: %s", exc)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=POLL_INTERVAL_S)
            except asyncio.TimeoutError:
                continue

    # ------------------------------------------------------------------
    # Tick
    # ------------------------------------------------------------------
    async def _tick(self) -> None:
        now = time.time()
        sites = list(HEARTBEAT_PIPELINES.keys())
        results = await asyncio.gather(*[
            self._fetch_site(site) for site in sites
        ], return_exceptions=True)

        # 1) per-site reachability + infra events
        site_data: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for site, result in zip(sites, results):
            sh = self._sites[site]
            sh["last_check"] = now
            if isinstance(result, Exception):
                ok, err, data = False, "{0}: {1}".format(type(result).__name__, result), {}
            else:
                data, ok, err = result
            prev_reachable = sh["reachable"]
            sh["reachable"] = ok
            sh["error"]     = err
            if ok:
                sh["last_ok"] = now
                if prev_reachable is False and sh["event_key"]:
                    self._infra_events.pop(site, None)
                    sh["event_key"] = None
            else:
                if prev_reachable is True or sh["event_key"] is None:
                    sh["event_key"] = "infra:{0}@{1}".format(site, int(now))
                    self._infra_events[site] = {
                        "key":       sh["event_key"],
                        "component": "heartbeat",
                        "site":      site,
                        "since":     now,
                        "error":     err or "unreachable",
                    }
            site_data[site] = data if ok else {}

        # 2) merge per-listener heartbeat data (only from reachable sites)
        for site, data in site_data.items():
            for lid, payload in data.items():
                row = self._listeners.get(lid)
                if row is None:
                    self._listeners[lid] = {
                        "id":         lid,
                        "site":       payload.get("site", site),
                        "oem":        payload.get("oem", ""),
                        "listener":   payload.get("listener", ""),
                        "state":      "unknown",
                        "last_seen":  None,
                        "age_s":      None,
                        "down_since": None,
                        "msg_count":  0,
                        "hb_seq":     0,
                        "event_key":  None,
                    }
                    row = self._listeners[lid]
                if payload.get("last_seen") is not None:
                    row["last_seen"] = payload["last_seen"]
                    row["msg_count"] = payload.get("msg_count", row.get("msg_count", 0))
                    row["hb_seq"]    = payload.get("hb_seq",    row.get("hb_seq",    0))
                    if payload.get("listener"):
                        row["listener"] = payload["listener"]

        # 3) compute each listener's state
        for lid, row in self._listeners.items():
            site = row["site"]
            sh = self._sites.get(site, {})
            prev_state = row["state"]

            if sh.get("reachable") is False:
                row["state"] = "infra_down"
                row["age_s"] = (now - row["last_seen"]) if row.get("last_seen") else None
                if lid in self._down_events:
                    self._down_events.pop(lid, None)
                row["down_since"] = None
                row["event_key"]  = None
                continue

            last = row.get("last_seen")
            if last is None:
                if (now - self._started_at) > DOWN_THRESHOLD_S:
                    row["state"] = "down"
                else:
                    row["state"] = "down" if prev_state in ("down", "up", "infra_down") else "unknown"
                age = None
            else:
                age = now - last
                row["state"] = "down" if age > DOWN_THRESHOLD_S else "up"
            row["age_s"] = age

            if row["state"] == "down" and prev_state != "down":
                row["down_since"] = now
                row["event_key"]  = "{0}@{1}".format(lid, int(now))
                self._down_events[lid] = {
                    "key":        row["event_key"],
                    "id":         lid,
                    "site":       row["site"],
                    "oem":        row["oem"],
                    "listener":   row["listener"],
                    "down_since": now,
                    "raised_at":  now,
                    "age_s":      age,
                }
                log.warning(
                    "listener DOWN site=%s oem=%s name=%s age=%s",
                    row["site"], row["oem"], row["listener"],
                    "?" if age is None else "{0:.0f}s".format(age),
                )
            elif row["state"] == "up":
                if prev_state == "down":
                    log.info(
                        "listener UP   site=%s oem=%s name=%s",
                        row["site"], row["oem"], row["listener"],
                    )
                row["down_since"] = None
                row["event_key"]  = None
                self._down_events.pop(lid, None)

    async def _fetch_site(
        self, site: str,
    ) -> Tuple[Dict[str, Dict[str, Any]], bool, Optional[str]]:
        cfg = HEARTBEAT_PIPELINES[site]
        key = "heartbeat:{0}".format(site)
        out: Dict[str, Dict[str, Any]] = {}

        # latest hb_seq + _time per listener
        try:
            rows = await self._pool.query(
                key, _flux_latest_hb(cfg["bucket"], LOOKBACK_WINDOW),
            )
        except InfluxQueryError as exc:
            return out, False, exc.reason
        except Exception as exc:
            return out, False, "{0}: {1}".format(type(exc).__name__, exc)

        for row in rows:
            site_tag = row.get("site") or site
            oem      = row.get("oem")
            listener = row.get("listener")
            ts       = _row_ts(row)
            if not (oem and listener and ts):
                continue
            try:
                hb_seq = int(row.get("_value") or 0)
            except (TypeError, ValueError):
                hb_seq = 0
            lid = "{0}:{1}".format(site_tag, oem)
            out[lid] = {
                "site":      site_tag,
                "oem":       oem,
                "listener":  listener,
                "last_seen": ts,
                "hb_seq":    hb_seq,
            }

        # msg_count enrichment — best effort, doesn't change reachability.
        try:
            rows2 = await self._pool.query(
                key, _flux_latest_msg_count(cfg["bucket"], LOOKBACK_WINDOW),
            )
            for row in rows2:
                site_tag = row.get("site") or site
                oem      = row.get("oem")
                if not oem:
                    continue
                try:
                    n = int(row.get("_value") or 0)
                except (TypeError, ValueError):
                    n = 0
                lid = "{0}:{1}".format(site_tag, oem)
                if lid in out:
                    out[lid]["msg_count"] = n
        except Exception:
            pass

        return out, True, None

    # ------------------------------------------------------------------
    # Alert-bucket reachability (per vendor x site Influx container)
    # ------------------------------------------------------------------
    def _scan_alert_buckets(self) -> None:
        """Inspect the AlertMonitor's per-bucket status and raise / clear
        an infra event for any vendor x site alert store that is
        currently unreachable. Mirrors the heartbeat-store flow so the
        InfrastructureDownModal fires for ANY downed Influx container."""
        if self._alert_monitor is None:
            log.debug("scan_alert_buckets: no alert_monitor wired in")
            return
        try:
            statuses = getattr(self._alert_monitor, "_bucket_status", None)
        except Exception:
            statuses = None
        if not isinstance(statuses, dict):
            log.debug("scan_alert_buckets: _bucket_status missing on alert_monitor")
            return
        log.info(
            "scan_alert_buckets: %d tracked, %d down -> %s",
            len(statuses),
            sum(1 for s in statuses.values() if not s.get("ok")),
            [k for k, s in statuses.items() if not s.get("ok")],
        )
        now = time.time()
        seen: set = set()
        for key, st in statuses.items():
            site   = str(st.get("site") or "").upper()
            vendor = str(st.get("vendor") or "").lower()
            if not site or not vendor:
                continue
            pair = (site, vendor)
            seen.add(pair)
            ok    = bool(st.get("ok"))
            error = st.get("error") or "unreachable"
            existing = self._alert_infra.get(pair)
            if ok:
                if existing is not None:
                    self._alert_infra.pop(pair, None)
            else:
                if existing is None:
                    self._alert_infra[pair] = {
                        "key":       "infra:alert:{0}:{1}@{2}".format(site, vendor, int(now)),
                        "component": "alert",
                        "site":      site,
                        "vendor":    vendor,
                        "since":     now,
                        "error":     str(error),
                    }
        # Drop stale entries for buckets the monitor no longer tracks.
        for pair in list(self._alert_infra.keys()):
            if pair not in seen:
                self._alert_infra.pop(pair, None)

    # ------------------------------------------------------------------
    # Snapshot + broadcast
    # ------------------------------------------------------------------
    def snapshot(self) -> Dict[str, Any]:
        listeners = []
        for row in self._listeners.values():
            listeners.append({
                "id":         row["id"],
                "site":       row["site"],
                "oem":        row["oem"],
                "listener":   row["listener"],
                "state":      row["state"],
                "last_seen":  row.get("last_seen"),
                "age_s":      row.get("age_s"),
                "down_since": row.get("down_since"),
                "msg_count":  row.get("msg_count", 0),
                "hb_seq":     row.get("hb_seq", 0),
                "event_key":  row.get("event_key"),
            })
        listeners.sort(key=lambda r: (
            {"down": 0, "infra_down": 1, "unknown": 2, "up": 3}.get(r["state"], 4),
            -(r.get("down_since") or 0),
            r["site"], r["oem"],
        ))
        sites = []
        for site, sh in self._sites.items():
            sites.append({
                "site":       site,
                "reachable":  bool(sh.get("reachable")),
                "last_check": sh.get("last_check"),
                "last_ok":    sh.get("last_ok"),
                "error":      sh.get("error"),
            })
        return {
            "ok":               True,
            "as_of":            time.time(),
            "poll_interval":    POLL_INTERVAL_S,
            "down_threshold_s": DOWN_THRESHOLD_S,
            "sites":            sites,
            "listeners":        listeners,
            "down_events":      sorted(
                self._down_events.values(), key=lambda e: e.get("raised_at", 0),
            ),
            "infra_events":     sorted(
                [
                    *self._infra_events.values(),
                    *self._alert_infra.values(),
                ],
                key=lambda e: e.get("since", 0),
            ),
        }

    async def _broadcast(self) -> None:
        snap = self.snapshot()
        await self._hub.broadcast(
            "listener-health",
            json.dumps({"type": "listener-health", **snap}, default=str),
        )

    async def hydrate(self, ws: Any) -> None:
        """Push a snapshot to a newly-connected client."""
        snap = self.snapshot()
        await ws.send_text(
            json.dumps({"type": "hydrate", **snap}, default=str),
        )
