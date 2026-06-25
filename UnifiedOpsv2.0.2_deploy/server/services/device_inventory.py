"""Device inventory service.

The OEM cards on the dashboard need two numbers per vendor:

  * total integrated devices  - storage arrays / fabric switches that
    the listener IP whitelist is configured for. Static, comes from
    `server/data/device_inventory.json` (built from the listener
    IP maps via scripts/_build_device_inventory.py).
  * alerting devices in the current range - storage arrays that have
    written at least one alert into Influx in the selected time range.
    Computed at query time from the per-bucket `array_name` tag, then
    intersected with the static inventory so devices that haven't been
    seen yet but are wired in still count towards "total".

Python 3.9+ compatible.
"""
from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from .alert_monitor import VENDOR_BUCKETS
from .influx_pool import InfluxPool, InfluxQueryError

log = logging.getLogger("unifiedops.devices")


_DEFAULT_INVENTORY_PATH = Path(__file__).resolve().parents[1] / "data" / "device_inventory.json"


# Vendor identifiers used everywhere on the bus + frontend.
_VENDORS = ("hitachi", "brocade", "netapp", "dell")
_SITES   = ("CDVL", "BCP", "SIFY", "UAT")


def _load_inventory_json(path: Path) -> Dict[str, Dict[str, List[str]]]:
    """Load the static inventory JSON. Returns an empty skeleton if the
    file is missing (e.g. first boot before pack has been run)."""
    skeleton: Dict[str, Dict[str, List[str]]] = {
        v: {s: [] for s in _SITES} for v in _VENDORS
    }
    if not path.is_file():
        log.warning("device inventory file not found: %s", path)
        return skeleton
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        log.warning("failed to parse %s: %s", path, exc)
        return skeleton
    inv = data.get("inventory") if isinstance(data, dict) else None
    if not isinstance(inv, dict):
        return skeleton
    for vendor in _VENDORS:
        sites = inv.get(vendor) or {}
        if not isinstance(sites, dict):
            continue
        for site in _SITES:
            devices = sites.get(site) or []
            if isinstance(devices, list):
                skeleton[vendor][site] = [str(d) for d in devices if d]
    return skeleton


# ---------------------------------------------------------------------------
# Influx query - distinct array_name per bucket in range
# ---------------------------------------------------------------------------
_MEASUREMENT_FILTER = 'r._measurement != "syslog_listener_heartbeat"'
_COUNT_FIELD_FILTER = 'r._field == "raw_message"'


def _flux_alerting_arrays(bucket: str, range_key: str) -> str:
    """Return one row per distinct alerting array_name in the range.

    `distinct(column: "array_name")` replaces `_value` with the distinct
    array_name and drops the rest, so the consumer just reads `_value`.
    """
    return (
        f'from(bucket: "{bucket}")\n'
        f'  |> range(start: -{range_key})\n'
        f'  |> filter(fn: (r) => {_MEASUREMENT_FILTER})\n'
        f'  |> filter(fn: (r) => {_COUNT_FIELD_FILTER})\n'
        f'  |> filter(fn: (r) => exists r.array_name and r.array_name != "unknown")\n'
        f'  |> keep(columns: ["array_name"])\n'
        f'  |> distinct(column: "array_name")\n'
        f'  |> keep(columns: ["_value"])\n'
    )


def _bucket_key(cfg: Dict[str, str]) -> str:
    return "devices:{0}:{1}".format(cfg["site"], cfg["vendor"])


class DeviceService:
    """Snapshots `{total, alerting}` device counts per vendor + per site."""

    def __init__(
        self,
        pool: InfluxPool,
        inventory_path: Optional[Path] = None,
    ) -> None:
        self._pool = pool
        self._inventory_path = inventory_path or _DEFAULT_INVENTORY_PATH
        self._inventory: Dict[str, Dict[str, List[str]]] = _load_inventory_json(
            self._inventory_path
        )
        # Precompute the (vendor, device) -> site reverse map so we can
        # bucket alerting array_names without scanning the inventory each
        # time.
        self._device_site: Dict[Tuple[str, str], str] = {}
        for v, sites in self._inventory.items():
            for s, names in sites.items():
                for n in names:
                    self._device_site[(v, n)] = s

    # ------------------------------------------------------------------
    # static helpers
    # ------------------------------------------------------------------
    def inventory(self) -> Dict[str, Dict[str, List[str]]]:
        """Raw static map (vendor -> site -> [device names])."""
        return self._inventory

    def total_counts(self) -> Dict[str, Dict[str, Any]]:
        """Per-vendor totals + per-site totals from the static inventory."""
        out: Dict[str, Dict[str, Any]] = {}
        for vendor in _VENDORS:
            sites = self._inventory.get(vendor, {})
            total = sum(len(d) for d in sites.values())
            out[vendor] = {
                "total":   total,
                "by_site": {s: len(sites.get(s, [])) for s in _SITES if sites.get(s)},
            }
        return out

    # ------------------------------------------------------------------
    # live snapshot
    # ------------------------------------------------------------------
    async def get_snapshot(
        self,
        range_key: str,
        sites:   Optional[List[str]] = None,
        vendors: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Return per-vendor + per-site device counts for the given range.

        Shape (matches what the OEM cards render):
            {
              "range": "6h",
              "vendors": {
                 "hitachi": {
                    "total":     82,
                    "alerting":   3,
                    "by_site":   { "CDVL": {"total":28,"alerting":1}, ... },
                 },
                 ...
              },
              "grand_total":     181,
              "grand_alerting":    4
            }
        """
        sites_u   = {s.upper() for s in (sites   or [])}
        vendors_l = {v.lower() for v in (vendors or [])}

        relevant_buckets = []
        for cfg in VENDOR_BUCKETS:
            if sites_u   and cfg["site"]   not in sites_u:   continue
            if vendors_l and cfg["vendor"] not in vendors_l: continue
            if not cfg.get("token"): continue
            relevant_buckets.append(cfg)

        per_bucket_rows = await asyncio.gather(*[
            self._safe_query(_bucket_key(b), _flux_alerting_arrays(b["bucket"], range_key))
            for b in relevant_buckets
        ])

        # (vendor, site) -> set of distinct alerting array_names.
        # distinct() puts the unique array_name into `_value`, so read
        # that first; fall back to `array_name` in case the upstream
        # query shape changes.
        alerting_by_pair: Dict[Tuple[str, str], Set[str]] = {}
        for cfg, rows in zip(relevant_buckets, per_bucket_rows):
            vendor = cfg["vendor"].lower()
            site   = cfg["site"].upper()
            bucket = alerting_by_pair.setdefault((vendor, site), set())
            for r in rows:
                name = r.get("_value") or r.get("array_name")
                if name and name != "unknown":
                    bucket.add(str(name))

        vendors_out: Dict[str, Any] = {}
        grand_total = 0
        grand_alert = 0

        for vendor in _VENDORS:
            if vendors_l and vendor not in vendors_l:
                continue
            sites_inv = self._inventory.get(vendor, {})
            by_site: Dict[str, Dict[str, int]] = {}
            v_total    = 0
            v_alerting_names: Set[str] = set()
            for site in _SITES:
                if sites_u and site not in sites_u:
                    continue
                inventory_names = set(sites_inv.get(site, []))
                alerting_names  = alerting_by_pair.get((vendor, site), set())
                # alerting devices must be in the inventory; unknown
                # array_names (e.g. from listeners with empty IP_FILTER)
                # still count as alerting even if not pre-registered.
                effective_alerting = alerting_names if alerting_names else set()
                site_total    = len(inventory_names)
                site_alerting = len(effective_alerting & inventory_names) \
                                + len(effective_alerting - inventory_names)
                by_site[site] = {"total": site_total, "alerting": site_alerting}
                v_total += site_total
                v_alerting_names |= effective_alerting
            v_alert = len(v_alerting_names)
            vendors_out[vendor] = {
                "total":    v_total,
                "alerting": v_alert,
                "by_site":  by_site,
            }
            grand_total += v_total
            grand_alert += v_alert

        return {
            "range":         range_key,
            "sites":         sorted(sites_u)   if sites_u   else [],
            "vendors":       vendors_out,
            "grand_total":   grand_total,
            "grand_alerting": grand_alert,
        }

    # ------------------------------------------------------------------
    async def _safe_query(self, key: str, flux: str) -> List[Dict[str, Any]]:
        try:
            return await self._pool.query(key, flux)
        except InfluxQueryError as exc:
            log.warning("device %s query failed: %s", key, exc.reason)
            return []
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("device %s query crash: %s", key, exc)
            return []
