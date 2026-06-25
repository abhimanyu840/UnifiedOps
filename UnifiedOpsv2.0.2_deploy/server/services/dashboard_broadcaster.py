"""
Dashboard broadcaster.

Holds the WS-side fan-out for /ws/dashboard:
  - tracks each connected client's subscription (range, sites, vendors)
  - runs an independent asyncio loop that, every N seconds, computes one
    `DashboardService.get_snapshot()` for each UNIQUE subscription key,
    then pushes the snapshot to all clients subscribed under that key
  - immediately re-pushes when a client (re-)subscribes so the UI never
    waits a full tick for the first paint

All InfluxDB work goes through the shared `InfluxPool` thread pool, so
N concurrent dashboard subscribers cost ONE Flux fan-out per unique
subscription key — not N.

Python 3.9+ compatible.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from fastapi import WebSocket

from services.dashboard import DashboardService, RANGE_DURATIONS_S

log = logging.getLogger("unifiedops.dashboard_broadcaster")


POLL_INTERVAL_S = max(2, int(os.environ.get("HITRACK_DASHBOARD_POLL_SECS", "5")))


class SubscriptionKey:
    """Hashable view onto a (range, sites, vendors) subscription."""
    __slots__ = ("range", "sites", "vendors", "_h")

    def __init__(
        self,
        range_key: str,
        sites:     Tuple[str, ...],
        vendors:   Tuple[str, ...],
    ) -> None:
        self.range = range_key
        self.sites = sites
        self.vendors = vendors
        self._h = hash((range_key, sites, vendors))

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, SubscriptionKey)
            and self.range == other.range
            and self.sites == other.sites
            and self.vendors == other.vendors
        )

    def __hash__(self) -> int:
        return self._h

    def as_dict(self) -> Dict[str, Any]:
        return {
            "range":   self.range,
            "sites":   list(self.sites),
            "vendors": list(self.vendors),
        }


def _norm(values: Optional[List[str]]) -> Tuple[str, ...]:
    return tuple(sorted(set(v.strip() for v in (values or []) if v)))


class DashboardBroadcaster:
    """Owns the /ws/dashboard fan-out — per-client subscriptions + a
    shared snapshot-per-unique-key push loop."""

    def __init__(self, service: DashboardService) -> None:
        self._service = service
        self._subs:   Dict[WebSocket, SubscriptionKey] = {}
        self._lock = asyncio.Lock()
        self._stop = asyncio.Event()
        self._task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="dashboard-broadcaster")

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
        log.info("DashboardBroadcaster started — poll=%ds", POLL_INTERVAL_S)
        while not self._stop.is_set():
            try:
                await self._tick()
            except Exception as exc:  # pragma: no cover - defensive
                log.warning("DashboardBroadcaster tick failed: %s", exc)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=POLL_INTERVAL_S)
            except asyncio.TimeoutError:
                continue

    # ------------------------------------------------------------------
    # Subscriptions
    # ------------------------------------------------------------------
    async def register(self, ws: WebSocket) -> None:
        """Register a fresh client. No subscription is set yet — the
        client must send `{type: "subscribe", range, sites, vendors}`."""
        async with self._lock:
            # Insert with a default subscription so the first tick can
            # push something even before the client explicitly subscribes.
            self._subs[ws] = SubscriptionKey("6h", _norm(None), _norm(None))

    async def unregister(self, ws: WebSocket) -> None:
        async with self._lock:
            self._subs.pop(ws, None)

    async def subscribe(
        self,
        ws:        WebSocket,
        range_key: str,
        sites:     Optional[List[str]],
        vendors:   Optional[List[str]],
    ) -> None:
        if range_key not in RANGE_DURATIONS_S:
            range_key = "6h"
        key = SubscriptionKey(range_key, _norm(sites), _norm(vendors))
        async with self._lock:
            self._subs[ws] = key
        # Immediate push so the client doesn't wait a whole tick for the
        # first snapshot under the new subscription.
        await self._push_one(ws, key)

    def size(self) -> int:
        return len(self._subs)

    # ------------------------------------------------------------------
    # Per-tick fan-out
    # ------------------------------------------------------------------
    async def _tick(self) -> None:
        async with self._lock:
            if not self._subs:
                return
            # Group clients by subscription key.
            groups: Dict[SubscriptionKey, List[WebSocket]] = {}
            for ws, key in self._subs.items():
                groups.setdefault(key, []).append(ws)

        # Compute snapshots for each unique key in parallel — one Flux
        # fan-out per (range, sites, vendors) tuple no matter how many
        # subscribers are on it.
        unique_keys = list(groups.keys())
        snapshots = await asyncio.gather(*[
            self._service.get_snapshot(k.range, list(k.sites), list(k.vendors))
            for k in unique_keys
        ], return_exceptions=True)

        for key, snap in zip(unique_keys, snapshots):
            if isinstance(snap, Exception):
                log.warning(
                    "dashboard snapshot compute failed (range=%s sites=%s vendors=%s): %s",
                    key.range, key.sites, key.vendors, snap,
                )
                continue
            await self._broadcast_to_group(groups.get(key, []), key, snap)

    async def _push_one(self, ws: WebSocket, key: SubscriptionKey) -> None:
        try:
            snap = await self._service.get_snapshot(
                key.range, list(key.sites), list(key.vendors),
            )
        except Exception as exc:
            log.warning("dashboard immediate push failed: %s", exc)
            return
        await self._broadcast_to_group([ws], key, snap)

    async def _broadcast_to_group(
        self,
        ws_list: List[WebSocket],
        key:     SubscriptionKey,
        snap:    Dict[str, Any],
    ) -> None:
        if not ws_list:
            return
        payload = {
            "type":         "dashboard",
            "subscription": key.as_dict(),
            "snapshot":     snap,
            "sent_at":      time.time(),
        }
        text = json.dumps(payload, default=str)
        dead: List[WebSocket] = []
        for ws in ws_list:
            try:
                await ws.send_text(text)
            except Exception as exc:
                log.info("ws/dashboard send failed (%s); marking client dead", exc)
                dead.append(ws)
        if dead:
            async with self._lock:
                for ws in dead:
                    self._subs.pop(ws, None)
