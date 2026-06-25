"""
Shared async-friendly wrapper around the sync `influxdb-client` package.

The official package is synchronous. Wrapping every Flux query in
`loop.run_in_executor()` keeps the asyncio event loop free to drive the
WebSocket fanout while N buckets are queried in parallel on a pool of
worker threads.

Public surface:

    pool = InfluxPool(executor)
    pool.register("CDVL:hitachi", url=..., token=..., org=..., bucket=...)
    rows = await pool.query("CDVL:hitachi", flux)

Each call returns a list of dicts shaped like a flattened Flux table row;
the underlying client / connection pool is reused across calls.

Python 3.9+ compatible (uses `from __future__ import annotations`).
"""
from __future__ import annotations

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple

from influxdb_client import InfluxDBClient
from influxdb_client.client.exceptions import InfluxDBError
from urllib3.exceptions import HTTPError as Urllib3HTTPError

log = logging.getLogger("unifiedops.influx_pool")


class InfluxQueryError(Exception):
    """Raised by `InfluxPool.query` when an upstream bucket call fails."""

    def __init__(self, key: str, reason: str, *, status: Optional[int] = None) -> None:
        super().__init__(reason)
        self.key    = key
        self.reason = reason
        self.status = status


class InfluxPool:
    """One `InfluxDBClient` per registered bucket + shared executor.

    Bucket registrations are keyed by a free-form string (typically
    "{site}:{vendor}" for alert buckets and "{site}:heartbeat" for the
    heartbeat buckets) so callers can look up their client without
    juggling URL / token combinations.
    """

    def __init__(self, executor: ThreadPoolExecutor, *, default_timeout_ms: int = 3000) -> None:
        self._executor = executor
        self._timeout_ms = default_timeout_ms
        self._clients: Dict[str, InfluxDBClient] = {}
        self._meta:    Dict[str, Dict[str, str]] = {}

    # ------------------------------------------------------------------
    # Registration / introspection
    # ------------------------------------------------------------------
    def register(
        self,
        key: str,
        *,
        url: str,
        token: str,
        org: str,
        verify_ssl: bool = False,
        timeout_ms: Optional[int] = None,
    ) -> None:
        if not token:
            log.warning("InfluxPool: refusing to register %s — empty token", key)
            return
        if key in self._clients:
            return
        self._clients[key] = InfluxDBClient(
            url=url,
            token=token,
            org=org,
            verify_ssl=verify_ssl,
            timeout=timeout_ms or self._timeout_ms,
        )
        self._meta[key] = {"url": url, "org": org}
        log.info("InfluxPool: registered %s -> %s", key, url)

    def registered(self) -> List[str]:
        return list(self._clients.keys())

    def meta(self, key: str) -> Dict[str, str]:
        return self._meta.get(key, {})

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------
    async def query(self, key: str, flux: str) -> List[Dict[str, Any]]:
        """Run a Flux query against the registered bucket and return rows.

        Raises `InfluxQueryError` on any failure so the caller can flip
        the site's reachability flag and the React UI can surface the
        infra-down modal.
        """
        client = self._clients.get(key)
        if client is None:
            raise InfluxQueryError(key, "bucket not registered")
        org = self._meta[key]["org"]
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor, self._query_sync, key, client, org, flux,
        )

    @staticmethod
    def _query_sync(
        key: str,
        client: InfluxDBClient,
        org: str,
        flux: str,
    ) -> List[Dict[str, Any]]:
        t0 = time.perf_counter()
        try:
            tables = client.query_api().query(query=flux, org=org)
        except InfluxDBError as exc:
            status = getattr(exc, "response", None)
            code = getattr(status, "status", None) if status is not None else None
            raise InfluxQueryError(key, f"InfluxDBError: {exc}", status=code) from exc
        except Urllib3HTTPError as exc:
            raise InfluxQueryError(key, f"transport: {exc!s}") from exc
        except Exception as exc:
            raise InfluxQueryError(key, f"{type(exc).__name__}: {exc}") from exc

        rows: List[Dict[str, Any]] = []
        for table in tables:
            for record in table.records:
                values = dict(record.values)
                # `record.get_time()` and `record.get_value()` both raise
                # KeyError if the underlying columns were dropped by a
                # `keep(...)` / aggregate; treat those as optional so a
                # row-count-only query (or a `count()` aggregate) still
                # produces parseable rows.
                try:
                    t = record.get_time()
                except (KeyError, Exception):
                    t = None
                if t is not None:
                    values["_time"] = t.isoformat().replace("+00:00", "Z")
                try:
                    values["_value"] = record.get_value()
                except (KeyError, Exception):
                    if "_value" not in values:
                        values["_value"] = None
                rows.append(values)
        log.debug(
            "InfluxPool.query %s rows=%d elapsed=%.0fms",
            key, len(rows), (time.perf_counter() - t0) * 1000,
        )
        return rows

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def close(self) -> None:
        for k, c in list(self._clients.items()):
            try:
                c.close()
            except Exception:  # pragma: no cover - defensive
                log.warning("InfluxPool: error closing client %s", k)
        self._clients.clear()
        self._meta.clear()


# ---------------------------------------------------------------------------
# WebSocket fanout hub — shared by the alert + listener-health monitors.
# ---------------------------------------------------------------------------
class WsHub:
    """Tracks a set of active WebSocket clients and broadcasts a payload
    to all of them in lock-step. Failed sends drop the client.

    Generic enough to back both `/ws/alerts` and `/ws/listener-health`.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self._conns: set = set()
        self._lock = asyncio.Lock()
        self.last_payload: Optional[Tuple[str, str]] = None  # (type, json_text)

    async def register(self, ws: Any) -> None:
        async with self._lock:
            self._conns.add(ws)

    async def unregister(self, ws: Any) -> None:
        async with self._lock:
            self._conns.discard(ws)

    def size(self) -> int:
        return len(self._conns)

    async def broadcast(self, msg_type: str, payload_json: str) -> int:
        """Send `payload_json` (already serialized) to every client.

        Returns the number of successful sends. Dead clients are dropped.
        """
        async with self._lock:
            if not self._conns:
                self.last_payload = (msg_type, payload_json)
                return 0
            clients = list(self._conns)
        sent = 0
        dead: List[Any] = []
        text = payload_json
        for ws in clients:
            try:
                await ws.send_text(text)
                sent += 1
            except Exception as exc:
                log.info("WsHub[%s] send failed: %s", self.name, exc)
                dead.append(ws)
        if dead:
            async with self._lock:
                for ws in dead:
                    self._conns.discard(ws)
        self.last_payload = (msg_type, text)
        return sent
