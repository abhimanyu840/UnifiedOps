#!/usr/bin/env python3
"""
=============================================================================
  Syslog Trap Listener -> InfluxDB v2  (UnifiedOps -- NetApp ONTAP / EMS)
  Location: CDVL
=============================================================================

Standalone listener for the CDVL NetApp pipeline. Receives NetApp ONTAP
EMS (Event Management System) syslog forwards on UDP/TCP and writes
parsed events into a dedicated Influx bucket on the local CDVL VM.

This is a *placeholder* parser — the binding and Influx writer are
production-grade, but the trap-body parsing is intentionally minimal
until the real NetApp EMS schema is reverse-engineered for this site.
The point shape is:

    measurement = "netapp_event"
    tags        = vendor=NetApp, location=CDVL, source_ip, hostname?
    fields      = bytes, preview (truncated body), severity_raw?

A future iteration replaces the parser with the proper ONTAP EMS struct.

Configuration overrides (typical: /etc/hi-track/listener.netapp.cdvl.env):

    HITRACK_INFLUX_URL      default http://127.0.0.1:8286
    HITRACK_INFLUX_TOKEN    *** required for writes ***
    HITRACK_INFLUX_ORG      default HDFC
    HITRACK_INFLUX_BUCKET   default NetApp_CDVL_Bucket
    HITRACK_LISTEN_HOST     default 0.0.0.0
    HITRACK_LISTEN_PORT     default 516           (TCP listens on +1)
    HITRACK_TEST_MODE       "1" to enable verbose logging for dev
"""
from __future__ import annotations

import logging
import os
import re
import socket
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


# ---------------------------------------------------------------------------
# LOCATION / VENDOR
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Inline syslog body parser (severity from <PRI>, category from keyword regex).
# Previously lived in `_syslog_helpers.py`; inlined so this listener stays
# completely self-contained (no sibling-module imports).
# ---------------------------------------------------------------------------
_PRI_RE = re.compile(r"<(?P<pri>\d{1,3})>")

_PRI_SEVERITY = {
    0: "critical",   # emergency
    1: "critical",   # alert
    2: "critical",   # critical
    3: "error",
    4: "warning",
    5: "notice",
    6: "informational",
    7: "informational",  # debug
}

_CATEGORY_RULES = (
    ("disk_failure",       re.compile(r"\b(disk|drive|hdd|ssd|nvme)\b.*\b(fail|fault|error|bad)\b", re.I)),
    ("disk_failure",       re.compile(r"\bdisk\.(fail|error|fault)", re.I)),
    ("controller_fault",   re.compile(r"\bcontroller\b.*\b(fail|fault|takeover|offline|down)\b", re.I)),
    ("controller_fault",   re.compile(r"node.fault|computeNodeFault", re.I)),
    ("power_failure",      re.compile(r"\bpower\b.*\b(fail|loss|down|fault)\b|\bpsu\b", re.I)),
    ("temperature_alarm",  re.compile(r"temp(erature)?|thermal|overheat", re.I)),
    ("fan_failure",        re.compile(r"\bfan\b|\bblower\b", re.I)),
    ("battery_alert",      re.compile(r"\bbattery\b|\bbbu\b|nvram.*battery", re.I)),
    ("raid_degraded",      re.compile(r"raid.*(degraded|rebuild|fail)", re.I)),
    ("volume_alert",       re.compile(r"\bvolume\b.*\b(full|offline|error|threshold|capacity)\b|\baggr\b.*\bfull\b", re.I)),
    ("snapshot_alert",     re.compile(r"snapshot|\bsnap\b.*(full|fail|create|delete)", re.I)),
    ("replication_alert",  re.compile(r"\breplication\b|snapmirror|\bsrdf\b|metrocluster", re.I)),
    ("port_fault",         re.compile(r"\b(port|link|fcp|iscsi)\b.*\b(down|fail|fault|offline|disabled)\b", re.I)),
    ("firmware_alert",     re.compile(r"firmware|microcode", re.I)),
    ("config_change",      re.compile(r"config(uration)?.*\b(change|modify|update|set)\b|lun.*\b(create|delete|map|unmap)\b", re.I)),
    ("auth_failure",       re.compile(r"(auth|login|ssh|console)\s*(fail|denied|error|invalid)", re.I)),
    ("license_alert",      re.compile(r"\blicense\b", re.I)),
    ("env_warning",        re.compile(r"hwHealthStateChanged|health.*alert|callhome", re.I)),
)


def parse_event(body):
    """Return (severity, trap_category) parsed from a syslog body."""
    if not body:
        return "informational", "other"
    s = body.lstrip()
    if s.startswith("[SOURCE_IP="):
        cut = s.find("] ")
        if cut > 0:
            s = s[cut + 2:].lstrip()
    severity = "informational"
    m = _PRI_RE.match(s)
    if m:
        try:
            severity = _PRI_SEVERITY.get(int(m.group("pri")) & 0x07, "informational")
        except (TypeError, ValueError):
            pass
    category = "other"
    for _cat, _pattern in _CATEGORY_RULES:
        if _pattern.search(body):
            category = _cat
            break
    return severity, category


VENDOR   = "NetApp"
LOCATION = "CDVL"

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
INFLUX_URL    = os.environ.get("HITRACK_INFLUX_URL",    "http://127.0.0.1:8286")
INFLUX_TOKEN  = os.environ.get("HITRACK_INFLUX_TOKEN",  "")
INFLUX_ORG    = os.environ.get("HITRACK_INFLUX_ORG",    "HDFC")
INFLUX_BUCKET = os.environ.get("HITRACK_INFLUX_BUCKET", "NetApp_CDVL_Bucket")

LISTEN_HOST = os.environ.get("HITRACK_LISTEN_HOST", "0.0.0.0")
LISTEN_PORT = int(os.environ.get("HITRACK_LISTEN_PORT", "516"))
TEST_MODE   = os.environ.get("HITRACK_TEST_MODE", "0") == "1"

BUFFER_SIZE = 8192

logging.basicConfig(
    level=logging.DEBUG if TEST_MODE else logging.INFO,
    format=f"%(asctime)s [%(levelname)s] netapp-cdvl: %(message)s",
)
LOG = logging.getLogger("hitrack.listener.netapp.cdvl")

raw_log = logging.getLogger("raw_syslog_netapp_cdvl")
raw_log.setLevel(logging.INFO)
raw_fh = logging.FileHandler("syslog_trap_listener_netapp_cdvl_raw_syslog_data.log")
raw_fh.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
raw_log.addHandler(raw_fh)
raw_log.propagate = False

# ---------------------------------------------------------------------------
# NetApp filer IP -> hostname map (placeholder; refresh from inventory)
# ---------------------------------------------------------------------------
NETAPP_IP_MAP = {
    # "10.227.62.18":  "FAS_8200_4187-CDVL",
    # "10.227.62.19":  "AFF_A800_2210-CDVL",
}

_HOSTNAME_RE = re.compile(r"<\d+>\d+\s+\S+\s+(\S+)\s")

# ---------------------------------------------------------------------------
# Heartbeat — each listener is its own service, so the heartbeat config /
# counter / loop live INSIDE this file (no shared helper module).
# ---------------------------------------------------------------------------
HB_URL      = os.environ.get("HITRACK_HEARTBEAT_URL",    "").strip()
HB_TOKEN    = os.environ.get("HITRACK_HEARTBEAT_TOKEN",  "").strip()
HB_ORG      = os.environ.get("HITRACK_HEARTBEAT_ORG",    "HDFC").strip()
HB_BUCKET   = os.environ.get("HITRACK_HEARTBEAT_BUCKET", "").strip()
HB_INTERVAL = max(5, int(os.environ.get("HITRACK_HEARTBEAT_INTERVAL", "15")))
HB_LISTENER = f"{VENDOR.lower()}-{LOCATION.lower()}"

_msg_count: int = 0


def _heartbeat_loop() -> None:
    if not (HB_URL and HB_TOKEN and HB_BUCKET):
        LOG.info("heartbeat disabled - HITRACK_HEARTBEAT_URL/TOKEN/BUCKET not set")
        return
    try:
        from influxdb_client import InfluxDBClient, Point, WritePrecision
        from influxdb_client.client.write_api import SYNCHRONOUS

        hb_client = InfluxDBClient(url=HB_URL, token=HB_TOKEN, org=HB_ORG)
        hb_write  = hb_client.write_api(write_options=SYNCHRONOUS)
    except Exception as exc:
        LOG.warning("heartbeat disabled - influx client init failed: %s", exc)
        return

    started_at = time.time()
    seq = 0
    LOG.info("heartbeat thread up -> %s/%s every %ds", HB_URL, HB_BUCKET, HB_INTERVAL)
    while True:
        try:
            seq += 1
            point = (
                Point("syslog_listener_heartbeat")
                .tag("listener", HB_LISTENER)
                .tag("site",     LOCATION)
                .tag("oem",      VENDOR)
                .field("alive",       True)
                .field("msg_count",   int(_msg_count))
                .field("queue_depth", 0)
                .field("uptime_s",    int(time.time() - started_at))
                .field("hb_seq",      seq)
                .time(datetime.now(timezone.utc), WritePrecision.NS)
            )
            hb_write.write(bucket=HB_BUCKET, org=HB_ORG, record=point)
        except Exception as exc:
            LOG.warning("heartbeat write failed: %s", exc)
        time.sleep(HB_INTERVAL)


def _start_heartbeat() -> None:
    threading.Thread(
        target=_heartbeat_loop, daemon=True, name=f"hb-{HB_LISTENER}",
    ).start()

# ---------------------------------------------------------------------------
# Influx writer (best-effort)
# ---------------------------------------------------------------------------
_write_api = None
_influx_enabled = bool(INFLUX_TOKEN)

if _influx_enabled:
    try:
        from influxdb_client import InfluxDBClient, Point, WritePrecision
        from influxdb_client.client.write_api import SYNCHRONOUS

        _client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        _write_api = _client.write_api(write_options=SYNCHRONOUS)
        LOG.info("InfluxDB writer enabled -> %s (bucket=%s)", INFLUX_URL, INFLUX_BUCKET)
    except Exception as exc:  # pragma: no cover
        LOG.warning("InfluxDB connect failed (%s) - falling back to log-only", exc)
        _influx_enabled = False
        _write_api = None
else:
    LOG.warning("HITRACK_INFLUX_TOKEN not set - running in log-only mode")


_msg_count = 0


def _record(source_ip: str, raw: bytes) -> None:
    global _msg_count
    _msg_count += 1
    try:
        raw_str = raw.decode("utf-8", errors="replace").strip()
        if raw_str:
            raw_log.info(f"[{source_ip}] {raw_str}")
    except Exception:
        pass

    body = raw.decode("utf-8", errors="replace").strip()
    preview = body.replace("\n", " ")[:240]
    hostname = ""
    m = _HOSTNAME_RE.search(body)
    if m:
        hostname = m.group(1)

    array_name = NETAPP_IP_MAP.get(source_ip, hostname or "unknown")
    severity, trap_category = parse_event(body)

    LOG.info("%d bytes from %s (%s) :: %s", len(raw), source_ip, array_name, preview)

    if not _influx_enabled or _write_api is None:
        return

    try:
        point = (
            Point("netapp_event")
            .tag("vendor", VENDOR)
            .tag("location", LOCATION)
            .tag("source_ip", source_ip)
            .tag("array_name", array_name)
            .tag("hostname", hostname or "unknown")
            .tag("severity", severity)
            .tag("trap_category", trap_category)
            .field("bytes", len(raw)).field("preview", preview).field("raw_message", body)
            .field("error_message", body)
            .time(datetime.now(timezone.utc), WritePrecision.NS)
        )
        _write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
    except Exception as exc:  # pragma: no cover
        LOG.warning("Influx write failed: %s", exc)


# ---------------------------------------------------------------------------
# UDP / TCP listeners
# ---------------------------------------------------------------------------
def _udp_loop(sock: socket.socket) -> None:
    while True:
        try:
            data, addr = sock.recvfrom(BUFFER_SIZE)
            _record(addr[0], data)
        except OSError:
            break


def _tcp_client(conn: socket.socket, addr) -> None:
    try:
        buf = b""
        conn.settimeout(30)
        while True:
            chunk = conn.recv(BUFFER_SIZE)
            if not chunk:
                break
            buf += chunk
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                if line.strip():
                    _record(addr[0], line)
        if buf.strip():
            _record(addr[0], buf)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _tcp_loop(sock: socket.socket) -> None:
    while True:
        try:
            conn, addr = sock.accept()
            threading.Thread(target=_tcp_client, args=(conn, addr), daemon=True).start()
        except OSError:
            break


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------
def main() -> None:
    LOG.info("=" * 60)
    LOG.info(" NetApp Syslog Listener (CDVL) - starting up")
    LOG.info(" Influx URL    : %s", INFLUX_URL)
    LOG.info(" Influx bucket : %s", INFLUX_BUCKET)
    LOG.info(" Bind          : %s:%d", LISTEN_HOST, LISTEN_PORT)
    LOG.info(" IP_FILTER     : %d entries", len(NETAPP_IP_MAP))
    LOG.info("=" * 60)

    _start_heartbeat()

    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp.bind((LISTEN_HOST, LISTEN_PORT))

    tcp_sock: Optional[socket.socket] = None
    try:
        tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp_sock.bind((LISTEN_HOST, LISTEN_PORT + 1))
        tcp_sock.listen(50)
        LOG.info("UDP %d / TCP %d ready", LISTEN_PORT, LISTEN_PORT + 1)
    except OSError as exc:
        LOG.warning("TCP bind on %d failed: %s", LISTEN_PORT + 1, exc)
        tcp_sock = None

    udp_thread = threading.Thread(target=_udp_loop, args=(udp,), daemon=True)
    udp_thread.start()
    if tcp_sock is not None:
        threading.Thread(target=_tcp_loop, args=(tcp_sock,), daemon=True).start()

    try:
        udp_thread.join()
    except KeyboardInterrupt:
        LOG.info("Shutdown requested")
    finally:
        try:
            udp.close()
        except Exception:
            pass
        if tcp_sock is not None:
            try:
                tcp_sock.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
