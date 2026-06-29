#!/usr/bin/env python3
"""
=============================================================================
  SNMP Trap Listener -> InfluxDB v2  (UnifiedOps -- Dell EMC)
  Location: BCP
=============================================================================

Standalone SNMP v1/v2c listener for the BCP Dell EMC pipeline.
Captures all SNMP trap variables and categorizes them using regex.

    HITRACK_INFLUX_URL      default http://127.0.0.1:8387
    HITRACK_INFLUX_TOKEN    *** required for writes ***
    HITRACK_INFLUX_ORG      default HDFC
    HITRACK_INFLUX_BUCKET   default Dell_BCP_Bucket
    HITRACK_LISTEN_HOST     default 0.0.0.0
    HITRACK_LISTEN_PORT     default 162
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import threading
import time
from datetime import datetime, timezone
from typing import Optional

from pysnmp.entity import engine, config
from pysnmp.carrier.asyncio.dgram import udp
from pysnmp.entity.rfc3413 import ntfrcv

# ---------------------------------------------------------------------------
# Inline regex parsing
# ---------------------------------------------------------------------------
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
    """Return (severity, trap_category) parsed from the aggregated trap body."""
    if not body:
        return "informational", "other"
    
    # Simple severity heuristic from SNMP trap text
    severity = "warning"
    body_lower = body.lower()
    if "critical" in body_lower or "fatal" in body_lower or "fail" in body_lower or "fault" in body_lower:
        severity = "critical"
    elif "error" in body_lower:
        severity = "error"
    elif "info" in body_lower or "clear" in body_lower or "ok" in body_lower:
        severity = "informational"

    category = "other"
    for _cat, _pattern in _CATEGORY_RULES:
        if _pattern.search(body):
            category = _cat
            break
    return severity, category

VENDOR   = "Dell"
LOCATION = "BCP"

INFLUX_URL    = os.environ.get("HITRACK_INFLUX_URL",    "http://127.0.0.1:8387")
INFLUX_TOKEN  = os.environ.get("HITRACK_INFLUX_TOKEN",  "")
INFLUX_ORG    = os.environ.get("HITRACK_INFLUX_ORG",    "HDFC")
INFLUX_BUCKET = os.environ.get("HITRACK_INFLUX_BUCKET", "Dell_BCP_Bucket")

LISTEN_HOST = os.environ.get("HITRACK_LISTEN_HOST", "0.0.0.0")
# Default to port 162 for SNMP Traps
LISTEN_PORT = int(os.environ.get("HITRACK_LISTEN_PORT", "162"))
TEST_MODE   = os.environ.get("HITRACK_TEST_MODE", "0") == "1"

logging.basicConfig(
    level=logging.DEBUG if TEST_MODE else logging.INFO,
    format=f"%(asctime)s [%(levelname)s] dell-bcp: %(message)s",
)
LOG = logging.getLogger("hitrack.listener.dell.bcp")

raw_log = logging.getLogger("raw_snmp_dell_bcp")
raw_log.setLevel(logging.INFO)
raw_fh = logging.FileHandler("syslog_trap_listener_dell_bcp_raw_snmp_data.log")
raw_fh.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
raw_log.addHandler(raw_fh)
raw_log.propagate = False

DELL_IP_MAP = {
    # "10.225.41.50": "PowerStore_5000-BCP",
}

# ---------------------------------------------------------------------------
# Heartbeat — inline per-listener
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
    threading.Thread(target=_heartbeat_loop, daemon=True, name=f"hb-{HB_LISTENER}").start()

_write_api = None
_influx_enabled = bool(INFLUX_TOKEN)

if _influx_enabled:
    try:
        from influxdb_client import InfluxDBClient, Point, WritePrecision
        from influxdb_client.client.write_api import SYNCHRONOUS

        _client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        _write_api = _client.write_api(write_options=SYNCHRONOUS)
        LOG.info("InfluxDB writer enabled -> %s (bucket=%s)", INFLUX_URL, INFLUX_BUCKET)
    except Exception as exc:
        LOG.warning("InfluxDB connect failed (%s) - falling back to log-only", exc)
        _influx_enabled = False
        _write_api = None
else:
    LOG.warning("HITRACK_INFLUX_TOKEN not set - running in log-only mode")

def _record(source_ip, raw_message):
    global _msg_count
    _msg_count += 1
    
    if raw_message:
        raw_log.info(f"[{source_ip}] {raw_message}")

    preview = raw_message[:240]
    array_name = DELL_IP_MAP.get(source_ip, "unknown")
    severity, trap_category = parse_event(raw_message)
    
    LOG.info("Trap from %s (%s) [%s] :: %s", source_ip, array_name, severity, preview)
    
    if not _influx_enabled or _write_api is None:
        return
        
    try:
        point = (
            Point("dell_event")
            .tag("vendor", VENDOR).tag("location", LOCATION)
            .tag("source_ip", source_ip).tag("array_name", array_name)
            .tag("hostname", array_name)
            .tag("severity", severity)
            .tag("trap_category", trap_category)
            .field("bytes", len(raw_message)).field("preview", preview)
            .field("raw_message", raw_message)
            .field("error_message", raw_message)
            .time(datetime.now(timezone.utc), WritePrecision.NS)
        )
        _write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
    except Exception as exc:
        LOG.warning("Influx write failed: %s", exc)

def _on_trap_received(snmpEngine, stateReference, contextEngineId, contextName, varBinds, cbCtx):
    transportDomain, transportAddress = snmpEngine.msgAndPduDsp.getTransportInfo(stateReference)
    source_ip = transportAddress[0] if transportAddress else "unknown"
    
    parts = []
    for name, val in varBinds:
        val_str = val.prettyPrint()
        if val_str and val_str.strip():
            parts.append(val_str)
            
    aggregated_message = " | ".join(parts)
    _record(source_ip, aggregated_message)

async def _snmp_loop():
    snmpEngine = engine.SnmpEngine()
    
    try:
        config.add_transport(
            snmpEngine,
            udp.DOMAIN_NAME,
            udp.UdpTransport().open_server_mode((LISTEN_HOST, LISTEN_PORT))
        )
        LOG.info("UDP %d ready for SNMP traps", LISTEN_PORT)
    except Exception as exc:
        LOG.error("Failed to bind SNMP on UDP %d: %s", LISTEN_PORT, exc)
        return

    config.add_v1_system(snmpEngine, 'dell-area', 'public')

    ntfrcv.NotificationReceiver(snmpEngine, _on_trap_received)

    while True:
        await asyncio.sleep(3600)

def main():
    LOG.info("=" * 60)
    LOG.info(" Dell SNMP Listener (BCP) - starting up")
    LOG.info(" Influx URL    : %s", INFLUX_URL)
    LOG.info(" Influx bucket : %s", INFLUX_BUCKET)
    LOG.info(" Bind          : %s:%d", LISTEN_HOST, LISTEN_PORT)
    LOG.info(" IP_FILTER     : %d entries", len(DELL_IP_MAP))
    LOG.info("=" * 60)
    _start_heartbeat()

    try:
        asyncio.run(_snmp_loop())
    except KeyboardInterrupt:
        LOG.info("Shutdown requested")

if __name__ == "__main__":
    main()
