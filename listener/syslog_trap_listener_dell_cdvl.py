#!/usr/bin/env python3
"""
=============================================================================
  SNMP Trap Listener -> InfluxDB v2  (UnifiedOps -- Dell EMC)
  Location: CDVL
=============================================================================

Self-contained SNMP trap receiver for the CDVL Dell storage estate.
Listens on UDP 162 for SNMPv1 / v2c / v3 traps, decodes device-specific 
OID structures, filters to HARDWARE-ONLY alerts, and writes to InfluxDB v2.
"""
from __future__ import annotations

import os
import json
import warnings
try:
    from cryptography.utils import CryptographyDeprecationWarning
    warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)
except ImportError:
    pass
warnings.filterwarnings("ignore", category=DeprecationWarning, module="pysnmp")

# Enable pysnmp debugging if requested
if os.environ.get("HITRACK_DEBUG", "").lower() == "true":
    import logging
    logging.getLogger('pysnmp').setLevel(logging.DEBUG)
import re
import threading
import time
import logging
import ipaddress
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS, WriteOptions

try:
    from pysnmp.entity import engine as snmp_engine, config as snmp_config
    from pysnmp.entity.rfc3413 import ntfrcv
    from pysnmp.carrier.asyncio.dgram import udp as snmp_udp
except ImportError as _err:
    raise SystemExit(
        "pysnmplib is required. Install with: pip install pysnmplib influxdb-client\n"
        f"Original error: {_err}"
    )

LOCATION = "CDVL"
VENDOR   = "Dell"

INFLUX_URL    = os.environ.get("HITRACK_INFLUX_URL",    "http://127.0.0.1:8086")
INFLUX_TOKEN  = os.environ.get("HITRACK_INFLUX_TOKEN",  "hitrack-dev-token-please-change")
INFLUX_ORG    = os.environ.get("HITRACK_INFLUX_ORG",    "HDFC")
INFLUX_BUCKET = os.environ.get("HITRACK_INFLUX_BUCKET", "SNMP_DELL_Bucket")

LISTEN_HOST = os.environ.get("HITRACK_LISTEN_HOST", "0.0.0.0")
LISTEN_PORT = int(os.environ.get("HITRACK_LISTEN_PORT", "162"))

SNMP_COMMUNITY = os.environ.get("HITRACK_SNMP_COMMUNITY", "public")

V3_USER       = os.environ.get("HITRACK_V3_USER",       "").strip()
V3_AUTH_KEY   = os.environ.get("HITRACK_V3_AUTH_KEY",   "").strip()
V3_PRIV_KEY   = os.environ.get("HITRACK_V3_PRIV_KEY",   "").strip()
V3_AUTH_PROTO = os.environ.get("HITRACK_V3_AUTH_PROTO", "SHA").upper()
V3_PRIV_PROTO = os.environ.get("HITRACK_V3_PRIV_PROTO", "AES").upper()

LOG_LEVEL = logging.INFO

WORKER_THREADS           = max(2, int(os.environ.get("HITRACK_WORKER_THREADS",   "16")))
WRITE_BATCH              = os.environ.get("HITRACK_WRITE_BATCH", "1").lower() in ("1", "true", "yes", "on")
WRITE_BATCH_SIZE         = max(1, int(os.environ.get("HITRACK_WRITE_BATCH_SIZE", "200")))
WRITE_FLUSH_MS           = max(50, int(os.environ.get("HITRACK_WRITE_FLUSH_MS",  "1000")))
WRITE_JITTER_MS          = max(0,  int(os.environ.get("HITRACK_WRITE_JITTER_MS", "0")))
WRITE_RETRY_INTERVAL_MS  = max(50, int(os.environ.get("HITRACK_WRITE_RETRY_MS",  "1000")))

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"snmp_trap_listener_{LOCATION.lower()}.log"),
    ],
)
log = logging.getLogger(f"snmp_trap_listener_{LOCATION.lower()}")

raw_log = logging.getLogger(f"raw_snmp_{LOCATION.lower()}")
raw_log.setLevel(logging.INFO)
_raw_fh = logging.FileHandler(f"snmp_trap_listener_{LOCATION.lower()}_raw.log")
_raw_fh.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
raw_log.addHandler(_raw_fh)
raw_log = logging.getLogger(f"raw_snmp_{LOCATION.lower()}")
raw_log.setLevel(logging.INFO)
_raw_fh = logging.FileHandler(f"snmp_trap_listener_{LOCATION.lower()}_raw.log")
_raw_fh.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
raw_log.addHandler(_raw_fh)
raw_log.propagate = False

decoded_log = logging.getLogger(f"decoded_snmp_{LOCATION.lower()}")
decoded_log.setLevel(logging.INFO)
_dec_fh = logging.FileHandler(f"snmp_trap_listener_{LOCATION.lower()}_decoded.log")
_dec_fh.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
decoded_log.addHandler(_dec_fh)
decoded_log.propagate = False

HB_URL      = os.environ.get("HITRACK_HEARTBEAT_URL",    "").strip()
HB_TOKEN    = os.environ.get("HITRACK_HEARTBEAT_TOKEN",  "").strip()
HB_ORG      = os.environ.get("HITRACK_HEARTBEAT_ORG",    "HDFC").strip()
HB_BUCKET   = os.environ.get("HITRACK_HEARTBEAT_BUCKET", "").strip()
HB_INTERVAL = max(5, int(os.environ.get("HITRACK_HEARTBEAT_INTERVAL", "15")))
HB_LISTENER = f"{VENDOR.lower()}-{LOCATION.lower()}"

_trap_count: int = 0

def _heartbeat_loop() -> None:
    if not (HB_URL and HB_TOKEN and HB_BUCKET):
        log.info("heartbeat disabled - HITRACK_HEARTBEAT_URL/TOKEN/BUCKET not set")
        return
    try:
        hb_client = InfluxDBClient(url=HB_URL, token=HB_TOKEN, org=HB_ORG, verify_ssl=False)
        hb_write  = hb_client.write_api(write_options=SYNCHRONOUS)
    except Exception as exc:
        log.warning("heartbeat disabled - influx init failed: %s", exc)
        return

    started_at = time.time()
    seq = 0
    log.info("heartbeat -> %s/%s every %ds", HB_URL, HB_BUCKET, HB_INTERVAL)
    while True:
        try:
            seq += 1
            pt = (
                Point("snmp_listener_heartbeat")
                .tag("listener", HB_LISTENER)
                .tag("site",     LOCATION)
                .tag("oem",      VENDOR)
                .field("alive",       True)
                .field("trap_count",  int(_trap_count))
                .field("queue_depth", 0)
                .field("uptime_s",    int(time.time() - started_at))
                .field("hb_seq",      seq)
                .time(datetime.now(timezone.utc), WritePrecision.NS)
            )
            hb_write.write(bucket=HB_BUCKET, org=HB_ORG, record=pt)
        except Exception as exc:
            log.warning("heartbeat write failed: %s", exc)
        time.sleep(HB_INTERVAL)

def _start_heartbeat() -> None:
    threading.Thread(target=_heartbeat_loop, daemon=True,
                     name=f"hb-{HB_LISTENER}").start()

IP_FILTER: dict[str, str] = {
    "10.227.66.72":   "unity_storage",
    "10.227.65.59":   "powermax_storage",
    "10.227.65.60":   "powermax_storage",
    "10.227.65.61":   "powermax_storage",
    "10.227.65.62":   "powermax_storage",
    "10.227.66.187":  "powermax_storage",
    "10.227.66.188":  "powermax_storage",
    "10.227.66.189":  "powermax_storage",
    "10.227.66.190":  "powermax_storage",
    "10.226.157.202": "powermax_storage",
    "10.226.157.203": "powermax_storage",
    "10.226.157.204": "powermax_storage",
    "10.226.157.205": "powermax_storage",
    "10.226.157.206": "powermax_storage",
    "10.226.157.207": "powermax_storage",
    "10.226.157.208": "powermax_storage",
    "10.226.157.209": "powermax_storage",
    "10.229.232.211": "powermax_storage",
    "10.229.232.212": "powermax_storage",
    "10.227.66.74":   "powervault_storage",
    "10.227.66.75":   "powervault_storage",
}

IP_TO_STORAGE_NAME: dict[str, str] = {
    "10.227.66.72":   "Unity_XT480_CKM01221805039-CDVL",
    "10.227.65.59":   "PowerMAX8500_CK220201130-CDVL",
    "10.227.65.60":   "PowerMAX8500_CK220201130-CDVL",
    "10.227.65.61":   "PowerMAX8500_CK220201130-CDVL",
    "10.227.65.62":   "PowerMAX8500_CK220201130-CDVL",
    "10.227.66.187":  "PowerMAX8500_CK220201147-CDVL",
    "10.227.66.188":  "PowerMAX8500_CK220201147-CDVL",
    "10.227.66.189":  "PowerMAX8500_CK220201147-CDVL",
    "10.227.66.190":  "PowerMAX8500_CK220201147-CDVL",
    "10.226.157.202": "PowerMAX8500_CK220201143-CDVL",
    "10.226.157.203": "PowerMAX8500_CK220201143-CDVL",
    "10.226.157.204": "PowerMAX8500_CK220201143-CDVL",
    "10.226.157.205": "PowerMAX8500_CK220201143-CDVL",
    "10.226.157.206": "PowerMAX8500_CK220201151-CDVL",
    "10.226.157.207": "PowerMAX8500_CK220201151-CDVL",
    "10.226.157.208": "PowerMAX8500_CK220201151-CDVL",
    "10.226.157.209": "PowerMAX8500_CK220201151-CDVL",
    "10.229.232.211": "PowerMAX8500_CK220201144_UAT-CDVL",
    "10.229.232.212": "PowerMAX8500_CK220201144_UAT-CDVL",
    "10.227.66.74":   "PowerVault_GSCDM54-CDVL",
    "10.227.66.75":   "PowerVault_GSCDM54-CDVL",
}

# Add your extracted hex Engine IDs here for statically registering SNMPv3 users.
KNOWN_ENGINE_IDS: dict[str, bytes] = {
    "10.227.65.59": bytes.fromhex("8000047304323230323031313330"),
    "10.227.65.60": bytes.fromhex("8000047304323230323031313330"),
    "10.227.65.61": bytes.fromhex("8000047304323230323031313330"),
    "10.227.65.62": bytes.fromhex("8000047304323230323031313330"),
    "10.227.66.187": bytes.fromhex("8000047304323230323031313437"),
    "10.227.66.188": bytes.fromhex("8000047304323230323031313437"),
    "10.227.66.189": bytes.fromhex("8000047304323230323031313437"),
    "10.227.66.190": bytes.fromhex("8000047304323230323031313437"),
    "10.226.157.202": bytes.fromhex("8000047304323230323031313433"),
    "10.226.157.203": bytes.fromhex("8000047304323230323031313433"),
    "10.226.157.204": bytes.fromhex("8000047304323230323031313433"),
    "10.226.157.205": bytes.fromhex("8000047304323230323031313433"),
    "10.226.157.206": bytes.fromhex("8000047304323230323031313531"),
    "10.226.157.207": bytes.fromhex("8000047304323230323031313531"),
    "10.226.157.208": bytes.fromhex("8000047304323230323031313531"),
    "10.226.157.209": bytes.fromhex("8000047304323230323031313531"),
    "10.229.232.211": bytes.fromhex("8000047304323230323031313434"),
    "10.229.232.212": bytes.fromhex("8000047304323230323031313434"),
    # Note: Unity and PowerVault Engine IDs still need to be populated manually.
}


def _build_filter_table(ip_filter: dict[str, str]):
    table = []
    for entry, measurement in ip_filter.items():
        try:
            table.append((ipaddress.ip_network(entry, strict=False), measurement))
        except ValueError:
            log.warning("Invalid IP filter entry skipped: %s", entry)
    return table

FILTER_TABLE = _build_filter_table(IP_FILTER)

def classify_source(ip_str: str) -> str | None:
    try:
        addr = ipaddress.ip_address(ip_str)
    except ValueError:
        return None
    for network, measurement in FILTER_TABLE:
        if addr in network:
            return measurement
    return None

def resolve_storage_name(ip_str: str) -> str:
    return IP_TO_STORAGE_NAME.get(ip_str, "unknown")

UNITY_ENT          = "1.3.6.1.4.1.1139.103"
UNITY_VB_CODE      = "1.3.6.1.4.1.1139.103.1.1.1"  
UNITY_VB_SEV       = "1.3.6.1.4.1.1139.103.1.1.2"  
UNITY_VB_DESC      = "1.3.6.1.4.1.1139.103.1.1.3"  
UNITY_VB_SYSNAME   = "1.3.6.1.4.1.1139.103.1.1.4"  
UNITY_VB_SYSSER    = "1.3.6.1.4.1.1139.103.1.1.5"  
UNITY_VB_SOLUTION  = "1.3.6.1.4.1.1139.103.1.1.6"  
UNITY_VB_ALERTID   = "1.3.6.1.4.1.1139.103.1.1.7"  
UNITY_VB_TIMESTAMP = "1.3.6.1.4.1.1139.103.1.1.8"  
UNITY_VB_SUMMARY   = "1.3.6.1.4.1.1139.103.1.1.9"  
UNITY_VB_COMPONENT = "1.3.6.1.4.1.1139.103.1.1.10" 

UNITY_SEV_MAP: dict[int, str] = {
    0: "critical", 1: "error", 2: "warning",
    3: "notice", 4: "informational", 5: "informational",
}

PMAX_ENT           = "1.3.6.1.4.1.1139.3.8888"
PMAX_VB_SOURCE     = "1.3.6.1.4.1.1139.3.8888.1"
PMAX_VB_EVTID      = "1.3.6.1.4.1.1139.3.8888.2"
PMAX_VB_COMPONENT  = "1.3.6.1.4.1.1139.3.8888.3"
PMAX_VB_DESC       = "1.3.6.1.4.1.1139.3.8888.4"
PMAX_VB_SERIAL     = "1.3.6.1.4.1.1139.3.8888.5"
PMAX_VB_TIMESTAMP  = "1.3.6.1.4.1.1139.3.8888.6"

PMAX_SOURCE_MAP: dict[int, str] = {
    1: "CLARIION", 2: "SYMMETRIX", 4: "ECC", 8: "CELERRA",
    16: "CONNECTRIX", 32: "SRDF", 64: "INVISTA",
}

PMAX_HW_COMPONENT_EXACT: set[int] = {
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
    100, 101, 102, 103, 104, 105,
    200, 201, 202, 203, 204,
    300, 301, 302, 303,
    400, 401, 402, 403,
    500, 501, 502, 503, 504, 505,
    1029, 1030, 1031, 1032, 1033, 1034,
}

FCMGMT_VB_DESCR    = "1.3.6.1.3.94.1.11.1.9"
FCMGMT_VB_SEV      = "1.3.6.1.3.94.1.11.1.6"
FCMGMT_VB_TYPE     = "1.3.6.1.3.94.1.11.1.7"
FCMGMT_VB_OBJ      = "1.3.6.1.3.94.1.11.1.8"
FCMGMT_VB_NAME     = "1.3.6.1.3.94.1.6.1.3"
FCMGMT_VB_ID       = "1.3.6.1.3.94.1.6.1.20"

FCMGMT_SEV_MAP: dict[int, str] = {
    1: "unknown", 2: "informational", 3: "warning",
    4: "warning", 5: "error", 6: "critical", 7: "emergency",
}
FCMGMT_HW_TYPES: set[int] = {3, 5}

PVAULT_SEV_MAP: dict[int, str] = {
    1: "critical", 2: "error", 3: "warning", 4: "informational",
}

OID_SNMP_TRAP_OID  = "1.3.6.1.6.3.1.1.4.1.0"

_HW_CATEGORY_PATTERNS: tuple[tuple[str, re.Pattern], ...] = (
    ("disk_failure",       re.compile(r"\b(disk|drive|hdd|ssd|nvme|pdev|ldev)\s*(fail|fault|error|predict|degrad|miss|remov|replac)\b", re.I)),
    ("fan_failure",        re.compile(r"\b(fan|cooling\s*module|cooling\s*unit)\s*(fail|fault|error|alarm|critical|stop|degrad)\b", re.I)),
    ("power_failure",      re.compile(r"\b(power\s*supply|psu|power\s*module|ac\s*input|power\s*interrupt|power\s*fail)\b", re.I)),
    ("temperature_alarm",  re.compile(r"\b(temp(erature)?|thermal|overtemp|heat)\s*(high|low|critical|alarm|error|warning)\b", re.I)),
    ("battery_alert",      re.compile(r"\b(battery|sps|bbu|standby\s*power|nvram|nvmem|ups|charger)\s*(fail|low|fault|critical|warn|replac|discharg)\b", re.I)),
    ("controller_fault",   re.compile(r"\b(storage\s*processor|sp[ab]?|director|controller|da|fa|ra|sa|be\s*emulation|engine)\s*(fail|fault|degrad|offlin|unavail|panic|reset|reboot)\b", re.I)),
    ("port_fault",         re.compile(r"\b(port|fc\s*port|iscsi|fibre\s*channel|frontend|backend|link|hba|lif|eth)\s*(down|fail|fault|error|offlin|degrad|disconnect)\b", re.I)),
    ("memory_fault",       re.compile(r"\b(memory|mem|dimm|ram|cache\s*mem|ecc)\s*(fail|error|fault|degrad|correct|uncorrect)\b", re.I)),
    ("cache_fault",        re.compile(r"\b(write\s*cache|cache\s*card|dirty\s*cache|nvram|spcache)\s*(fail|error|fault|degrad|dirty|lost)\b", re.I)),
    ("enclosure_fault",    re.compile(r"\b(enclosure|chassis|shelf|dae|dpe|io\s*module|iom|lcc|back.plane)\s*(fail|fault|error|degrad|offlin)\b", re.I)),
    ("raid_degraded",      re.compile(r"\b(raid|protection|redundancy|parity|mirror|stripe)\s*(degrad|fail|error|lost|break|suspend)\b", re.I)),
    ("replication_alert",  re.compile(r"\b(replication|srdf|timefinder|snap|clone|mirror\s*copy)\s*(fail|error|break|suspend|split|interrupt)\b", re.I)),
    ("link_down",          re.compile(r"\b(link|connection|path|interconnect)\s*(down|lost|fail|broken|disconnect)\b", re.I)),
)

_ANY_HW_RE = re.compile(
    r"\b(disk|drive|fan|power|psu|temp|battery|sps|bbu|"
    r"sp[ab]?|controller|director|port|fc\b|iscsi|fibre|link|hba|"
    r"memory|cache|nvram|enclosure|chassis|shelf|dae|dpe|iom|"
    r"raid|redundancy|parity|ecc|overtemp|replication|srdf|fault)\b", re.I
)

ALL_HW_CATS: frozenset[str] = frozenset({
    "disk_failure", "fan_failure", "power_failure", "temperature_alarm",
    "battery_alert", "controller_fault", "port_fault", "memory_fault",
    "cache_fault", "enclosure_fault", "raid_degraded", "replication_alert",
    "link_down", "hardware_alert",
})

def classify_hw_category(description: str) -> str | None:
    if not description:
        return None
    for cat, pat in _HW_CATEGORY_PATTERNS:
        if pat.search(description):
            return cat
    if _ANY_HW_RE.search(description):
        return "hardware_alert"
    return None

_UNITY_COMPONENT_MAP: dict[str, str] = {
    "disk": "disk_failure", "drive": "disk_failure", "fan": "fan_failure",
    "power_supply": "power_failure", "power supply": "power_failure",
    "battery": "battery_alert", "sps": "battery_alert", "memory": "memory_fault",
    "cache": "cache_fault", "nvram": "cache_fault", "storage_processor": "controller_fault",
    "storage processor": "controller_fault", "sp": "controller_fault",
    "i/o_module": "enclosure_fault", "io module": "enclosure_fault",
    "enclosure": "enclosure_fault", "temperature": "temperature_alarm",
    "cabling": "enclosure_fault", "management_port": "port_fault",
    "port": "port_fault", "link": "link_down",
}

def classify_unity_component(component: str) -> str | None:
    low = component.strip().lower()
    for key, cat in _UNITY_COMPONENT_MAP.items():
        if key in low:
            return cat
    return None

def _vb_str_prefix(vb: dict, oid_prefix: str) -> str | None:
    for k, v in vb.items():
        if k.startswith(oid_prefix.rstrip(".")):
            s = str(v).strip().strip("'\"")
            return s if s else None
    return None

def _vb_int_prefix(vb: dict, oid_prefix: str) -> int | None:
    v = _vb_str_prefix(vb, oid_prefix)
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None

def _extract_any_string(vb: dict) -> str:
    best = ""
    for v in vb.values():
        s = str(v).strip()
        if len(s) > len(best) and re.search(r"[a-zA-Z]", s):
            best = s
    return best

def _add_trap_bools(fields: dict, active_cat: str) -> None:
    for cat in ALL_HW_CATS:
        fields[f"trap_{cat}"] = (cat == active_cat)

def decode_trap(
    source_ip: str, enterprise_oid: str, trap_oid: str,
    var_binds: list[tuple[str, str]], snmp_version: str,
    generic_trap: int, specific_trap: int,
) -> dict | None:
    global _trap_count
    _trap_count += 1

    vb: dict[str, str] = {}
    for oid_str, val_str in var_binds:
        key = oid_str.rstrip(".")
        vb[key] = val_str
        if key.endswith(".0"):
            vb[key[:-2]] = val_str

    ent = (enterprise_oid or trap_oid or "").lstrip(".")

    fields: dict = {
        "source_ip":      source_ip,
        "snmp_version":   snmp_version,
        "enterprise_oid": enterprise_oid,
        "trap_oid":       trap_oid,
        "generic_trap":   generic_trap,
        "specific_trap":  specific_trap,
    }

    if ent.startswith("1.3.6.1.4.1.1139.103") or ent.startswith("1.3.6.1.4.1.1139.18"):
        return _decode_unity(source_ip, ent, vb, fields)

    if ent.startswith("1.3.6.1.4.1.1139.3"):
        return _decode_powermax_storevntd(source_ip, vb, fields)

    if trap_oid.startswith("1.3.6.1.3.94") or ent.startswith("1.3.6.1.3.94"):
        return _decode_fcmgmt(source_ip, vb, fields)

    if ent.startswith("1.3.6.1.4.1.674.10893") or ent.startswith("1.3.6.1.4.1.674.10895"):
        return _decode_powervault(source_ip, ent, vb, fields)

    description = _extract_any_string(vb)
    if description:
        cat = classify_hw_category(description)
        if cat:
            fields.update({
                "device_type":   "unknown_dell",
                "trap_category": cat,
                "severity":      "unknown",
                "description":   description,
                "error_message": description,
            })
            _add_trap_bools(fields, cat)
            return fields

    fields["is_valid"] = False
    return fields

def _decode_unity(source_ip: str, ent: str, vb: dict, fields: dict) -> dict | None:
    sev_raw   = _vb_int_prefix(vb, UNITY_VB_SEV)
    severity  = UNITY_SEV_MAP.get(sev_raw, "unknown") if sev_raw is not None else "unknown"

    if sev_raw is not None and sev_raw >= 4:
        fields["is_valid"] = False
        return fields

    description = _vb_str_prefix(vb, UNITY_VB_DESC)     or _vb_str_prefix(vb, UNITY_VB_SUMMARY)
    component   = _vb_str_prefix(vb, UNITY_VB_COMPONENT) or ""
    system_name = _vb_str_prefix(vb, UNITY_VB_SYSNAME)   or ""
    serial      = _vb_str_prefix(vb, UNITY_VB_SYSSER)    or ""
    alert_code  = _vb_str_prefix(vb, UNITY_VB_CODE)      or ""
    alert_id    = _vb_str_prefix(vb, UNITY_VB_ALERTID)   or ""
    solution    = _vb_str_prefix(vb, UNITY_VB_SOLUTION)  or ""
    timestamp   = _vb_str_prefix(vb, UNITY_VB_TIMESTAMP) or ""

    cat = classify_unity_component(component)
    if not cat:
        cat = classify_hw_category(f"{component} {description}")
    if not cat:
        fields["is_valid"] = False
        return fields

    fields.update({
        "device_type":   "unity_xt",
        "trap_category": cat,
        "severity":      severity,
        "alert_code":    alert_code,
        "alert_id":      alert_id,
        "component":     component,
        "system_name":   system_name,
        "serial_number": serial,
        "solution":      solution,
        "timestamp_str": timestamp,
        "description":   description or "",
        "error_message": description or f"Unity alert code {alert_code}",
    })
    _add_trap_bools(fields, cat)
    return fields

def _decode_powermax_storevntd(source_ip: str, vb: dict, fields: dict) -> dict | None:
    source_code = _vb_int_prefix(vb, PMAX_VB_SOURCE)
    event_id    = _vb_int_prefix(vb, PMAX_VB_EVTID)
    comp_code   = _vb_int_prefix(vb, PMAX_VB_COMPONENT)
    description = _vb_str_prefix(vb, PMAX_VB_DESC)    or ""
    serial      = _vb_str_prefix(vb, PMAX_VB_SERIAL)  or ""
    timestamp   = _vb_str_prefix(vb, PMAX_VB_TIMESTAMP) or ""

    source_name = PMAX_SOURCE_MAP.get(source_code or 0, f"source_{source_code}")

    cat: str | None = None
    if comp_code is not None and comp_code in PMAX_HW_COMPONENT_EXACT:
        cat = classify_hw_category(description) or "hardware_alert"
    if not cat:
        cat = classify_hw_category(description)
    if not cat:
        fields["is_valid"] = False
        return fields

    fields.update({
        "device_type":    "powermax_8500",
        "trap_category":  cat,
        "severity":       "error",
        "event_id":       str(event_id or ""),
        "component_code": str(comp_code or ""),
        "source_name":    source_name,
        "serial_number":  serial,
        "timestamp_str":  timestamp,
        "description":    description,
        "error_message":  description or f"PowerMAX event {event_id} component {comp_code}",
    })
    _add_trap_bools(fields, cat)
    return fields

def _decode_fcmgmt(source_ip: str, vb: dict, fields: dict) -> dict | None:
    sev_raw     = _vb_int_prefix(vb, FCMGMT_VB_SEV)
    type_raw    = _vb_int_prefix(vb, FCMGMT_VB_TYPE)
    description = _vb_str_prefix(vb, FCMGMT_VB_DESCR) or ""
    obj_str     = _vb_str_prefix(vb, FCMGMT_VB_OBJ)   or ""
    unit_name   = _vb_str_prefix(vb, FCMGMT_VB_NAME)  or ""
    unit_id     = _vb_str_prefix(vb, FCMGMT_VB_ID)    or ""

    severity = FCMGMT_SEV_MAP.get(sev_raw or 0, "unknown")

    if type_raw not in FCMGMT_HW_TYPES and (sev_raw or 0) < 3:
        fields["is_valid"] = False
        return fields

    cat = classify_hw_category(f"{description} {obj_str}")
    if not cat:
        fields["is_valid"] = False
        return fields

    fields.update({
        "device_type":    "powermax_8500",
        "trap_category":  cat,
        "severity":       severity,
        "event_type":     str(type_raw or ""),
        "system_name":    unit_name,
        "serial_number":  unit_id,
        "description":    description,
        "object":         obj_str,
        "error_message":  description or f"FCMGMT event type {type_raw} sev {sev_raw}",
    })
    _add_trap_bools(fields, cat)
    return fields

def _decode_powervault(source_ip: str, ent: str, vb: dict, fields: dict) -> dict | None:
    sev_raw = _vb_int_prefix(vb, "1.3.6.1.4.1.674.10893.1.10.1.1")
    if sev_raw is None:
        sev_raw = _vb_int_prefix(vb, "1.3.6.1.4.1.674.10895.1.10.1.1")
        
    description = _vb_str_prefix(vb, "1.3.6.1.4.1.674.10893.1.10.1.2") or _vb_str_prefix(vb, "1.3.6.1.4.1.674.10895.1.10.1.2") or ""
    system_name = _vb_str_prefix(vb, "1.3.6.1.4.1.674.10893.1.10.1.3") or _vb_str_prefix(vb, "1.3.6.1.4.1.674.10895.1.10.1.3") or ""
    comp_type   = _vb_str_prefix(vb, "1.3.6.1.4.1.674.10893.1.10.1.4") or _vb_str_prefix(vb, "1.3.6.1.4.1.674.10895.1.10.1.4") or ""
    serial      = _vb_str_prefix(vb, "1.3.6.1.4.1.674.10893.1.10.1.5") or _vb_str_prefix(vb, "1.3.6.1.4.1.674.10895.1.10.1.5") or ""

    if not description:
        description = _extract_any_string(vb)

    severity = PVAULT_SEV_MAP.get(sev_raw or 0, "unknown")

    if sev_raw is not None and sev_raw >= 4:
        if not _ANY_HW_RE.search(description):
            fields["is_valid"] = False
            return fields

    cat = classify_hw_category(f"{comp_type} {description}")
    if not cat:
        fields["is_valid"] = False
        return fields

    fields.update({
        "device_type":    "powervault",
        "trap_category":  cat,
        "severity":       severity,
        "component":      comp_type,
        "system_name":    system_name,
        "serial_number":  serial,
        "description":    description,
        "error_message":  description or f"PowerVault alert sev {sev_raw}",
    })
    _add_trap_bools(fields, cat)
    return fields

class InfluxWriter:
    def __init__(self):
        self.client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, verify_ssl=False)
        if WRITE_BATCH:
            opts = WriteOptions(
                batch_size=WRITE_BATCH_SIZE, flush_interval=WRITE_FLUSH_MS,
                jitter_interval=WRITE_JITTER_MS, retry_interval=WRITE_RETRY_INTERVAL_MS,
            )
            self.write_api = self.client.write_api(write_options=opts)
        else:
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    def write(self, measurement: str, source_ip: str, fields: dict) -> None:
        storage_name = resolve_storage_name(source_ip)
        pt = (
            Point(measurement)
            .tag("source_ip",     source_ip)
            .tag("environment",   LOCATION)
            .tag("vendor",        "dell")
            .tag("device_type",   fields.get("device_type",   "unknown"))
            .tag("severity",      fields.get("severity",      "unknown"))
            .tag("trap_category", fields.get("trap_category", "none"))
            .tag("array_name",    storage_name)
            .tag("snmp_version",  fields.get("snmp_version",  "unknown"))
            .time(datetime.now(timezone.utc), WritePrecision.NS)
        )

        for key in (
            "description", "error_message", "component", "system_name",
            "serial_number", "alert_code", "alert_id", "solution",
            "timestamp_str", "object", "event_id", "component_code",
            "source_name", "enterprise_oid", "trap_oid",
        ):
            val = fields.get(key)
            if val not in (None, ""):
                pt = pt.field(key, str(val)[:1024])

        for key in ("generic_trap", "specific_trap"):
            val = fields.get(key)
            if val is not None:
                try:
                    pt = pt.field(key, int(val))
                except (TypeError, ValueError):
                    pass

        for key, val in fields.items():
            if key.startswith("trap_") and isinstance(val, bool):
                pt = pt.field(key, val)

        try:
            self.write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=pt)
            log.debug("Written -> %s [%s] sev=%s cat=%s array=%s",
                      measurement, source_ip, fields.get("severity"), fields.get("trap_category"), storage_name)
        except Exception as exc:
            log.error("InfluxDB write failed: %s", exc)

    def close(self) -> None:
        try:
            self.client.close()
        except Exception:
            pass

def _get_auth_protocol(name: str):
    protos = {
        "MD5":    snmp_config.usmHMACMD5AuthProtocol,
        "SHA":    snmp_config.usmHMACSHAAuthProtocol,
        "SHA256": getattr(snmp_config, "usmHMACSHA256AuthProtocol", snmp_config.usmHMACSHAAuthProtocol),
        "SHA384": getattr(snmp_config, "usmHMACSHA384AuthProtocol", snmp_config.usmHMACSHAAuthProtocol),
        "SHA512": getattr(snmp_config, "usmHMACSHA512AuthProtocol", snmp_config.usmHMACSHAAuthProtocol),
        "NOAUTH": snmp_config.usmNoAuthProtocol,
    }
    return protos.get(name.upper(), snmp_config.usmHMACSHAAuthProtocol)

def _get_priv_protocol(name: str):
    protos = {
        "DES":    snmp_config.usmDESPrivProtocol,
        "3DES":   getattr(snmp_config, "usm3DESEDEPrivProtocol", snmp_config.usmDESPrivProtocol),
        "AES":    snmp_config.usmAesCfb128Protocol,
        "AES128": snmp_config.usmAesCfb128Protocol,
        "AES192": getattr(snmp_config, "usmAesCfb192Protocol", snmp_config.usmAesCfb128Protocol),
        "AES256": getattr(snmp_config, "usmAesCfb256Protocol", snmp_config.usmAesCfb128Protocol),
        "NOPRIV": snmp_config.usmNoPrivProtocol,
    }
    return protos.get(name.upper(), snmp_config.usmAesCfb128Protocol)

def build_snmp_engine() -> snmp_engine.SnmpEngine:
    eng = snmp_engine.SnmpEngine()
    add_v1_fn = getattr(snmp_config, "add_v1_system", getattr(snmp_config, "addV1System", None))
    if add_v1_fn:
        add_v1_fn(eng, "default-area", SNMP_COMMUNITY)

    if V3_USER and V3_AUTH_KEY:
        auth_proto = _get_auth_protocol(V3_AUTH_PROTO)
        priv_proto = _get_priv_protocol(V3_PRIV_PROTO) if V3_PRIV_KEY else snmp_config.usmNoPrivProtocol
        
        # 1. Register for local engine ID (standard behavior)
        try:
            add_user_fn = getattr(snmp_config, "add_v3_user", snmp_config.addV3User)
            add_user_fn(eng, V3_USER, auth_proto, V3_AUTH_KEY, priv_proto, V3_PRIV_KEY or "")
        except Exception as exc:
            log.warning("SNMP v3 local user registration failed: %s", exc)

        # 1.5. Register explicitly for all known remote Engine IDs (deduplicated)
        seen_engine_ids = set()
        for ip, engine_id_bytes in KNOWN_ENGINE_IDS.items():
            if engine_id_bytes in seen_engine_ids:
                continue
            seen_engine_ids.add(engine_id_bytes)
            try:
                add_user_fn(
                    eng, V3_USER, auth_proto, V3_AUTH_KEY,
                    priv_proto, V3_PRIV_KEY or "", securityEngineId=engine_id_bytes
                )
                log.info("Statically registered SNMPv3 keys for known Engine ID: %s (IP: %s)", engine_id_bytes.hex(), ip)
            except Exception as e:
                log.warning("Failed to statically register keys for Engine ID %s: %s", engine_id_bytes.hex(), e)

        # 2. Dynamically clone the user for ANY remote Engine ID that sends us a trap
        def on_engine_id_discovery(snmpEngine, execpoint, variables, cbCtx):
            securityEngineId = variables.get("securityEngineId")
            if securityEngineId:
                try:
                    add_user_fn(
                        snmpEngine, V3_USER, auth_proto, V3_AUTH_KEY,
                        priv_proto, V3_PRIV_KEY or "", securityEngineId=securityEngineId
                    )
                    log.info("Localized SNMPv3 keys for newly discovered Engine ID: %s", securityEngineId.prettyPrint())
                except Exception as e:
                    log.error("Failed to localize keys for Engine ID %s: %s", securityEngineId.prettyPrint(), e)

        try:
            register_fn = getattr(eng.observer, "register_observer", None)
            if not register_fn:
                register_fn = getattr(eng.observer, "registerObserver", None)
            
            if register_fn:
                register_fn(
                    on_engine_id_discovery,
                    "rfc3412.receiveMessage:request",
                    "rfc2576.registerContextEngineId",
                    "rfc3414.processIncomingMsg"
                )
            else:
                log.warning("PySNMP Observer not found, dynamic Engine ID discovery might fail.")
        except Exception as e:
            log.warning("Failed to register SNMPv3 Engine ID observer: %s", e)


    import socket
    try:
        # Pre-flight check to ensure the port isn't secretly held by another process (like snmptrapd)
        _test_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        _test_sock.bind((LISTEN_HOST, LISTEN_PORT))
        _test_sock.close()
    except OSError as e:
        log.error("FATAL: Cannot bind to %s:%s. Another process is already using this port! Error: %s", LISTEN_HOST, LISTEN_PORT, e)
        raise SystemExit(1)
        
    add_transport_fn = getattr(snmp_config, "add_transport", getattr(snmp_config, "addTransport", None))
    add_transport_fn(
        eng,
        snmp_udp.domainName,
        snmp_udp.UdpTransport().openServerMode((LISTEN_HOST, LISTEN_PORT)),
    )
    return eng

def make_trap_callback(writer: InfluxWriter, pool: ThreadPoolExecutor):
    def _trap_cb(snmpEngine, stateReference, contextEngineId, contextName, varBinds, cbCtx) -> None:
        try:
            msg_dsp = getattr(snmpEngine, "message_dispatcher", getattr(snmpEngine, "msgAndPduDsp", None))
            get_info = getattr(msg_dsp, "get_transport_info", getattr(msg_dsp, "getTransportInfo", None))
            transportDomain, transportAddress = get_info(stateReference)
            source_ip = str(transportAddress[0]) if transportAddress else "0.0.0.0"
        except Exception:
            source_ip = "0.0.0.0"

        vb_pairs: list[tuple[str, str]] = []
        for oid_obj, val_obj in varBinds:
            try:
                vb_pairs.append((str(oid_obj), val_obj.prettyPrint()))
            except Exception:
                continue

        enterprise_oid = ""
        trap_oid       = ""
        generic_trap   = -1
        specific_trap  = -1
        snmp_version   = "v2c"

        for oid_str, val_str in vb_pairs:
            if oid_str == OID_SNMP_TRAP_OID:
                trap_oid = val_str.strip()
                break

        if not trap_oid:
            enterprise_oid = ""
        else:
            parts = trap_oid.rsplit(".", 2)
            if len(parts) == 3 and parts[-2] == "0":
                enterprise_oid = parts[0]
            else:
                enterprise_oid = trap_oid
                
        # Universal Raw Log
        raw_log.info(
            "TRAP src=%s ver=%s ent=%s trapoid=%s varbinds=%s",
            source_ip, snmp_version, enterprise_oid, trap_oid,
            [(o, v[:80]) for o, v in vb_pairs],
        )

        measurement = classify_source(source_ip)

        pool.submit(
            _safe_process,
            writer, measurement, source_ip,
            enterprise_oid, trap_oid, vb_pairs,
            snmp_version, generic_trap, specific_trap,
        )

    return _trap_cb

def _safe_process(
    writer: InfluxWriter, measurement: str, source_ip: str,
    enterprise_oid: str, trap_oid: str, vb_pairs: list[tuple[str, str]],
    snmp_version: str, generic_trap: int, specific_trap: int,
) -> None:
    try:
        fields = decode_trap(
            source_ip, enterprise_oid, trap_oid, vb_pairs,
            snmp_version, generic_trap, specific_trap,
        )
        if fields is None:
            return

        # Universal Decoded Log
        decoded_log.info("DECODED: %s", json.dumps(fields))

        if measurement is None:
            return
            
        if not fields.get("is_valid", True):
            return

        log.info(
            "[TRAP] %s (%s) -> [%s] sev=%s cat=%s | %s",
            source_ip, resolve_storage_name(source_ip),
            measurement, fields.get("severity", "?"), fields.get("trap_category", "?"),
            fields.get("error_message", "")[:120],
        )
        writer.write(measurement, source_ip, fields)
    except Exception as exc:
        log.exception("Worker crashed processing trap from %s: %s", source_ip, exc)

async def _snmp_loop():
    eng = build_snmp_engine()
    writer = InfluxWriter()
    pool = ThreadPoolExecutor(max_workers=WORKER_THREADS, thread_name_prefix="snmp-worker")
    
    ntfrcv.NotificationReceiver(eng, make_trap_callback(writer, pool))
    
    log.info("=" * 64)
    log.info(" SNMP Trap Listener (%s) - starting up", LOCATION)
    log.info(" Vendor           : %s", VENDOR)
    log.info(" Influx URL       : %s", INFLUX_URL)
    log.info(" Influx bucket    : %s", INFLUX_BUCKET)
    log.info(" SNMP UDP listen  : %s:%d", LISTEN_HOST, LISTEN_PORT)
    log.info("=" * 64)
    
    _start_heartbeat()
    
    try:
        await asyncio.Event().wait()
    finally:
        eng.transportDispatcher.closeDispatcher()
        writer.close()
        pool.shutdown(wait=False)

def main() -> None:
    try:
        asyncio.run(_snmp_loop())
    except KeyboardInterrupt:
        log.info("Shutdown requested")

if __name__ == "__main__":
    main()
