#!/usr/bin/env python3
"""
=============================================================================
  SNMP Trap Listener -> InfluxDB v2  (Hi-Track / HDFC -- CDVL Dell pipeline)
=============================================================================

Self-contained SNMP trap receiver for the CDVL Dell storage
estate.  Listens on UDP 162 for SNMPv1 / v2c / v3 traps, decodes
device-specific OID structures, filters to HARDWARE-ONLY alerts, and writes
to InfluxDB v2.  A periodic heartbeat is written to a separate bucket so the
monitoring dashboard can detect a dead listener.
"""
from __future__ import annotations

import os
import re
import threading
import time
import logging
import ipaddress
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

# ── influxdb ──────────────────────────────────────────────────────────────
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS, WriteOptions

# ── pysnmp ────────────────────────────────────────────────────────────────
from pysnmp.entity import engine as snmp_engine, config as snmp_config
from pysnmp.entity.rfc3413 import ntfrcv
from pysnmp.carrier.asyncio.dgram import udp as snmp_udp

# ---------------------------------------------------------------------------
# LOCATION / VENDOR
# ---------------------------------------------------------------------------
LOCATION = "CDVL"
VENDOR   = "Dell"

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
INFLUX_URL    = os.environ.get("HITRACK_INFLUX_URL",    "http://127.0.0.1:8386")
INFLUX_TOKEN  = os.environ.get("HITRACK_INFLUX_TOKEN",  "hitrack-dev-token-please-change")
INFLUX_ORG    = os.environ.get("HITRACK_INFLUX_ORG",    "HDFC")
INFLUX_BUCKET = os.environ.get("HITRACK_INFLUX_BUCKET", "Dell_CDVL_Bucket")

LISTEN_HOST = os.environ.get("HITRACK_LISTEN_HOST", "0.0.0.0")
LISTEN_PORT = int(os.environ.get("HITRACK_LISTEN_PORT", "162"))

SNMP_COMMUNITY = os.environ.get("HITRACK_SNMP_COMMUNITY", "public")

# SNMPv3 credentials (Unity XT requires v3)
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

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
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
raw_log.propagate = False

# ---------------------------------------------------------------------------
# HEARTBEAT
# ---------------------------------------------------------------------------
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
        hb_client = InfluxDBClient(url=HB_URL, token=HB_TOKEN, org=HB_ORG)
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

# ---------------------------------------------------------------------------
# IP → MEASUREMENT + STORAGE NAME
# ---------------------------------------------------------------------------
IP_FILTER: dict[str, str] = {
    # ── Unity XT 480 | 22C-STR-H-20401  D5-3F09-R16  S/N CKM01221805039 ──
    "10.227.66.72":   "unity_storage",

    # ── PowerMAX 8500 | 24B-STR-H-20513  D6-6FA06R06  S/N CK220201130 ──
    # Inventory: 10.227.65.59-10.227.65.62
    "10.227.65.59":   "powermax_storage",
    "10.227.65.60":   "powermax_storage",
    "10.227.65.61":   "powermax_storage",
    "10.227.65.62":   "powermax_storage",

    # PowerMAX 8500 | 24B-STR-H-20512  D9-7F04R07  S/N CK220201147
    # Inventory: 10.227.66.187-10.227.66.190
    "10.227.66.187":  "powermax_storage",
    "10.227.66.188":  "powermax_storage",
    "10.227.66.189":  "powermax_storage",
    "10.227.66.190":  "powermax_storage",

    # PowerMAX 8500 | 24B-STR-H-20516  D9-9FB03-R06  S/N CK220201143
    # Inventory: 10.226.157.202-10.226.157.205
    "10.226.157.202": "powermax_storage",
    "10.226.157.203": "powermax_storage",
    "10.226.157.204": "powermax_storage",
    "10.226.157.205": "powermax_storage",

    # PowerMAX 8500 | 24B-STR-H-20519  D9-9FB04-R05  S/N CK220201151
    # Inventory: 10.226.157.206-10.226.157.209
    "10.226.157.206": "powermax_storage",
    "10.226.157.207": "powermax_storage",
    "10.226.157.208": "powermax_storage",
    "10.226.157.209": "powermax_storage",

    # PowerMAX 8500 UAT | 24B-STR-H-20511  D6-6FA06R08  S/N CK220201144
    # Inventory: 10.229.232.211-10.229.232.212
    "10.229.232.211": "powermax_storage",
    "10.229.232.212": "powermax_storage",

    # ── PowerVault | 24I-STR-H-20538  D5-3F09-R16  S/N G5CDM54 ──
    # Inventory: 10.227.66.74-10.227.66.75
    "10.227.66.74":   "powervault_storage",
    "10.227.66.75":   "powervault_storage",
}

IP_TO_STORAGE_NAME: dict[str, str] = {
    # Unity XT 480 | 22C-STR-H-20401
    "10.227.66.72":   "Unity_XT480_20401-CDVL",

    # PowerMAX 8500 | 24B-STR-H-20513
    "10.227.65.59":   "PowerMAX8500_20513-CDVL",
    "10.227.65.60":   "PowerMAX8500_20513-CDVL",
    "10.227.65.61":   "PowerMAX8500_20513-CDVL",
    "10.227.65.62":   "PowerMAX8500_20513-CDVL",

    # PowerMAX 8500 | 24B-STR-H-20512
    "10.227.66.187":  "PowerMAX8500_20512-CDVL",
    "10.227.66.188":  "PowerMAX8500_20512-CDVL",
    "10.227.66.189":  "PowerMAX8500_20512-CDVL",
    "10.227.66.190":  "PowerMAX8500_20512-CDVL",

    # PowerMAX 8500 | 24B-STR-H-20516
    "10.226.157.202": "PowerMAX8500_20516-CDVL",
    "10.226.157.203": "PowerMAX8500_20516-CDVL",
    "10.226.157.204": "PowerMAX8500_20516-CDVL",
    "10.226.157.205": "PowerMAX8500_20516-CDVL",

    # PowerMAX 8500 | 24B-STR-H-20519
    "10.226.157.206": "PowerMAX8500_20519-CDVL",
    "10.226.157.207": "PowerMAX8500_20519-CDVL",
    "10.226.157.208": "PowerMAX8500_20519-CDVL",
    "10.226.157.209": "PowerMAX8500_20519-CDVL",

    # PowerMAX 8500 UAT | 24B-STR-H-20511
    "10.229.232.211": "PowerMAX8500_20511_UAT-CDVL",
    "10.229.232.212": "PowerMAX8500_20511_UAT-CDVL",

    # PowerVault | 24I-STR-H-20538
    "10.227.66.74":   "PowerVault_20538-CDVL",
    "10.227.66.75":   "PowerVault_20538-CDVL",
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
    if not FILTER_TABLE:
        return f"dell_{LOCATION.lower()}_storage"
    try:
        addr = ipaddress.ip_address(ip_str)
    except ValueError:
        return None
    for network, measurement in FILTER_TABLE:
        if addr in network:
            return measurement
    return None

def resolve_storage_name(ip_str: str) -> str:
    return IP_TO_STORAGE_NAME.get(ip_str, f"Unknown_Dell_{LOCATION}")

# ---------------------------------------------------------------------------
# DELL / EMC OID DEFINITIONS
# ---------------------------------------------------------------------------
EMC_OID            = "1.3.6.1.4.1.1139"           # EMC / Dell Technologies
DELL_OID           = "1.3.6.1.4.1.674"             # Dell Inc.

UNITY_ENT          = "1.3.6.1.4.1.1139.103"
UNITY_TRAP_OID     = "1.3.6.1.4.1.1139.103.1.1.0.1"   
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
UNITY_ENT_OLD      = "1.3.6.1.4.1.1139.18"

UNITY_SEV_MAP: dict[int, str] = {
    0: "critical", 1: "error", 2: "warning", 3: "notice", 4: "informational", 5: "informational"
}

PMAX_ENT           = "1.3.6.1.4.1.1139.3.8888"
PMAX_VB_SOURCE     = "1.3.6.1.4.1.1139.3.8888.1"   
PMAX_VB_EVTID      = "1.3.6.1.4.1.1139.3.8888.2"   
PMAX_VB_COMPONENT  = "1.3.6.1.4.1.1139.3.8888.3"   
PMAX_VB_DESC       = "1.3.6.1.4.1.1139.3.8888.4"   
PMAX_VB_SERIAL     = "1.3.6.1.4.1.1139.3.8888.5"   
PMAX_VB_TIMESTAMP  = "1.3.6.1.4.1.1139.3.8888.6"   

PMAX_SOURCE_MAP: dict[int, str] = {
    1:  "CLARIION", 2:  "SYMMETRIX", 4:  "ECC", 8:  "CELERRA", 16: "CONNECTRIX", 32: "SRDF", 64: "INVISTA"
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

FCMGMT_TRAP_OID    = "1.3.6.1.3.94.0.1"            
FCMGMT_VB_DESCR    = "1.3.6.1.3.94.1.11.1.9"       
FCMGMT_VB_SEV      = "1.3.6.1.3.94.1.11.1.6"       
FCMGMT_VB_TYPE     = "1.3.6.1.3.94.1.11.1.7"       
FCMGMT_VB_OBJ      = "1.3.6.1.3.94.1.11.1.8"       
FCMGMT_VB_NAME     = "1.3.6.1.3.94.1.6.1.3"        
FCMGMT_VB_ID       = "1.3.6.1.3.94.1.6.1.20"       

FCMGMT_SEV_MAP: dict[int, str] = {
    1: "unknown", 2: "informational", 3: "warning", 4: "warning", 5: "error", 6: "critical", 7: "emergency",
}
FCMGMT_HW_TYPES: set[int] = {3, 5}   

PVAULT_ENT_OLD     = "1.3.6.1.4.1.674.10893"       
PVAULT_ENT_NEW     = "1.3.6.1.4.1.674.10895"       
PVAULT_VB_SEV      = ".1.1.0"    
PVAULT_VB_DESC     = ".1.2.0"
PVAULT_VB_SYSNAME  = ".1.3.0"
PVAULT_VB_COMPTYPE = ".1.4.0"    
PVAULT_VB_SERIAL   = ".1.5.0"    

PVAULT_SEV_MAP: dict[int, str] = {
    1: "critical", 2: "error", 3: "warning", 4: "informational",
}

OID_SNMP_TRAP_OID  = "1.3.6.1.6.3.1.1.4.1.0"      
OID_SYS_UPTIME     = "1.3.6.1.2.1.1.3.0"           

# ---------------------------------------------------------------------------
# HARDWARE ALERT CLASSIFICATION
# ---------------------------------------------------------------------------

_HW_CATEGORY_PATTERNS: tuple[tuple[str, re.Pattern], ...] = (
    ("disk_failure",       re.compile(r"(disk|drive|hdd|ssd|nvme|pdev|ldev)\s*(fail|fault|error|predict|degrad|miss|remov|replac)", re.I)),
    ("fan_failure",        re.compile(r"(fan|cooling\s*module|cooling\s*unit)\s*(fail|fault|error|alarm|critical|stop|degrad)", re.I)),
    ("power_failure",      re.compile(r"(power\s*supply|psu|power\s*module|ac\s*input|power\s*interrupt|power\s*fail)", re.I)),
    ("temperature_alarm",  re.compile(r"(temp(erature)?|thermal|overtemp|heat)\s*(high|low|critical|alarm|error|warning|warning)", re.I)),
    ("battery_alert",      re.compile(r"(battery|sps|bbu|standby\s*power|nvram|nvmem|ups|charger)\s*(fail|low|fault|critical|warn|replac|discharg)", re.I)),
    ("controller_fault",   re.compile(r"(storage\s*processor|sp[ab]?|director|controller|da|fa|ra|sa|be\s*emulation|engine)\s*(fail|fault|degrad|offlin|unavail|panic|reset|reboot)", re.I)),
    ("port_fault",         re.compile(r"(port|fc\s*port|iscsi|fibre\s*channel|frontend|backend|link|hba|lif|eth)\s*(down|fail|fault|error|offlin|degrad|disconnect)", re.I)),
    ("memory_fault",       re.compile(r"(memory|mem|dimm|ram|cache\s*mem|ecc)\s*(fail|error|fault|degrad|correct|uncorrect)", re.I)),
    ("cache_fault",        re.compile(r"(write\s*cache|cache\s*card|dirty\s*cache|nvram|spcache)\s*(fail|error|fault|degrad|dirty|lost)", re.I)),
    ("enclosure_fault",    re.compile(r"(enclosure|chassis|shelf|dae|dpe|io\s*module|iom|lcc|back.plane)\s*(fail|fault|error|degrad|offlin)", re.I)),
    ("raid_degraded",      re.compile(r"(raid|protection|redundancy|parity|mirror|stripe)\s*(degrad|fail|error|lost|break|suspend)", re.I)),
    ("replication_alert",  re.compile(r"(replication|srdf|timefinder|snap|clone|mirror\s*copy)\s*(fail|error|break|suspend|split|interrupt)", re.I)),
    ("link_down",          re.compile(r"(link|connection|path|interconnect)\s*(down|lost|fail|broken|disconnect)", re.I)),
)

_ANY_HW_RE = re.compile(
    r"(disk|drive|fan|power|psu|temp|battery|sps|bbu|"
    r"sp[ab]?|controller|director|port|fc|iscsi|fibre|link|hba|"
    r"memory|cache|nvram|enclosure|chassis|shelf|dae|dpe|iom|"
    r"raid|redundancy|parity|ecc|overtemp|replication|srdf|fault)",
    re.I,
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
    "disk":             "disk_failure",
    "drive":            "disk_failure",
    "fan":              "fan_failure",
    "power_supply":     "power_failure",
    "power supply":     "power_failure",
    "battery":          "battery_alert",
    "sps":              "battery_alert",
    "memory":           "memory_fault",
    "cache":            "cache_fault",
    "nvram":            "cache_fault",
    "storage_processor":"controller_fault",
    "storage processor":"controller_fault",
    "sp":               "controller_fault",
    "i/o_module":       "enclosure_fault",
    "io module":        "enclosure_fault",
    "enclosure":        "enclosure_fault",
    "temperature":      "temperature_alarm",
    "cabling":          "enclosure_fault",
    "management_port":  "port_fault",
    "port":             "port_fault",
    "link":             "link_down",
}

def classify_unity_component(component: str) -> str | None:
    low = component.strip().lower()
    for key, cat in _UNITY_COMPONENT_MAP.items():
        if key in low:
            return cat
    return None

# ---------------------------------------------------------------------------
# TRAP DECODER
# ---------------------------------------------------------------------------

def decode_trap(
    source_ip: str,
    enterprise_oid: str,
    trap_oid: str,
    var_binds: list[tuple[str, str]],
    snmp_version: str,
    generic_trap: int,
    specific_trap: int,
) -> dict | None:
    global _trap_count
    _trap_count += 1

    vb: dict[str, str] = {}
    for oid_str, val_str in var_binds:
        key = oid_str.rstrip(".")
        vb[key] = val_str
        if key.endswith(".0"):
            vb[key[:-2]] = val_str

    raw_log.info(
        "TRAP src=%s ver=%s ent=%s trapoid=%s varbinds=%s",
        source_ip, snmp_version, enterprise_oid, trap_oid,
        [(o, v[:80]) for o, v in var_binds],
    )

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

    if ent.startswith("1.3.6.1.4.1.1139.3.8888"):
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

    log.debug("DROP unknown enterprise %s from %s", ent, source_ip)
    return None

def _decode_unity(source_ip: str, ent: str, vb: dict, fields: dict) -> dict | None:
    sev_raw   = _vb_int(vb, UNITY_VB_SEV)
    severity  = UNITY_SEV_MAP.get(sev_raw, "unknown") if sev_raw is not None else "unknown"

    if sev_raw is not None and sev_raw >= 4:
        log.debug("DROP Unity INFO/OK trap (sev=%d) from %s", sev_raw, source_ip)
        return None

    description = _vb_str(vb, UNITY_VB_DESC)     or _vb_str(vb, UNITY_VB_SUMMARY)
    component   = _vb_str(vb, UNITY_VB_COMPONENT) or ""
    system_name = _vb_str(vb, UNITY_VB_SYSNAME)   or ""
    serial      = _vb_str(vb, UNITY_VB_SYSSER)    or ""
    alert_code  = _vb_str(vb, UNITY_VB_CODE)      or ""
    alert_id    = _vb_str(vb, UNITY_VB_ALERTID)   or ""
    solution    = _vb_str(vb, UNITY_VB_SOLUTION)  or ""
    timestamp   = _vb_str(vb, UNITY_VB_TIMESTAMP) or ""

    cat = classify_unity_component(component)
    if not cat:
        cat = classify_hw_category(f"{component} {description}")
    if not cat:
        log.debug("DROP Unity non-hardware trap (comp=%s desc=%s) from %s",
                  component, description, source_ip)
        return None

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
    source_code = _vb_int(vb, PMAX_VB_SOURCE)
    event_id    = _vb_int(vb, PMAX_VB_EVTID)
    comp_code   = _vb_int(vb, PMAX_VB_COMPONENT)
    description = _vb_str(vb, PMAX_VB_DESC)    or ""
    serial      = _vb_str(vb, PMAX_VB_SERIAL)  or ""
    timestamp   = _vb_str(vb, PMAX_VB_TIMESTAMP) or ""

    source_name = PMAX_SOURCE_MAP.get(source_code or 0, f"source_{source_code}")

    cat: str | None = None
    if comp_code is not None and comp_code in PMAX_HW_COMPONENT_EXACT:
        cat = classify_hw_category(description) or "hardware_alert"
    if not cat:
        cat = classify_hw_category(description)
    if not cat:
        log.debug(
            "DROP PowerMAX non-hardware trap (src=%s evt=%s comp=%s) from %s",
            source_name, event_id, comp_code, source_ip,
        )
        return None

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
        log.debug("DROP FCMGMT non-hardware event (type=%s sev=%s) from %s",
                  type_raw, sev_raw, source_ip)
        return None

    cat = classify_hw_category(f"{description} {obj_str}")
    if not cat:
        log.debug("DROP FCMGMT non-hardware description '%s' from %s",
                  description[:80], source_ip)
        return None

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
    pvault_base = ent.rstrip(".")

    sev_raw     = _vb_int(vb, pvault_base + PVAULT_VB_SEV)
    description = _vb_str(vb, pvault_base + PVAULT_VB_DESC)    or ""
    system_name = _vb_str(vb, pvault_base + PVAULT_VB_SYSNAME) or ""
    comp_type   = _vb_str(vb, pvault_base + PVAULT_VB_COMPTYPE)or ""
    serial      = _vb_str(vb, pvault_base + PVAULT_VB_SERIAL)  or ""

    if not description:
        description = _extract_any_string(vb)

    severity = PVAULT_SEV_MAP.get(sev_raw or 0, "unknown")

    if sev_raw is not None and sev_raw >= 4:
        if not _ANY_HW_RE.search(description):
            log.debug("DROP PowerVault INFO trap from %s", source_ip)
            return None

    cat = classify_hw_category(f"{comp_type} {description}")
    if not cat:
        log.debug("DROP PowerVault non-hardware trap '%s' from %s",
                  description[:80], source_ip)
        return None

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

def _vb_val(vb: dict, oid_base: str) -> str | None:
    key = oid_base.rstrip(".")
    v = vb.get(key) or vb.get(key + ".0")
    return v if v is not None else None

def _vb_str(vb: dict, oid_base: str) -> str | None:
    v = _vb_val(vb, oid_base)
    if v is None: return None
    s = str(v).strip().strip("'"")
    return s if s else None

def _vb_int(vb: dict, oid_base: str) -> int | None:
    v = _vb_val(vb, oid_base)
    if v is None: return None
    try: return int(str(v).strip())
    except (ValueError, TypeError): return None

def _vb_str_prefix(vb: dict, oid_prefix: str) -> str | None:
    for k, v in vb.items():
        if k.startswith(oid_prefix.rstrip(".")):
            s = str(v).strip().strip("'"")
            return s if s else None
    return None

def _vb_int_prefix(vb: dict, oid_prefix: str) -> int | None:
    v = _vb_str_prefix(vb, oid_prefix)
    if v is None: return None
    try: return int(v)
    except (ValueError, TypeError): return None

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

class InfluxWriter:
    def __init__(self):
        self.client = InfluxDBClient(
            url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, verify_ssl=False,
        )
        if WRITE_BATCH:
            opts = WriteOptions(
                batch_size=WRITE_BATCH_SIZE,
                flush_interval=WRITE_FLUSH_MS,
                jitter_interval=WRITE_JITTER_MS,
                retry_interval=WRITE_RETRY_INTERVAL_MS,
            )
            self.write_api = self.client.write_api(write_options=opts)
            log.info("InfluxDB -> %s (bucket=%s) [batch=%d flush_ms=%d]", INFLUX_URL, INFLUX_BUCKET, WRITE_BATCH_SIZE, WRITE_FLUSH_MS)
        else:
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            log.info("InfluxDB -> %s (bucket=%s) [SYNCHRONOUS]", INFLUX_URL, INFLUX_BUCKET)

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
                try: pt = pt.field(key, int(val))
                except (TypeError, ValueError): pass

        for key, val in fields.items():
            if key.startswith("trap_") and isinstance(val, bool):
                pt = pt.field(key, val)

        try:
            self.write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=pt)
            log.debug("Written -> %s [%s] sev=%s cat=%s array=%s | %s",
                measurement, source_ip, fields.get("severity"), fields.get("trap_category"),
                storage_name, fields.get("error_message", "")[:80])
        except Exception as exc:
            log.error("InfluxDB write failed: %s", exc)

    def close(self) -> None:
        try: self.client.close()
        except Exception: pass

def _get_auth_protocol(name: str):
    protos = {
        "MD5":    snmp_config.usmHMACMD5AuthProtocol,
        "SHA":    snmp_config.usmHMACSHAAuthProtocol,
        "SHA256": getattr(snmp_config, "usmHMACSHA256AuthProtocol", snmp_config.usmHMACSHAAuthProtocol),
        "NOAUTH": snmp_config.usmNoAuthProtocol,
    }
    return protos.get(name.upper(), snmp_config.usmHMACSHAAuthProtocol)

def _get_priv_protocol(name: str):
    protos = {
        "DES":    snmp_config.usmDESPrivProtocol,
        "AES":    snmp_config.usmAesCfb128Protocol,
        "AES256": getattr(snmp_config, "usmAesCfb256Protocol", snmp_config.usmAesCfb128Protocol),
        "NOPRIV": snmp_config.usmNoPrivProtocol,
    }
    return protos.get(name.upper(), snmp_config.usmAesCfb128Protocol)

def build_snmp_engine() -> snmp_engine.SnmpEngine:
    eng = snmp_engine.SnmpEngine()

    snmp_config.addV1System(eng, "default-area", SNMP_COMMUNITY)

    if V3_USER and V3_AUTH_KEY:
        auth_proto = _get_auth_protocol(V3_AUTH_PROTO)
        priv_proto = _get_priv_protocol(V3_PRIV_PROTO) if V3_PRIV_KEY else snmp_config.usmNoPrivProtocol
        try:
            snmp_config.addV3User(eng, V3_USER, auth_proto, V3_AUTH_KEY, priv_proto, V3_PRIV_KEY or "")
        except Exception as exc:
            log.warning("SNMP v3 user registration failed: %s", exc)

    try:
        snmp_config.add_transport(
            eng,
            snmp_udp.DOMAIN_NAME,
            snmp_udp.UdpTransport().open_server_mode((LISTEN_HOST, LISTEN_PORT)),
        )
    except AttributeError:
        snmp_config.addTransport(
            eng,
            snmp_udp.domainName,
            snmp_udp.UdpTransport().openServerMode((LISTEN_HOST, LISTEN_PORT)),
        )

    return eng

_source_ip_local = threading.local()

def _pre_receive_observer(snmpEngine, execpoint, variables, cbCtx) -> None:
    if execpoint == "rfc3412.receiveMessage:request":
        addr = variables.get("transportAddress", ("0.0.0.0", 0))
        try: _source_ip_local.ip = str(addr[0])
        except Exception: _source_ip_local.ip = "0.0.0.0"

def make_trap_callback(writer: InfluxWriter, pool: ThreadPoolExecutor):
    def _trap_cb(snmpEngine, stateReference, contextEngineId, contextName, varBinds, cbCtx) -> None:
        source_ip = getattr(_source_ip_local, "ip", "0.0.0.0")

        measurement = classify_source(source_ip)
        if measurement is None:
            log.debug("DROP trap from non-allowed IP: %s", source_ip)
            return

        vb_pairs: list[tuple[str, str]] = []
        for oid_obj, val_obj in varBinds:
            try:
                vb_pairs.append((str(oid_obj), val_obj.prettyPrint()))
            except Exception:
                continue

        enterprise_oid, trap_oid = "", ""
        generic_trap, specific_trap = -1, -1
        snmp_version = "v2c"

        for oid_str, val_str in vb_pairs:
            if oid_str == OID_SNMP_TRAP_OID:
                trap_oid = val_str.strip()
                break

        if trap_oid:
            parts = trap_oid.rsplit(".", 2)
            enterprise_oid = parts[0] if len(parts) == 3 and parts[-2] == "0" else trap_oid

        pool.submit(_safe_process, writer, measurement, source_ip, enterprise_oid, trap_oid, vb_pairs, snmp_version, generic_trap, specific_trap)

    return _trap_cb

def _safe_process(writer, measurement, source_ip, enterprise_oid, trap_oid, vb_pairs, snmp_version, generic_trap, specific_trap) -> None:
    try:
        fields = decode_trap(source_ip, enterprise_oid, trap_oid, vb_pairs, snmp_version, generic_trap, specific_trap)
        if fields:
            writer.write(measurement, source_ip, fields)
    except Exception as exc:
        log.exception("Worker crashed processing trap from %s: %s", source_ip, exc)

async def async_main():
    eng = build_snmp_engine()
    eng.observer.registerObserver(_pre_receive_observer, "rfc3412.receiveMessage:request")
    
    cb = make_trap_callback(writer, pool)
    ntfrcv.NotificationReceiver(eng, cb)

    while True:
        await asyncio.sleep(3600)

writer = None
pool = None

def main() -> None:
    log.info("=" * 64)
    log.info(f" SNMP Trap Listener (CDVL) - starting up")
    log.info(" Influx URL       : %s",  INFLUX_URL)
    log.info(" Influx bucket    : %s",  INFLUX_BUCKET)
    log.info(" SNMP UDP listen  : %s:%d", LISTEN_HOST, LISTEN_PORT)
    log.info(" IP_FILTER        : %d entries", len(IP_FILTER))
    log.info(" Storage mapping  : %d entries", len(IP_TO_STORAGE_NAME))
    log.info("=" * 64)

    _start_heartbeat()

    global writer, pool
    writer = InfluxWriter()
    pool = ThreadPoolExecutor(max_workers=WORKER_THREADS, thread_name_prefix="snmp-worker")

    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        log.info("Shutdown requested")
    finally:
        pool.shutdown(wait=True)
        writer.close()

if __name__ == "__main__":
    main()
