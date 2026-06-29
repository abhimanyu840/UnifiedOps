#!/usr/bin/env python3
"""
=============================================================================
  Syslog Trap Listener -> InfluxDB v2  (Hi-Track / HDFC -- SIFY pipeline)
=============================================================================

Self-contained listener for the SIFY location. The engine logic and the
SIFY IP maps live in this single file - no shared modules, no other
location referenced.

Configuration overrides (typical: /etc/hi-track/listener.sify.env):

    HITRACK_INFLUX_URL      default http://127.0.0.1:8086
    HITRACK_INFLUX_TOKEN    *** required for writes ***
    HITRACK_INFLUX_ORG      default HDFC
    HITRACK_INFLUX_BUCKET   default SYSLOG_HIT_SIFY_Bucket
    HITRACK_LISTEN_HOST     default 0.0.0.0
    HITRACK_LISTEN_PORT     default 514          (TCP listens on +1)
    HITRACK_TEST_MODE       "1" to enable loopback spoofing for dev
    HITRACK_TEST_DEFAULT_IP fallback source IP for test mode
"""
from __future__ import annotations

import os
import socket
import threading
import time
import re
import logging
import ipaddress
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS, WriteOptions

# ---------------------------------------------------------------------------
# LOCATION / VENDOR
# ---------------------------------------------------------------------------
LOCATION = "SIFY"
VENDOR   = "Hitachi"

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
INFLUX_URL    = os.environ.get("HITRACK_INFLUX_URL",    "http://127.0.0.1:8086")
INFLUX_TOKEN  = os.environ.get("HITRACK_INFLUX_TOKEN",  "hitrack-dev-token-please-change")
INFLUX_ORG    = os.environ.get("HITRACK_INFLUX_ORG",    "HDFC")
INFLUX_BUCKET = os.environ.get("HITRACK_INFLUX_BUCKET", "SYSLOG_HIT_SIFY_Bucket")

LISTEN_HOST = os.environ.get("HITRACK_LISTEN_HOST", "0.0.0.0")
LISTEN_PORT = int(os.environ.get("HITRACK_LISTEN_PORT", "514"))

BUFFER_SIZE = 8192
LOG_LEVEL   = logging.INFO

# Multithreaded ingestion knobs - see CDVL listener for documentation.
WORKER_THREADS           = max(2, int(os.environ.get("HITRACK_WORKER_THREADS", "16")))
WRITE_BATCH              = os.environ.get("HITRACK_WRITE_BATCH", "1").lower() in ("1", "true", "yes", "on")
WRITE_BATCH_SIZE         = max(1, int(os.environ.get("HITRACK_WRITE_BATCH_SIZE", "200")))
WRITE_FLUSH_MS           = max(50, int(os.environ.get("HITRACK_WRITE_FLUSH_MS", "1000")))
WRITE_JITTER_MS          = max(0, int(os.environ.get("HITRACK_WRITE_JITTER_MS", "0")))
WRITE_RETRY_INTERVAL_MS  = max(50, int(os.environ.get("HITRACK_WRITE_RETRY_MS", "1000")))

TEST_MODE = os.environ.get("HITRACK_TEST_MODE", "").lower() in ("1", "true", "yes", "on")
TEST_DEFAULT_IP = os.environ.get("HITRACK_TEST_DEFAULT_IP", "10.226.63.165")
TEST_LOOPBACK_IPS = ("127.0.0.1", "::1")
TEST_SOURCE_PREFIX_RE = re.compile(r"^\s*\[SOURCE_IP=(?P<ip>[0-9a-fA-F\.:]+)\]\s*")

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("syslog_trap_listener_sify.log"),
    ],
)
log = logging.getLogger("syslog_trap_listener_sify")

raw_log = logging.getLogger("raw_syslog_sify")
raw_log.setLevel(logging.INFO)
raw_fh = logging.FileHandler("syslog_trap_listener_sify_raw_syslog_data.log")
raw_fh.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
raw_log.addHandler(raw_fh)
raw_log.propagate = False

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
        log.info("heartbeat disabled - HITRACK_HEARTBEAT_URL/TOKEN/BUCKET not set")
        return
    try:
        hb_client = InfluxDBClient(url=HB_URL, token=HB_TOKEN, org=HB_ORG)
        hb_write  = hb_client.write_api(write_options=SYNCHRONOUS)
    except Exception as exc:
        log.warning("heartbeat disabled - influx client init failed: %s", exc)
        return

    started_at = time.time()
    seq = 0
    log.info("heartbeat thread up -> %s/%s every %ds", HB_URL, HB_BUCKET, HB_INTERVAL)
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
            log.warning("heartbeat write failed: %s", exc)
        time.sleep(HB_INTERVAL)


def _start_heartbeat() -> None:
    threading.Thread(
        target=_heartbeat_loop, daemon=True, name=f"hb-{HB_LISTENER}",
    ).start()


# ---------------------------------------------------------------------------
# IP MAPPINGS (SIFY only)
# Source IP -> measurement bucket. Anything not listed is dropped at the
# source by classify_source().
# ---------------------------------------------------------------------------
IP_FILTER: dict[str, str] = {
    "10.226.63.165":  "modular_storage",
    "10.226.63.166":  "modular_storage",
    "10.226.63.167":  "modular_storage",
    "10.226.63.162":  "modular_storage",
    "10.226.63.126":  "modular_storage",
    "10.226.63.127":  "modular_storage",
    "10.226.63.128":  "modular_storage",
    "10.226.63.129":  "modular_storage",
    "10.226.63.130":  "modular_storage",
    "10.226.63.131":  "modular_storage",
    "10.226.81.105":  "modular_storage",
    "10.226.81.106":  "modular_storage",
    "10.226.81.107":  "modular_storage",
    "10.226.81.108":  "modular_storage",
    "10.226.81.109":  "modular_storage",
    "10.226.81.110":  "modular_storage",
    "10.226.80.13":   "modular_storage",
    "10.226.81.115":  "modular_storage",
    "10.226.81.116":  "modular_storage",
    "10.226.82.0":    "modular_storage",
    "10.226.80.234":  "modular_storage",
    "10.226.80.235":  "modular_storage",
    "10.226.80.14":   "modular_storage",
    "10.226.81.100":  "modular_storage",
    "10.226.81.101":  "modular_storage",
    "10.226.80.15":   "modular_storage",
    "10.226.81.103":  "modular_storage",
    "10.226.81.104":  "modular_storage",
    "10.226.81.0":    "modular_storage",
    "10.226.81.112":  "modular_storage",
    "10.226.81.113":  "modular_storage",
    "10.226.81.2":    "modular_storage",
    "10.226.81.192":  "modular_storage",
    "10.226.81.193":  "modular_storage",
    "10.226.81.3":    "modular_storage",
    "10.226.81.212":  "modular_storage",
    "10.226.81.213":  "modular_storage",
    "10.226.81.255":  "modular_storage",
    "10.226.82.169":  "modular_storage",
    "10.226.82.170":  "modular_storage",
    "10.226.79.159":  "modular_storage",
    "10.226.79.143":  "modular_storage",
    "10.226.79.144":  "modular_storage",
    "10.226.79.160":  "modular_storage",
    "10.226.79.146":  "modular_storage",
    "10.226.79.147":  "modular_storage",
    "10.226.79.161":  "modular_storage",
    "10.226.79.134":  "modular_storage",
    "10.226.79.135":  "modular_storage",
    "10.226.79.162":  "modular_storage",
    "10.226.79.137":  "modular_storage",
    "10.226.79.138":  "modular_storage",

    "10.226.63.1":    "enterprise_storage",
    "10.226.63.2":    "enterprise_storage",
    "10.226.63.3":    "enterprise_storage",
    "10.226.63.4":    "enterprise_storage",
    "10.226.81.92":   "enterprise_storage",
    "10.226.82.219":  "enterprise_storage",
    "10.226.83.83":   "enterprise_storage",
    "10.226.83.107":  "enterprise_storage",
    "10.226.83.177":  "enterprise_storage",
    "10.226.157.197": "enterprise_storage",
    "10.226.157.198": "enterprise_storage",
}

# ---------------------------------------------------------------------------
# IP -> friendly storage name (SIFY only). Each VSP storage box has multiple
# management IPs; they all map to the same -SIFY-suffixed hostname so every
# alert is tagged with the correct storage system.
# ---------------------------------------------------------------------------
IP_TO_STORAGE_NAME: dict[str, str] = {
    "10.226.63.1":    "VSP_5500_30648-SIFY",
    "10.226.63.2":    "VSP_5500_30649-SIFY",
    "10.226.63.3":    "VSP_5500_30650-SIFY",
    "10.226.63.4":    "VSP_5500_30651-SIFY",
    "10.226.63.165":  "VSP_G700_445784-SIFY",
    "10.226.63.166":  "VSP_G700_445784-SIFY",
    "10.226.63.167":  "VSP_G700_445784-SIFY",
    "10.226.63.162":  "VSP_G350_482542-SIFY",
    "10.226.63.126":  "VSP_G370_482340-SIFY",
    "10.226.63.127":  "VSP_G370_482340-SIFY",
    "10.226.63.128":  "VSP_G370_482340-SIFY",
    "10.226.63.129":  "VSP_G370_482531-SIFY",
    "10.226.63.130":  "VSP_G370_482531-SIFY",
    "10.226.63.131":  "VSP_G370_482531-SIFY",
    "10.226.80.13":   "VSP_E990_418368-SIFY",
    "10.226.81.115":  "VSP_E990_418368-SIFY",
    "10.226.81.116":  "VSP_E990_418368-SIFY",
    "10.226.82.0":    "VSP_E990_417949-SIFY",
    "10.226.80.234":  "VSP_E990_417949-SIFY",
    "10.226.80.235":  "VSP_E990_417949-SIFY",
    "10.226.80.14":   "VSP_E990_418331-SIFY",
    "10.226.81.100":  "VSP_E990_418331-SIFY",
    "10.226.81.101":  "VSP_E990_418331-SIFY",
    "10.226.80.15":   "VSP_E990_418360-SIFY",
    "10.226.81.103":  "VSP_E990_418360-SIFY",
    "10.226.81.104":  "VSP_E990_418360-SIFY",
    "10.226.81.92":   "VSP_5500_31302-SIFY",
    "10.226.81.105":  "VSP_G370_454453-SIFY",
    "10.226.81.106":  "VSP_G370_454453-SIFY",
    "10.226.81.107":  "VSP_G370_454453-SIFY",
    "10.226.81.108":  "VSP_G370_454484-SIFY",
    "10.226.81.109":  "VSP_G370_454484-SIFY",
    "10.226.81.110":  "VSP_G370_454484-SIFY",
    "10.226.81.0":    "VSP_E990_418369-SIFY",
    "10.226.81.112":  "VSP_E990_418369-SIFY",
    "10.226.81.113":  "VSP_E990_418369-SIFY",
    "10.226.81.2":    "VSP_E990_418295-SIFY",
    "10.226.81.192":  "VSP_E990_418295-SIFY",
    "10.226.81.193":  "VSP_E990_418295-SIFY",
    "10.226.81.3":    "VSP_E990_448524-SIFY",
    "10.226.81.212":  "VSP_E990_448524-SIFY",
    "10.226.81.213":  "VSP_E990_448524-SIFY",
    "10.226.81.217":  "VSP_G350_485946-SIFY",
    "10.226.81.215":  "VSP_G350_485946-SIFY",
    "10.226.81.216":  "VSP_G350_485946-SIFY",
    "10.226.81.255":  "VSP_E990_418719-SIFY",
    "10.226.82.169":  "VSP_E990_418719-SIFY",
    "10.226.82.170":  "VSP_E990_418719-SIFY",
    "10.226.82.219":  "VSP_5600_40279-SIFY",
    "10.226.83.83":   "VSP_5600_40530-SIFY",
    "10.226.79.159":  "VSP_E1090_715344-SIFY",
    "10.226.79.143":  "VSP_E1090_715344-SIFY",
    "10.226.79.144":  "VSP_E1090_715344-SIFY",
    "10.226.83.107":  "VSP_5600_60522-SIFY",
    "10.226.79.160":  "VSP_E1090_715381-SIFY",
    "10.226.79.146":  "VSP_E1090_715381-SIFY",
    "10.226.79.147":  "VSP_E1090_715381-SIFY",
    "10.226.83.177":  "VSP_5600_40850-SIFY",
    "10.226.79.161":  "VSP_E1090_715444-SIFY",
    "10.226.79.134":  "VSP_E1090_715444-SIFY",
    "10.226.79.135":  "VSP_E1090_715444-SIFY",
    "10.226.79.162":  "VSP_E1090_715445-SIFY",
    "10.226.79.137":  "VSP_E1090_715445-SIFY",
    "10.226.79.138":  "VSP_E1090_715445-SIFY",
    "10.226.109.61":  "VSP_5600_41542-SIFY",
    "10.226.109.62":  "VSP_5600_41542-SIFY",
    "10.226.109.63":  "VSP_5600_41542-SIFY",
    "10.226.109.64":  "VSP_5600_41542-SIFY",
    "10.226.109.65":  "VSP_5600_41542-SIFY",
    "10.226.109.66":  "VSP_5600_41542-SIFY",
    "10.226.109.67":  "VSP_5600_41542-SIFY",
    "10.226.109.68":  "VSP_5600_41542-SIFY",
    "10.226.157.197": "VSP_5600_41551-SIFY",
    "10.226.157.198": "VSP_5600_41551-SIFY",
}


# ---------------------------------------------------------------------------
# IP FILTER HELPERS
# ---------------------------------------------------------------------------

def _build_filter_table(ip_filter):
    table = []
    for entry, measurement in ip_filter.items():
        try:
            net = ipaddress.ip_network(entry, strict=False)
            table.append((net, measurement))
        except ValueError:
            log.warning("Invalid IP filter entry skipped: %s", entry)
    return table


FILTER_TABLE = _build_filter_table(IP_FILTER)


def classify_source(ip_str):
    try:
        addr = ipaddress.ip_address(ip_str)
    except ValueError:
        return None
    for network, measurement in FILTER_TABLE:
        if addr in network:
            return measurement
    return None


def resolve_storage_name(ip_str):
    return IP_TO_STORAGE_NAME.get(ip_str, "unknown")


def apply_test_mode(raw, source_ip):
    if not TEST_MODE:
        return raw, source_ip, False
    is_loopback_or_private = (source_ip in TEST_LOOPBACK_IPS) or source_ip.startswith("127.")
    try:
        text = raw.decode("utf-8", errors="replace")
    except Exception:
        text = ""

    spoof_ip = None
    m = TEST_SOURCE_PREFIX_RE.search(text)
    if m:
        spoof_ip = m.group("ip")
        text = TEST_SOURCE_PREFIX_RE.sub("", text, count=1)
        raw = text.encode("utf-8", errors="replace")

    if is_loopback_or_private:
        chosen_ip = spoof_ip or TEST_DEFAULT_IP
        if chosen_ip not in IP_FILTER:
            log.warning(
                "TEST_MODE: spoof IP %s is not in IP_FILTER; falling back to %s",
                chosen_ip, TEST_DEFAULT_IP,
            )
            chosen_ip = TEST_DEFAULT_IP
        return raw, chosen_ip, True
    return raw, source_ip, False


# ---------------------------------------------------------------------------
# SYSLOG PARSING (RFC3164 / RFC5424)
# ---------------------------------------------------------------------------

RFC3164_RE = re.compile(
    r"^<(?P<pri>\d{1,3})>\s*"
    r"(?P<timestamp>\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+"
    r"(?P<hostname>\S+)\s+"
    r"(?P<process>[^:\[]+)(?:\[(?P<pid>\d+)\])?:\s*"
    r"(?P<message>.+)$"
)

RFC5424_RE = re.compile(
    r"^<(?P<pri>\d{1,3})>"
    r"(?P<version>\d+)\s+"
    r"(?P<timestamp>\S+)\s+"
    r"(?P<hostname>\S+)\s+"
    r"(?P<appname>\S+)\s+"
    r"(?P<procid>\S+)\s+"
    r"(?P<msgid>\S+)\s+"
    r"(?P<structured_data>\[.*?\]|-)\s*"
    r"(?P<message>.*)$"
)

TRAP_PATTERNS = {
    "fan_failure":       re.compile(r"fan\s*(fail|fault|error|alarm)", re.I),
    "power_failure":     re.compile(r"power\s*(fail|fault|loss|alarm)", re.I),
    "disk_failure":      re.compile(r"(disk|drive|hdd|ssd|nvme)\s*(fail|fault|error|removed)", re.I),
    "controller_fault":  re.compile(r"controller\s*(fail|fault|takeover|offline)", re.I),
    "link_down":         re.compile(r"link\s*(down|fail|lost)", re.I),
    "temperature_alarm": re.compile(r"temp(erature)?\s*(high|low|alarm|critical|warning)", re.I),
    "raid_degraded":     re.compile(r"raid\s*(degraded|rebuild|fail)", re.I),
    "snapshot_alert":    re.compile(r"snapshot\s*(full|fail|create|delete|error)", re.I),
    "replication_alert": re.compile(r"replication\s*(fail|lag|error|break)", re.I),
    "volume_alert":      re.compile(r"volume\s*(full|offline|error|mount|unmount)", re.I),
    "port_fault":        re.compile(r"port\s*(fail|down|error|fault)", re.I),
    "battery_alert":     re.compile(r"battery\s*(low|fail|replace|charge)", re.I),
    "auth_failure":      re.compile(r"(auth|login|ssh|console)\s*(fail|denied|error)", re.I),
    "config_change":     re.compile(r"config(uration)?\s*(change|modify|update|set)", re.I),
    "firmware_alert":    re.compile(r"firmware\s*(update|upgrade|fail|mismatch)", re.I),
    # NTP / time synchronisation. Hitachi VSP raises SIM 7FFA00
    # "Synchronization time failure" for NTP issues (see SIM Reference Guide).
    "ntp_alert":         re.compile(
        r"(synchroniz(?:ation|ed)?\s*time|time\s*sync(?:hroniz(?:ation|ed)?)?|"
        r"\bntp\b|\bsntp\b|chrony|time\s*server|clock\s*(drift|skew|sync))",
        re.I,
    ),
}


def decode_priority(pri):
    facility_names = [
        "kern", "user", "mail", "daemon", "auth", "syslog", "lpr", "news",
        "uucp", "cron", "authpriv", "ftp", "ntp", "log_audit", "log_alert",
        "clock", "local0", "local1", "local2", "local3", "local4", "local5",
        "local6", "local7",
    ]
    severity_names = [
        "emergency", "alert", "critical", "error",
        "warning", "notice", "informational", "debug",
    ]
    facility = pri >> 3
    severity = pri & 0x07
    return {
        "facility":      facility_names[facility] if facility < len(facility_names) else str(facility),
        "severity":      severity_names[severity] if severity < len(severity_names) else str(severity),
        "facility_code": facility,
        "severity_code": severity,
    }


def extract_trap_flags(message):
    flags = {}
    for trap_type, pattern in TRAP_PATTERNS.items():
        flags[f"trap_{trap_type}"] = bool(pattern.search(message))
    return flags


# ---------------------------------------------------------------------------
# HITACHI SVP MESSAGE PARSER
# ---------------------------------------------------------------------------
# Hitachi storage arrays emit at least three variants of the same trap
# format depending on firmware vintage / family:
#
#   "SVP Storage: <seq>,<sev>,<array>,Hitachi_syslog,RefCode:<hex>,<text>"
#   "GUM Storage: <seq>,<sev>,<array>(Serial#xxx),NewHitrack,RefCode:<hex>,<text>"
#   "<seq>,<sev>,<array>,Hitachi_syslog,RefCode:<hex>,<text>"             (no prefix)
#
# Both must parse to the same category. The only differences are the syslog
# prefix word and the constant token between the array name and "RefCode:".
HITACHI_SVP_RE = re.compile(
    r"(?:(?:SVP|GUM)\s+Storage:\s*)?"
    r"(?P<seq>\d+)\s*,\s*"
    r"(?P<svp_severity>Acute|Serious|Moderate|Service|Info)\s*,\s*"
    r"(?P<array>[^,]+?)\s*,\s*"
    r"(?:Hitachi_syslog|NewHitrack|Hitachi[_-]?Track)\s*,\s*"
    r"RefCode\s*:\s*(?P<refcode>[A-Fa-f0-9]+)\s*,\s*"
    r"(?P<svp_text>.+?)\s*$",
    re.I,
)

HITACHI_REFCODE_MAP = {
    "7FFFFF": "test_trap",
    "FFFFFF": "test_trap",
    "000000": "test_trap",

    "180000": "audit_log",
    "1C0000": "controller_fault",

    "388F00": "power_failure",
    "389F00": "power_failure",
    "39A000": "temperature_alarm",

    "3C9500": "controller_fault",
    "3C9600": "controller_fault",

    "410000": "format_complete",
    "410001": "format_complete",
    "410002": "format_complete",
    "410100": "format_complete",
    "410200": "format_complete",
    "410201": "format_complete",
    "410300": "format_complete",

    "47E700": "volume_alert",
    "47EC00": "volume_alert",

    "491000": "cache_alert",

    "50F000": "firmware_alert",

    "603000": "shared_memory_alert",
    "610001": "backup_alert",
    "610002": "backup_alert",
    "623FFE": "shared_memory_alert",
    "624000": "shared_memory_alert",
    "628000": "dp_pool_full",
    "62B000": "dp_pool_threshold",
    "62B100": "dp_pool_threshold",
    "62C000": "dp_pool_full",

    "660100": "encryption_alert",
    "660200": "encryption_alert",
    "670000": "cache_alert",

    "682000": "dedupe_alert",
    "760000": "controller_fault",

    "7D0900": "controller_fault",

    "7FF102": "replication_alert",
    "7FF104": "snapshot_alert",
    "7FF106": "volume_alert",
    # Per Hitachi SIM Reference Guide page 19: 7FFA00 is
    # "Synchronization time failure" (NTP / time-sync alert), not a generic
    # config change.
    "7FFA00": "ntp_alert",

    "AC6000": "power_failure",
    "AC6100": "power_failure",
    "AC6200": "power_failure",
    "AC6300": "power_failure",

    "AF7000": "temperature_alarm",
    "AF7100": "temperature_alarm",
    "AFF400": "air_filter",

    "BFC010": "alarm_led",

    "EE0000": "qos_alert",
    "EE1000": "qos_alert",
    "EE2000": "qos_alert",

    "EFD000": "external_storage",

    "FE0000": "battery_alert",
    "FE0100": "cache_alert",
    "FE0200": "cache_alert",

    "FFD400": "env_warning",
    "FFE700": "shared_memory_alert",
    "FFE800": "cache_alert",
    "FFEB00": "shared_memory_alert",
    "FFEF00": "shared_memory_alert",
    "FFF400": "cache_alert",
}

HITACHI_REFCODE_PREFIX4_MAP = {
    "1420": "controller_fault",
    "2120": "port_fault",
    "2130": "controller_fault",
    "2140": "controller_fault",
    "2153": "controller_fault",
    "2154": "controller_fault",
    "2157": "controller_fault",
    "2180": "link_down",
    "2190": "link_down",
    "2193": "link_down",
    "2194": "link_down",
    "21A8": "port_fault",
    "21AA": "port_fault",
    "21D0": "external_storage",
    "21D1": "external_storage",
    "21D2": "external_storage",

    "3070": "controller_fault",
    "3071": "controller_fault",
    "3072": "controller_fault",
    "3073": "controller_fault",
    "3075": "cache_alert",
    "3076": "controller_fault",
    "3077": "controller_fault",
    "3078": "controller_fault",
    "3080": "controller_fault",

    "3990": "controller_fault",
    "3991": "controller_fault",
    "3993": "controller_fault",
    "399D": "power_failure",
    "399E": "env_warning",
    "399F": "env_warning",
    "39B0": "controller_fault",
    "3C97": "firmware_alert",

    "434":  "disk_failure",
    "43B":  "disk_failure",
    "43C":  "disk_failure",
    "451":  "correction_copy",
    "452":  "correction_copy",
    "453":  "correction_copy",
    "454":  "correction_copy",
    "455":  "correction_copy",
    "461":  "dynamic_sparing",
    "462":  "dynamic_sparing",
    "463":  "dynamic_sparing",
    "464":  "dynamic_sparing",
    "465":  "dynamic_sparing",
    "468":  "dynamic_sparing",
    "46A":  "dynamic_sparing",

    "47D":  "snapshot_alert",
    "47F":  "volume_alert",
    "4A80": "firmware_alert",
    "4B3":  "snapshot_alert",
    "4C1":  "pdev_erase",
    "4C2":  "pdev_erase",
    "4C3":  "pdev_erase",
    "4C4":  "disk_failure",
    "4D1":  "ldev_blockade",
    "4E0":  "media_sanitization",
    "4E2":  "media_sanitization",
    "4E4":  "media_sanitization",
    "4E6":  "media_sanitization",
    "4E8":  "media_sanitization",

    "501":  "disk_failure",
    "502":  "disk_failure",
    "505":  "battery_alert",
    "508":  "battery_alert",
    "50B":  "disk_failure",
    "50C":  "disk_failure",
    "50D":  "battery_alert",
    "50E":  "battery_alert",

    "602":  "dp_pool_full",
    "603":  "shared_memory_alert",
    "604":  "dp_pool_threshold",
    "605":  "dp_pool_threshold",
    "606":  "dp_pool_threshold",
    "623":  "dp_pool_full",
    "627":  "dp_pool_full",
    "629":  "dp_pool_threshold",
    "62A":  "dp_pool_threshold",
    "62B":  "dp_pool_threshold",
    "62C":  "dp_pool_threshold",
    "62D":  "dp_pool_threshold",
    "62E":  "dp_pool_threshold",
    "631":  "dp_pool_full",
    "632":  "dp_pool_full",
    "633":  "dp_pool_full",
    "634":  "dp_pool_full",
    "641":  "tier_relocation",

    "6610": "encryption_alert",
    "6620": "encryption_alert",
    "6800": "dedupe_alert",
    "681":  "dedupe_alert",

    "7900": "boot_error",
    "7C1":  "config_change",

    "7D00": "gum_alert",
    "7D01": "gum_alert",
    "7D02": "gum_alert",
    "7D03": "audit_log",
    "7D04": "audit_log",
    "7D05": "gum_alert",
    "7D06": "controller_fault",
    "7D07": "auth_failure",
    "7D08": "gum_alert",
    "7D0A": "firmware_alert",
    "7D0B": "backup_alert",

    "7FF7": "license_alert",
    "7FF8": "license_alert",
    "7FF9": "license_alert",

    "AC50": "power_failure",
    "AC51": "power_failure",
    "AC80": "controller_fault",

    "AF00": "env_warning",
    "AF10": "temperature_alarm",
    "AF11": "temperature_alarm",
    "AF12": "temperature_alarm",
    "AF13": "temperature_alarm",
    "AF20": "power_failure",
    "AF21": "power_failure",
    "AF30": "env_warning",
    "AF31": "env_warning",
    "AF32": "firmware_alert",
    "AF33": "power_failure",
    "AF40": "battery_alert",
    "AF41": "battery_alert",
    "AF42": "env_warning",
    "AF43": "env_warning",
    "AF44": "firmware_alert",
    "AF45": "firmware_alert",
    "AF46": "temperature_alarm",
    "AF48": "power_failure",
    "AF49": "power_failure",
    "AF4A": "power_failure",
    "AF4B": "power_failure",
    "AF4C": "fan_failure",
    "AF4D": "env_warning",
    "AF4E": "env_warning",
    "AF50": "power_failure",
    "AF51": "power_failure",
    "AF52": "power_failure",
    "AF60": "power_failure",
    "AF61": "power_failure",
    "AF62": "power_failure",
    "AF80": "env_warning",
    "AF81": "env_warning",
    "AF82": "env_warning",
    "AFA0": "firmware_alert",
    "AFA1": "firmware_alert",
    "AFA2": "firmware_alert",
    "AFB9": "controller_fault",
    "AFE4": "air_filter",
    "AFF0": "ups_alert",
    "AFF1": "gum_alert",
    "AFF2": "cache_alert",
    "AFF3": "fan_failure",

    "CF10": "sas_port",
    "CF11": "sas_port",
    "CF12": "sas_port",
    "CF13": "sas_port",
    "CF14": "sas_port",
    "CF88": "controller_fault",
    "CF8A": "controller_fault",

    "D00":  "replication_alert",
    "D01":  "replication_alert",
    "D02":  "replication_alert",
    "D10":  "replication_alert",
    "D11":  "replication_alert",
    "D12":  "replication_alert",
    "D13":  "replication_alert",
    "D14":  "replication_alert",
    "D15":  "replication_alert",
    "D16":  "replication_alert",
    "D17":  "replication_alert",
    "D18":  "replication_alert",
    "D19":  "replication_alert",
    "D1A":  "replication_alert",
    "D1B":  "replication_alert",
    "D1Z":  "replication_alert",

    "D40":  "replication_pair_suspend",
    "D41":  "replication_pair_suspend",
    "D42":  "replication_pair_suspend",
    "D44":  "replication_pair_suspend",
    "D45":  "replication_pair_suspend",
    "D46":  "comm_error",
    "D47":  "replication_pair_suspend",
    "D4F":  "replication_pair_suspend",

    "D80":  "replication_alert",
    "D81":  "replication_alert",
    "D82":  "replication_alert",
    "D83":  "replication_alert",
    "D84":  "replication_alert",
    "D85":  "replication_alert",
    "D86":  "replication_alert",
    "D87":  "replication_alert",
    "D88":  "replication_alert",
    "D89":  "replication_alert",

    "D90":  "replication_alert",
    "D91":  "replication_alert",
    "D92":  "replication_alert",
    "D93":  "replication_alert",
    "D94":  "replication_alert",
    "D95":  "replication_alert",
    "D96":  "replication_alert",
    "D97":  "replication_alert",
    "D98":  "replication_alert",
    "D99":  "replication_alert",
    "D9A":  "replication_alert",
    "D9B":  "replication_alert",
    "D9C":  "replication_alert",
    "D9D":  "replication_alert",
    "D9E":  "replication_alert",
    "D9F":  "replication_alert",
    "DA0":  "replication_alert",
    "DA1":  "replication_alert",
    "DA2":  "replication_alert",
    "DA3":  "replication_alert",
    "DA4":  "replication_alert",
    "DA5":  "replication_alert",
    "DA6":  "replication_alert",

    "DC0":  "replication_pair_suspend",
    "DC1":  "replication_pair_suspend",
    "DC2":  "replication_pair_suspend",
    "DC4":  "replication_pair_suspend",
    "DC5":  "replication_pair_suspend",
    "DC6":  "comm_error",
    "DC7":  "replication_pair_suspend",
    "DC8":  "replication_pair_suspend",
    "DC9":  "replication_pair_suspend",
    "DCA":  "replication_pair_suspend",
    "DCE":  "replication_journal",
    "DCF":  "replication_journal",

    "DD0":  "gad_alert",
    "DD1":  "gad_alert",
    "DD2":  "gad_alert",
    "DD3":  "gad_alert",

    "DEE":  "gad_alert",
    "DEF":  "gad_alert",

    "DF6":  "drive_port_error",
    "DF7":  "drive_port_error",
    "DF8":  "drive_port_error",
    "DF9":  "drive_port_error",
    "DFA":  "ldev_blockade",
    "DFB":  "ldev_blockade",
    "DFC":  "drive_port_error",
    "DFD":  "drive_port_error",
    "DFF":  "drive_temp_error",

    "EF0":  "disk_failure",
    "EF1":  "disk_failure",
    "EF2":  "disk_failure",
    "EF4":  "cache_alert",
    "EF5":  "external_storage",
    "EF9":  "ldev_blockade",
    "EFA":  "drive_temp_error",
    "EFC":  "disk_failure",
    "EFD":  "external_storage",
    "EFE":  "controller_fault",
    "EFF":  "controller_fault",

    "FE03": "cache_alert",
    "FE04": "battery_alert",
    "FF21": "controller_fault",
    "FF4":  "cache_alert",
    "FF5":  "external_storage",
    "FFC3": "cache_alert",
    "FFCB": "controller_fault",
    "FFCC": "cache_alert",
    "FFCD": "cache_alert",
    "FFCF": "cache_alert",
    "FFE2": "shared_memory_alert",
    "FFE4": "shared_memory_alert",
    "FFEA": "shared_memory_alert",
    "FFEE": "shared_memory_alert",
    "FFF0": "cache_alert",
    "FFF5": "cache_alert",
    "FFF7": "gum_alert",
    "FFF9": "cache_alert",
    "FFFA": "battery_alert",
    "FFFE": "env_warning",
}

HITACHI_REFCODE_PREFIX_MAP = {
    "3A": "disk_failure",
    "62": "dp_pool_threshold",
    "21": "port_fault",
    "15": "power_failure",
    "16": "fan_failure",
    "7D": "gum_alert",
    "30": "link_down",
    "DC": "replication_pair_suspend",
    "DD": "gad_alert",
    "DF": "drive_port_error",
    "EF": "disk_failure",
    "AF": "env_warning",
    "AC": "power_failure",
    "CF": "sas_port",
    "FE": "cache_alert",
    "FF": "cache_alert",
    "EE": "qos_alert",
}

HITACHI_SEVERITY_MAP = {
    "acute":    "critical",
    "serious":  "error",
    "moderate": "warning",
    "service":  "notice",
    "info":     "informational",
}


HITACHI_TEST_TRAP_RE = re.compile(
    r"\b(test\s*sim|test\s*trap|test\s*message|communication\s*test|svp\s*test)\b",
    re.I,
)

HITACHI_COMM_ERROR_RE = re.compile(
    r"\bcommunication\s*(error|failure|fault|timeout|lost)\b",
    re.I,
)


def parse_hitachi_svp(message):
    if not message:
        return None
    m = HITACHI_SVP_RE.search(message)
    if not m:
        return None

    refcode = m.group("refcode").upper()
    svp_sev = m.group("svp_severity").strip().lower()
    svp_text = m.group("svp_text").strip()
    # GUM-format arrays append "(Serial#NNNNNN)" to the array name. Strip
    # it so downstream display sees a clean hostname like "VSP_E1090_715428".
    raw_array = re.sub(r"\s*\(\s*Serial\s*#\s*[^)]+\)\s*$", "", m.group("array")).strip()

    trap_category = (
        HITACHI_REFCODE_MAP.get(refcode)
        or HITACHI_REFCODE_PREFIX4_MAP.get(refcode[:4])
        or HITACHI_REFCODE_PREFIX4_MAP.get(refcode[:3])
        or HITACHI_REFCODE_PREFIX_MAP.get(refcode[:2])
        or "other"
    )

    if HITACHI_TEST_TRAP_RE.search(svp_text):
        trap_category = "test_trap"
    elif HITACHI_COMM_ERROR_RE.search(svp_text):
        trap_category = "comm_error"
    elif TRAP_PATTERNS["ntp_alert"].search(svp_text):
        trap_category = "ntp_alert"

    return {
        "vendor":          "hitachi",
        "ref_code":        refcode,
        "svp_seq":         m.group("seq"),
        "svp_severity":    svp_sev,
        "svp_text":        svp_text,
        "svp_array":       raw_array,
        "trap_category":   trap_category,
        "mapped_severity": HITACHI_SEVERITY_MAP.get(svp_sev, "informational"),
    }


# ---------------------------------------------------------------------------
# MASTER PARSER
# ---------------------------------------------------------------------------

def parse_syslog(raw, source_ip):
    global _msg_count
    _msg_count += 1
    try:
        text = raw.decode("utf-8", errors="replace").strip()
        if text:
            raw_log.info(f"[{source_ip}] {text}")
    except Exception:
        return None

    if not text:
        return None

    fields = {"raw_message": text}

    m = RFC5424_RE.match(text)
    if m:
        gd = m.groupdict()
        pri = int(gd["pri"])
        fields.update(decode_priority(pri))
        fields.update({
            "syslog_version":  gd.get("version", ""),
            "timestamp_str":   gd.get("timestamp", ""),
            "hostname":        gd.get("hostname", ""),
            "app_name":        gd.get("appname", ""),
            "proc_id":         gd.get("procid", ""),
            "msg_id":          gd.get("msgid", ""),
            "structured_data": gd.get("structured_data", ""),
            "message":         gd.get("message", ""),
            "syslog_format":   "RFC5424",
            "priority":        pri,
        })
    else:
        m = RFC3164_RE.match(text)
        if m:
            gd = m.groupdict()
            pri = int(gd["pri"])
            fields.update(decode_priority(pri))
            fields.update({
                "timestamp_str": gd.get("timestamp", ""),
                "hostname":      gd.get("hostname", ""),
                "process":       (gd.get("process", "") or "").strip(),
                "pid":           gd.get("pid", ""),
                "message":       gd.get("message", ""),
                "syslog_format": "RFC3164",
                "priority":      pri,
            })
        else:
            fields.update({
                "message":       text,
                "syslog_format": "UNKNOWN",
            })

    # Search BOTH raw + RFC3164-stripped message because the "SVP Storage:"
    # prefix is consumed by RFC3164 parsing and only survives in raw_message.
    raw_text = fields.get("raw_message", "") or ""
    msg_text = fields.get("message", "") or ""
    hitachi = parse_hitachi_svp(raw_text) or parse_hitachi_svp(msg_text)

    fields.update(extract_trap_flags(msg_text or raw_text))

    all_hitachi_cats = (
        set(HITACHI_REFCODE_MAP.values())
        | set(HITACHI_REFCODE_PREFIX4_MAP.values())
        | set(HITACHI_REFCODE_PREFIX_MAP.values())
        | {"other"}
    )

    if hitachi:
        fields["severity"]      = hitachi["mapped_severity"]
        fields["vendor"]        = "hitachi"
        fields["ref_code"]      = hitachi["ref_code"]
        fields["trap_category"] = hitachi["trap_category"]
        fields["svp_seq"]       = hitachi["svp_seq"]
        fields["svp_text"]      = hitachi["svp_text"]
        fields["svp_severity"]  = hitachi["svp_severity"]
        for cat in all_hitachi_cats:
            key = f"trap_{cat}"
            fields[key] = fields.get(key, False) or (cat == hitachi["trap_category"])
    else:
        fields["vendor"]        = "generic"
        fields["trap_category"] = "none"
        for cat in all_hitachi_cats:
            fields.setdefault(f"trap_{cat}", False)

    storage_name = resolve_storage_name(source_ip)
    fields["array_name"] = storage_name
    if storage_name != "unknown":
        fields["hostname"] = storage_name

    fields["error_message"] = fields.get("svp_text") or fields.get("message") or fields.get("raw_message")

    return fields


# ---------------------------------------------------------------------------
# INFLUXDB WRITER
# ---------------------------------------------------------------------------

class InfluxWriter:
    def __init__(self):
        self.client = InfluxDBClient(
            url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, verify_ssl=False
        )
        if WRITE_BATCH:
            opts = WriteOptions(
                batch_size=WRITE_BATCH_SIZE,
                flush_interval=WRITE_FLUSH_MS,
                jitter_interval=WRITE_JITTER_MS,
                retry_interval=WRITE_RETRY_INTERVAL_MS,
            )
            self.write_api = self.client.write_api(write_options=opts)
            log.info(
                "InfluxDB client initialised -> %s (bucket=%s) "
                "[batch=%d flush_ms=%d]",
                INFLUX_URL, INFLUX_BUCKET, WRITE_BATCH_SIZE, WRITE_FLUSH_MS,
            )
        else:
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            log.info(
                "InfluxDB client initialised -> %s (bucket=%s) [SYNCHRONOUS]",
                INFLUX_URL, INFLUX_BUCKET,
            )

    def write(self, measurement, source_ip, fields):
        point = (
            Point(measurement)
            .tag("source_ip",     source_ip)
            .tag("environment",   LOCATION)
            .tag("syslog_format", fields.get("syslog_format", "UNKNOWN"))
            .tag("severity",      fields.get("severity", "unknown"))
            .tag("facility",      fields.get("facility", "unknown"))
            .tag("vendor",        fields.get("vendor", "unknown"))
            .tag("trap_category", fields.get("trap_category", "none"))
            .tag("array_name",    fields.get("array_name", "unknown"))
            .time(datetime.now(timezone.utc), WritePrecision.NS)
        )

        str_fields = [
            "hostname", "app_name", "proc_id", "msg_id", "process",
            "pid", "message", "raw_message", "structured_data",
            "timestamp_str", "ref_code", "svp_seq", "svp_text",
            "svp_severity", "error_message",
        ]
        for key in str_fields:
            val = fields.get(key)
            if val is not None and val != "":
                point = point.field(key, str(val))

        for key in ("priority", "facility_code", "severity_code"):
            val = fields.get(key)
            if val is not None:
                try:
                    point = point.field(key, int(val))
                except (TypeError, ValueError):
                    pass

        for key, val in fields.items():
            if key.startswith("trap_") and isinstance(val, bool):
                point = point.field(key, val)

        try:
            self.write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
            log.debug(
                "Written -> %s [%s] sev=%s cat=%s array=%s",
                measurement, source_ip,
                fields.get("severity"), fields.get("trap_category"),
                fields.get("array_name"),
            )
        except Exception as exc:
            log.error("InfluxDB write to %s failed: %s", INFLUX_BUCKET, exc)

    def close(self):
        self.client.close()


# ---------------------------------------------------------------------------
# UDP LISTENER
# ---------------------------------------------------------------------------

class UDPSyslogListener(threading.Thread):
    def __init__(self, writer, pool):
        super().__init__(daemon=True, name="UDPSyslogListener")
        self.writer = writer
        self.pool = pool

    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((LISTEN_HOST, LISTEN_PORT))
        log.info(
            "UDP syslog listener started on %s:%d (workers=%d)",
            LISTEN_HOST, LISTEN_PORT, WORKER_THREADS,
        )

        while True:
            try:
                data, addr = sock.recvfrom(BUFFER_SIZE)
                self.pool.submit(self._safe_handle, data, addr[0])
            except Exception as exc:
                log.error("UDP receive error: %s", exc)

    def _safe_handle(self, data, source_ip):
        try:
            self._handle(data, source_ip)
        except Exception as exc:
            log.exception("UDP worker crashed processing packet from %s: %s",
                          source_ip, exc)

    def _handle(self, data, source_ip):
        data, source_ip, spoofed = apply_test_mode(data, source_ip)
        measurement = classify_source(source_ip)
        if measurement is None:
            log.debug("Dropped packet from non-allowed IP: %s", source_ip)
            return

        if spoofed:
            log.info("TEST_MODE: attributing loopback packet to %s", source_ip)

        fields = parse_syslog(data, source_ip)
        if fields is None:
            log.warning("Failed to parse syslog from %s", source_ip)
            return

        log.info(
            "[UDP] %s (%s) -> [%s] sev=%s cat=%s | %s",
            source_ip, fields.get("array_name", "unknown"),
            measurement,
            fields.get("severity", "?"),
            fields.get("trap_category", "?"),
            (fields.get("message", "") or "")[:100],
        )

        self.writer.write(measurement, source_ip, fields)


# ---------------------------------------------------------------------------
# TCP LISTENER
# ---------------------------------------------------------------------------

class TCPSyslogListener(threading.Thread):
    def __init__(self, writer):
        super().__init__(daemon=True, name="TCPSyslogListener")
        self.writer = writer

    def run(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((LISTEN_HOST, LISTEN_PORT))
        srv.listen(50)
        log.info("TCP syslog listener started on %s:%d", LISTEN_HOST, LISTEN_PORT)

        while True:
            try:
                conn, addr = srv.accept()
                t = threading.Thread(
                    target=self._handle_client,
                    args=(conn, addr[0]),
                    daemon=True,
                )
                t.start()
            except Exception as exc:
                log.error("TCP accept error: %s", exc)

    def _handle_client(self, conn, source_ip):
        loopback = source_ip in TEST_LOOPBACK_IPS or source_ip.startswith("127.")
        if not (TEST_MODE and loopback):
            measurement = classify_source(source_ip)
            if measurement is None:
                log.debug("TCP connection rejected from non-allowed IP: %s", source_ip)
                conn.close()
                return
        else:
            measurement = None

        buf = b""
        try:
            while True:
                chunk = conn.recv(BUFFER_SIZE)
                if not chunk:
                    break
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    if not line:
                        continue
                    line, effective_ip, spoofed = apply_test_mode(line, source_ip)
                    line_measurement = measurement or classify_source(effective_ip)
                    if line_measurement is None:
                        continue
                    fields = parse_syslog(line, effective_ip)
                    if not fields:
                        continue
                    log.info(
                        "[TCP] %s (%s)%s -> [%s] sev=%s cat=%s | %s",
                        effective_ip, fields.get("array_name", "unknown"),
                        " (spoofed)" if spoofed else "",
                        line_measurement,
                        fields.get("severity", "?"),
                        fields.get("trap_category", "?"),
                        (fields.get("message", "") or "")[:100],
                    )
                    self.writer.write(line_measurement, effective_ip, fields)
        except Exception as exc:
            log.error("TCP client error (%s): %s", source_ip, exc)
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------

def main():
    missing = sorted(set(IP_FILTER) - set(IP_TO_STORAGE_NAME))
    if missing:
        log.warning(
            "%d IPs are in IP_FILTER but missing from IP_TO_STORAGE_NAME: %s",
            len(missing), ", ".join(missing),
        )

    log.info("=" * 60)
    log.info(" Syslog Trap Listener (%s) - starting up", LOCATION)
    log.info(" Influx URL      : %s", INFLUX_URL)
    log.info(" Influx bucket   : %s", INFLUX_BUCKET)
    log.info(" Measurements    : modular_storage | enterprise_storage")
    log.info(" IP_FILTER       : %d entries", len(IP_FILTER))
    log.info(" Storage mapping : %d entries", len(IP_TO_STORAGE_NAME))
    log.info(" Hitachi SVP     : enabled")
    log.info("=" * 60)
    _start_heartbeat()

    writer = InfluxWriter()

    pool = ThreadPoolExecutor(
        max_workers=WORKER_THREADS,
        thread_name_prefix="syslog-worker",
    )

    udp = UDPSyslogListener(writer, pool)
    tcp = TCPSyslogListener(writer)

    udp.start()
    tcp.start()

    try:
        udp.join()
        tcp.join()
    except KeyboardInterrupt:
        log.info("Shutting down - KeyboardInterrupt received.")
    finally:
        pool.shutdown(wait=False)
        writer.close()
        log.info("InfluxDB client closed. Bye.")


if __name__ == "__main__":
    main()
