#!/usr/bin/env python3
"""
=============================================================================
  NetApp ONTAP EMS Syslog Listener -> InfluxDB v2  (UnifiedOps -- SIFY)
=============================================================================

Production listener for SIFY NetApp AFF systems.  Receives ONTAP EMS
(Event Management System) syslog forwards on TCP/UDP port 516 and writes
**hardware-only** alerts into a dedicated InfluxDB bucket.

ONTAP EMS uses a legacy-netapp syslog format (RFC 3164 variant):
    <PRI>TIMESTAMP NodeName: process: event.name:severity: Message text

The EMS event name (e.g. callhome.diskFailure, monitor.shelf.fault) is
the key discriminator.  Only hardware-related events are persisted;
everything else (audit, CIFS, NFS, LUN ops, etc.) is silently dropped.

Configuration overrides (typical: /etc/hi-track/listener.netapp.sify.env):

    HITRACK_INFLUX_URL      default http://127.0.0.1:8287
    HITRACK_INFLUX_TOKEN    *** required for writes ***
    HITRACK_INFLUX_ORG      default HDFC
    HITRACK_INFLUX_BUCKET   default NetApp_SIFY_Bucket
    HITRACK_LISTEN_HOST     default 0.0.0.0
    HITRACK_LISTEN_PORT     default 516
    HITRACK_TEST_MODE       "1" to enable loopback spoofing for dev
    HITRACK_TEST_DEFAULT_IP fallback source IP for test mode
"""
from __future__ import annotations

import ipaddress
import logging
import os
import re
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS, WriteOptions

# ---------------------------------------------------------------------------
# LOCATION / VENDOR
# ---------------------------------------------------------------------------
LOCATION = "SIFY"
VENDOR   = "NetApp"

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
INFLUX_URL    = os.environ.get("HITRACK_INFLUX_URL",    "http://127.0.0.1:8287")
INFLUX_TOKEN  = os.environ.get("HITRACK_INFLUX_TOKEN",  "hitrack-dev-token-please-change")
INFLUX_ORG    = os.environ.get("HITRACK_INFLUX_ORG",    "HDFC")
INFLUX_BUCKET = os.environ.get("HITRACK_INFLUX_BUCKET", "NetApp_SIFY_Bucket")

LISTEN_HOST = os.environ.get("HITRACK_LISTEN_HOST", "0.0.0.0")
LISTEN_PORT = int(os.environ.get("HITRACK_LISTEN_PORT", "516"))

BUFFER_SIZE = 8192
LOG_LEVEL   = logging.INFO

WORKER_THREADS           = max(2, int(os.environ.get("HITRACK_WORKER_THREADS", "16")))
WRITE_BATCH              = os.environ.get("HITRACK_WRITE_BATCH", "1").lower() in ("1", "true", "yes", "on")
WRITE_BATCH_SIZE         = max(1, int(os.environ.get("HITRACK_WRITE_BATCH_SIZE", "200")))
WRITE_FLUSH_MS           = max(50, int(os.environ.get("HITRACK_WRITE_FLUSH_MS", "1000")))
WRITE_JITTER_MS          = max(0, int(os.environ.get("HITRACK_WRITE_JITTER_MS", "0")))
WRITE_RETRY_INTERVAL_MS  = max(50, int(os.environ.get("HITRACK_WRITE_RETRY_MS", "1000")))

TEST_MODE = os.environ.get("HITRACK_TEST_MODE", "").lower() in ("1", "true", "yes", "on")
TEST_DEFAULT_IP = os.environ.get("HITRACK_TEST_DEFAULT_IP", "")
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
        logging.FileHandler("syslog_trap_listener_netapp_sify.log"),
    ],
)
log = logging.getLogger("syslog_trap_listener_netapp_sify")

raw_log = logging.getLogger("raw_syslog_netapp_sify")
raw_log.setLevel(logging.INFO)
raw_fh = logging.FileHandler("syslog_trap_listener_netapp_sify_raw_syslog_data.log")
raw_fh.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
raw_log.addHandler(raw_fh)
raw_log.propagate = False

# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------
HB_URL      = os.environ.get("HITRACK_HEARTBEAT_URL",    "").strip()
HB_TOKEN    = os.environ.get("HITRACK_HEARTBEAT_TOKEN",  "").strip()
HB_ORG      = os.environ.get("HITRACK_HEARTBEAT_ORG",    "HDFC").strip()
HB_BUCKET   = os.environ.get("HITRACK_HEARTBEAT_BUCKET", "").strip()
HB_INTERVAL = max(5, int(os.environ.get("HITRACK_HEARTBEAT_INTERVAL", "15")))
HB_LISTENER = f"{VENDOR.lower()}-{LOCATION.lower()}"

_msg_count: int = 0
_hw_count: int = 0


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
                .field("hw_count",    int(_hw_count))
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
# IP MAPPINGS (SIFY)
# Extracted from SIFY Rabale NetApp inventory.
# ---------------------------------------------------------------------------
IP_FILTER: Dict[str, str] = {
    "10.226.60.240": "netapp_storage",
    "10.226.62.6": "netapp_storage",
    "10.226.62.100": "netapp_storage",
    "10.226.62.155": "netapp_storage",
    "10.226.63.156": "netapp_storage",
    "10.226.80.76": "netapp_storage",
    "10.226.80.100": "netapp_storage",
    "10.226.80.166": "netapp_storage",
    "10.226.81.201": "netapp_storage",
    "10.226.82.235": "netapp_storage",
    "10.226.82.226": "netapp_storage",
    "10.226.82.224": "netapp_storage",
    "10.226.83.92": "netapp_storage",
    "10.226.83.90": "netapp_storage",
    "10.226.83.93": "netapp_storage",
    "10.226.116.218": "netapp_storage",
    "10.226.157.145": "netapp_storage",
    "10.226.157.150": "netapp_storage",
    "10.226.157.139": "netapp_storage",
    "10.226.157.144": "netapp_storage",
    "10.226.157.127": "netapp_storage",
    "10.226.157.132": "netapp_storage",
    "10.226.157.115": "netapp_storage",
    "10.226.157.120": "netapp_storage",
    "10.64.12.14": "netapp_storage",
    "10.64.12.19": "netapp_storage",
}

IP_TO_STORAGE_NAME: Dict[str, str] = {
    "10.226.60.240": "AFF_A700_20154-SIFY",
    "10.226.62.6": "AFF_A800_20217-SIFY",
    "10.226.62.100": "AFF_A800_20248-SIFY",
    "10.226.62.155": "AFF_A700_20236-SIFY",
    "10.226.63.156": "AFF_A700_20317-SIFY",
    "10.226.80.76": "AFF_A800_20333-SIFY",
    "10.226.80.100": "AFF_A800_20335-SIFY",
    "10.226.80.166": "AFF_A800_20338-SIFY",
    "10.226.81.201": "AFF_A700_20363-SIFY",
    "10.226.82.235": "AFF_A800_20400-SIFY",
    "10.226.82.226": "AFF_A800_20403-SIFY",
    "10.226.82.224": "AFF_A800_20404-SIFY",
    "10.226.83.92": "AFF_A800_20430-SIFY",
    "10.226.83.90": "AFF_A800_20431-SIFY",
    "10.226.83.93": "AFF_A800_20431-SIFY",
    "10.226.116.218": "AFF_A900_20491-SIFY",
    "10.226.157.145": "AFF_A900_20490-SIFY",
    "10.226.157.150": "AFF_A900_20490-SIFY",
    "10.226.157.139": "AFF_A900_20488-SIFY",
    "10.226.157.144": "AFF_A900_20488-SIFY",
    "10.226.157.127": "AFF_A900_20487-SIFY",
    "10.226.157.132": "AFF_A900_20487-SIFY",
    "10.226.157.115": "AFF_A900_20486-SIFY",
    "10.226.157.120": "AFF_A900_20486-SIFY",
    "10.64.12.14": "AFF_A900_20514-SIFY",
    "10.64.12.19": "AFF_A900_20514-SIFY",
}

# ---------------------------------------------------------------------------
# IP FILTER HELPERS
# ---------------------------------------------------------------------------

def _build_filter_table(ip_filter: Dict[str, str]) -> List[Tuple[object, str]]:
    table = []
    for entry, measurement in ip_filter.items():
        try:
            net = ipaddress.ip_network(entry, strict=False)
            table.append((net, measurement))
        except ValueError:
            log.warning("Invalid IP filter entry skipped: %s", entry)
    return table


FILTER_TABLE = _build_filter_table(IP_FILTER)


def classify_source(ip_str: str) -> Optional[str]:
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


def apply_test_mode(raw: bytes, source_ip: str) -> Tuple[bytes, str, bool]:
    if not TEST_MODE:
        return raw, source_ip, False
    is_loopback = source_ip in TEST_LOOPBACK_IPS or source_ip.startswith("127.")
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

    if is_loopback:
        chosen_ip = spoof_ip or TEST_DEFAULT_IP
        if chosen_ip and chosen_ip not in IP_FILTER:
            log.warning(
                "TEST_MODE: spoof IP %s is not in IP_FILTER",
                chosen_ip,
            )
        return raw, chosen_ip or source_ip, True
    return raw, source_ip, False


# ---------------------------------------------------------------------------
# NETAPP ONTAP EMS EVENT NAME -> HARDWARE CATEGORY MAPPING
# ---------------------------------------------------------------------------
NETAPP_HARDWARE_EMS_EXACT: Dict[str, str] = {
    # ── Hardware / Health Monitor ──
    "hm.alert.raised":                    "hardware_alert",
    "hm.alert.cleared":                   "hardware_cleared",
    "hm.monitor.startingMonitoring":      "hardware_notice",
    "callhome.hm.alert.major":            "hardware_alert",
    "callhome.hm.alert.minor":            "hardware_alert",

    # ── Fan ──
    "callhome.shlf.fan":                  "fan_failure",
    "callhome.shlf.fan.warn":             "fan_warning",
    "monitor.fan.critical":               "fan_failure",
    "monitor.fan.failed":                 "fan_failure",
    "monitor.fan.warning":                "fan_warning",
    "monitor.fan.ok":                     "fan_ok",
    "callhome.fan.failed":                "fan_failure",

    # ── Power Supply ──
    "callhome.shlf.power.intr":           "power_failure",
    "callhome.shlf.ps.fault":             "power_failure",
    "monitor.psu.failed":                 "power_failure",
    "monitor.psu.warning":                "power_warning",
    "monitor.psu.ok":                     "power_ok",
    "callhome.psu.failed":                "power_failure",
    "monitor.ioCard.degraded":            "hardware_alert",

    # ── Temperature ──
    "callhome.shlf.overtemp":             "temperature_alarm",
    "callhome.shlf.fault":                "shelf_fault",
    "monitor.temp.critical":              "temperature_alarm",
    "monitor.temp.warning":               "temperature_warning",
    "monitor.temp.ok":                    "temperature_ok",
    "callhome.temp.high":                 "temperature_alarm",
    "callhome.temp.low":                  "temperature_alarm",

    # ── Disk / Drive ──
    "disk.failure":                       "disk_failure",
    "disk.predictiveFailure":             "disk_failure",
    "disk.ioMedErr":                      "disk_warning",
    "disk.slippedSector":                 "disk_warning",
    "disk.realmSlippedSector":            "disk_warning",
    "disk.encryptionErr":                 "disk_warning",
    "disk.readError":                     "disk_warning",
    "disk.writeError":                    "disk_warning",
    "disk.readReassign":                  "disk_warning",
    "disk.writeReassign":                 "disk_warning",
    "disk.removed":                       "disk_failure",
    "callhome.disk.failure":              "disk_failure",
    "callhome.disk.predictive.failure":   "disk_failure",
    "callhome.disk.missing.carrier":      "disk_failure",

    # ── RAID (hardware-driven degradation) ──
    "raid.rg.degraded":                   "raid_degraded",
    "raid.rg.double.degraded":            "raid_degraded",
    "raid.disk.missing":                  "disk_failure",
    "raid.spare.missing":                 "disk_warning",
    "raid.rg.recons.started":             "raid_rebuild",
    "raid.rg.recons.done":                "raid_rebuild",
    "callhome.raid.degraded":             "raid_degraded",

    # ── NVRAM / Battery ──
    "nvram.battery.low":                  "battery_alert",
    "nvram.battery.failed":               "battery_alert",
    "nvram.battery.discharging":          "battery_alert",
    "nvram.battery.charging":             "battery_ok",
    "nvram.battery.fullyCharged":         "battery_ok",
    "nvram.lowcharge":                    "battery_alert",
    "callhome.nvram.battery":             "battery_alert",
    "callhome.nvram.battery.low":         "battery_alert",
    "callhome.nvram.hw.failure":          "battery_alert",

    # ── Shelf / SES ──
    "ses.accessError":                    "shelf_fault",
    "ses.portError":                      "shelf_fault",
    "ses.statusEvent":                    "shelf_event",
    "ses.configError":                    "shelf_fault",
    "shelf.fault":                        "shelf_fault",
    "shelf.temp.fan.fail":                "fan_failure",
    "shelf.environmental.sensor.error":   "shelf_fault",
    "callhome.ses.error":                 "shelf_fault",

    # ── Controller / HA ──
    "cf.takeover.started":                "controller_takeover",
    "cf.takeover.done":                   "controller_takeover",
    "cf.takeover.of.partner.started":     "controller_takeover",
    "cf.giveback.started":                "controller_giveback",
    "cf.giveback.done":                   "controller_giveback",
    "cf.giveback.completion":             "controller_giveback",
    "cf.reboot":                          "controller_fault",
    "cf.interconnect.down":               "controller_fault",
    "cf.interconnect.up":                 "controller_link",
    "cf.partner.link.status":             "controller_link",
    "cf.partner.not.responding":          "controller_fault",
    "cf.partner.down":                    "controller_fault",
    "callhome.reboot.failure":            "controller_fault",

    # ── Node ──
    "node.panic":                         "node_fault",
    "node.down":                          "node_fault",
    "node.up":                            "node_ok",
    "node.failed":                        "node_fault",
    "callhome.panic":                     "node_fault",

    # ── Network port / link ──
    "port.linkDown":                      "link_down",
    "port.linkUp":                        "link_up",
    "net.port.linkDown":                  "link_down",
    "net.port.linkUp":                    "link_up",
    "callhome.sp.net.link.err":           "link_down",

    # ── HBA / SAS / FC ──
    "HBA.offline":                        "port_fault",
    "HBA.online":                         "port_ok",
    "sas.path.disconnect":                "port_fault",
    "sas.path.connect":                   "port_ok",
    "fci.link.down":                      "link_down",
    "fci.link.up":                        "link_up",
    "fci.port.offline":                   "port_fault",
    "fci.port.online":                    "port_ok",
    "callhome.fc.hba.fault":              "port_fault",

    # ── Service Processor / BMC ──
    "sp.heartbeat.stopped":               "sp_alert",
    "sp.heartbeat.resumed":               "sp_alert",
    "sp.firmware.update.failed":          "sp_alert",
    "sp.reset":                           "sp_alert",
    "callhome.sp.firmware.update.fail":   "sp_alert",
    "callhome.sp.hb.stopped":             "sp_alert",
}

NETAPP_HARDWARE_EMS_PREFIX: Tuple[Tuple[str, str], ...] = (
    ("callhome.shlf.",      "shelf_fault"),
    ("callhome.nvram.",     "battery_alert"),
    ("callhome.disk.",      "disk_failure"),
    ("callhome.fan.",       "fan_failure"),
    ("callhome.psu.",       "power_failure"),
    ("callhome.temp.",      "temperature_alarm"),
    ("callhome.hm.",        "hardware_alert"),
    ("callhome.ses.",       "shelf_fault"),
    ("callhome.fc.",        "port_fault"),
    ("callhome.sp.",        "sp_alert"),
    ("callhome.reboot.",    "controller_fault"),
    ("callhome.panic",      "node_fault"),
    ("callhome.raid.",      "raid_degraded"),
    ("hm.",                 "hardware_alert"),
    ("monitor.fan",         "fan_failure"),
    ("monitor.psu",         "power_failure"),
    ("monitor.temp",        "temperature_alarm"),
    ("monitor.io",          "hardware_alert"),
    ("disk.",               "disk_failure"),
    ("nvram.",              "battery_alert"),
    ("ses.",                "shelf_fault"),
    ("shelf.",              "shelf_fault"),
    ("cf.",                 "controller_fault"),
    ("raid.",               "raid_degraded"),
    ("HBA.",                "port_fault"),
    ("sas.",                "port_fault"),
    ("fci.",                "link_down"),
    ("sp.",                 "sp_alert"),
    ("port.link",           "link_down"),
    ("net.port.",           "link_down"),
)

ALL_HW_CATEGORIES = sorted(set(
    list(NETAPP_HARDWARE_EMS_EXACT.values()) +
    [cat for _, cat in NETAPP_HARDWARE_EMS_PREFIX]
))

def classify_ems_event(event_name: str) -> Optional[str]:
    lower = event_name.lower()
    exact = NETAPP_HARDWARE_EMS_EXACT.get(event_name) or NETAPP_HARDWARE_EMS_EXACT.get(lower)
    if exact:
        return exact

    for prefix, category in NETAPP_HARDWARE_EMS_PREFIX:
        if lower.startswith(prefix.lower()):
            return category
    return None


# ---------------------------------------------------------------------------
# SYSLOG PARSING
# ---------------------------------------------------------------------------
FACILITY_NAMES = [
    "kern", "user", "mail", "daemon", "auth", "syslog", "lpr", "news",
    "uucp", "cron", "authpriv", "ftp", "ntp", "log_audit", "log_alert",
    "clock", "local0", "local1", "local2", "local3", "local4", "local5",
    "local6", "local7",
]
SEVERITY_NAMES = [
    "emergency", "alert", "critical", "error",
    "warning", "notice", "informational", "debug",
]

ONTAP_LEGACY_RE = re.compile(
    r"(?:<\d+>)?"
    r"(?:\w{3}\s+\d+\s+\d{2}:\d{2}:\d{2}\s+)?"
    r"(?:\S+\s+)?"
    r"\[?"
    r"(?P<node>[\w][\w.\-]*)"
    r":\s*"
    r"(?P<process>[\w.\-_]+)"
    r":\s*"
    r"(?P<ems_event>[\w][\w.]*[\w])"
    r":\s*"
    r"(?:\[?\s*(?P<ems_severity>"
    r"EMERGENCY|ALERT|CRITICAL|ERROR|WARNING|NOTICE|INFORMATIONAL|DEBUG|"
    r"kern_emerg|kern_alert|kern_crit|kern_err|kern_warning|kern_notice|kern_info|kern_debug|"
    r"emergency|alert|critical|error|warning|notice|informational|debug"
    r")\s*\]?\s*:?\s*)?"
    r"\]?\s*:?\s*"
    r"(?P<message>.+)",
    re.I,
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

PRI_RE = re.compile(r"^<(?P<pri>\d{1,3})>")

ONTAP_SEVERITY_MAP: Dict[str, str] = {
    "emergency": "critical", "kern_emerg": "critical",
    "alert": "critical", "kern_alert": "critical",
    "critical": "critical", "kern_crit": "critical",
    "error": "error", "kern_err": "error",
    "warning": "warning", "kern_warning": "warning",
    "notice": "notice", "kern_notice": "notice",
    "informational": "informational", "kern_info": "informational",
    "debug": "informational", "kern_debug": "informational",
}


def decode_priority(pri: int) -> Dict[str, object]:
    facility = pri >> 3
    severity = pri & 0x07
    return {
        "facility":      FACILITY_NAMES[facility] if facility < len(FACILITY_NAMES) else str(facility),
        "severity":      SEVERITY_NAMES[severity] if severity < len(SEVERITY_NAMES) else str(severity),
        "facility_code": facility,
        "severity_code": severity,
    }


def parse_syslog(raw: bytes, source_ip: str) -> Optional[Dict[str, object]]:
    global _msg_count
    _msg_count += 1

    try:
        text = raw.decode("utf-8", errors="replace").strip()
        if text:
            raw_log.info("[%s] %s", source_ip, text)
    except Exception:
        return None

    if not text:
        return None

    fields: Dict[str, object] = {"raw_message": text}

    pri_match = PRI_RE.match(text)
    if pri_match:
        pri = int(pri_match.group("pri"))
        fields.update(decode_priority(pri))
        fields["priority"] = pri
    else:
        fields["severity"] = "informational"
        fields["facility"] = "local0"
        fields["facility_code"] = 16
        fields["severity_code"] = 6

    ems_event_name = ""
    ems_severity_raw = ""
    ems_process = ""
    node_name = ""
    message = text

    m = ONTAP_LEGACY_RE.search(text)
    if m:
        node_name = m.group("node") or ""
        ems_process = m.group("process") or ""
        ems_event_name = m.group("ems_event") or ""
        ems_severity_raw = (m.group("ems_severity") or "").strip().lower()
        message = m.group("message") or ""
        fields["syslog_format"] = "ONTAP_LEGACY"
    else:
        m5 = RFC5424_RE.match(text)
        if m5:
            gd = m5.groupdict()
            node_name = gd.get("hostname", "")
            ems_process = gd.get("appname", "")
            message = gd.get("message", "")
            fields["syslog_format"] = "RFC5424"
            inner = ONTAP_LEGACY_RE.search(message)
            if inner:
                ems_event_name = inner.group("ems_event") or ""
                ems_severity_raw = (inner.group("ems_severity") or "").strip().lower()
                message = inner.group("message") or message
        else:
            fields["syslog_format"] = "UNKNOWN"

    trap_category = None
    if ems_event_name:
        trap_category = classify_ems_event(ems_event_name)

    if trap_category is None:
        log.debug(
            "Dropped non-hardware event from %s: ems=%s msg=%.80s",
            source_ip, ems_event_name or "(none)", message,
        )
        return None

    global _hw_count
    _hw_count += 1

    if ems_severity_raw and ems_severity_raw in ONTAP_SEVERITY_MAP:
        fields["severity"] = ONTAP_SEVERITY_MAP[ems_severity_raw]

    storage_name = resolve_storage_name(source_ip)
    fields["hostname"] = node_name or storage_name
    fields["message"] = message
    fields["vendor"] = "netapp"
    fields["trap_category"] = trap_category
    fields["array_name"] = storage_name
    fields["ems_event_name"] = ems_event_name
    fields["ems_severity"] = ems_severity_raw
    fields["ems_process"] = ems_process
    fields["error_message"] = message

    for cat in ALL_HW_CATEGORIES:
        fields[f"trap_{cat}"] = (cat == trap_category)

    return fields


# ---------------------------------------------------------------------------
# INFLUXDB WRITER
# ---------------------------------------------------------------------------

class InfluxWriter:
    def __init__(self) -> None:
        self.client = InfluxDBClient(
            url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, verify_ssl=False
        )
        if WRITE_BATCH:
            opts = WriteOptions(
                batch_size=WRITE_BATCH_SIZE, flush_interval=WRITE_FLUSH_MS,
                jitter_interval=WRITE_JITTER_MS, retry_interval=WRITE_RETRY_INTERVAL_MS,
            )
            self.write_api = self.client.write_api(write_options=opts)
            log.info("InfluxDB -> %s (bucket=%s) [batch=%d]", INFLUX_URL, INFLUX_BUCKET, WRITE_BATCH_SIZE)
        else:
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            log.info("InfluxDB -> %s (bucket=%s) [SYNCHRONOUS]", INFLUX_URL, INFLUX_BUCKET)

    def write(self, measurement: str, source_ip: str, fields: Dict[str, object]) -> None:
        point = (
            Point(measurement)
            .tag("source_ip", source_ip).tag("environment", LOCATION)
            .tag("syslog_format", str(fields.get("syslog_format", "UNKNOWN")))
            .tag("severity", str(fields.get("severity", "unknown")))
            .tag("facility", str(fields.get("facility", "unknown")))
            .tag("vendor", str(fields.get("vendor", "unknown")))
            .tag("trap_category", str(fields.get("trap_category", "none")))
            .tag("array_name", str(fields.get("array_name", "unknown")))
            .time(datetime.now(timezone.utc), WritePrecision.NS)
        )
        for key in ("hostname", "message", "raw_message", "error_message",
                     "ems_event_name", "ems_severity", "ems_process"):
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
        except Exception as exc:
            log.error("InfluxDB write failed: %s", exc)

    def close(self) -> None:
        self.client.close()


# ---------------------------------------------------------------------------
# TCP LISTENER
# ---------------------------------------------------------------------------

class TCPSyslogListener(threading.Thread):
    def __init__(self, writer: InfluxWriter) -> None:
        super().__init__(daemon=True, name="TCPSyslogListener")
        self.writer = writer

    def run(self) -> None:
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((LISTEN_HOST, LISTEN_PORT))
        srv.listen(50)
        log.info("TCP listener on %s:%d", LISTEN_HOST, LISTEN_PORT)
        while True:
            try:
                conn, addr = srv.accept()
                threading.Thread(target=self._handle_client, args=(conn, addr[0]), daemon=True).start()
            except Exception as exc:
                log.error("TCP accept error: %s", exc)

    def _handle_client(self, conn: socket.socket, source_ip: str) -> None:
        loopback = source_ip in TEST_LOOPBACK_IPS or source_ip.startswith("127.")
        if not (TEST_MODE and loopback):
            measurement = classify_source(source_ip)
            if measurement is None:
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
                while buf:
                    m = re.match(br'^(\d+)\s(.*)', buf, re.DOTALL)
                    if m:
                        msg_len = int(m.group(1))
                        header_len = len(m.group(1)) + 1
                        if len(buf) >= header_len + msg_len:
                            line = buf[header_len : header_len + msg_len]
                            buf = buf[header_len + msg_len :]
                        else:
                            break
                    elif b"\n" in buf:
                        line, buf = buf.split(b"\n", 1)
                    elif b"\0" in buf:
                        line, buf = buf.split(b"\0", 1)
                    else:
                        break

                    if not line.strip():
                        continue
                    line, effective_ip, spoofed = apply_test_mode(line, source_ip)
                    line_measurement = measurement or classify_source(effective_ip)
                    if line_measurement is None:
                        continue
                    fields = parse_syslog(line, effective_ip)
                    if not fields:
                        continue
                    log.info("[TCP] %s (%s)%s sev=%s cat=%s ems=%s", effective_ip,
                             fields.get("array_name"), " (spoofed)" if spoofed else "",
                             fields.get("severity"), fields.get("trap_category"),
                             fields.get("ems_event_name"))
                    self.writer.write(line_measurement, effective_ip, fields)
        except Exception as exc:
            log.error("TCP client error (%s): %s", source_ip, exc)
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------

def main() -> None:
    missing = sorted(set(IP_FILTER) - set(IP_TO_STORAGE_NAME))
    if missing:
        log.warning("%d IPs missing from IP_TO_STORAGE_NAME: %s", len(missing), ", ".join(missing))

    log.info("=" * 60)
    log.info(" NetApp ONTAP EMS Listener (%s) - starting up", LOCATION)
    log.info(" Influx URL      : %s", INFLUX_URL)
    log.info(" Influx bucket   : %s", INFLUX_BUCKET)
    log.info(" IP_FILTER       : %d entries", len(IP_FILTER))
    log.info(" HW categories   : %d", len(ALL_HW_CATEGORIES))
    log.info("=" * 60)
    _start_heartbeat()

    writer = InfluxWriter()
    tcp = TCPSyslogListener(writer)
    tcp.start()

    try:
        tcp.join()
    except KeyboardInterrupt:
        log.info("Shutting down - KeyboardInterrupt received.")
    finally:
        writer.close()
        log.info("InfluxDB client closed. Bye.")


if __name__ == "__main__":
    main()
