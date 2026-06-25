#!/usr/bin/env python3
"""
=============================================================================
  Syslog Trap Listener -> InfluxDB v2  (UnifiedOps -- Brocade FOS via SANnav)
  Location pair: CDVL + SIFY  (one SANnav forwards both DCs to this VM)
=============================================================================

Self-contained listener for the CDVL+SIFY Brocade SAN pipeline. Engine logic
and the switch inventory for *both* CDVL and SIFY live in this single file;
no shared modules, no other location referenced.

Wire format observed on UDP/TCP 515 (RFC5424, BOM-prefixed message body):

  <PRI>1 TIMESTAMP HOSTNAME APPNAME PROCID - \\
       [timestamp@1588 value="..."]                \\
       [msgid@1588    value="HAM-1004"]            \\
       [severity@1588 value="WARNING"]             \\
       [swname@1588   value="NTTC_X7_113_FAB1"]    \\
       [...]                                        \\
       <BOM>FOS message text

Pipeline:
  1) classify_source()   -> drop anything not from a known SANnav.
  2) parse RFC5424       -> pull structured-data block(s).
  3) parse_sannav_sd()   -> {msgid, severity, swname, timestamp, ...}.
  4) Resolve switch by swname (fallback: source IP, fallback: SANnav).
  5) Categorise:
        - msgid prefix matches FOS module (BL/EM/HAM/HIL/...) -> Brocade FOS
        - msgid prefix matches SSMP/NOS/...                   -> SANnav event
        - keyword refinement on the FOS message body picks a finer bucket.

Configuration overrides (typical: /etc/hi-track/listener.cdvl_n_sify.env):

    HITRACK_INFLUX_URL      default http://127.0.0.1:8086
    HITRACK_INFLUX_TOKEN    *** required for writes ***
    HITRACK_INFLUX_ORG      default HDFC
    HITRACK_INFLUX_BUCKET   default SYSLOG_BRCD_CDVL_SIFY_Bucket
    HITRACK_LISTEN_HOST     default 0.0.0.0
    HITRACK_LISTEN_PORT     default 515          (TCP listens on +1)
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
# LOCATION
# ---------------------------------------------------------------------------
LOCATION_PAIR = "CDVL_SIFY"
LOCATION      = "CDVL"
VENDOR        = "Brocade"

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
INFLUX_URL    = os.environ.get("HITRACK_INFLUX_URL",    "http://127.0.0.1:8086")
INFLUX_TOKEN  = os.environ.get("HITRACK_INFLUX_TOKEN",  "hitrack-dev-token-please-change")
INFLUX_ORG    = os.environ.get("HITRACK_INFLUX_ORG",    "HDFC")
INFLUX_BUCKET = os.environ.get("HITRACK_INFLUX_BUCKET", "SYSLOG_BRCD_CDVL_SIFY_Bucket")
INFLUX_REPORT_URL = os.environ.get("HITRACK_INFLUX_REPORT_URL", "")
INFLUX_REPORT_TOKEN = os.environ.get("HITRACK_INFLUX_REPORT_TOKEN", "")
INFLUX_BUCKET_REPORT = os.environ.get("HITRACK_INFLUX_REPORT_BUCKET", f"unified-ops-bucket-health-check-report-{LOCATION.lower()}")

LISTEN_HOST = os.environ.get("HITRACK_LISTEN_HOST", "0.0.0.0")
LISTEN_PORT = int(os.environ.get("HITRACK_LISTEN_PORT", "515"))

BUFFER_SIZE = 8192
LOG_LEVEL   = logging.INFO

WORKER_THREADS           = max(2, int(os.environ.get("HITRACK_WORKER_THREADS", "16")))
WRITE_BATCH              = os.environ.get("HITRACK_WRITE_BATCH", "1").lower() in ("1", "true", "yes", "on")
WRITE_BATCH_SIZE         = max(1, int(os.environ.get("HITRACK_WRITE_BATCH_SIZE", "200")))
WRITE_FLUSH_MS           = max(50, int(os.environ.get("HITRACK_WRITE_FLUSH_MS", "1000")))
WRITE_JITTER_MS          = max(0, int(os.environ.get("HITRACK_WRITE_JITTER_MS", "0")))
WRITE_RETRY_INTERVAL_MS  = max(50, int(os.environ.get("HITRACK_WRITE_RETRY_MS", "1000")))

TEST_MODE = os.environ.get("HITRACK_TEST_MODE", "").lower() in ("1", "true", "yes", "on")
TEST_DEFAULT_IP = os.environ.get("HITRACK_TEST_DEFAULT_IP", "0.0.0.0")
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
        logging.FileHandler("syslog_trap_listener_cdvl_n_sify.log"),
    ],
)
log = logging.getLogger("syslog_trap_listener_cdvl_n_sify")

raw_log = logging.getLogger("raw_syslog_cdvl_n_sify")
raw_log.setLevel(logging.INFO)
raw_fh = logging.FileHandler("syslog_trap_listener_cdvl_n_sify_raw_syslog_data.log")
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
# SOURCE / INVENTORY MAPS
# ---------------------------------------------------------------------------

SANNAV_SOURCES: set[str] = {
    # TODO: populate with the CDVL+SIFY SANnav IP once provided
    # "10.x.y.z": "<sannav-hostname>",
}


# Each value: (environment, owner, model). Keys are UPPER-CASE normalised.
SWITCH_INFO: dict[str, tuple] = {
    # --------------------- CDVL (NTT Chandivali) -------------------------
    "CDVL_7840_HUR_FAB1":      ("CDVL", "Hitachi", "7840 FCIP Switch"),
    "CDVL_7840_HUR_FAB2":      ("CDVL", "Hitachi", "7840 FCIP Switch"),
    "NTTC_DC6A_7840_HUR_FAB1": ("CDVL", "Hitachi", "7840 FCIP Switch"),
    "NTTC_DC6A_7840_HUR_FAB2": ("CDVL", "Hitachi", "7840 FCIP Switch"),

    "NTTC_X6_107_FAB1": ("CDVL", "Netapp", "DCX X6-8"),
    "NTTC_X6_108_FAB2": ("CDVL", "Netapp", "DCX X6-8"),

    "NTTC_X7_101_FAB1": ("CDVL", "Netapp",  "DCX X7-8"),
    "NTTC_X7_102_FAB2": ("CDVL", "Netapp",  "DCX X7-8"),
    "NTTC_X7_103_FAB1": ("CDVL", "Netapp",  "DCX X7-8"),
    "NTTC_X7_104_FAB2": ("CDVL", "Netapp",  "DCX X7-8"),
    "NTTC_X7_105_FAB1": ("CDVL", "Netapp",  "DCX X7-8"),
    "NTTC_X7_106_FAB2": ("CDVL", "Netapp",  "DCX X7-8"),
    "NTTC_X7_109_FAB1": ("CDVL", "Hitachi", "DCX X7-8"),
    "NTTC_X7_110_FAB2": ("CDVL", "Hitachi", "DCX X7-8"),
    "NTTC_X7_111_FAB1": ("CDVL", "Hitachi", "DCX X7-8"),
    "NTTC_X7_112_FAB2": ("CDVL", "Hitachi", "DCX X7-8"),
    "NTTC_X7_113_FAB1": ("CDVL", "Hitachi", "DCX X7-8"),
    "NTTC_X7_114_FAB2": ("CDVL", "Hitachi", "DCX X7-8"),
    "NTTC_X7_115_FAB1": ("CDVL", "Netapp",  "DCX X7-8"),
    "NTTC_X7_116_FAB2": ("CDVL", "Netapp",  "DCX X7-8"),
    "NTTC_X7_117_FAB1": ("CDVL", "Netapp",  "DCX X7-8"),
    "NTTC_X7_118_FAB2": ("CDVL", "Netapp",  "DCX X7-8"),

    "NTT_DC9_X7_121_FAB1": ("CDVL", "Netapp",  "DCX X7-8"),
    "NTT_DC9_X7_122_FAB2": ("CDVL", "Netapp",  "DCX X7-8"),
    "NTT_DC9_X7_123_FAB1": ("CDVL", "Hitachi", "DCX X7-8"),
    "NTT_DC9_X7_124_FAB2": ("CDVL", "Hitachi", "DCX X7-8"),
    "NTT_DC9_X7_125_FAB1": ("CDVL", "Hitachi", "DCX X7-8"),
    "NTT_DC9_X7_126_FAB2": ("CDVL", "Hitachi", "DCX X7-8"),
    "NTT_DC9_X7_127_FAB1": ("CDVL", "Hitachi", "DCX X7-8"),
    "NTT_DC9_X7_128_FAB2": ("CDVL", "Hitachi", "DCX X7-8"),

    # The two un-named DCX X7-8 / Hitachi switches on 10.227.66.210-211
    # and 10.227.66.212-213. Replace these placeholder names once the real
    # switch names are confirmed; SD swname will then resolve cleanly.
    "CDVL_X7_UNNAMED_FAB1": ("CDVL", "Hitachi", "DCX X7-8"),
    "CDVL_X7_UNNAMED_FAB2": ("CDVL", "Hitachi", "DCX X7-8"),

    # --------------------- SIFY (SIFY Rabale) ----------------------------
    "DAKC_7840_HUR_FAB1": ("SIFY", "Hitachi", "7840 FCIP Switch"),
    "DAKC_7840_HUR_FAB2": ("SIFY", "Hitachi", "7840 FCIP Switch"),

    "SIFY_X6_101_FAB1": ("SIFY", "Netapp", "DCX X6-8"),
    "SIFY_X6_102_FAB2": ("SIFY", "Netapp", "DCX X6-8"),
    "SIFY_X6_103_FAB1": ("SIFY", "Netapp", "DCX X6-8"),
    "SIFY_X6_104_FAB2": ("SIFY", "Netapp", "DCX X6-8"),
    "SIFY_X6_105_FAB1": ("SIFY", "Netapp", "DCX X6-8"),
    "SIFY_X6_106_FAB2": ("SIFY", "Netapp", "DCX X6-8"),
    "SIFY_X6_107_FAB1": ("SIFY", "Netapp", "DCX X6-8"),
    "SIFY_X6_108_FAB2": ("SIFY", "Netapp", "DCX X6-8"),
    "SIFY_X6_109_FAB1": ("SIFY", "Netapp", "DCX X6-8"),
    "SIFY_X6_110_FAB2": ("SIFY", "Netapp", "DCX X6-8"),

    "SIFY_X7_111_FAB1": ("SIFY", "Netapp", "DCX X7-8"),
    "SIFY_X7_112_FAB2": ("SIFY", "Netapp", "DCX X7-8"),
}


SWITCH_IP_TO_NAME: dict[str, str] = {
    # --------------------- CDVL ------------------------------------------
    "10.66.12.197":   "CDVL_7840_HUR_FAB1",
    "10.66.12.198":   "CDVL_7840_HUR_FAB2",
    "10.227.63.173":  "NTTC_DC6A_7840_HUR_FAB1",
    "10.227.63.174":  "NTTC_DC6A_7840_HUR_FAB2",

    "10.227.62.65":   "NTTC_X6_107_FAB1",
    "10.227.62.66":   "NTTC_X6_107_FAB1",
    "10.227.62.67":   "NTTC_X6_107_FAB1",
    "10.227.62.68":   "NTTC_X6_108_FAB2",
    "10.227.62.69":   "NTTC_X6_108_FAB2",
    "10.227.62.70":   "NTTC_X6_108_FAB2",

    "10.227.61.37":   "NTTC_X7_101_FAB1",
    "10.227.61.40":   "NTTC_X7_102_FAB2",
    "10.227.61.43":   "NTTC_X7_103_FAB1",
    "10.227.61.46":   "NTTC_X7_104_FAB2",
    "10.227.61.227":  "NTTC_X7_105_FAB1",
    "10.227.61.230":  "NTTC_X7_106_FAB2",

    "10.227.63.142":  "NTTC_X7_109_FAB1",
    "10.227.63.145":  "NTTC_X7_110_FAB2",
    "10.227.63.148":  "NTTC_X7_111_FAB1",
    "10.227.63.151":  "NTTC_X7_112_FAB2",

    "10.227.64.135":  "NTTC_X7_113_FAB1",
    "10.227.64.136":  "NTTC_X7_113_FAB1",
    "10.227.64.137":  "NTTC_X7_113_FAB1",
    "10.227.64.138":  "NTTC_X7_114_FAB2",
    "10.227.64.139":  "NTTC_X7_114_FAB2",
    "10.227.64.140":  "NTTC_X7_114_FAB2",

    "10.227.66.102":  "NTTC_X7_115_FAB1",
    "10.227.66.103":  "NTTC_X7_115_FAB1",
    "10.227.66.104":  "NTTC_X7_115_FAB1",
    "10.227.66.105":  "NTTC_X7_116_FAB2",
    "10.227.66.106":  "NTTC_X7_116_FAB2",
    "10.227.66.107":  "NTTC_X7_116_FAB2",
    "10.227.66.202":  "NTTC_X7_117_FAB1",
    "10.227.66.203":  "NTTC_X7_117_FAB1",
    "10.227.66.204":  "NTTC_X7_117_FAB1",
    "10.227.66.205":  "NTTC_X7_118_FAB2",
    "10.227.66.206":  "NTTC_X7_118_FAB2",
    "10.227.66.207":  "NTTC_X7_118_FAB2",

    "10.226.116.229": "NTT_DC9_X7_121_FAB1",
    "10.226.116.230": "NTT_DC9_X7_121_FAB1",
    "10.226.116.231": "NTT_DC9_X7_121_FAB1",
    "10.226.116.232": "NTT_DC9_X7_122_FAB2",
    "10.226.116.233": "NTT_DC9_X7_122_FAB2",
    "10.226.116.234": "NTT_DC9_X7_122_FAB2",
    "10.226.116.247": "NTT_DC9_X7_123_FAB1",
    "10.226.116.248": "NTT_DC9_X7_123_FAB1",
    "10.226.116.249": "NTT_DC9_X7_123_FAB1",
    "10.226.116.250": "NTT_DC9_X7_124_FAB2",
    "10.226.116.251": "NTT_DC9_X7_124_FAB2",
    "10.226.116.252": "NTT_DC9_X7_124_FAB2",
    "10.226.116.235": "NTT_DC9_X7_125_FAB1",
    "10.226.116.236": "NTT_DC9_X7_125_FAB1",
    "10.226.116.237": "NTT_DC9_X7_125_FAB1",
    "10.226.116.238": "NTT_DC9_X7_126_FAB2",
    "10.226.116.239": "NTT_DC9_X7_126_FAB2",
    "10.226.116.240": "NTT_DC9_X7_126_FAB2",
    "10.226.116.241": "NTT_DC9_X7_127_FAB1",
    "10.226.116.242": "NTT_DC9_X7_127_FAB1",
    "10.226.116.243": "NTT_DC9_X7_127_FAB1",
    "10.226.116.244": "NTT_DC9_X7_128_FAB2",
    "10.226.116.245": "NTT_DC9_X7_128_FAB2",
    "10.226.116.246": "NTT_DC9_X7_128_FAB2",

    "10.227.66.210":  "CDVL_X7_UNNAMED_FAB1",
    "10.227.66.211":  "CDVL_X7_UNNAMED_FAB1",
    "10.227.66.212":  "CDVL_X7_UNNAMED_FAB2",
    "10.227.66.213":  "CDVL_X7_UNNAMED_FAB2",

    # --------------------- SIFY ------------------------------------------
    "10.226.14.79":   "DAKC_7840_HUR_FAB2",
    "10.226.14.80":   "DAKC_7840_HUR_FAB1",

    "10.226.63.70":   "SIFY_X6_109_FAB1",
    "10.226.63.73":   "SIFY_X6_110_FAB2",
    "10.226.63.122":  "SIFY_X6_103_FAB1",
    "10.226.63.125":  "SIFY_X6_104_FAB2",
    "10.226.63.170":  "SIFY_X6_101_FAB1",
    "10.226.63.173":  "SIFY_X6_102_FAB2",

    "10.226.80.174":  "SIFY_X6_107_FAB1",
    "10.226.80.177":  "SIFY_X6_108_FAB2",
    "10.226.80.180":  "SIFY_X6_105_FAB1",
    "10.226.80.183":  "SIFY_X6_106_FAB2",

    "10.226.83.4":    "SIFY_X7_111_FAB1",
    "10.226.83.7":    "SIFY_X7_112_FAB2",
}


# ---------------------------------------------------------------------------
# IP FILTER + SWITCH RESOLVER
# ---------------------------------------------------------------------------

def _build_filter_table(ip_col):
    table = []
    items = ip_col.keys() if hasattr(ip_col, "keys") else ip_col
    for entry in items:
        try:
            net = ipaddress.ip_network(entry, strict=False)
            table.append((net, "san_switch"))
        except ValueError:
            log.warning("Invalid SANnav IP entry skipped: %s", entry)
    return table


FILTER_TABLE = _build_filter_table(SANNAV_SOURCES)


# Dev / lab bypass: when HITRACK_ACCEPT_LOOPBACK=1, packets arriving over
# the loopback adapter are treated as a valid SAN switch trap so the
# trap-sender UI on the same dev VM can drive the pipeline. In production
# this is left unset and packets from non-SANnav hosts are still rejected.
_ACCEPT_LOOPBACK = os.environ.get("HITRACK_ACCEPT_LOOPBACK", "1") == "1"


def classify_source(ip_str):
    try:
        addr = ipaddress.ip_address(ip_str)
    except ValueError:
        return None
    if _ACCEPT_LOOPBACK and addr.is_loopback:
        return "san_switch"
    if TEST_MODE and _ACCEPT_LOOPBACK and ip_str in SWITCH_IP_TO_NAME:
        return "san_switch"
    for network, measurement in FILTER_TABLE:
        if addr in network:
            return measurement
    return None


def normalize_swname(swname):
    if not swname:
        return ""
    return swname.split(".", 1)[0].strip().upper()


def resolve_switch_meta(swname, source_ip):
    """Return (display_name, environment, owner, model)."""
    name = normalize_swname(swname)
    if name and name in SWITCH_INFO:
        env, owner, model = SWITCH_INFO[name]
        return name, env, owner, model

    if source_ip in SWITCH_IP_TO_NAME:
        ip_name = SWITCH_IP_TO_NAME[source_ip].upper()
        if ip_name in SWITCH_INFO:
            env, owner, model = SWITCH_INFO[ip_name]
            return ip_name, env, owner, model

    if source_ip in SANNAV_SOURCES:
        return "SANnav_Server", LOCATION_PAIR, "Broadcom", "SANnav"

    return name or "unknown", LOCATION_PAIR, "unknown", "unknown"


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
        if chosen_ip not in SANNAV_SOURCES and chosen_ip not in SWITCH_IP_TO_NAME:
            log.warning(
                "TEST_MODE: spoof IP %s is not known; falling back to %s",
                chosen_ip, TEST_DEFAULT_IP,
            )
            chosen_ip = TEST_DEFAULT_IP
        return raw, chosen_ip, True
    return raw, source_ip, False


# ---------------------------------------------------------------------------
# SYSLOG PARSING (RFC3164 / RFC5424)
# ---------------------------------------------------------------------------

RFC5424_RE = re.compile(
    r"^<(?P<pri>\d{1,3})>"
    r"(?P<version>\d+)\s+"
    r"(?P<timestamp>\S+)\s+"
    r"(?P<hostname>\S+)\s+"
    r"(?P<appname>\S+)\s+"
    r"(?P<procid>\S+)\s+"
    r"(?P<msgid>\S+)\s+"
    r"(?P<structured_data>(?:\[[^\[\]]*\]\s*)+|-)\s*"
    r"(?P<message>.*)$"
)

RFC3164_RE = re.compile(
    r"^<(?P<pri>\d{1,3})>\s*"
    r"(?P<timestamp>\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+"
    r"(?P<hostname>\S+)\s+"
    r"(?P<process>[^:\[]+)(?:\[(?P<pid>\d+)\])?:\s*"
    r"(?P<message>.+)$"
)

SANNAV_SD_RE = re.compile(
    r"\[(?P<key>[A-Za-z0-9_-]+)@\d+\s+value=\"(?P<value>[^\"]*)\"\]"
)


def parse_sannav_sd(sd_text):
    if not sd_text or sd_text == "-":
        return {}
    out = {}
    for m in SANNAV_SD_RE.finditer(sd_text):
        out[m.group("key").lower()] = m.group("value")
    return out


def strip_bom(s):
    if not s:
        return s
    while s and s[0] == "\ufeff":
        s = s[1:]
    return s


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


# ---------------------------------------------------------------------------
# BROCADE FOS / SANNAV CLASSIFICATION
# ---------------------------------------------------------------------------

BRCD_MODULE_LABEL = {
    "HIL":  "Hardware Interface Layer",
    "EM":   "Environmental Monitor",
    "BL":   "Blade",
    "PORT": "Port Driver / Port Hardware",
    "HAM":  "HA Manager",
    "HAMK": "HAM Kernel",
    "CHS":  "Chassis",
    "PS":   "Power Supply / Performance Monitor",
    "FV":   "Flow Vision / FRU Validator",
    "BM":   "Blade Manager",
    "PMGR": "Port Manager",
    "PDM":  "Platform Data Manager",
    "SS":   "System Services / Support Save",
    "PSWP": "Port Swap",
    "PLAT": "Platform",
    "MAPS": "Monitoring and Alerting Policy Suite",
    "SEC":  "Security",
    "AUDIT": "Audit",
    "LOG":  "Logging",
    "FICON": "FICON Channel",
    "FCIP": "FCIP Tunnel",
    "IPADM": "IP Address Management",
    "RAS":  "RAS Subsystem",
    "ZONE": "Zoning",
    "FABRIC": "Fabric Services",
    "SNMP": "SNMP",
    "SYS":  "System",
    "SEC2": "Extended Security",
    "FSSM": "Firmware State Manager",
    "AUTH": "Authentication",
}

BRCD_MODULE_CATEGORY = {
    "HIL":  "hardware_event",
    "EM":   "environmental_event",
    "BL":   "blade_event",
    "PORT": "port_event",
    "HAM":  "ha_alert",
    "HAMK": "ha_alert",
    "CHS":  "chassis_event",
    "PS":   "performance_monitor",
    "FV":   "flow_vision",
    "BM":   "blade_manager",
    "PMGR": "port_manager",
    "HC":   "health_check",
    "PDM":  "pdm_event",
    "SS":   "support_save",
    "PSWP": "port_swap",
    "PLAT": "platform_event",
    "MAPS": "maps_violation",
    "SEC":  "security_event",
    "AUDIT": "audit_log",
    "LOG":  "log_event",
    "FICON": "ficon_event",
    "FCIP": "fcip_event",
    "IPADM": "ip_event",
    "RAS":  "ras_event",
    "ZONE": "zone_event",
    "FABRIC": "fabric_event",
    "SNMP": "snmp_event",
    "SYS":  "system_event",
    "SEC2": "security_event",
    "FSSM": "firmware_alert",
    "AUTH": "auth_failure",
}

SANNAV_MODULE_CATEGORY = {
    "SSMP-AUTH":    "auth_event",
    "SSMP-AUDIT":   "audit_log",
    "SSMP-USER":    "auth_event",
    "SSMP-LICENSE": "license_alert",
    "SSMP-PERF":    "sannav_performance",
    "SSMP":         "sannav_event",
    "NOS":          "sannav_event",
    "SVCNAVIGATOR": "sannav_event",
    "SHS":          "sannav_event",
    "SCC":          "sannav_event",
}


def lookup_sannav_category(module_full):
    if not module_full:
        return "sannav_event"
    parts = module_full.upper().split("-")
    for n in range(len(parts), 0, -1):
        prefix = "-".join(parts[:n])
        if prefix in SANNAV_MODULE_CATEGORY:
            return SANNAV_MODULE_CATEGORY[prefix]
    return "sannav_event"


BRCD_KEYWORD_PATTERNS = [
    ("ntp_alert",         re.compile(r"\bntp\b|\bsntp\b|time\s*sync(hroniz(ation|ed)?)?|clock\s*(drift|skew|sync)", re.I)),
    ("fips_alert",        re.compile(r"\bfips\b", re.I)),
    ("license_alert",     re.compile(r"\blicense\b|end\s*of\s*support", re.I)),
    ("audit_log",         re.compile(r"\baudit\b", re.I)),
    ("auth_failure",      re.compile(r"(auth(entication)?|login|ssh|console|password)\s*(fail|denied|error)", re.I)),
    ("wwn_alert",         re.compile(r"\bwwn\b|world[\s_-]*wide[\s_-]*name", re.I)),
    ("airflow_alert",     re.compile(r"air[\s_-]*flow|psu-?fan", re.I)),
    ("battery_alert",     re.compile(r"\bbattery\b", re.I)),
    ("fan_missing",       re.compile(r"\b(fan|blower)(s|\s*fru)?\b.*\b(missing|absent|remove(d)?)\b|missing\s*(fan|blower)", re.I)),
    ("fan_failure",       re.compile(
        r"\b(fan|blower)(s)?\b.*\b(fail(ed|s)?|fault(ed|y)?|stop(ped)?|below|above|out\s*of\s*service|not\s*supplying|threshold)\b|"
        r"\b\d+\s*(fan|blower)s?\s*(fail|fault|out\s*of\s*service)|"
        r"blower\s*\d+",
        re.I,
    )),
    ("temperature_alarm", re.compile(r"\b(temp(erature)?|overheat|thermal|cool(ing)?)\b", re.I)),
    ("voltage_alert",     re.compile(r"\bvoltage\b|\binput\s*voltage\b", re.I)),
    ("power_failure",     re.compile(r"\bpower\s*(fail|loss|down|fault|supply|consum)|\bpsu\b|\bps\d*\b|excessive\s*power|AC\s*(fail|loss|input)", re.I)),
    ("optic_alert",       re.compile(r"\bsfp\b|\bqsfp\b|\btsfp\b|\boptic(s|al)?\b|mini[\s_-]*sfp|\bmcu\b|\bdsp\b|\bserdes\b", re.I)),
    ("firmware_alert",    re.compile(r"\b(firmware|fpga|sysfpga|fabric\s*os|fos)\b|inconsistent\s*\S*\s*version|version\s*mismatch|cold\s*upgrade|firmwaredownload", re.I)),
    ("flow_vision",       re.compile(r"flow\s*vision|sys_flow_monitor|sys_mon_analytics|\bflow\b.*\b(activ|deactiv|reset)\b|\bsim\s*port\b", re.I)),
    ("config_change",     re.compile(r"\bconfig(uration)?\b.*\b(change|update|modif|set|reset|enabled|disabled)\b", re.I)),
    ("support_save",      re.compile(r"support\s*save|supportsave|ffdc", re.I)),
    ("ha_failover",       re.compile(r"\bfailover\b|warm\s*recovery|cold\s*recovery|hareboot", re.I)),
    ("ha_alert",          re.compile(r"high\s*availability|\bstandby\s*cp\b|\bactive\s*cp\b|heartbeat|hamk|\bha\b\s*(reboot|sync)", re.I)),
    ("port_fault",        re.compile(r"\bport\b.*\b(fail|fault|faulted|disable(d)?|offline|down)\b|link\s*(down|fail|lost|timeout)", re.I)),
    ("blade_fault",       re.compile(
        r"\bblade\b.*\b(fail|fault|faulted|disable(d)?|reset|offline|incompatible)\b|"
        r"\b(fault(ed|ing)?|disabl(e|ed|ing)?|reset(ting)?|offline|incompatib\w*|suppress\w*)\s+\w*\s*\bblade\b|"
        r"slot\s*\d+\s*(disable|faulted|powered\s*off)",
        re.I,
    )),
    ("chassis_alert",     re.compile(r"\bchassis\b.*\b(disable|fault|reboot|critical)\b", re.I)),
    ("fru_event",         re.compile(r"\bfru\b.*\b(insert|remove(d)?|access|inconsist|reinitial)", re.I)),
    ("env_warning",       re.compile(r"environmental\s*limits|environmental\s*specifications", re.I)),
]


BRCD_ALERT_SEVERITY: dict[str, str] = {
    "BL-1000": "INFO", "BL-1001": "INFO", "BL-1002": "CRITICAL",
    "BL-1003": "CRITICAL", "BL-1004": "CRITICAL", "BL-1006": "INFO",
    "BL-1007": "WARNING", "BL-1008": "CRITICAL", "BL-1009": "CRITICAL",
    "BL-1010": "WARNING", "BL-1011": "CRITICAL", "BL-1012": "ERROR",
    "BL-1013": "ERROR", "BL-1014": "ERROR", "BL-1015": "ERROR",
    "BL-1016": "CRITICAL", "BL-1017": "INFO", "BL-1018": "INFO",
    "BL-1019": "ERROR", "BL-1020": "CRITICAL", "BL-1021": "INFO",
    "BL-1022": "CRITICAL", "BL-1023": "CRITICAL", "BL-1024": "INFO",
    "BL-1025": "INFO", "BL-1026": "CRITICAL", "BL-1027": "CRITICAL",
    "BL-1028": "CRITICAL", "BL-1029": "INFO", "BL-1030": "INFO",
    "BL-1031": "CRITICAL", "BL-1032": "CRITICAL", "BL-1033": "CRITICAL",
    "BL-1034": "INFO", "BL-1035": "INFO", "BL-1036": "CRITICAL",
    "BL-1037": "CRITICAL", "BL-1038": "CRITICAL", "BL-1039": "CRITICAL",
    "BL-1040": "CRITICAL", "BL-1041": "CRITICAL", "BL-1045": "ERROR",
    "BL-1046": "ERROR", "BL-1047": "INFO", "BL-1048": "WARNING",
    "BL-1049": "INFO", "BL-1050": "WARNING", "BL-1052": "WARNING",
    "BL-1053": "WARNING", "BL-1054": "ERROR", "BL-1055": "ERROR",
    "BL-1056": "ERROR", "BL-1057": "CRITICAL", "BL-1058": "INFO",
    "BL-1061": "ERROR", "BL-1062": "ERROR", "BL-1063": "ERROR",
    "BL-1064": "ERROR", "BL-1065": "INFO", "BL-1066": "ERROR",
    "BL-1080": "ERROR", "BL-1081": "ERROR", "BL-1082": "ERROR",
    "BL-1083": "INFO", "BL-1084": "WARNING", "BL-1085": "WARNING",
    "BL-1086": "WARNING", "BL-1087": "CRITICAL", "BL-1088": "CRITICAL",
    "BL-1089": "ERROR",

    "BM-1001": "ERROR", "BM-1002": "INFO", "BM-1003": "WARNING",
    "BM-1004": "INFO", "BM-1005": "WARNING", "BM-1006": "INFO",
    "BM-1007": "INFO", "BM-1008": "WARNING", "BM-1009": "WARNING",
    "BM-1010": "INFO", "BM-1053": "WARNING", "BM-1054": "INFO",
    "BM-1055": "WARNING", "BM-1056": "INFO", "BM-1058": "WARNING",

    "CHS-1002": "ERROR", "CHS-1003": "ERROR", "CHS-1004": "ERROR",
    "CHS-1005": "ERROR", "CHS-1006": "INFO", "CHS-1007": "INFO",
    "CHS-1008": "INFO",

    "EM-1001": "CRITICAL", "EM-1002": "INFO", "EM-1003": "CRITICAL",
    "EM-1004": "CRITICAL", "EM-1005": "CRITICAL", "EM-1006": "CRITICAL",
    "EM-1008": "CRITICAL", "EM-1009": "CRITICAL", "EM-1010": "CRITICAL",
    "EM-1011": "CRITICAL", "EM-1012": "CRITICAL", "EM-1013": "ERROR",
    "EM-1014": "ERROR", "EM-1015": "WARNING", "EM-1016": "WARNING",
    "EM-1017": "WARNING", "EM-1018": "ERROR", "EM-1019": "WARNING",
    "EM-1020": "ERROR", "EM-1028": "ERROR", "EM-1029": "ERROR",
    "EM-1031": "ERROR", "EM-1033": "ERROR", "EM-1034": "ERROR",
    "EM-1035": "ERROR", "EM-1036": "WARNING", "EM-1037": "INFO",
    "EM-1042": "WARNING", "EM-1043": "WARNING", "EM-1044": "WARNING",
    "EM-1045": "WARNING", "EM-1046": "ERROR", "EM-1047": "ERROR",
    "EM-1048": "INFO", "EM-1049": "INFO", "EM-1050": "INFO",
    "EM-1051": "INFO", "EM-1057": "WARNING", "EM-1058": "WARNING",
    "EM-1059": "ERROR", "EM-1060": "WARNING", "EM-1061": "WARNING",
    "EM-1062": "CRITICAL", "EM-1063": "CRITICAL", "EM-1064": "ERROR",
    "EM-1065": "WARNING", "EM-1066": "INFO", "EM-1067": "WARNING",
    "EM-1068": "ERROR", "EM-1069": "INFO", "EM-1070": "INFO",
    "EM-1071": "CRITICAL", "EM-1072": "CRITICAL", "EM-1073": "CRITICAL",
    "EM-1074": "CRITICAL", "EM-1075": "WARNING", "EM-1076": "WARNING",
    "EM-1100": "CRITICAL", "EM-1101": "CRITICAL", "EM-1134": "ERROR",
    "EM-1220": "ERROR", "EM-1221": "INFO", "EM-1222": "WARNING",
    "EM-1223": "INFO", "EM-1224": "INFO", "EM-1225": "INFO",
    "EM-1226": "INFO", "EM-1227": "ERROR", "EM-1228": "CRITICAL",
    "EM-1229": "CRITICAL", "EM-1230": "CRITICAL", "EM-1235": "CRITICAL",
    "EM-1240": "WARNING", "EM-1241": "WARNING", "EM-2003": "ERROR",
    "EM-2004": "WARNING",

    "FV-1001": "INFO", "FV-1002": "INFO", "FV-1003": "INFO",
    "FV-1004": "INFO", "FV-1007": "INFO", "FV-1008": "WARNING",
    "FV-1009": "ERROR", "FV-1010": "INFO", "FV-1011": "INFO",
    "FV-1012": "INFO", "FV-1013": "INFO",
    "FV-3000": "INFO", "FV-3001": "INFO", "FV-3002": "INFO",
    "FV-3003": "INFO", "FV-3004": "INFO", "FV-3005": "INFO",
    "FV-3006": "INFO", "FV-3007": "INFO", "FV-3008": "INFO",
    "FV-3009": "INFO", "FV-3010": "INFO", "FV-3011": "INFO",
    "FV-3012": "INFO", "FV-3013": "INFO", "FV-3014": "INFO",
    "FV-3015": "INFO", "FV-3016": "INFO", "FV-3017": "INFO",
    "FV-3018": "INFO", "FV-3019": "INFO", "FV-3020": "INFO",
    "FV-3021": "INFO", "FV-3022": "INFO", "FV-3023": "INFO",
    "FV-3024": "WARNING", "FV-3025": "INFO", "FV-3026": "INFO",
    "FV-3027": "INFO",

    "HAM-1001": "CRITICAL", "HAM-1002": "INFO", "HAM-1004": "INFO",
    "HAM-1005": "INFO", "HAM-1006": "CRITICAL", "HAM-1007": "CRITICAL",
    "HAM-1008": "CRITICAL", "HAM-1009": "CRITICAL", "HAM-1010": "CRITICAL",
    "HAM-1011": "CRITICAL", "HAM-1013": "ERROR", "HAM-1014": "ERROR",
    "HAM-1015": "INFO", "HAM-1016": "CRITICAL", "HAM-1017": "INFO",
    "HAM-1018": "INFO", "HAM-1019": "CRITICAL", "HAM-1020": "WARNING",
    "HAM-1021": "CRITICAL", "HAM-1022": "ERROR", "HAM-1023": "INFO",
    "HAM-1024": "ERROR", "HAM-1025": "CRITICAL",

    "HAMK-1001": "CRITICAL", "HAMK-1002": "INFO", "HAMK-1003": "INFO",
    "HAMK-1004": "INFO",

    "HIL-1101": "ERROR", "HIL-1102": "ERROR", "HIL-1103": "ERROR",
    "HIL-1104": "ERROR", "HIL-1105": "ERROR", "HIL-1106": "ERROR",
    "HIL-1107": "CRITICAL", "HIL-1108": "CRITICAL", "HIL-1109": "ERROR",
    "HIL-1201": "WARNING", "HIL-1202": "ERROR", "HIL-1203": "ERROR",
    "HIL-1204": "ERROR", "HIL-1206": "ERROR", "HIL-1207": "ERROR",
    "HIL-1208": "INFO",
    "HIL-1301": "WARNING", "HIL-1302": "WARNING", "HIL-1303": "ERROR",
    "HIL-1304": "ERROR", "HIL-1305": "ERROR", "HIL-1306": "ERROR",
    "HIL-1307": "ERROR", "HIL-1308": "ERROR", "HIL-1309": "ERROR",
    "HIL-1310": "WARNING", "HIL-1311": "INFO", "HIL-1312": "WARNING",
    "HIL-1401": "WARNING", "HIL-1402": "WARNING", "HIL-1403": "WARNING",
    "HIL-1404": "WARNING", "HIL-1405": "WARNING",
    "HIL-1501": "WARNING", "HIL-1502": "CRITICAL", "HIL-1503": "CRITICAL",
    "HIL-1504": "INFO", "HIL-1505": "WARNING", "HIL-1506": "CRITICAL",
    "HIL-1507": "WARNING", "HIL-1508": "WARNING", "HIL-1509": "WARNING",
    "HIL-1510": "WARNING", "HIL-1511": "WARNING", "HIL-1512": "WARNING",
    "HIL-1513": "CRITICAL", "HIL-1514": "CRITICAL", "HIL-1515": "CRITICAL",
    "HIL-1516": "WARNING", "HIL-1517": "WARNING", "HIL-1518": "CRITICAL",
    "HIL-1519": "WARNING",
    "HIL-1601": "ERROR", "HIL-1602": "CRITICAL", "HIL-1603": "CRITICAL",
    "HIL-1605": "INFO", "HIL-1610": "WARNING", "HIL-1611": "CRITICAL",
    "HIL-1612": "CRITICAL", "HIL-1613": "INFO", "HIL-1614": "WARNING",
    "HIL-1615": "WARNING", "HIL-1621": "WARNING", "HIL-1623": "INFO",
    "HIL-1624": "WARNING", "HIL-1625": "WARNING", "HIL-1626": "WARNING",
    "HIL-1627": "WARNING", "HIL-1628": "WARNING", "HIL-1629": "WARNING",
    "HIL-1630": "INFO", "HIL-1650": "ERROR", "HIL-1651": "ERROR",
    "HIL-1652": "WARNING", "HIL-1653": "WARNING", "HIL-1654": "CRITICAL",
    "HIL-1655": "WARNING", "HIL-1656": "ERROR", "HIL-1657": "INFO",
    "HIL-1658": "WARNING", "HIL-1659": "ERROR", "HIL-1660": "CRITICAL",

    "PDM-1001": "WARNING", "PDM-1002": "WARNING", "PDM-1003": "WARNING",
    "PDM-1004": "WARNING", "PDM-1005": "WARNING", "PDM-1006": "WARNING",
    "PDM-1007": "WARNING", "PDM-1008": "WARNING", "PDM-1009": "WARNING",
    "PDM-1010": "WARNING", "PDM-1011": "WARNING", "PDM-1012": "WARNING",
    "PDM-1013": "WARNING", "PDM-1014": "WARNING", "PDM-1017": "ERROR",
    "PDM-1019": "WARNING", "PDM-1020": "WARNING", "PDM-1021": "WARNING",
    "PDM-1022": "WARNING", "PDM-1023": "WARNING", "PDM-1024": "WARNING",
    "PDM-1025": "WARNING", "PDM-1026": "WARNING", "PDM-1027": "WARNING",
    "PDM-1028": "WARNING",

    "PLAT-1000": "ERROR", "PLAT-1001": "INFO", "PLAT-1002": "ERROR",
    "PLAT-1003": "INFO", "PLAT-1004": "CRITICAL", "PLAT-1005": "ERROR",
    "PLAT-1006": "WARNING", "PLAT-1007": "WARNING", "PLAT-1008": "WARNING",
    "PLAT-1009": "WARNING", "PLAT-1010": "CRITICAL", "PLAT-1011": "CRITICAL",
    "PLAT-1072": "CRITICAL", "PLAT-1073": "CRITICAL", "PLAT-1100": "WARNING",
    "PLAT-2000": "INFO", "PLAT-2001": "INFO",

    "PMGR-1001": "INFO", "PMGR-1002": "ERROR", "PMGR-1003": "INFO",
    "PMGR-1004": "ERROR", "PMGR-1005": "INFO", "PMGR-1006": "ERROR",
    "PMGR-1007": "INFO", "PMGR-1008": "ERROR", "PMGR-1009": "INFO",
    "PMGR-1010": "ERROR", "PMGR-1011": "INFO", "PMGR-1012": "INFO",
    "PMGR-1013": "INFO", "PMGR-1014": "ERROR",

    "PORT-1003": "WARNING", "PORT-1004": "INFO", "PORT-1005": "WARNING",
    "PORT-1006": "INFO", "PORT-1007": "INFO", "PORT-1008": "INFO",
    "PORT-1009": "INFO", "PORT-1010": "INFO", "PORT-1011": "WARNING",
    "PORT-1012": "INFO", "PORT-1013": "INFO", "PORT-1014": "INFO",
    "PORT-1015": "INFO", "PORT-1016": "INFO", "PORT-1017": "INFO",
    "PORT-1018": "INFO", "PORT-1019": "INFO", "PORT-1020": "INFO",
    "PORT-1021": "INFO", "PORT-1022": "INFO", "PORT-1023": "INFO",
    "PORT-1024": "INFO", "PORT-1025": "INFO", "PORT-1026": "INFO",
    "PORT-1027": "INFO", "PORT-1028": "INFO", "PORT-1029": "INFO",
    "PORT-1030": "INFO", "PORT-1031": "INFO", "PORT-1032": "INFO",
    "PORT-1033": "INFO", "PORT-1034": "INFO", "PORT-1035": "INFO",
    "PORT-1036": "INFO", "PORT-1037": "INFO", "PORT-1038": "INFO",
    "PORT-1039": "INFO", "PORT-1040": "INFO", "PORT-1041": "INFO",
    "PORT-1042": "INFO", "PORT-1043": "INFO", "PORT-1044": "INFO",
    "PORT-1045": "INFO", "PORT-1046": "INFO", "PORT-1047": "INFO",
    "PORT-1048": "INFO", "PORT-1049": "INFO", "PORT-1050": "INFO",
    "PORT-1051": "INFO", "PORT-1052": "INFO", "PORT-1053": "INFO",
    "PORT-1054": "INFO", "PORT-1055": "INFO", "PORT-1056": "INFO",
    "PORT-1057": "INFO", "PORT-1058": "INFO", "PORT-1059": "INFO",
    "PORT-1060": "INFO", "PORT-1061": "INFO", "PORT-1062": "INFO",
    "PORT-1063": "INFO", "PORT-1064": "INFO", "PORT-1065": "INFO",
    "PORT-1066": "INFO", "PORT-1067": "INFO", "PORT-1068": "INFO",
    "PORT-1069": "INFO", "PORT-1070": "INFO", "PORT-1071": "INFO",
    "PORT-1072": "INFO", "PORT-1073": "INFO", "PORT-1074": "INFO",
    "PORT-1075": "INFO", "PORT-1076": "INFO", "PORT-1077": "INFO",
    "PORT-1078": "INFO", "PORT-1079": "INFO", "PORT-1080": "INFO",
    "PORT-1081": "INFO", "PORT-1082": "INFO",

    "PS-1000": "CRITICAL", "PS-1001": "INFO", "PS-1002": "INFO",
    "PS-1009": "WARNING",

    "PSWP-1001": "INFO", "PSWP-1002": "INFO", "PSWP-1003": "INFO",
    "PSWP-1004": "INFO", "PSWP-1005": "ERROR", "PSWP-1006": "ERROR",
    "PSWP-1007": "ERROR", "PSWP-1008": "INFO",

    "SS-1000": "INFO", "SS-1001": "WARNING", "SS-1002": "INFO",
    "SS-1003": "WARNING", "SS-1004": "WARNING", "SS-1005": "WARNING",
    "SS-1006": "WARNING", "SS-1007": "WARNING", "SS-1008": "WARNING",
    "SS-1009": "WARNING", "SS-1010": "INFO", "SS-1011": "INFO",
    "SS-1012": "INFO", "SS-1013": "INFO", "SS-1014": "INFO",
    "SS-1015": "INFO", "SS-1016": "INFO",

    # MAPS monitoring alerts (Alarms / Events / Violations)
    "MAPS-1001": "CRITICAL", "MAPS-1002": "WARNING",
    "MAPS-1003": "WARNING", "MAPS-1004": "INFO",

    # Security audit
    "SEC-3020": "INFO", "SEC-3022": "WARNING",
    "SEC-3023": "WARNING", "SEC-3024": "WARNING",
}


SEVERITY_FIELD_MAP = {
    "info":          "informational",
    "informational": "informational",
    "warning":       "warning",
    "warn":          "warning",
    "error":         "error",
    "err":           "error",
    "critical":      "critical",
    "crit":          "critical",
    "notice":        "notice",
    "alert":         "alert",
    "emergency":     "emergency",
    "emerg":         "emergency",
    "debug":         "debug",
}


ALL_BRCD_CATEGORIES = (
    set(BRCD_MODULE_CATEGORY.values())
    | set(SANNAV_MODULE_CATEGORY.values())
    | {cat for cat, _ in BRCD_KEYWORD_PATTERNS}
    | {"sannav_event", "sannav_performance", "other"}
)

# Categories that represent physical hardware faults. These go to the
# main alert bucket (dashboard). Everything else goes to the report bucket.
HARDWARE_CATEGORIES = {
    "hardware_event", "environmental_event", "blade_event",
    "chassis_event", "fan_failure", "fan_missing",
    "temperature_alarm", "voltage_alert", "power_failure",
    "optic_alert", "battery_alert", "blade_fault",
    "chassis_alert", "fru_event", "env_warning",
    "airflow_alert", "ntp_alert", "fips_alert", 
     "wwn_alert", "firmware_alert", "flow_vision", "ha_alert", "port_fault",
}
def classify_brcd_category(module, message_text):
    text = message_text or ""
    for cat, pattern in BRCD_KEYWORD_PATTERNS:
        if pattern.search(text):
            return cat
    return BRCD_MODULE_CATEGORY.get((module or "").upper(), "other")


# ---------------------------------------------------------------------------
# Alert ID parsing.
# Matches both FOS (HAM-1004) and SANnav-style (SSMP-AUTH-1025) msgids.
# ---------------------------------------------------------------------------
ALERT_ID_RE = re.compile(
    r"^(?P<full>(?P<module_first>[A-Z][A-Z0-9]*)(?:-[A-Z][A-Z0-9]*)*-(?P<code>\d+))$"
)

BRCD_CODE_RE = re.compile(
    r"\[(?P<module>[A-Z][A-Z0-9]*)-(?P<code>\d+)\]",
)


def split_alert_id(alert_id):
    if not alert_id:
        return None, None, None
    m = ALERT_ID_RE.match(alert_id.strip().upper())
    if not m:
        return None, None, None
    full = m.group("full")
    parts = full.split("-")
    return m.group("module_first"), "-".join(parts[:-1]), m.group("code")


def parse_alert_from_sd(sd, msg_text):
    if not sd:
        return None
    msgid_raw = (sd.get("msgid") or "").strip()
    sev_raw   = (sd.get("severity") or "").strip().lower()
    swname    = (sd.get("swname") or "").strip()
    if not msgid_raw:
        return None

    module_first, module_full, code = split_alert_id(msgid_raw)
    if module_first is None:
        module_first = msgid_raw.upper()
        module_full  = msgid_raw.upper()
        code         = ""
        full         = msgid_raw.upper()
    else:
        full = msgid_raw.upper()

    if module_first in BRCD_MODULE_CATEGORY:
        vendor   = "brocade"
        category = classify_brcd_category(module_first, msg_text)
    else:
        vendor   = "sannav"
        category = lookup_sannav_category(module_full)

    catalog_sev = (BRCD_ALERT_SEVERITY.get(full, "") or "").lower()
    chosen_sev  = sev_raw or catalog_sev
    mapped_sev  = SEVERITY_FIELD_MAP.get(chosen_sev, "informational")

    return {
        "vendor":           vendor,
        "alert_id":         full,
        "brcd_module":      module_full or module_first,
        "brcd_module_desc": BRCD_MODULE_LABEL.get(module_first, "SANnav / Other"),
        "brcd_code":        code,
        "fos_seq":          "",
        "fos_attr":         "",
        "fos_timestamp":    sd.get("timestamp", ""),
        "fos_severity":     sev_raw,
        "fos_message":      msg_text,
        "fos_switch_name":  swname,
        "trap_category":    category,
        "mapped_severity":  mapped_sev,
        "username":         sd.get("username", ""),
    }


# ---------------------------------------------------------------------------
# Brocade RASLog fallback parser (used if SANnav ever forwards raw RASLog).
# ---------------------------------------------------------------------------
BRCD_FULL_RE = re.compile(
    r"(?:(?P<fos_ts>\d{4}/\d{1,2}/\d{1,2}[-T ]\d{1,2}:\d{2}:\d{2})\s*,\s*)?"
    r"\[(?P<module>[A-Z][A-Z0-9]*)-(?P<code>\d+)\]\s*,\s*"
    r"(?P<seq>\d+)\s*,\s*"
    r"(?P<attr>[^,]+?)\s*,\s*"
    r"(?P<fos_severity>INFO|WARNING|ERROR|CRITICAL)\s*,\s*"
    r"(?P<switch_name>[^,]+?)\s*,\s*"
    r"(?P<fos_message>.+?)\s*$",
    re.I,
)


def parse_brcd_raslog(raw_text):
    if not raw_text:
        return None

    full_m = BRCD_FULL_RE.search(raw_text)
    if full_m:
        module = full_m.group("module").upper()
        code = full_m.group("code")
        alert_id = f"{module}-{code}"
        fos_sev_raw = (full_m.group("fos_severity") or "").strip().lower()
        fos_message = (full_m.group("fos_message") or "").strip()
        switch_name = (full_m.group("switch_name") or "").strip()
        seq = (full_m.group("seq") or "").strip()
        attr = (full_m.group("attr") or "").strip()
        fos_ts = (full_m.group("fos_ts") or "").strip()
    else:
        m = BRCD_CODE_RE.search(raw_text)
        if not m:
            return None
        module = m.group("module").upper()
        code = m.group("code")
        alert_id = f"{module}-{code}"
        fos_sev_raw = ""
        fos_message = raw_text.strip()
        switch_name = ""
        seq = ""
        attr = ""
        fos_ts = ""

    catalog_sev = BRCD_ALERT_SEVERITY.get(alert_id, "").lower()
    chosen_sev = fos_sev_raw or catalog_sev
    mapped_sev = SEVERITY_FIELD_MAP.get(chosen_sev, "informational")
    trap_category = classify_brcd_category(module, fos_message or raw_text)

    return {
        "vendor":           "brocade",
        "alert_id":         alert_id,
        "brcd_module":      module,
        "brcd_module_desc": BRCD_MODULE_LABEL.get(module, "Unknown"),
        "brcd_code":        code,
        "fos_seq":          seq,
        "fos_attr":         attr,
        "fos_timestamp":    fos_ts,
        "fos_severity":     fos_sev_raw or catalog_sev,
        "fos_message":      fos_message,
        "fos_switch_name":  switch_name,
        "trap_category":    trap_category,
        "mapped_severity":  mapped_sev,
        "username":         "",
    }


# ---------------------------------------------------------------------------
# MASTER PARSER
# ---------------------------------------------------------------------------

def parse_syslog(raw, source_ip):
    global _msg_count
    _msg_count += 1
    try:
        text = raw.decode("utf-8", errors="replace").strip()
    except Exception:
        return None

    if not text:
        return None

    text = strip_bom(text)
    fields = {"raw_message": text}
    sd = {}

    m = RFC5424_RE.match(text)
    if m:
        gd = m.groupdict()
        pri = int(gd["pri"])
        fields.update(decode_priority(pri))
        msg_text = strip_bom(gd.get("message", "") or "")
        fields.update({
            "syslog_version":  gd.get("version", ""),
            "timestamp_str":   gd.get("timestamp", ""),
            "hostname":        gd.get("hostname", ""),
            "app_name":        gd.get("appname", ""),
            "proc_id":         gd.get("procid", ""),
            "msg_id":          gd.get("msgid", ""),
            "structured_data": gd.get("structured_data", ""),
            "message":         msg_text,
            "syslog_format":   "RFC5424",
            "priority":        pri,
        })
        sd = parse_sannav_sd(gd.get("structured_data", ""))
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
                "message":       strip_bom(gd.get("message", "") or ""),
                "syslog_format": "RFC3164",
                "priority":      pri,
            })
        else:
            fields.update({
                "message":       text,
                "syslog_format": "UNKNOWN",
            })

    raw_text = fields.get("raw_message", "") or ""
    msg_text = fields.get("message", "") or ""
    alert = parse_alert_from_sd(sd, msg_text or raw_text)
    if not alert:
        alert = parse_brcd_raslog(msg_text) or parse_brcd_raslog(raw_text)

    if alert:
        fields["severity"]         = alert["mapped_severity"]
        fields["vendor"]           = alert["vendor"]
        fields["alert_id"]         = alert["alert_id"]
        fields["brcd_module"]      = alert["brcd_module"]
        fields["brcd_module_desc"] = alert["brcd_module_desc"]
        fields["brcd_code"]        = alert["brcd_code"]
        fields["fos_seq"]          = alert["fos_seq"]
        fields["fos_attr"]         = alert["fos_attr"]
        fields["fos_timestamp"]    = alert["fos_timestamp"]
        fields["fos_severity"]     = alert["fos_severity"]
        fields["fos_message"]      = alert["fos_message"]
        fields["fos_switch_name"]  = alert["fos_switch_name"]
        fields["trap_category"]    = alert["trap_category"]
        fields["username"]         = alert.get("username", "") or ""
        if not fields.get("message") and alert.get("fos_message"):
            fields["message"] = alert["fos_message"]
        fields["error_message"]    = alert.get("fos_message") or msg_text or raw_text
        for cat in ALL_BRCD_CATEGORIES:
            fields[f"trap_{cat}"] = (cat == alert["trap_category"])
    else:
        fields["vendor"]        = fields.get("vendor", "generic")
        fields["trap_category"] = "none"
        fields["error_message"] = msg_text or raw_text
        for cat in ALL_BRCD_CATEGORIES:
            fields.setdefault(f"trap_{cat}", False)

    swname_in = (sd.get("swname") if sd else "") or (alert.get("fos_switch_name") if alert else "") or ""
    name, env, owner, model = resolve_switch_meta(swname_in, source_ip)
    fields["environment"] = env
    fields["switch_name"] = name
    fields["owner"]       = owner
    fields["model"]       = model
    if name and name != "unknown":
        fields["hostname"] = name

    if sd:
        for k, v in sd.items():
            if v and f"sd_{k}" not in fields:
                fields[f"sd_{k}"] = v

    return fields


# ---------------------------------------------------------------------------
# INFLUXDB WRITER
# ---------------------------------------------------------------------------

class InfluxWriter:
    def __init__(self, url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, bucket=INFLUX_BUCKET):
        self.bucket = bucket
        self.client = InfluxDBClient(
            url=url, token=token, org=org, verify_ssl=False
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
                INFLUX_URL, bucket, WRITE_BATCH_SIZE, WRITE_FLUSH_MS,
            )
        else:
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            log.info(
                "InfluxDB client initialised -> %s (bucket=%s) [SYNCHRONOUS]",
                url, bucket,
            )

    def write(self, measurement, source_ip, fields):
        point = (
            Point(measurement)
            .tag("source_ip",     source_ip)
            .tag("location_pair", LOCATION_PAIR)
            .tag("environment",   fields.get("environment", LOCATION_PAIR))
            .tag("syslog_format", fields.get("syslog_format", "UNKNOWN"))
            .tag("severity",      fields.get("severity", "unknown"))
            .tag("facility",      fields.get("facility", "unknown"))
            .tag("vendor",        fields.get("vendor", "unknown"))
            .tag("trap_category", fields.get("trap_category", "none"))
            .tag("switch_name",   fields.get("switch_name", "unknown"))
            .tag("owner",         fields.get("owner", "unknown"))
            .tag("model",         fields.get("model", "unknown"))
            .tag("brcd_module",   fields.get("brcd_module", "unknown"))
            .tag("alert_id",      fields.get("alert_id", "unknown"))
            .time(datetime.now(timezone.utc), WritePrecision.NS)
        )

        str_fields = [
            "hostname", "app_name", "proc_id", "msg_id", "process",
            "pid", "message", "raw_message", "structured_data",
            "timestamp_str",
            "alert_id", "brcd_module", "brcd_module_desc", "brcd_code",
            "fos_seq", "fos_attr", "fos_timestamp", "fos_severity",
            "fos_message", "fos_switch_name",
            "username", "error_message",
        ]
        for key in str_fields:
            val = fields.get(key)
            if val is not None and val != "":
                point = point.field(key, str(val))

        for key, val in fields.items():
            if key.startswith("sd_") and val is not None and val != "":
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
            self.write_api.write(bucket=self.bucket, org=INFLUX_ORG, record=point)
        except Exception as exc:
            log.error("InfluxDB write to %s failed: %s", self.bucket, exc)

    def close(self):
        self.client.close()


# ---------------------------------------------------------------------------
# UDP LISTENER
# ---------------------------------------------------------------------------

class UDPSyslogListener(threading.Thread):
    def __init__(self, writer, report_writer, pool):
        super().__init__(daemon=True, name="UDPSyslogListener")
        self.writer = writer
        self.report_writer = report_writer
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
        try:
            raw_str = data.decode("utf-8", errors="replace").strip()
            if raw_str:
                raw_log.info(f"[{source_ip}] {raw_str}")
        except Exception:
            pass

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
            "[UDP] %s (%s/%s/%s) -> [%s] sev=%s id=%s cat=%s | %s",
            source_ip, fields.get("environment", LOCATION_PAIR),
            fields.get("switch_name", "unknown"),
            fields.get("owner", "unknown"),
            measurement,
            fields.get("severity", "?"),
            fields.get("alert_id", "?"),
            fields.get("trap_category", "?"),
            (fields.get("fos_message") or fields.get("message", "") or "")[:120],
        )

        vendor = fields.get("vendor")
        category = fields.get("trap_category", "")

        if vendor == "brocade" and category in HARDWARE_CATEGORIES:
            # Physical switch hardware faults → main alert bucket
            self.writer.write(measurement, source_ip, fields)
        if self.report_writer:
            # Everything else (MAPS, events, security, SANnav, health checks) → report bucket
            self.report_writer.write(measurement, source_ip, fields)


# ---------------------------------------------------------------------------
# TCP LISTENER
# ---------------------------------------------------------------------------

class TCPSyslogListener(threading.Thread):
    def __init__(self, writer, report_writer, pool):
        super().__init__(daemon=True, name="TCPSyslogListener")
        self.influx_writer = writer
        self.report_writer = report_writer
        self.pool = pool

    def run(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((LISTEN_HOST, LISTEN_PORT + 1))
        srv.listen(50)
        log.info("TCP syslog listener started on %s:%d", LISTEN_HOST, LISTEN_PORT + 1)

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
                    try:
                        raw_str = line.decode("utf-8", errors="replace").strip()
                        if raw_str:
                            raw_log.info(f"[{source_ip}] {raw_str}")
                    except Exception:
                        pass
                    line, effective_ip, spoofed = apply_test_mode(line, source_ip)
                    line_measurement = measurement or classify_source(effective_ip)
                    if line_measurement is None:
                        continue
                    fields = parse_syslog(line, effective_ip)
                    if not fields:
                        continue
                    log.info(
                        "[TCP] %s (%s/%s/%s)%s -> [%s] sev=%s id=%s cat=%s | %s",
                        effective_ip,
                        fields.get("environment", LOCATION_PAIR),
                        fields.get("switch_name", "unknown"),
                        fields.get("owner", "unknown"),
                        " (spoofed)" if spoofed else "",
                        line_measurement,
                        fields.get("severity", "?"),
                        fields.get("alert_id", "?"),
                        fields.get("trap_category", "?"),
                        (fields.get("fos_message") or fields.get("message", "") or "")[:120],
                    )
                    vendor = fields.get("vendor")
                    category = fields.get("trap_category", "")
                    if vendor == "brocade" and category in HARDWARE_CATEGORIES:
                        self.influx_writer.write(line_measurement, effective_ip, fields)
                    if self.report_writer:
                        self.report_writer.write(line_measurement, effective_ip, fields)
        except Exception as exc:
            log.error("TCP client error (%s): %s", source_ip, exc)
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------

def main():
    cdvl_count = sum(1 for v in SWITCH_INFO.values() if v[0] == "CDVL")
    sify_count = sum(1 for v in SWITCH_INFO.values() if v[0] == "SIFY")
    other      = len(SWITCH_INFO) - cdvl_count - sify_count

    log.info("=" * 70)
    log.info(" Syslog Trap Listener (%s, Brocade FOS via SANnav) - starting up", LOCATION_PAIR)
    log.info(" Influx URL          : %s", INFLUX_URL)
    log.info(" Influx bucket       : %s", INFLUX_BUCKET)
    log.info(" Listen port         : %d (UDP) / %d (TCP)", LISTEN_PORT, LISTEN_PORT + 1)
    log.info(" Measurement         : san_switch")
    log.info(" SANnav sources      : %s", ", ".join(SANNAV_SOURCES) or "(none configured)")
    log.info(" Switch inventory    : %d total (CDVL=%d, SIFY=%d, other=%d)",
             len(SWITCH_INFO), cdvl_count, sify_count, other)
    log.info(" IP fallback entries : %d", len(SWITCH_IP_TO_NAME))
    log.info(" Brocade catalog     : %d alerts", len(BRCD_ALERT_SEVERITY))
    log.info("=" * 70)

    if not SANNAV_SOURCES:
        log.warning("SANNAV_SOURCES is empty - every incoming packet will be dropped.")

    writer = InfluxWriter()
    report_writer = None
    if INFLUX_REPORT_URL and INFLUX_REPORT_TOKEN:
        report_writer = InfluxWriter(
            url=INFLUX_REPORT_URL,
            token=INFLUX_REPORT_TOKEN,
            org=INFLUX_ORG,
            bucket=INFLUX_BUCKET_REPORT
        )

    _start_heartbeat()

    pool = ThreadPoolExecutor(
        max_workers=WORKER_THREADS,
        thread_name_prefix="syslog-worker",
    )

    udp = UDPSyslogListener(writer, report_writer, pool)
    tcp = TCPSyslogListener(writer, report_writer, pool)

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
