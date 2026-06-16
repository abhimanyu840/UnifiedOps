#!/usr/bin/env python3
"""
=============================================================================
  UnifiedOps - Dev syslog-trap sender (no storage hardware required)
=============================================================================

Crafts realistic Hitachi VSP GUM/SVP syslog packets and sends them over
UDP to a running listener. Sole purpose: lets a developer exercise the
listener -> InfluxDB -> FastAPI -> React path on a laptop without any
actual storage array.

Reference codes (RefCode) and severities are taken from
"SIM Reference Guide for VSP E Series and VSP G130, G/F350, G/F370,
G/F700, G/F900" (MK-97HM85023-09, Dec 2020).

The packet shape matches HITACHI_SVP_RE in
listener/syslog_trap_listener_<loc>.py:

    <PRI>1 2026-05-23T12:00:00Z host hitachi-trap - - - 100001, Acute,
        VSP_G350_485887-CDVL, Hitachi_syslog, RefCode: 628000,
        DP Protect attribute setting of DRU

and is prefixed with `[SOURCE_IP=10.x.x.x]` so the listener's
TEST_MODE strips it off and treats the packet as if it had arrived
from that storage IP.

IMPORTANT: the target listener MUST have HITRACK_TEST_MODE=1 in its
env file. Otherwise packets from 127.0.0.1 get dropped (since loopback
is not in any IP_FILTER).

Usage
-----
    # Single packet, default catalog entry
    python trap_sender.py

    # Pick a specific reference code + severity
    python trap_sender.py --location CDVL --refcode 628000 --severity Serious

    # Random IP from the location + random catalog refcode
    python trap_sender.py --location BCP --random

    # Burst: 20 packets, 0.5 s apart, random refcodes
    python trap_sender.py --location SIFY --random --count 20 --interval 0.5

    # Point at a different listener port
    python trap_sender.py --target 127.0.0.1:5514

    # Custom text + custom refcode
    python trap_sender.py --refcode A99999 --text "Custom test alert"

    # Show the catalog
    python trap_sender.py --list-refcodes
"""
from __future__ import annotations

import argparse
import datetime as dt
import random
import socket
import sys
import time
from typing import Dict, List, Tuple


# ---------------------------------------------------------------------------
# Catalogue of realistic SIM reference codes per the SIM Reference Guide.
# (severity, ref_code, message). Every refcode below is in the
# HITACHI_REFCODE_MAP / HITACHI_REFCODE_PREFIX*_MAP in the listener, so
# `parse_hitachi_svp()` resolves a real `trap_category` for it (instead
# of falling back to "other").
#
# Severities (Acute/Serious/Moderate/Service/Info) are taken from the SIM
# Reference Guide for VSP E Series and VSP G/F 130/350/370/700/900
# (MK-97HM85023-09) and follow the Hitachi LED colour convention:
#   Acute    = red    LED, immediate action required
#   Serious  = red    LED, scheduled service required
#   Moderate = yellow LED, monitor closely
#   Service  = green  LED, informational service event
#   Info     = no LED, purely informational
# ---------------------------------------------------------------------------
SIM_CATALOGUE: List[Tuple[str, str, str]] = [
    # ---- User-troubleshooting codes (SIM Reference Guide, sec. 2) ------
    ("Moderate", "602001", "Pool blocking"),
    ("Moderate", "603000", "SM Space Warning"),
    ("Moderate", "604001", "Exceeded Threshold of actual pool use rate"),
    ("Moderate", "605001", "Actual pool use rate reaches upper limit"),
    ("Moderate", "624000", "SM Full"),
    ("Serious",  "627001", "The DP POOL LDEV blockade"),
    ("Serious",  "628000", "DP Protect attribute setting of DRU"),
    ("Moderate", "629001", "Exceeded Warning Threshold of DP pool use rate"),
    ("Moderate", "62B000", "Threshold of DP pool use rate remains exceeded"),
    ("Moderate", "62C001", "Exceeded Depletion Threshold of DP pool use rate"),
    ("Moderate", "62D001", "Exceeded Fixed outage Threshold of DP pool use rate"),
    ("Moderate", "660100", "No free encryption key"),
    ("Service",  "660200", "Remaining free encryption key warning"),
    ("Moderate", "670000", "Warning for depletion of cache management devices"),
    ("Moderate", "7D0301", "GUM AuditLog lost (CTL1)"),
    ("Moderate", "7D0401", "GUM AuditLog Warning Threshold was exceeded (CTL1)"),
    ("Service",  "EE0000", "Volume I/O upper limit reached"),
    ("Service",  "EE2000", "Volume I/O response delay"),

    # ---- Service-personnel codes (SIM Reference Guide, sec. 3) ---------
    ("Moderate", "142001", "Transmitted data abnormality between MP and GUM"),
    ("Moderate", "180000", "Audit Log lost"),
    ("Service",  "1C0000", "Detected a specific error code SSB"),
    ("Moderate", "212001", "Channel port blocking"),
    ("Moderate", "213001", "CHB blocking"),
    ("Moderate", "214001", "DKB blocking"),
    ("Moderate", "215301", "PECB blocking"),
    ("Service",  "307000", "CHK1A threshold over"),
    ("Moderate", "307300", "Processor blocking"),
    ("Serious",  "307800", "BFM error"),
    ("Moderate", "388F00", "PS OFF impossible"),
    ("Moderate", "389F00", "PS OFF impossible (Device reserved)"),
    ("Service",  "39A000", "The upper temperature limit was exceeded"),
    ("Service",  "410000", "Format complete (Normal end)"),
    ("Service",  "410100", "Quick Format finish"),
    ("Service",  "434001", "Drive media error"),
    ("Serious",  "43B001", "Drive blockade (media)(with redundancy)"),
    ("Serious",  "43C001", "Drive blockade (media)(without redundancy)"),
    ("Service",  "451001", "Correction copy start"),
    ("Service",  "452001", "Correction copy normal end"),
    ("Serious",  "453001", "Correction copy abnormal end"),
    ("Service",  "461001", "Dynamic sparing start (Drive copy)"),
    ("Service",  "462001", "Dynamic sparing normal end (Drive copy)"),
    ("Service",  "491000", "Cache overload condition"),
    ("Service",  "4C1001", "PDEV Erase Start"),
    ("Serious",  "4D1001", "Differential area blocking"),
    ("Service",  "501001", "Drive temporary error"),
    ("Service",  "502001", "Drive media error"),
    ("Service",  "50B001", "Flash drive End of life"),
    ("Moderate", "6610FF", "Acquisition of encryption key from KMS failed"),
    ("Serious",  "6620FF", "Encryption key setting abnormality"),
    ("Moderate", "760000", "CUDG detected error"),
    ("Moderate", "7D0001", "GUM error"),
    ("Moderate", "7D0101", "LAN error (Internal Network)"),
    ("Moderate", "7D0201", "LAN error (CTL1-CTL2)"),
    ("Moderate", "7D0501", "Notification of Alert failed"),
    ("Serious",  "7D0900", "DKC warning"),
    ("Service",  "7FFA00", "Synchronization time failure"),
    ("Moderate", "AC6000", "DKC was set to power error mode"),
    ("Serious",  "AC8001", "Server failure"),
    ("Moderate", "AF1001", "MP Temperature abnormality warning"),
    ("Moderate", "AF1101", "External temperature warning"),
    ("Moderate", "AF1201", "External temperature alarm"),
    ("Moderate", "AF1301", "Thermal monitor warning"),
    ("Moderate", "AF2001", "DKCPS warning"),
    ("Moderate", "AF3001", "Environmental microcomputer warning"),
    ("Moderate", "AF4101", "Battery replacement should be scheduled"),
    ("Moderate", "AF8001", "ENC warning"),
    ("Moderate", "AFF001", "UPS warning"),
    ("Moderate", "AFF101", "GUM warning"),
    ("Moderate", "AFF201", "CFM error"),
    ("Moderate", "AFF301", "FAN warning"),
    ("Service",  "AFF400", "Life expiry warning for DKC air filter"),
    ("Serious",  "BFC010", "DKC ALARM LED light on"),
    ("Moderate", "CF1001", "SAS CTL blocking"),
    ("Moderate", "CF1201", "SAS PORT BLOCK"),
    ("Serious",  "CF1301", "Abnormal error detection"),
    ("Moderate", "CF8801", "CTL blocking"),
    ("Moderate", "CF8A01", "CTL blockade due to CTL interconnect path failure"),
    ("Serious",  "D40001", "Pair suspend (RIO path closed)"),
    ("Serious",  "D41001", "Pair suspend (P-VOL error)"),
    ("Serious",  "D42001", "Pair suspend (S-VOL error)"),
    ("Serious",  "DD0001", "GAD for this volume was suspended"),
    ("Serious",  "DEF0FF", "Quorum Disk Blocked"),
    ("Service",  "DF6001", "Drive port temporary error (Drive path: Boundary 0)"),
    ("Moderate", "DF8001", "Drive port blockade (Drive path: Boundary 0)"),
    ("Serious",  "DFA001", "LDEV blockade (Path 0 / Drive port blockade)"),
    ("Serious",  "EF0001", "Drive blockade (drive)(with redundancy)"),
    ("Serious",  "EF1001", "Drive blockade (drive)(without redundancy)"),
    ("Moderate", "EF4001", "Pinned slot"),
    ("Moderate", "EF5001", "External VOL Write Error"),
    ("Serious",  "EF9001", "LDEV blockade (Effect of drive blockade)"),
    ("Service",  "EFA001", "Drive temporary error"),
    ("Serious",  "EFC001", "Correction access occurred"),
    ("Serious",  "EFD000", "External storage system connection device blockade"),
    ("Serious",  "FE0000", "Cache battery is being charged"),
    ("Service",  "FE0100", "End of Cache Write Through"),
    ("Moderate", "FE0200", "Start of Cache Write Through"),
    ("Moderate", "FF4001", "Pinned slot"),
    ("Moderate", "FF5001", "External VOL Read Error"),
    ("Serious",  "FFE700", "Shared memory is volatilized"),
    ("Serious",  "FFF400", "Area blocking"),
    ("Moderate", "FFFA01", "Battery warning"),

    # ---- Extras to broaden coverage of HITACHI_REFCODE_MAP buckets ----
    ("Acute",    "7D0900", "DKC severe warning - dispatch required"),
    ("Acute",    "AC6000", "DKC powered off due to power error"),
    ("Acute",    "FFE700", "Shared memory volatilization - data risk"),
    ("Serious",  "180000", "Audit log lost - retention compromised"),
    ("Service",  "7FF102", "TrueCopy pair status changed"),
    ("Service",  "7FF104", "Snapshot pair status changed"),
    ("Moderate", "AFF400", "Air filter approaching end of life"),
    ("Info",     "7FFFFF", "Test SIM - communication check"),
    ("Info",     "FFFFFF", "Test trap - validation message"),
]


# ---------------------------------------------------------------------------
# Source IP -> array_name maps. Every IP below is taken straight from the
# corresponding listener's IP_TO_STORAGE_NAME (the only IPs the listener
# will accept past its IP filter). Keep these in sync with the listener
# files by running scripts/_dump_listener_ips.py and pasting the output.
#
# Sizes (current):
#   CDVL : 46 ips -> 28 unique VSP arrays
#   BCP  : 54 ips -> 24 unique VSP arrays
#   SIFY : 74 ips -> 30 unique VSP arrays
# ---------------------------------------------------------------------------
LOCATION_IP_MAP: Dict[str, Dict[str, str]] = {
    "CDVL": {
        "10.1.40.8":      "VSP_E1090_745276-CDVL",
        "10.1.40.9":      "VSP_E1090_745276-CDVL",
        "10.1.40.11":     "VSP_E1090_745249-CDVL",
        "10.1.40.12":     "VSP_E1090_745249-CDVL",
        "10.5.7.228":     "VSP_5500_30629-CDVL",
        "10.5.7.229":     "VSP_5500_30630-CDVL",
        "10.5.7.230":     "VSP_5500_30631-CDVL",
        "10.5.7.231":     "VSP_5500_30637-CDVL",
        "10.226.116.16":  "VSP_5600_42021-CDVL",
        "10.226.116.17":  "VSP_5600_42024-CDVL",
        "10.226.116.18":  "VSP_5600_42025-CDVL",
        "10.226.117.2":   "VSP_5600_42031-CDVL",
        "10.227.60.189":  "VSP_G350_485887-CDVL",
        "10.227.60.190":  "VSP_G350_485887-CDVL",
        "10.227.61.50":   "VSP_E990_418407-CDVL",
        "10.227.61.51":   "VSP_E990_418407-CDVL",
        "10.227.61.53":   "VSP_G350_485539-CDVL",
        "10.227.61.54":   "VSP_G350_485539-CDVL",
        "10.227.61.56":   "VSP_E990_418296-CDVL",
        "10.227.61.57":   "VSP_E990_418296-CDVL",
        "10.227.61.125":  "VSP_5500_33796-CDVL",
        "10.227.61.127":  "VSP_G370_486443-CDVL",
        "10.227.61.128":  "VSP_G370_486443-CDVL",
        "10.227.61.130":  "VSP_G370_454543-CDVL",
        "10.227.61.131":  "VSP_G370_454543-CDVL",
        "10.227.62.221":  "VSP_E990_418407-CDVL",
        "10.227.62.222":  "VSP_E990_418407-CDVL",
        "10.227.63.5":    "VSP_5600_40359-CDVL",
        "10.227.63.80":   "VSP_5600_40524-CDVL",
        "10.227.63.141":  "VSP_5600_40772-CDVL",
        "10.227.68.117":  "VSP_E1090_715242-CDVL",
        "10.227.68.118":  "VSP_E1090_715242-CDVL",
        "10.227.68.120":  "VSP_E1090_745191-CDVL",
        "10.227.68.121":  "VSP_E1090_745191-CDVL",
        "10.227.68.123":  "VSP_E1090_745185-CDVL",
        "10.227.68.124":  "VSP_E1090_745185-CDVL",
        "10.227.68.126":  "VSP_E1090_715315-CDVL",
        "10.227.68.127":  "VSP_E1090_715315-CDVL",
        "10.227.68.129":  "VSP_E1090_715316-CDVL",
        "10.227.68.130":  "VSP_E1090_715316-CDVL",
        "10.227.68.132":  "VSP_E1090_715318-CDVL",
        "10.227.68.133":  "VSP_E1090_715318-CDVL",
        "10.227.71.50":   "VSP_E1090_715300-CDVL",
        "10.227.71.51":   "VSP_E1090_715300-CDVL",
        "10.229.230.232": "VSP_E1090_715333-CDVL",
        "10.229.230.233": "VSP_E1090_715333-CDVL",
    },
    "BCP": {
        "10.65.4.8":     "VSP_E990_418477-BCP",
        "10.65.4.9":     "VSP_E990_418477-BCP",
        "10.65.4.89":    "VSP_G370_483369-BCP",
        "10.65.4.90":    "VSP_G370_483369-BCP",
        "10.65.4.91":    "VSP_G370_483369-BCP",
        "10.65.4.92":    "VSP_G370_483670-BCP",
        "10.65.4.93":    "VSP_G370_483670-BCP",
        "10.65.4.94":    "VSP_G370_483670-BCP",
        "10.65.4.99":    "VSP_5600_40281-BCP",
        "10.65.4.112":   "VSP_G700_445731-BCP",
        "10.65.4.113":   "VSP_G700_445731-BCP",
        "10.65.4.114":   "VSP_G700_445731-BCP",
        "10.65.4.115":   "VSP_G350_482130-BCP",
        "10.65.4.116":   "VSP_G350_482130-BCP",
        "10.65.4.117":   "VSP_G350_482130-BCP",
        "10.65.5.5":     "VSP_E990_418269-BCP",
        "10.65.5.6":     "VSP_E990_418613-BCP",
        "10.65.5.7":     "VSP_E990_418477-BCP",
        "10.65.5.223":   "VSP_E990_418269-BCP",
        "10.65.5.224":   "VSP_E990_418269-BCP",
        "10.65.7.184":   "VSP_E990_418613-BCP",
        "10.65.7.185":   "VSP_E990_418613-BCP",
        "10.65.12.189":  "VSP_5600_40777-BCP",
        "10.65.13.200":  "VSP_5600_40512-BCP",
        "10.65.14.103":  "VSP_5600_40891-BCP",
        "10.65.14.241":  "VSP_5600_40766-BCP",
        "10.65.15.147":  "VSP_5600_41550-BCP",
        "10.65.15.148":  "VSP_5600_41548-BCP",
        "10.225.39.253": "VSP_5500_30628-BCP",
        "10.225.39.254": "VSP_5500_30636-BCP",
        "10.225.51.53":  "VSP_E1090_715428-BCP",
        "10.225.51.54":  "VSP_E1090_715428-BCP",
        "10.225.51.219": "VSP_E1090_715428-BCP",
        "10.225.72.108": "VSP_E1090_715334-BCP",
        "10.225.72.109": "VSP_E1090_715334-BCP",
        "10.225.72.111": "VSP_E1090_715329-BCP",
        "10.225.72.112": "VSP_E1090_715329-BCP",
        "10.225.72.114": "VSP_E1090_715346-BCP",
        "10.225.72.115": "VSP_E1090_715346-BCP",
        "10.225.72.117": "VSP_E1090_715339-BCP",
        "10.225.72.118": "VSP_E1090_715339-BCP",
        "10.225.72.120": "VSP_E1090_715380-BCP",
        "10.225.72.121": "VSP_E1090_715380-BCP",
        "10.225.72.123": "VSP_E1090_715382-BCP",
        "10.225.72.124": "VSP_E1090_715382-BCP",
        "10.225.72.126": "VSP_E1090_715399-BCP",
        "10.225.72.127": "VSP_E1090_715399-BCP",
        "10.225.72.139": "VSP_E1090_715329-BCP",
        "10.225.72.140": "VSP_E1090_715334-BCP",
        "10.225.72.142": "VSP_E1090_715339-BCP",
        "10.225.72.144": "VSP_E1090_715346-BCP",
        "10.225.72.145": "VSP_E1090_715380-BCP",
        "10.225.72.146": "VSP_E1090_715382-BCP",
        "10.225.72.147": "VSP_E1090_715399-BCP",
    },
    "SIFY": {
        "10.226.63.1":    "VSP_5500_30648-SIFY",
        "10.226.63.2":    "VSP_5500_30649-SIFY",
        "10.226.63.3":    "VSP_5500_30650-SIFY",
        "10.226.63.4":    "VSP_5500_30651-SIFY",
        "10.226.63.126":  "VSP_G370_482340-SIFY",
        "10.226.63.127":  "VSP_G370_482340-SIFY",
        "10.226.63.128":  "VSP_G370_482340-SIFY",
        "10.226.63.129":  "VSP_G370_482531-SIFY",
        "10.226.63.130":  "VSP_G370_482531-SIFY",
        "10.226.63.131":  "VSP_G370_482531-SIFY",
        "10.226.63.162":  "VSP_G350_482542-SIFY",
        "10.226.63.165":  "VSP_G700_445784-SIFY",
        "10.226.63.166":  "VSP_G700_445784-SIFY",
        "10.226.63.167":  "VSP_G700_445784-SIFY",
        "10.226.79.134":  "VSP_E1090_715444-SIFY",
        "10.226.79.135":  "VSP_E1090_715444-SIFY",
        "10.226.79.137":  "VSP_E1090_715445-SIFY",
        "10.226.79.138":  "VSP_E1090_715445-SIFY",
        "10.226.79.143":  "VSP_E1090_715344-SIFY",
        "10.226.79.144":  "VSP_E1090_715344-SIFY",
        "10.226.79.146":  "VSP_E1090_715381-SIFY",
        "10.226.79.147":  "VSP_E1090_715381-SIFY",
        "10.226.79.159":  "VSP_E1090_715344-SIFY",
        "10.226.79.160":  "VSP_E1090_715381-SIFY",
        "10.226.79.161":  "VSP_E1090_715444-SIFY",
        "10.226.79.162":  "VSP_E1090_715445-SIFY",
        "10.226.80.13":   "VSP_E990_418368-SIFY",
        "10.226.80.14":   "VSP_E990_418331-SIFY",
        "10.226.80.15":   "VSP_E990_418360-SIFY",
        "10.226.80.234":  "VSP_E990_417949-SIFY",
        "10.226.80.235":  "VSP_E990_417949-SIFY",
        "10.226.81.0":    "VSP_E990_418369-SIFY",
        "10.226.81.2":    "VSP_E990_418295-SIFY",
        "10.226.81.3":    "VSP_E990_448524-SIFY",
        "10.226.81.92":   "VSP_5500_31302-SIFY",
        "10.226.81.100":  "VSP_E990_418331-SIFY",
        "10.226.81.101":  "VSP_E990_418331-SIFY",
        "10.226.81.103":  "VSP_E990_418360-SIFY",
        "10.226.81.104":  "VSP_E990_418360-SIFY",
        "10.226.81.105":  "VSP_G370_454453-SIFY",
        "10.226.81.106":  "VSP_G370_454453-SIFY",
        "10.226.81.107":  "VSP_G370_454453-SIFY",
        "10.226.81.108":  "VSP_G370_454484-SIFY",
        "10.226.81.109":  "VSP_G370_454484-SIFY",
        "10.226.81.110":  "VSP_G370_454484-SIFY",
        "10.226.81.112":  "VSP_E990_418369-SIFY",
        "10.226.81.113":  "VSP_E990_418369-SIFY",
        "10.226.81.115":  "VSP_E990_418368-SIFY",
        "10.226.81.116":  "VSP_E990_418368-SIFY",
        "10.226.81.192":  "VSP_E990_418295-SIFY",
        "10.226.81.193":  "VSP_E990_418295-SIFY",
        "10.226.81.212":  "VSP_E990_448524-SIFY",
        "10.226.81.213":  "VSP_E990_448524-SIFY",
        "10.226.81.215":  "VSP_G350_485946-SIFY",
        "10.226.81.216":  "VSP_G350_485946-SIFY",
        "10.226.81.217":  "VSP_G350_485946-SIFY",
        "10.226.81.255":  "VSP_E990_418719-SIFY",
        "10.226.82.0":    "VSP_E990_417949-SIFY",
        "10.226.82.169":  "VSP_E990_418719-SIFY",
        "10.226.82.170":  "VSP_E990_418719-SIFY",
        "10.226.82.219":  "VSP_5600_40279-SIFY",
        "10.226.83.83":   "VSP_5600_40530-SIFY",
        "10.226.83.107":  "VSP_5600_60522-SIFY",
        "10.226.83.177":  "VSP_5600_40850-SIFY",
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
    },
}

DEFAULT_TARGETS: Dict[str, str] = {
    "CDVL": "127.0.0.1:5514",
    "BCP":  "127.0.0.1:5515",
    "SIFY": "127.0.0.1:5516",
}

# ---------------------------------------------------------------------------
# Multi-vendor configuration
# ---------------------------------------------------------------------------

VENDORS = ["Hitachi", "Brocade", "SANnav", "NetApp", "Dell"]

# Per-vendor dev-mode listener ports
VENDOR_TARGETS: Dict[str, Dict[str, str]] = {
    "Hitachi": {
        "CDVL": "127.0.0.1:5514",
        "BCP":  "127.0.0.1:5515",
        "SIFY": "127.0.0.1:5516",
    },
    "Brocade": {
        # Brocade is split into 2 location-pair listeners in v2:
        #   cdvl_n_sify -> port 5214 (CDVL + SIFY traffic)
        #   bcp_n_uat   -> port 5215 (BCP + UAT traffic)
        # All four locations are exposed in the UI; CDVL/SIFY both hit
        # 5214 and BCP/UAT both hit 5215 — the listener tags the alert
        # with the correct `environment` based on the switch name.
        "CDVL": "127.0.0.1:5214",
        "SIFY": "127.0.0.1:5214",
        "BCP":  "127.0.0.1:5215",
        "UAT":  "127.0.0.1:5215",
    },
    "SANnav": {
        "CDVL": "127.0.0.1:5214",
        "SIFY": "127.0.0.1:5214",
        "BCP":  "127.0.0.1:5215",
        "UAT":  "127.0.0.1:5215",
    },
    "NetApp": {
        "CDVL": "127.0.0.1:5314",
        "BCP":  "127.0.0.1:5315",
        "SIFY": "127.0.0.1:5316",
    },
    "Dell": {
        "CDVL": "127.0.0.1:5414",
        "BCP":  "127.0.0.1:5415",
        "SIFY": "127.0.0.1:5416",
    },
}

# Per-vendor IP -> array_name map. Brocade entries are taken directly from
# listener/syslog_trap_listener_*_n_*.py's SWITCH_IP_TO_NAME / SWITCH_INFO
# maps so the listener's resolve_switch_meta() returns the real switch
# identity for every IP we send from.
VENDOR_STORAGE_IPS: Dict[str, Dict[str, Dict[str, str]]] = {
    "Hitachi": LOCATION_IP_MAP,
    "SANnav": {
        "CDVL": {
            "10.66.12.197":   "CDVL_7840_HUR_FAB1",
            "10.66.12.198":   "CDVL_7840_HUR_FAB2",
        },
        "SIFY": {
            "10.226.79.134":  "SIFY_7840_HUR_FAB1",
        },
        "BCP": {
            "10.65.4.8":      "BCP_7840_HUR_FAB1",
        },
        "UAT": {
            "10.65.15.147":   "UAT_7840_HUR_FAB1",
        }
    },
    "Brocade": {
        "CDVL": {
            "10.66.12.197":   "CDVL_7840_HUR_FAB1",
            "10.66.12.198":   "CDVL_7840_HUR_FAB2",
            "10.227.63.173":  "NTTC_DC6A_7840_HUR_FAB1",
            "10.227.61.37":   "NTTC_X7_101_FAB1",
            "10.227.61.40":   "NTTC_X7_102_FAB2",
            "10.227.61.43":   "NTTC_X7_103_FAB1",
            "10.227.61.46":   "NTTC_X7_104_FAB2",
            "10.227.61.227":  "NTTC_X7_105_FAB1",
            "10.227.61.230":  "NTTC_X7_106_FAB2",
            "10.227.63.142":  "NTTC_X7_109_FAB1",
            "10.227.63.148":  "NTTC_X7_111_FAB1",
            "10.227.64.135":  "NTTC_X7_113_FAB1",
            "10.227.66.102":  "NTTC_X7_115_FAB1",
            "10.227.66.202":  "NTTC_X7_117_FAB1",
            "10.226.116.229": "NTT_DC9_X7_121_FAB1",
            "10.226.116.247": "NTT_DC9_X7_123_FAB1",
            "10.226.116.235": "NTT_DC9_X7_125_FAB1",
            "10.226.116.241": "NTT_DC9_X7_127_FAB1",
        },
        "SIFY": {
            "10.226.14.80":   "DAKC_7840_HUR_FAB1",
            "10.226.14.79":   "DAKC_7840_HUR_FAB2",
            "10.226.63.70":   "SIFY_X6_109_FAB1",
            "10.226.63.73":   "SIFY_X6_110_FAB2",
            "10.226.63.122":  "SIFY_X6_103_FAB1",
            "10.226.63.125":  "SIFY_X6_104_FAB2",
            "10.226.63.170":  "SIFY_X6_101_FAB1",
            "10.226.63.173":  "SIFY_X6_102_FAB2",
            "10.226.80.174":  "SIFY_X6_107_FAB1",
            "10.226.80.180":  "SIFY_X6_105_FAB1",
            "10.226.83.4":    "SIFY_X7_111_FAB1",
            "10.226.83.7":    "SIFY_X7_112_FAB2",
        },
        "BCP": {
            "10.225.42.1":  "BCP_X7_201_FAB1",
            "10.225.42.2":  "BCP_X7_202_FAB2",
            "10.225.42.3":  "BCP_X6_203_FAB1",
            "10.225.42.4":  "BCP_X6_204_FAB2",
        },
        "UAT": {
            "10.225.50.1":  "UAT_X6_301_FAB1",
            "10.225.50.2":  "UAT_X6_302_FAB2",
        },
    },
    "NetApp": {
        "CDVL": {
            "10.227.62.18": "FAS_8200_4187-CDVL",
            "10.227.62.19": "AFF_A800_2210-CDVL",
            "10.227.62.20": "AFF_A400_2211-CDVL",
            "10.227.62.21": "FAS_8300_4188-CDVL",
        },
        "BCP": {
            "10.225.40.12": "AFF_A800_4112-BCP",
            "10.225.40.13": "FAS_8300_4188-BCP",
            "10.225.40.14": "AFF_C800_4113-BCP",
        },
        "SIFY": {
            "10.226.64.10": "AFF_C400_2210-SIFY",
            "10.226.64.11": "AFF_A400_2211-SIFY",
            "10.226.64.12": "FAS_2750_4189-SIFY",
        },
    },
    "Dell": {
        "CDVL": {
            "10.227.64.85": "PowerMax_8500_CKM00220100123-CDVL",
            "10.227.64.86": "PowerStore_5000T-CDVL",
            "10.227.64.87": "PowerMax_2500_CKM00220100456-CDVL",
        },
        "BCP": {
            "10.225.41.50": "PowerStore_5000T-BCP",
            "10.225.41.51": "PowerMax_8500_CKM00220100789-BCP",
            "10.225.41.52": "PowerScale_F900-BCP",
        },
        "SIFY": {
            "10.226.65.30": "PowerStore_3000T-SIFY",
            "10.226.65.31": "PowerMax_2500_CKM00220100999-SIFY",
            "10.226.65.32": "PowerScale_H500-SIFY",
        },
    },
}

# Per-vendor reference catalogues (severity, code, text).
#
# Brocade entries are the real FOS event-IDs from the listener's
# BRCD_ALERT_SEVERITY catalog (which is itself sourced from the Broadcom
# "Fabric OS Message Reference Manual 9.2.x"). The listener resolves
# each msgid to a real Brocade module + maps the FOS severity word
# (INFO/WARNING/ERROR/CRITICAL) to our canonical severity bucket.
#
#   FOS severity -> trap_sender severity
#     CRITICAL   -> Acute
#     ERROR      -> Serious
#     WARNING    -> Moderate
#     INFO       -> Service / Info
#
# NetApp / Dell catalogues use realistic EMS / PowerStore event IDs.
# Those listeners are placeholders (no full structured parse), but the
# `severity_from_body()` PRI fallback now correctly tags severity +
# `category_from_body()` derives a trap_category from keyword matching.
VENDOR_CATALOGUES: Dict[str, List[Tuple[str, str, str]]] = {
    "Hitachi": [], # Filled in below — points to SIM_CATALOGUE
    "Brocade": [
        # ---- Hardware (HIL) -- Hardware Interface Layer -----------------
        ("Acute",    "HIL-1502",  "Power supply fault - critical"),
        ("Acute",    "HIL-1602",  "Cooling fan fault - critical"),
        ("Acute",    "HIL-1612",  "Temperature sensor critical threshold"),
        ("Serious",  "HIL-1202",  "Hardware diagnostic error detected"),
        ("Moderate", "HIL-1310",  "Hardware threshold approaching"),
        # ---- Environmental Monitor (EM) ---------------------------------
        ("Acute",    "EM-1003",   "Temperature sensor failure - immediate action required"),
        ("Acute",    "EM-1011",   "Fan FRU failed - immediate replacement required"),
        ("Acute",    "EM-1001",   "Environmental monitor self-test failed"),
        ("Serious",  "EM-1013",   "Temperature above warning threshold"),
        ("Moderate", "EM-1015",   "Cooling temperature warning"),
        ("Moderate", "EM-1043",   "Power supply input voltage out of range"),
        # ---- Blade / chassis (BL / CHS / BM) ----------------------------
        ("Acute",    "BL-1002",   "Blade slot reset"),
        ("Acute",    "BL-1004",   "Blade fault - replacement required"),
        ("Acute",    "BL-1011",   "Blade powered down due to fault"),
        ("Serious",  "BL-1013",   "Blade reset due to error"),
        ("Serious",  "CHS-1002",  "Chassis power configuration error"),
        ("Moderate", "BM-1003",   "Blade manager - configuration mismatch"),
        # ---- HA Manager (HAM) -------------------------------------------
        ("Acute",    "HAM-1001",  "Active CP failure - failover triggered"),
        ("Acute",    "HAM-1010",  "Standby CP failed - HA degraded"),
        ("Serious",  "HAM-1013",  "HA heartbeat lost"),
        ("Info",     "HAM-1004",  "HA state transition to active"),
        # ---- Ports / Port Manager ---------------------------------------
        ("Moderate", "PORT-1003", "Port state change - link down"),
        ("Moderate", "PORT-1011", "Port disabled by administrator"),
        ("Serious",  "PMGR-1002", "Port manager - port fault detected"),
        ("Serious",  "PMGR-1010", "Port disabled due to errors"),
        # ---- Flow Vision (FV) -------------------------------------------
        ("Moderate", "FV-3024",   "Flow Vision threshold violation"),
        ("Info",     "FV-1001",   "Flow Vision flow activated"),
        # ---- Platform / Power Supply ------------------------------------
        ("Acute",    "PLAT-1004", "Platform fault - service required"),
        ("Acute",    "PLAT-1010", "Platform power critical"),
        ("Acute",    "PS-1000",   "Power supply critical - PSU offline"),
        ("Moderate", "PS-1009",   "Power supply warning - voltage drift"),
        # ---- Port Swap / Support Save -----------------------------------
        ("Serious",  "PSWP-1005", "Port swap operation failed"),
        ("Moderate", "SS-1001",   "Support save started"),
    ],
    "SANnav": [
        # ---- SANnav -----------------------------------------------------
        ("Serious",  "SSMP-AUTH-1025", "SANnav authentication failure - max retries"),
        ("Moderate", "SSMP-AUDIT-1100","SANnav audit event - config change"),
        ("Info",     "SSMP-1200",      "SANnav system event"),
    ],
    "NetApp": [
        # ---- Hardware / disk -------------------------------------------
        ("Acute",    "callhome.disk.fail",              "Disk failure detected - drive replacement required"),
        ("Acute",    "raid.config.filesystem.disk.missing", "Filesystem disk missing - aggregate at risk"),
        ("Serious",  "raid.config.disk.bad",            "RAID configuration - disk marked bad"),
        ("Serious",  "disk.failmsg",                    "Disk drive failed - sparing engaged"),
        ("Moderate", "disk.predictedFailure",           "Disk predicted to fail soon"),
        # ---- Controller / cluster --------------------------------------
        ("Acute",    "callhome.cf.takeover",            "Controller takeover triggered - cluster failover"),
        ("Acute",    "cf.fm.partnerInterconnectDown",   "Cluster interconnect partner is down"),
        ("Serious",  "monitor.shelfPower.warning",      "Shelf power supply warning"),
        ("Serious",  "fsm.ringStateError",              "Cluster ring state error"),
        # ---- NVRAM / cache battery -------------------------------------
        ("Acute",    "nvram.battery.lowBattery",        "NVRAM battery low - data risk"),
        ("Moderate", "nvram.battery.discharged",        "NVRAM battery discharged - charging"),
        # ---- Storage capacity ------------------------------------------
        ("Serious",  "monitor.volume.nearlyFull",       "Volume nearly full - capacity exhausting"),
        ("Moderate", "monitor.volume.full",             "Volume full - writes blocked"),
        ("Moderate", "wafl.vol.autoSize.fail",          "Volume auto-size grow failed"),
        ("Moderate", "monitor.aggregate.nearlyFull",    "Aggregate nearly full"),
        # ---- Replication / SnapMirror ----------------------------------
        ("Serious",  "snapmirror.dst.OutOfDate",        "SnapMirror destination out of date"),
        ("Service",  "snapmirror.dst.transfer.complete","SnapMirror destination transfer complete"),
        ("Moderate", "snapmirror.dst.err.transfer",     "SnapMirror transfer error - retry pending"),
        # ---- Network / port -------------------------------------------
        ("Serious",  "fcp.adapter.link.down",           "FCP adapter link down"),
        ("Serious",  "iscsi.notice.lif.down",           "iSCSI LIF down"),
        ("Service",  "nic.linkup",                      "Network interface up"),
        # ---- Auth / audit ---------------------------------------------
        ("Moderate", "secd.auth.noServers",             "Authentication - no servers available"),
        ("Service",  "lun.create.success",              "LUN created successfully"),
    ],
    "Dell": [
        # ---- PowerStore severity codes ---------------------------------
        ("Acute",    "hwHealthStateChanged.critical",   "Hardware health degraded to CRITICAL"),
        ("Acute",    "diskFailed",                      "Disk failed - replace drive"),
        ("Acute",    "replicationSessionDown",          "Replication session down"),
        ("Acute",    "computeNodeFault",                "Compute node fault - failover triggered"),
        ("Acute",    "powerSupplyFailed",               "Power supply failed - PSU offline"),
        # ---- Errors ---------------------------------------------------
        ("Serious",  "raidGroupDegraded",               "RAID group degraded - one or more disks failed"),
        ("Serious",  "fanFailed",                       "Cooling fan failed"),
        ("Serious",  "temperatureCritical",             "Temperature critical threshold exceeded"),
        ("Serious",  "lunOffline",                      "LUN offline - attention required"),
        # ---- Warnings -------------------------------------------------
        ("Moderate", "raidGroupRebuild",                "RAID group rebuild in progress"),
        ("Moderate", "storagePoolThresholdWarning",     "Storage pool threshold reached - capacity warning"),
        ("Moderate", "psuVoltageWarning",               "PSU output voltage out of expected range"),
        ("Moderate", "temperatureWarning",              "Temperature warning threshold exceeded"),
        ("Moderate", "batteryDegraded",                 "Battery degraded - schedule replacement"),
        # ---- PowerMax / Symmetrix --------------------------------------
        ("Acute",    "symAlertCallHome.critical",       "PowerMax call-home critical alert"),
        ("Serious",  "symDirectorOffline",              "PowerMax director offline"),
        ("Moderate", "symDeviceServiceState",           "PowerMax device service state changed"),
        ("Service",  "symGroupCreated",                 "PowerMax storage group created"),
        # ---- Service / info -------------------------------------------
        ("Service",  "lunCreated",                      "LUN created"),
        ("Service",  "snapShotComplete",                "Snapshot creation complete"),
        ("Service",  "softwareUpdateComplete",          "Software upgrade completed successfully"),
    ],
}
VENDOR_CATALOGUES["Hitachi"] = SIM_CATALOGUE

# severity -> syslog priority (facility=local0 (16) << 3 | severity).
# Matches what real arrays emit: warning/error/critical map to <132>/<131>/<130>.
SYSLOG_PRI_FOR_SEVERITY: Dict[str, int] = {
    "Acute":    130,   # critical
    "Serious":  131,   # error
    "Moderate": 132,   # warning
    "Service":  133,   # notice
    "Info":     134,   # informational
}

VALID_SEVERITIES = list(SYSLOG_PRI_FOR_SEVERITY.keys())


# ---------------------------------------------------------------------------
# Packet builder
# ---------------------------------------------------------------------------

def build_packet(
    *,
    source_ip: str,
    array_name: str,
    severity: str,
    refcode: str,
    text: str,
    seq: int,
    use_gum_envelope: bool = True,
    rfc5424: bool = True,
    hostname: str = "hitachi-trap",
) -> bytes:
    pri = SYSLOG_PRI_FOR_SEVERITY.get(severity, 132)
    envelope = "GUM Storage" if use_gum_envelope else "SVP Storage"
    keyword = "NewHitrack" if use_gum_envelope else "Hitachi_syslog"
    payload = (
        f"{envelope}: {seq}, {severity}, {array_name}, "
        f"{keyword}, RefCode: {refcode}, {text}"
    )
    if rfc5424:
        now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        msg = (
            f"[SOURCE_IP={source_ip}] "
            f"<{pri}>1 {now} {hostname} hitachi-trap - - - {payload}"
        )
    else:
        now = dt.datetime.now().strftime("%b %d %H:%M:%S")
        msg = (
            f"[SOURCE_IP={source_ip}] "
            f"<{pri}>{now} {hostname} hitachi-trap: {payload}"
        )
    return msg.encode("utf-8")


_SEV_WORD: Dict[str, str] = {
    "Acute":    "EMERGENCY",
    "Serious":  "ERROR",
    "Moderate": "WARNING",
    "Service":  "NOTICE",
    "Info":     "INFO",
}


def build_brocade_packet(
    *, source_ip: str, array_name: str, severity: str,
    refcode: str, text: str, seq: int,
) -> bytes:
    """Brocade FOS via SANnav: RFC5424 with structured-data block.
       Mirrors what the listener at port 514/515 expects."""
    pri = SYSLOG_PRI_FOR_SEVERITY.get(severity, 132)
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    sannav_ts = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    sd = (
        f"[timestamp@1588 value=\"{sannav_ts}\"]"
        f"[msgid@1588 value=\"{refcode}\"]"
        f"[severity@1588 value=\"{_SEV_WORD.get(severity, 'WARNING')}\"]"
        f"[swname@1588 value=\"{array_name}\"]"
        f"[swip@1588 value=\"{source_ip}\"]"
    )
    msg = (
        f"[SOURCE_IP={source_ip}] "
        f"<{pri}>1 {now} sannav-fwd SVCFM SANNAV - {sd} \ufeff{text}"
    )
    return msg.encode("utf-8")


def build_sannav_packet(
    *, source_ip: str, array_name: str, severity: str,
    refcode: str, text: str, seq: int,
) -> bytes:
    """SANnav specific syslog event (alarms, events, violations)."""
    pri = SYSLOG_PRI_FOR_SEVERITY.get(severity, 132)
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    sannav_ts = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    sd = (
        f"[timestamp@1588 value=\"{sannav_ts}\"]"
        f"[msgid@1588 value=\"{refcode}\"]"
        f"[severity@1588 value=\"{_SEV_WORD.get(severity, 'WARNING')}\"]"
        f"[swname@1588 value=\"{array_name}\"]"
        f"[swip@1588 value=\"{source_ip}\"]"
    )
    msg = (
        f"[SOURCE_IP={source_ip}] "
        f"<{pri}>1 {now} sannav-fwd SVCFM SANNAV - {sd} \ufeff{text}"
    )
    return msg.encode("utf-8")


def build_netapp_packet(
    *, source_ip: str, array_name: str, severity: str,
    refcode: str, text: str, seq: int,
) -> bytes:
    """NetApp ONTAP EMS-style syslog. Placeholder format for dev."""
    pri = SYSLOG_PRI_FOR_SEVERITY.get(severity, 132)
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    sd = (
        f"[event=\"{refcode}\"]"
        f"[severity=\"{_SEV_WORD.get(severity, 'WARNING')}\"]"
        f"[seqnum=\"{seq}\"]"
    )
    msg = (
        f"[SOURCE_IP={source_ip}] "
        f"<{pri}>1 {now} {array_name} EMS - - - {sd} {text}"
    )
    return msg.encode("utf-8")


def build_dell_packet(
    *, source_ip: str, array_name: str, severity: str,
    refcode: str, text: str, seq: int,
) -> bytes:
    """Dell PowerStore / PowerMax-style syslog. Placeholder format for dev."""
    pri = SYSLOG_PRI_FOR_SEVERITY.get(severity, 132)
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    msg = (
        f"[SOURCE_IP={source_ip}] "
        f"<{pri}>1 {now} {array_name} PowerStore - - - "
        f"{refcode}: severity={severity}, seq={seq}, message=\"{text}\""
    )
    return msg.encode("utf-8")


VENDOR_BUILDERS = {
    "Hitachi": None,  # Uses build_packet() directly (needs envelope/rfc args)
    "Brocade": build_brocade_packet,
    "SANnav":  build_sannav_packet,
    "NetApp":  build_netapp_packet,
    "Dell":    build_dell_packet,
}


def build_packet_for_vendor(
    vendor: str,
    *,
    source_ip: str,
    array_name: str,
    severity: str,
    refcode: str,
    text: str,
    seq: int,
    use_gum_envelope: bool = True,
    rfc5424: bool = True,
    hostname: str = "hitachi-trap",
) -> bytes:
    """Wrapper that delegates to the appropriate builder function."""
    if vendor == "Hitachi":
        return build_packet(
            source_ip=source_ip, array_name=array_name, severity=severity,
            refcode=refcode, text=text, seq=seq,
            use_gum_envelope=use_gum_envelope,
            rfc5424=rfc5424,
            hostname=hostname,
        )
    
    builder = VENDOR_BUILDERS.get(vendor)
    if not builder:
        raise ValueError(f"Unknown vendor {vendor}")
    
    return builder(
        source_ip=source_ip, array_name=array_name, severity=severity,
        refcode=refcode, text=text, seq=seq,
    )


def pick_source_ip_for_vendor(
    vendor: str, location: str, explicit_ip: str = "",
) -> Tuple[str, str]:
    vendor_map = VENDOR_STORAGE_IPS.get(vendor, {})
    pool = vendor_map.get(location, {})
    if explicit_ip:
        return explicit_ip, pool.get(explicit_ip, f"UNKNOWN-{vendor}-{location}")
    if not pool:
        return "10.0.0.0", f"UNKNOWN-{vendor}-{location}"
    ip = random.choice(list(pool.keys()))
    return ip, pool[ip]


def pick_catalogue_entry_for_vendor(
    vendor: str, refcode: str = "",
) -> Tuple[str, str, str]:
    catalogue = VENDOR_CATALOGUES.get(vendor) or SIM_CATALOGUE
    if not refcode:
        return random.choice(catalogue)
    refcode_upper = refcode.upper()
    for sev, code, text in catalogue:
        if code.upper() == refcode_upper:
            return sev, code, text
    return ("Moderate", refcode_upper, f"Custom trap (refcode not in {vendor} catalogue)")


def parse_target(target: str) -> Tuple[str, int]:
    if ":" not in target:
        raise argparse.ArgumentTypeError(
            f"--target must be host:port, got {target!r}"
        )
    host, port_str = target.rsplit(":", 1)
    try:
        port = int(port_str)
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"bad port in --target: {target!r}") from e
    return host, port


def pick_source_ip(location: str, explicit_ip: str = "") -> Tuple[str, str]:
    pool = LOCATION_IP_MAP[location]
    if explicit_ip:
        if explicit_ip not in pool:
            print(
                f"warning: {explicit_ip} not in {location} IP map; "
                f"the listener will fall back to TEST_DEFAULT_IP.",
                file=sys.stderr,
            )
            return explicit_ip, f"UNKNOWN-{location}"
        return explicit_ip, pool[explicit_ip]
    ip = random.choice(list(pool.keys()))
    return ip, pool[ip]


def pick_catalogue_entry(refcode: str = "") -> Tuple[str, str, str]:
    if not refcode:
        return random.choice(SIM_CATALOGUE)
    refcode_upper = refcode.upper()
    for sev, code, text in SIM_CATALOGUE:
        if code.upper() == refcode_upper:
            return sev, code, text
    # Unknown - return a passthrough entry.
    return ("Moderate", refcode_upper, "Custom trap (refcode not in catalogue)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def cmd_list_refcodes() -> None:
    print(f"{'Severity':<9}  {'RefCode':<8}  Description")
    print(f"{'-' * 9}  {'-' * 8}  {'-' * 60}")
    for sev, code, text in SIM_CATALOGUE:
        print(f"{sev:<9}  {code:<8}  {text}")
    print(f"\n{len(SIM_CATALOGUE)} entries.")


def main() -> int:
    p = argparse.ArgumentParser(
        description="UnifiedOps dev trap sender (Hitachi syslog/UDP)."
    )
    p.add_argument(
        "--location", "-l",
        choices=list(LOCATION_IP_MAP.keys()),
        default="CDVL",
        help="Which location to spoof. Picks IP + array_name from that "
             "location's map. Default: CDVL.",
    )
    p.add_argument(
        "--source-ip",
        default="",
        help="Specific source IP to spoof (must be in the location's map "
             "for the listener to keep it).",
    )
    p.add_argument(
        "--target", "-t",
        default="",
        type=str,
        help="host:port to send to. Default per location: "
             + ", ".join(f"{k}={v}" for k, v in DEFAULT_TARGETS.items()),
    )
    p.add_argument(
        "--refcode", "-r",
        default="",
        help="SIM reference code (hex). If not in the built-in catalogue "
             "you must also pass --text.",
    )
    p.add_argument(
        "--severity", "-s",
        choices=VALID_SEVERITIES,
        default="",
        help="Override the severity from the catalogue.",
    )
    p.add_argument(
        "--text",
        default="",
        help="Override the message text. Useful when --refcode is custom.",
    )
    p.add_argument(
        "--random",
        action="store_true",
        help="Pick a random catalogue entry + random IP from the location.",
    )
    p.add_argument(
        "--count", "-n",
        type=int, default=1,
        help="Number of packets to send. Default 1. Ignored when --infinite is set.",
    )
    p.add_argument(
        "--infinite",
        action="store_true",
        help="Keep sending until killed. Use with --interval to throttle.",
    )
    p.add_argument(
        "--interval", "-i",
        type=float, default=0.0,
        help="Seconds to sleep between packets in batch / infinite mode.",
    )
    p.add_argument(
        "--svp",
        action="store_true",
        help="Use the legacy 'SVP Storage / Hitachi_syslog' envelope instead "
             "of 'GUM Storage / NewHitrack' (the BCP/SIFY arrays emit GUM).",
    )
    p.add_argument(
        "--rfc3164",
        action="store_true",
        help="Send as RFC3164 instead of RFC5424.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print packets instead of sending.",
    )
    p.add_argument(
        "--list-refcodes",
        action="store_true",
        help="Print the built-in SIM catalogue and exit.",
    )
    args = p.parse_args()

    if args.list_refcodes:
        cmd_list_refcodes()
        return 0

    target_str = args.target or DEFAULT_TARGETS[args.location]
    host, port = parse_target(target_str)

    sock = None
    if not args.dry_run:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    sent = 0
    i = 0
    total = float("inf") if args.infinite else args.count
    while i < total:
        # Picker: explicit refcode overrides --random.
        if args.refcode:
            sev, code, text = pick_catalogue_entry(args.refcode)
        elif args.random:
            sev, code, text = random.choice(SIM_CATALOGUE)
        else:
            # First call defaults to the first catalogue entry; subsequent
            # calls reuse it unless --random.
            sev, code, text = SIM_CATALOGUE[0]

        severity = args.severity or sev
        message  = args.text or text

        source_ip, array_name = pick_source_ip(args.location, args.source_ip)
        seq = 100000 + random.randint(0, 99999)

        packet = build_packet(
            source_ip=source_ip,
            array_name=array_name,
            severity=severity,
            refcode=code,
            text=message,
            seq=seq,
            use_gum_envelope=not args.svp,
            rfc5424=not args.rfc3164,
        )

        if args.dry_run:
            print(packet.decode("utf-8", errors="replace"))
        else:
            assert sock is not None
            sock.sendto(packet, (host, port))
            label = "inf" if args.infinite else str(args.count)
            print(
                f"[{i + 1}/{label}] -> {host}:{port}  "
                f"loc={args.location} ip={source_ip} array={array_name} "
                f"sev={severity} refcode={code}",
                flush=True,
            )
            sent += 1

        i += 1
        more_to_go = args.infinite or i < args.count
        if args.interval and more_to_go:
            time.sleep(args.interval)

    if sock is not None:
        sock.close()
        print(f"\nSent {sent} packet(s) to {host}:{port}.")

    return 0


if __name__ == "__main__":
    sys.exit(main())

