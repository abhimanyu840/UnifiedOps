#!/usr/bin/env bash
# =============================================================================
#  UnifiedOpsv2 — airgapped install
#
#  Run on the UI RHEL VM after unpacking UnifiedOpsv2.zip into /opt/unifiedops.
#  Steps:
#    1. pip install from the bundled offline wheel cache using system python
#    2. Drop systemd unit files into /etc/systemd/system
#    3. systemctl daemon-reload
#
#  Idempotent: re-running is safe.
# =============================================================================
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/unifiedops}"
UNIT_DIR="${UNIT_DIR:-/etc/systemd/system}"
PYTHON_BIN="/usr/bin/python"

if [[ "${EUID}" -ne 0 ]]; then
    echo "ERR: run as root (sudo)" >&2
    exit 1
fi

if [[ ! -d "$APP_DIR" ]]; then
    echo "ERR: $APP_DIR does not exist; unpack UnifiedOpsv2.zip there first" >&2
    exit 1
fi

echo "==> creating user/group 'unifiedops' if missing"
if ! id -u unifiedops >/dev/null 2>&1; then
    useradd --system --no-create-home --shell /sbin/nologin unifiedops
fi
chown -R unifiedops:unifiedops "$APP_DIR"

WHEELS_DIR="$APP_DIR/offline/pip-wheels"
if [[ -d "$WHEELS_DIR" ]]; then
    echo "==> installing from offline wheel cache ($WHEELS_DIR)"
    "$PYTHON_BIN" -m pip install \
        --no-index \
        --find-links "$WHEELS_DIR" \
        -r "$APP_DIR/requirements.txt"
else
    echo "==> wheel cache missing — falling back to PyPI (needs internet!)"
    "$PYTHON_BIN" -m pip install -r "$APP_DIR/requirements.txt"
fi

echo "==> log + state dirs"
install -d -o unifiedops -g unifiedops /var/log/unifiedops /var/lib/unifiedops

echo "==> systemd unit files"
SERVICES=(
    "unifiedops-ui-server"
    "unifiedops-listener-hitachi-bcp"
    "unifiedops-listener-hitachi-cdvl"
    "unifiedops-listener-hitachi-sify"
    "unifiedops-listener-brcd-bcp-uat"
    "unifiedops-listener-brcd-cdvl-sify"
    "unifiedops-listener-dell-bcp"
    "unifiedops-listener-dell-cdvl"
    "unifiedops-listener-dell-sify"
    "unifiedops-listener-netapp-bcp"
    "unifiedops-listener-netapp-cdvl"
    "unifiedops-listener-netapp-sify"
)

for u in "${SERVICES[@]}"; do
    src="$APP_DIR/deploy/${u}.service"
    if [[ -f "$src" ]]; then
        install -m 0644 "$src" "$UNIT_DIR/${u}.service"
        echo "   installed $UNIT_DIR/${u}.service"
    fi
done

echo "==> daemon-reload"
systemctl daemon-reload

cat <<EOF

=== UnifiedOpsv2 installed ===

App:   $APP_DIR
Logs:  /var/log/unifiedops

To configure and enable a service on this VM:
    sudo systemctl edit --full <service-name>
    sudo systemctl enable --now <service-name>

Available Listener Services (Deploy only the ones assigned to this VM!):
  Hitachi:  unifiedops-listener-hitachi-bcp, unifiedops-listener-hitachi-cdvl, unifiedops-listener-hitachi-sify
  Brocade:  unifiedops-listener-brcd-bcp-uat, unifiedops-listener-brcd-cdvl-sify
  Dell:     unifiedops-listener-dell-bcp, unifiedops-listener-dell-cdvl, unifiedops-listener-dell-sify
  NetApp:   unifiedops-listener-netapp-bcp, unifiedops-listener-netapp-cdvl, unifiedops-listener-netapp-sify
EOF
