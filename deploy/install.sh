#!/usr/bin/env bash
# =============================================================================
#  UnifiedOpsv2 — airgapped install
#
#  Run on the UI RHEL VM after unpacking UnifiedOpsv2.zip into /opt/unifiedops.
#  Steps:
#    1. Create a Python 3.9 venv at /opt/unifiedops/.venv
#    2. pip install from the bundled offline wheel cache (no internet needed)
#    3. Drop systemd unit files into /etc/systemd/system
#    4. Drop the env example into /etc/unifiedops/ if missing
#    5. systemctl daemon-reload
#
#  Idempotent: re-running is safe.
# =============================================================================
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/unifiedops}"
VENV_DIR="$APP_DIR/.venv"
ETC_DIR="${ETC_DIR:-/etc/unifiedops}"
UNIT_DIR="${UNIT_DIR:-/etc/systemd/system}"

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

echo "==> Python venv ($VENV_DIR)"
if [[ ! -d "$VENV_DIR" ]]; then
    python3.9 -m venv "$VENV_DIR"
fi
"$VENV_DIR/bin/python" -m pip install --quiet --upgrade pip

WHEELS_DIR="$APP_DIR/offline/pip-wheels"
if [[ -d "$WHEELS_DIR" ]]; then
    echo "==> installing from offline wheel cache ($WHEELS_DIR)"
    "$VENV_DIR/bin/pip" install \
        --no-index \
        --find-links "$WHEELS_DIR" \
        -r "$APP_DIR/requirements.txt"
else
    echo "==> wheel cache missing — falling back to PyPI (needs internet!)"
    "$VENV_DIR/bin/pip" install -r "$APP_DIR/requirements.txt"
fi

echo "==> log + state dirs"
install -d -o unifiedops -g unifiedops /var/log/unifiedops /var/lib/unifiedops

echo "==> $ETC_DIR (env file location)"
install -d -m 0750 -o unifiedops -g unifiedops "$ETC_DIR"
if [[ ! -f "$ETC_DIR/ui.env" ]]; then
    install -m 0640 -o unifiedops -g unifiedops \
        "$APP_DIR/deploy/unifiedops-ui.env.example" "$ETC_DIR/ui.env"
    echo "   wrote $ETC_DIR/ui.env (copy of example) — EDIT TOKENS BEFORE STARTING SERVICES"
fi

echo "==> systemd unit files"
for u in unifiedops-ui-server unifiedops-listener-brcd-bcp-uat unifiedops-listener-brcd-cdvl-sify; do
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
Env:   $ETC_DIR/ui.env       <-- EDIT before starting UI server
Logs:  /var/log/unifiedops

Next (UI VM):
    sudo \$EDITOR $ETC_DIR/ui.env
    sudo systemctl enable --now unifiedops-ui-server

Next (Listener VM - BCP + UAT):
    sudo systemctl edit --full unifiedops-listener-brcd-bcp-uat  # Edit Environment variables in service
    sudo systemctl enable --now unifiedops-listener-brcd-bcp-uat

Next (Listener VM - CDVL + SIFY):
    sudo systemctl edit --full unifiedops-listener-brcd-cdvl-sify  # Edit Environment variables in service
    sudo systemctl enable --now unifiedops-listener-brcd-cdvl-sify

Logs:
    sudo journalctl -u unifiedops-listener-brcd-bcp-uat -f
    sudo journalctl -u unifiedops-listener-brcd-cdvl-sify -f
EOF


