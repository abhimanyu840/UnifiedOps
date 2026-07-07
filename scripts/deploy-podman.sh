#!/usr/bin/env bash
# =============================================================================
#  deploy-podman.sh
#  Loads Podman image archives and installs systemd Quadlet container units
#  on an airgapped RHEL 9 VM.
# =============================================================================
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/unifiedops}"
QUADLET_DIR="/etc/containers/systemd"

if [[ "${EUID}" -ne 0 ]]; then
    echo "ERR: run as root (sudo)" >&2
    exit 1
fi

if [[ ! -d "$APP_DIR" ]]; then
    echo "ERR: $APP_DIR does not exist; unpack UnifiedOpsv2.zip there first" >&2
    exit 1
fi

echo "==> Loading Podman images"
ARCHIVE_DIR="$APP_DIR/offline-bundles/podman-archives"
if [[ -d "$ARCHIVE_DIR" ]]; then
    for tarball in "$ARCHIVE_DIR"/*.tar; do
        if [[ -f "$tarball" ]]; then
            echo "   Loading $tarball..."
            podman load -i "$tarball"
        fi
    done
else
    echo "ERR: $ARCHIVE_DIR missing. Did you run build-podman-archives.sh?" >&2
    exit 1
fi

echo "==> Installing systemd Quadlets"
mkdir -p "$QUADLET_DIR"

CONTAINERS=(
    "hi-track-ui"
)

for c in "${CONTAINERS[@]}"; do
    src="$APP_DIR/deploy/podman/${c}.container"
    if [[ -f "$src" ]]; then
        install -m 0644 "$src" "$QUADLET_DIR/${c}.container"
        echo "   installed $QUADLET_DIR/${c}.container"
    fi
done

echo "==> daemon-reload"
systemctl daemon-reload

cat <<EOF

=== UnifiedOps Podman Containers Installed ===

To configure and enable a container on this VM:
    sudo systemctl enable --now hi-track-ui

EOF
