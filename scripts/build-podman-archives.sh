#!/usr/bin/env bash
# =============================================================================
#  build-podman-archives.sh
#  Builds the Podman images for UnifiedOps and exports them as .tar archives
#  for airgapped deployment.
#  Run this on a connected build box (RHEL 9).
# =============================================================================
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> Building UI & Server Image"
podman build -t localhost/unifiedops-ui:latest -f deploy/podman/ui.Containerfile .

echo "==> Building Listener Image"
podman build -t localhost/unifiedops-listener:latest -f deploy/podman/listener.Containerfile .

mkdir -p offline-bundles/podman-archives

echo "==> Saving UI Image to tar archive"
podman save -o offline-bundles/podman-archives/unifiedops-ui.tar localhost/unifiedops-ui:latest

echo "==> Saving Listener Image to tar archive"
podman save -o offline-bundles/podman-archives/unifiedops-listener.tar localhost/unifiedops-listener:latest

echo "==> Done. Archives are in offline-bundles/podman-archives/"
