#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 3 ]; then
  echo "Usage: $0 <image-repo> <tag> <digest-file>"
  echo "Example: $0 ghcr.io/quantulabs/8004-indexer v1.6.0 docker/digests.yml"
  exit 1
fi

IMAGE_REPO="$1"
TAG="$2"
DIGEST_FILE="$3"

DIGEST="$(docker buildx imagetools inspect "${IMAGE_REPO}:${TAG}" --format '{{.Manifest.Digest}}')"
TIMESTAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

mkdir -p "$(dirname "${DIGEST_FILE}")"

cat > "${DIGEST_FILE}" <<YAML
images:
  repository: ${IMAGE_REPO}
  latest:
    version: "${TAG}"
    digest: "${DIGEST}"
    created_at: "${TIMESTAMP}"
  history:
    - version: "${TAG}"
      digest: "${DIGEST}"
      created_at: "${TIMESTAMP}"
YAML

echo "Recorded digest ${DIGEST} for ${IMAGE_REPO}:${TAG} -> ${DIGEST_FILE}"
