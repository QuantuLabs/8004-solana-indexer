#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <image-repo> <tag> [cosign-key]"
  echo "Example: $0 ghcr.io/quantulabs/8004-indexer v1.6.0"
  exit 1
fi

IMAGE_REPO="$1"
TAG="$2"
COSIGN_KEY="${3:-}"
IMAGE_REF="${IMAGE_REPO}:${TAG}"

DIGEST="$(docker buildx imagetools inspect "${IMAGE_REF}" --format '{{.Manifest.Digest}}')"
PINNED_REF="${IMAGE_REPO}@${DIGEST}"

echo "Image: ${IMAGE_REF}"
echo "Digest: ${DIGEST}"
echo "Pinned: ${PINNED_REF}"

if command -v syft >/dev/null 2>&1; then
  mkdir -p artifacts/sbom
  syft "${PINNED_REF}" -o json > "artifacts/sbom/${TAG}.json"
  echo "SBOM written to artifacts/sbom/${TAG}.json"
else
  echo "syft not found: skipping SBOM generation"
fi

if command -v cosign >/dev/null 2>&1; then
  if [ -n "${COSIGN_KEY}" ]; then
    cosign verify --key "${COSIGN_KEY}" "${PINNED_REF}"
  else
    cosign verify "${PINNED_REF}"
  fi
  echo "Cosign verification completed"
else
  echo "cosign not found: skipping signature verification"
fi
