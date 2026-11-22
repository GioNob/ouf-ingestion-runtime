#!/usr/bin/env bash
set -euo pipefail

# GitHub's release asset replaces registry images that are no longer public.
# Keep the exact release used by the acceptance job and verify its asset digest.
image=ouf-ingestion-minio-ci:RELEASE.2025-09-07T16-13-09Z
build_dir="$(mktemp -d "${RUNNER_TEMP:-/tmp}/ouf-ingestion-minio-image.XXXXXX")"
trap 'rm -rf "$build_dir"' EXIT
curl --fail --location --retry 3 --max-time 240 \
  --output "$build_dir/minio" \
  https://github.com/minio/minio/releases/download/RELEASE.2025-09-07T16-13-09Z/minio.linux-amd64.RELEASE.2025-09-07T16-13-09Z
printf '%s  %s\n' \
  7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f \
  "$build_dir/minio" | sha256sum --check --status
chmod 755 "$build_dir/minio"
cat >"$build_dir/Dockerfile" <<'EOF'
FROM scratch
COPY minio /minio
ENTRYPOINT ["/minio"]
EOF
docker build --quiet --tag "$image" "$build_dir" >/dev/null
printf '%s\n' "$image"
