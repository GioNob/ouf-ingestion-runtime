#!/usr/bin/env bash
set -euo pipefail

if ! [[ ${1:-} =~ ^[a-f0-9]{7,40}$ ]]; then
  echo 'Expected an immutable predecessor commit SHA' >&2
  exit 2
fi
previous=$(git rev-parse --verify "$1^{commit}")
[[ "$previous" == "$1"* ]] || { echo 'Predecessor did not resolve to the requested SHA' >&2; exit 2; }
candidate=$(git rev-parse --verify 'HEAD^{commit}')
scratch=$(mktemp -d "${RUNNER_TEMP:-/tmp}/ingestion-return.XXXXXXXX")
name=$(basename "$scratch")
cleanup() {
  docker rm -f "$name-old" "$name-new" "$name-next" >/dev/null 2>&1 || true
  rm -rf -- "$scratch"
}
trap cleanup EXIT

build() {
  local role=$1 revision=$2 id label
  git archive --format=tar "$revision" | docker build --iidfile "$scratch/$role.id" \
    --label "org.opencontainers.image.revision=$revision" -
  id=$(cat "$scratch/$role.id")
  label=$(docker image inspect "$id" --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')
  [[ "$label" == "$revision" ]] || { echo "$role image revision mismatch" >&2; return 1; }
  echo "$role revision=$revision image=$id"
  git ls-tree --name-only "$revision:src/main/resources/db/migration" | \
    grep -E '^V.+__.+\.sql$' | LC_ALL=C sort > "$scratch/$role.migrations"
}
build old "$previous"
build new "$candidate"
node scripts/ci/predecessor-scenario.mjs "$scratch" "$name"
