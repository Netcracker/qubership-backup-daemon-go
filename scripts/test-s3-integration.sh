#!/usr/bin/env bash
# Run S3 integration tests against a local MinIO instance.
#
# Usage:
#   ./scripts/test-s3-integration.sh            # start minio, run tests, stop minio
#   ./scripts/test-s3-integration.sh --keep      # leave minio running after tests
#   ./scripts/test-s3-integration.sh --no-start  # skip docker compose (minio already running)
#
# Environment variables (with defaults matching docker-compose-minio.yml):
#   MINIO_URL        http://localhost:9000
#   MINIO_ACCESS_KEY minioadmin
#   MINIO_SECRET_KEY minioadmin
#   MINIO_BUCKET     test-bucket
#   MINIO_REGION     us-east-1

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$REPO_ROOT/docker-compose-minio.yml"
DAEMON_DIR="$REPO_ROOT/backup-daemon"

KEEP=false
NO_START=false
for arg in "$@"; do
  case "$arg" in
    --keep)     KEEP=true ;;
    --no-start) NO_START=true ;;
  esac
done

export MINIO_URL="${MINIO_URL:-http://localhost:9000}"
export MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
export MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
export MINIO_BUCKET="${MINIO_BUCKET:-test-bucket}"
export MINIO_REGION="${MINIO_REGION:-us-east-1}"

cleanup() {
  if [[ "$KEEP" == "false" && "$NO_START" == "false" ]]; then
    echo "--- Stopping MinIO ---"
    docker compose -f "$COMPOSE_FILE" down --volumes --remove-orphans 2>/dev/null || true
  fi
}
trap cleanup EXIT

if [[ "$NO_START" == "false" ]]; then
  echo "--- Starting MinIO ---"
  docker compose -f "$COMPOSE_FILE" up -d minio --wait
  echo "--- Creating test bucket ---"
  docker compose -f "$COMPOSE_FILE" --profile init run --rm minio-init
fi

echo "--- MinIO endpoint: $MINIO_URL  bucket: $MINIO_BUCKET ---"
echo "--- Running S3 integration tests ---"

cd "$DAEMON_DIR"
go test -v -count=1 -tags integration -timeout 180s ./app/utils/... ./app/tasks/... ./app/controller/...

echo "--- All integration tests passed ---"
