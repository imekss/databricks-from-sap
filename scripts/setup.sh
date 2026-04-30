#!/usr/bin/env bash
# N3N3 — First-time setup script.
#
# Usage:
#   ./scripts/setup.sh [dev|prod]
#
# Examples:
#   ./scripts/setup.sh          # defaults to dev
#   ./scripts/setup.sh dev      # explicit dev target
#   ./scripts/setup.sh prod     # prod target
#
# What it does:
#   1. Installs Python dependencies
#   2. Deploys the bundle to the specified target
#   3. Creates the bronze schema (if it doesn't exist)
#   4. Creates the raw_data volume (if it doesn't exist)
#   5. Uploads CSVs from data/raw/ to the volume
#   6. Runs the full medallion pipeline (bronze → silver → gold)
#
# Prerequisites:
#   - Databricks CLI installed and authenticated
#   - uv installed

set -e

# Parse target argument (default: dev)
TARGET="${1:-dev}"
if [ "$TARGET" != "dev" ] && [ "$TARGET" != "prod" ]; then
  echo "Error: target must be 'dev' or 'prod' (got '$TARGET')"
  exit 1
fi

CATALOG="n3n3"

# Determine schema name based on target
if [ "$TARGET" = "dev" ]; then
  USERNAME=$(databricks current-user me --output json | python3 -c "
import sys, json
email = json.load(sys.stdin)['userName']
print(email.split('@')[0].replace('.', '_'))
")
  SCHEMA="${USERNAME}_bronze"
  DISPLAY_USER="${USERNAME}"
else
  SCHEMA="bronze"
  DISPLAY_USER="(prod — shared)"
fi

VOLUME_NAME="raw_data"
VOLUME_PATH="/Volumes/${CATALOG}/${SCHEMA}/${VOLUME_NAME}"

echo "=== N3N3 Setup ==="
echo "Target: ${TARGET}"
echo "User: ${DISPLAY_USER}"
echo "Volume: ${VOLUME_PATH}"
echo ""

# 1. Install dependencies
echo "→ Installing dependencies..."
uv sync --dev

# 2. Deploy bundle
echo "→ Deploying bundle..."
databricks bundle deploy -t "${TARGET}"

# 3. Create schema if needed
echo "→ Ensuring schema exists..."
databricks schemas create "${SCHEMA}" "${CATALOG}" 2>/dev/null && \
  echo "  ✓ Schema created" || \
  echo "  ✓ Schema already exists"

# 4. Create volume if needed
echo "→ Ensuring volume exists..."
databricks volumes create "${CATALOG}" "${SCHEMA}" "${VOLUME_NAME}" MANAGED 2>/dev/null && \
  echo "  ✓ Volume created" || \
  echo "  ✓ Volume already exists"

# 5. Upload CSVs
echo "→ Uploading CSVs to volume..."
databricks fs cp -r data/raw/ "dbfs:${VOLUME_PATH}/" --overwrite
echo "  ✓ CSVs uploaded"

# 6. Run pipeline
echo "→ Running full medallion refresh..."
databricks bundle run n3n3_full_refresh -t "${TARGET}"

echo ""
echo "=== Done! ==="
if [ "$TARGET" = "dev" ]; then
  echo "P&L mart: ${CATALOG}.${USERNAME}_gold.profit_and_loss"
else
  echo "P&L mart: ${CATALOG}.gold.profit_and_loss"
fi