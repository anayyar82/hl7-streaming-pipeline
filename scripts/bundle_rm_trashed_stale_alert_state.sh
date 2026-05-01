#!/usr/bin/env bash
# Remove the hl7_gold_message_freshness_stale alert from local Terraform state when the workspace object
# was moved to Trash (lifecycle_state DELETED, parent_path .../Trash). Databricks does not allow
# updating parent_path off Trash; state rm + redeploy recreates the alert under parent_path in
# resources/hl7_sql_alerts.yml.
#
# Usage (from repo root):
#   ./scripts/bundle_rm_trashed_stale_alert_state.sh [dev|staging|prod]
#
# Requires: prior `databricks bundle deploy` or `bundle validate` so .databricks/bundle/<target>/terraform exists.

set -euo pipefail
TARGET="${1:-dev}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TF_DIR="${ROOT}/.databricks/bundle/${TARGET}/terraform"
TF_BIN="${ROOT}/.databricks/bundle/${TARGET}/bin/terraform"

if [[ ! -d "$TF_DIR" ]]; then
  echo "ERROR: $TF_DIR not found. Run: databricks bundle validate -t $TARGET" >&2
  exit 1
fi

cd "$ROOT"
if [[ -x "$TF_BIN" ]]; then
  TF=( "$TF_BIN" )
else
  TF=( terraform )
fi

ADDR='databricks_alert_v2.hl7_gold_message_freshness_stale'
if ! "${TF[@]}" -chdir="$TF_DIR" state show "$ADDR" &>/dev/null; then
  echo "State has no $ADDR — nothing to remove."
  exit 0
fi

echo "Removing $ADDR from Terraform state under $TF_DIR ..."
"${TF[@]}" -chdir="$TF_DIR" state rm "$ADDR"
echo "Done. Recreate the alert with: databricks bundle deploy -t $TARGET"
