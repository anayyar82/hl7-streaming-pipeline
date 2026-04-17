#!/usr/bin/env bash
# Run HL7 bundle jobs in dependency order with a full DLT graph refresh.
# Usage (from repo root):
#   ./scripts/run_full_stack_full_refresh.sh [dev|staging|prod]
#
# Steps: sample data → DLT (full-refresh-all) → AutoML → inference → Lakebase load.
# AutoML is slow; skip with:  SKIP_AUTOML=1 ./scripts/run_full_stack_full_refresh.sh
#
# Requires: databricks CLI authenticated for the workspace profile in databricks.yml

set -euo pipefail
TARGET="${1:-dev}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "Target: $TARGET"
echo "==> 1/5 hl7_sample_data (regenerate landing HL7)"
databricks bundle run hl7_sample_data -t "$TARGET"

echo "==> 2/5 hl7_streaming_dlt (full graph recompute — slower than incremental)"
databricks bundle run hl7_streaming_dlt -t "$TARGET" --full-refresh-all

if [[ "${SKIP_AUTOML:-}" == "1" ]]; then
  echo "==> 3/5 hl7_automl_training SKIPPED (SKIP_AUTOML=1)"
else
  echo "==> 3/5 hl7_automl_training (ephemeral ML cluster; may take several minutes)"
  databricks bundle run hl7_automl_training -t "$TARGET"
fi

echo "==> 4/5 hl7_model_inference"
databricks bundle run hl7_model_inference -t "$TARGET"

echo "==> 5/5 hl7_lakebase_load"
databricks bundle run hl7_lakebase_load -t "$TARGET"

echo "Full stack finished."
