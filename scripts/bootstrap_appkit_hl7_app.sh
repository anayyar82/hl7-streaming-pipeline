#!/usr/bin/env bash
# Scaffold guidance for a second HL7 UI on Databricks AppKit (Node + React).
# Official SDK: https://github.com/databricks/appkit
# Docs: https://databricks.github.io/appkit/docs/
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TARGET="${ROOT}/hl7-appkit-app"

print_bundle_snippet() {
  cat <<'EOF'
# The main repo bundle cannot list two git-sourced apps (Duplicate app source code path).
# Use the dedicated bundle: bundles/hl7_appkit/ — `cd bundles/hl7_appkit && databricks bundle deploy -t dev`
# The resource is already in bundles/hl7_appkit/resources/hl7_appkit_app_resource.yml; edit `variables` there for Git URL/branch.
EOF
}

usage() {
  echo "Usage: $0 [--print-bundle-snippet | --init-hint]"
  echo ""
  echo "  --print-bundle-snippet   Print a pointer to bundles/hl7_appkit/ (separate app bundle; no paste into main YAML)."
  echo "  --init-hint              Show the recommended bootstrap commands (no files changed)."
  echo ""
  echo "Prerequisites: Node.js 22+, Databricks CLI 0.295+ (https://docs.databricks.com/aws/en/dev-tools/cli/tutorial)."
}

case "${1:-}" in
  --print-bundle-snippet)
    print_bundle_snippet
    exit 0
    ;;
  --init-hint)
    echo "Recommended (official AppKit quick start):"
    echo "  mkdir -p \"$TARGET\" && cd \"$TARGET\""
    echo "  databricks apps init"
    echo ""
    echo "Follow prompts (Lakebase / Analytics / Genie plugins as needed). Then:"
    echo "  npm run build"
    echo "  Copy Lakebase and Genie settings from hl7-forecasting-app/app.yaml into the App env (.env locally; app.yaml or Apps UI in workspace)."
    echo "  Commit hl7-appkit-app/, then from repo:  cd bundles/hl7_appkit && databricks bundle deploy -t dev"
    echo "  databricks apps deploy hl7app-appkit --source-code-path \"$TARGET\"   # if not using Git-only redeploy"
    exit 0
    ;;
  -h|--help|"")
    usage
    exit 0
    ;;
  *)
    echo "Unknown option: $1" >&2
    usage >&2
    exit 1
    ;;
esac
