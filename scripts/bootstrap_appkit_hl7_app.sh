#!/usr/bin/env bash
# Scaffold guidance for a second HL7 UI on Databricks AppKit (Node + React).
# Official SDK: https://github.com/databricks/appkit
# Docs: https://databricks.github.io/appkit/docs/
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TARGET="${ROOT}/hl7-appkit-app"

print_bundle_snippet() {
  cat <<'EOF'
# --- Paste under `resources:` → `apps:` (e.g. in resources/hl7_pipeline.yml), then `databricks bundle deploy`.
# Requires directory hl7-appkit-app/ in this repo (from `databricks apps init`) committed and built for deploy.
# Match env vars to hl7-forecasting-app/app.yaml (Lakebase, Genie, jobs, UC) and add plugin-specific vars from AppKit docs.
    hl7app_appkit:
      name: hl7app_appkit
      description: "HL7 ED/ICU — AppKit (Node/React); migrate features from Streamlit incrementally"
      git_repository:
        provider: ${var.app_git_provider}
        url: ${var.app_git_url}
      git_source:
        branch: ${var.app_git_branch}
        source_code_path: hl7-appkit-app
      user_api_scopes:
        - sql
        - dashboards.genie
      permissions:
        - level: CAN_USE
          group_name: users
EOF
}

usage() {
  echo "Usage: $0 [--print-bundle-snippet | --init-hint]"
  echo ""
  echo "  --print-bundle-snippet   Print YAML to register a second Git-based Databricks App (hl7app_appkit)."
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
    echo "  Commit hl7-appkit-app/, add the bundle snippet from:  $0 --print-bundle-snippet"
    echo "  databricks bundle deploy -t dev"
    echo "  databricks apps deploy hl7app_appkit --source-code-path \"$TARGET\"   # if not using Git-only redeploy"
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
