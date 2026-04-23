"""
HL7 app environment values — one place for IDs that many pages use.

Set in Databricks Apps (or `app.yaml`): `HL7_PIPELINE_ID` = DLT pipeline from the bundle
`resources.pipelines.hl7_streaming_dlt` after `databricks bundle deploy -t dev` (see
`databricks bundle summary -t dev` → Pipelines).
"""

from __future__ import annotations

import os


def hl7_pipeline_id() -> str:
    """DLT / Lakeflow pipeline id for this workspace (not the bundle resource name)."""
    return (os.environ.get("HL7_PIPELINE_ID") or "").strip()
