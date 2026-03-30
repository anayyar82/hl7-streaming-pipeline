"""
Run Databricks jobs and DLT pipeline updates from the app (Workspace API).
"""

from __future__ import annotations

import os

import streamlit as st

from utils.theme import apply_theme
from utils.databricks_trigger import (
    parse_job_id,
    trigger_job,
    trigger_pipeline_update,
)

st.set_page_config(page_title="Run Databricks Jobs", page_icon="🚀", layout="wide")
apply_theme()

st.title("Run Databricks jobs")
st.caption(
    "Start **Delta Live Tables** updates and **Workflows** jobs using the app’s Databricks identity. "
    "IDs come from environment variables (set in **Apps → hl7app → Environment** or `app.yaml`)."
)

with st.expander("What this page does — permissions & order", expanded=False):
    st.markdown(
        """
**Purpose**  
Avoid switching to the Jobs UI for routine refreshes: start the **DLT pipeline**, **ML jobs**, and **Lakebase load** with one click each.

**Service principal**  
The app runs as a Databricks **service principal**. It must be granted:
- **Can run** (or equivalent) on each Workflow job you configure below.  
- Permission to **run updates** on the DLT pipeline.

**Typical order (dependencies)**  
1. **Sample data** (optional) — regenerates landing HL7.  
2. **DLT pipeline** — refreshes gold tables in Unity Catalog.  
3. **AutoML training** (occasional) — retrains registered models.  
4. **Model inference** — writes `gold_forecast_predictions` and backfills actuals.  
5. **Lakebase load** — syncs UC gold into Postgres for this app.

Steps **3–5** can run after DLT completes; inference should not rely on stale features. This page **starts** runs asynchronously — it does **not** wait for each step to finish.

**URLs**  
If `DATABRICKS_ORG_ID` is set (workspace org id from your browser URL `?o=...`), links open the correct workspace context.
        """
    )

PIPELINE_ID = (os.environ.get("HL7_PIPELINE_ID") or "").strip()
JOB_SAMPLE = parse_job_id(os.environ.get("HL7_JOB_SAMPLE_DATA"))
JOB_AUTOML = parse_job_id(os.environ.get("HL7_JOB_AUTOML"))
JOB_INFERENCE = parse_job_id(os.environ.get("HL7_JOB_INFERENCE"))
JOB_LAKEBASE = parse_job_id(os.environ.get("HL7_JOB_LAKEBASE_LOAD"))
JOB_SYNC = parse_job_id(os.environ.get("HL7_JOB_LAKEBASE_SYNC"))

missing = []
if not PIPELINE_ID:
    missing.append("`HL7_PIPELINE_ID`")
if JOB_INFERENCE is None:
    missing.append("`HL7_JOB_INFERENCE`")
if JOB_LAKEBASE is None:
    missing.append("`HL7_JOB_LAKEBASE_LOAD`")

if missing:
    st.warning(
        "Set these environment variables on the app, then redeploy or restart: "
        + ", ".join(missing)
        + ". Optional: `HL7_JOB_SAMPLE_DATA`, `HL7_JOB_AUTOML`, `HL7_JOB_LAKEBASE_SYNC`, `DATABRICKS_ORG_ID`."
    )

st.markdown("### Delta Live Tables")

c1, c2 = st.columns(2)
with c1:
    dlt_full = st.checkbox("Full refresh (rebuild pipeline tables — slower)", value=False, key="dlt_full")
with c2:
    st.caption("Use full refresh only when schema or backfill logic changed.")

if st.button("Start DLT pipeline update", type="primary", disabled=not PIPELINE_ID, key="btn_dlt"):
    with st.spinner("Starting pipeline…"):
        r = trigger_pipeline_update(PIPELINE_ID, full_refresh=dlt_full)
    if r.ok:
        st.success(r.message)
        if r.url:
            st.markdown(f"[Open pipeline update in workspace]({r.url})")
    else:
        st.error(r.message)

st.markdown("### Workflows jobs")

rows = [
    ("Sample HL7 data", JOB_SAMPLE, "HL7_JOB_SAMPLE_DATA", "Default params only — use **0a · Sample → volume** to set days/patients/clear."),
    ("AutoML training", JOB_AUTOML, "HL7_JOB_AUTOML", "Retrain models; run when features/hyperparams change."),
    ("Model inference", JOB_INFERENCE, "HL7_JOB_INFERENCE", "Scores models → predictions table; after DLT features exist."),
    ("Lakebase load", JOB_LAKEBASE, "HL7_JOB_LAKEBASE_LOAD", "UC → Postgres for this app; after DLT (and optional inference)."),
    ("Lakebase sync (snapshot)", JOB_SYNC, "HL7_JOB_LAKEBASE_SYNC", "Optional UC mirror job if you use it."),
]

for label, jid, env_key, hint in rows:
    st.markdown(f"**{label}** (`{env_key}`)")
    st.caption(hint)
    bcol1, bcol2 = st.columns([1, 3])
    with bcol1:
        go = st.button("Run", key=f"job_{env_key}", disabled=jid is None)
    if go and jid is not None:
        with st.spinner(f"Starting job {jid}…"):
            r = trigger_job(jid)
        if r.ok:
            st.success(r.message)
            if r.url:
                st.markdown(f"[Open job run]({r.url})")
        else:
            st.error(r.message)
    st.markdown("")

st.markdown("---")
with st.expander("Environment snapshot (IDs loaded by this app)", expanded=False):
    cfg = {
        "HL7_PIPELINE_ID": PIPELINE_ID or "—",
        "HL7_JOB_SAMPLE_DATA": str(JOB_SAMPLE or "—"),
        "HL7_JOB_AUTOML": str(JOB_AUTOML or "—"),
        "HL7_JOB_INFERENCE": str(JOB_INFERENCE or "—"),
        "HL7_JOB_LAKEBASE_LOAD": str(JOB_LAKEBASE or "—"),
        "HL7_JOB_LAKEBASE_SYNC": str(JOB_SYNC or "—"),
        "DATABRICKS_ORG_ID": (os.environ.get("DATABRICKS_ORG_ID") or "—"),
    }
    st.json(cfg)
