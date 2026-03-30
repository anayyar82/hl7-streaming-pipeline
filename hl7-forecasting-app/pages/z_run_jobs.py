"""
Run Databricks jobs and DLT pipeline updates from the app (Workspace API).
"""

from __future__ import annotations

import os
from datetime import timedelta

import pandas as pd
import streamlit as st

from utils.theme import apply_theme
from utils.navigation import render_sidebar_nav
from utils.databricks_trigger import (
    job_run_url,
    parse_job_id,
    trigger_job,
    trigger_pipeline_update,
)
from utils.workflow_progress import format_task_completion_line, summarize_workflow_run

st.set_page_config(page_title="Run Databricks Jobs", page_icon="🚀", layout="wide")
apply_theme()
render_sidebar_nav()

st.title("Run Databricks jobs")
st.caption(
    "Start **Delta Live Tables** updates and **Workflows** jobs using the app’s Databricks identity. "
    "IDs come from environment variables (set in **Apps → hl7app → Environment** or `app.yaml`)."
)

with st.expander("What this page does — permissions & order", expanded=False):
    st.markdown(
        """
**Purpose**  
Avoid switching to the Jobs UI for routine refreshes. Prefer the **bundled workflow** job (DLT → inference → Lakebase) for one click; or start **individual** jobs / DLT below.

**Service principal**  
The app runs as a Databricks **service principal**. It must be granted:
- **Can run** on the bundled workflow job (`HL7_JOB_REFRESH_WORKFLOW`) and on each child job it invokes.  
- **Can run** on any standalone job you trigger here.  
- Permission to **run updates** on the DLT pipeline (for standalone DLT button and for the workflow’s pipeline task).

**Bundled workflow**  
The bundle deploys a multi-task job: **DLT refresh** → **model inference** → **Lakebase load**. The app polls the run and shows **which task is active**, **step completion %**, and **DLT update metrics** when the API exposes them.

**Typical order (if not using the bundle)**  
1. **Sample data** (optional) — regenerates landing HL7.  
2. **DLT pipeline** — refreshes gold tables in Unity Catalog.  
3. **AutoML training** (occasional) — retrains registered models.  
4. **Model inference** — writes `gold_forecast_predictions` and backfills actuals.  
5. **Lakebase load** — syncs UC gold into Postgres for this app.

Standalone buttons **start** runs asynchronously — they do **not** wait for completion. The **workflow monitor** auto-refreshes while you keep the page open (Streamlit ≥ 1.33).

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
JOB_REFRESH_WORKFLOW = parse_job_id(os.environ.get("HL7_JOB_REFRESH_WORKFLOW"))

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
        + ". Optional: `HL7_JOB_SAMPLE_DATA`, `HL7_JOB_AUTOML`, `HL7_JOB_LAKEBASE_SYNC`, "
        "`HL7_JOB_REFRESH_WORKFLOW`, `DATABRICKS_ORG_ID`."
    )

if JOB_REFRESH_WORKFLOW is None:
    st.info(
        "Optional: after `databricks bundle deploy`, find the job **HL7 DLT → Inference → Lakebase**, "
        "copy its numeric **Job ID** into app env **`HL7_JOB_REFRESH_WORKFLOW`**, then redeploy the app."
    )

st.markdown("### Bundled workflow (DLT → inference → Lakebase)")
st.caption(
    "Runs the **multi-task** job from the bundle: refreshes the DLT pipeline, then **model inference**, "
    "then **Lakebase load**. Progress uses **completed tasks / total tasks**; DLT rows/bytes appear when the Pipelines API returns them."
)

_wf_btn_col, _wf_clr_col = st.columns([1, 1])
with _wf_btn_col:
    start_wf = st.button(
        "Run bundled workflow",
        type="primary",
        disabled=JOB_REFRESH_WORKFLOW is None,
        key="btn_refresh_workflow",
    )
with _wf_clr_col:
    if st.button("Clear workflow monitor", key="btn_wf_clear"):
        st.session_state.pop("hl7_wf_run_id", None)
        st.session_state.pop("hl7_wf_job_id", None)
        st.rerun()

if start_wf and JOB_REFRESH_WORKFLOW is not None:
    with st.spinner("Starting workflow…"):
        r = trigger_job(JOB_REFRESH_WORKFLOW)
    if r.ok and r.run_id is not None:
        st.session_state["hl7_wf_run_id"] = r.run_id
        st.session_state["hl7_wf_job_id"] = JOB_REFRESH_WORKFLOW
        st.success(r.message)
        if r.url:
            st.markdown(f"[Open run in workspace]({r.url})")
    elif r.ok:
        st.success(r.message)
    else:
        st.error(r.message)

try:
    _wf_frag = st.fragment(run_every=timedelta(seconds=6))
except (TypeError, AttributeError):
    _wf_frag = None


def _render_workflow_monitor() -> None:
    rid = st.session_state.get("hl7_wf_run_id")
    jid = st.session_state.get("hl7_wf_job_id")
    if not rid or not jid:
        st.caption("No active workflow run in this session — click **Run bundled workflow** to track progress here.")
        return
    s = summarize_workflow_run(int(rid))
    if s.error:
        st.error(s.error)
        return
    m1, m2, m3 = st.columns(3)
    m1.metric("Run state", s.life_cycle_state or "—")
    m2.metric("Result", s.result_state or "—")
    m3.metric("Steps done", f"{s.tasks_finished} / {s.tasks_total}")
    pct = max(0.0, min(100.0, s.progress_pct))
    st.progress(pct / 100.0)
    st.markdown(
        f"**Workflow progress:** {pct:.0f}% — **{s.tasks_finished}** of **{s.tasks_total}** tasks finished."
    )
    st.markdown(format_task_completion_line(s))
    if s.life_cycle_state == "TERMINATED":
        st.success("This workflow run has finished — see Result above and per-task Notes in the table.")
    if s.state_message and s.life_cycle_state == "TERMINATED":
        st.caption(s.state_message[:400])
    if s.tasks:
        tbl = pd.DataFrame(
            [
                {
                    "Task": t.task_key,
                    "Type": t.kind,
                    "Lifecycle": t.lifecycle,
                    "Result": t.result,
                    "Notes": (t.detail or t.state_message or "")[:500],
                }
                for t in s.tasks
            ]
        )
        st.dataframe(tbl, use_container_width=True, hide_index=True)
    st.markdown(f"[Job run in workspace]({job_run_url(int(jid), int(rid))})")


if _wf_frag is not None:

    @_wf_frag
    def _wf_auto_panel() -> None:
        _render_workflow_monitor()

    _wf_auto_panel()
    st.caption("Workflow monitor auto-refreshes about every **6 seconds**.")
else:
    _render_workflow_monitor()
    if st.session_state.get("hl7_wf_run_id"):
        st.warning(
            "Upgrade **Streamlit** to enable `st.fragment(run_every=…)` for automatic refresh; "
            "use **Refresh** below after each task advances."
        )
        if st.button("Refresh workflow status", type="primary", key="wf_manual_refresh"):
            st.rerun()

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
        "HL7_JOB_REFRESH_WORKFLOW": str(JOB_REFRESH_WORKFLOW or "—"),
        "DATABRICKS_ORG_ID": (os.environ.get("DATABRICKS_ORG_ID") or "—"),
    }
    st.json(cfg)
