"""
Live Databricks activity — active job runs and DLT pipeline state (auto-refresh).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import streamlit as st

from utils.hl7_env import hl7_pipeline_id
from utils.theme import apply_theme
from utils.navigation import render_sidebar_nav
from utils.databricks_activity import (
    collect_active_runs,
    collect_recent_runs,
    configured_job_list,
    get_pipeline_snapshot,
)
from utils.databricks_trigger import pipeline_update_url

st.set_page_config(page_title="Live Activity", page_icon="📡", layout="wide")
apply_theme()
render_sidebar_nav()

PIPELINE_ID = hl7_pipeline_id()

st.title("Live activity")
st.caption(
    "Near–real-time view of **Workflows** and **Delta Live Tables** driven by the Databricks Workspace API. "
    "Data refreshes automatically when your Streamlit version supports it."
)

with st.expander("What this page does", expanded=False):
    st.markdown(
        """
**Purpose**  
See **what is running now** (and what just finished) without opening the Jobs or Pipelines UI.

**Behavior**  
- **DLT**: reads pipeline metadata and the **latest update** state.  
- **Jobs**: lists **active** runs per configured id, plus a **recent history** strip.  

**Latency**  
This is **polling**, not sub-second streaming. Expect **10–20 seconds** between refreshes when auto-refresh is on.

**Permissions**  
The app identity must be allowed to **read** job run history and **view** the pipeline (same workspace).
        """
    )

# Auto-refresh wrapper (Streamlit ≥ ~1.33)
try:
    _frag = st.fragment(run_every=timedelta(seconds=14))
except (TypeError, AttributeError):
    _frag = None


def _render_activity() -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    st.markdown(f"**Last polled:** `{now}`")

    jobs = configured_job_list()

    # ---- DLT ----
    st.subheader("Delta Live Tables pipeline")
    if not PIPELINE_ID:
        st.warning("`HL7_PIPELINE_ID` is not set — pipeline status unavailable.")
    else:
        snap = get_pipeline_snapshot(PIPELINE_ID)
        if snap.error:
            st.error(snap.error)
        else:
            pc1, pc2, pc3, pc4 = st.columns(4)
            pc1.metric("Pipeline", snap.name or "(unnamed)")
            pc2.metric("Catalog state", snap.state or "—")
            pc3.metric("Latest update", snap.latest_update_state or "—")
            pc4.metric("Update id", snap.update_id[:8] + "…" if len(snap.update_id) > 8 else snap.update_id or "—")
            if snap.cluster_id:
                st.caption(f"Cluster: `{snap.cluster_id}`")
            if snap.update_id:
                uurl = pipeline_update_url(PIPELINE_ID, snap.update_id)
                st.markdown(f"[Open this update in workspace]({uurl})")

    st.markdown("---")

    # ---- Active jobs ----
    st.subheader("Active Workflow runs")
    active_df = collect_active_runs(jobs)
    if active_df.empty:
        st.info("No jobs configured or no data returned.")
    else:
        life = active_df["Lifecycle"].astype(str).str.upper()
        busy = life.isin(
            ["RUNNING", "PENDING", "TERMINATING", "WAITING_FOR_RESOURCES", "QUEUED"]
        ).sum()
        st.metric("Jobs with a live run", int(busy))
        st.dataframe(active_df, use_container_width=True, hide_index=True)

    st.markdown("---")

    # ---- Recent history ----
    st.subheader("Recent runs (per job)")
    recent = collect_recent_runs(jobs, limit_per_job=5)
    if recent.empty:
        st.caption("No recent run history.")
    else:
        st.dataframe(recent, use_container_width=True, hide_index=True)


if _frag is not None:

    @_frag
    def _auto_panel() -> None:
        _render_activity()

    _auto_panel()
    st.caption("Auto-refresh ~ every **14 seconds**.")
else:
    _render_activity()
    st.warning(
        "Auto-refresh needs a newer **Streamlit** (`st.fragment` + `run_every`). "
        "Use the button below or upgrade the app environment."
    )
    if st.button("Refresh now", type="primary"):
        st.rerun()

st.markdown("---")
st.page_link("pages/0a_sample_to_volume.py", label="Sample data → volume", icon="📤")
st.page_link("pages/z_run_jobs.py", label="Run Databricks jobs", icon="🚀")
st.page_link("pages/0_status.py", label="System status (Lakebase)", icon="✅")
