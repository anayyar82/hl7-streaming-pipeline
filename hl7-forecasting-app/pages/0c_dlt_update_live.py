"""
Live DLT / Lakeflow pipeline update monitor — per-flow status and row metrics from event log.
"""

from __future__ import annotations

from datetime import timedelta

import streamlit as st

from utils.hl7_env import hl7_pipeline_id
from utils.databricks_activity import get_pipeline_snapshot
from utils.databricks_trigger import pipeline_update_url
from utils.dlt_live_monitor import (
    build_dlt_update_kpi_html,
    events_to_dataframes,
    fetch_pipeline_events,
    fetch_update_row,
)
from utils.navigation import render_sidebar_nav
from utils.theme import apply_theme

st.set_page_config(page_title="DLT update live", page_icon="🔄", layout="wide")
apply_theme()
render_sidebar_nav()

PIPELINE_ID = hl7_pipeline_id()

st.title("DLT update — live monitor")
st.caption(
    "Polls **Pipelines API** (`get_update` + **event log**). "
    "`flow_progress` events carry per-flow **status** (RUNNING, COMPLETED, …) and **row metrics** when Databricks emits them."
)

with st.expander("What this page does", expanded=False):
    st.markdown(
        """
**Purpose**  
After you start a pipeline update (from **Run jobs** or the workspace), watch **update state** and a **flow-level** table derived from `flow_progress` events.

**Real time**  
This is **polling** (default ~5s when auto-refresh is on), not a websocket. Latency is a few seconds behind the workspace UI.

**Permissions**  
The app identity needs permission to **read** the pipeline and **list pipeline events** (same as viewing the pipeline in the workspace).

**Limits**  
The events API returns a **bounded page** (max 100 per request). Very chatty pipelines may rotate older events out of the window.

Docs: [Pipeline event log schema](https://docs.databricks.com/aws/en/ldp/monitor-event-log-schema)
        """
    )

if not PIPELINE_ID:
    st.error("Set **`HL7_PIPELINE_ID`** on the app (see `app.yaml`).")
    st.stop()

snap = get_pipeline_snapshot(PIPELINE_ID)
if snap.error:
    st.error(snap.error)
    st.stop()

default_uid = (snap.update_id or "").strip()

c1, c2, c3 = st.columns([2, 2, 1])
with c1:
    st.text_input(
        "Update id",
        value=default_uid,
        help="From the latest pipeline update URL or from **Run jobs** after starting DLT.",
        key="dlt_mon_update_id",
    )
with c2:
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("Sync update id from pipeline", help="Pull latest update id from pipeline metadata"):
        st.session_state["dlt_mon_update_id"] = default_uid
        st.rerun()
with c3:
    auto = st.toggle("Auto-refresh ~5s", value=True, key="dlt_mon_auto")

try:
    _frag = st.fragment(run_every=timedelta(seconds=5))
except (TypeError, AttributeError):
    _frag = None


def _panel() -> None:
    uid = (st.session_state.get("dlt_mon_update_id") or "").strip()
    if not uid:
        st.warning("No **update id** yet — start a DLT update, then **Sync** or paste the id from the pipeline URL.")
        if snap.update_id:
            st.caption(f"Latest known from API: `{snap.update_id}`")
        return

    urow = fetch_update_row(PIPELINE_ID, uid)
    if urow.get("error"):
        st.error(urow["error"])
    else:
        st.markdown(build_dlt_update_kpi_html(urow), unsafe_allow_html=True)
        if urow.get("cause"):
            st.caption(str(urow["cause"])[:400])

    events = fetch_pipeline_events(PIPELINE_ID, max_results=100, update_id_hint=uid)
    flow_df, ev_df = events_to_dataframes(events)

    st.subheader("Flows (from flow_progress events)")
    if flow_df.empty:
        st.info(
            "No **flow_progress** rows in the current event window. "
            "The update may still be **QUEUED** / **INITIALIZING**, or events have scrolled off — try again in a few seconds."
        )
    else:
        st.dataframe(flow_df, use_container_width=True, hide_index=True)

    with st.expander("Recent pipeline events (compact)", expanded=False):
        if ev_df.empty:
            st.caption("No events returned — check API permissions.")
        else:
            st.dataframe(ev_df, use_container_width=True, hide_index=True)

    st.markdown(f"[Open this update in workspace]({pipeline_update_url(PIPELINE_ID, uid)})")
    st.page_link("pages/z_run_jobs.py", label="Run Databricks jobs (start DLT)", icon="🚀")


if auto and _frag is not None:

    @_frag
    def _auto() -> None:
        _panel()

    _auto()
elif auto and _frag is None:
    st.warning("Install **Streamlit ≥ 1.33** with `st.fragment(run_every=…)` for auto-refresh; using manual refresh.")
    _panel()
    if st.button("Refresh now", type="primary"):
        st.rerun()
else:
    _panel()
    if st.button("Refresh now", type="primary"):
        st.rerun()
