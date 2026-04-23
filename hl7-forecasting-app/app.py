"""
HL7App - ED & ICU Operations Dashboard

Home: system health (Lakebase SLOs + DLT) only. Other metrics live on dedicated pages.
"""

import streamlit as st

from utils.db import run_query_batch
from utils.theme import apply_theme
from utils.navigation import render_sidebar_nav, render_home_footer
from utils.health import health_freshness_queries, render_system_health_hero

st.set_page_config(
    page_title="HL7App - ED & ICU Operations",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

apply_theme()
render_sidebar_nav()

DOCS_BASE = "https://github.com/anayyar82/hl7-streaming-pipeline/blob/main"
ARCH_DOCS_HREF = f"{DOCS_BASE}/docs/ARCHITECTURE.md"


def _home_health_batch() -> dict:
    return run_query_batch(health_freshness_queries(), quiet=True)


st.markdown(
    f"""
<div class="hl7-app-header hl7-app-header--tight">
  <p class="hl7-app-eyebrow">ED &amp; ICU · Lakebase</p>
  <h1>Home</h1>
  <p class="hl7-app-lead">Operational health: Lakebase data freshness and DLT pipeline. Analytics and head-line KPIs are on
  <strong>Real-time</strong> and <strong>Trends</strong> (sidebar). Architecture: <a href="{ARCH_DOCS_HREF}" target="_blank" rel="noopener noreferrer">docs/ARCHITECTURE.md</a> on Git.</p>
</div>
    """,
    unsafe_allow_html=True,
)

a1, a2, a3, a4 = st.columns(4, gap="small")
with a1:
    st.page_link("pages/1_realtime.py", label="Real-time", icon="📊", use_container_width=True)
with a2:
    st.page_link("pages/0_status.py", label="Status", icon="📡", use_container_width=True)
with a3:
    st.page_link("pages/z_run_jobs.py", label="Jobs", icon="🚀", use_container_width=True)
with a4:
    st.page_link("pages/8_genie_chat.py", label="Genie", icon="💬", use_container_width=True)

st.markdown("")

_hb = _home_health_batch()
with st.container(border=True):
    render_system_health_hero(_hb)

st.caption("**Refresh** (in the health panel) re-runs the Lakebase freshness query and the DLT snapshot.")

render_home_footer()
