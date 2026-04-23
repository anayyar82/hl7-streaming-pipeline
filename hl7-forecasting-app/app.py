"""
HL7App — Streamlit home: architecture, security, and system health.
"""

import streamlit as st

from utils.db import run_query_batch
from utils.theme import apply_theme
from utils.navigation import render_sidebar_nav, render_home_footer
from utils.health import health_freshness_queries, render_system_health_hero

st.set_page_config(
    page_title="HL7App",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

apply_theme()
render_sidebar_nav()

DOCS_BASE = "https://github.com/anayyar82/hl7-streaming-pipeline/blob/main"
ARCH_DOCS_HREF = f"{DOCS_BASE}/docs/ARCHITECTURE.md"
DLT_DOCS = "https://docs.databricks.com/en/delta-live-tables/index.html"
APPS_DOCS = "https://docs.databricks.com/en/dev-tools/databricks-apps.html"


def _home_health_batch() -> dict:
    return run_query_batch(health_freshness_queries(), quiet=True)

HOME_ARCH_HTML = f"""
<div class="hl7-home-root">
  <header class="hl7-hero-2025">
    <p class="hl7-hero-kicker">Medallion · Databricks</p>
    <h1 class="hl7-hero-title">ED &amp; ICU intelligence</h1>
    <p class="hl7-hero-sub">Architecture, security posture, and live path health in one place. Analytics, throughput, and deep exploration are on
    <strong>Real-time</strong>, <strong>Trends</strong>, and <strong>Operations</strong> in the sidebar — not on Home.</p>
  </header>

  <div class="hl7-home-bento">
    <section class="hl7-bento hl7-bento--flow" aria-labelledby="flow-heading">
      <div class="hl7-bento-head">
        <h2 id="flow-heading" class="hl7-bento-title">Data flow</h2>
        <p class="hl7-bento-deck">End-to-end path from files to apps. Details align with the <a href="{ARCH_DOCS_HREF}" target="_blank" rel="noopener noreferrer">architecture doc</a> and the runbook in <code>docs/ARCHITECTURE.md</code>.</p>
      </div>
      <ol class="hl7-flow-timeline">
        <li class="hl7-flow-item">
          <span class="hl7-flow-idx" aria-hidden="true">1</span>
          <div class="hl7-flow-body">
            <h3 class="hl7-flow-h">Ingest</h3>
            <p>HL7 (and similar) land in <strong>Unity Catalog volumes</strong>. Optional synthetic batches for demos. No direct app writes to gold — landing is the system of record for raw files.</p>
          </div>
        </li>
        <li class="hl7-flow-item">
          <span class="hl7-flow-idx" aria-hidden="true">2</span>
          <div class="hl7-flow-body">
            <h3 class="hl7-flow-h">DLT (bronze → silver)</h3>
            <p>Incremental <a href="{DLT_DOCS}" target="_blank" rel="noopener noreferrer">Delta Live Tables</a> parse, validate, and conformed schemas. Watermark-driven idempotency; Photon where enabled on Lakeflow.</p>
          </div>
        </li>
        <li class="hl7-flow-item">
          <span class="hl7-flow-idx" aria-hidden="true">3</span>
          <div class="hl7-flow-body">
            <h3 class="hl7-flow-h">Gold (Unity Catalog)</h3>
            <p>Business <strong>facts and dimensions</strong> in a governed schema. <strong>UC</strong> is the access boundary for SQL, Genie, and downstream jobs — grant <code>SELECT</code> to the automation and app identities that need it.</p>
          </div>
        </li>
        <li class="hl7-flow-item">
          <span class="hl7-flow-idx" aria-hidden="true">4</span>
          <div class="hl7-flow-body">
            <h3 class="hl7-flow-h">ML &amp; features</h3>
            <p>AutoML training, batch <strong>inference</strong>, and feature / prediction tables in Delta. Scores follow the same medallion and sync rules as the rest of gold.</p>
          </div>
        </li>
        <li class="hl7-flow-item">
          <span class="hl7-flow-idx" aria-hidden="true">5</span>
          <div class="hl7-flow-body">
            <h3 class="hl7-flow-h">Analytical read path</h3>
            <p>Job-driven materialization to a <strong>low-latency SQL</strong> front (JDBC) for sub-second app queries. Identity uses Databricks <strong>OAuth</strong> into the data plane, not shared passwords in this repo.</p>
          </div>
        </li>
        <li class="hl7-flow-item hl7-flow-item--last">
          <span class="hl7-flow-idx" aria-hidden="true">6</span>
          <div class="hl7-flow-body">
            <h3 class="hl7-flow-h">Consumption</h3>
            <p><a href="{APPS_DOCS}" target="_blank" rel="noopener noreferrer">Databricks Apps</a> (this UI, optional AppKit), <strong>Genie</strong> with curated spaces, and Lakeview dashboards. Same governed tables, different entry points.</p>
          </div>
        </li>
      </ol>
    </section>

    <section class="hl7-bento hl7-bento--sec" aria-labelledby="sec-heading">
      <div class="hl7-bento-head">
        <h2 id="sec-heading" class="hl7-bento-title">Security &amp; governance</h2>
        <p class="hl7-bento-deck">High-level model; implement with UC grants, bundle scopes, and the grant notebooks in the repo.</p>
      </div>
      <ul class="hl7-sec-list">
        <li>
          <span class="hl7-sec-tag">UC</span>
          <p><strong>Unity Catalog</strong> — <code>USE CATALOG / SCHEMA / SELECT</code> on the HL7 gold schema for the app service principal. No broad <code>MODIFY</code> in the app path; jobs use their own principals.</p>
        </li>
        <li>
          <span class="hl7-sec-tag">Apps</span>
          <p><strong>Identity &amp; API</strong> — Databricks App runs with a <strong>service principal</strong>; the bundle sets <code>user_api_scopes</code> (e.g. <code>sql</code>, <code>dashboards.genie</code>) for workspace API calls. Env vars hold pipeline / job / org ids — not secrets in the browser.</p>
        </li>
        <li>
          <span class="hl7-sec-tag">Data plane</span>
          <p><strong>Read path &amp; OAuth</strong> — <code>databricks_create_role</code> maps the app to a role on the analytical store. Grants in <code>11_lakebase_*.py</code> and Genie UCs in <code>12_genie_uc_grants.py</code>.</p>
        </li>
        <li>
          <span class="hl7-sec-tag">Genie</span>
          <p><strong>SQL &amp; warehouse</strong> — The Genie space needs <strong>CAN USE</strong> on the SQL warehouse and consistent UC <strong>SELECT</strong> for tables exposed to natural-language Q&amp;A.</p>
        </li>
        <li>
          <span class="hl7-sec-tag">Net / ops</span>
          <p>Prefer private and org-approved connectivity to data services. <strong>Secrets</strong> for email or third parties live in Databricks <strong>secret scopes</strong> in jobs, not in git-tracked <code>app.yaml</code> text.</p>
        </li>
      </ul>
      <a class="hl7-sec-cta" href="{ARCH_DOCS_HREF}" target="_blank" rel="noopener noreferrer">Read full security &amp; flow — <code>docs/ARCHITECTURE.md</code> ↗</a>
    </section>
  </div>
</div>
"""

st.markdown(HOME_ARCH_HTML, unsafe_allow_html=True)

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

st.caption("**Refresh** in the health section re-runs freshness queries and the DLT snapshot.")

render_home_footer()
