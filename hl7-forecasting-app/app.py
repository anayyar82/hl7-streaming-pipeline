"""
HL7App - ED & ICU Operations Dashboard

Real-time operations dashboard and ML forecasting for Emergency Department
and ICU arrivals/discharges, powered by Lakebase Postgres.
"""

import streamlit as st

from utils.theme import apply_theme
from utils.navigation import render_sidebar_nav, render_home_navigation
from utils.ui import home_focus_picker, home_quick_links

st.set_page_config(
    page_title="HL7App - ED & ICU Operations",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

apply_theme()
render_sidebar_nav()

# ---- Clinical intelligence + Platform + Genie bento (first) ----
render_home_navigation()

st.markdown('<div class="hl7-section-divider" aria-hidden="true"></div>', unsafe_allow_html=True)

# ---- Hero (themed: hl7-hero-2025) ----
st.markdown(
    """
<div class="hl7-hero-2025">
  <p class="hl7-hero-kicker">ED &amp; ICU · Databricks Apps</p>
  <h1 class="hl7-hero-title">HL7 ED &amp; ICU Operations</h1>
  <p class="hl7-hero-sub">
    Real-time census, clinical analytics, and ML forecasts on <strong>Unity Catalog</strong> gold via <strong>Lakebase</strong>.
    The grid above is the fastest path; the <strong>sidebar</strong> lists every page under <strong>Clinical</strong>, <strong>Platform</strong> (DLT · jobs · health), and <strong>Genie</strong> for natural-language Q&amp;A.
  </p>
  <div class="hl7-badge-row" aria-label="Stack">
    <span class="hl7-badge">DLT</span>
    <span class="hl7-badge">Unity Catalog</span>
    <span class="hl7-badge">Lakebase</span>
    <span class="hl7-badge">AutoML · MLflow</span>
    <span class="hl7-badge">Databricks Apps</span>
  </div>
</div>
    """,
    unsafe_allow_html=True,
)

# ---- Data flow (logical pipeline) ----
st.markdown(
    """
<div class="hl7-dataflow-wrap hl7-dataflow-wrap--home" role="region" aria-label="Data flow">
  <p class="hl7-home-eyebrow">Logical pipeline</p>
  <h2 class="hl7-dataflow-title">From HL7 files to this app</h2>
  <p class="hl7-dataflow-deck">
    One path from raw messages to dashboards and models — for orientation, not a literal network map.
  </p>
  <div class="hl7-arch" role="list">
  <div class="hl7-arch-step" role="listitem"><strong>HL7 files</strong><span>Volume / landing</span></div>
  <span class="hl7-arch-arrow" aria-hidden="true">→</span>
  <div class="hl7-arch-step" role="listitem"><strong>Bronze / Silver</strong><span>DLT parse &amp; conform</span></div>
  <span class="hl7-arch-arrow" aria-hidden="true">→</span>
  <div class="hl7-arch-step" role="listitem"><strong>Gold</strong><span>UC tables · facts &amp; dims</span></div>
  <span class="hl7-arch-arrow" aria-hidden="true">→</span>
  <div class="hl7-arch-step" role="listitem"><strong>ML layer</strong><span>Features · AutoML · predictions</span></div>
  <span class="hl7-arch-arrow" aria-hidden="true">→</span>
  <div class="hl7-arch-step" role="listitem"><strong>Lakebase</strong><span>Postgres API to gold</span></div>
  <span class="hl7-arch-arrow" aria-hidden="true">→</span>
  <div class="hl7-arch-step hl7-arch-step--app" role="listitem"><strong>This app</strong><span>Streamlit + Genie</span></div>
  </div>
</div>
    """,
    unsafe_allow_html=True,
)

with st.container(border=True):
    st.markdown('<p class="hl7-panel-eyebrow">Shortcuts</p>', unsafe_allow_html=True)
    st.markdown("### Quick start")
    st.caption("Pick a focus; the three buttons below follow your choice.")
    focus = home_focus_picker()
    home_quick_links(focus)

with st.expander("What each page is for (quick map)", expanded=False):
    st.markdown(
        """
### Clinical intelligence (Lakebase)
| Page | What it does |
|------|----------------|
| **Real-time ops** | Current ED/ICU census and hourly strips. |
| **Trends** | Daily rollups, heatmaps, ED vs ICU. |
| **ML forecasting** | Predictions, bands, timelines, vs actuals. |
| **Model performance** | MAE, MAPE, coverage, model comparison. |
| **Patient & clinical** | Demographics, diagnoses, labs, orders. |
| **Combined forecast** | ED+ICU system pressure and ratios. |
| **HL7 operations** | Message throughput, pipeline breakdown. |

### Platform (Databricks workspace)
| Page | What it does |
|------|----------------|
| **System status** | Per-table freshness matrix and runbook. |
| **Sample → volume** | Land HL7 files into the UC landing volume. |
| **Live activity** | DLT + Workflows run status (auto-refresh). |
| **DLT update live** | Per-flow status + row metrics from pipeline event log. |
| **Run jobs & workflow** | DLT, bundled workflow, inference, Lakebase load. |
| **Platform pulse** | Cross-stack KPIs, treemap, ML snapshot. |
| **Load test** | Parallel connection/latency checks to Lakebase. |

### Ask your data
| **Genie** | Plain-English Q&A over your Genie space. |

KPIs, throughput charts, and connection details: use **System status** or the clinical pages in the sidebar.
        """
    )

st.markdown("---")
st.markdown(
    '<p class="hl7-home-footer">Unity Catalog · Delta Live Tables · MLflow AutoML · Lakebase · Databricks Apps</p>',
    unsafe_allow_html=True,
)
