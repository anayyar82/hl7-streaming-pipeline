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

# ---- Hero ----
st.markdown("---")
st.markdown(
    """
<div class="hl7-hero">
  <h1>HL7 ED & ICU Operations</h1>
  <p>
    Operational census, clinical analytics, and ML forecasts on top of a Databricks medallion pipeline:
    ingest → <strong>Delta Live Tables</strong> gold → <strong>Unity Catalog</strong> → <strong>Lakebase</strong> → this app.
    Pages are grouped in the sidebar: <strong>Clinical intelligence</strong> (Lakebase dashboards), <strong>Platform</strong> (DLT, jobs, ingest, stack pulse), and <strong>Genie</strong> for natural-language questions.
  </p>
  <div class="hl7-badge-row">
    <span class="hl7-badge">DLT</span>
    <span class="hl7-badge">UNITY CATALOG</span>
    <span class="hl7-badge">LAKEBASE</span>
    <span class="hl7-badge">AUTOML / MLFLOW</span>
    <span class="hl7-badge">DATABRICKS APPS</span>
  </div>
</div>
    """,
    unsafe_allow_html=True,
)

# ---- Architecture strip ----
st.markdown("**Data flow** (logical)")
st.markdown(
    """
<div class="hl7-arch">
  <div class="hl7-arch-step"><strong>HL7 files</strong><span>Volume / landing</span></div>
  <span class="hl7-arch-arrow">→</span>
  <div class="hl7-arch-step"><strong>Bronze / Silver</strong><span>DLT parse &amp; conform</span></div>
  <span class="hl7-arch-arrow">→</span>
  <div class="hl7-arch-step"><strong>Gold</strong><span>UC tables · facts &amp; dims</span></div>
  <span class="hl7-arch-arrow">→</span>
  <div class="hl7-arch-step"><strong>ML layer</strong><span>Features · AutoML · predictions</span></div>
  <span class="hl7-arch-arrow">→</span>
  <div class="hl7-arch-step"><strong>Lakebase</strong><span>Postgres API to gold</span></div>
  <span class="hl7-arch-arrow">→</span>
  <div class="hl7-arch-step"><strong>This app</strong><span>Streamlit + Genie</span></div>
</div>
    """,
    unsafe_allow_html=True,
)

with st.container(border=True):
    st.markdown('<p class="hl7-panel-eyebrow">Interactive</p>', unsafe_allow_html=True)
    st.markdown("### Quick start")
    st.caption("Choose what you’re here for — the three links below switch to match.")
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
st.caption(
    "Powered by Databricks Unity Catalog · Delta Live Tables · MLflow AutoML · Lakebase · Apps"
)
