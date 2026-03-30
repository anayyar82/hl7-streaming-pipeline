"""
HL7App - ED & ICU Operations Dashboard

Real-time operations dashboard and ML forecasting for Emergency Department
and ICU arrivals/discharges, powered by Lakebase Postgres.
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from utils.db import run_query, PGHOST, PGDATABASE, ENDPOINT_NAME
from utils import queries
from utils.theme import apply_theme
from utils.navigation import render_sidebar_nav, render_home_navigation

st.set_page_config(
    page_title="HL7App - ED & ICU Operations",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

apply_theme()
render_sidebar_nav()

# ---- Hero ----
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
| **Run jobs & workflow** | DLT, bundled workflow, inference, Lakebase load. |
| **Platform pulse** | Cross-stack KPIs, treemap, ML snapshot. |

### Ask your data
| **Genie** | Plain-English Q&A over your Genie space. |

**Home** — connection, KPIs, throughput, and the visual map below.
        """
    )

# ---- KPI row ----
st.markdown("### Operational snapshot")

k1, k2, k3, k4 = st.columns(4)

def _safe_int(df, col, default=0):
    if df is None or df.empty or col not in df.columns:
        return default
    v = df[col].iloc[0]
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return default
    return int(v)

try:
    enc_df = run_query(queries.HOME_ENCOUNTER_COUNT_7D, quiet=True)
    k1.metric("Encounters (7d)", f"{_safe_int(enc_df, 'n'):,}")
except Exception:
    k1.metric("Encounters (7d)", "—")

try:
    msg_df = run_query(queries.HOME_MESSAGE_VOLUME_24H, quiet=True)
    k2.metric("HL7 messages (24h)", f"{_safe_int(msg_df, 'messages_24h'):,}")
except Exception:
    k2.metric("HL7 messages (24h)", "—")

try:
    ml_df = run_query(queries.HOME_ML_PREDICTION_OVERVIEW, quiet=True)
    k3.metric("ML predictions", f"{_safe_int(ml_df, 'total_predictions'):,}", help="Total rows in predictions table")
except Exception:
    k3.metric("ML predictions", "—")

try:
    pat_df = run_query(queries.PATIENT_COUNTS, quiet=True)
    k4.metric("Patients", f"{_safe_int(pat_df, 'total_patients'):,}")
except Exception:
    k4.metric("Patients", "—")

# ---- Charts ----
ch_left, ch_right = st.columns(2)

with ch_left:
    st.markdown("### Pipeline throughput (72h)")
    st.caption("`gold_message_metrics` aggregated hourly — shows DLT-fed ingest cadence.")
    try:
        tp = run_query(queries.HOME_THROUGHPUT_RECENT, quiet=True)
        if not tp.empty:
            tp["processing_hour"] = pd.to_datetime(tp["processing_hour"])
            tp["total_messages"] = pd.to_numeric(tp["total_messages"], errors="coerce").fillna(0)
            fig_tp = go.Figure(
                go.Bar(
                    x=tp["processing_hour"],
                    y=tp["total_messages"],
                    marker_color="#2563eb",
                    opacity=0.85,
                )
            )
            fig_tp.update_layout(
                height=280,
                margin=dict(t=20),
                xaxis_title="Hour",
                yaxis_title="Messages",
                showlegend=False,
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(248,250,252,0.9)",
            )
            st.plotly_chart(fig_tp, use_container_width=True)
        else:
            st.info("No throughput rows in the last 72 hours.")
    except Exception as e:
        st.warning(f"Throughput chart unavailable: {e}")

with ch_right:
    st.markdown("### Encounters (30d)")
    st.caption("`gold_encounter_fact` daily counts — clinical volume landing in UC gold.")
    try:
        tr = run_query(queries.HOME_ENCOUNTER_TREND_30D, quiet=True)
        if not tr.empty:
            tr["d"] = pd.to_datetime(tr["d"])
            tr["encounter_count"] = pd.to_numeric(tr["encounter_count"], errors="coerce").fillna(0)
            fig_tr = go.Figure(
                go.Scatter(
                    x=tr["d"],
                    y=tr["encounter_count"],
                    mode="lines",
                    line=dict(color="#059669", width=2),
                    fill="tozeroy",
                    fillcolor="rgba(5, 150, 105, 0.1)",
                )
            )
            fig_tr.update_layout(
                height=280,
                margin=dict(t=20),
                showlegend=False,
                xaxis_title="Date",
                yaxis_title="Encounters",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(248,250,252,0.9)",
            )
            st.plotly_chart(fig_tr, use_container_width=True)
        else:
            st.info("No encounters in the last 30 days.")
    except Exception as e:
        st.warning(f"Encounter trend unavailable: {e}")

# ---- Lakebase connection ----
st.markdown("---")
st.markdown("### Lakebase connection")

TABLE_COUNT_QUERY = """
SELECT
    COUNT(*) AS table_count,
    string_agg(tablename, ', ' ORDER BY tablename) AS tables
FROM pg_tables
WHERE schemaname = 'ankur_nayyar'
"""

try:
    info_df = run_query(TABLE_COUNT_QUERY)
    if not info_df.empty:
        tbl_count = int(info_df["table_count"].iloc[0])
        tbl_list = info_df["tables"].iloc[0] or ""

        col_conn, col_stats = st.columns([1, 1])

        with col_conn:
            st.markdown(
                '<div class="conn-box">'
                "<b>Host:</b> <code>"
                + str(PGHOST)
                + "</code><br>"
                "<b>Database:</b> <code>"
                + str(PGDATABASE)
                + "</code><br>"
                "<b>Schema:</b> <code>ankur_nayyar</code><br>"
                "<b>Endpoint:</b> <code>"
                + str(ENDPOINT_NAME)
                + "</code><br>"
                f"<b>Tables:</b> <code>{tbl_count}</code>"
                "</div>",
                unsafe_allow_html=True,
            )

        with col_stats:
            st.metric("Gold tables in schema", tbl_count)
            if tbl_list:
                with st.expander("Table names"):
                    for t in sorted(tbl_list.split(", ")):
                        st.code(t, language=None)
    else:
        st.info("Connected to Lakebase but no tables found in schema.")
except Exception as e:
    st.warning(f"Could not query Lakebase metadata: {e}")

st.markdown("---")
render_home_navigation()
st.markdown("---")
st.caption(
    "Powered by Databricks Unity Catalog · Delta Live Tables · MLflow AutoML · Lakebase · Apps"
)
