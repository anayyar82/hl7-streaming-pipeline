"""
Platform pulse — live Lakebase metrics, ingest mix, and ML feature snapshot (Databricks showcase).
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from utils.db import run_query
from utils import queries
from utils.theme import apply_theme
from utils.navigation import render_sidebar_nav

st.set_page_config(page_title="Platform Pulse", page_icon="⚡", layout="wide")
apply_theme()
render_sidebar_nav()

st.title("Platform pulse")
st.caption(
    "A **Databricks-shaped** view: Unity Catalog gold in **Lakebase**, DLT-derived tables, and **AutoML** feature lines."
)

with st.expander("What this page does", expanded=False):
    st.markdown(
        """
**Purpose**  
Surface **cross-cutting** metrics that are hard to see on a single operational page: how much is landing in the lake, how encounters trend, what **ML features** look like before models consume them, and the **shape of HL7 traffic**.

**Why it matters**  
This is the kind of “data product” layer teams build on Databricks: governed tables (UC), incremental pipelines (DLT), low-latency serving (**Lakebase**), and ML features + predictions consumed by apps and Genie.

**This page does not** replace **System status** (per-table health) or **Operations** (detailed throughput filters)—it complements them with executive-style snapshots.
        """
    )

# ---- Live KPIs ----
st.header("Live snapshot")

c1, c2, c3, c4 = st.columns(4)

try:
    enc = run_query(queries.HOME_ENCOUNTER_COUNT_7D, quiet=True)
    n_enc = int(enc["n"].iloc[0]) if not enc.empty else 0
except Exception:
    n_enc = 0

try:
    ml = run_query(queries.HOME_ML_PREDICTION_OVERVIEW, quiet=True)
    if not ml.empty:
        n_pred = int(ml["total_predictions"].iloc[0])
        n_scored = int(ml["scored_predictions"].iloc[0])
        last_run = ml["latest_forecast_run"].iloc[0]
    else:
        n_pred, n_scored, last_run = 0, 0, None
except Exception:
    n_pred, n_scored, last_run = 0, 0, None

try:
    msg = run_query(queries.HOME_MESSAGE_VOLUME_24H, quiet=True)
    n_msg = int(msg["messages_24h"].iloc[0]) if not msg.empty else 0
except Exception:
    n_msg = 0

try:
    pat = run_query(queries.PATIENT_COUNTS, quiet=True)
    n_pat = int(pat["total_patients"].iloc[0]) if not pat.empty else 0
except Exception:
    n_pat = 0

c1.metric("Encounters (7d)", f"{n_enc:,}")
c2.metric("HL7 msgs (24h)", f"{n_msg:,}")
c3.metric("ML predictions (total)", f"{n_pred:,}", help="Rows in gold_forecast_predictions")
c4.metric("Patients (dim)", f"{n_pat:,}")

if last_run is not None and str(last_run) != "NaT":
    st.caption(f"Latest inference batch: **{last_run}** · Scored with actuals: **{n_scored:,}** rows")

st.markdown("---")

# ---- Charts row ----
left, right = st.columns(2)

with left:
    st.subheader("Encounter volume (30 days)")
    st.caption("From **`gold_encounter_fact`** — fact table fed by DLT silver/gold.")
    try:
        trend = run_query(queries.HOME_ENCOUNTER_TREND_30D, quiet=True)
        if not trend.empty:
            trend["d"] = pd.to_datetime(trend["d"])
            trend["encounter_count"] = pd.to_numeric(trend["encounter_count"], errors="coerce").fillna(0)
            fig = go.Figure()
            fig.add_trace(
                go.Scatter(
                    x=trend["d"],
                    y=trend["encounter_count"],
                    mode="lines",
                    fill="tozeroy",
                    line=dict(color="#2563eb", width=2),
                    fillcolor="rgba(37, 99, 235, 0.12)",
                    name="Encounters",
                )
            )
            fig.update_layout(
                height=320,
                margin=dict(t=20),
                showlegend=False,
                xaxis_title="Date",
                yaxis_title="Encounters",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(248,250,252,0.8)",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No encounter rows in the last 30 days.")
    except Exception as e:
        st.warning(f"Could not load encounter trend: {e}")

with right:
    st.subheader("Ingest mix (HL7)")
    st.caption("Top paths in **`gold_message_metrics`** — message type × facility.")
    try:
        mix = run_query(queries.MESSAGE_MIX_FOR_TREEMAP, quiet=True)
        if not mix.empty:
            mix["msg_count"] = pd.to_numeric(mix["msg_count"], errors="coerce").fillna(0)
            mix = mix[mix["msg_count"] > 0]
            fig = px.treemap(
                mix,
                path=["message_type", "sending_facility"],
                values="msg_count",
                color="msg_count",
                color_continuous_scale="Blues",
                title="",
            )
            fig.update_layout(height=320, margin=dict(t=10, l=0, r=0, b=0))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No message metrics for treemap.")
    except Exception as e:
        st.warning(f"Could not build treemap: {e}")

st.markdown("---")

# ---- ML features sample ----
st.subheader("ML feature snapshot (ED)")
st.caption(
    "Last 24 hourly rows from **`gold_ed_forecast_features`** — lags, rollings, and calendar flags produced in DLT for **AutoML**."
)
try:
    feat = run_query(queries.PLATFORM_FEATURE_SAMPLE, quiet=True)
    if not feat.empty:
        feat = feat.sort_values("event_hour")
        st.dataframe(feat, use_container_width=True, hide_index=True)

        num = [
            c
            for c in feat.columns
            if c != "event_hour" and pd.api.types.is_numeric_dtype(feat[c])
        ]
        if not num:
            for c in ("arrivals", "discharges", "arrivals_lag_1h", "arrivals_rolling_24h", "cumulative_net_census"):
                if c in feat.columns:
                    feat[c] = pd.to_numeric(feat[c], errors="coerce")
            num = [
                c
                for c in feat.columns
                if c != "event_hour" and pd.api.types.is_numeric_dtype(feat[c])
            ]
        if num:
            melt = feat.melt(id_vars=["event_hour"], value_vars=num[:6], var_name="feature", value_name="value")
            melt["event_hour"] = pd.to_datetime(melt["event_hour"])
            fig2 = px.line(
                melt,
                x="event_hour",
                y="value",
                color="feature",
                title="Selected numeric features over recent hours",
            )
            fig2.update_layout(height=360, margin=dict(t=40), legend=dict(orientation="h", y=1.12))
            st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No ED forecast feature rows yet. Run DLT and Lakebase load.")
except Exception as e:
    st.warning(f"Could not load feature sample: {e}")

st.markdown("---")
st.markdown(
    """
**Databricks capabilities highlighted here**

| Capability | Where it shows up |
|------------|-------------------|
| **Delta Live Tables** | Curated gold tables (`gold_*`) with declarative dependencies |
| **Unity Catalog** | Governed schema **`ankur_nayyar`** mirrored to Lakebase |
| **Lakebase** | This app reads the same logical tables as SQL warehouses, via Postgres wire |
| **AutoML / MLflow** | Feature tables + registry-backed models scored in jobs |
| **Apps + Genie** | Hosted Streamlit + optional natural language over UC |
    """
)
