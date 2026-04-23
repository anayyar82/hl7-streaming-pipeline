"""
HL7App - ED & ICU Operations Dashboard

Real-time operations dashboard and ML forecasting for Emergency Department
and ICU arrivals/discharges, powered by Lakebase Postgres.
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from utils.db import run_query_batch, PGHOST, PGDATABASE, ENDPOINT_NAME
from utils.streamlit_refresh import run_live_dashboard
from utils import queries
from utils.theme import apply_theme
from utils.navigation import render_sidebar_nav, render_home_navigation
from utils.plotly_selection import selection_state_from_chart, selected_row_indices
from utils.ui import home_focus_picker, home_quick_links
from utils.health import health_freshness_queries, render_freshness_metrics_row

st.set_page_config(
    page_title="HL7App - ED & ICU Operations",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

apply_theme()
render_sidebar_nav()

TABLE_COUNT_QUERY = """
SELECT
    COUNT(*) AS table_count,
    string_agg(tablename, ', ' ORDER BY tablename) AS tables
FROM pg_tables
WHERE schemaname = 'ankur_nayyar'
"""

# ---- Hero ----
st.markdown(
    """
<div class="hl7-hero">
  <div class="hl7-hero-inner">
    <p class="hl7-hero-kicker">Medallion · real-time · ML on gold</p>
    <h1>HL7 ED & ICU Operations</h1>
    <p class="hl7-hero-deck">
      One place for <strong>operational census</strong>, <strong>clinical activity</strong>, and <strong>forecasts</strong> — served from
      <strong>Delta Live Tables</strong> gold through <strong>Unity Catalog</strong> to <strong>Lakebase (Postgres)</strong> for a fast, governed app and Genie.
    </p>
    <p class="hl7-hero-meta">
      <strong>Clinical</strong> dashboards in the left nav · <strong>Platform</strong> (DLT, jobs, health) in the next group · <strong>Genie</strong> for ask-your-data in plain English.
    </p>
    <div class="hl7-badge-row" aria-label="Stack">
      <span class="hl7-badge">DLT</span>
      <span class="hl7-badge">Unity Catalog</span>
      <span class="hl7-badge">Lakebase</span>
      <span class="hl7-badge">AutoML &amp; MLflow</span>
      <span class="hl7-badge">Databricks Apps</span>
    </div>
  </div>
</div>
    """,
    unsafe_allow_html=True,
)

# ---- Data pipeline (logical) ----
st.markdown(
    """
<div class="hl7-dataflow-wrap">
  <p class="hl7-home-eyebrow">End-to-end path</p>
  <h2 class="hl7-dataflow-title">Data flow</h2>
  <p class="hl7-dataflow-deck">Logical order — <strong>Run jobs &amp; workflow</strong> in the app mirrors this when you refresh the full stack.</p>
  <div class="hl7-arch">
    <div class="hl7-arch-step"><strong>HL7</strong><span>Volume / landing</span></div>
    <span class="hl7-arch-arrow" aria-hidden="true">→</span>
    <div class="hl7-arch-step"><strong>Bronze &amp; Silver</strong><span>DLT parse &amp; conform</span></div>
    <span class="hl7-arch-arrow" aria-hidden="true">→</span>
    <div class="hl7-arch-step"><strong>Gold</strong><span>UC tables · facts / dims</span></div>
    <span class="hl7-arch-arrow" aria-hidden="true">→</span>
    <div class="hl7-arch-step"><strong>ML</strong><span>Features · training · predict</span></div>
    <span class="hl7-arch-arrow" aria-hidden="true">→</span>
    <div class="hl7-arch-step"><strong>Lakebase</strong><span>Postgres to gold</span></div>
    <span class="hl7-arch-arrow" aria-hidden="true">→</span>
    <div class="hl7-arch-step"><strong>HL7App</strong><span>Streamlit + Genie</span></div>
  </div>
</div>
    """,
    unsafe_allow_html=True,
)

ed1, ed2 = st.columns(2, gap="medium")
with ed1:
    with st.expander("2-minute demo path", expanded=False):
        st.markdown(
            """
1. Scroll to **System health** and **snapshot** (below) — SLOs + live KPIs.  
2. **Run jobs** — DLT → Inference → Lakebase bundle, then **Refresh** here.  
3. **Real-time ops** &amp; **ML forecasting** for census and models.  
4. **Genie** — one NL question.  
5. **System status** for the per-table matrix.
            """
        )
        c_demo1, c_demo2 = st.columns(2, gap="small")
        c_demo1.page_link("pages/z_run_jobs.py", label="Run jobs", icon="🚀", use_container_width=True)
        c_demo2.page_link("pages/0_status.py", label="System status", icon="📡", use_container_width=True)
with ed2:
    with st.expander("All pages (quick map)", expanded=False):
        st.markdown(
            """
**Clinical:** real-time, trends, ML, model performance, patient &amp; clinical, combined forecast, HL7 ops.  
**Platform:** system status, sample to volume, live activity, DLT live, run jobs, platform pulse.  
**Genie:** ask your data.
            """
        )

with st.container(border=True):
    st.markdown(
        """
<p class="hl7-panel-eyebrow" style="margin-top:0">Choose your lane</p>
<h3 style="margin:0 0 6px; font-size:1.2rem; font-weight:700; color:#0f172a; border:none">Quick start</h3>
<p style="margin:0; font-size:0.9rem; color:#64748b; line-height:1.5">Use the control and links — the three buttons update for <strong>clinical</strong>, <strong>platform</strong>, or <strong>Genie</strong> workflows.</p>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("")
    focus = home_focus_picker()
    home_quick_links(focus)

with st.expander("Full page index (detail)", expanded=False):
    st.markdown(
        """
### Clinical intelligence
| Page | What it does |
|------|--------------|
| **Real-time ops** | Current ED/ICU census and hourly flow. |
| **Trends** | Daily rollups, heatmaps, ED vs ICU. |
| **ML forecasting** | Horizons, bands, predicted vs actual. |
| **Model performance** | MAE, MAPE, coverage, comparison. |
| **Patient & clinical** | Demographics, dx, labs, orders. |
| **Combined forecast** | ED+ICU pressure and ratios. |
| **HL7 operations** | Message throughput, breakdown. |

### Platform
| Page | What it does |
|------|--------------|
| **System status** | Per-table freshness &amp; runbook. |
| **Sample → volume** | Land files to UC volume. |
| **Live activity** | DLT + job runs. |
| **DLT update live** | Per-flow + row metrics. |
| **Run jobs** | DLT, bundle, inference, Lakebase. |
| **Platform pulse** | Treemap, ML snapshot. |
| **Load test** | Optional Lakebase probe. |

### Genie
| **Ask your data** | Natural language over your space. |
        """
    )

def _safe_int(df, col, default=0):
    if df is None or df.empty or col not in df.columns:
        return default
    v = df[col].iloc[0]
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return default
    return int(v)


def _home_chart_layout(**kwargs):
    """Consistent Plotly look on Home (matches theme typography)."""
    base = {
        "template": "plotly_white",
        "font": {"family": "IBM Plex Sans, sans-serif", "size": 12, "color": "#475569"},
        "height": 300,
        "margin": {"t": 28, "b": 44, "l": 52, "r": 20},
        "xaxis": {"gridcolor": "#e2e8f0", "zeroline": False, "linecolor": "#cbd5e1"},
        "yaxis": {"gridcolor": "#e2e8f0", "zeroline": False, "linecolor": "#cbd5e1"},
        "paper_bgcolor": "rgba(255,255,255,0.98)",
        "plot_bgcolor": "#f8fafc",
    }
    base.update(kwargs)
    return base


def _home_snapshot_and_charts() -> None:
    st.markdown(
        """
<div class="hl7-live-head">
  <p class="hl7-home-eyebrow">Observability</p>
  <h2>System health &amp; SLOs</h2>
  <p class="hl7-live-deck">Lakebase freshness and DLT state — same <strong>ok / stale / critical</strong> rules as
  <strong>System status</strong>. Run jobs if something looks old.</p>
</div>
        """,
        unsafe_allow_html=True,
    )
    home_batch = run_query_batch(
        {
            **health_freshness_queries(),
            "enc": queries.HOME_ENCOUNTER_COUNT_7D,
            "msg": queries.HOME_MESSAGE_VOLUME_24H,
            "ml": queries.HOME_ML_PREDICTION_OVERVIEW,
            "pat": queries.PATIENT_COUNTS,
            "tp": queries.HOME_THROUGHPUT_RECENT,
            "tr": queries.HOME_ENCOUNTER_TREND_30D,
            "info": TABLE_COUNT_QUERY,
        },
        quiet=True,
    )
    render_freshness_metrics_row(home_batch)
    st.markdown("---")
    hc1, hc2 = st.columns([4, 1], vertical_alignment="center")
    with hc1:
        st.markdown(
            """
<p class="hl7-home-eyebrow" style="margin:0 0 2px">Live metrics</p>
<h3 style="margin:0; font-size:1.2rem; font-weight:700; color:#0f172a; border:none">Operational snapshot</h3>
<p style="margin:4px 0 0; font-size:0.88rem; color:#64748b; line-height:1.45">Counts from Lakebase gold — same path as <strong>Real-time</strong> and <strong>Trends</strong>.</p>
            """,
            unsafe_allow_html=True,
        )
    with hc2:
        if st.button("Refresh", help="Reload metrics and charts from the database", use_container_width=True):
            st.rerun()

    k1, k2, k3, k4 = st.columns(4, gap="medium")

    enc_df = home_batch.get("enc", pd.DataFrame())
    try:
        k1.metric(
            "Encounters (7d)",
            f"{_safe_int(enc_df, 'n'):,}",
            help="Row count from gold encounter fact — last 7 days window in query.",
        )
    except Exception:
        k1.metric("Encounters (7d)", "—")

    msg_df = home_batch.get("msg", pd.DataFrame())
    try:
        k2.metric(
            "HL7 messages (24h)",
            f"{_safe_int(msg_df, 'messages_24h'):,}",
            help="Aggregated HL7 message volume in the trailing 24 hours.",
        )
    except Exception:
        k2.metric("HL7 messages (24h)", "—")

    ml_df = home_batch.get("ml", pd.DataFrame())
    try:
        k3.metric("ML predictions", f"{_safe_int(ml_df, 'total_predictions'):,}", help="Total rows in predictions table")
    except Exception:
        k3.metric("ML predictions", "—")

    pat_df = home_batch.get("pat", pd.DataFrame())
    try:
        k4.metric(
            "Patients",
            f"{_safe_int(pat_df, 'total_patients'):,}",
            help="Distinct patients represented in the patient dimension / fact pipeline.",
        )
    except Exception:
        k4.metric("Patients", "—")

    # ---- Charts ----
    st.markdown(
        """
<div class="hl7-charts-head" style="margin-top:8px">
  <p class="hl7-home-eyebrow">Visual analytics</p>
  <h2>Throughput &amp; encounters</h2>
  <p class="hl7-charts-deck"><strong>Left:</strong> hourly message volume (72h). <strong>Right:</strong> daily encounters (30d).
  Use the chart toolbar to <strong>box- or lasso-select</strong> — totals show under the chart.</p>
</div>
        """,
        unsafe_allow_html=True,
    )
    ch_left, ch_right = st.columns(2, gap="large")

    _PLOT_CONFIG = {"displayModeBar": True, "scrollZoom": True, "responsive": True, "toImageButtonOptions": {"format": "png", "scale": 2}}

    with ch_left:
        st.markdown("**Pipeline throughput (72h)** · `gold_message_metrics`")
        st.caption("Select a range in the chart to see hour-level totals below.")
        try:
            tp = home_batch.get("tp", pd.DataFrame())
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
                    **_home_chart_layout(
                        xaxis_title="Time",
                        yaxis_title="Messages",
                        showlegend=False,
                        dragmode="select",
                        selectdirection="h",
                    ),
                )
                fig_tp.update_xaxes(tickformat="%b %d %H:%M", tickangle=-25, automargin=True)
                fig_tp.update_yaxes(automargin=True)
                ev_tp = st.plotly_chart(
                    fig_tp,
                    use_container_width=True,
                    config=_PLOT_CONFIG,
                    on_select="rerun",
                    key="home_throughput_chart",
                    selection_mode=["points", "box", "lasso"],
                )
                sel_tp = selection_state_from_chart(ev_tp, "home_throughput_chart", st.session_state)
                idx_tp = selected_row_indices(tp, sel_tp, "processing_hour")
                if idx_tp:
                    sub = tp.iloc[idx_tp].sort_values("processing_hour")
                    tot = int(sub["total_messages"].sum())
                    st.markdown("##### Selection — throughput")
                    m1, m2, m3 = st.columns(3)
                    m1.metric("Hours selected", len(idx_tp))
                    m2.metric("Messages in selection", f"{tot:,}")
                    m3.metric(
                        "Avg / hour",
                        f"{tot // len(idx_tp):,}" if idx_tp else "—",
                    )
                    show = sub.copy()
                    show["processing_hour"] = show["processing_hour"].dt.strftime("%Y-%m-%d %H:%M")
                    st.dataframe(
                        show.rename(columns={"processing_hour": "Hour", "total_messages": "Messages"}),
                        use_container_width=True,
                        hide_index=True,
                    )
            else:
                st.info("No throughput rows in the last 72 hours.")
        except Exception as e:
            st.warning(f"Throughput chart unavailable: {e}")

    with ch_right:
        st.markdown("**Encounters (30d)** · `gold_encounter_fact` (daily)")
        st.caption("Box-select a date range to see rollups under the chart.")
        try:
            tr = home_batch.get("tr", pd.DataFrame())
            if not tr.empty:
                tr["d"] = pd.to_datetime(tr["d"])
                tr["encounter_count"] = pd.to_numeric(tr["encounter_count"], errors="coerce").fillna(0)
                fig_tr = go.Figure(
                    go.Scatter(
                        x=tr["d"],
                        y=tr["encounter_count"],
                        mode="lines+markers",
                        line=dict(color="#059669", width=2),
                        fill="tozeroy",
                        fillcolor="rgba(5, 150, 105, 0.1)",
                        marker=dict(size=8, color="#059669", line=dict(width=1, color="#ffffff")),
                    )
                )
                fig_tr.update_layout(
                    **_home_chart_layout(
                        xaxis_title="Date",
                        yaxis_title="Encounters",
                        showlegend=False,
                        dragmode="select",
                        selectdirection="h",
                    ),
                )
                fig_tr.update_xaxes(tickformat="%b %d", automargin=True)
                fig_tr.update_yaxes(automargin=True)
                ev_tr = st.plotly_chart(
                    fig_tr,
                    use_container_width=True,
                    config=_PLOT_CONFIG,
                    on_select="rerun",
                    key="home_encounters_chart",
                    selection_mode=["points", "box", "lasso"],
                )
                sel_tr = selection_state_from_chart(ev_tr, "home_encounters_chart", st.session_state)
                idx_tr = selected_row_indices(tr, sel_tr, "d")
                if idx_tr:
                    sub = tr.iloc[idx_tr].sort_values("d")
                    tot_e = int(sub["encounter_count"].sum())
                    st.markdown("##### Selection — encounters")
                    e1, e2, e3 = st.columns(3)
                    e1.metric("Days selected", len(idx_tr))
                    e2.metric("Encounters in selection", f"{tot_e:,}")
                    e3.metric(
                        "Avg / day",
                        f"{tot_e // len(idx_tr):,}" if idx_tr else "—",
                    )
                    show_e = sub.copy()
                    show_e["d"] = show_e["d"].dt.strftime("%Y-%m-%d")
                    st.dataframe(
                        show_e.rename(columns={"d": "Date", "encounter_count": "Encounters"}),
                        use_container_width=True,
                        hide_index=True,
                    )
            else:
                st.info("No encounters in the last 30 days.")
        except Exception as e:
            st.warning(f"Encounter trend unavailable: {e}")

    # ---- Lakebase connection ----
    st.markdown("---")
    st.markdown(
        """
<div class="hl7-live-head" style="border-bottom:none; padding-bottom:0">
  <p class="hl7-home-eyebrow">Connection</p>
  <h2 style="margin:0; font-size:1.2rem; font-weight:700; color:#0f172a; border:none">Lakebase (this app)</h2>
  <p class="hl7-live-deck" style="margin-top:4px">Postgres via OAuth — the same connection your Genie and clinical pages use.</p>
</div>
        """,
        unsafe_allow_html=True,
    )

    try:
        info_df = home_batch.get("info", pd.DataFrame())
        if not info_df.empty:
            tbl_count = int(info_df["table_count"].iloc[0])
            tbl_list = info_df["tables"].iloc[0] or ""

            with st.container(border=True):
                col_conn, col_stats = st.columns([1, 1], gap="large", vertical_alignment="start")

                with col_conn:
                    st.markdown(
                        '<div class="conn-box">'
                        "<b>Host</b> <code>"
                        + str(PGHOST)
                        + "</code><br>"
                        "<b>Database</b> <code>"
                        + str(PGDATABASE)
                        + "</code><br>"
                        "<b>Schema</b> <code>ankur_nayyar</code><br>"
                        "<b>Endpoint</b> <code>"
                        + str(ENDPOINT_NAME)
                        + "</code><br>"
                        f"<b>Tables visible</b> <code>{tbl_count}</code>"
                        "</div>",
                        unsafe_allow_html=True,
                    )

                with col_stats:
                    st.metric("Gold relations in schema", tbl_count, help="From pg_tables in the connected schema.")
                    if tbl_list:
                        with st.expander("Table list", expanded=False):
                            for t in sorted(tbl_list.split(", ")):
                                st.code(t, language=None)
        else:
            st.info("Connected to Lakebase but no tables found in schema.")
    except Exception as e:
        st.warning(f"Could not query Lakebase metadata: {e}")


run_live_dashboard(_home_snapshot_and_charts, interval_seconds=22, manual_key="hl7_home_live_refresh")

st.markdown("---")
render_home_navigation()
st.markdown("---")
st.markdown(
    '<p class="hl7-powered-foot">'
    "Databricks &mdash; Unity Catalog · Delta Live Tables · MLflow &amp; AutoML · Lakebase · Genie &middot; Apps"
    "</p>",
    unsafe_allow_html=True,
)
