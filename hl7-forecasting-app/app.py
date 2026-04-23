"""
HL7App - ED & ICU Operations Dashboard

Real-time operations dashboard and ML forecasting for Emergency Department
and ICU arrivals/discharges, powered by Lakebase Postgres.
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from utils.db import run_query_batch, PGHOST, PGDATABASE, ENDPOINT_NAME
from utils import queries
from utils.theme import apply_theme
from utils.navigation import render_sidebar_nav, render_home_footer
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

# ---- Header + primary actions (full navigation stays in the sidebar) ----
st.markdown(
    """
<div class="hl7-app-header">
  <p class="hl7-app-eyebrow">ED &amp; ICU · Lakebase</p>
  <h1>Operations dashboard</h1>
  <p class="hl7-app-lead">Census, throughput, and model outputs on Unity Catalog gold, served to this app and Genie. Below is the <strong>end-to-end reference architecture</strong> the bundled jobs follow when you refresh the stack.</p>
</div>
    """,
    unsafe_allow_html=True,
)

# ---- Reference architecture (data path); keep in sync with Run jobs / DLT order ----
st.markdown(
    """
<div class="hl7-architect-panel">
  <p class="hl7-home-eyebrow">System architecture</p>
  <h2 class="hl7-dataflow-title" style="margin-top:0">Data path — landing to app</h2>
  <p class="hl7-dataflow-deck">Medallion pattern: ingest → conform → gold facts/dims → ML features &amp; scores → Lakebase for low-latency SQL → this app and Genie on the same Lakebase read path.</p>
  <div class="hl7-arch" role="list" aria-label="Pipeline layers">
    <div class="hl7-arch-step" role="listitem">
      <span class="hl7-arch-badge" aria-hidden="true">L1</span>
      <strong>HL7 &amp; landing</strong>
      <span>Volume, raw feeds</span>
    </div>
    <span class="hl7-arch-arrow" aria-hidden="true">→</span>
    <div class="hl7-arch-step" role="listitem">
      <span class="hl7-arch-badge" aria-hidden="true">L2</span>
      <strong>Bronze &amp; silver</strong>
      <span>DLT parse, conform</span>
    </div>
    <span class="hl7-arch-arrow" aria-hidden="true">→</span>
    <div class="hl7-arch-step" role="listitem">
      <span class="hl7-arch-badge" aria-hidden="true">L3</span>
      <strong>Gold (UC)</strong>
      <span>Facts, dimensions</span>
    </div>
    <span class="hl7-arch-arrow" aria-hidden="true">→</span>
    <div class="hl7-arch-step" role="listitem">
      <span class="hl7-arch-badge" aria-hidden="true">L4</span>
      <strong>ML layer</strong>
      <span>Features, train, predict</span>
    </div>
    <span class="hl7-arch-arrow" aria-hidden="true">→</span>
    <div class="hl7-arch-step" role="listitem">
      <span class="hl7-arch-badge" aria-hidden="true">L5</span>
      <strong>Lakebase</strong>
      <span>Postgres sync</span>
    </div>
    <span class="hl7-arch-arrow" aria-hidden="true">→</span>
    <div class="hl7-arch-step hl7-arch-step--app" role="listitem">
      <span class="hl7-arch-badge" aria-hidden="true">L6</span>
      <strong>HL7App</strong>
      <span>Streamlit + Genie</span>
    </div>
  </div>
  <p class="hl7-architect-foot">Operational order for a full refresh: <strong>sample data</strong> (optional) → <strong>DLT</strong> → <strong>inference</strong> → <strong>Lakebase load</strong> — use <strong>Jobs</strong> in the app or the bundle workflow.</p>
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

with st.container(border=True):
    st.markdown(
        """
<p class="hl7-pro-quickstart-title" style="margin:0 0 2px">More shortcuts</p>
<p class="hl7-pro-quickstart-hint" style="margin:0 0 12px">Select a group; the three links match that group.</p>
        """,
        unsafe_allow_html=True,
    )
    focus = home_focus_picker()
    home_quick_links(focus)
st.markdown("")

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
        "font": {"family": "DM Sans, sans-serif", "size": 12, "color": "#5A6F77"},
        "height": 300,
        "margin": {"t": 28, "b": 44, "l": 52, "r": 20},
        "xaxis": {"gridcolor": "#DCE0E2", "zeroline": False, "linecolor": "#DCE0E2"},
        "yaxis": {"gridcolor": "#DCE0E2", "zeroline": False, "linecolor": "#DCE0E2"},
        "paper_bgcolor": "rgba(255,255,255,0.98)",
        "plot_bgcolor": "#F9F7F4",
    }
    base.update(kwargs)
    return base


def _home_snapshot_and_charts() -> None:
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

    with st.container(border=True):
        st.markdown(
            """
<div class="hl7-observability-top">
  <p class="hl7-home-eyebrow">Health</p>
  <h2 class="hl7-section-h2">SLOs &amp; DLT</h2>
  <p class="hl7-live-deck">Rules match <strong>System status</strong> (ok / stale / critical). Stale? Run the pipeline from <strong>Jobs</strong>.</p>
</div>
            """,
            unsafe_allow_html=True,
        )
        render_freshness_metrics_row(home_batch)
        st.divider()
        hc1, hc2 = st.columns([4, 1], gap="medium")
        with hc1:
            st.markdown(
                """
    <p class="hl7-home-eyebrow" style="margin:0 0 2px">KPIs</p>
    <h3 class="hl7-section-h3">Headline metrics</h3>
    <p class="hl7-ops-snapshot-deck">Lakebase gold, same as Real-time and Trends. Use <strong>Refresh</strong> to reload.</p>
                """,
                unsafe_allow_html=True,
            )
        with hc2:
            st.write("")  # align with title block
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
    with st.container(border=True):
        st.markdown(
            """
<div class="hl7-charts-head">
  <p class="hl7-home-eyebrow">Analytics</p>
  <h2 class="hl7-section-h2">Throughput &amp; encounters</h2>
  <p class="hl7-charts-deck">72h message volume (left) and 30d encounters (right). Select a range on a chart for subtotals.</p>
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
                            marker_color="#2272B4",
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
                        line=dict(color="#00A972", width=2),
                        fill="tozeroy",
                        fillcolor="rgba(0, 169, 114, 0.12)",
                        marker=dict(size=8, color="#00A972", line=dict(width=1, color="#ffffff")),
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

    with st.container(border=True):
        st.markdown(
            """
<div class="hl7-lakebase-head">
  <p class="hl7-home-eyebrow">Data connection</p>
  <h2 class="hl7-section-h2">Lakebase / Postgres</h2>
  <p class="hl7-live-deck">Read path shared with the rest of the app and Genie.</p>
</div>
            """,
            unsafe_allow_html=True,
        )
        try:
            info_df = home_batch.get("info", pd.DataFrame())
            if not info_df.empty:
                tbl_count = int(info_df["table_count"].iloc[0])
                tbl_list = info_df["tables"].iloc[0] or ""
                col_conn, col_stats = st.columns(2, gap="large")

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


_home_snapshot_and_charts()
st.caption("**Refresh** reloads data from the database for the block above.")

render_home_footer()
