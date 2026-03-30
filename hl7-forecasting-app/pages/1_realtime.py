"""
Real-Time Operations - Live ED & ICU census and hourly trends with filters.
"""

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd

from utils.db import run_query
from utils import queries
from utils.filters import (
    sidebar_section, facility_filter, department_filter,
    apply_facility, weekend_toggle, apply_weekend,
)
from utils.theme import apply_theme
from utils.navigation import render_sidebar_nav

st.set_page_config(page_title="Real-Time Operations", page_icon="📊", layout="wide")
apply_theme()
render_sidebar_nav()
st.title("Real-Time ED & ICU Operations")

with st.expander("What this page does", expanded=False):
    st.markdown(
        """
**Purpose**  
Answer: **“What does the ED/ICU picture look like right now?”** using the latest snapshot in Lakebase.

**What you will see**  
- **Current census-style metrics** (estimated in-department counts, arrivals) by facility and department.  
- **Hourly** arrivals/discharges trends for the recent window.  
- **Filters** narrow to one facility, ED vs ICU, and weekday/weekend patterns.

**Data note**  
Figures reflect what has landed in **gold** tables after DLT processing; refresh timing follows your pipeline and Lakebase load schedule—not true sub-second streaming unless upstream is near real time.
        """
    )

sidebar_section("Real-Time Filters")

# ---- Current Census ----
st.header("Current Department Census")

try:
    census_df = run_query(queries.CURRENT_CENSUS)

    if census_df.empty:
        st.warning("No census data available yet. Run the DLT pipeline to populate tables.")
    else:
        dept_sel = department_filter(census_df, key="rt_dept")
        fac_sel = facility_filter(census_df, key="rt_fac")

        filtered = census_df.copy()
        if dept_sel:
            filtered = filtered[filtered["department"].isin(dept_sel)]
        filtered = apply_facility(filtered, fac_sel)

        if filtered.empty:
            st.info("No data matches the selected filters.")
        else:
            snapshot_time = filtered["snapshot_at"].max() if "snapshot_at" in filtered.columns else None

            ed_census = filtered[filtered["department"] == "ED"]
            icu_census = filtered[filtered["department"] == "ICU"]

            ed_total = int(ed_census["estimated_census"].sum()) if not ed_census.empty else 0
            icu_total = int(icu_census["estimated_census"].sum()) if not icu_census.empty else 0
            ed_arrivals = int(ed_census["total_arrivals"].sum()) if not ed_census.empty else 0
            icu_arrivals = int(icu_census["total_arrivals"].sum()) if not icu_census.empty else 0
            ed_discharges = int(ed_census["total_discharges"].sum()) if not ed_census.empty else 0
            icu_discharges = int(icu_census["total_discharges"].sum()) if not icu_census.empty else 0
            total_census = int(filtered["estimated_census"].sum())

            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Total Census", total_census)
            c2.metric("ED Census", ed_total)
            c3.metric("ICU Census", icu_total)
            c4.metric("ED Arrivals", ed_arrivals)
            c5.metric("ICU Arrivals", icu_arrivals)

            c6, c7, c8 = st.columns(3)
            c6.metric("ED Discharges", ed_discharges)
            c7.metric("ICU Discharges", icu_discharges)
            if snapshot_time:
                c8.metric("Last Snapshot", str(snapshot_time)[:19])

            st.dataframe(
                filtered[["location_facility", "department", "total_arrivals",
                           "total_discharges", "estimated_census", "snapshot_at"]],
                use_container_width=True,
                hide_index=True,
            )

except Exception as e:
    st.error(f"Failed to load census data: {e}")

st.markdown("---")

# ---- Hourly Trends ----
st.header("Hourly Arrivals & Discharges")

wknd_choice = weekend_toggle(key="rt_wknd")
st.sidebar.markdown("---")
time_range = st.sidebar.radio(
    "Time Window", ["Last 24h", "All Data"], key="rt_time", horizontal=True,
)

tab_ed, tab_icu = st.tabs(["Emergency Department", "Intensive Care Unit"])


def _render_hourly(hourly_df: pd.DataFrame, dept_label: str, colors: tuple[str, str, str]):
    hourly_df = apply_facility(hourly_df, fac_sel if "fac_sel" in dir() else [])
    hourly_df = apply_weekend(hourly_df, wknd_choice)

    if hourly_df.empty:
        st.info(f"No {dept_label} hourly data for selected filters.")
        return

    hourly_df["event_hour"] = pd.to_datetime(hourly_df["event_hour"])

    total_arrivals = int(hourly_df["arrivals"].sum())
    total_discharges = int(hourly_df["discharges"].sum())
    peak_hour = hourly_df.loc[hourly_df["arrivals"].idxmax()]

    m1, m2, m3 = st.columns(3)
    m1.metric(f"{dept_label} Total Arrivals", f"{total_arrivals:,}")
    m2.metric(f"{dept_label} Total Discharges", f"{total_discharges:,}")
    m3.metric("Peak Hour", str(peak_hour["event_hour"])[:16] if not hourly_df.empty else "N/A")

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Bar(x=hourly_df["event_hour"], y=hourly_df["arrivals"],
               name="Arrivals", marker_color=colors[0], opacity=0.8),
        secondary_y=False,
    )
    fig.add_trace(
        go.Bar(x=hourly_df["event_hour"], y=hourly_df["discharges"],
               name="Discharges", marker_color=colors[1], opacity=0.8),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(x=hourly_df["event_hour"], y=hourly_df["net_change"].cumsum(),
                   name="Cumulative Net", line=dict(color=colors[2], width=2)),
        secondary_y=True,
    )
    fig.update_layout(barmode="group", height=400, margin=dict(t=30),
                      legend=dict(orientation="h", y=1.12))
    fig.update_yaxes(title_text="Count", secondary_y=False)
    fig.update_yaxes(title_text="Cumulative Net", secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Raw Data"):
        st.dataframe(hourly_df, use_container_width=True, hide_index=True)


with tab_ed:
    try:
        query = queries.ED_HOURLY_LAST_24H if time_range == "Last 24h" else queries.ED_HOURLY_ALL
        ed_hourly = run_query(query)
        if ed_hourly.empty and time_range == "Last 24h":
            ed_hourly = run_query(queries.ED_HOURLY_ALL)
        _render_hourly(ed_hourly, "ED", ("#4CAF50", "#F44336", "#2196F3"))
    except Exception as e:
        st.error(f"Failed to load ED hourly data: {e}")

with tab_icu:
    try:
        query = queries.ICU_HOURLY_LAST_24H if time_range == "Last 24h" else queries.ICU_HOURLY_ALL
        icu_hourly = run_query(query)
        if icu_hourly.empty and time_range == "Last 24h":
            icu_hourly = run_query(queries.ICU_HOURLY_ALL)
        _render_hourly(icu_hourly, "ICU", ("#FF9800", "#9C27B0", "#00BCD4"))
    except Exception as e:
        st.error(f"Failed to load ICU hourly data: {e}")

st.caption("Data sourced from Lakebase Postgres gold tables.")
