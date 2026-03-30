"""
Trends & Analytics - Daily summaries, hour-of-day patterns, ED vs ICU with filters.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np

from utils.db import run_query
from utils import queries
from utils.filters import (
    sidebar_section, facility_filter, date_range_filter, weekend_toggle,
    apply_facility, apply_date_range, apply_weekend,
)
from utils.theme import apply_theme

st.set_page_config(page_title="Trends & Analytics", page_icon="📈", layout="wide")
apply_theme()
st.title("Trends & Analytics")

with st.expander("What this page does", expanded=False):
    st.markdown(
        """
**Purpose**  
**Historical and same-day patterns**—how busy ED and ICU were over days and hours, not a single “right now” snapshot.

**What you will see**  
- **Daily summaries** (ED tab / ICU tab): visits, arrivals, discharges, length-of-stay style rollups by day.  
- **Hour-of-day** and **day-of-week** views: when peaks happen.  
- **ED vs ICU** comparisons on shared charts where data allows.

**Filters**  
Facility, **date range**, and optional **weekend-only** slice so you can compare like with like.
        """
    )

sidebar_section("Trend Filters")

# ---- Load data once ----
ed_daily_raw = run_query(queries.ED_DAILY_SUMMARY)
icu_daily_raw = run_query(queries.ICU_DAILY_SUMMARY)

combined_raw = pd.concat([
    ed_daily_raw.assign(dept="ED") if not ed_daily_raw.empty else pd.DataFrame(),
    icu_daily_raw.assign(dept="ICU") if not icu_daily_raw.empty else pd.DataFrame(),
], ignore_index=True)

fac_sel = facility_filter(combined_raw, key="tr_fac")
start_date, end_date = date_range_filter(combined_raw, key="tr_dr")

# ---- Daily Summaries ----
st.header("Daily Summaries")

tab_ed, tab_icu = st.tabs(["ED Daily", "ICU Daily"])

with tab_ed:
    try:
        if ed_daily_raw.empty:
            st.info("No ED daily summary data available.")
        else:
            ed_daily = apply_facility(ed_daily_raw.copy(), fac_sel)
            ed_daily["activity_date"] = pd.to_datetime(ed_daily["activity_date"])
            ed_daily = apply_date_range(ed_daily, start_date, end_date)

            if ed_daily.empty:
                st.info("No data matches the selected filters.")
            else:
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Avg Daily Arrivals", round(ed_daily["total_arrivals"].mean(), 1))
                col2.metric(
                    "Avg LOS (min)",
                    round(ed_daily["avg_los_minutes"].dropna().mean(), 1)
                    if not ed_daily["avg_los_minutes"].dropna().empty else "N/A",
                )
                col3.metric("Avg Unique Patients/Day", round(ed_daily["unique_patients"].mean(), 1))
                col4.metric("Total Days", len(ed_daily))

                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=ed_daily["activity_date"], y=ed_daily["total_arrivals"],
                    name="Arrivals", fill="tozeroy", line=dict(color="#4CAF50"),
                ))
                fig.add_trace(go.Scatter(
                    x=ed_daily["activity_date"], y=ed_daily["total_discharges"],
                    name="Discharges", fill="tozeroy", line=dict(color="#F44336"),
                ))
                fig.update_layout(title="ED Daily Arrivals vs Discharges", height=350, margin=dict(t=40))
                st.plotly_chart(fig, use_container_width=True)

                if ed_daily["avg_los_minutes"].notna().any():
                    fig_los = px.bar(
                        ed_daily.dropna(subset=["avg_los_minutes"]),
                        x="activity_date", y="avg_los_minutes",
                        title="ED Average Length of Stay (minutes)",
                        color_discrete_sequence=["#2196F3"],
                    )
                    fig_los.update_layout(height=300, margin=dict(t=40))
                    st.plotly_chart(fig_los, use_container_width=True)

                with st.expander("Full ED Daily Data"):
                    st.dataframe(ed_daily, use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"Failed to load ED daily summary: {e}")

with tab_icu:
    try:
        if icu_daily_raw.empty:
            st.info("No ICU daily summary data available.")
        else:
            icu_daily = apply_facility(icu_daily_raw.copy(), fac_sel)
            icu_daily["activity_date"] = pd.to_datetime(icu_daily["activity_date"])
            icu_daily = apply_date_range(icu_daily, start_date, end_date)

            if icu_daily.empty:
                st.info("No data matches the selected filters.")
            else:
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Avg Daily Arrivals", round(icu_daily["total_arrivals"].mean(), 1))
                col2.metric(
                    "Avg LOS (hours)",
                    round(icu_daily["avg_los_hours"].dropna().mean(), 1)
                    if not icu_daily["avg_los_hours"].dropna().empty else "N/A",
                )
                col3.metric("Avg Beds Used/Day", round(icu_daily["beds_used"].mean(), 1))
                col4.metric("Total Days", len(icu_daily))

                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=icu_daily["activity_date"], y=icu_daily["total_arrivals"],
                    name="Arrivals", fill="tozeroy", line=dict(color="#FF9800"),
                ))
                fig.add_trace(go.Scatter(
                    x=icu_daily["activity_date"], y=icu_daily["total_discharges"],
                    name="Discharges", fill="tozeroy", line=dict(color="#9C27B0"),
                ))
                fig.update_layout(title="ICU Daily Arrivals vs Discharges", height=350, margin=dict(t=40))
                st.plotly_chart(fig, use_container_width=True)

                if icu_daily["avg_los_hours"].notna().any():
                    fig_los = px.bar(
                        icu_daily.dropna(subset=["avg_los_hours"]),
                        x="activity_date", y="avg_los_hours",
                        title="ICU Average Length of Stay (hours)",
                        color_discrete_sequence=["#00BCD4"],
                    )
                    fig_los.update_layout(height=300, margin=dict(t=40))
                    st.plotly_chart(fig_los, use_container_width=True)

                with st.expander("Full ICU Daily Data"):
                    st.dataframe(icu_daily, use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"Failed to load ICU daily summary: {e}")

st.markdown("---")

# ---- Hour-of-Day Heatmap ----
st.header("Arrival Patterns by Hour of Day")

wknd_choice = weekend_toggle(key="tr_wknd")

try:
    ed_hourly = run_query(queries.ED_HOURLY_ALL)
    if not ed_hourly.empty:
        ed_hourly = apply_facility(ed_hourly, fac_sel)
        ed_hourly = apply_weekend(ed_hourly, wknd_choice)

        if ed_hourly.empty:
            st.info("No hourly data for selected filters.")
        else:
            DAY_LABELS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]

            pivot = (
                ed_hourly
                .groupby(["day_of_week", "hour_of_day"])["arrivals"]
                .mean()
                .reset_index()
            )
            heatmap_data = pivot.pivot(
                index="day_of_week", columns="hour_of_day", values="arrivals"
            ).fillna(0)

            heatmap_data.index = [
                DAY_LABELS[int(d) - 1] if 1 <= int(d) <= 7 else str(d)
                for d in heatmap_data.index
            ]

            fig = px.imshow(
                heatmap_data,
                labels=dict(x="Hour of Day", y="Day of Week", color="Avg Arrivals"),
                title="ED Average Arrivals by Hour & Day of Week",
                color_continuous_scale="YlOrRd",
                aspect="auto",
            )
            fig.update_layout(height=350, margin=dict(t=40))
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No hourly data available for heatmap.")

except Exception as e:
    st.error(f"Failed to build heatmap: {e}")

st.markdown("---")

# ---- ED vs ICU Comparison ----
st.header("ED vs ICU Daily Comparison")

try:
    ed_d = apply_facility(ed_daily_raw.copy(), fac_sel) if not ed_daily_raw.empty else pd.DataFrame()
    icu_d = apply_facility(icu_daily_raw.copy(), fac_sel) if not icu_daily_raw.empty else pd.DataFrame()

    if not ed_d.empty:
        ed_d["activity_date"] = pd.to_datetime(ed_d["activity_date"])
        ed_d = apply_date_range(ed_d, start_date, end_date)
    if not icu_d.empty:
        icu_d["activity_date"] = pd.to_datetime(icu_d["activity_date"])
        icu_d = apply_date_range(icu_d, start_date, end_date)

    if not ed_d.empty and not icu_d.empty:
        ed_agg = (
            ed_d.groupby("activity_date")
            .agg({"total_arrivals": "sum", "total_discharges": "sum"})
            .reset_index()
            .rename(columns={"total_arrivals": "ed_arrivals", "total_discharges": "ed_discharges"})
        )
        icu_agg = (
            icu_d.groupby("activity_date")
            .agg({"total_arrivals": "sum", "total_discharges": "sum"})
            .reset_index()
            .rename(columns={"total_arrivals": "icu_arrivals", "total_discharges": "icu_discharges"})
        )
        comparison = pd.merge(ed_agg, icu_agg, on="activity_date", how="outer").fillna(0)
        comparison = comparison.sort_values("activity_date")

        fig = go.Figure()
        fig.add_trace(go.Bar(x=comparison["activity_date"], y=comparison["ed_arrivals"],
                             name="ED Arrivals", marker_color="#4CAF50"))
        fig.add_trace(go.Bar(x=comparison["activity_date"], y=comparison["icu_arrivals"],
                             name="ICU Arrivals", marker_color="#FF9800"))
        fig.update_layout(barmode="group", height=350, margin=dict(t=30),
                          legend=dict(orientation="h", y=1.1))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Insufficient data for comparison.")

except Exception as e:
    st.error(f"Failed to load comparison data: {e}")

st.markdown("---")
st.header("Encounter volume by patient class")
st.caption("Daily counts from **`gold_encounter_fact`** (DLT gold) — useful for capacity and mix discussions.")

try:
    ev = run_query(queries.ENCOUNTER_VOLUME_TREND)
    if not ev.empty:
        ev["encounter_date"] = pd.to_datetime(ev["encounter_date"])
        ev["encounter_count"] = pd.to_numeric(ev["encounter_count"], errors="coerce").fillna(0)
        fig_ev = px.area(
            ev,
            x="encounter_date",
            y="encounter_count",
            color="patient_class_desc",
            title="Encounters per day by patient class",
        )
        fig_ev.update_layout(height=380, margin=dict(t=40), legend=dict(orientation="h", y=-0.2))
        st.plotly_chart(fig_ev, use_container_width=True)
    else:
        st.info("No encounter trend data.")
except Exception as e:
    st.warning(f"Encounter volume chart unavailable: {e}")

st.caption("All trends computed from Lakebase Postgres gold tables.")
