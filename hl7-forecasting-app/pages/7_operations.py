"""
Operations & Pipeline Monitor - Message throughput, pipeline health, patient activity with filters.
"""

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import pandas as pd

from utils.db import run_query
from utils import queries
from utils.filters import (
    sidebar_section, facility_filter, date_range_filter,
    apply_facility, apply_date_range,
)

st.set_page_config(page_title="Operations", page_icon="⚙️", layout="wide")
st.title("Pipeline Operations & Data Monitor")

with st.expander("What this page does", expanded=False):
    st.markdown(
        """
**Purpose**  
**Ingestion and pipeline health**: are HL7 messages flowing, and how does activity break down—separate from bedside ED/ICU census views.

**What you will see**  
- **Data freshness**: latest raw/processed message timestamps and metric row counts.  
- **Throughput over time**: message volume trends.  
- **Pipeline / message-type** views: what kinds of traffic the stream carries.  
- **Patient activity by class** and related operational breakdowns (where implemented in queries).

**Filters**  
Date range, message type, facility, patient class—so you can focus on a slice of traffic.

**When to use it**  
Suspected ingest delays, empty downstream tables, or demos of “how much HL7 we process.”
        """
    )

sidebar_section("Operations Filters")

# ---- Data Freshness ----
st.header("Data Freshness")

try:
    fresh_df = run_query(queries.DATA_FRESHNESS)
    if not fresh_df.empty and fresh_df["latest_message"].iloc[0] is not None:
        c1, c2, c3 = st.columns(3)
        c1.metric("Latest Message", str(fresh_df["latest_message"].iloc[0]))
        c2.metric("Latest Processing Hour", str(fresh_df["latest_processing_hour"].iloc[0]))
        c3.metric("Total Metric Rows", f"{int(fresh_df['total_metric_rows'].iloc[0]):,}")
    else:
        st.warning("No message metrics available yet.")
except Exception as e:
    st.error(f"Failed to load data freshness: {e}")

st.markdown("---")

# ---- Message Throughput ----
st.header("Message Throughput Over Time")

try:
    throughput_df = run_query(queries.MESSAGE_THROUGHPUT)
    if throughput_df.empty:
        st.info("No throughput data available.")
    else:
        throughput_df["processing_hour"] = pd.to_datetime(throughput_df["processing_hour"])

        tp_start, tp_end = date_range_filter(
            throughput_df, col="processing_hour", key="ops_tp_dr", label="Throughput Date Range",
        )
        throughput_df = apply_date_range(throughput_df, tp_start, tp_end, col="processing_hour")

        if throughput_df.empty:
            st.info("No throughput data for selected dates.")
        else:
            m1, m2, m3 = st.columns(3)
            m1.metric("Total Messages", f"{int(throughput_df['total_messages'].sum()):,}")
            m2.metric("Unique Messages", f"{int(throughput_df['total_unique'].sum()):,}")
            m3.metric("Hours Covered", len(throughput_df))

            fig = make_subplots(
                rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08,
                subplot_titles=("Messages per Hour", "Message Types & Facilities"),
            )
            fig.add_trace(
                go.Bar(x=throughput_df["processing_hour"],
                       y=throughput_df["total_messages"].astype(int),
                       name="Total Messages", marker_color="#2196F3", opacity=0.8),
                row=1, col=1,
            )
            fig.add_trace(
                go.Scatter(x=throughput_df["processing_hour"],
                           y=throughput_df["total_unique"].astype(int),
                           name="Unique Messages", line=dict(color="#FF5722", width=2)),
                row=1, col=1,
            )
            fig.add_trace(
                go.Scatter(x=throughput_df["processing_hour"],
                           y=throughput_df["message_types"].astype(int),
                           name="Message Types", line=dict(color="#4CAF50", width=2)),
                row=2, col=1,
            )
            fig.add_trace(
                go.Scatter(x=throughput_df["processing_hour"],
                           y=throughput_df["facilities"].astype(int),
                           name="Facilities", line=dict(color="#9C27B0", width=2)),
                row=2, col=1,
            )
            fig.update_layout(height=550, margin=dict(t=50), legend=dict(orientation="h", y=1.05))
            st.plotly_chart(fig, use_container_width=True)
except Exception as e:
    st.error(f"Failed to load throughput data: {e}")

st.markdown("---")

# ---- Message Breakdown ----
st.header("Message Breakdown by Type & Facility")

try:
    msg_df = run_query(queries.MESSAGE_METRICS)
    if msg_df.empty:
        st.info("No message metrics available.")
    else:
        msg_df["processing_hour"] = pd.to_datetime(msg_df["processing_hour"])

        msg_types = sorted(msg_df["message_type"].dropna().unique())
        sel_msg_types = st.sidebar.multiselect(
            "Message Type", msg_types, default=msg_types, key="ops_mtype",
        )
        facilities = sorted(msg_df["sending_facility"].dropna().unique())
        sel_facilities = st.sidebar.multiselect(
            "Sending Facility", facilities, default=facilities, key="ops_sfac",
        )

        msg_filtered = msg_df.copy()
        if sel_msg_types:
            msg_filtered = msg_filtered[msg_filtered["message_type"].isin(sel_msg_types)]
        if sel_facilities:
            msg_filtered = msg_filtered[msg_filtered["sending_facility"].isin(sel_facilities)]

        if msg_filtered.empty:
            st.info("No data for selected message filters.")
        else:
            tab_type, tab_facility, tab_raw = st.tabs(
                ["By Message Type", "By Facility", "Raw Data"]
            )

            with tab_type:
                type_agg = (
                    msg_filtered.groupby("message_type", as_index=False)
                    .agg(total_messages=("message_count", "sum"),
                         total_unique=("unique_messages", "sum"))
                    .sort_values("total_messages", ascending=False)
                )
                fig = px.bar(
                    type_agg, x="message_type", y="total_messages",
                    color="total_unique", color_continuous_scale="Viridis",
                    title="Message Volume by Type",
                )
                fig.update_layout(height=350, margin=dict(t=40))
                st.plotly_chart(fig, use_container_width=True)

            with tab_facility:
                fac_agg = (
                    msg_filtered.groupby("sending_facility", as_index=False)
                    .agg(total_messages=("message_count", "sum"),
                         total_unique=("unique_messages", "sum"))
                    .sort_values("total_messages", ascending=False)
                )
                fig = px.bar(
                    fac_agg, x="sending_facility", y="total_messages",
                    color="total_unique", color_continuous_scale="Teal",
                    title="Message Volume by Facility",
                )
                fig.update_layout(height=350, margin=dict(t=40))
                st.plotly_chart(fig, use_container_width=True)

            with tab_raw:
                st.dataframe(msg_filtered, use_container_width=True, hide_index=True)
except Exception as e:
    st.error(f"Failed to load message metrics: {e}")

st.markdown("---")

# ---- Patient Activity ----
st.header("Patient Activity by Class")

try:
    activity_df = run_query(queries.PATIENT_ACTIVITY)
    if activity_df.empty:
        st.info("No patient activity data available.")
    else:
        activity_df["activity_date"] = pd.to_datetime(activity_df["activity_date"])

        fac_sel = facility_filter(activity_df, key="ops_fac")
        activity_df = apply_facility(activity_df, fac_sel)

        pa_start, pa_end = date_range_filter(
            activity_df, key="ops_pa_dr", label="Activity Date Range",
        )
        activity_df = apply_date_range(activity_df, pa_start, pa_end)

        patient_classes = sorted(activity_df["patient_class_desc"].dropna().unique())
        sel_classes = st.sidebar.multiselect(
            "Patient Class", patient_classes, default=patient_classes, key="ops_pclass",
        )
        if sel_classes:
            activity_df = activity_df[activity_df["patient_class_desc"].isin(sel_classes)]

        if activity_df.empty:
            st.info("No patient activity for selected filters.")
        else:
            m1, m2, m3 = st.columns(3)
            m1.metric("Total Encounters", f"{int(activity_df['encounter_count'].sum()):,}")
            m2.metric("Unique Patients", f"{int(activity_df['unique_patients'].sum()):,}")
            m3.metric("Activity Days", activity_df["activity_date"].nunique())

            fig = px.area(
                activity_df, x="activity_date", y="encounter_count",
                color="patient_class_desc",
                title="Encounter Volume by Patient Class",
            )
            fig.update_layout(height=400, margin=dict(t=40),
                              legend=dict(orientation="h", y=1.1))
            st.plotly_chart(fig, use_container_width=True)

            col_a, col_b = st.columns(2)

            with col_a:
                provider_agg = (
                    activity_df.groupby("patient_class_desc", as_index=False)
                    .agg(total_encounters=("encounter_count", "sum"),
                         total_patients=("unique_patients", "sum"),
                         total_providers=("unique_providers", "sum"))
                )
                st.subheader("Summary by Patient Class")
                st.dataframe(provider_agg, use_container_width=True, hide_index=True)

            with col_b:
                location_agg = (
                    activity_df.groupby("location_facility", as_index=False)
                    .agg(total_encounters=("encounter_count", "sum"),
                         total_patients=("unique_patients", "sum"))
                    .sort_values("total_encounters", ascending=False)
                )
                fig_loc = px.pie(
                    location_agg, names="location_facility", values="total_encounters",
                    title="Encounters by Facility", hole=0.4,
                )
                fig_loc.update_layout(height=350, margin=dict(t=40))
                st.plotly_chart(fig_loc, use_container_width=True)

except Exception as e:
    st.error(f"Failed to load patient activity: {e}")

st.caption("Data sourced from Lakebase Postgres gold tables.")
