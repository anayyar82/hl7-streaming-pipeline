"""
Patient & Clinical Analytics - Demographics, diagnoses, labs, allergies, orders with filters.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

from utils.db import run_query
from utils import queries
from utils.filters import sidebar_section
from utils.theme import apply_theme

st.set_page_config(page_title="Patient & Clinical", page_icon="🏥", layout="wide")
apply_theme()
st.title("Patient & Clinical Analytics")

with st.expander("What this page does", expanded=False):
    st.markdown(
        """
**Purpose**  
**Who** is represented in the HL7-derived gold data—not operational census charts. Use it for population mix, coding, and clinical activity summaries.

**What you will see**  
- **Demographics** (counts, sex distribution) and high-level patient dimension stats.  
- **Diagnoses** (top codes/descriptions by volume).  
- **Labs / observations** (common tests, abnormal rates where available).  
- **Allergies** and **orders** (medication or other order activity by type or provider).

**Filters**  
Sidebar **Clinical filters** narrow coding system, severity, priority, provider search, etc., depending on the chart.

**Data note**  
All figures come from **de-identified or demo** pipeline data in Lakebase; treat as analytic aggregates, not a source of truth for individual patient care.
        """
    )

sidebar_section("Clinical Filters")

# ---- Patient Demographics ----
st.header("Patient Demographics")

col_left, col_right = st.columns(2)

with col_left:
    try:
        counts_df = run_query(queries.PATIENT_COUNTS)
        if not counts_df.empty:
            c1, c2, c3 = st.columns(3)
            c1.metric("Total Patients", f"{int(counts_df['total_patients'].iloc[0]):,}")
            c2.metric("States", int(counts_df['states_represented'].iloc[0]))
            c3.metric("Source Systems", int(counts_df['source_systems'].iloc[0]))
    except Exception as e:
        st.error(f"Failed to load patient counts: {e}")

with col_right:
    try:
        demo_df = run_query(queries.PATIENT_DEMOGRAPHICS)
        if not demo_df.empty:
            fig = px.pie(
                demo_df, names="sex", values="patient_count",
                title="Patient Sex Distribution", hole=0.4,
            )
            fig.update_layout(height=300, margin=dict(t=40, b=20))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No demographics data available.")
    except Exception as e:
        st.error(f"Failed to load demographics: {e}")

st.markdown("---")

# ---- Top Diagnoses ----
st.header("Top Diagnoses")

try:
    diag_df = run_query(queries.TOP_DIAGNOSES)
    if diag_df.empty:
        st.info("No diagnosis data available.")
    else:
        coding_systems = sorted(diag_df["diagnosis_coding_system"].dropna().unique())
        sel_systems = st.sidebar.multiselect(
            "Coding System", coding_systems, default=coding_systems, key="diag_sys",
        )
        if sel_systems:
            diag_filtered = diag_df[diag_df["diagnosis_coding_system"].isin(sel_systems)]
        else:
            diag_filtered = diag_df

        n_top = st.sidebar.slider("Top N Diagnoses", 5, 20, 15, key="diag_n")

        if diag_filtered.empty:
            st.info("No diagnoses for selected coding systems.")
        else:
            m1, m2, m3 = st.columns(3)
            m1.metric("Unique Diagnoses", len(diag_filtered))
            m2.metric("Total Occurrences", f"{int(diag_filtered['diagnosis_count'].sum()):,}")
            m3.metric("Unique Patients", f"{int(diag_filtered['unique_patients'].sum()):,}")

            fig = px.bar(
                diag_filtered.head(n_top),
                x="diagnosis_count", y="diagnosis_description",
                orientation="h", color="unique_patients",
                color_continuous_scale="Teal",
                labels={"diagnosis_count": "Count", "diagnosis_description": "Diagnosis",
                        "unique_patients": "Unique Patients"},
            )
            fig.update_layout(height=max(350, n_top * 28), margin=dict(l=0),
                              yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig, use_container_width=True)

            with st.expander("Diagnosis Data"):
                st.dataframe(diag_filtered, use_container_width=True, hide_index=True)
except Exception as e:
    st.error(f"Failed to load diagnoses: {e}")

st.markdown("---")

# ---- Lab / Observation Results ----
st.header("Lab & Observation Results")

tab_summary, tab_abnormal = st.tabs(["Summary by Test", "Abnormal Flags"])

with tab_summary:
    try:
        obs_df = run_query(queries.OBSERVATION_SUMMARY)
        if obs_df.empty:
            st.info("No observation data available.")
        else:
            value_types = sorted(obs_df["value_type"].dropna().unique())
            sel_vtypes = st.sidebar.multiselect(
                "Value Type", value_types, default=value_types, key="obs_vtype",
            )
            obs_filtered = obs_df[obs_df["value_type"].isin(sel_vtypes)] if sel_vtypes else obs_df

            obs_filtered["abnormal_pct"] = (
                obs_filtered["abnormal_count"] / obs_filtered["obs_count"] * 100
            ).round(1)

            m1, m2, m3 = st.columns(3)
            m1.metric("Tests Tracked", len(obs_filtered))
            m2.metric("Total Observations", f"{int(obs_filtered['obs_count'].sum()):,}")
            m3.metric("Abnormal Rate",
                       f"{obs_filtered['abnormal_pct'].mean():.1f}%" if not obs_filtered.empty else "N/A")

            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=obs_filtered["observation_text"], y=obs_filtered["obs_count"],
                name="Total Observations", marker_color="#2196F3",
            ))
            fig.add_trace(go.Bar(
                x=obs_filtered["observation_text"], y=obs_filtered["abnormal_count"],
                name="Abnormal", marker_color="#F44336",
            ))
            fig.update_layout(barmode="group", height=400, margin=dict(t=30),
                              xaxis_tickangle=-45, legend=dict(orientation="h", y=1.1))
            st.plotly_chart(fig, use_container_width=True)

            with st.expander("Observation Details"):
                st.dataframe(obs_filtered, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Failed to load observations: {e}")

with tab_abnormal:
    try:
        abn_df = run_query(queries.ABNORMAL_FLAGS_DIST)
        if abn_df.empty:
            st.info("No abnormal flag data available.")
        else:
            fig = px.bar(
                abn_df, x="abnormal_flags", y="flag_count",
                title="Abnormal Flag Distribution",
                color="flag_count", color_continuous_scale="Reds",
            )
            fig.update_layout(height=350, margin=dict(t=40))
            st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Failed to load abnormal flags: {e}")

st.markdown("---")

# ---- Allergies ----
st.header("Allergy Overview")

col_allergy, col_severity = st.columns(2)

with col_allergy:
    try:
        allergy_df = run_query(queries.ALLERGY_OVERVIEW)
        if allergy_df.empty:
            st.info("No allergy data available.")
        else:
            sev_filter = st.sidebar.radio(
                "Allergy Severity", ["All", "Severe Only", "Non-Severe"],
                key="alg_sev", horizontal=True,
            )
            if sev_filter == "Severe Only":
                allergy_show = allergy_df[allergy_df["is_severe"] == True]  # noqa: E712
            elif sev_filter == "Non-Severe":
                allergy_show = allergy_df[allergy_df["is_severe"] == False]  # noqa: E712
            else:
                allergy_show = allergy_df

            fig = px.bar(
                allergy_show.head(10),
                x="allergy_count", y="allergen_description",
                orientation="h", color="is_severe",
                title="Top Allergens",
                labels={"allergy_count": "Count", "allergen_description": "Allergen",
                        "is_severe": "Severe"},
            )
            fig.update_layout(height=400, margin=dict(l=0), yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Failed to load allergies: {e}")

with col_severity:
    try:
        sev_df = run_query(queries.ALLERGY_SEVERITY_DIST)
        if sev_df.empty:
            st.info("No severity data available.")
        else:
            fig = px.pie(
                sev_df, names="severity", values="allergy_count",
                title="Severity Distribution", hole=0.4,
            )
            fig.update_layout(height=400, margin=dict(t=40))
            st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Failed to load allergy severity: {e}")

st.markdown("---")

# ---- Orders ----
st.header("Orders & Provider Activity")

tab_orders, tab_providers = st.tabs(["Top Orders", "Provider Volume"])

with tab_orders:
    try:
        order_df = run_query(queries.ORDER_ACTIVITY)
        if order_df.empty:
            st.info("No order data available.")
        else:
            priorities = sorted(order_df["priority"].dropna().unique())
            sel_priorities = st.sidebar.multiselect(
                "Order Priority", priorities, default=priorities, key="ord_pri",
            )
            order_show = order_df[order_df["priority"].isin(sel_priorities)] if sel_priorities else order_df

            m1, m2 = st.columns(2)
            m1.metric("Total Orders", f"{int(order_show['order_count'].sum()):,}")
            m2.metric("Unique Patients", f"{int(order_show['unique_patients'].sum()):,}")

            fig = px.treemap(
                order_show,
                path=["priority", "universal_service_text"],
                values="order_count", color="unique_patients",
                color_continuous_scale="Viridis",
                title="Order Activity by Priority and Service",
            )
            fig.update_layout(height=500, margin=dict(t=40))
            st.plotly_chart(fig, use_container_width=True)

            with st.expander("Order Details"):
                st.dataframe(order_show, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Failed to load orders: {e}")

with tab_providers:
    try:
        prov_df = run_query(queries.ORDER_PROVIDER_VOLUME)
        if prov_df.empty:
            st.info("No provider data available.")
        else:
            provider_search = st.sidebar.text_input("Search Provider", key="prov_search")
            prov_show = prov_df
            if provider_search:
                prov_show = prov_df[
                    prov_df["ordering_provider_name"]
                    .str.contains(provider_search, case=False, na=False)
                ]

            fig = px.bar(
                prov_show, x="order_count", y="ordering_provider_name",
                orientation="h", title="Top Ordering Providers",
                color="order_count", color_continuous_scale="Blues",
            )
            fig.update_layout(height=450, margin=dict(l=0), yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Failed to load provider data: {e}")

st.caption("Data sourced from Lakebase Postgres gold tables.")
