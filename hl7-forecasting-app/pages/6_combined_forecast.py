"""
Combined Forecasting - ED + ICU system pressure, cross-department analysis with filters.
"""

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import pandas as pd
import numpy as np

from utils.db import run_query
from utils import queries
from utils.filters import (
    sidebar_section, date_range_filter, weekend_toggle,
    apply_date_range, apply_weekend,
)

st.set_page_config(page_title="Combined Forecasting", page_icon="📈", layout="wide")
st.title("Combined ED + ICU Forecasting")

sidebar_section("Combined Filters")

# ---- System Pressure ----
st.header("System Pressure (ED + ICU)")

try:
    combined_df = run_query(queries.COMBINED_FEATURES)

    if combined_df.empty:
        st.info("No combined forecast data available. Run the DLT pipeline and Lakebase load job.")
    else:
        combined_df["event_hour"] = pd.to_datetime(combined_df["event_hour"])

        dr_start, dr_end = date_range_filter(
            combined_df, col="event_hour", key="cb_dr", label="Event Date Range",
        )
        combined_df = apply_date_range(combined_df, dr_start, dr_end, col="event_hour")

        wknd = weekend_toggle(key="cb_wknd")
        combined_df = apply_weekend(combined_df, wknd)

        if combined_df.empty:
            st.info("No data for selected filters.")
        else:
            last_row = combined_df.iloc[-1]
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Net System Pressure", int(last_row.get("net_system_pressure", 0)))
            c2.metric("ED:ICU Ratio", f"{last_row.get('ed_to_icu_ratio', 0):.2f}")
            c3.metric("Total Arrivals (last hr)", int(last_row.get("total_arrivals", 0)))
            c4.metric("Total Discharges (last hr)", int(last_row.get("total_discharges", 0)))
            c5.metric("Data Points", f"{len(combined_df):,}")

            st.markdown("---")

            fig = make_subplots(
                rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08,
                subplot_titles=("ED vs ICU Arrivals", "Net System Pressure"),
            )
            fig.add_trace(
                go.Scatter(x=combined_df["event_hour"], y=combined_df["ed_arrivals"],
                           name="ED Arrivals", line=dict(color="#4CAF50", width=2)),
                row=1, col=1,
            )
            fig.add_trace(
                go.Scatter(x=combined_df["event_hour"], y=combined_df["icu_arrivals"],
                           name="ICU Arrivals", line=dict(color="#FF9800", width=2)),
                row=1, col=1,
            )
            fig.add_trace(
                go.Scatter(x=combined_df["event_hour"], y=combined_df["ed_discharges"],
                           name="ED Discharges", line=dict(color="#4CAF50", width=1, dash="dot")),
                row=1, col=1,
            )
            fig.add_trace(
                go.Scatter(x=combined_df["event_hour"], y=combined_df["icu_discharges"],
                           name="ICU Discharges", line=dict(color="#FF9800", width=1, dash="dot")),
                row=1, col=1,
            )

            pressure = combined_df["net_system_pressure"].astype(float)
            colors = ["#F44336" if p > 0 else "#4CAF50" for p in pressure]
            fig.add_trace(
                go.Bar(x=combined_df["event_hour"], y=pressure,
                       name="Net System Pressure", marker_color=colors, opacity=0.7),
                row=2, col=1,
            )
            fig.update_layout(height=600, margin=dict(t=50),
                              legend=dict(orientation="h", y=1.05), showlegend=True)
            st.plotly_chart(fig, use_container_width=True)

except Exception as e:
    st.error(f"Failed to load combined features: {e}")

st.markdown("---")

# ---- ED-to-ICU Ratio Trend ----
st.header("ED-to-ICU Arrival Ratio Over Time")

try:
    if not combined_df.empty:
        fig_ratio = go.Figure()
        ratio = combined_df["ed_to_icu_ratio"].astype(float)
        fig_ratio.add_trace(
            go.Scatter(x=combined_df["event_hour"], y=ratio,
                       name="ED:ICU Ratio", line=dict(color="#2196F3", width=2),
                       fill="tozeroy", fillcolor="rgba(33,150,243,0.1)")
        )
        avg_ratio = ratio.mean()
        fig_ratio.add_hline(y=avg_ratio, line_dash="dash", line_color="red",
                            annotation_text=f"Avg: {avg_ratio:.2f}")
        fig_ratio.update_layout(height=350, margin=dict(t=30), yaxis_title="Ratio")
        st.plotly_chart(fig_ratio, use_container_width=True)
    else:
        st.info("No data available for ratio chart.")
except Exception as e:
    st.error(f"Failed to render ratio chart: {e}")

st.markdown("---")

# ---- Rolling Averages Comparison ----
st.header("24-Hour Rolling Averages")

try:
    if not combined_df.empty:
        fig_rolling = make_subplots(specs=[[{"secondary_y": True}]])

        for col_name, label, color in [
            ("ed_arrivals_rolling_24h", "ED Arrivals (24h)", "#4CAF50"),
            ("icu_arrivals_rolling_24h", "ICU Arrivals (24h)", "#FF9800"),
            ("ed_discharges_rolling_24h", "ED Discharges (24h)", "#2196F3"),
            ("icu_discharges_rolling_24h", "ICU Discharges (24h)", "#9C27B0"),
        ]:
            if col_name in combined_df.columns:
                fig_rolling.add_trace(
                    go.Scatter(x=combined_df["event_hour"],
                               y=combined_df[col_name].astype(float),
                               name=label, line=dict(color=color, width=2)),
                    secondary_y=False,
                )
        fig_rolling.update_layout(height=400, margin=dict(t=30),
                                  legend=dict(orientation="h", y=1.12))
        fig_rolling.update_yaxes(title_text="24h Rolling Total")
        st.plotly_chart(fig_rolling, use_container_width=True)
    else:
        st.info("No rolling average data available.")
except Exception as e:
    st.error(f"Failed to render rolling averages: {e}")

st.markdown("---")

# ---- Feature Heatmaps ----
st.header("Hour-of-Day Arrival Patterns")

tab_ed_feat, tab_icu_feat = st.tabs(["ED Features", "ICU Features"])

with tab_ed_feat:
    try:
        ed_feat = run_query(queries.ED_FEATURES_SUMMARY)
        if ed_feat.empty:
            st.info("No ED feature data available.")
        else:
            ed_feat["event_hour"] = pd.to_datetime(ed_feat["event_hour"])
            ed_feat = apply_date_range(ed_feat, dr_start, dr_end, col="event_hour")
            ed_feat["date"] = ed_feat["event_hour"].dt.date
            ed_feat["hour"] = ed_feat["event_hour"].dt.hour

            pivot = ed_feat.pivot_table(
                values="arrivals", index="date", columns="hour",
                aggfunc="sum", fill_value=0,
            )
            fig = px.imshow(
                pivot, aspect="auto", color_continuous_scale="YlOrRd",
                title="ED Arrivals Heatmap (Date x Hour)",
                labels=dict(x="Hour of Day", y="Date", color="Arrivals"),
            )
            fig.update_layout(height=450, margin=dict(t=40))
            st.plotly_chart(fig, use_container_width=True)

            with st.expander("ED Feature Details"):
                st.dataframe(
                    ed_feat[["event_hour", "arrivals", "discharges", "arrivals_rolling_24h",
                             "cumulative_net_census", "is_night_shift", "is_holiday_window"]],
                    use_container_width=True, hide_index=True,
                )
    except Exception as e:
        st.error(f"Failed to load ED features: {e}")

with tab_icu_feat:
    try:
        icu_feat = run_query(queries.ICU_FEATURES_SUMMARY)
        if icu_feat.empty:
            st.info("No ICU feature data available.")
        else:
            icu_feat["event_hour"] = pd.to_datetime(icu_feat["event_hour"])
            icu_feat = apply_date_range(icu_feat, dr_start, dr_end, col="event_hour")
            icu_feat["date"] = icu_feat["event_hour"].dt.date
            icu_feat["hour"] = icu_feat["event_hour"].dt.hour

            pivot = icu_feat.pivot_table(
                values="arrivals", index="date", columns="hour",
                aggfunc="sum", fill_value=0,
            )
            fig = px.imshow(
                pivot, aspect="auto", color_continuous_scale="YlGnBu",
                title="ICU Arrivals Heatmap (Date x Hour)",
                labels=dict(x="Hour of Day", y="Date", color="Arrivals"),
            )
            fig.update_layout(height=450, margin=dict(t=40))
            st.plotly_chart(fig, use_container_width=True)

            with st.expander("ICU Feature Details"):
                st.dataframe(
                    icu_feat[["event_hour", "arrivals", "discharges", "arrivals_rolling_24h",
                              "cumulative_net_census", "is_night_shift", "is_holiday_window"]],
                    use_container_width=True, hide_index=True,
                )
    except Exception as e:
        st.error(f"Failed to load ICU features: {e}")

st.caption("Data sourced from Lakebase Postgres gold tables.")
