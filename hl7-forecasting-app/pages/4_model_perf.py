"""
Model Performance - Accuracy metrics, MAE/MAPE trends, model comparison.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

from utils.db import run_query
from utils import queries

st.set_page_config(page_title="Model Performance", page_icon="📉", layout="wide")
st.title("Model Performance Tracking")

# ---- Summary Metrics ----
st.header("Model Accuracy Summary")

try:
    summary = run_query(queries.ACCURACY_SUMMARY)

    if summary.empty:
        st.warning(
            "No accuracy data available. Predictions need actual values to compute accuracy. "
            "Run the inference job after the forecast horizon passes."
        )
    else:
        col1, col2, col3 = st.columns(3)
        col1.metric(
            "Models Tracked",
            summary["model_name"].nunique(),
        )
        col2.metric(
            "Overall Avg MAE",
            round(summary["avg_mae"].mean(), 2),
        )
        col3.metric(
            "Overall Avg MAPE",
            f"{round(summary['avg_mape_pct'].mean(), 1)}%",
        )

        st.dataframe(
            summary.rename(columns={
                "model_name": "Model",
                "department": "Dept",
                "target_metric": "Metric",
                "forecast_horizon_hours": "Horizon (h)",
                "total_periods": "Periods",
                "avg_mae": "Avg MAE",
                "avg_mape_pct": "Avg MAPE %",
                "avg_coverage_pct": "Coverage %",
                "total_predictions": "Total Predictions",
            }),
            use_container_width=True,
            hide_index=True,
        )

        st.markdown("---")

        # ---- MAE by Model & Horizon ----
        st.subheader("Mean Absolute Error by Model & Horizon")

        fig = px.bar(
            summary,
            x="model_name",
            y="avg_mae",
            color="forecast_horizon_hours",
            barmode="group",
            title="Average MAE by Model and Forecast Horizon",
            labels={
                "model_name": "Model",
                "avg_mae": "Avg MAE",
                "forecast_horizon_hours": "Horizon (h)",
            },
            color_continuous_scale="Viridis",
        )
        fig.update_layout(height=400, margin=dict(t=40))
        st.plotly_chart(fig, use_container_width=True)

        # ---- Coverage Chart ----
        st.subheader("Prediction Interval Coverage")

        fig_cov = px.bar(
            summary,
            x="model_name",
            y="avg_coverage_pct",
            color="department",
            barmode="group",
            title="Confidence Interval Coverage (%) - Target: ~90%",
            color_discrete_map={"ED": "#4CAF50", "ICU": "#FF9800"},
        )
        fig_cov.add_hline(
            y=90, line_dash="dash", line_color="red",
            annotation_text="90% target",
        )
        fig_cov.update_layout(height=350, margin=dict(t=40))
        st.plotly_chart(fig_cov, use_container_width=True)

except Exception as e:
    st.error(f"Failed to load accuracy summary: {e}")

st.markdown("---")

# ---- Accuracy Over Time ----
st.header("Accuracy Trends Over Time")

try:
    accuracy = run_query(queries.FORECAST_ACCURACY)

    if not accuracy.empty:
        accuracy["prediction_date"] = pd.to_datetime(accuracy["prediction_date"])

        models = sorted(accuracy["model_name"].dropna().unique())
        selected_model = st.selectbox("Select Model", models, key="perf_model")

        model_data = accuracy[accuracy["model_name"] == selected_model].sort_values(
            "prediction_date"
        )

        if not model_data.empty:
            col1, col2 = st.columns(2)

            with col1:
                fig_mae = px.line(
                    model_data,
                    x="prediction_date",
                    y="mae",
                    color="forecast_horizon_hours",
                    title=f"MAE Over Time - {selected_model}",
                    markers=True,
                )
                fig_mae.update_layout(height=350, margin=dict(t=40))
                st.plotly_chart(fig_mae, use_container_width=True)

            with col2:
                if model_data["mape"].notna().any():
                    fig_mape = px.line(
                        model_data.dropna(subset=["mape"]),
                        x="prediction_date",
                        y="mape",
                        color="forecast_horizon_hours",
                        title=f"MAPE (%) Over Time - {selected_model}",
                        markers=True,
                    )
                    fig_mape.update_layout(height=350, margin=dict(t=40))
                    st.plotly_chart(fig_mape, use_container_width=True)
                else:
                    st.info("No MAPE data available for this model.")

            with st.expander("Raw Accuracy Data"):
                st.dataframe(model_data, use_container_width=True, hide_index=True)
        else:
            st.info("No data for the selected model.")
    else:
        st.info("No accuracy data available yet.")

except Exception as e:
    st.error(f"Failed to load accuracy trends: {e}")

st.markdown("---")

# ---- Model Comparison ----
st.header("Model Comparison")

try:
    summary = run_query(queries.ACCURACY_SUMMARY)

    if not summary.empty:
        fig = px.scatter(
            summary,
            x="avg_mae",
            y="avg_mape_pct",
            size="total_predictions",
            color="model_name",
            symbol="department",
            hover_data=["forecast_horizon_hours", "avg_coverage_pct"],
            title="Model Comparison: MAE vs MAPE (bubble size = total predictions)",
        )
        fig.update_layout(
            height=450,
            margin=dict(t=40),
            xaxis_title="Average MAE",
            yaxis_title="Average MAPE (%)",
        )
        st.plotly_chart(fig, use_container_width=True)

except Exception as e:
    st.error(f"Failed to build model comparison: {e}")

st.caption("Accuracy computed by comparing predictions to actuals after the forecast horizon passes.")
