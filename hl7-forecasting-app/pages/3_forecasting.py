"""
ML Forecasting - Predicted vs actual, confidence intervals, multi-horizon view.
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

from utils.db import run_query
from utils import queries
from utils.theme import apply_theme
from utils.navigation import render_sidebar_nav
from utils.streamlit_refresh import run_live_dashboard

st.set_page_config(page_title="ML Forecasting", page_icon="🔮", layout="wide")
apply_theme()
render_sidebar_nav()
st.title("ML Forecasting Predictions")

with st.expander("What this page does — and how to read it", expanded=False):
    st.markdown(
        """
**Purpose**  
Shows **ML model outputs**: what the trained forecasts say for upcoming hours, and (when backfilled) how those numbers compared to what actually happened.

---

**What you are looking at**  
Databricks **AutoML** trained separate models on historical features (lags, rolling averages, calendar effects, etc.). A scheduled **inference job** scores the latest features and writes **predictions** into Delta; this app reads them from Lakebase.

**Columns in the tables**  
| Column | Meaning |
|--------|---------|
| **Target metric** | What we try to predict: arrivals, discharges, or (combined) total arrivals. |
| **Horizon** | How far ahead: **1h**, **4h**, **8h**, or **24h** after the feature snapshot. |
| **Predicted value** | The model’s best guess for that target hour. |
| **Lower / upper bound** | An uncertainty range; the model is calibrated so reality often falls **between** these (often ~90% of the time when trained that way). |
| **Confidence %** | Related to how wide that band is (higher often means a narrower, more aggressive band—depends on training). |
| **Actual** | Real count from hourly census **after** that hour happened (filled by a backfill step). |
| **Error** | How far off we were once actual is known. |

**Forecast timeline chart**  
- **Blue line** — Predicted values over **target hour**.  
- **Shaded band** — Lower–upper uncertainty.  
- **Green diamonds** — **Actual** values (only where backfill has run).

**Predicted vs actual scatter**  
Each point is one scored prediction. The **dashed diagonal** is “perfect” (predicted = actual). Points **above** the line = we predicted **too high**; **below** = too low.
        """
    )

def _forecasting_body() -> None:
    # ---- Latest Forecast Predictions ----
    st.header("Latest Forecast Predictions")
    st.caption("Most recent **forecast_generated_at** batch; ED and ICU side by side.")
    
    try:
        preds = run_query(queries.LATEST_PREDICTIONS)
    
        if preds.empty:
            st.warning(
                "No predictions available. Run the AutoML training and inference jobs first."
            )
        else:
            latest_gen = preds["forecast_generated_at"].max()
            latest_preds = preds[preds["forecast_generated_at"] == latest_gen]
    
            st.caption(f"Forecast generated at: **{latest_gen}**")
    
            col1, col2 = st.columns(2)
    
            ed_preds = latest_preds[latest_preds["department"] == "ED"]
            icu_preds = latest_preds[latest_preds["department"] == "ICU"]
    
            with col1:
                st.subheader("ED Forecasts")
                if not ed_preds.empty:
                    st.dataframe(
                        ed_preds[["target_metric", "forecast_horizon_hours",
                                  "predicted_value", "lower_bound", "upper_bound",
                                  "confidence_pct", "actual_value", "error"]],
                        use_container_width=True,
                        hide_index=True,
                    )
                else:
                    st.info("No ED predictions.")
    
            with col2:
                st.subheader("ICU Forecasts")
                if not icu_preds.empty:
                    st.dataframe(
                        icu_preds[["target_metric", "forecast_horizon_hours",
                                   "predicted_value", "lower_bound", "upper_bound",
                                   "confidence_pct", "actual_value", "error"]],
                        use_container_width=True,
                        hide_index=True,
                    )
                else:
                    st.info("No ICU predictions.")
    
    except Exception as e:
        st.error(f"Failed to load predictions: {e}")
    
    st.markdown("---")
    
    # ---- Forecast Timeline with Confidence Bands ----
    st.header("Forecast Timeline")
    st.caption("Pick department, metric, and horizon; compare predicted path to actuals when available.")
    
    try:
        preds_7d = run_query(queries.PREDICTIONS_LAST_7D)
    
        if not preds_7d.empty:
            preds_7d["target_hour"] = pd.to_datetime(preds_7d["target_hour"])
    
            departments = sorted(preds_7d["department"].dropna().unique())
            metrics = sorted(preds_7d["target_metric"].dropna().unique())
            horizons = sorted(preds_7d["forecast_horizon_hours"].dropna().unique())
    
            sel_dept = st.selectbox("Department", departments, key="fc_dept")
            sel_metric = st.selectbox("Metric", metrics, key="fc_metric")
            sel_horizon = st.selectbox("Horizon (hours)", horizons, key="fc_horizon")
    
            filtered = preds_7d[
                (preds_7d["department"] == sel_dept)
                & (preds_7d["target_metric"] == sel_metric)
                & (preds_7d["forecast_horizon_hours"] == sel_horizon)
            ].sort_values("target_hour")
    
            if not filtered.empty:
                fig = go.Figure()
    
                if filtered["prediction_upper_bound"].notna().any():
                    fig.add_trace(go.Scatter(
                        x=filtered["target_hour"],
                        y=filtered["prediction_upper_bound"],
                        mode="lines",
                        line=dict(width=0),
                        showlegend=False,
                        name="Upper Bound",
                    ))
                    fig.add_trace(go.Scatter(
                        x=filtered["target_hour"],
                        y=filtered["prediction_lower_bound"],
                        mode="lines",
                        line=dict(width=0),
                        fill="tonexty",
                        fillcolor="rgba(33, 150, 243, 0.15)",
                        showlegend=True,
                        name="Confidence Band",
                    ))
    
                fig.add_trace(go.Scatter(
                    x=filtered["target_hour"],
                    y=filtered["predicted_value"],
                    mode="lines+markers",
                    name="Predicted",
                    line=dict(color="#2196F3", width=2),
                    marker=dict(size=5),
                ))
    
                if filtered["actual_value"].notna().any():
                    fig.add_trace(go.Scatter(
                        x=filtered["target_hour"],
                        y=filtered["actual_value"],
                        mode="markers",
                        name="Actual",
                        marker=dict(color="#4CAF50", size=8, symbol="diamond"),
                    ))
    
                fig.update_layout(
                    title=f"{sel_dept} {sel_metric} - {sel_horizon}h Horizon",
                    xaxis_title="Target Hour",
                    yaxis_title=sel_metric.replace("_", " ").title(),
                    height=450,
                    margin=dict(t=40),
                    legend=dict(orientation="h", y=1.12),
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No data for the selected combination.")
        else:
            st.info("No predictions in the last 7 days.")
    
    except Exception as e:
        st.error(f"Failed to build forecast timeline: {e}")
    
    st.markdown("---")
    
    # ---- Predicted vs Actual Scatter ----
    st.header("Predicted vs Actual")
    st.caption("Only points with both predicted and **actual** values. Closer to the diagonal = better calibration.")
    
    try:
        pva = run_query(queries.PREDICTIONS_VS_ACTUALS)
    
        if not pva.empty:
            fig = px.scatter(
                pva,
                x="actual_value",
                y="predicted_value",
                color="department",
                symbol="target_metric",
                hover_data=["model_name", "forecast_horizon_hours", "absolute_error"],
                title="Predicted vs Actual Values",
                color_discrete_map={"ED": "#4CAF50", "ICU": "#FF9800"},
            )
            max_val = max(
                pva["actual_value"].max(),
                pva["predicted_value"].max(),
            )
            fig.add_trace(go.Scatter(
                x=[0, max_val],
                y=[0, max_val],
                mode="lines",
                line=dict(dash="dash", color="gray"),
                name="Perfect Prediction",
                showlegend=True,
            ))
            fig.update_layout(height=450, margin=dict(t=40))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No predictions with actual values available yet.")
    
    except Exception as e:
        st.error(f"Failed to load predicted vs actual: {e}")
    
    st.markdown("---")
    st.header("Latest inference batch by horizon")
    st.caption("Row counts per **forecast_horizon_hours** in the newest **forecast_generated_at** slice (job output shape).")
    
    try:
        preds_hz = run_query(queries.LATEST_PREDICTIONS)
        if not preds_hz.empty:
            latest_ts = preds_hz["forecast_generated_at"].max()
            batch = preds_hz[preds_hz["forecast_generated_at"] == latest_ts]
            hz = (
                batch.groupby("forecast_horizon_hours", as_index=False)
                .size()
                .rename(columns={"size": "prediction_rows"})
            )
            hz["forecast_horizon_hours"] = pd.to_numeric(hz["forecast_horizon_hours"], errors="coerce")
            hz = hz.sort_values("forecast_horizon_hours")
            hz["horizon_label"] = hz["forecast_horizon_hours"].astype("Int64").astype(str) + "h"
            fig_hz = px.bar(
                hz,
                x="horizon_label",
                y="prediction_rows",
                text_auto=True,
                color="horizon_label",
                color_discrete_sequence=px.colors.qualitative.Bold,
                title=f"Horizons for run at {latest_ts}",
            )
            fig_hz.update_layout(height=320, margin=dict(t=40), showlegend=False, xaxis_title="Forecast horizon")
            st.plotly_chart(fig_hz, use_container_width=True)
    except Exception as e:
        st.warning(f"Horizon summary unavailable: {e}")
    
    st.caption("Predictions generated by MLflow AutoML models via Databricks inference jobs.")

run_live_dashboard(_forecasting_body, interval_seconds=22, manual_key="hl7_forecasting_live_refresh")
