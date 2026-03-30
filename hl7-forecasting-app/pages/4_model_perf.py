"""
Model Performance - Accuracy metrics, MAE/MAPE trends, model comparison.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

from utils.db import run_query
from utils import queries
from utils.theme import apply_theme
from utils.navigation import render_sidebar_nav

st.set_page_config(page_title="Model Performance", page_icon="📉", layout="wide")
apply_theme()
render_sidebar_nav()
st.title("Model Performance Tracking")

with st.expander("What this page does — and what the metrics mean", expanded=False):
    st.markdown(
        """
**Purpose**  
After predictions exist and **actual** counts are backfilled from hourly census, this page answers: **“How good were the models?”**

**What this page is for**  
We forecast ED/ICU arrivals and discharges. After the **target hour** has happened, we compare the forecast to real hourly census. This page shows how accurate those comparisons were.

| Term | Plain-language meaning |
|------|-------------------------|
| **MAE** (mean absolute error) | Average size of the mistake **in the same units as the thing you count** (e.g. patients per hour). **Lower is better.** Example: MAE 2.5 means we were off by about 2.5 on average. |
| **MAPE** (mean absolute percent error) | Error expressed as a **percent of the actual value**. **Lower is better.** Helps compare when volumes differ a lot. Omitted when actuals are zero. |
| **Forecast horizon (1h / 4h / 8h / 24h)** | How many hours **ahead** the model was trying to see. Longer horizons are usually harder. |
| **Coverage %** | Share of times the **real value fell between** the model’s lower and upper bound. If the model was built for ~90% confidence, you want coverage **near 90%**. Much lower = bands too tight; much higher = bands too wide. |
| **Model name** (e.g. `ed_arrivals_forecast`) | One model per **department** (ED, ICU, or ALL) and **target** (arrivals, discharges, or total arrivals for combined). |

**How to read the charts**  
- **Summary table** — Averages over time for each model, department, metric, and horizon.  
- **MAE bars** — Compare models; color shows the horizon.  
- **Coverage bars** — Whether uncertainty bands behaved as intended (~90% target line).  
- **Trends over time** — See if accuracy is stable, improving, or drifting.  
- **Model comparison scatter** — Each bubble is one bucket (model + department + metric + horizon). **Left** on the chart = lower MAE; **down** = lower MAPE; **bigger bubble** = more scored predictions. Ideal is **toward the lower-left** (if both errors matter equally for you).

**Where the numbers come from**  
Aggregated in Delta as **`gold_forecast_accuracy`**, then copied to Lakebase for this app. Empty sections usually mean **`actual_value`** is not filled yet (inference backfill + DLT + lakebase load).
        """
    )

# ---- Summary Metrics ----
st.header("Model Accuracy Summary")
st.caption(
    "MAE / MAPE / coverage are computed only for predictions that already have a real **actual** from census."
)

try:
    summary = run_query(queries.ACCURACY_SUMMARY)

    if summary.empty:
        st.info(
            "**Why this is empty:** accuracy comes from **`gold_forecast_accuracy`** in Lakebase, which is built "
            "only from predictions that already have **`actual_value`** filled (compare forecast to real census).\n\n"
            "1. **`hl7_model_inference`** backfills `actual_value` when **`target_hour`** exists in **`gold_*_hourly_census`** "
            "(re-run inference periodically after wall-clock time passes the predicted hours).\n"
            "2. **DLT** must refresh so **`gold_forecast_accuracy`** recomputes from updated predictions.\n"
            "3. **`hl7_lakebase_load`** copies that table into Postgres for this app."
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
        st.caption(
            "MAE is in **count units** (e.g. patients per hour for arrivals). Smaller bars = smaller typical error."
        )

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
        st.caption(
            "Shows how often **actual** fell inside the predicted lower–upper band. The dashed line is a **~90%** reference if models were trained with that confidence goal."
        )

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
st.caption(
    "Each line is a **forecast horizon**. Use this to spot drift (accuracy getting worse over calendar time)."
)

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
        st.info("No rows in **`gold_forecast_accuracy`** yet — same prerequisites as the summary above (backfill → DLT → lakebase load).")

except Exception as e:
    st.error(f"Failed to load accuracy trends: {e}")

st.markdown("---")

# ---- Model Comparison ----
st.header("Model Comparison")
st.caption(
    "One bubble per row in the summary table: trade-off between average MAE (horizontal) and average MAPE (vertical)."
)

try:
    comparison = run_query(queries.ACCURACY_SUMMARY)

    if not comparison.empty:
        # Lakebase can return aggregates as object/Decimal; Plotly requires numeric `size`.
        for col in ("avg_mae", "avg_mape_pct", "avg_coverage_pct", "total_predictions"):
            if col in comparison.columns:
                comparison[col] = pd.to_numeric(comparison[col], errors="coerce")
        comparison = comparison.assign(
            _bubble=comparison["total_predictions"].fillna(1).clip(lower=1).astype(float)
        )
        fig = px.scatter(
            comparison,
            x="avg_mae",
            y="avg_mape_pct",
            size="_bubble",
            color="model_name",
            symbol="department",
            hover_data=["forecast_horizon_hours", "avg_coverage_pct", "total_predictions"],
            title="Model Comparison: MAE vs MAPE (bubble size = total predictions)",
        )
        fig.update_layout(
            height=450,
            margin=dict(t=40),
            xaxis_title="Average MAE",
            yaxis_title="Average MAPE (%)",
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No comparison data until **`gold_forecast_accuracy`** has rows (see summary section).")

except Exception as e:
    st.error(f"Failed to build model comparison: {e}")

st.caption(
    "Accuracy = predictions with `actual_value` set vs census, rolled up in DLT to `gold_forecast_accuracy`, then loaded to Lakebase."
)
