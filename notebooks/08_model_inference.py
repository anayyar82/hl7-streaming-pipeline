# Databricks notebook source
# MAGIC %md
# MAGIC # HL7 Forecasting - Model Inference & Predictions
# MAGIC
# MAGIC Loads champion models from Unity Catalog Model Registry, runs inference
# MAGIC against the latest feature data, and writes predictions to the
# MAGIC `gold_forecast_predictions` table.
# MAGIC
# MAGIC Schedule this notebook as a Databricks Job (e.g., hourly) to produce
# MAGIC rolling forecasts for ED and ICU arrivals/discharges.
# MAGIC
# MAGIC ## Forecast Horizons
# MAGIC | Horizon | Description |
# MAGIC |---------|-------------|
# MAGIC | 1h | Next-hour prediction |
# MAGIC | 4h | 4-hour lookahead |
# MAGIC | 8h | Shift-level forecast |
# MAGIC | 24h | Next-day forecast |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "users", "Catalog")
dbutils.widgets.text("schema", "ankur_nayyar", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

MODEL_REGISTRY_PREFIX = f"{catalog}.{schema}"
PREDICTIONS_TABLE = f"{catalog}.{schema}.gold_forecast_predictions"

FORECAST_HORIZONS = [1, 4, 8, 24]

MODEL_CONFIGS = [
    {"name": "ed_arrivals_forecast",       "department": "ED",  "target_metric": "arrivals",       "feature_table": "gold_ed_forecast_features"},
    {"name": "ed_discharges_forecast",     "department": "ED",  "target_metric": "discharges",     "feature_table": "gold_ed_forecast_features"},
    {"name": "icu_arrivals_forecast",      "department": "ICU", "target_metric": "arrivals",       "feature_table": "gold_icu_forecast_features"},
    {"name": "icu_discharges_forecast",    "department": "ICU", "target_metric": "discharges",     "feature_table": "gold_icu_forecast_features"},
    {"name": "combined_arrivals_forecast", "department": "ALL", "target_metric": "total_arrivals", "feature_table": "gold_combined_forecast_features"},
]

print(f"Model registry: {MODEL_REGISTRY_PREFIX}")
print(f"Predictions table: {PREDICTIONS_TABLE}")
print(f"Forecast horizons: {FORECAST_HORIZONS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import mlflow
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
from datetime import datetime, timedelta
import uuid
import pandas as pd
import numpy as np

mlflow.set_registry_uri("databricks-uc")
print(f"MLflow version: {mlflow.__version__}")
# DBR 17.3 ML ships MLflow 3 with the image; avoid pip-upgrading mlflow on the cluster.
# Use plain pyfunc.predict (no start_span around predict) for stable batch inference.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Models from Registry

# COMMAND ----------

loaded_models = {}
client = mlflow.tracking.MlflowClient()

for config in MODEL_CONFIGS:
    full_name = f"{MODEL_REGISTRY_PREFIX}.{config['name']}"
    try:
        model_version = client.get_model_version_by_alias(full_name, "champion")
        model_uri = f"models:/{full_name}@champion"
        model = mlflow.pyfunc.load_model(model_uri)
        loaded_models[config["name"]] = {
            "model": model,
            "version": model_version.version,
            "config": config,
        }
        print(f"Loaded: {full_name} v{model_version.version}")
    except Exception as e:
        print(f"Skip {full_name}: {e}")

print(f"\nLoaded {len(loaded_models)}/{len(MODEL_CONFIGS)} models")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Preparation

# COMMAND ----------

EXCLUDE_COLS = ["event_hour", "location_facility", "department"]

def get_latest_features(feature_table, n_rows=48):
    """Get the most recent feature rows for inference."""
    df = spark.table(f"{catalog}.{schema}.{feature_table}")
    return (
        df.orderBy(F.col("event_hour").desc())
        .limit(n_rows)
        .orderBy("event_hour")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Inference

# COMMAND ----------

all_predictions = []
inference_ts = datetime.utcnow()

for model_name, model_info in loaded_models.items():
    config = model_info["config"]
    model = model_info["model"]
    version = model_info["version"]

    print(f"\nRunning inference: {config['name']}")
    print(f"  Department: {config['department']}, Target: {config['target_metric']}")

    features_df = get_latest_features(config["feature_table"])
    latest_row = features_df.orderBy(F.col("event_hour").desc()).first()

    if latest_row is None:
        print(f"  No feature data available - skipping")
        continue

    latest_hour = latest_row["event_hour"]
    location = latest_row["location_facility"] if "location_facility" in features_df.columns else "ALL"

    feature_cols = [c for c in features_df.columns if c not in EXCLUDE_COLS]
    pdf = features_df.select(feature_cols).toPandas().dropna(subset=[config["target_metric"]])

    if pdf.empty:
        print(f"  Empty features - skipping")
        continue

    try:
        predictions = model.predict(pdf)

        pred_std = float(np.std(predictions)) if len(predictions) > 1 else 0.0
        latest_pred = float(predictions[-1]) if len(predictions) > 0 else 0.0

        for horizon in FORECAST_HORIZONS:
            target_hour = latest_hour + timedelta(hours=horizon)

            confidence = max(0.5, 1.0 - (horizon * 0.02))
            margin = float(pred_std * (1 + horizon * 0.1) * 1.96)

            all_predictions.append({
                "prediction_id": str(uuid.uuid4()),
                "model_name": config["name"],
                "model_version": str(version),
                "department": config["department"],
                "location_facility": str(location),
                "forecast_generated_at": inference_ts,
                "target_metric": config["target_metric"],
                "forecast_horizon_hours": horizon,
                "target_hour": target_hour,
                "predicted_value": float(round(max(0, latest_pred), 2)),
                "prediction_lower_bound": float(round(max(0, latest_pred - margin), 2)),
                "prediction_upper_bound": float(round(latest_pred + margin, 2)),
                "confidence_level": float(round(confidence, 3)),
                "feature_snapshot_hour": latest_hour,
                "actual_value": None,
                "absolute_error": None,
                "created_at": inference_ts,
            })

        print(f"  Generated {len(FORECAST_HORIZONS)} predictions (horizons: {FORECAST_HORIZONS})")
        print(f"  Latest prediction: {latest_pred:.2f} {config['target_metric']}")

    except Exception as e:
        print(f"  Inference failed: {e}")

print(f"\nTotal predictions: {len(all_predictions)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Predictions

# COMMAND ----------

if all_predictions:
    pred_schema = StructType([
        StructField("prediction_id", StringType()),
        StructField("model_name", StringType()),
        StructField("model_version", StringType()),
        StructField("department", StringType()),
        StructField("location_facility", StringType()),
        StructField("forecast_generated_at", TimestampType()),
        StructField("target_metric", StringType()),
        StructField("forecast_horizon_hours", IntegerType()),
        StructField("target_hour", TimestampType()),
        StructField("predicted_value", DoubleType()),
        StructField("prediction_lower_bound", DoubleType()),
        StructField("prediction_upper_bound", DoubleType()),
        StructField("confidence_level", DoubleType()),
        StructField("feature_snapshot_hour", TimestampType()),
        StructField("actual_value", DoubleType()),
        StructField("absolute_error", DoubleType()),
        StructField("created_at", TimestampType()),
    ])

    pred_df = spark.createDataFrame(all_predictions, schema=pred_schema)

    try:
        if spark.catalog.tableExists(PREDICTIONS_TABLE):
            pred_df.write.format("delta").mode("append").saveAsTable(PREDICTIONS_TABLE)
        else:
            pred_df.write.format("delta").mode("overwrite").saveAsTable(PREDICTIONS_TABLE)
    except AnalysisException as e:
        err = str(e)
        if "DELTA_NOT_A_DATABRICKS_DELTA_TABLE" in err or "NOT_A_DATABRICKS_DELTA_TABLE" in err:
            print(
                f"WARN: {PREDICTIONS_TABLE} is not a managed Delta table (e.g. Lakebase foreign stub). "
                "Dropping the UC entry and recreating as managed Delta."
            )
            spark.sql(f"DROP TABLE IF EXISTS {PREDICTIONS_TABLE}")
            pred_df.write.format("delta").mode("overwrite").saveAsTable(PREDICTIONS_TABLE)
        elif "does not support append" in err or "UNSUPPORTED_FEATURE.TABLE_OPERATION" in err:
            # Leftover DLT-managed table from older pipeline definitions; replace with normal Delta.
            print(f"WARN: {PREDICTIONS_TABLE} does not allow batch append (e.g. DLT-owned). Recreating as managed Delta.")
            spark.sql(f"DROP TABLE IF EXISTS {PREDICTIONS_TABLE}")
            pred_df.write.format("delta").mode("overwrite").saveAsTable(PREDICTIONS_TABLE)
        else:
            raise

    print(f"Wrote {len(all_predictions)} predictions to {PREDICTIONS_TABLE}")

    display(pred_df.orderBy("model_name", "forecast_horizon_hours"))
else:
    print("No predictions generated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Backfill Actuals (for predictions whose horizon has passed)
# MAGIC Joins past predictions with actual observed values to compute error metrics.

# COMMAND ----------

try:
    predictions = spark.table(PREDICTIONS_TABLE).filter(F.col("actual_value").isNull())

    if predictions.count() > 0:
        ed_census = spark.table(f"{catalog}.{schema}.gold_ed_hourly_census")
        icu_census = spark.table(f"{catalog}.{schema}.gold_icu_hourly_census")

        ed_actuals = (
            ed_census.groupBy("event_hour")
            .agg(
                F.sum("arrivals").alias("actual_arrivals"),
                F.sum("discharges").alias("actual_discharges"),
            )
            .withColumn("department", F.lit("ED"))
        )

        icu_actuals = (
            icu_census.groupBy("event_hour")
            .agg(
                F.sum("arrivals").alias("actual_arrivals"),
                F.sum("discharges").alias("actual_discharges"),
            )
            .withColumn("department", F.lit("ICU"))
        )

        # Combined model uses department ALL + total_arrivals; join must use summed ED+ICU hourly totals.
        ed_h = ed_actuals.select(
            "event_hour",
            F.col("actual_arrivals").alias("_ed_arr"),
            F.col("actual_discharges").alias("_ed_dis"),
        )
        icu_h = icu_actuals.select(
            "event_hour",
            F.col("actual_arrivals").alias("_icu_arr"),
            F.col("actual_discharges").alias("_icu_dis"),
        )
        combined_actuals = (
            ed_h.join(icu_h, "event_hour", "outer")
            .withColumn(
                "actual_arrivals",
                F.coalesce(F.col("_ed_arr"), F.lit(0)) + F.coalesce(F.col("_icu_arr"), F.lit(0)),
            )
            .withColumn(
                "actual_discharges",
                F.coalesce(F.col("_ed_dis"), F.lit(0)) + F.coalesce(F.col("_icu_dis"), F.lit(0)),
            )
            .select("event_hour", "actual_arrivals", "actual_discharges")
            .withColumn("department", F.lit("ALL"))
        )

        all_actuals = ed_actuals.unionByName(icu_actuals).unionByName(combined_actuals)

        updated = (
            predictions.alias("p")
            .join(
                all_actuals.alias("a"),
                (
                    F.date_trunc("hour", F.col("p.target_hour"))
                    == F.date_trunc("hour", F.col("a.event_hour"))
                )
                & (F.col("p.department") == F.col("a.department")),
                "left",
            )
            .withColumn(
                "new_actual",
                F.when(
                    F.col("p.target_metric") == "arrivals",
                    F.col("a.actual_arrivals"),
                )
                .when(
                    F.col("p.target_metric") == "discharges",
                    F.col("a.actual_discharges"),
                )
                .when(
                    F.col("p.target_metric") == "total_arrivals",
                    F.col("a.actual_arrivals"),
                ),
            )
            .filter(F.col("new_actual").isNotNull())
        )

        if updated.count() > 0:
            backfilled = (
                updated.select(
                    F.col("p.prediction_id").alias("prediction_id"),
                    F.col("new_actual").alias("actual_value"),
                    F.round(F.abs(F.col("p.predicted_value") - F.col("new_actual")), 2).alias("absolute_error"),
                )
            )

            backfilled.createOrReplaceTempView("backfill_updates")

            spark.sql(f"""
                MERGE INTO {PREDICTIONS_TABLE} t
                USING backfill_updates s
                ON t.prediction_id = s.prediction_id
                WHEN MATCHED THEN UPDATE SET
                    t.actual_value = s.actual_value,
                    t.absolute_error = s.absolute_error
            """)

            print(f"Backfilled {backfilled.count()} predictions with actual values")
        else:
            c_max = ed_census.agg(F.max("event_hour").alias("m")).collect()[0]["m"]
            icu_max = icu_census.agg(F.max("event_hour").alias("m")).collect()[0]["m"]
            _hi = [x for x in (c_max, icu_max) if x is not None]
            census_hi = max(_hi) if _hi else None
            p_hi = predictions.agg(F.max("target_hour").alias("m")).collect()[0]["m"]
            print(
                "No predictions matched census for backfill (check target_hour vs hourly census). "
                f"max(target_hour)={p_hi}, max census event_hour≈{census_hi}. "
                "Regenerate sample data if the window ends before your forecast targets."
            )
    else:
        print("All predictions already have actual values")

except Exception as e:
    print(f"Backfill note: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prediction Summary

# COMMAND ----------

try:
    summary = spark.sql(f"""
        SELECT
            model_name,
            department,
            target_metric,
            forecast_horizon_hours,
            COUNT(*) as prediction_count,
            ROUND(AVG(predicted_value), 2) as avg_predicted,
            ROUND(AVG(actual_value), 2) as avg_actual,
            ROUND(AVG(absolute_error), 2) as avg_error,
            ROUND(AVG(CASE WHEN actual_value > 0
                THEN ABS(predicted_value - actual_value) / actual_value * 100
                ELSE NULL END), 1) as mape_pct
        FROM {PREDICTIONS_TABLE}
        GROUP BY model_name, department, target_metric, forecast_horizon_hours
        ORDER BY model_name, forecast_horizon_hours
    """)
    display(summary)
except Exception as e:
    print(f"Summary: {e}")
