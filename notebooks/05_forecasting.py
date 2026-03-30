# Databricks notebook source
# MAGIC %md
# MAGIC # HL7 Streaming Pipeline - ED & ICU Forecasting
# MAGIC 
# MAGIC Feature engineering tables that power predictive models for forecasting
# MAGIC Emergency Department and ICU arrivals and discharges.
# MAGIC 
# MAGIC ## Approach
# MAGIC 1. **Feature Engineering** (this notebook) builds time-series features from
# MAGIC    the hourly census tables in `04_reports.py`.
# MAGIC 2. **Model Training** (external notebook / MLflow) trains Prophet, ARIMA,
# MAGIC    or gradient-boosted models against the feature tables.
# MAGIC 3. **Prediction Output** lands back into `gold_forecast_predictions`
# MAGIC    which dashboards consume.
# MAGIC 
# MAGIC ## Feature Categories
# MAGIC | Category | Examples |
# MAGIC |----------|---------|
# MAGIC | Calendar | hour_of_day, day_of_week, month, is_weekend, is_holiday_window |
# MAGIC | Lag | arrivals_lag_1h, arrivals_lag_24h, arrivals_lag_168h (1 week) |
# MAGIC | Rolling | arrivals_rolling_6h, arrivals_rolling_24h, arrivals_rolling_7d |
# MAGIC | Trend | arrivals_ewma_24h, discharges_ewma_24h |
# MAGIC | Cross-dept | ed_arrivals_vs_icu_arrivals, ed_net_change_lag_6h |
# MAGIC 
# MAGIC ## Tables
# MAGIC | Table | Description |
# MAGIC |-------|-------------|
# MAGIC | `gold_ed_forecast_features` | Hourly feature vector for ED forecasting |
# MAGIC | `gold_icu_forecast_features` | Hourly feature vector for ICU forecasting |
# MAGIC | `gold_combined_forecast_features` | Cross-department feature vector for multi-metric models |
# MAGIC | `gold_forecast_predictions` | Scored predictions (**UC Delta table** written by the inference job — not a DLT dataset, so batch append works) |
# MAGIC
# MAGIC > Count-like columns are materialized as **BIGINT** (Spark `long`). If **Lakebase/Postgres** still uses `INTEGER` for arrivals, lags, or `cumulative_net_census`, you will see **integer out of range** — alter those columns to `BIGINT` or recreate the tables (see `10_lakebase_load.py` DDL).

# COMMAND ----------

import dlt
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Window Definitions

# COMMAND ----------

def build_forecast_features(census_df, department_label):
    """
    Build time-series forecast features from an hourly census table.

    Input schema: event_hour, location_facility, arrivals, discharges,
                  net_change, hour_of_day, day_of_week, is_weekend

    Returns DataFrame with lag, rolling, and calendar features.
    """
    w_time = Window.partitionBy("location_facility").orderBy("event_hour")

    hours = lambda n: n * 3600

    df = census_df.withColumn("department", F.lit(department_label))

    # --- Calendar features ---
    df = (
        df
        .withColumn("month", F.month("event_hour"))
        .withColumn("day_of_month", F.dayofmonth("event_hour"))
        .withColumn("week_of_year", F.weekofyear("event_hour"))
        .withColumn("is_night_shift", F.when(F.col("hour_of_day").between(19, 23) | F.col("hour_of_day").between(0, 6), 1).otherwise(0))
        .withColumn("is_holiday_window",
            F.when(
                (F.col("month") == 12) & (F.col("day_of_month").between(24, 31)) |
                (F.col("month") == 1) & (F.col("day_of_month") == 1) |
                (F.col("month") == 7) & (F.col("day_of_month") == 4) |
                (F.col("month") == 11) & (F.col("day_of_month").between(22, 28) & (F.col("day_of_week") == 5)),
                1
            ).otherwise(0)
        )
    )

    # --- Lag features (arrivals) ---
    df = (
        df
        .withColumn("arrivals_lag_1h", F.lag("arrivals", 1).over(w_time))
        .withColumn("arrivals_lag_2h", F.lag("arrivals", 2).over(w_time))
        .withColumn("arrivals_lag_4h", F.lag("arrivals", 4).over(w_time))
        .withColumn("arrivals_lag_6h", F.lag("arrivals", 6).over(w_time))
        .withColumn("arrivals_lag_12h", F.lag("arrivals", 12).over(w_time))
        .withColumn("arrivals_lag_24h", F.lag("arrivals", 24).over(w_time))
        .withColumn("arrivals_lag_168h", F.lag("arrivals", 168).over(w_time))
    )

    # --- Lag features (discharges) ---
    df = (
        df
        .withColumn("discharges_lag_1h", F.lag("discharges", 1).over(w_time))
        .withColumn("discharges_lag_2h", F.lag("discharges", 2).over(w_time))
        .withColumn("discharges_lag_6h", F.lag("discharges", 6).over(w_time))
        .withColumn("discharges_lag_12h", F.lag("discharges", 12).over(w_time))
        .withColumn("discharges_lag_24h", F.lag("discharges", 24).over(w_time))
        .withColumn("discharges_lag_168h", F.lag("discharges", 168).over(w_time))
    )

    # --- Lag features (net census change) ---
    df = (
        df
        .withColumn("net_change_lag_1h", F.lag("net_change", 1).over(w_time))
        .withColumn("net_change_lag_6h", F.lag("net_change", 6).over(w_time))
        .withColumn("net_change_lag_24h", F.lag("net_change", 24).over(w_time))
    )

    # --- Rolling window features (arrivals) ---
    w_6h = Window.partitionBy("location_facility").orderBy(F.col("event_hour").cast("long")).rangeBetween(-hours(6), 0)
    w_12h = Window.partitionBy("location_facility").orderBy(F.col("event_hour").cast("long")).rangeBetween(-hours(12), 0)
    w_24h = Window.partitionBy("location_facility").orderBy(F.col("event_hour").cast("long")).rangeBetween(-hours(24), 0)
    w_7d = Window.partitionBy("location_facility").orderBy(F.col("event_hour").cast("long")).rangeBetween(-hours(168), 0)

    df = (
        df
        .withColumn("arrivals_rolling_6h", F.sum(F.col("arrivals").cast("long")).over(w_6h))
        .withColumn("arrivals_rolling_12h", F.sum(F.col("arrivals").cast("long")).over(w_12h))
        .withColumn("arrivals_rolling_24h", F.sum(F.col("arrivals").cast("long")).over(w_24h))
        .withColumn("arrivals_rolling_7d", F.sum(F.col("arrivals").cast("long")).over(w_7d))
        .withColumn("arrivals_avg_6h", F.round(F.avg("arrivals").over(w_6h), 2))
        .withColumn("arrivals_avg_24h", F.round(F.avg("arrivals").over(w_24h), 2))
        .withColumn("arrivals_std_24h", F.round(F.stddev("arrivals").over(w_24h), 2))
    )

    # --- Rolling window features (discharges) ---
    df = (
        df
        .withColumn("discharges_rolling_6h", F.sum(F.col("discharges").cast("long")).over(w_6h))
        .withColumn("discharges_rolling_12h", F.sum(F.col("discharges").cast("long")).over(w_12h))
        .withColumn("discharges_rolling_24h", F.sum(F.col("discharges").cast("long")).over(w_24h))
        .withColumn("discharges_rolling_7d", F.sum(F.col("discharges").cast("long")).over(w_7d))
        .withColumn("discharges_avg_24h", F.round(F.avg("discharges").over(w_24h), 2))
    )

    # --- Cumulative census estimate (bigint: unbounded sum can exceed INT32; Postgres/Lakebase INTEGER overflows) ---
    df = df.withColumn(
        "cumulative_net_census",
        F.sum(F.col("net_change").cast("long")).over(
            Window.partitionBy("location_facility")
            .orderBy("event_hour")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
    )

    # --- Same-hour-last-week ratio (seasonality signal) ---
    df = df.withColumn(
        "arrivals_wow_ratio",
        F.when(
            F.col("arrivals_lag_168h") > 0,
            F.round(F.col("arrivals") / F.col("arrivals_lag_168h"), 2)
        )
    )

    # Force BIGINT for any count / lag / rolling column so Delta and Lakebase never see INT32 overflow.
    _bigint_cols = [
        "arrivals",
        "discharges",
        "net_change",
        "arrivals_lag_1h",
        "arrivals_lag_2h",
        "arrivals_lag_4h",
        "arrivals_lag_6h",
        "arrivals_lag_12h",
        "arrivals_lag_24h",
        "arrivals_lag_168h",
        "discharges_lag_1h",
        "discharges_lag_2h",
        "discharges_lag_6h",
        "discharges_lag_12h",
        "discharges_lag_24h",
        "discharges_lag_168h",
        "net_change_lag_1h",
        "net_change_lag_6h",
        "net_change_lag_24h",
        "arrivals_rolling_6h",
        "arrivals_rolling_12h",
        "arrivals_rolling_24h",
        "arrivals_rolling_7d",
        "discharges_rolling_6h",
        "discharges_rolling_12h",
        "discharges_rolling_24h",
        "discharges_rolling_7d",
        "cumulative_net_census",
    ]
    for c in _bigint_cols:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("long"))

    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_ed_forecast_features
# MAGIC Hourly feature vector for ED arrival and discharge forecasting.

# COMMAND ----------

@dlt.table(
    name="gold_ed_forecast_features",
    comment="Hourly feature table for ED arrival/discharge forecasting models"
)
def gold_ed_forecast_features():
    ed_census = dlt.read("gold_ed_hourly_census")
    return build_forecast_features(ed_census, "ED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_icu_forecast_features
# MAGIC Hourly feature vector for ICU arrival and discharge forecasting.

# COMMAND ----------

@dlt.table(
    name="gold_icu_forecast_features",
    comment="Hourly feature table for ICU arrival/discharge forecasting models"
)
def gold_icu_forecast_features():
    icu_census = dlt.read("gold_icu_hourly_census")
    return build_forecast_features(icu_census, "ICU")

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_combined_forecast_features
# MAGIC Cross-department feature table that joins ED and ICU signals for
# MAGIC models that predict both departments simultaneously.
# MAGIC ED boarding in the ICU and ICU transfers from ED create cross-signals.

# COMMAND ----------

@dlt.table(
    name="gold_combined_forecast_features",
    comment="Combined ED + ICU feature table for multi-department forecasting"
)
def gold_combined_forecast_features():
    ed = dlt.read("gold_ed_hourly_census")
    icu = dlt.read("gold_icu_hourly_census")

    ed_agg = (
        ed.groupBy("event_hour")
        .agg(
            F.sum("arrivals").alias("ed_arrivals"),
            F.sum("discharges").alias("ed_discharges"),
            F.sum("net_change").alias("ed_net_change"),
        )
    )

    icu_agg = (
        icu.groupBy("event_hour")
        .agg(
            F.sum("arrivals").alias("icu_arrivals"),
            F.sum("discharges").alias("icu_discharges"),
            F.sum("net_change").alias("icu_net_change"),
        )
    )

    combined = (
        ed_agg
        .join(icu_agg, "event_hour", "full_outer")
        .na.fill(0)
        .withColumn("hour_of_day", F.hour("event_hour"))
        .withColumn("day_of_week", F.dayofweek("event_hour"))
        .withColumn("is_weekend", F.col("day_of_week").isin(1, 7).cast("int"))
        .withColumn("month", F.month("event_hour"))
        .withColumn("total_arrivals", F.col("ed_arrivals") + F.col("icu_arrivals"))
        .withColumn("total_discharges", F.col("ed_discharges") + F.col("icu_discharges"))
    )

    w_time = Window.orderBy("event_hour")
    w_24h = Window.orderBy(F.col("event_hour").cast("long")).rangeBetween(-24 * 3600, 0)

    combined = (
        combined
        .withColumn("ed_arrivals_lag_1h", F.lag("ed_arrivals", 1).over(w_time))
        .withColumn("ed_arrivals_lag_24h", F.lag("ed_arrivals", 24).over(w_time))
        .withColumn("icu_arrivals_lag_1h", F.lag("icu_arrivals", 1).over(w_time))
        .withColumn("icu_arrivals_lag_24h", F.lag("icu_arrivals", 24).over(w_time))
        .withColumn("ed_arrivals_rolling_24h", F.sum(F.col("ed_arrivals").cast("long")).over(w_24h))
        .withColumn("icu_arrivals_rolling_24h", F.sum(F.col("icu_arrivals").cast("long")).over(w_24h))
        .withColumn("ed_discharges_rolling_24h", F.sum(F.col("ed_discharges").cast("long")).over(w_24h))
        .withColumn("icu_discharges_rolling_24h", F.sum(F.col("icu_discharges").cast("long")).over(w_24h))
        .withColumn(
            "ed_to_icu_ratio",
            F.when(F.col("icu_arrivals") > 0,
                   F.round(F.col("ed_arrivals") / F.col("icu_arrivals"), 2))
        )
        .withColumn(
            "net_system_pressure",
            F.col("ed_net_change") + F.col("icu_net_change")
        )
    )

    _combined_bigint = [
        "ed_arrivals",
        "ed_discharges",
        "ed_net_change",
        "icu_arrivals",
        "icu_discharges",
        "icu_net_change",
        "total_arrivals",
        "total_discharges",
        "ed_arrivals_lag_1h",
        "ed_arrivals_lag_24h",
        "icu_arrivals_lag_1h",
        "icu_arrivals_lag_24h",
        "ed_arrivals_rolling_24h",
        "icu_arrivals_rolling_24h",
        "ed_discharges_rolling_24h",
        "icu_discharges_rolling_24h",
        "net_system_pressure",
    ]
    for c in _combined_bigint:
        if c in combined.columns:
            combined = combined.withColumn(c, F.col(c).cast("long"))

    return combined

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_forecast_predictions (external to DLT)
# MAGIC 
# MAGIC Inference job `08_model_inference.py` creates and appends to UC Delta table
# MAGIC `{catalog}.{schema}.gold_forecast_predictions`. It is **not** defined here as `@dlt.table`
# MAGIC because DLT-managed tables do not allow batch append from jobs (`UNSUPPORTED_FEATURE.TABLE_OPERATION`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_forecast_accuracy
# MAGIC Tracks model accuracy over time by comparing predictions to actuals.
# MAGIC Populated once actual values are known (after the forecast horizon passes).

# COMMAND ----------

@dlt.table(
    name="gold_forecast_accuracy",
    comment="Forecast accuracy tracking - actuals vs predictions"
)
def gold_forecast_accuracy():
    catalog = spark.conf.get("hl7.catalog", "users")
    schema = spark.conf.get("hl7.schema", "ankur_nayyar")
    predictions = spark.table(f"`{catalog}`.`{schema}`.`gold_forecast_predictions`").filter(
        F.col("actual_value").isNotNull()
    )

    return (
        predictions
        .withColumn("absolute_pct_error",
            F.when(F.col("actual_value") > 0,
                   F.round(F.abs(F.col("predicted_value") - F.col("actual_value")) / F.col("actual_value") * 100, 2))
        )
        .withColumn("prediction_date", F.to_date("target_hour"))
        .groupBy("prediction_date", "model_name", "model_version", "department", "target_metric", "forecast_horizon_hours")
        .agg(
            F.count("*").alias("prediction_count"),
            F.round(F.avg("absolute_error"), 2).alias("mae"),
            F.round(F.avg(F.pow("absolute_error", 2)), 2).alias("mse"),
            F.round(F.avg("absolute_pct_error"), 2).alias("mape"),
            F.round(F.avg(
                F.when(
                    (F.col("actual_value") >= F.col("prediction_lower_bound")) &
                    (F.col("actual_value") <= F.col("prediction_upper_bound")),
                    1
                ).otherwise(0)
            ) * 100, 1).alias("coverage_pct"),
        )
    )
