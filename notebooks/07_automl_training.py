# Databricks notebook source
# MAGIC %md
# MAGIC # HL7 Forecasting - AutoML Model Training with MLflow
# MAGIC
# MAGIC Trains forecasting models for ED and ICU metrics using Databricks AutoML.
# MAGIC Models are tracked in MLflow and registered in Unity Catalog for serving.
# MAGIC
# MAGIC ## Models Trained
# MAGIC | Model | Target | Description |
# MAGIC |-------|--------|-------------|
# MAGIC | `ed_arrivals_forecast` | `arrivals` | Hourly ED arrival volume |
# MAGIC | `ed_discharges_forecast` | `discharges` | Hourly ED discharge volume |
# MAGIC | `icu_arrivals_forecast` | `arrivals` | Hourly ICU arrival volume |
# MAGIC | `icu_discharges_forecast` | `discharges` | Hourly ICU discharge volume |
# MAGIC | `combined_census_forecast` | `total_arrivals` | System-wide arrival volume |
# MAGIC
# MAGIC ## Workflow
# MAGIC 1. Read feature tables from `gold_*_forecast_features`
# MAGIC 2. Run Databricks AutoML regression for each target
# MAGIC 3. Log best model + metrics to MLflow
# MAGIC 4. Register champion models in Unity Catalog Model Registry

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from datetime import datetime

dbutils.widgets.text("catalog", "users", "Catalog")
dbutils.widgets.text("schema", "ankur_nayyar", "Schema")
# AutoML places experiments under: /Users/<user>/databricks_automl/<this_prefix>/...
# Do NOT use leading / or "Shared/..." — that becomes .../databricks_automl/Shared/... (404).
dbutils.widgets.text("experiment_prefix", "hl7_forecasting", "Relative folder under databricks_automl")
dbutils.widgets.dropdown("timeout_minutes", "5", ["5", "10", "15", "30", "60"], "AutoML Timeout (min)")
# Reusing the same experiment_name on reruns causes RESOURCE_ALREADY_EXISTS ("Node named '...' already exists").
dbutils.widgets.dropdown(
    "experiment_naming",
    "unique_per_run",
    ["unique_per_run", "stable"],
    "Experiment path: unique_per_run = timestamp suffix (recommended); stable = fixed path (rerun may fail)",
)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
_raw_prefix = dbutils.widgets.get("experiment_prefix").strip().strip("/")
experiment_rel_path = _raw_prefix or "hl7_forecasting"
timeout_minutes = int(dbutils.widgets.get("timeout_minutes"))
experiment_naming = dbutils.widgets.get("experiment_naming")

# One timestamp per notebook/job execution so all five models group under the same run folder.
AUTOML_RUN_SESSION = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

MODEL_REGISTRY_PREFIX = f"{catalog}.{schema}"
print(f"Catalog: {catalog}, Schema: {schema}")
print(f"Model registry: {MODEL_REGISTRY_PREFIX}.*")
print(f"AutoML experiment path (relative): {experiment_rel_path}")
print(f"AutoML run session: {AUTOML_RUN_SESSION} (used when experiment_naming=unique_per_run)")
print(f"AutoML timeout: {timeout_minutes} min per model")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup MLflow

# COMMAND ----------

import mlflow
import databricks.automl as automl
import pyspark.sql.functions as F
from pyspark.sql.types import *
from databricks.sdk import WorkspaceClient

mlflow.set_registry_uri("databricks-uc")
print(f"MLflow version: {mlflow.__version__}")
# MLflow 3+: unified tracking + tracing (DBR 17 ML includes MLflow 3)
try:
    mlflow.tracing.enable()
    print("MLflow tracing enabled")
except Exception as _te:
    print(f"MLflow tracing not available: {_te}")

# AutoML expects parent: /Users/<user>/databricks_automl/<experiment_rel_path>
def _workspace_user_name():
    try:
        return WorkspaceClient().current_user.me().user_name
    except Exception:
        import json
        ctx = json.loads(
            dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
        )
        return (ctx.get("tags") or {}).get("user")


try:
    w = WorkspaceClient()
    user_name = _workspace_user_name()
    if not user_name:
        raise RuntimeError("Could not resolve workspace user (tags.user / current_user.me)")
    automl_parent = f"/Users/{user_name}/databricks_automl/{experiment_rel_path}"
    w.workspace.mkdirs(automl_parent)
    print(f"Ensured AutoML workspace parent exists: {automl_parent}")
except Exception as _e:
    print(f"Could not mkdir AutoML parent (training may 404): {_e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Feature Tables

# COMMAND ----------

ed_features = spark.table(f"{catalog}.{schema}.gold_ed_forecast_features").na.fill(0)
icu_features = spark.table(f"{catalog}.{schema}.gold_icu_forecast_features").na.fill(0)
combined_features = spark.table(f"{catalog}.{schema}.gold_combined_forecast_features").na.fill(0)

print(f"ED features:       {ed_features.count()} rows, {len(ed_features.columns)} cols")
print(f"ICU features:      {icu_features.count()} rows, {len(icu_features.columns)} cols")
print(f"Combined features: {combined_features.count()} rows, {len(combined_features.columns)} cols")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Selection
# MAGIC Drop non-feature columns before AutoML training.

# COMMAND ----------

EXCLUDE_COLS = ["event_hour", "location_facility", "department"]

def prepare_features(df, target_col):
    """Prepare a Pandas DataFrame for AutoML by dropping non-feature columns and NULLs."""
    feature_cols = [c for c in df.columns if c not in EXCLUDE_COLS]
    pdf = df.select(feature_cols).toPandas()
    pdf = pdf.dropna(subset=[target_col])
    return pdf

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Training Configuration

# COMMAND ----------

MODEL_CONFIGS = [
    {
        "name": "ed_arrivals_forecast",
        "display": "ED Arrivals Forecast",
        "df": ed_features,
        "target": "arrivals",
        "department": "ED",
    },
    {
        "name": "ed_discharges_forecast",
        "display": "ED Discharges Forecast",
        "df": ed_features,
        "target": "discharges",
        "department": "ED",
    },
    {
        "name": "icu_arrivals_forecast",
        "display": "ICU Arrivals Forecast",
        "df": icu_features,
        "target": "arrivals",
        "department": "ICU",
    },
    {
        "name": "icu_discharges_forecast",
        "display": "ICU Discharges Forecast",
        "df": icu_features,
        "target": "discharges",
        "department": "ICU",
    },
    {
        "name": "combined_arrivals_forecast",
        "display": "Combined System Arrivals Forecast",
        "df": combined_features,
        "target": "total_arrivals",
        "department": "ALL",
    },
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run AutoML Training Loop
# MAGIC Trains each model using Databricks AutoML regression, logs to MLflow,
# MAGIC and registers the best model in Unity Catalog.

# COMMAND ----------

training_results = []

for config in MODEL_CONFIGS:
    model_name = config["name"]
    target_col = config["target"]
    full_model_name = f"{MODEL_REGISTRY_PREFIX}.{model_name}"

    print(f"\n{'='*70}")
    print(f"Training: {config['display']}")
    print(f"  Target: {target_col}")
    print(f"  Model: {full_model_name}")
    print(f"{'='*70}")

    pdf = prepare_features(config["df"], target_col)
    print(f"  Training samples: {len(pdf)}")

    if len(pdf) < 10:
        print(f"  SKIPPED - insufficient data ({len(pdf)} rows)")
        training_results.append({
            "model": model_name,
            "status": "SKIPPED",
            "reason": f"Only {len(pdf)} rows",
        })
        continue

    if experiment_naming == "stable":
        experiment_name = f"{experiment_rel_path}/{model_name}"
        print(f"  AutoML experiment_name (stable): {experiment_name} — may fail on rerun if node exists")
    else:
        # Fresh MLflow/AutoML experiment path each job run avoids RESOURCE_ALREADY_EXISTS on graph nodes.
        experiment_name = f"{experiment_rel_path}/run_{AUTOML_RUN_SESSION}/{model_name}"
        print(f"  AutoML experiment_name: {experiment_name}")

    try:
        summary = automl.regress(
            dataset=pdf,
            target_col=target_col,
            primary_metric="rmse",
            timeout_minutes=timeout_minutes,
            max_trials=50,
            experiment_name=experiment_name,
        )

        best_run = summary.best_trial
        print(f"  Best trial: {best_run.model_description}")
        print(f"  RMSE: {best_run.metrics.get('test_rmse', 'N/A')}")
        print(f"  R2:   {best_run.metrics.get('test_r2_score', 'N/A')}")
        print(f"  MAE:  {best_run.metrics.get('test_mean_absolute_error', 'N/A')}")

        with mlflow.start_run(run_id=best_run.mlflow_run_id):
            mlflow.set_tags({
                "department": config["department"],
                "target_metric": target_col,
                "pipeline": "hl7_streaming",
                "training_type": "automl_regression",
                "training_samples": str(len(pdf)),
            })

        model_uri = f"runs:/{best_run.mlflow_run_id}/model"
        try:
            registered = mlflow.register_model(model_uri, full_model_name)
            version = registered.version
            print(f"  Registered: {full_model_name} v{version}")

            client = mlflow.tracking.MlflowClient()
            client.set_registered_model_alias(full_model_name, "champion", version)
            print(f"  Alias 'champion' set to v{version}")
        except Exception as reg_err:
            print(f"  Registration note: {reg_err}")
            version = "N/A"

        training_results.append({
            "model": model_name,
            "status": "SUCCESS",
            "best_trial": best_run.model_description,
            "rmse": best_run.metrics.get("test_rmse"),
            "r2": best_run.metrics.get("test_r2_score"),
            "mae": best_run.metrics.get("test_mean_absolute_error"),
            "run_id": best_run.mlflow_run_id,
            "version": version,
            "training_samples": len(pdf),
        })

    except Exception as e:
        print(f"  FAILED: {e}")
        training_results.append({
            "model": model_name,
            "status": "FAILED",
            "error": str(e),
        })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Training Summary

# COMMAND ----------

import pandas as pd

results_df = pd.DataFrame(training_results)
display(results_df)

successful = [r for r in training_results if r["status"] == "SUCCESS"]
print(f"\nTraining complete: {len(successful)}/{len(MODEL_CONFIGS)} models succeeded")


def _fmt_m(v):
    if v is None:
        return "N/A"
    try:
        return f"{float(v):.4f}"
    except (TypeError, ValueError):
        return str(v)


for r in successful:
    print(f"  {r['model']}: RMSE={_fmt_m(r.get('rmse'))}, R2={_fmt_m(r.get('r2'))}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Training Metadata to Gold Table
# MAGIC Persists training run metadata for dashboard consumption.

# COMMAND ----------

if successful:
    metadata_rows = []
    for r in successful:
        metadata_rows.append({
            "model_name": r["model"],
            "model_version": str(r.get("version", "N/A")),
            "training_timestamp": datetime.utcnow().isoformat(),
            "rmse": float(r.get("rmse") or 0),
            "r2_score": float(r.get("r2") or 0),
            "mae": float(r.get("mae") or 0),
            "best_trial_description": r.get("best_trial", ""),
            "mlflow_run_id": r.get("run_id", ""),
            "training_samples": int(r.get("training_samples") or 0),
        })

    metadata_df = spark.createDataFrame(metadata_rows)
    metadata_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_model_training_metadata")
    print(f"Saved training metadata to {catalog}.{schema}.gold_model_training_metadata")
