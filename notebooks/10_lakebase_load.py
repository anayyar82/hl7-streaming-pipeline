# Databricks notebook source
# MAGIC %md
# MAGIC # Load Gold Tables into Lakebase Postgres
# MAGIC
# MAGIC Reads each gold table from Unity Catalog via Spark and writes data
# MAGIC directly into the Lakebase Autoscaling project **ankurhlsproject** using
# MAGIC `psycopg2` batch inserts (TRUNCATE + INSERT snapshot pattern).
# MAGIC
# MAGIC ## Parameters
# MAGIC | Parameter   | Description                |
# MAGIC |-------------|----------------------------|
# MAGIC | catalog     | Unity Catalog name         |
# MAGIC | schema      | Schema containing gold tables |
# MAGIC | project_id  | Lakebase Autoscaling project name |

# COMMAND ----------

import os
import json
import traceback
import requests
from datetime import datetime

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
project_id = dbutils.widgets.get("project_id")

dbutils.widgets.text(
    "lakebase_connect_user",
    "",
    "Databricks login email for /api/2.0/postgres/credentials (required on serverless jobs; bundle passes this from databricks.yml)",
)

PG_HOST = "ep-wandering-meadow-d1tdkigo.database.us-west-2.cloud.databricks.com"
PG_PORT = 5432
PG_DBNAME = "databricks_postgres"
PG_SCHEMA = "ankur_nayyar"
PG_ENDPOINT = "projects/ankurhlsproject/branches/production/endpoints/primary"

print(f"Catalog:   {catalog}")
print(f"Schema:    {schema}")
print(f"Project:   {project_id}")
print(f"PG Host:   {PG_HOST}")
print(f"PG DB:     {PG_DBNAME}")
print(f"PG Schema: {PG_SCHEMA}")
print(f"Endpoint:  {PG_ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Lakebase Credentials via REST API

# COMMAND ----------


def _resolve_lakebase_pg_user() -> str:
    """Same resolution strategy as 11_lakebase_grants.py — ctx.tags() is not allowed on serverless."""
    w = (dbutils.widgets.get("lakebase_connect_user") or "").strip()
    if w:
        return w
    try:
        from pyspark.sql import SparkSession

        s = SparkSession.getActiveSession()
        if s is not None:
            for key in (
                "spark.databricks.userInfo.userName",
                "spark.databricks.userInfo.userId",
                "spark.databricks.clusterUsageTags.userEmail",
                "spark.databricks.clusterUsageTags.user",
            ):
                v = s.conf.get(key, None)
                if v:
                    return str(v).strip()
    except Exception:
        pass
    try:
        t = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .tags()
            .get("user")
        )
        if t is not None and hasattr(t, "get"):
            s = t.get()
        else:
            s = str(t) if t is not None else ""
        s = str(s).strip() if s else ""
        if s:
            return s
    except Exception:
        pass
    try:
        s = str(
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .userName()
            .get()
        ).strip()
        if s:
            return s
    except Exception:
        pass
    return ""


def _workspace_url() -> str:
    """Databricks may set host without scheme (e.g. DATABRICKS_HOST); requests needs https://."""

    def _ensure_scheme(url: str) -> str:
        u = url.strip().rstrip("/")
        if not u:
            return ""
        if u.startswith("http://") or u.startswith("https://"):
            return u
        return "https://" + u

    try:
        from pyspark.sql import SparkSession

        s = SparkSession.getActiveSession()
        if s is not None:
            u = s.conf.get("spark.databricks.workspaceUrl", None)
            if u:
                return _ensure_scheme(str(u))
    except Exception:
        pass
    h = os.environ.get("DATABRICKS_HOST", "").strip()
    if h:
        return _ensure_scheme(h)
    return "https://e2-demo-field-eng.cloud.databricks.com"


ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
api_token = ctx.apiToken().get()
pg_user = _resolve_lakebase_pg_user()
if not pg_user:
    raise ValueError(
        "Could not determine Lakebase Postgres user. On serverless jobs, set job parameter "
        "**lakebase_connect_user** (bundle: var.lakebase_connect_user in databricks.yml) to your "
        "Databricks login email — same as 11_lakebase_grants."
    )
workspace_url = _workspace_url()

cred_resp = requests.post(
    f"{workspace_url}/api/2.0/postgres/credentials",
    headers={"Authorization": f"Bearer {api_token}"},
    json={"endpoint": PG_ENDPOINT},
)
cred_resp.raise_for_status()
cred_data = cred_resp.json()
pg_pass = cred_data["token"]
print(f"Credential generated for user: {pg_user}")
print(f"Token expires: {cred_data.get('expire_time', 'unknown')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Gold Table Schemas (Postgres DDL)

# COMMAND ----------

GOLD_TABLES = {
    "gold_ed_hourly_census": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS "{schema}"."gold_ed_hourly_census" (
                event_hour       TIMESTAMP,
                location_facility TEXT,
                arrivals         INTEGER,
                discharges       INTEGER,
                net_change       INTEGER,
                hour_of_day      INTEGER,
                day_of_week      INTEGER,
                is_weekend       INTEGER,
                PRIMARY KEY (event_hour, location_facility)
            )
        """,
    },
    "gold_icu_hourly_census": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS "{schema}"."gold_icu_hourly_census" (
                event_hour       TIMESTAMP,
                location_facility TEXT,
                arrivals         INTEGER,
                discharges       INTEGER,
                net_change       INTEGER,
                hour_of_day      INTEGER,
                day_of_week      INTEGER,
                is_weekend       INTEGER,
                PRIMARY KEY (event_hour, location_facility)
            )
        """,
    },
    "gold_ed_daily_summary": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS "{schema}"."gold_ed_daily_summary" (
                activity_date      DATE,
                location_facility  TEXT,
                total_encounters   BIGINT,
                total_arrivals     BIGINT,
                total_discharges   BIGINT,
                unique_patients    BIGINT,
                avg_los_minutes    DOUBLE PRECISION,
                median_los_minutes DOUBLE PRECISION,
                max_los_minutes    DOUBLE PRECISION,
                peak_arrival_hour  INTEGER,
                unique_providers   BIGINT,
                PRIMARY KEY (activity_date, location_facility)
            )
        """,
    },
    "gold_icu_daily_summary": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS "{schema}"."gold_icu_daily_summary" (
                activity_date      DATE,
                location_facility  TEXT,
                total_encounters   BIGINT,
                total_arrivals     BIGINT,
                total_discharges   BIGINT,
                unique_patients    BIGINT,
                avg_los_hours      DOUBLE PRECISION,
                median_los_hours   DOUBLE PRECISION,
                max_los_hours      DOUBLE PRECISION,
                peak_arrival_hour  INTEGER,
                unique_providers   BIGINT,
                beds_used          BIGINT,
                PRIMARY KEY (activity_date, location_facility)
            )
        """,
    },
    "gold_department_census_current": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS "{schema}"."gold_department_census_current" (
                location_facility TEXT,
                department        TEXT,
                total_arrivals    BIGINT,
                total_discharges  BIGINT,
                estimated_census  BIGINT,
                snapshot_at       TIMESTAMP,
                PRIMARY KEY (location_facility, department)
            )
        """,
    },
    "gold_forecast_predictions": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS "{schema}"."gold_forecast_predictions" (
                prediction_id          TEXT PRIMARY KEY,
                model_name             TEXT,
                model_version          TEXT,
                department             TEXT,
                location_facility      TEXT,
                forecast_generated_at  TIMESTAMP,
                target_metric          TEXT,
                forecast_horizon_hours INTEGER,
                target_hour            TIMESTAMP,
                predicted_value        DOUBLE PRECISION,
                prediction_lower_bound DOUBLE PRECISION,
                prediction_upper_bound DOUBLE PRECISION,
                confidence_level       DOUBLE PRECISION,
                feature_snapshot_hour  TIMESTAMP,
                actual_value           DOUBLE PRECISION,
                absolute_error         DOUBLE PRECISION,
                created_at             TIMESTAMP
            )
        """,
    },
    "gold_forecast_accuracy": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS "{schema}"."gold_forecast_accuracy" (
                prediction_date        DATE,
                model_name             TEXT,
                model_version          TEXT,
                department             TEXT,
                target_metric          TEXT,
                forecast_horizon_hours INTEGER,
                prediction_count       BIGINT,
                mae                    DOUBLE PRECISION,
                mse                    DOUBLE PRECISION,
                mape                   DOUBLE PRECISION,
                coverage_pct           DOUBLE PRECISION,
                PRIMARY KEY (prediction_date, model_name, department, target_metric, forecast_horizon_hours)
            )
        """,
    },
    "gold_encounter_fact": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS "{schema}"."gold_encounter_fact" (
                encounter_key        TEXT PRIMARY KEY,
                patient_key          TEXT,
                patient_id           TEXT,
                visit_number         TEXT,
                patient_class        TEXT,
                patient_class_desc   TEXT,
                admission_type       TEXT,
                admit_source         TEXT,
                location_facility    TEXT,
                location_point_of_care TEXT,
                location_room        TEXT,
                location_bed         TEXT,
                hospital_service     TEXT,
                attending_doctor_id  TEXT,
                attending_doctor_name TEXT,
                admit_datetime       TEXT,
                discharge_datetime   TEXT,
                message_type         TEXT,
                trigger_event        TEXT,
                message_id           TEXT,
                source_system        TEXT,
                created_at           TIMESTAMP
            )
        """,
    },
    "gold_patient_dim": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS "{schema}"."gold_patient_dim" (
                patient_key          TEXT PRIMARY KEY,
                patient_id           TEXT,
                patient_id_authority TEXT,
                patient_name_full    TEXT,
                patient_name_family  TEXT,
                patient_name_given   TEXT,
                date_of_birth        TEXT,
                sex                  TEXT,
                race                 TEXT,
                address_full         TEXT,
                address_city         TEXT,
                address_state        TEXT,
                address_zip          TEXT,
                phone_primary        TEXT,
                marital_status       TEXT,
                first_seen_at        TIMESTAMP,
                last_updated_at      TIMESTAMP,
                source_system        TEXT
            )
        """,
    },
    "gold_observation_fact": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS "{schema}"."gold_observation_fact" (
                observation_key            TEXT PRIMARY KEY,
                patient_key                TEXT,
                patient_id                 TEXT,
                observation_id             TEXT,
                observation_text           TEXT,
                value_type                 TEXT,
                observation_value_text     TEXT,
                observation_value_numeric  DOUBLE PRECISION,
                units                      TEXT,
                reference_range            TEXT,
                abnormal_flags             TEXT,
                is_abnormal                BOOLEAN,
                observation_result_status  TEXT,
                observation_datetime       TEXT,
                message_id                 TEXT,
                source_system              TEXT,
                created_at                 TIMESTAMP
            )
        """,
    },
    "gold_diagnosis_fact": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS "{schema}"."gold_diagnosis_fact" (
                diagnosis_key            TEXT PRIMARY KEY,
                patient_key              TEXT,
                patient_id               TEXT,
                diagnosis_code           TEXT,
                diagnosis_description    TEXT,
                diagnosis_coding_system  TEXT,
                diagnosis_datetime       TEXT,
                diagnosis_type           TEXT,
                diagnosis_priority       TEXT,
                message_id               TEXT,
                source_system            TEXT,
                created_at               TIMESTAMP
            )
        """,
    },
    "gold_allergy_fact": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS "{schema}"."gold_allergy_fact" (
                allergy_key          TEXT PRIMARY KEY,
                patient_key          TEXT,
                patient_id           TEXT,
                allergen_type_code   TEXT,
                allergen_type_text   TEXT,
                allergen_code        TEXT,
                allergen_description TEXT,
                severity_code        TEXT,
                severity_text        TEXT,
                is_severe            BOOLEAN,
                reaction             TEXT,
                identification_date  TEXT,
                message_id           TEXT,
                source_system        TEXT,
                created_at           TIMESTAMP
            )
        """,
    },
    "gold_order_fact": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS "{schema}"."gold_order_fact" (
                order_key                       TEXT PRIMARY KEY,
                patient_key                     TEXT,
                patient_id                      TEXT,
                placer_order_number             TEXT,
                filler_order_number             TEXT,
                universal_service_id            TEXT,
                universal_service_text          TEXT,
                universal_service_coding_system TEXT,
                priority                        TEXT,
                requested_datetime              TEXT,
                observation_datetime            TEXT,
                observation_end_datetime        TEXT,
                ordering_provider_id            TEXT,
                ordering_provider_name          TEXT,
                result_status                   TEXT,
                message_id                      TEXT,
                source_system                   TEXT,
                created_at                      TIMESTAMP
            )
        """,
    },
    "gold_message_metrics": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS "{schema}"."gold_message_metrics" (
                processing_hour    TIMESTAMP,
                message_type       TEXT,
                sending_facility   TEXT,
                message_count      BIGINT,
                unique_messages    BIGINT,
                first_message_at   TIMESTAMP,
                last_message_at    TIMESTAMP,
                PRIMARY KEY (processing_hour, message_type, sending_facility)
            )
        """,
    },
    "gold_patient_activity": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS "{schema}"."gold_patient_activity" (
                activity_date      DATE,
                location_facility  TEXT,
                patient_class_desc TEXT,
                unique_patients    BIGINT,
                encounter_count    BIGINT,
                unique_providers   BIGINT,
                PRIMARY KEY (activity_date, location_facility, patient_class_desc)
            )
        """,
    },
    "gold_ed_forecast_features": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS "{schema}"."gold_ed_forecast_features" (
                event_hour              TIMESTAMP,
                location_facility       TEXT,
                arrivals                BIGINT,
                discharges              BIGINT,
                net_change              BIGINT,
                hour_of_day             INTEGER,
                day_of_week             INTEGER,
                is_weekend              INTEGER,
                department              TEXT,
                month                   INTEGER,
                day_of_month            INTEGER,
                week_of_year            INTEGER,
                is_night_shift          INTEGER,
                is_holiday_window       INTEGER,
                arrivals_lag_1h         BIGINT,
                arrivals_lag_2h         BIGINT,
                arrivals_lag_4h         BIGINT,
                arrivals_lag_6h         BIGINT,
                arrivals_lag_12h        BIGINT,
                arrivals_lag_24h        BIGINT,
                arrivals_lag_168h       BIGINT,
                discharges_lag_1h       BIGINT,
                discharges_lag_2h       BIGINT,
                discharges_lag_6h       BIGINT,
                discharges_lag_12h      BIGINT,
                discharges_lag_24h      BIGINT,
                discharges_lag_168h     BIGINT,
                net_change_lag_1h       BIGINT,
                net_change_lag_6h       BIGINT,
                net_change_lag_24h      BIGINT,
                arrivals_rolling_6h     DOUBLE PRECISION,
                arrivals_rolling_12h    DOUBLE PRECISION,
                arrivals_rolling_24h    DOUBLE PRECISION,
                arrivals_rolling_7d     DOUBLE PRECISION,
                arrivals_avg_6h         DOUBLE PRECISION,
                arrivals_avg_24h        DOUBLE PRECISION,
                arrivals_std_24h        DOUBLE PRECISION,
                discharges_rolling_6h   DOUBLE PRECISION,
                discharges_rolling_12h  DOUBLE PRECISION,
                discharges_rolling_24h  DOUBLE PRECISION,
                discharges_rolling_7d   DOUBLE PRECISION,
                discharges_avg_24h      DOUBLE PRECISION,
                cumulative_net_census   DOUBLE PRECISION,
                arrivals_wow_ratio      DOUBLE PRECISION,
                PRIMARY KEY (event_hour, location_facility)
            )
        """,
    },
    "gold_icu_forecast_features": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS "{schema}"."gold_icu_forecast_features" (
                event_hour              TIMESTAMP,
                location_facility       TEXT,
                arrivals                BIGINT,
                discharges              BIGINT,
                net_change              BIGINT,
                hour_of_day             INTEGER,
                day_of_week             INTEGER,
                is_weekend              INTEGER,
                department              TEXT,
                month                   INTEGER,
                day_of_month            INTEGER,
                week_of_year            INTEGER,
                is_night_shift          INTEGER,
                is_holiday_window       INTEGER,
                arrivals_lag_1h         BIGINT,
                arrivals_lag_2h         BIGINT,
                arrivals_lag_4h         BIGINT,
                arrivals_lag_6h         BIGINT,
                arrivals_lag_12h        BIGINT,
                arrivals_lag_24h        BIGINT,
                arrivals_lag_168h       BIGINT,
                discharges_lag_1h       BIGINT,
                discharges_lag_2h       BIGINT,
                discharges_lag_6h       BIGINT,
                discharges_lag_12h      BIGINT,
                discharges_lag_24h      BIGINT,
                discharges_lag_168h     BIGINT,
                net_change_lag_1h       BIGINT,
                net_change_lag_6h       BIGINT,
                net_change_lag_24h      BIGINT,
                arrivals_rolling_6h     DOUBLE PRECISION,
                arrivals_rolling_12h    DOUBLE PRECISION,
                arrivals_rolling_24h    DOUBLE PRECISION,
                arrivals_rolling_7d     DOUBLE PRECISION,
                arrivals_avg_6h         DOUBLE PRECISION,
                arrivals_avg_24h        DOUBLE PRECISION,
                arrivals_std_24h        DOUBLE PRECISION,
                discharges_rolling_6h   DOUBLE PRECISION,
                discharges_rolling_12h  DOUBLE PRECISION,
                discharges_rolling_24h  DOUBLE PRECISION,
                discharges_rolling_7d   DOUBLE PRECISION,
                discharges_avg_24h      DOUBLE PRECISION,
                cumulative_net_census   DOUBLE PRECISION,
                arrivals_wow_ratio      DOUBLE PRECISION,
                PRIMARY KEY (event_hour, location_facility)
            )
        """,
    },
    "gold_combined_forecast_features": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS "{schema}"."gold_combined_forecast_features" (
                event_hour                   TIMESTAMP PRIMARY KEY,
                ed_arrivals                  BIGINT,
                ed_discharges                BIGINT,
                ed_net_change                BIGINT,
                icu_arrivals                 BIGINT,
                icu_discharges               BIGINT,
                icu_net_change               BIGINT,
                hour_of_day                  INTEGER,
                day_of_week                  INTEGER,
                is_weekend                   INTEGER,
                month                        INTEGER,
                total_arrivals               BIGINT,
                total_discharges             BIGINT,
                ed_arrivals_lag_1h           BIGINT,
                ed_arrivals_lag_24h          BIGINT,
                icu_arrivals_lag_1h          BIGINT,
                icu_arrivals_lag_24h         BIGINT,
                ed_arrivals_rolling_24h      DOUBLE PRECISION,
                icu_arrivals_rolling_24h     DOUBLE PRECISION,
                ed_discharges_rolling_24h    DOUBLE PRECISION,
                icu_discharges_rolling_24h   DOUBLE PRECISION,
                ed_to_icu_ratio              DOUBLE PRECISION,
                net_system_pressure          DOUBLE PRECISION
            )
        """,
    },
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Each Gold Table

# COMMAND ----------

import math
import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras

BATCH_SIZE = 1000

# Postgres BIGINT is signed 64-bit; Spark→pandas can yield NaN/Inf in numeric cols and break inserts.
_PG_BIGINT_MIN = -9223372036854775808
_PG_BIGINT_MAX = 9223372036854775807

# Repopulated from Delta each run; DROP ensures DDL changes (e.g. DOUBLE vs BIGINT) apply — IF NOT EXISTS alone keeps stale types.
_FORECAST_TABLES_RECREATE = frozenset({
    "gold_ed_forecast_features",
    "gold_icu_forecast_features",
    "gold_combined_forecast_features",
})


def _sanitize_sql_value(v):
    """None/NaN/Inf → None for safe Postgres binding."""
    if v is None:
        return None
    if isinstance(v, (float, np.floating)):
        if math.isnan(float(v)) or math.isinf(float(v)):
            return None
        return v
    if isinstance(v, np.integer):
        return int(v)
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    return v


def _sanitize_row_tuple(row: tuple) -> tuple:
    out = []
    for v in row:
        v = _sanitize_sql_value(v)
        if type(v) is bool:
            out.append(v)
            continue
        if isinstance(v, (int, np.integer)):
            iv = int(v)
            if iv < _PG_BIGINT_MIN or iv > _PG_BIGINT_MAX:
                out.append(None)
                continue
        out.append(v)
    return tuple(out)


def load_table(table_name: str, table_def: dict) -> dict:
    """Read a gold table via Spark, ensure PG table exists, truncate, and batch insert.

    Always runs CREATE TABLE IF NOT EXISTS in Postgres so downstream apps (e.g. Streamlit status)
    can query the table even when UC is missing or empty — otherwise relation does not exist
    and clients see SQL errors.
    """
    fqn = f"{catalog}.{schema}.{table_name}"
    result = {"table": table_name, "status": "SUCCESS", "rows": 0, "error": None}

    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DBNAME,
            user=pg_user, password=pg_pass,
            sslmode="require",
        )
        conn.autocommit = False
        cur = conn.cursor()

        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{PG_SCHEMA}"')
        ddl = table_def["ddl"].format(schema=PG_SCHEMA)
        if table_name in _FORECAST_TABLES_RECREATE:
            print(f"  ↻  {table_name}: DROP+CREATE (forecast DDL / types refreshed for Lakebase)")
            cur.execute(f'DROP TABLE IF EXISTS "{PG_SCHEMA}"."{table_name}" CASCADE')
        cur.execute(ddl)

        if not spark.catalog.tableExists(fqn):
            cur.execute(f'TRUNCATE TABLE "{PG_SCHEMA}"."{table_name}"')
            conn.commit()
            cur.close()
            conn.close()
            result["status"] = "SKIPPED_NOT_FOUND"
            print(f"  ⏭  {table_name}: source table not found in Unity Catalog – PG table ensured empty")
            return result

        df = spark.sql(f"SELECT * FROM {fqn}")
        row_count = df.count()
        if row_count == 0:
            cur.execute(f'TRUNCATE TABLE "{PG_SCHEMA}"."{table_name}"')
            conn.commit()
            cur.close()
            conn.close()
            result["status"] = "SKIPPED_EMPTY"
            print(f"  ⏭  {table_name}: 0 rows in UC – PG truncated")
            return result

        pdf = df.toPandas()
        columns = list(pdf.columns)

        cur.execute(f'TRUNCATE TABLE "{PG_SCHEMA}"."{table_name}"')

        col_list = ", ".join(f'"{c}"' for c in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        insert_sql = f'INSERT INTO "{PG_SCHEMA}"."{table_name}" ({col_list}) VALUES ({placeholders})'

        rows_data = [_sanitize_row_tuple(tuple(row)) for row in pdf.itertuples(index=False, name=None)]
        for i in range(0, len(rows_data), BATCH_SIZE):
            batch = rows_data[i : i + BATCH_SIZE]
            psycopg2.extras.execute_batch(cur, insert_sql, batch)

        conn.commit()
        cur.close()
        conn.close()

        result["rows"] = row_count
        print(f"  ✅ {table_name}: {row_count} rows loaded")

    except Exception as e:
        result["status"] = "FAILED"
        result["error"] = str(e)
        print(f"  ❌ {table_name}: {e}")
        traceback.print_exc()

    return result

# COMMAND ----------

print(f"Starting Lakebase load at {datetime.now().isoformat()}")
print(f"Source: {catalog}.{schema}  →  Postgres {PG_HOST}/{PG_DBNAME} schema={PG_SCHEMA}")
print("=" * 70)

results = []
for tbl_name, tbl_def in GOLD_TABLES.items():
    r = load_table(tbl_name, tbl_def)
    results.append(r)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

import pandas as pd

summary_df = pd.DataFrame(results)
print("\n" + "=" * 70)
print("LOAD SUMMARY")
print("=" * 70)

success = summary_df[summary_df["status"] == "SUCCESS"]
failed = summary_df[summary_df["status"] == "FAILED"]
skipped_empty = summary_df[summary_df["status"] == "SKIPPED_EMPTY"]
skipped_missing = summary_df[summary_df["status"] == "SKIPPED_NOT_FOUND"]

print(f"  Succeeded:     {len(success)}  ({success['rows'].sum()} total rows)")
print(f"  Skipped empty: {len(skipped_empty)}")
print(f"  Skipped n/a:   {len(skipped_missing)}")
print(f"  Failed:        {len(failed)}")

if len(failed) > 0:
    print("\nFailed tables:")
    for _, row in failed.iterrows():
        print(f"  - {row['table']}: {row['error']}")
    print(f"\nWARNING: {len(failed)} table(s) failed. Other tables loaded successfully.")
else:
    print("\nAll tables loaded successfully!")

display(summary_df)
