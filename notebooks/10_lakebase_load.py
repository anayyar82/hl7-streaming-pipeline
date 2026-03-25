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

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
api_token = ctx.apiToken().get()
pg_user = ctx.tags().get("user").get()
workspace_url = "https://e2-demo-field-eng.cloud.databricks.com"

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
                arrivals                INTEGER,
                discharges              INTEGER,
                net_change              INTEGER,
                hour_of_day             INTEGER,
                day_of_week             INTEGER,
                is_weekend              INTEGER,
                department              TEXT,
                month                   INTEGER,
                day_of_month            INTEGER,
                week_of_year            INTEGER,
                is_night_shift          INTEGER,
                is_holiday_window       INTEGER,
                arrivals_lag_1h         INTEGER,
                arrivals_lag_2h         INTEGER,
                arrivals_lag_4h         INTEGER,
                arrivals_lag_6h         INTEGER,
                arrivals_lag_12h        INTEGER,
                arrivals_lag_24h        INTEGER,
                arrivals_lag_168h       INTEGER,
                discharges_lag_1h       INTEGER,
                discharges_lag_2h       INTEGER,
                discharges_lag_6h       INTEGER,
                discharges_lag_12h      INTEGER,
                discharges_lag_24h      INTEGER,
                discharges_lag_168h     INTEGER,
                net_change_lag_1h       INTEGER,
                net_change_lag_6h       INTEGER,
                net_change_lag_24h      INTEGER,
                arrivals_rolling_6h     BIGINT,
                arrivals_rolling_12h    BIGINT,
                arrivals_rolling_24h    BIGINT,
                arrivals_rolling_7d     BIGINT,
                arrivals_avg_6h         DOUBLE PRECISION,
                arrivals_avg_24h        DOUBLE PRECISION,
                arrivals_std_24h        DOUBLE PRECISION,
                discharges_rolling_6h   BIGINT,
                discharges_rolling_12h  BIGINT,
                discharges_rolling_24h  BIGINT,
                discharges_rolling_7d   BIGINT,
                discharges_avg_24h      DOUBLE PRECISION,
                cumulative_net_census   BIGINT,
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
                arrivals                INTEGER,
                discharges              INTEGER,
                net_change              INTEGER,
                hour_of_day             INTEGER,
                day_of_week             INTEGER,
                is_weekend              INTEGER,
                department              TEXT,
                month                   INTEGER,
                day_of_month            INTEGER,
                week_of_year            INTEGER,
                is_night_shift          INTEGER,
                is_holiday_window       INTEGER,
                arrivals_lag_1h         INTEGER,
                arrivals_lag_2h         INTEGER,
                arrivals_lag_4h         INTEGER,
                arrivals_lag_6h         INTEGER,
                arrivals_lag_12h        INTEGER,
                arrivals_lag_24h        INTEGER,
                arrivals_lag_168h       INTEGER,
                discharges_lag_1h       INTEGER,
                discharges_lag_2h       INTEGER,
                discharges_lag_6h       INTEGER,
                discharges_lag_12h      INTEGER,
                discharges_lag_24h      INTEGER,
                discharges_lag_168h     INTEGER,
                net_change_lag_1h       INTEGER,
                net_change_lag_6h       INTEGER,
                net_change_lag_24h      INTEGER,
                arrivals_rolling_6h     BIGINT,
                arrivals_rolling_12h    BIGINT,
                arrivals_rolling_24h    BIGINT,
                arrivals_rolling_7d     BIGINT,
                arrivals_avg_6h         DOUBLE PRECISION,
                arrivals_avg_24h        DOUBLE PRECISION,
                arrivals_std_24h        DOUBLE PRECISION,
                discharges_rolling_6h   BIGINT,
                discharges_rolling_12h  BIGINT,
                discharges_rolling_24h  BIGINT,
                discharges_rolling_7d   BIGINT,
                discharges_avg_24h      DOUBLE PRECISION,
                cumulative_net_census   BIGINT,
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
                ed_arrivals_rolling_24h      BIGINT,
                icu_arrivals_rolling_24h     BIGINT,
                ed_discharges_rolling_24h    BIGINT,
                icu_discharges_rolling_24h   BIGINT,
                ed_to_icu_ratio              DOUBLE PRECISION,
                net_system_pressure          BIGINT
            )
        """,
    },
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Each Gold Table

# COMMAND ----------

import psycopg2
import psycopg2.extras

BATCH_SIZE = 1000

def load_table(table_name: str, table_def: dict) -> dict:
    """Read a gold table via Spark, create PG table, truncate, and batch insert."""
    fqn = f"{catalog}.{schema}.{table_name}"
    result = {"table": table_name, "status": "SUCCESS", "rows": 0, "error": None}

    try:
        if not spark.catalog.tableExists(fqn):
            result["status"] = "SKIPPED_NOT_FOUND"
            print(f"  ⏭  {table_name}: source table not found in Unity Catalog – skipping")
            return result

        df = spark.sql(f"SELECT * FROM {fqn}")
        row_count = df.count()
        if row_count == 0:
            result["status"] = "SKIPPED_EMPTY"
            print(f"  ⏭  {table_name}: 0 rows – skipping")
            return result

        pdf = df.toPandas()
        columns = list(pdf.columns)

        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DBNAME,
            user=pg_user, password=pg_pass,
            sslmode="require",
        )
        conn.autocommit = False
        cur = conn.cursor()

        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{PG_SCHEMA}"')
        ddl = table_def["ddl"].format(schema=PG_SCHEMA)
        cur.execute(ddl)
        cur.execute(f'TRUNCATE TABLE "{PG_SCHEMA}"."{table_name}"')

        col_list = ", ".join(f'"{c}"' for c in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        insert_sql = f'INSERT INTO "{PG_SCHEMA}"."{table_name}" ({col_list}) VALUES ({placeholders})'

        rows_data = [tuple(row) for row in pdf.itertuples(index=False, name=None)]
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
