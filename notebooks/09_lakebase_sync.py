# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase Autoscaling Setup - Gold Tables for Sync
# MAGIC
# MAGIC Prepares gold tables for sync into the **Lakebase Autoscaling** project
# MAGIC `ankurhlsproject`.
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC 1. Enables CDF (`delta.enableChangeDataFeed`) on each source table
# MAGIC 2. Validates the Autoscaling project exists
# MAGIC 3. Generates instructions for creating synced tables via the UI
# MAGIC
# MAGIC > **API Limitation:** The `create_synced_database_table` SDK only supports
# MAGIC > Provisioned Lakebase instances. For Autoscaling projects, synced tables
# MAGIC > must be created via the Databricks Catalog UI.
# MAGIC > ([Terraform issue #5456](https://github.com/databricks/terraform-provider-databricks/issues/5456))
# MAGIC
# MAGIC ## Project Details
# MAGIC - Resource: `projects/ankurhlsproject`
# MAGIC - UID: `74e95be5-74fe-4372-8a6b-369cb3dcf1f7`
# MAGIC - Endpoint: `ep-wandering-meadow-d1tdkigo.database.us-west-2.cloud.databricks.com`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "users", "Catalog")
dbutils.widgets.text("schema", "ankur_nayyar", "Schema")
dbutils.widgets.text("project_id", "ankurhlsproject", "Lakebase Project ID")
dbutils.widgets.dropdown("sync_mode", "SNAPSHOT", ["TRIGGERED", "SNAPSHOT"], "Sync Mode")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
project_id = dbutils.widgets.get("project_id")
sync_mode = dbutils.widgets.get("sync_mode")

print(f"Catalog:    {catalog}")
print(f"Schema:     {schema}")
print(f"Project ID: {project_id}")
print(f"Sync Mode:  {sync_mode}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tables to Sync

# COMMAND ----------

TABLES_TO_SYNC = [
    {"name": "gold_ed_hourly_census", "primary_key": ["event_hour", "location_facility"]},
    {"name": "gold_icu_hourly_census", "primary_key": ["event_hour", "location_facility"]},
    {"name": "gold_ed_daily_summary", "primary_key": ["activity_date", "location_facility"]},
    {"name": "gold_icu_daily_summary", "primary_key": ["activity_date", "location_facility"]},
    {"name": "gold_department_census_current", "primary_key": ["location_facility", "department"]},
    {"name": "gold_forecast_predictions", "primary_key": ["prediction_id"]},
    {"name": "gold_forecast_accuracy", "primary_key": ["prediction_date", "model_name", "department", "target_metric", "forecast_horizon_hours"]},
    {"name": "gold_encounter_fact", "primary_key": ["encounter_key"]},
    {"name": "gold_patient_dim", "primary_key": ["patient_key"]},
]

print(f"Tables to sync: {len(TABLES_TO_SYNC)}")
for t in TABLES_TO_SYNC:
    print(f"  - {t['name']} (PK: {', '.join(t['primary_key'])})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Enable Change Data Feed

# COMMAND ----------

cdf_results = []
for table_cfg in TABLES_TO_SYNC:
    fqn = f"{catalog}.{schema}.{table_cfg['name']}"
    try:
        spark.sql(f"ALTER TABLE {fqn} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
        cdf_results.append({"table": fqn, "cdf_status": "ENABLED"})
        print(f"  CDF enabled: {fqn}")
    except Exception as e:
        err_msg = str(e)
        if "TABLE_OR_VIEW_NOT_FOUND" in err_msg or "DELTA_TABLE_NOT_FOUND" in err_msg:
            cdf_results.append({"table": fqn, "cdf_status": "TABLE_NOT_FOUND"})
            print(f"  SKIPPED (not found): {fqn}")
        else:
            cdf_results.append({"table": fqn, "cdf_status": "OK"})
            print(f"  CDF set: {fqn}")

ready_count = sum(1 for r in cdf_results if r["cdf_status"] in ("ENABLED", "OK"))
print(f"\nCDF complete. {ready_count}/{len(cdf_results)} tables ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Validate Autoscaling Project

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
project_resource = f"projects/{project_id}"
project_valid = False
endpoint_host = None

try:
    project = w.postgres.get_project(name=project_resource)
    project_valid = True
    print(f"Project found: {project.name}")
    if project.spec:
        print(f"  Display Name: {project.spec.display_name}")
        print(f"  PG Version:   {project.spec.pg_version}")

    branches = list(w.postgres.list_branches(parent=project_resource))
    if branches:
        branch = branches[0]
        print(f"  Branch: {branch.name}")
        endpoints = list(w.postgres.list_endpoints(parent=branch.name))
        if endpoints:
            ep = endpoints[0]
            print(f"  Endpoint: {ep.name}")
            if ep.spec:
                endpoint_host = ep.spec.hostname
                print(f"  Host: {endpoint_host}")
                print(f"  Port: {ep.spec.port}")
except Exception as e:
    print(f"Project check: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Synced Tables via UI
# MAGIC
# MAGIC The SDK `create_synced_database_table` does not support Autoscaling projects.
# MAGIC Create synced tables in the Databricks Catalog UI.

# COMMAND ----------

ready_tables = [
    t for t in TABLES_TO_SYNC
    if any(r["table"] == f"{catalog}.{schema}.{t['name']}" and r["cdf_status"] in ("ENABLED", "OK") for r in cdf_results)
]

print("=" * 70)
print("SYNCED TABLE CREATION - UI INSTRUCTIONS")
print("=" * 70)
print()
print("For each table:")
print(f"  1. Go to Catalog > {catalog} > {schema}")
print(f"  2. Click the source table name")
print(f"  3. Click 'Create' > 'Synced Table'")
print(f"  4. Database type: Lakebase Serverless (Autoscaling)")
print(f"  5. Project: {project_id}")
print(f"  6. Sync mode: {sync_mode}")
print(f"  7. Verify Primary Key, click Create")
print()
for t in ready_tables:
    pk = ", ".join(t["primary_key"])
    print(f"  {catalog}.{schema}.{t['name']}")
    print(f"    -> synced name: {t['name']}_synced | PK: {pk}")
    print()
print(f"Total: {len(ready_tables)} tables")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Check Existing Synced Tables

# COMMAND ----------

existing_synced = []
for t in TABLES_TO_SYNC:
    synced_name = f"{catalog}.{schema}.{t['name']}_synced"
    try:
        spark.sql(f"DESCRIBE TABLE {synced_name}")
        existing_synced.append(synced_name)
        print(f"  EXISTS: {synced_name}")
    except Exception:
        print(f"  NOT YET: {synced_name}")

print(f"\n{len(existing_synced)}/{len(TABLES_TO_SYNC)} synced tables exist.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

import json

summary = {
    "catalog": catalog,
    "schema": schema,
    "project": project_resource,
    "project_valid": project_valid,
    "endpoint_host": endpoint_host,
    "sync_mode": sync_mode,
    "cdf_ready": ready_count,
    "synced_existing": len(existing_synced),
    "synced_needed": len(ready_tables) - len(existing_synced),
}

print(json.dumps(summary, indent=2, default=str))
dbutils.notebook.exit(json.dumps(summary, default=str))
