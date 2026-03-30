# Databricks notebook source
# MAGIC %md
# MAGIC # Grant Unity Catalog + warehouse access for HL7App (Genie)
# MAGIC
# MAGIC Run as a user who can grant on the catalog/schema and on the SQL warehouse (e.g. metastore admin or object owner).
# MAGIC
# MAGIC This grants the **hl7app** service principal:
# MAGIC - **`USE CATALOG`**, **`USE SCHEMA`**, **`SELECT`** on the HL7 gold schema (so Genie-generated SQL can read tables)
# MAGIC - **`CAN USE`** on the SQL warehouse your Genie space uses (must match the warehouse configured on the Genie space)
# MAGIC
# MAGIC **Before running:** confirm the app service principal UUID:
# MAGIC ```bash
# MAGIC databricks apps get hl7app -o json | jq -r .service_principal_client_id
# MAGIC ```
# MAGIC
# MAGIC If it differs from the default widget value, update the widget and re-run.

# COMMAND ----------

dbutils.widgets.text("catalog", "users", "Catalog")
dbutils.widgets.text("schema", "ankur_nayyar", "Schema (HL7 gold)")
dbutils.widgets.text(
    "app_service_principal",
    "f348a6bb-fdfc-4826-9fa4-6cbc0d156ae8",
    "hl7app service principal (client) ID",
)
dbutils.widgets.text(
    "warehouse_name",
    "",
    "SQL warehouse NAME for Genie (empty = skip warehouse grant)",
)

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
sp = dbutils.widgets.get("app_service_principal").strip()
wh_name = dbutils.widgets.get("warehouse_name").strip()

# UC principal: service principal UUID in backticks
principal = f"`{sp}`"
full_schema = f"`{catalog}`.`{schema}`"

print(f"Catalog: {catalog}")
print(f"Schema:  {schema}")
print(f"Principal: {principal}")
print(f"Warehouse grant: {'yes — ' + wh_name if wh_name else 'skipped (widget empty)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply grants

# COMMAND ----------

grants = [
    f"GRANT USE CATALOG ON CATALOG `{catalog}` TO {principal}",
    f"GRANT USE SCHEMA ON SCHEMA {full_schema} TO {principal}",
    f"GRANT SELECT ON SCHEMA {full_schema} TO {principal}",
]

if wh_name:
    grants.append(f"GRANT CAN_USE ON WAREHOUSE `{wh_name}` TO {principal}")

for sql in grants:
    print(f"\n>>> {sql}")
    spark.sql(sql)

print("\nDone.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: verify schema privileges
# MAGIC
# MAGIC Adjust if your workspace exposes a different information schema view.

# COMMAND ----------

display(spark.sql(f"SHOW GRANTS ON SCHEMA `{catalog}`.`{schema}`"))
