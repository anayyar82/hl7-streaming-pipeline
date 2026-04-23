# Databricks notebook source
# MAGIC %md
# MAGIC # Grant Lakebase Permissions to HL7App Service Principal
# MAGIC
# MAGIC **Important:** the app’s service principal UUID changes if the Databricks App is recreated.
# MAGIC Set the widget to the **current** value from:
# MAGIC `databricks apps get hl7app -o json | jq -r '.service_principal_client_id'`
# MAGIC (must match **`PGUSER`** in `hl7-forecasting-app/app.yaml`).
# MAGIC
# MAGIC Run this notebook as your user (schema owner) in the workspace that owns the Lakebase project.

# COMMAND ----------

# MAGIC %pip install psycopg2-binary requests
# MAGIC %restart_python

# COMMAND ----------

import requests
import psycopg2

dbutils.widgets.text(
    "app_sp_client_id",
    "",
    "service_principal_client_id (UUID) from: databricks apps get hl7app -o json",
)
dbutils.widgets.text(
    "lakebase_connect_user",
    "",
    "Your Databricks / Lakebase user email. Set for serverless jobs; optional in classic interactive.",
)
APP_SP_CLIENT_ID = dbutils.widgets.get("app_sp_client_id").strip()
if not APP_SP_CLIENT_ID:
    raise ValueError(
        "Set widget **app_sp_client_id** to hl7app's service_principal_client_id UUID "
        "(Apps → hl7app → Environment, or CLI above)."
    )

PG_HOST = "ep-wandering-meadow-d1tdkigo.database.us-west-2.cloud.databricks.com"
PG_PORT = 5432
PG_DBNAME = "databricks_postgres"
PG_SCHEMA = "ankur_nayyar"
PG_ENDPOINT = "projects/ankurhlsproject/branches/production/endpoints/primary"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Credentials (as current user / schema owner)
# MAGIC
# MAGIC **Serverless note:** do not use `getContext().tags()["user"]` (Py4J whitelist in jobs). Set widget **lakebase_connect_user** to your
# MAGIC Databricks email, or set bundle variable `lakebase_connect_user` in `databricks.yml` (dev) / job parameters.

# COMMAND ----------

def _resolve_lakebase_pg_user() -> str:
    w = (dbutils.widgets.get("lakebase_connect_user") or "").strip()
    if w:
        return w
    # Spark (often has user in serverless)
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
    # Classic interactive only (getContext().tags() is blocked in serverless)
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


ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
api_token = ctx.apiToken().get()
pg_user = _resolve_lakebase_pg_user()
if not pg_user:
    raise ValueError(
        "Could not determine Lakebase Postgres user. In serverless jobs, set the "
        "**lakebase_connect_user** widget (or job parameter) to your Databricks login email, "
        "e.g. you@databricks.com — the same user used to get /api/2.0/postgres/credentials."
    )
workspace_url = "https://e2-demo-field-eng.cloud.databricks.com"

cred_resp = requests.post(
    f"{workspace_url}/api/2.0/postgres/credentials",
    headers={"Authorization": f"Bearer {api_token}"},
    json={"endpoint": PG_ENDPOINT},
)
cred_resp.raise_for_status()
pg_pass = cred_resp.json()["token"]
print(f"Connected as: {pg_user}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create OAuth role for the service principal (required)
# MAGIC
# MAGIC The function **`databricks_create_role`** lives in the **`databricks_auth`** extension.
# MAGIC If you skip **`CREATE EXTENSION`**, Postgres returns *function does not exist (42883)*.
# MAGIC
# MAGIC Order: (1) enable extension, (2) create role, (3) GRANT privileges.
# MAGIC
# MAGIC Docs: [Connect external app to Lakebase using SDK](https://docs.databricks.com/aws/en/oltp/projects/external-apps-connect#create-postgres-role-for-the-service-principal)

# COMMAND ----------

conn = psycopg2.connect(
    host=PG_HOST,
    port=PG_PORT,
    dbname=PG_DBNAME,
    user=pg_user,
    password=pg_pass,
    sslmode="require",
)
conn.autocommit = True
cur = conn.cursor()

sp = APP_SP_CLIENT_ID

print("Step 1: Enable databricks_auth extension (defines databricks_create_role)")
try:
    cur.execute("CREATE EXTENSION IF NOT EXISTS databricks_auth")
    print("  -> OK")
except Exception as e:
    print(f"  -> ERROR: {e}")
    raise

print("\nStep 2: Register OAuth role for service principal (use explicit ::text casts)")
print("  Note: second arg must be lowercase 'service_principal' per Databricks Apps + Lakebase tutorial.")
create_role_sql = "SELECT databricks_create_role(%s::text, %s::text)"
try:
    cur.execute(create_role_sql, (sp, "service_principal"))
    row = cur.fetchone()
    print(f"  -> OK: {row}")
except Exception as e:
    print(f"  -> WARN (role may already exist): {e}")

cur.close()
conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run GRANT Statements

# COMMAND ----------

conn = psycopg2.connect(
    host=PG_HOST,
    port=PG_PORT,
    dbname=PG_DBNAME,
    user=pg_user,
    password=pg_pass,
    sslmode="require",
)
conn.autocommit = True
cur = conn.cursor()

sp = APP_SP_CLIENT_ID

grants = [
    f'GRANT USAGE ON SCHEMA "{PG_SCHEMA}" TO "{sp}"',
    f'GRANT SELECT ON ALL TABLES IN SCHEMA "{PG_SCHEMA}" TO "{sp}"',
    f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{PG_SCHEMA}" GRANT SELECT ON TABLES TO "{sp}"',
    f'GRANT CONNECT ON DATABASE "{PG_DBNAME}" TO "{sp}"',
]

print(f"Granting permissions to service principal: {sp}")
print(f"Schema: {PG_SCHEMA}")
print("=" * 70)

for grant_sql in grants:
    try:
        print(f"  Running: {grant_sql}")
        cur.execute(grant_sql)
        print(f"  -> OK")
    except Exception as e:
        print(f"  -> WARN: {e}")

cur.close()
conn.close()
print("\nAll grants applied.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify: List Tables Accessible by SP

# COMMAND ----------

conn = psycopg2.connect(
    host=PG_HOST,
    port=PG_PORT,
    dbname=PG_DBNAME,
    user=pg_user,
    password=pg_pass,
    sslmode="require",
)
cur = conn.cursor()

sp = APP_SP_CLIENT_ID

cur.execute(f"""
    SELECT grantee, table_schema, table_name, privilege_type
    FROM information_schema.table_privileges
    WHERE grantee = '{sp}'
      AND table_schema = '{PG_SCHEMA}'
    ORDER BY table_name, privilege_type
""")
rows = cur.fetchall()

if rows:
    print(f"Grants for {sp} on schema {PG_SCHEMA}:")
    print("-" * 80)
    for r in rows:
        print(f"  {r[2]:45s} {r[3]}")
    print(f"\nTotal: {len(rows)} grant(s)")
else:
    print(f"No explicit table grants found for {sp}.")
    print("This may be normal if grants are inherited via role membership.")

cur.close()
conn.close()
