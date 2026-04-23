# Databricks notebook source
# MAGIC %md
# MAGIC # HL7 — Daily insight email (HTML + charts)
# MAGIC
# MAGIC Polished **HTML** digest with inline charts (Unity Catalog gold). Email is sent via **SendGrid**
# MAGIC if you store an API key in a secret (see below); otherwise the same HTML is written to **DBFS
# MAGIC FileStore** and the path is printed.
# MAGIC
# MAGIC ## SendGrid (optional)
# MAGIC 1. Create a [SendGrid](https://sendgrid.com) API key (free tier: 100 emails/day).
# MAGIC 2. Verify a **single sender** (your work email is fine).
# MAGIC 3. In Databricks: create a secret scope, e.g. `email-insight`, with:
# MAGIC    - `sendgrid_api_key` — the API key
# MAGIC    - `from_email` (optional) — verified sender; defaults to the **recipient** widget if omitted
# MAGIC
# MAGIC Grant the **job run identity** (you or a service principal) read access to that scope.
# MAGIC
# MAGIC ## Parameters
# MAGIC | Widget | Description |
# MAGIC |--------|-------------|
# MAGIC | catalog, schema | Unity Catalog location of gold tables |
# MAGIC | recipient | To address for the email |
# MAGIC | secret_scope | Scope name for SendGrid (empty = skip send, write HTML to FileStore only) |

# COMMAND ----------

# MAGIC %pip install -q sendgrid==6.11.0 matplotlib==3.9.2
# MAGIC # Serverless: ensure plotting stack; no-op if already present

# COMMAND ----------

import base64
import io
import os
import re
from datetime import datetime, timezone

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession

# COMMAND ----------

def _esc(s) -> str:
    if s is None:
        return ""
    t = str(s)
    return (
        t.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _fig_to_data_uri(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=120, bbox_inches="tight", facecolor="#0f172a")
    plt.close(fig)
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("ascii")
    return f"data:image/png;base64,{b64}"


def _metric_block(label: str, value: str, sub: str = "") -> str:
    return f"""
    <td style="padding:12px 16px; background:#1e293b; border-radius:8px; width:33%; vertical-align:top;">
      <div style="font-size:11px; text-transform:uppercase; letter-spacing:0.06em; color:#94a3b8;">{label}</div>
      <div style="font-size:24px; font-weight:700; color:#f8fafc; margin:6px 0 4px;">{value}</div>
      <div style="font-size:12px; color:#64748b;">{sub}</div>
    </td>
    """

# COMMAND ----------

dbutils.widgets.text("catalog", "users", "catalog")
dbutils.widgets.text("schema", "ankur_nayyar", "schema")
dbutils.widgets.text("recipient", "ankur.nayyar@databricks.com", "recipient")
dbutils.widgets.text("secret_scope", "email-insight", "secret_scope (SendGrid)")

spark = SparkSession.builder.getOrCreate()
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
recipient = (dbutils.widgets.get("recipient") or "").strip()
secret_scope = (dbutils.widgets.get("secret_scope") or "").strip()

# COMMAND ----------

fqn = f"`{catalog}`.`{schema}`"
ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
day = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# Headline metrics (same spirit as the old SQL alert)
m_enc = spark.sql(
    f"""
    SELECT COUNT(*) AS n FROM {fqn}.`gold_encounter_fact`
    WHERE to_date(created_at) >= date_sub(current_date(), 7)
    """
).collect()[0][0]
m_msg = spark.sql(
    f"""
    SELECT COALESCE(SUM(message_count), 0) AS n FROM {fqn}.`gold_message_metrics`
    WHERE CAST(processing_hour AS timestamp) >= current_timestamp() - INTERVAL 1 DAY
    """
).collect()[0][0]
m_pat = spark.sql(f"SELECT COUNT(*) AS n FROM {fqn}.`gold_patient_dim`").collect()[0][0]
m_ml = spark.sql(
    f"""
    SELECT COUNT(*) AS n FROM {fqn}.`gold_forecast_predictions`
    WHERE forecast_generated_at >= current_timestamp() - INTERVAL 1 DAY
    """
).collect()[0][0]
m_last = spark.sql(
    f"SELECT max(last_message_at) AS t FROM {fqn}.`gold_message_metrics`"
).collect()[0][0]
m_today = spark.sql(
    f"""
    SELECT COUNT(*) AS n FROM {fqn}.`gold_encounter_fact`
    WHERE to_date(created_at) = current_date()
    """
).collect()[0][0]


def _n(v) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


m_enc, m_msg, m_pat, m_ml, m_today = _n(m_enc), _n(m_msg), _n(m_pat), _n(m_ml), _n(m_today)

# Trends
df_ed = spark.sql(
    f"""
    SELECT
      to_date(created_at) AS d,
      CAST(COUNT(*) AS BIGINT) AS encounter_count
    FROM {fqn}.`gold_encounter_fact`
    WHERE created_at >= current_timestamp() - INTERVAL 30 DAY
    GROUP BY to_date(created_at)
    ORDER BY d
    """
).toPandas()

df_hr = spark.sql(
    f"""
    SELECT
      date_trunc('hour', CAST(processing_hour AS timestamp)) AS hr,
      SUM(message_count)::double AS total_messages
    FROM {fqn}.`gold_message_metrics`
    WHERE CAST(processing_hour AS timestamp) >= current_timestamp() - INTERVAL 3 DAY
    GROUP BY 1
    ORDER BY 1
    """
).toPandas()

df_mix = spark.sql(
    f"""
    SELECT
      coalesce(message_type, 'Unknown') AS message_type,
      SUM(message_count)::double AS c
    FROM {fqn}.`gold_message_metrics`
    GROUP BY message_type
    ORDER BY c DESC
    LIMIT 10
    """
).toPandas()

# COMMAND ----------

# Charts
fig1, ax1 = plt.subplots(figsize=(6.2, 2.8), facecolor="#0f172a")
if not df_ed.empty and "d" in df_ed.columns and "encounter_count" in df_ed.columns:
    dts = pd.to_datetime(df_ed["d"])
    ax1.plot(dts, df_ed["encounter_count"], color="#38bdf8", linewidth=2)
    ax1.fill_between(dts, df_ed["encounter_count"], color="#0ea5e4", alpha=0.2)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
else:
    ax1.text(0.5, 0.5, "No 30d encounter data", ha="center", color="#94a3b8")
ax1.set_facecolor("#0f172a")
ax1.set_title("Encounters by day (30 days)", color="#e2e8f0", fontsize=10)
ax1.tick_params(colors="#94a3b8", labelsize=8)
ax1.spines["bottom"].set_color("#334155")
ax1.spines["left"].set_color("#334155")
ax1.yaxis.label.set_color("#94a3b8")
ax1.xaxis.label.set_color("#94a3b8")
ax1.grid(alpha=0.2)
uri1 = _fig_to_data_uri(fig1)

fig2, ax2 = plt.subplots(figsize=(6.2, 2.8), facecolor="#0f172a")
if not df_hr.empty and "hr" in df_hr.columns and "total_messages" in df_hr.columns:
    t = pd.to_datetime(df_hr["hr"])
    ax2.plot(t, df_hr["total_messages"], color="#a78bfa", linewidth=1.6, marker="o", markersize=2)
    ax2.fill_between(t, df_hr["total_messages"], color="#7c3aed", alpha=0.15)
else:
    ax2.text(0.5, 0.5, "No 72h throughput", ha="center", color="#94a3b8")
ax2.set_facecolor("#0f172a")
ax2.set_title("Message throughput (hourly, last ~72h)", color="#e2e8f0", fontsize=10)
ax2.tick_params(colors="#94a3b8", labelsize=8)
ax2.spines["bottom"].set_color("#334155")
ax2.spines["left"].set_color("#334155")
ax2.xaxis.set_major_formatter(mdates.DateFormatter("%a %Hh"))
ax2.grid(axis="y", alpha=0.2)
fig2.autofmt_xdate()
uri2 = _fig_to_data_uri(fig2)

fig3, ax3 = plt.subplots(figsize=(6.2, 3.2), facecolor="#0f172a")
if not df_mix.empty and "c" in df_mix.columns:
    y = list(range(len(df_mix)))
    ax3.barh(y, df_mix["c"].values, color="#34d399", height=0.65)
    ax3.set_yticks(y)
    ax3.set_yticklabels(
        [str(x)[:28] for x in df_mix["message_type"].values] if "message_type" in df_mix.columns else [],
        color="#e2e8f0",
        fontsize=7,
    )
    ax3.invert_yaxis()
else:
    ax3.text(0.5, 0.5, "No message mix", ha="center", color="#94a3b8")
ax3.set_facecolor("#0f172a")
ax3.set_title("Top message types (all time in gold)", color="#e2e8f0", fontsize=10)
ax3.tick_params(colors="#94a3b8", labelsize=8)
ax3.spines["bottom"].set_color("#334155")
ax3.spines["left"].set_color("#334155")
ax3.grid(axis="x", alpha=0.2)
uri3 = _fig_to_data_uri(fig3)

# COMMAND ----------

st_last = "—"
if m_last is not None:
    tlast = pd.to_datetime(m_last, errors="coerce")
    st_last = str(m_last)[:19] if not pd.isna(tlast) else "—"

html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>HL7 data insights — {day}</title>
</head>
<body style="margin:0; padding:0; background:#0b1220; font-family: 'Segoe UI', system-ui, sans-serif; color:#e2e8f0;">
  <div style="max-width:700px; margin:0 auto; padding:24px 16px 40px;">

    <div style="background:linear-gradient(120deg, #0ea5e4 0%, #7c3aed 100%); border-radius:12px; padding:20px 22px; margin-bottom:20px;">
      <div style="font-size:12px; opacity:0.9; margin-bottom:6px;">HL7 Medallion · {catalog}.{schema}</div>
      <h1 style="margin:0; font-size:22px; font-weight:700; letter-spacing:-0.02em;">Daily data insights</h1>
      <p style="margin:8px 0 0; font-size:14px; opacity:0.95;">{_esc(ts)} · Unity Catalog gold · automated digest</p>
    </div>

    <p style="color:#94a3b8; font-size:14px; line-height:1.5; margin:0 0 20px;">
      Key operational metrics from the same gold tables that power the HL7 app and Lakeview dashboards.
      Stale or empty charts usually mean the DLT or Lakebase load has not run recently in this environment.
    </p>

    <table role="presentation" style="width:100%; border-collapse:separate; border-spacing:8px;">
      <tr>
        {_metric_block("Encounters (7d)", f"{m_enc:,.0f}", "gold_encounter_fact")}
        {_metric_block("Messages (24h)", f"{m_msg:,.0f}", "gold_message_metrics")}
        {_metric_block("Total patients", f"{m_pat:,.0f}", "gold_patient_dim")}
      </tr>
      <tr>
        {_metric_block("ML rows (24h)", f"{m_ml:,.0f}", "gold_forecast_predictions")}
        {_metric_block("Encounters today", f"{m_today:,.0f}", "as of UTC day boundary")}
        {_metric_block("Last message at", _esc(st_last), "from gold_message_metrics max")}
      </tr>
    </table>

    <h2 style="color:#e2e8f0; font-size:16px; margin:28px 0 8px; font-weight:600;">Trends</h2>
    <div style="background:#111827; border:1px solid #1e293b; border-radius:10px; padding:8px; margin-bottom:12px;">
      <img src="{uri1}" alt="Encounters 30d" style="width:100%; height:auto; display:block; border-radius:6px;" />
    </div>
    <div style="background:#111827; border:1px solid #1e293b; border-radius:10px; padding:8px; margin-bottom:12px;">
      <img src="{uri2}" alt="Throughput 72h" style="width:100%; height:auto; display:block; border-radius:6px;" />
    </div>
    <div style="background:#111827; border:1px solid #1e293b; border-radius:10px; padding:8px; margin-bottom:8px;">
      <img src="{uri3}" alt="Message types" style="width:100%; height:auto; display:block; border-radius:6px;" />
    </div>

    <p style="color:#64748b; font-size:12px; line-height:1.6; margin:24px 0 0;">
      Generated by job <strong>HL7 Daily insight (HTML + charts email)</strong>.
      <br />Workspace gold path: {catalog}.{_esc(schema)} · Not for clinical or billing decisions.
    </p>
  </div>
</body></html>
"""

print(f"HTML length: {len(html):,} chars")

# COMMAND ----------

# Write to FileStore (always) for audit + open in browser; also used if email not configured
out_name = f"hl7_insight_{day}.html"
db_path = f"dbfs:/FileStore/hl7_insights/{out_name}"
try:
    dbutils.fs.mkdirs("dbfs:/FileStore/hl7_insights")
except Exception as e:
    print(f"mkdirs note: {e}")
try:
    dbutils.fs.put(db_path, html, True)
    print(f"Wrote: {db_path} — open in workspace via **Data** → **DBFS** or mount FileStore")
except Exception as e:
    print(f"dbutils.fs.put failed, trying local dbfs: {e}")
    os.makedirs("/dbfs/FileStore/hl7_insights", exist_ok=True)
    with open(f"/dbfs/FileStore/hl7_insights/{out_name}", "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Wrote: /dbfs/FileStore/hl7_insights/{out_name}")

# COMMAND ----------

ss = (secret_scope or "").strip().lower()
send_try = bool(ss) and ss not in ("none", "-", "false", "0", "skip")

sent = False
if send_try:
    key = None
    try:
        key = dbutils.secrets.get(scope=secret_scope, key="sendgrid_api_key")
    except Exception as e:
        err = str(e)
        if "Secret does not exist" in err:
            print(
                f"SendGrid skipped: no `sendgrid_api_key` in Databricks secret scope `{secret_scope}`.\n"
                f"  Fix: User Settings → Secret scopes → add scope + key, or run: "
                f"`databricks secrets create-scope {secret_scope}` then "
                f"`databricks secrets put-secret {secret_scope} sendgrid_api_key`.\n"
                f"  Or set job param `secret_scope` to empty to skip email (HTML still on DBFS)."
            )
        else:
            short = err if len(err) < 450 else err[:450] + "…"
            print(f"SendGrid: could not read `sendgrid_api_key`: {short}")
    if key:
        try:
            try:
                from_email = dbutils.secrets.get(scope=secret_scope, key="from_email")
            except Exception:
                from_email = None
            if not (from_email or "").strip():
                from_email = recipient
            if not re.match(r"^[^@]+@[^@]+\.[^@]+", (from_email or "").strip()):
                raise ValueError("from_email (secret) or recipient must be a valid email")
            from sendgrid import SendGridAPIClient
            from sendgrid.helpers.mail import Content, Email, Mail, To

            message = Mail(
                from_email=Email(from_email, "HL7 Insights"),
                to_emails=To(recipient),
                subject=f"HL7 data insights — {day}",
                html_content=Content("text/html", html),
            )
            sgc = SendGridAPIClient(key)
            resp = sgc.send(message)
            print(f"SendGrid status: {resp.status_code}")
            if resp.status_code in (200, 201, 202):
                sent = True
        except Exception as e:
            print(f"SendGrid send failed: {e}")

# COMMAND ----------

if not sent:
    print(
        "To receive this digest by email, add SendGrid: "
        f"create secret scope `{_esc(secret_scope) or 'email-insight'}` with `sendgrid_api_key` and optional `from_email`."
    )
    print(
        f"HTML is on DBFS: dbfs:/FileStore/hl7_insights/{out_name} (download or mount to view offline)."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Done
# MAGIC Check output above: either **SendGrid** confirmation or the **DBFS path** to open the same HTML in a browser.
