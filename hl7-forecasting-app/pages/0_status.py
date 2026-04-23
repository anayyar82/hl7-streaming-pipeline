"""
System status — Lakebase gold freshness, row counts, and runbook pointers.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from utils.db import run_query, PGHOST, PGDATABASE, ENDPOINT_NAME
from utils import queries
from utils.theme import apply_theme
from utils.navigation import render_sidebar_nav
from utils.streamlit_refresh import run_live_dashboard
from utils.health import render_status_slo_banner

st.set_page_config(page_title="System Status", page_icon="📡", layout="wide")
apply_theme()
render_sidebar_nav()
st.title("System status")
st.caption(
    "Snapshot of **Lakebase** gold tables (row counts and latest activity). "
    "Use this page to see whether loads and ML outputs look current."
)

with st.expander("What this page does", expanded=False):
    st.markdown(
        """
**Purpose**  
A **health check** for the Lakebase copy of your gold tables—not the full Databricks UI.

**What you will see**  
- Whether Postgres can be reached and which **schema** is queried.  
- A **matrix of tables**: approximate row counts, latest timestamp columns where available, and **ok / stale / critical** based on configurable thresholds.  
- Short **runbook** text: which Databricks jobs to run (DLT, inference, Lakebase load) when something looks old or empty.

**When to use it**  
Before demos or after deployments; when charts elsewhere look empty or out of date.
        """
    )

# SLO strip + DLT: keep **outside** the auto-refresh fragment so links and subheaders do not
# re-mount every few seconds (reduces layout flicker in Databricks App iframes).
with st.container(border=True):
    render_status_slo_banner()


def _hours_since(ts) -> float | None:
    if ts is None:
        return None
    t = pd.to_datetime(ts, errors="coerce")
    if pd.isna(t):
        return None
    t = t.tz_localize(None) if getattr(t, "tzinfo", None) else t
    return (pd.Timestamp.now() - t).total_seconds() / 3600.0


def _lakebase_table_exists(table: str) -> bool:
    df = run_query(
        """
        SELECT 1 AS one
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        LIMIT 1
        """,
        (queries.SCHEMA, table),
        quiet=True,
    )
    return not df.empty


def _status_label(age_h: float | None, stale_after: float | None) -> str:
    if age_h is None:
        return "—"
    # No threshold in STATUS_MONITOR_SPECS: show age in "Age" but do not label as ok/stale/critical
    if stale_after is None:
        return "no SLO"
    if age_h <= stale_after:
        return "ok"
    if age_h <= stale_after * 2:
        return "stale"
    return "critical"


def _status_main() -> None:
    if st.button("Refresh snapshot", type="primary"):
        st.rerun()
    
    # ---- Lakebase connectivity (same pool as other pages) ----
    st.subheader("Connection")
    c1, c2, c3 = st.columns(3)
    c1.metric("Postgres host", PGHOST.split(".")[0] + "…")
    c2.metric("Database", PGDATABASE)
    c3.metric("Lakebase endpoint", ENDPOINT_NAME.split("/")[-1] or ENDPOINT_NAME)
    
    st.markdown("---")
    
    # ---- Forecast activity (single query) ----
    st.subheader("ML predictions (24h)")
    try:
        pred_exists = _lakebase_table_exists("gold_forecast_predictions")
        fa = run_query(queries.STATUS_FORECAST_ACTIVITY, quiet=True)
        if not pred_exists:
            st.warning(
                "Table **`gold_forecast_predictions`** is not in Lakebase yet. "
                "Run **`hl7_lakebase_load`** after Delta has rows (pipeline **`05_forecasting`**, then **`hl7_model_inference`**)."
            )
        elif fa.empty:
            st.warning(
                "Could not read **`gold_forecast_predictions`** from Lakebase (query returned no result). "
                "Check app SP grants and that the table exists in schema **`ankur_nayyar`**."
            )
        else:
            r0 = fa.iloc[0]
            last_run = r0.get("last_run")
            rows_24h = int(r0.get("rows_24h") or 0)
            if last_run is not None or rows_24h > 0:
                m1, m2, m3 = st.columns(3)
                m1.metric("Last inference time", str(last_run)[:19])
                m2.metric("Prediction rows (24h)", f"{rows_24h:,}")
                m3.metric("Models in table", int(r0.get("distinct_models") or 0))
            else:
                st.info(
                    "Lakebase has **`gold_forecast_predictions`**, but it is **empty** (no scored rows yet). "
                    "Typical order: DLT **`05_forecasting`** → **`hl7_automl_training`** (registers **champion** models) → **`hl7_model_inference`** → **`hl7_lakebase_load**`. "
                    "Use **Run jobs & workflow** in the sidebar, or `databricks bundle run hl7_model_inference -t dev` then `hl7_lakebase_load`."
                )
    except Exception as e:
        st.warning(f"Forecast summary: {e}")
    
    st.markdown("---")
    
    # ---- Per-table matrix ----
    st.subheader("Gold table freshness")
    st.caption(
        "**ok** = last activity within the table’s SLO target · **stale** = up to **2×** that target · **critical** = older. "
        "**no SLO** = we only show last activity, no health rule (e.g. `gold_forecast_accuracy`). "
        "**Patient** SLO is **7d**; **encounters** is **3d**—same `last_updated_at` can be **stale** for one and **ok** for the other. "
        "The **SLO snapshot** on this page uses three headline rules only."
    )
    st.caption(
        "**📭 not in Lakebase** = Postgres has no table yet (run **`hl7_lakebase_load`** and check **`hl7_lakebase_app_grants`**). "
        "**❓ no data** = query error. Forecast feature tables also need DLT **`05_forecasting`** in UC before load can copy rows."
    )
    
    rows_out = []
    
    for spec in queries.STATUS_MONITOR_SPECS:
        df = run_query(spec["sql"], quiet=True)
        stale_h = spec.get("stale_after_hours")
        if df.empty:
            chk = "missing in Lakebase" if not _lakebase_table_exists(spec["table"]) else "no data"
            rows_out.append(
                {
                    "Layer": spec["layer"],
                    "Area": spec["area"],
                    "Table": spec["table"],
                    "Rows": None,
                    "Last activity": None,
                    "Age (h)": None,
                    "Check": chk,
                }
            )
            continue
        r = df.iloc[0]
        rc = r.get("row_count")
        la = r.get("last_activity")
        try:
            rc_int = int(rc) if rc is not None and not pd.isna(rc) else 0
        except (TypeError, ValueError):
            rc_int = 0
        age = _hours_since(la)
        la_missing = la is None or pd.isna(pd.to_datetime(la, errors="coerce"))
        label = _status_label(age, stale_h)
        if rc_int == 0 and la_missing:
            label = "empty"
    
        rows_out.append(
            {
                "Layer": spec["layer"],
                "Area": spec["area"],
                "Table": spec["table"],
                "Rows": rc_int,
                "Last activity": la,
                "Age (h)": round(age, 1) if age is not None else None,
                "Check": label,
            }
        )
    
    summary = pd.DataFrame(rows_out)
    if not summary.empty:
        ok_n = (summary["Check"] == "ok").sum()
        stale_n = (summary["Check"] == "stale").sum()
        crit_n = (summary["Check"] == "critical").sum()
        slo_na_n = (summary["Check"] == "no SLO").sum()
        s1, s2, s3, s4 = st.columns(4)
        s1.metric("Tables · ok", int(ok_n))
        s2.metric("Tables · stale", int(stale_n))
        s3.metric("Tables · critical", int(crit_n))
        s4.metric("Tables tracked", len(summary))
        if slo_na_n:
            st.caption(f"**{slo_na_n}** table(s) have **no age SLO** in config (e.g. accuracy roll-up) — we still show *Age* only.")
    
        display = summary.copy()
        _chk = {
            "ok": "✅ ok",
            "stale": "⚠️ stale",
            "critical": "🔴 critical",
            "no SLO": "ℹ️ no SLO (age only)",
            "empty": "⬜ empty",
            "no data": "❓ no data",
            "missing in Lakebase": "📭 not in Lakebase",
            "n/a": "➖ n/a",
            "—": "—",
        }
        display["Check"] = display["Check"].map(lambda x: _chk.get(str(x), str(x)))
        h = min(560, 38 + 32 * max(4, len(display)))
        st.dataframe(
            display,
            use_container_width=True,
            hide_index=True,
            height=h,
        )
    else:
        st.warning("No status rows built.")
    
    st.markdown("---")
    
    # ---- Runbook (Databricks; not queried live — app has SQL scope to Lakebase) ----
    with st.expander("Runbook — what to run in Databricks when something is stale", expanded=False):
        st.markdown(
            """
    1. **DLT** — Run or refresh **`hl7_streaming_pipeline`** (Bronze → Gold). The **`05_forecasting`** notebook must complete so `gold_*_forecast_features` exist in UC with data.  
    2. **Lakebase load** — Job **`hl7_lakebase_load`**: `databricks bundle run hl7_lakebase_load -t dev` — creates missing Postgres tables and copies gold; re-run after DLT or inference.  
    3. **ML inference** — Job **`hl7_model_inference`** after features exist — updates `gold_forecast_predictions` in Delta; re-run **lakebase load** so this app sees new rows.  
    4. **AutoML** — **`hl7_automl_training`** when you need new champion models.  
    
    **Order (typical):** DLT → (optional AutoML) → inference → lakebase load → refresh this page.
    
    **From this app:** **0a · Sample → volume** lands HL7 on the volume. **Run jobs & workflow** (`pages/z_run_jobs.py`) is the one-click path when **`HL7_JOB_REFRESH_WORKFLOW`** is set — DLT, inference, and Lakebase load. **0b · Live activity** shows live runs.
    
    **Git app deploy:** If the workspace requires Git, push to the repo configured on **hl7app** and deploy from **Compute → Apps**.
            """
        )
    
    st.caption("Data source: Lakebase schema `ankur_nayyar` · Times are as stored in Postgres (naive local).")

run_live_dashboard(_status_main, interval_seconds=45, manual_key="hl7_status_live_refresh")
