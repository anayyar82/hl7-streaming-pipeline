# HL7 streaming — reference architecture

## Purpose

End-to-end flow for **ED & ICU** operational analytics: **HL7** (and similar feeds) land in Unity Catalog, move through a **medallion** DLT pipeline, support **ML** training and batch scoring, and sync the **gold** read model to **Lakebase Postgres** for the **HL7App** (Streamlit / AppKit) and **Genie**.

## Data path (logical layers)

| Layer | Role | Technology |
|-------|------|------------|
| L1 | Raw HL7 batches, file landing | UC **volume** (`0a` sample, volume paths in bundle vars) |
| L2 | Parse, conform, de-duplicate | **DLT** bronze / silver (Lakeflow) |
| L3 | Business facts and dimensions | DLT **gold** tables in UC (catalog + schema) |
| L4 | Feature prep, training, batch inference | Jobs: AutoML, inference, optional sync |
| L5 | Analytical / low-latency read model | **Lakebase** (Postgres) — `databricks_postgres` |
| L6 | Consumption | Databricks **Apps** (Streamlit, AppKit), **Genie**, Lakeview **dashboards** |

## Operational run order (full stack)

For a full refresh, jobs are typically run in this dependency order (see `scripts/run_full_stack_full_refresh.sh` and the **Run jobs** page in the app):

1. **Sample → volume** (optional — regenerates synthetic HL7 in landing)  
2. **DLT pipeline** (full-refresh or incremental)  
3. **ML training** (optional, long)  
4. **Batch inference**  
5. **Lakebase load** (gold → Postgres)

A bundle workflow may wrap **DLT → inference → Lakebase** as a single multi-task job where configured.

## Data flow (detail)

1. **Ingest** — Files land in Unity Catalog **volumes**; optional synthetic HL7 for demos (`0a` sample). No direct app writes to gold.
2. **DLT (Lakeflow)** — **Bronze** raw rows, **silver** conformed, **gold** business facts/aggregates. Incremental or full refresh; Photon where enabled.
3. **Gold in UC** — `USE CATALOG` / `USE SCHEMA` / `SELECT` granted to the automation identity that syncs to the read path (and to **Genie**-backed analysis when configured).
4. **ML** — **AutoML** and batch **inference** jobs read gold features, write scores back to **Delta** (then mirrored to the read path for the app).
5. **Read-optimized path** — Job-driven sync from gold into a **JDBC/Postgres**-compatible service (e.g. Lakebase) for sub-second app queries. OAuth via Databricks; login name maps to a **Databricks-managed role** (`databricks_create_role`), not a shared user password in the app image.
6. **Consumption** — This **app** and **Genie** share the same governed tables (via grants). Deep links to jobs/pipelines use `DATABRICKS_ORG_ID` and job IDs in environment configuration.

## Security and identity (architecture)

- **Unity Catalog (UC)**  
  - Grant **least privilege**: `USE CATALOG`, `USE SCHEMA`, `SELECT` on the HL7 **gold** schema for the app’s service principal.  
  - **Genie** also needs the same (or a curated subset) and **`CAN USE`** on the **SQL warehouse** the Genie space is attached to.

- **App identity (Databricks Apps)**  
  - Each app runs as a **service principal** (or delegated user context per product behavior). The bundle sets **`user_api_scopes`** (e.g. `sql`, `dashboards.genie`) so the runtime can call workspace APIs.  
  - **No static warehouse passwords in Git**: connection uses OAuth/token flows provided by the Apps + Lakebase product.

- **Analytical read path (JDBC, e.g. Lakebase)**  
  - **`databricks_create_role`** maps the Databricks app identity to a **Postgres role**; grants notebook `11_lakebase_app_grants.py` aligns `SELECT` with UC.  
  - Access is **governed** the same as warehouse SQL against the same logical data.

- **Network and secrets**  
  - Prefer private connectivity to data planes where your org requires it; hostnames in `app.yaml` are environment-specific.  
  - **Secrets** (e.g. SendGrid) use **secret scopes** in jobs, not the Streamlit bundle.

- **Reference notebooks**  
  - `11_lakebase_grants.py`, `12_genie_uc_grants.py` — one-shot grants and alignment checks.

## Code map

| Area | Location |
|------|----------|
| DLT + jobs bundle | `resources/hl7_pipeline.yml`, `databricks.yml` |
| Streamlit UI | `hl7-forecasting-app/` |
| AppKit UI | `hl7-appkit-app/`, `bundles/hl7_appkit/` |
| Design / ops (this file) | `docs/ARCHITECTURE.md` |

## External references

- [Lakeflow (DLT)](https://docs.databricks.com/lakeflow/index.html)  
- [Databricks Apps](https://docs.databricks.com/en/dev-tools/databricks-apps.html)  
- [AppKit](https://databricks.github.io/appkit/docs/)  
- [Genie: API, RBAC with OAuth, factory rollout](GENIE_API_RBAC_AND_FACTORY.md) (this repo)
