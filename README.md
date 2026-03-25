# HL7 Streaming Pipeline with Databricks

A production-ready HL7 v2.x streaming pipeline built on Databricks using Delta Live Tables (DLT) for declarative data processing, with real-time ED/ICU reporting, predictive forecasting, and an interactive Streamlit dashboard served via Databricks Apps and backed by Lakebase Postgres.

**Powered by [funke](https://github.com/databricks-field-eng/funke)** -- Databricks Field Engineering library for HL7v2 message parsing.

---

## End-to-End Architecture

```
                              Databricks Asset Bundles (IaC)
  ┌──────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                  │
  │   HL7 Source Files        Delta Live Tables Pipeline (Photon)                    │
  │   ┌──────────┐     ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌─────────────┐   │
  │   │ Volume:  │────▶│  Bronze  │─▶│  Silver  │─▶│   Gold   │─▶│  Reports &  │   │
  │   │ landing/ │     │ Raw+Parse│  │ Segments │  │ Dim+Fact │  │ Forecasting │   │
  │   └──────────┘     └──────────┘  └──────────┘  └────┬─────┘  └──────┬──────┘   │
  │                                                      │               │           │
  │              ┌───────────────────────────────────────┘               │           │
  │              │                                                       │           │
  │              ▼                                                       ▼           │
  │   ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────────┐      │
  │   │ AutoML Training  │    │ Model Inference  │    │  Lakebase Load Job   │      │
  │   │ (07_automl)      │───▶│ (08_inference)   │    │  (10_lakebase_load)  │      │
  │   └──────────────────┘    └────────┬─────────┘    └──────────┬─────────┘       │
  │                                     │                         │                  │
  │              ┌──────────────────────┘                         │                  │
  │              ▼                                                ▼                  │
  │   ┌──────────────────┐                          ┌──────────────────────┐        │
  │   │ MLflow Registry  │                          │  Lakebase Postgres   │        │
  │   │ (Versioned Models)│                          │  (ankurhlsproject)   │        │
  │   └──────────────────┘                          └──────────┬───────────┘        │
  │                                                             │                    │
  │   ┌────────────────────┐    ┌────────────────────┐         │                    │
  │   │ Lakeview Dashboard │    │ Databricks App     │◀────────┘                    │
  │   │ (ED/ICU Ops)       │    │ (HL7App/Streamlit) │                              │
  │   └────────────────────┘    └────────────────────┘                              │
  │                                                                                  │
  └──────────────────────────────────────────────────────────────────────────────────┘
```

### Data Flow Summary

1. **Ingest** -- Raw HL7 v2.x files land in a Unity Catalog Volume (`landing/`)
2. **Parse** -- DLT Bronze layer splits multi-message files and parses via `funke`
3. **Structure** -- DLT Silver extracts typed segments (MSH, PID, PV1, OBX, DG1, AL1, OBR)
4. **Enrich** -- DLT Gold creates dimensional models, fact tables, and operational metrics
5. **Report** -- DLT builds real-time ED/ICU census, hourly/daily summaries
6. **Forecast** -- DLT engineers features; AutoML trains models; inference scores predictions
7. **Serve** -- Gold tables are loaded into Lakebase Postgres for low-latency OLTP reads
8. **Visualize** -- Streamlit app (Databricks App) connects to Lakebase for interactive dashboards

---

## Medallion Architecture

### Pipeline Notebooks

| Notebook | Layer | Description |
|----------|-------|-------------|
| `01_bronze.py` | Bronze | Raw HL7 ingestion, multi-message splitting, funke parsing |
| `02_silver.py` | Silver | Segment extraction (MSH, PID, PV1, OBX, DG1, AL1, OBR) with quality rules |
| `03_gold.py` | Gold | Dimensions (patient), fact tables (encounter, observation, diagnosis, allergy, order), message metrics, patient activity |
| `04_reports.py` | Reports | Real-time ED & ICU census, hourly arrivals/discharges, daily summaries |
| `05_forecasting.py` | Forecasting | Feature engineering (lag, rolling, calendar, cross-dept), prediction outputs, accuracy tracking |

### Operational Notebooks

| Notebook | Purpose |
|----------|---------|
| `07_automl_training.py` | Trains ED/ICU forecast models via Databricks AutoML + MLflow |
| `08_model_inference.py` | Batch-scores latest features against registered models |
| `09_lakebase_sync.py` | Enables Change Data Feed for Unity Catalog tables |
| `10_lakebase_load.py` | Loads gold tables into Lakebase Postgres (TRUNCATE + batch INSERT) |
| `11_lakebase_grants.py` | Grants Postgres permissions to the app service principal |

---

## Lakebase Integration

The pipeline uses **Databricks Lakebase Autoscaling** to serve gold tables as a low-latency Postgres database for the Streamlit app.

```
Unity Catalog (Gold Tables)
        │
        │  10_lakebase_load.py
        │  (Spark read → psycopg2 batch INSERT)
        ▼
┌─────────────────────────────────────┐
│  Lakebase Autoscaling Project       │
│  Project: ankurhlsproject           │
│  Host: ep-wandering-meadow-*.com    │
│  Database: databricks_postgres      │
│  Schema: ankur_nayyar               │
│                                     │
│  18 gold tables loaded              │
│  OAuth credentials via REST API     │
│                                     │
│  Data API URL (PostgREST):          │
│  https://ep-wandering-meadow-       │
│  d1tdkigo.database.us-west-2.       │
│  cloud.databricks.com/api/2.0/      │
│  workspace/1444828305810485/         │
│  rest/databricks_postgres            │
└─────────────────────────────────────┘
        │
        │  psycopg3 ConnectionPool
        │  (OAuth token rotation)
        ▼
┌─────────────────────────────────────┐
│  Databricks App: hl7app             │
│  Framework: Streamlit               │
│  7 dashboard pages with filters     │
│  Service Principal auth             │
└─────────────────────────────────────┘
```

### Lakebase Setup (Step-by-Step)

#### 1. Create the Lakebase Project

Create a project in Lakebase App (e.g., `ankurhlsproject`) with Postgres 17. Wait for compute to become **ACTIVE**.

#### 2. Load Gold Tables

Run the Lakebase load notebook or DAB job:

```bash
databricks bundle run hl7_lakebase_load -t dev
```

This runs `notebooks/10_lakebase_load.py`, which reads each gold table from Unity Catalog via Spark and batch-inserts rows into Lakebase Postgres under the `ankur_nayyar` schema.

#### 3. Create the OAuth Postgres Role for the App Service Principal

The Databricks App runs as a service principal. That SP needs a Postgres role with `LAKEBASE_OAUTH_V1` auth to connect.

**Option A -- CLI (recommended):**

```bash
# Find the app's service principal client ID
databricks apps get hl7app | grep service_principal_client_id

# Create the OAuth role (identity_type must be SERVICE_PRINCIPAL)
databricks postgres create-role \
  projects/ankurhlsproject/branches/production \
  --json '{
    "spec": {
      "postgres_role": "<DATABRICKS_CLIENT_ID>",
      "identity_type": "SERVICE_PRINCIPAL"
    }
  }'
```

Verify the role was created correctly:

```bash
databricks postgres list-roles projects/ankurhlsproject/branches/production
```

You should see:

```
auth_method:   LAKEBASE_OAUTH_V1
identity_type: SERVICE_PRINCIPAL
```

> **Warning:** If the role shows `auth_method: NO_LOGIN` and `identity_type: IDENTITY_TYPE_UNSPECIFIED`, it was not registered as an OAuth role. Delete it and recreate using the CLI command above. This was the root cause of persistent `password authentication failed` errors. The SQL function `databricks_create_role()` can sometimes produce a `NO_LOGIN` role.

**Option B -- SQL (Lakebase SQL Editor):**

```sql
CREATE EXTENSION IF NOT EXISTS databricks_auth;
SELECT databricks_create_role('<DATABRICKS_CLIENT_ID>'::text, 'service_principal'::text);
```

After creating the role, verify via CLI that it shows `LAKEBASE_OAUTH_V1`. If it shows `NO_LOGIN`, delete and recreate via CLI Option A.

**Option C -- Lakebase UI:**

Navigate to your project → branch → **Roles & Databases** → **Add role** → **OAuth** tab → select the service principal.

#### 4. Grant Permissions

Connect to Lakebase as the schema owner and run:

```sql
GRANT CONNECT ON DATABASE databricks_postgres
  TO "<DATABRICKS_CLIENT_ID>";

GRANT USAGE ON SCHEMA ankur_nayyar
  TO "<DATABRICKS_CLIENT_ID>";

GRANT SELECT ON ALL TABLES IN SCHEMA ankur_nayyar
  TO "<DATABRICKS_CLIENT_ID>";

ALTER DEFAULT PRIVILEGES IN SCHEMA ankur_nayyar
  GRANT SELECT ON TABLES TO "<DATABRICKS_CLIENT_ID>";
```

Or run `notebooks/11_lakebase_grants.py` which executes all of the above.

You can also run grants via `psql`:

```bash
# Get a Postgres credential
TOKEN=$(databricks auth token --profile <profile> | jq -r .access_token)
PG_TOKEN=$(curl -s -X POST "$WORKSPACE_URL/api/2.0/postgres/credentials" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"endpoint": "projects/ankurhlsproject/branches/production/endpoints/primary"}' \
  | jq -r .token)

# Connect and run grants
PGPASSWORD="$PG_TOKEN" psql \
  -h ep-wandering-meadow-d1tdkigo.database.us-west-2.cloud.databricks.com \
  -p 5432 -U "your.email@databricks.com" -d databricks_postgres \
  -c 'GRANT CONNECT ON DATABASE databricks_postgres TO "<SP_UUID>";' \
  -c 'GRANT USAGE ON SCHEMA ankur_nayyar TO "<SP_UUID>";' \
  -c 'GRANT SELECT ON ALL TABLES IN SCHEMA ankur_nayyar TO "<SP_UUID>";' \
  -c 'ALTER DEFAULT PRIVILEGES IN SCHEMA ankur_nayyar GRANT SELECT ON TABLES TO "<SP_UUID>";'
```

#### 5. Verify Grants

```sql
SELECT table_name, privilege_type
FROM information_schema.table_privileges
WHERE grantee = '<DATABRICKS_CLIENT_ID>'
  AND table_schema = 'ankur_nayyar'
ORDER BY table_name;
```

All 13+ gold tables should show `SELECT` privileges.

### Loaded Tables

| # | Table | Source | Description |
|---|-------|--------|-------------|
| 1 | `gold_ed_hourly_census` | 04_reports | Hourly ED arrivals, discharges, net change |
| 2 | `gold_icu_hourly_census` | 04_reports | Hourly ICU arrivals, discharges, net change |
| 3 | `gold_ed_daily_summary` | 04_reports | Daily ED volume, LOS, peak hours |
| 4 | `gold_icu_daily_summary` | 04_reports | Daily ICU volume, LOS, beds used |
| 5 | `gold_department_census_current` | 04_reports | Live department census snapshot |
| 6 | `gold_forecast_predictions` | 05_forecasting | Scored predictions from ML models |
| 7 | `gold_forecast_accuracy` | 05_forecasting | Prediction accuracy metrics (MAE, MAPE) |
| 8 | `gold_encounter_fact` | 03_gold | Patient encounters with location, provider |
| 9 | `gold_patient_dim` | 03_gold | Patient demographics |
| 10 | `gold_observation_fact` | 03_gold | Lab results and vitals |
| 11 | `gold_diagnosis_fact` | 03_gold | Diagnosis codes (ICD) |
| 12 | `gold_allergy_fact` | 03_gold | Patient allergies and severity |
| 13 | `gold_order_fact` | 03_gold | Lab/procedure orders |
| 14 | `gold_message_metrics` | 03_gold | Hourly message processing metrics |
| 15 | `gold_patient_activity` | 03_gold | Daily patient activity by class |
| 16 | `gold_ed_forecast_features` | 05_forecasting | ED feature vectors (lag, rolling, calendar) |
| 17 | `gold_icu_forecast_features` | 05_forecasting | ICU feature vectors |
| 18 | `gold_combined_forecast_features` | 05_forecasting | Cross-department features |

---

## Databricks App (HL7App)

Interactive Streamlit dashboard deployed as a Databricks App, connecting to Lakebase Postgres for low-latency queries. All pages include sidebar filters for interactive exploration.

**URL:** `https://hl7app-1444828305810485.aws.databricksapps.com`

### Dashboard Pages

| Page | Name | Filters | Description |
|------|------|---------|-------------|
| 1 | **Real-Time Ops** | Department, Facility, Time Window, Weekday/Weekend | Live ED & ICU census, hourly arrivals/discharges, cumulative net, peak hour detection |
| 2 | **Trends** | Date Range, Facility, Weekday/Weekend | Daily summaries, LOS analytics, hour-of-day arrival heatmaps, ED vs ICU daily comparison |
| 3 | **ML Forecasting** | Department, Metric, Horizon | Predicted vs actual values, confidence intervals, forecast timelines |
| 4 | **Model Performance** | Model Selection | Accuracy metrics (MAE, MAPE), coverage trends, model comparison scatter |
| 5 | **Patient & Clinical** | Coding System, Top N, Value Type, Severity, Priority, Provider Search | Demographics, top diagnoses, lab results, allergies, orders, provider activity |
| 6 | **Combined Forecast** | Date Range, Weekday/Weekend | ED+ICU system pressure, ED-to-ICU ratio, rolling averages, feature heatmaps |
| 7 | **Operations** | Date Range, Message Type, Facility, Patient Class | Message throughput, pipeline health, data freshness, patient activity by class |

### App Architecture

```
hl7-forecasting-app/
├── app.py                  # Landing page: Lakebase connection card, table count, page index
├── app.yaml                # Databricks App config (env vars, startup command)
├── requirements.txt        # Python deps: streamlit, psycopg, plotly, databricks-sdk, requests
├── utils/
│   ├── db.py               # Lakebase connection (ConnectionPool + OAuth token rotation)
│   ├── queries.py          # All SQL queries organized by dashboard page
│   └── filters.py          # Reusable sidebar filter components (facility, date, department, etc.)
└── pages/
    ├── 1_realtime.py        # Real-time operations with department/facility/time filters
    ├── 2_trends.py          # Trend analytics with date range/facility/weekend filters
    ├── 3_forecasting.py     # ML forecast visualization with dept/metric/horizon filters
    ├── 4_model_perf.py      # Model accuracy tracking with model selection
    ├── 5_patient_clinical.py # Patient & clinical analytics with coding/severity/priority filters
    ├── 6_combined_forecast.py # Combined ED+ICU forecasting with date range/weekend filters
    └── 7_operations.py      # Pipeline operations with message type/facility/class filters
```

### App Configuration (`app.yaml`)

```yaml
command: ['streamlit', 'run', 'app.py']
env:
  - name: PGHOST
    value: 'ep-wandering-meadow-d1tdkigo.database.us-west-2.cloud.databricks.com'
  - name: PGDATABASE
    value: 'databricks_postgres'
  - name: PGPORT
    value: '5432'
  - name: PGSSLMODE
    value: 'require'
  - name: ENDPOINT_NAME
    value: 'projects/ankurhlsproject/branches/production/endpoints/primary'
```

> **Do NOT** add `--server.port 8501` or `--server.address` to the command. The runtime sets `DATABRICKS_APP_PORT` / `STREAMLIT_SERVER_PORT` automatically. A mismatched port causes "App Not Available" even though the API shows RUNNING.

The `PGUSER` is automatically set to `DATABRICKS_CLIENT_ID` by the runtime (the app's service principal UUID).

### Authentication Flow

```
Databricks App Runtime
    │
    │  WorkspaceClient() (auto-configured SP credentials)
    ▼
POST {DATABRICKS_HOST}/api/2.0/postgres/credentials
    │  Body: { "endpoint": "projects/ankurhlsproject/branches/production/endpoints/primary" }
    │  Headers: WorkspaceClient.config.authenticate()
    ▼
OAuth Token (short-lived, ~60 min)
    │
    │  psycopg3 ConnectionPool with OAuthConnection
    │  (generates a fresh token for each new connection)
    ▼
Lakebase Postgres (sslmode=require)
```

### Filter Components (`utils/filters.py`)

The app uses a shared filter module for consistent sidebar controls across all pages:

| Filter | Function | Used On |
|--------|----------|---------|
| Facility Multiselect | `facility_filter()` | Pages 1, 2, 7 |
| Department Radio | `department_filter()` | Page 1 |
| Date Range Picker | `date_range_filter()` | Pages 2, 6, 7 |
| Weekday/Weekend Toggle | `weekend_toggle()` | Pages 1, 2, 6 |
| Apply helpers | `apply_facility()`, `apply_date_range()`, `apply_weekend()` | All filtered pages |

Pages 3, 4, 5 use page-specific filters (selectbox, multiselect, slider, text input) directly in the sidebar.

### Deploying the App

```bash
# Deploy the bundle (uploads source code to workspace)
databricks bundle deploy -t dev

# Deploy the app from the bundle path
databricks apps deploy hl7app \
  --source-code-path "/Workspace/Users/<user>/.bundle/hl7_streaming_pipeline/dev/files/hl7-forecasting-app"

# Check status
databricks apps get hl7app

# View logs
databricks apps logs hl7app

# Stop / start the app
databricks apps stop hl7app
databricks apps start hl7app
```

---

## Troubleshooting

### "App Not Available" in browser

**Cause:** `app.yaml` passes `--server.port 8501` or `--server.address`, which conflicts with the port the Databricks proxy expects.

**Fix:** Use only `command: ['streamlit', 'run', 'app.py']` in `app.yaml`. The runtime automatically sets the correct port via environment variables.

### `password authentication failed for user '<SP-UUID>'`

**Cause:** The Postgres role for the service principal was created with `auth_method: NO_LOGIN` instead of `LAKEBASE_OAUTH_V1`. This can happen when using `databricks_create_role()` in SQL.

**Diagnosis:** Check the role via CLI:

```bash
databricks postgres list-roles projects/ankurhlsproject/branches/production
```

If you see `NO_LOGIN` / `IDENTITY_TYPE_UNSPECIFIED` for the SP UUID, the role is broken.

**Fix:**

```bash
# Delete the broken role
databricks postgres delete-role \
  projects/ankurhlsproject/branches/production/roles/<role-id>

# Recreate with correct identity type
databricks postgres create-role \
  projects/ankurhlsproject/branches/production \
  --json '{
    "spec": {
      "postgres_role": "<DATABRICKS_CLIENT_ID>",
      "identity_type": "SERVICE_PRINCIPAL"
    }
  }'

# Re-apply grants (see step 4 in Lakebase Setup above)

# Restart the app for a fresh connection pool
databricks apps stop hl7app && databricks apps start hl7app
```

### `function databricks_create_role does not exist`

**Cause:** The `databricks_auth` extension was not enabled.

**Fix:**

```sql
CREATE EXTENSION IF NOT EXISTS databricks_auth;
```

### `error connecting in 'pool-N'` on every request

**Cause:** The connection pool was created when the role was broken. Streamlit caches the pool via `@st.cache_resource`.

**Fix:** Restart the app to clear the cached pool:

```bash
databricks apps stop hl7app && databricks apps start hl7app
```

### 300 Apps limit reached

Databricks workspaces have a 300 app limit. Delete unused apps:

```bash
databricks apps list | grep name
databricks apps delete <app-name>
```

---

## ED & ICU Reporting

Real-time operational reports derived from ADT messages:

| Table | Grain | What it answers |
|-------|-------|-----------------|
| `gold_ed_hourly_census` | hour x facility | How many ED arrivals/discharges this hour? |
| `gold_icu_hourly_census` | hour x facility | How many ICU arrivals/discharges this hour? |
| `gold_ed_daily_summary` | day x facility | Total ED volume, avg LOS, peak hour? |
| `gold_icu_daily_summary` | day x facility | Total ICU volume, avg LOS, beds used? |
| `gold_department_census_current` | facility x dept | What is the current census snapshot? |

### Department Classification

- **ED**: `patient_class = 'E'` or location contains `ED`, `ER`, `EMER`
- **ICU**: Location contains `ICU` or `hospital_service IN (ICU, CCU, MICU, SICU, NICU, PICU)`

### ADT Signal Mapping

| Trigger Event | Meaning | Census Effect |
|---------------|---------|---------------|
| A01 | Admit | +1 arrival |
| A03 | Discharge | +1 discharge |
| A04 | Register (ED walk-in) | +1 ED arrival |
| A02 | Transfer | +1 at destination, +1 at origin |

---

## Predictive Forecasting

### Feature Tables

| Table | Description |
|-------|-------------|
| `gold_ed_forecast_features` | Hourly feature vector for ED models |
| `gold_icu_forecast_features` | Hourly feature vector for ICU models |
| `gold_combined_forecast_features` | Cross-department features for multi-metric models |
| `gold_forecast_predictions` | Scored predictions from model inference |
| `gold_forecast_accuracy` | Prediction accuracy tracking (MAE, MAPE, coverage) |

### Feature Categories

| Category | Features | Rationale |
|----------|----------|-----------|
| Calendar | hour_of_day, day_of_week, month, is_weekend, is_night_shift, is_holiday_window | Captures cyclical and seasonal patterns |
| Lag | arrivals_lag_1h/2h/4h/6h/12h/24h/168h | Autoregressive signal from recent history |
| Rolling | arrivals_rolling_6h/12h/24h/7d, avg, stddev | Smoothed trend and volatility |
| Cross-dept | ed_to_icu_ratio, net_system_pressure | ED boarding affects ICU; ICU capacity affects ED holds |
| Trend | arrivals_wow_ratio, cumulative_net_census | Week-over-week growth, occupancy drift |

### Model Training Workflow

```
┌─────────────────────────┐     ┌──────────────────────┐     ┌──────────────────────────┐
│  Feature Tables (DLT)   │────▶│  AutoML Training     │────▶│  MLflow Model Registry   │
│  gold_*_forecast_features│     │  (07_automl_training) │     │  Versioned + Staged      │
└─────────────────────────┘     └──────────────────────┘     └──────────────────────────┘
                                                                        │
┌─────────────────────────┐     ┌──────────────────────┐                │
│  gold_forecast_predictions│◀───│  Inference Job        │◀───────────────┘
│  (Scored Predictions)    │     │  (08_model_inference)  │
└─────────────────────────┘     └──────────────────────┘
```

### Suggested Models

| Model | Best For | Library |
|-------|----------|---------|
| Prophet | Seasonal patterns, holidays, quick prototyping | `prophet` |
| XGBoost / LightGBM | Tabular features, cross-department signals | `xgboost`, `lightgbm` |
| ARIMA / SARIMAX | Pure time-series, strong autoregressive signal | `statsmodels` |
| DeepAR / N-BEATS | Multi-horizon, multiple related series | `gluonts`, `pytorch-forecasting` |

---

## Supported HL7 Message Types

- **ADT** (Admit/Discharge/Transfer): A01, A02, A03, A04, A08, etc.
- **ORU** (Observation Results): R01
- **ORM** (Orders): O01
- **SIU** (Scheduling): S12, S14, S15

---

## Project Structure

```
HL7 Streaming/
├── README.md                            # This file
├── databricks.yml                       # DAB bundle configuration
├── requirements.txt                     # Python dev dependencies
├── notebooks/
│   ├── hl7_dlt_pipeline.py              # Pipeline overview notebook
│   ├── 01_bronze.py                     # Bronze: ingestion + funke parsing
│   ├── 02_silver.py                     # Silver: segment extraction
│   ├── 03_gold.py                       # Gold: dimensions, facts, metrics
│   ├── 04_reports.py                    # Reports: ED/ICU census & summaries
│   ├── 05_forecasting.py               # Forecasting: features + predictions
│   ├── 07_automl_training.py            # AutoML model training
│   ├── 08_model_inference.py            # Batch model inference
│   ├── 09_lakebase_sync.py              # CDF enablement for Lakebase
│   ├── 10_lakebase_load.py              # Gold → Lakebase Postgres loader
│   └── 11_lakebase_grants.py            # Postgres grants for app SP
├── hl7-forecasting-app/                 # Databricks App (Streamlit)
│   ├── app.py                           # Landing page with Lakebase status card
│   ├── app.yaml                         # App config (env vars, startup command)
│   ├── requirements.txt                 # App Python dependencies
│   ├── utils/
│   │   ├── db.py                        # Lakebase connection pool + OAuth rotation
│   │   ├── queries.py                   # SQL queries for all dashboard pages
│   │   └── filters.py                   # Reusable sidebar filter components
│   └── pages/                           # Streamlit multipage dashboards
│       ├── 1_realtime.py                # Real-time ops (dept/facility/time/weekend)
│       ├── 2_trends.py                  # Trends (date range/facility/weekend)
│       ├── 3_forecasting.py             # ML forecasting (dept/metric/horizon)
│       ├── 4_model_perf.py              # Model performance (model selection)
│       ├── 5_patient_clinical.py        # Clinical (coding/severity/priority/provider)
│       ├── 6_combined_forecast.py       # Combined forecast (date range/weekend)
│       └── 7_operations.py              # Operations (date/msg type/facility/class)
├── dashboards/
│   ├── hl7_ed_icu_operations.lvdash.json       # Lakeview: ED/ICU Ops
│   └── hl7_patient_clinical_analytics.lvdash.json  # Lakeview: Patient Clinical
├── libraries/
│   └── funke-*.whl                      # funke wheel (deployed via DAB)
├── resources/
│   └── hl7_pipeline.yml                 # DAB resource definitions (jobs, pipelines, apps, dashboards)
├── src/
│   ├── hl7_parser.py                    # HL7 parsing utilities
│   ├── transformations.py               # Spark transformations
│   ├── schemas.py                       # Schema definitions
│   └── quality_rules.py                 # DLT expectations / quality rules
├── config/
│   ├── pipeline_config.json             # Pipeline parameters
│   └── hl7_mappings.json                # HL7 code mappings
├── tests/
│   ├── test_hl7_parser.py
│   ├── test_hl7_splitter.py
│   └── sample_messages/
└── deploy/
    └── dlt_pipeline_settings.json
```

---

## Deployment

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Databricks CLI v0.100+ with Asset Bundles support
- Databricks Runtime 13.0+ with Delta Live Tables
- Lakebase Autoscaling project created

### Deploy with Databricks Asset Bundles

```bash
# Validate the bundle
databricks bundle validate

# Deploy to dev
databricks bundle deploy -t dev

# Run the DLT pipeline
databricks bundle run hl7_streaming_dlt -t dev

# Load gold tables into Lakebase
databricks bundle run hl7_lakebase_load -t dev
```

### Set Up Lakebase Auth for the App

```bash
# Get the app's service principal client ID
SP_UUID=$(databricks apps get hl7app | jq -r .service_principal_client_id)
echo "Service Principal: $SP_UUID"

# Create OAuth Postgres role via CLI
databricks postgres create-role \
  projects/ankurhlsproject/branches/production \
  --json "{\"spec\": {\"postgres_role\": \"$SP_UUID\", \"identity_type\": \"SERVICE_PRINCIPAL\"}}"

# Verify it shows LAKEBASE_OAUTH_V1
databricks postgres list-roles projects/ankurhlsproject/branches/production

# Grant permissions (run as schema owner -- via notebook 11 or psql)
```

### Deploy and Run the App

```bash
# Deploy app
databricks apps deploy hl7app \
  --source-code-path "/Workspace/Users/<user>/.bundle/hl7_streaming_pipeline/dev/files/hl7-forecasting-app"

# Check it is running
databricks apps get hl7app

# View logs for errors
databricks apps logs hl7app
```

### Train and Score ML Models

```bash
# Train ML models
databricks bundle run hl7_automl_training -t dev

# Run model inference
databricks bundle run hl7_model_inference -t dev
```

### Configuration Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `catalog` | Unity Catalog name | `users` |
| `schema` | Target schema name | `ankur_nayyar` |
| `source_volume` | Landing volume for raw HL7 files | `landing` |
| `funke_wheel_name` | Filename of the funke wheel | `funke-0.1.0a1-py3-none-any.whl` |

### Deployed Resources (via DAB)

| Resource | Type | Description |
|----------|------|-------------|
| `hl7_streaming_dlt` | DLT Pipeline | 5-notebook Medallion pipeline (Photon, autoscale 1-2 workers) |
| `hl7_lakebase_load` | Job | Loads gold tables into Lakebase Postgres |
| `hl7_automl_training` | Job | Trains forecast models via AutoML |
| `hl7_model_inference` | Job | Hourly batch inference (PAUSED by default) |
| `hl7_lakebase_sync` | Job | CDF enablement for Lakebase sync |
| `hl7app` | App | Streamlit dashboard (Databricks App, `sql` scope) |
| `hl7_ed_icu_dashboard` | Dashboard | Lakeview dashboard for ED/ICU operations |
| `hl7_patient_clinical_dashboard` | Dashboard | Lakeview dashboard for patient clinical analytics |

---

## Complete Table Inventory

| # | Table | Notebook | Layer |
|---|-------|----------|-------|
| 1 | `bronze_hl7_raw` | 01_bronze | Bronze |
| 2 | `bronze_hl7_split` | 01_bronze | Bronze |
| 3 | `bronze_hl7_parsed` | 01_bronze | Bronze |
| 4 | `silver_hl7_parsed` | 02_silver | Silver |
| 5 | `silver_msh` | 02_silver | Silver |
| 6 | `silver_pid` | 02_silver | Silver |
| 7 | `silver_pv1` | 02_silver | Silver |
| 8 | `silver_evn` | 02_silver | Silver |
| 9 | `silver_obr` | 02_silver | Silver |
| 10 | `silver_obx` | 02_silver | Silver |
| 11 | `silver_dg1` | 02_silver | Silver |
| 12 | `silver_al1` | 02_silver | Silver |
| 13 | `silver_in1` | 02_silver | Silver |
| 14 | `gold_patient_dim` | 03_gold | Gold |
| 15 | `gold_encounter_fact` | 03_gold | Gold |
| 16 | `gold_observation_fact` | 03_gold | Gold |
| 17 | `gold_diagnosis_fact` | 03_gold | Gold |
| 18 | `gold_allergy_fact` | 03_gold | Gold |
| 19 | `gold_order_fact` | 03_gold | Gold |
| 20 | `gold_message_metrics` | 03_gold | Gold |
| 21 | `gold_patient_activity` | 03_gold | Gold |
| 22 | `gold_ed_hourly_census` | 04_reports | Reports |
| 23 | `gold_icu_hourly_census` | 04_reports | Reports |
| 24 | `gold_ed_daily_summary` | 04_reports | Reports |
| 25 | `gold_icu_daily_summary` | 04_reports | Reports |
| 26 | `gold_department_census_current` | 04_reports | Reports |
| 27 | `gold_ed_forecast_features` | 05_forecasting | Forecasting |
| 28 | `gold_icu_forecast_features` | 05_forecasting | Forecasting |
| 29 | `gold_combined_forecast_features` | 05_forecasting | Forecasting |
| 30 | `gold_forecast_predictions` | 05_forecasting | Forecasting |
| 31 | `gold_forecast_accuracy` | 05_forecasting | Forecasting |

---

## funke Field Access Pattern

```
hl7.SEGMENT[segment_rep].fields[field_num][field_rep][component][subcomponent]
```

| HL7 Field | funke Access Pattern | Description |
|-----------|---------------------|-------------|
| MSH-9-1 | `hl7.MSH[0].fields[9][0][1][1]` | Message Type |
| MSH-9-2 | `hl7.MSH[0].fields[9][0][2][1]` | Trigger Event |
| PID-3-1 | `hl7.PID[0].fields[3][0][1][1]` | Patient ID |
| PV1-2-1 | `hl7.PV1[0].fields[2][0][1][1]` | Patient Class |
| PV1-3-1 | `hl7.PV1[0].fields[3][0][1][1]` | Location (Point of Care) |

---

## License

Internal use only -- Healthcare Data Platform
