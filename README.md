# HL7 Streaming Pipeline with Databricks Delta Live Tables

A production-ready HL7 v2.x streaming pipeline built on Databricks using Delta Live Tables (DLT) for declarative data processing, with real-time ED/ICU reporting and predictive forecasting.

**Powered by [funke](https://github.com/databricks-field-eng/funke)** - Databricks Field Engineering library for HL7v2 message parsing.

## Architecture

This pipeline follows the **Medallion Architecture** with an extended analytics layer:

```
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────────┐    ┌────────────┐
│  Source   │───▶│  Bronze  │───▶│  Silver  │───▶│   Gold   │───▶│   Reports    │───▶│ Forecasting│
│ (HL7 Raw) │    │(Raw+Parse)│   │(Segments)│    │(Analytics)│   │ (ED/ICU Ops) │    │ (Predict)  │
└──────────┘    └──────────┘    └──────────┘    └──────────┘    └──────────────┘    └────────────┘
```

### Layers

| Notebook | Layer | Description |
|----------|-------|-------------|
| `01_bronze.py` | Bronze | Raw HL7 ingestion, multi-message splitting, funke parsing |
| `02_silver.py` | Silver | Segment extraction (MSH, PID, PV1, OBX, DG1, etc.) with quality rules |
| `03_gold.py` | Gold | Dimensions, fact tables, message metrics |
| `04_reports.py` | Reports | Real-time ED & ICU census, arrivals, discharges, daily summaries |
| `05_forecasting.py` | Forecasting | Feature engineering for predictive models, prediction outputs |

## ED & ICU Reporting

Real-time operational reports derived from ADT messages:

| Table | Grain | What it answers |
|-------|-------|-----------------|
| `gold_ed_hourly_census` | hour × facility | How many ED arrivals/discharges this hour? |
| `gold_icu_hourly_census` | hour × facility | How many ICU arrivals/discharges this hour? |
| `gold_ed_daily_summary` | day × facility | Total ED volume, avg LOS, peak hour? |
| `gold_icu_daily_summary` | day × facility | Total ICU volume, avg LOS, beds used? |
| `gold_department_census_current` | facility × dept | What is the current census snapshot? |

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

## Predictive Forecasting

Feature engineering tables for forecasting ED and ICU arrivals/discharges:

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
│  Feature Tables (DLT)   │────▶│  Model Training Job  │────▶│  MLflow Model Registry   │
│  gold_*_forecast_features│     │  (Prophet / XGBoost)  │     │  Versioned + Staged      │
└─────────────────────────┘     └──────────────────────┘     └──────────────────────────┘
                                                                        │
┌─────────────────────────┐     ┌──────────────────────┐                │
│  gold_forecast_predictions│◀───│  Inference Job        │◀───────────────┘
│  (Scored Predictions)    │     │  (Batch or Streaming)  │
└─────────────────────────┘     └──────────────────────┘
```

1. **Feature Tables** are populated by the DLT pipeline in real time
2. **Training Jobs** (scheduled or triggered) read features and train models
3. **Models** are registered in MLflow with staging/production labels
4. **Inference Jobs** score latest features and write to `gold_forecast_predictions`
5. **Dashboards** consume predictions and actuals for operational use

### Suggested Models

| Model | Best For | Library |
|-------|----------|---------|
| Prophet | Seasonal patterns, holidays, quick prototyping | `prophet` |
| XGBoost / LightGBM | Tabular features, cross-department signals | `xgboost`, `lightgbm` |
| ARIMA / SARIMAX | Pure time-series, strong autoregressive signal | `statsmodels` |
| DeepAR / N-BEATS | Multi-horizon, multiple related series | `gluonts`, `pytorch-forecasting` |

## Supported HL7 Message Types

- **ADT** (Admit/Discharge/Transfer): A01, A02, A03, A04, A08, etc.
- **ORU** (Observation Results): R01
- **ORM** (Orders): O01
- **SIU** (Scheduling): S12, S14, S15

## Project Structure

```
HL7 Streaming/
├── README.md
├── databricks.yml                    # DAB bundle configuration
├── requirements.txt
├── notebooks/
│   ├── hl7_dlt_pipeline.py          # Pipeline overview
│   ├── 01_bronze.py                 # Bronze: ingestion + funke parsing
│   ├── 02_silver.py                 # Silver: segment extraction
│   ├── 03_gold.py                   # Gold: dimensions + facts
│   ├── 04_reports.py                # Reports: ED/ICU census & summaries
│   └── 05_forecasting.py           # Forecasting: features + predictions
├── libraries/
│   └── funke-*.whl                  # funke wheel (deployed via DAB)
├── resources/
│   └── hl7_pipeline.yml             # DAB resource definitions
├── src/
│   ├── hl7_parser.py
│   ├── transformations.py
│   ├── schemas.py
│   └── quality_rules.py
├── config/
│   ├── pipeline_config.json
│   └── hl7_mappings.json
├── tests/
│   ├── test_hl7_parser.py
│   ├── test_hl7_splitter.py
│   └── sample_messages/
└── deploy/
    └── dlt_pipeline_settings.json
```

## Deployment

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Databricks CLI with Asset Bundles support
- Databricks Runtime 13.0+ with Delta Live Tables

### Deploy with Databricks Asset Bundles

```bash
# Validate the bundle
databricks bundle validate

# Deploy to dev
databricks bundle deploy -t dev

# Run the pipeline
databricks bundle run hl7_streaming_dlt -t dev
```

All configuration (catalog, schema, volume, wheel path) is driven by
variables in `databricks.yml` -- no hardcoded paths.

### Configuration Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `catalog` | Unity Catalog name | `ankur_nayyar_hl7` |
| `schema` | Target schema name | `hl7_streaming` |
| `source_volume` | Landing volume for raw HL7 files | `landing` |
| `funke_wheel_name` | Filename of the funke wheel | `funke-0.1.0a1-py3-none-any.whl` |

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

## License

Internal use only - Healthcare Data Platform
