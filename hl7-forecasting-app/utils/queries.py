"""
SQL queries for the HL7App.

All queries target Lakebase Postgres tables in the "ankur_nayyar" schema.
"""

SCHEMA = "ankur_nayyar"


def _fqn(table: str) -> str:
    return f'"{SCHEMA}"."{table}"'


# ---------------------------------------------------------------------------
# Real-Time Operations
# ---------------------------------------------------------------------------

CURRENT_CENSUS = f"""
SELECT
    location_facility,
    department,
    total_arrivals,
    total_discharges,
    estimated_census,
    snapshot_at
FROM {_fqn('gold_department_census_current')}
ORDER BY department, location_facility
"""

ED_HOURLY_LAST_24H = f"""
SELECT
    event_hour,
    location_facility,
    arrivals,
    discharges,
    net_change,
    hour_of_day,
    day_of_week,
    is_weekend
FROM {_fqn('gold_ed_hourly_census')}
WHERE event_hour >= NOW() - INTERVAL '24 hours'
ORDER BY event_hour
"""

ICU_HOURLY_LAST_24H = f"""
SELECT
    event_hour,
    location_facility,
    arrivals,
    discharges,
    net_change,
    hour_of_day,
    day_of_week,
    is_weekend
FROM {_fqn('gold_icu_hourly_census')}
WHERE event_hour >= NOW() - INTERVAL '24 hours'
ORDER BY event_hour
"""

ED_HOURLY_ALL = f"""
SELECT
    event_hour,
    location_facility,
    arrivals,
    discharges,
    net_change,
    hour_of_day,
    day_of_week,
    is_weekend
FROM {_fqn('gold_ed_hourly_census')}
ORDER BY event_hour
"""

ICU_HOURLY_ALL = f"""
SELECT
    event_hour,
    location_facility,
    arrivals,
    discharges,
    net_change,
    hour_of_day,
    day_of_week,
    is_weekend
FROM {_fqn('gold_icu_hourly_census')}
ORDER BY event_hour
"""

# ---------------------------------------------------------------------------
# Trends & Analytics
# ---------------------------------------------------------------------------

ED_DAILY_SUMMARY = f"""
SELECT
    activity_date,
    location_facility,
    total_encounters,
    total_arrivals,
    total_discharges,
    unique_patients,
    avg_los_minutes::numeric,
    median_los_minutes::numeric,
    max_los_minutes::numeric,
    peak_arrival_hour,
    unique_providers
FROM {_fqn('gold_ed_daily_summary')}
ORDER BY activity_date DESC
"""

ICU_DAILY_SUMMARY = f"""
SELECT
    activity_date,
    location_facility,
    total_encounters,
    total_arrivals,
    total_discharges,
    unique_patients,
    avg_los_hours::numeric,
    median_los_hours::numeric,
    max_los_hours::numeric,
    peak_arrival_hour,
    unique_providers,
    beds_used
FROM {_fqn('gold_icu_daily_summary')}
ORDER BY activity_date DESC
"""

ENCOUNTER_VOLUME_TREND = f"""
SELECT
    created_at::date AS encounter_date,
    patient_class_desc,
    COUNT(*) AS encounter_count
FROM {_fqn('gold_encounter_fact')}
GROUP BY created_at::date, patient_class_desc
ORDER BY encounter_date
"""

# ---------------------------------------------------------------------------
# ML Forecasting
# ---------------------------------------------------------------------------

LATEST_PREDICTIONS = f"""
SELECT
    model_name,
    model_version,
    department,
    target_metric,
    forecast_horizon_hours,
    target_hour,
    ROUND(predicted_value::numeric, 1) AS predicted_value,
    ROUND(prediction_lower_bound::numeric, 1) AS lower_bound,
    ROUND(prediction_upper_bound::numeric, 1) AS upper_bound,
    ROUND(confidence_level::numeric * 100, 0) AS confidence_pct,
    ROUND(actual_value::numeric, 1) AS actual_value,
    ROUND(absolute_error::numeric, 2) AS error,
    forecast_generated_at
FROM {_fqn('gold_forecast_predictions')}
ORDER BY forecast_generated_at DESC, department, target_metric, forecast_horizon_hours
"""

PREDICTIONS_LAST_7D = f"""
SELECT
    model_name,
    department,
    target_metric,
    forecast_horizon_hours,
    target_hour,
    predicted_value,
    prediction_lower_bound,
    prediction_upper_bound,
    actual_value,
    absolute_error,
    forecast_generated_at
FROM {_fqn('gold_forecast_predictions')}
WHERE forecast_generated_at >= NOW() - INTERVAL '7 days'
ORDER BY target_hour
"""

PREDICTIONS_VS_ACTUALS = f"""
SELECT
    model_name,
    department,
    target_metric,
    forecast_horizon_hours,
    target_hour,
    predicted_value,
    actual_value,
    absolute_error
FROM {_fqn('gold_forecast_predictions')}
WHERE actual_value IS NOT NULL
ORDER BY target_hour DESC
LIMIT 500
"""

# ---------------------------------------------------------------------------
# Model Performance
# ---------------------------------------------------------------------------

FORECAST_ACCURACY = f"""
SELECT
    prediction_date,
    model_name,
    model_version,
    department,
    target_metric,
    forecast_horizon_hours,
    prediction_count,
    mae,
    mse,
    mape,
    coverage_pct
FROM {_fqn('gold_forecast_accuracy')}
ORDER BY prediction_date DESC
"""

ACCURACY_SUMMARY = f"""
SELECT
    model_name,
    department,
    target_metric,
    forecast_horizon_hours,
    COUNT(*) AS total_periods,
    ROUND(AVG(mae)::numeric, 2) AS avg_mae,
    ROUND(AVG(mape)::numeric, 1) AS avg_mape_pct,
    ROUND(AVG(coverage_pct)::numeric, 1) AS avg_coverage_pct,
    SUM(prediction_count) AS total_predictions
FROM {_fqn('gold_forecast_accuracy')}
GROUP BY model_name, department, target_metric, forecast_horizon_hours
ORDER BY model_name, forecast_horizon_hours
"""

# ---------------------------------------------------------------------------
# Patient & Clinical
# ---------------------------------------------------------------------------

PATIENT_DEMOGRAPHICS = f"""
SELECT
    sex,
    COUNT(*) AS patient_count
FROM {_fqn('gold_patient_dim')}
GROUP BY sex
ORDER BY patient_count DESC
"""

PATIENT_COUNTS = f"""
SELECT
    COUNT(*) AS total_patients,
    COUNT(DISTINCT address_state) AS states_represented,
    COUNT(DISTINCT source_system) AS source_systems
FROM {_fqn('gold_patient_dim')}
"""

TOP_DIAGNOSES = f"""
SELECT
    diagnosis_code,
    diagnosis_description,
    diagnosis_coding_system,
    COUNT(*) AS diagnosis_count,
    COUNT(DISTINCT patient_id) AS unique_patients
FROM {_fqn('gold_diagnosis_fact')}
WHERE diagnosis_code IS NOT NULL AND diagnosis_code != ''
GROUP BY diagnosis_code, diagnosis_description, diagnosis_coding_system
ORDER BY diagnosis_count DESC
LIMIT 20
"""

OBSERVATION_SUMMARY = f"""
SELECT
    observation_id,
    observation_text,
    value_type,
    COUNT(*) AS obs_count,
    SUM(CASE WHEN is_abnormal THEN 1 ELSE 0 END) AS abnormal_count,
    ROUND(AVG(observation_value_numeric)::numeric, 2) AS avg_value,
    ROUND(MIN(observation_value_numeric)::numeric, 2) AS min_value,
    ROUND(MAX(observation_value_numeric)::numeric, 2) AS max_value
FROM {_fqn('gold_observation_fact')}
WHERE observation_id IS NOT NULL
GROUP BY observation_id, observation_text, value_type
ORDER BY obs_count DESC
LIMIT 20
"""

ABNORMAL_FLAGS_DIST = f"""
SELECT
    abnormal_flags,
    COUNT(*) AS flag_count
FROM {_fqn('gold_observation_fact')}
WHERE abnormal_flags IS NOT NULL AND abnormal_flags != ''
GROUP BY abnormal_flags
ORDER BY flag_count DESC
"""

ALLERGY_OVERVIEW = f"""
SELECT
    allergen_description,
    allergen_type_text,
    severity_text,
    is_severe,
    COUNT(*) AS allergy_count,
    COUNT(DISTINCT patient_id) AS unique_patients
FROM {_fqn('gold_allergy_fact')}
WHERE allergen_description IS NOT NULL
GROUP BY allergen_description, allergen_type_text, severity_text, is_severe
ORDER BY allergy_count DESC
LIMIT 20
"""

ALLERGY_SEVERITY_DIST = f"""
SELECT
    COALESCE(severity_text, 'Unknown') AS severity,
    is_severe,
    COUNT(*) AS allergy_count
FROM {_fqn('gold_allergy_fact')}
GROUP BY severity_text, is_severe
ORDER BY allergy_count DESC
"""

ORDER_ACTIVITY = f"""
SELECT
    universal_service_text,
    priority,
    result_status,
    COUNT(*) AS order_count,
    COUNT(DISTINCT patient_id) AS unique_patients
FROM {_fqn('gold_order_fact')}
WHERE universal_service_text IS NOT NULL
GROUP BY universal_service_text, priority, result_status
ORDER BY order_count DESC
LIMIT 20
"""

ORDER_PROVIDER_VOLUME = f"""
SELECT
    ordering_provider_name,
    ordering_provider_id,
    COUNT(*) AS order_count
FROM {_fqn('gold_order_fact')}
WHERE ordering_provider_name IS NOT NULL AND ordering_provider_name != ''
GROUP BY ordering_provider_name, ordering_provider_id
ORDER BY order_count DESC
LIMIT 15
"""

# ---------------------------------------------------------------------------
# Combined Forecasting
# ---------------------------------------------------------------------------

COMBINED_FEATURES = f"""
SELECT
    event_hour,
    ed_arrivals,
    ed_discharges,
    ed_net_change,
    icu_arrivals,
    icu_discharges,
    icu_net_change,
    total_arrivals,
    total_discharges,
    ed_to_icu_ratio,
    net_system_pressure,
    hour_of_day,
    day_of_week,
    is_weekend,
    ed_arrivals_rolling_24h,
    icu_arrivals_rolling_24h,
    ed_discharges_rolling_24h,
    icu_discharges_rolling_24h
FROM {_fqn('gold_combined_forecast_features')}
ORDER BY event_hour
"""

ED_FEATURES_SUMMARY = f"""
SELECT
    event_hour,
    location_facility,
    arrivals,
    discharges,
    net_change,
    arrivals_lag_1h,
    arrivals_lag_24h,
    arrivals_rolling_6h,
    arrivals_rolling_24h,
    arrivals_avg_24h,
    arrivals_std_24h,
    discharges_rolling_24h,
    cumulative_net_census,
    arrivals_wow_ratio,
    is_night_shift,
    is_holiday_window
FROM {_fqn('gold_ed_forecast_features')}
ORDER BY event_hour
"""

ICU_FEATURES_SUMMARY = f"""
SELECT
    event_hour,
    location_facility,
    arrivals,
    discharges,
    net_change,
    arrivals_lag_1h,
    arrivals_lag_24h,
    arrivals_rolling_6h,
    arrivals_rolling_24h,
    arrivals_avg_24h,
    arrivals_std_24h,
    discharges_rolling_24h,
    cumulative_net_census,
    arrivals_wow_ratio,
    is_night_shift,
    is_holiday_window
FROM {_fqn('gold_icu_forecast_features')}
ORDER BY event_hour
"""

# ---------------------------------------------------------------------------
# Pipeline & Operations
# ---------------------------------------------------------------------------

MESSAGE_METRICS = f"""
SELECT
    processing_hour,
    message_type,
    sending_facility,
    message_count,
    unique_messages,
    first_message_at,
    last_message_at
FROM {_fqn('gold_message_metrics')}
ORDER BY processing_hour DESC
"""

MESSAGE_THROUGHPUT = f"""
SELECT
    processing_hour,
    SUM(message_count) AS total_messages,
    SUM(unique_messages) AS total_unique,
    COUNT(DISTINCT message_type) AS message_types,
    COUNT(DISTINCT sending_facility) AS facilities
FROM {_fqn('gold_message_metrics')}
GROUP BY processing_hour
ORDER BY processing_hour
"""

PATIENT_ACTIVITY = f"""
SELECT
    activity_date,
    location_facility,
    patient_class_desc,
    unique_patients,
    encounter_count,
    unique_providers
FROM {_fqn('gold_patient_activity')}
ORDER BY activity_date DESC
"""

DATA_FRESHNESS = f"""
SELECT
    MAX(last_message_at) AS latest_message,
    MAX(processing_hour) AS latest_processing_hour,
    COUNT(*) AS total_metric_rows
FROM {_fqn('gold_message_metrics')}
"""

# ---------------------------------------------------------------------------
# System status page (Lakebase snapshot; one row per table)
# stale_after_hours: warn when last_activity older than this (None = no age rule)
# ---------------------------------------------------------------------------

STATUS_MONITOR_SPECS = [
    {
        "layer": "Stream",
        "area": "HL7 throughput",
        "table": "gold_message_metrics",
        "stale_after_hours": 36,
        "sql": f"""
            SELECT COUNT(*)::bigint AS row_count, MAX(last_message_at) AS last_activity
            FROM {_fqn("gold_message_metrics")}
        """,
    },
    {
        "layer": "Gold",
        "area": "Encounters",
        "table": "gold_encounter_fact",
        "stale_after_hours": 72,
        "sql": f"""
            SELECT COUNT(*)::bigint AS row_count, MAX(created_at) AS last_activity
            FROM {_fqn("gold_encounter_fact")}
        """,
    },
    {
        "layer": "Gold",
        "area": "Patients",
        "table": "gold_patient_dim",
        "stale_after_hours": 168,
        "sql": f"""
            SELECT COUNT(*)::bigint AS row_count, MAX(last_updated_at) AS last_activity
            FROM {_fqn("gold_patient_dim")}
        """,
    },
    {
        "layer": "Gold",
        "area": "ED census (hourly)",
        "table": "gold_ed_hourly_census",
        "stale_after_hours": 48,
        "sql": f"""
            SELECT COUNT(*)::bigint AS row_count, MAX(event_hour) AS last_activity
            FROM {_fqn("gold_ed_hourly_census")}
        """,
    },
    {
        "layer": "Gold",
        "area": "ICU census (hourly)",
        "table": "gold_icu_hourly_census",
        "stale_after_hours": 48,
        "sql": f"""
            SELECT COUNT(*)::bigint AS row_count, MAX(event_hour) AS last_activity
            FROM {_fqn("gold_icu_hourly_census")}
        """,
    },
    {
        "layer": "Gold",
        "area": "Department snapshot",
        "table": "gold_department_census_current",
        "stale_after_hours": 48,
        "sql": f"""
            SELECT COUNT(*)::bigint AS row_count, MAX(snapshot_at) AS last_activity
            FROM {_fqn("gold_department_census_current")}
        """,
    },
    {
        "layer": "ML",
        "area": "Forecast features",
        "table": "gold_ed_forecast_features",
        "stale_after_hours": 72,
        "sql": f"""
            SELECT COUNT(*)::bigint AS row_count, MAX(event_hour) AS last_activity
            FROM {_fqn("gold_ed_forecast_features")}
        """,
    },
    {
        "layer": "ML",
        "area": "Forecast features",
        "table": "gold_icu_forecast_features",
        "stale_after_hours": 72,
        "sql": f"""
            SELECT COUNT(*)::bigint AS row_count, MAX(event_hour) AS last_activity
            FROM {_fqn("gold_icu_forecast_features")}
        """,
    },
    {
        "layer": "ML",
        "area": "Combined features",
        "table": "gold_combined_forecast_features",
        "stale_after_hours": 72,
        "sql": f"""
            SELECT COUNT(*)::bigint AS row_count, MAX(event_hour) AS last_activity
            FROM {_fqn("gold_combined_forecast_features")}
        """,
    },
    {
        "layer": "ML",
        "area": "Predictions",
        "table": "gold_forecast_predictions",
        "stale_after_hours": 48,
        "sql": f"""
            SELECT COUNT(*)::bigint AS row_count, MAX(forecast_generated_at) AS last_activity
            FROM {_fqn("gold_forecast_predictions")}
        """,
    },
    {
        "layer": "ML",
        "area": "Accuracy rollup",
        "table": "gold_forecast_accuracy",
        "stale_after_hours": None,
        "sql": f"""
            SELECT COUNT(*)::bigint AS row_count, MAX(prediction_date::timestamp) AS last_activity
            FROM {_fqn("gold_forecast_accuracy")}
        """,
    },
]

STATUS_FORECAST_ACTIVITY = f"""
SELECT
    MAX(forecast_generated_at) AS last_run,
    COUNT(*) FILTER (
        WHERE forecast_generated_at >= NOW() - INTERVAL '24 hours'
    )::bigint AS rows_24h,
    COUNT(DISTINCT model_name)::bigint AS distinct_models
FROM {_fqn("gold_forecast_predictions")}
"""

# ---------------------------------------------------------------------------
# Home dashboard & platform pulse (cross-cutting summaries)
# ---------------------------------------------------------------------------

HOME_ENCOUNTER_COUNT_7D = f"""
SELECT COUNT(*)::bigint AS n
FROM {_fqn("gold_encounter_fact")}
WHERE created_at >= NOW() - INTERVAL '7 days'
"""

HOME_ML_PREDICTION_OVERVIEW = f"""
SELECT
    COUNT(*)::bigint AS total_predictions,
    COUNT(*) FILTER (WHERE actual_value IS NOT NULL)::bigint AS scored_predictions,
    MAX(forecast_generated_at) AS latest_forecast_run
FROM {_fqn("gold_forecast_predictions")}
"""

HOME_MESSAGE_VOLUME_24H = f"""
SELECT COALESCE(SUM(message_count), 0)::bigint AS messages_24h
FROM {_fqn("gold_message_metrics")}
WHERE processing_hour >= NOW() - INTERVAL '24 hours'
"""

HOME_THROUGHPUT_RECENT = f"""
SELECT
    processing_hour,
    SUM(message_count) AS total_messages
FROM {_fqn("gold_message_metrics")}
WHERE processing_hour >= NOW() - INTERVAL '72 hours'
GROUP BY processing_hour
ORDER BY processing_hour
"""

HOME_ENCOUNTER_TREND_30D = f"""
SELECT
    created_at::date AS d,
    COUNT(*)::bigint AS encounter_count
FROM {_fqn("gold_encounter_fact")}
WHERE created_at >= NOW() - INTERVAL '30 days'
GROUP BY created_at::date
ORDER BY d
"""

MESSAGE_MIX_FOR_TREEMAP = f"""
SELECT
    COALESCE(message_type, 'Unknown') AS message_type,
    COALESCE(sending_facility, 'Unknown') AS sending_facility,
    SUM(message_count)::bigint AS msg_count
FROM {_fqn("gold_message_metrics")}
GROUP BY message_type, sending_facility
ORDER BY msg_count DESC
LIMIT 50
"""

PLATFORM_FEATURE_SAMPLE = f"""
SELECT
    event_hour,
    location_facility,
    arrivals,
    discharges,
    arrivals_lag_1h,
    arrivals_rolling_24h,
    cumulative_net_census,
    is_night_shift,
    is_holiday_window
FROM {_fqn("gold_ed_forecast_features")}
ORDER BY event_hour DESC
LIMIT 24
"""
