-- Companion to adt_events_dashboard.json — same logic as each dataset (bronze.ensemble.ens_adt).
-- Run in a Databricks SQL / notebook cell to validate before importing the dashboard.

-- 1) ds_total_events
SELECT COUNT(*) AS total_events
FROM bronze.ensemble.ens_adt
WHERE message_type = 'ADT'
  AND event_time_stamp IS NOT NULL
  AND event_time_stamp >= date_sub(CURRENT_DATE(), 365);

-- 2) ds_unique_patients
SELECT COUNT(DISTINCT patient_mrn) AS unique_patients
FROM bronze.ensemble.ens_adt
WHERE message_type = 'ADT'
  AND event_time_stamp IS NOT NULL
  AND patient_mrn IS NOT NULL
  AND event_time_stamp >= date_sub(CURRENT_DATE(), 365);

-- 3) ds_admissions_today
SELECT COUNT(*) AS admissions_today
FROM bronze.ensemble.ens_adt
WHERE message_type = 'ADT'
  AND message_event_type = 'A01'
  AND event_time_stamp IS NOT NULL
  AND to_date(event_time_stamp) = CURRENT_DATE();

-- 4) ds_event_volume
SELECT DATE_TRUNC('DAY', event_time_stamp) AS event_date, COUNT(*) AS event_count
FROM bronze.ensemble.ens_adt
WHERE message_type = 'ADT'
  AND event_time_stamp IS NOT NULL
  AND event_time_stamp >= date_sub(CURRENT_DATE(), 365)
GROUP BY DATE_TRUNC('DAY', event_time_stamp)
ORDER BY 1;

-- 5) ds_events_by_type
SELECT message_event_type, COUNT(*) AS event_count
FROM bronze.ensemble.ens_adt
WHERE message_type = 'ADT'
  AND event_time_stamp IS NOT NULL
  AND event_time_stamp >= date_sub(CURRENT_DATE(), 365)
  AND message_event_type IS NOT NULL
GROUP BY message_event_type
ORDER BY event_count DESC;

-- 6) ds_patient_class
SELECT patient_class, COUNT(*) AS event_count
FROM bronze.ensemble.ens_adt
WHERE message_type = 'ADT'
  AND event_time_stamp IS NOT NULL
  AND event_time_stamp >= date_sub(CURRENT_DATE(), 365)
  AND patient_class IS NOT NULL
GROUP BY patient_class
ORDER BY event_count DESC;

-- 7) ds_top_departments
SELECT department, COUNT(*) AS event_count
FROM bronze.ensemble.ens_adt
WHERE message_type = 'ADT'
  AND event_time_stamp IS NOT NULL
  AND event_time_stamp >= date_sub(CURRENT_DATE(), 365)
  AND TRIM(department) <> ''
  AND department IS NOT NULL
GROUP BY department
ORDER BY event_count DESC
LIMIT 10;

-- 8) ds_facility_volume
SELECT facility, COUNT(*) AS event_count
FROM bronze.ensemble.ens_adt
WHERE message_type = 'ADT'
  AND event_time_stamp IS NOT NULL
  AND event_time_stamp >= date_sub(CURRENT_DATE(), 365)
  AND TRIM(facility) <> ''
  AND facility IS NOT NULL
GROUP BY facility
ORDER BY event_count DESC;

-- 9) ds_sending_facility
SELECT sending_facility, COUNT(*) AS event_count
FROM bronze.ensemble.ens_adt
WHERE message_type = 'ADT'
  AND event_time_stamp IS NOT NULL
  AND event_time_stamp >= date_sub(CURRENT_DATE(), 365)
  AND TRIM(sending_facility) <> ''
  AND sending_facility IS NOT NULL
GROUP BY sending_facility
ORDER BY event_count DESC;

-- 10) ds_admission_type
SELECT admission_type, COUNT(*) AS event_count
FROM bronze.ensemble.ens_adt
WHERE message_type = 'ADT'
  AND event_time_stamp IS NOT NULL
  AND event_time_stamp >= date_sub(CURRENT_DATE(), 365)
  AND admission_type IS NOT NULL
GROUP BY admission_type
ORDER BY event_count DESC;

-- 11) ds_financial_class
SELECT financial_class, COUNT(*) AS event_count
FROM bronze.ensemble.ens_adt
WHERE message_type = 'ADT'
  AND event_time_stamp IS NOT NULL
  AND event_time_stamp >= date_sub(CURRENT_DATE(), 365)
  AND financial_class IS NOT NULL
GROUP BY financial_class
ORDER BY event_count DESC;

-- 12) ds_discharge_disposition
SELECT discharge_diposition, COUNT(*) AS event_count
FROM bronze.ensemble.ens_adt
WHERE message_type = 'ADT'
  AND event_time_stamp IS NOT NULL
  AND event_time_stamp >= date_sub(CURRENT_DATE(), 365)
  AND discharge_diposition IS NOT NULL
GROUP BY discharge_diposition
ORDER BY event_count DESC;
