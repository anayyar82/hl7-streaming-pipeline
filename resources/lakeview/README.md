# ADT Lakeview dashboard (JSON)

| File | Role |
|------|------|
| `adt_events_dashboard.json` | Lakeview dashboard: **12** datasets, one `queryLines` string each (avoids broken token joins). |
| `ens_adt_dataset_queries.sql` | **Same 12 SQL statements** in readable form — run in Databricks SQL to validate before/after import. |

**Shared rules (all time-filtered metrics except A01-today):** `FROM bronze.ensemble.ens_adt` **→** `message_type = 'ADT'` **→** `event_time_stamp IS NOT NULL` **→** `event_time_stamp >= date_sub(CURRENT_DATE(), 365)` **→** field-specific `IS NOT NULL` / `TRIM` as needed. **A01 today** uses `to_date(event_time_stamp) = CURRENT_DATE()` instead of the 365d window.

### `queryLines` and “stuck together” SQL

If a tool **concatenates** the `queryLines` array **without newlines or spaces**, you can get invalid tokens like `unique_patientsFROM` or `ens_adtWHERE` (parse error). This JSON uses **one string per query** in `queryLines` so the SQL stays valid no matter how the array is joined.

**Valid multi-line in a SQL editor** (not the same as a bad join):

```sql
SELECT count(distinct patient_mrn) AS unique_patients
FROM bronze.ensemble.ens_adt
WHERE event_time_stamp >= date_sub(current_date(), 365);
```

**Minimal test (no `message_type` filter)** if you need to debug:

```sql
SELECT count(DISTINCT patient_mrn) AS unique_patients
FROM bronze.ensemble.ens_adt
WHERE event_time_stamp >= date_sub(current_date(), 365);
```

Add `AND message_type = 'ADT'` again once you confirm the base query runs.

## Table: `bronze.ensemble.ens_adt` (confirm in Unity Catalog)

Core columns used in the dashboard (from live schema / sample rows):

- **Identity / time:** `message_id`, `message_type` (use **`ADT`**), `message_event_type` (A01, A08, A03, …), `event_time_stamp`
- **Patient:** `patient_mrn`, `patient_har`, `patient_csn`, `patient_class`, `patient_first_name`, `patient_last_name`
- **Location / source:** `sending_facility` (e.g. EPIC), `facility` (e.g. IHS), `department` (e.g. PTFFX), `room`, `bed`, prior fields
- **PV1 style:** `admission_type` (e.g. EL), `financial_class` (e.g. COMM), `admit_date_time`, `discharge_date_time`, **`discharge_diposition`** (column name spelling in bronze)

`ds_sending_facility` splits **sending** application (MSH) from **facility** (site) — your sample has both.

---

## What was wrong in the original

1. **Trend chart (`w_event_volume`)** had `encodings: {}` and `disaggregated: false` without a proper query field list — the chart has nothing to plot on X/Y. The fixed file uses a **line** chart with `event_date` (x, temporal) and `event_count` (y) and `disaggregated: true` with explicit fields.
2. **Date range filter** was wired to `event_time_stamp` on **aggregate-only** datasets (e.g. `SELECT COUNT(*) ...`). Those results do not include `event_time_stamp`, so the filter / associative queries do not work and can blank or error the page. The updated dashboard **removes** that control and uses a **365-day lookback in SQL** instead. To use an interactive date range again, add a filter in the **Lakeview UI** against a **row-level** (or at least time-grained) dataset, or a semantic model the product supports.
3. **Table path** is still `bronze.ensemble.ens_adt` — change every dataset to your real **catalog.schema.table** if it differs.
4. **`discharge_diposition`** is kept to match a common misspelling in bronze; fix the name in place if your column is `discharge_disposition` instead.

## If a chart type fails

If **`line`** is not available in your workspace, edit `w_event_volume` and set `"widgetType": "bar"` (encodings can stay the same in many builds).

## Validate JSON

```bash
python3 -m json.tool resources/lakeview/adt_events_dashboard.json > /dev/null && echo OK
```
