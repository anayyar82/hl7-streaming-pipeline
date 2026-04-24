# ADT Lakeview dashboard (JSON)

File: `adt_events_dashboard.json` — import or merge into a **Lakeview** dashboard in Databricks (Dashboards / Lakeview UI) or use as a reference for `.lvdash.json` workflows.

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
