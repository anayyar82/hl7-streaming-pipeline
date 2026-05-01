# Reference — HL7 Streaming (e2-demo-field-eng, dev)

**Google Slides (paste this slide’s content here):**  
[HL7 Streaming deck](https://docs.google.com/presentation/d/1ViCSD922hbpnIbp-dM9VsLENJiiCVy2nbAK44sjhQSs/edit?slide=id.g3db67b81f56_0_0#slide=id.g3db67b81f56_0_0)

**Workspace:** `e2-demo-field-eng.cloud.databricks.com` · **Org:** `?o=1444828305810485`  
**Refresh IDs after redeploy:** `databricks bundle summary -t dev -o json`

---

## Slide title (suggested)

**Reference — Jobs, pipeline, alerts**

---

## Slide body (copy-paste)

**DLT pipeline**  
`https://e2-demo-field-eng.cloud.databricks.com/pipelines/a28d206f-e707-4951-ba16-42a94f075be0?o=1444828305810485`

**Workflows (Run now)**  
• Sample data — `…/jobs/544648305465960?o=1444828305810485`  
• AutoML training — `…/jobs/942558712516151?o=1444828305810485`  
• Model inference — `…/jobs/280816865078318?o=1444828305810485`  
• Lakebase load — `…/jobs/931240195475649?o=1444828305810485`  
• DLT → Inference → Lakebase — `…/jobs/179564813727985?o=1444828305810485`  
• Lakebase sync — `…/jobs/686734185398624?o=1444828305810485`  
• Genie UC grants — `…/jobs/254076791507194?o=1444828305810485`  
• Lakebase OAuth grants — `…/jobs/1000022517383894?o=1444828305810485`  
• Daily insight email — `…/jobs/734864605801011?o=1444828305810485`

*(Prefix every job link with `https://e2-demo-field-eng.cloud.databricks.com` — ellipsis above saves space on the slide.)*

**SQL alerts**  
• Message freshness (stale) — `…/sql/alerts-v2/4396818072956569?o=1444828305810485`  
• Daily insight (paused) — `…/sql/alerts-v2/278973608345579?o=1444828305810485`

**Genie** — `https://e2-demo-field-eng.cloud.databricks.com/genie/rooms/01f1286ab1d611dd92b5807d9280541b?o=1444828305810485`

**Repo / bundle** — `https://github.com/anayyar82/hl7-streaming-pipeline`

---

## Full URLs (speaker notes or appendix slide)

| Resource | URL |
|----------|-----|
| DLT `hl7_streaming_dlt` | https://e2-demo-field-eng.cloud.databricks.com/pipelines/a28d206f-e707-4951-ba16-42a94f075be0?o=1444828305810485 |
| Job `hl7_sample_data` | https://e2-demo-field-eng.cloud.databricks.com/jobs/544648305465960?o=1444828305810485 |
| Job `hl7_automl_training` | https://e2-demo-field-eng.cloud.databricks.com/jobs/942558712516151?o=1444828305810485 |
| Job `hl7_model_inference` | https://e2-demo-field-eng.cloud.databricks.com/jobs/280816865078318?o=1444828305810485 |
| Job `hl7_lakebase_load` | https://e2-demo-field-eng.cloud.databricks.com/jobs/931240195475649?o=1444828305810485 |
| Job `hl7_refresh_workflow` | https://e2-demo-field-eng.cloud.databricks.com/jobs/179564813727985?o=1444828305810485 |
| Job `hl7_lakebase_sync` | https://e2-demo-field-eng.cloud.databricks.com/jobs/686734185398624?o=1444828305810485 |
| Job `hl7_genie_uc_grants` | https://e2-demo-field-eng.cloud.databricks.com/jobs/254076791507194?o=1444828305810485 |
| Job `hl7_lakebase_app_grants` | https://e2-demo-field-eng.cloud.databricks.com/jobs/1000022517383894?o=1444828305810485 |
| Job `hl7_daily_insight_html_email` | https://e2-demo-field-eng.cloud.databricks.com/jobs/734864605801011?o=1444828305810485 |
| Alert `hl7_gold_message_freshness_stale` | https://e2-demo-field-eng.cloud.databricks.com/sql/alerts-v2/4396818072956569?o=1444828305810485 |
| Alert `hl7_daily_data_insight_email` | https://e2-demo-field-eng.cloud.databricks.com/sql/alerts-v2/278973608345579?o=1444828305810485 |

---

## Stack order (for narration)

`hl7_sample_data` → `hl7_streaming_dlt` (full refresh when needed) → optional `hl7_automl_training` → `hl7_model_inference` → `hl7_lakebase_load` — or run **`hl7_refresh_workflow`** for DLT + inference + Lakebase.

Script: `SKIP_AUTOML=1 ./scripts/run_full_stack_full_refresh.sh dev`
