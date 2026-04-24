# HL7 Streaming — evaluator cheat sheet (1 page)

Quick map from **rubric category → where to look in repo → what to say in demo**. Full narrative: `ARCHITECTURE.md`, `README.md`, `SOLUTION_BRIEF_HL7_STREAMING.md`.

| Category | Points | Evidence (paths) | One-liner for evaluators |
|----------|-------:|------------------|--------------------------|
| **End-to-end architecture** | 20 | `docs/ARCHITECTURE.md` (L1–L6), `databricks.yml` + `resources/hl7_pipeline.yml`, `hl7-forecasting-app/app.py` + `utils/navigation.py` | HL7 → UC volume → **DLT** gold → jobs (ML/inference) → **Lakebase** read path → **Databricks Apps**; **Genie** on UC; clinical vs platform UI split on purpose. |
| **Vibe coding** | 10 | *(process, not in Git)* | State 2–3 concrete examples (e.g. Streamlit/layout, Genie space SQL, bundle YAML, `genie_client`) and which assistant tools you used. |
| **Lakebase usage** | 15 | `hl7-forecasting-app/utils/db.py` (OAuth, token cache, lock, batch queries), `hl7-forecasting-app/pages/0d_load_test.py`, `notebooks/10_lakebase_load.py`, `notebooks/11_lakebase_grants.py` | **Low-latency, governed Postgres** for dashboard SQL; identity via **Databricks OAuth** / `databricks_create_role` (see architecture doc). Frame as **operational analytics read model**, not OLTP “shopping cart” unless you add that. |
| **Reverse ETL design** | 10 | `docs/ARCHITECTURE.md` (“Read-optimized path”), `notebooks/09_lakebase_sync.py` (CDF, synced-table prep / UI workflow for Autoscaling), `notebooks/10_lakebase_load.py` (Spark → Postgres **TRUNCATE + INSERT** snapshot), bundle jobs / workflow in `resources/hl7_pipeline.yml` + Run jobs UI | **Gold → Lakebase**: job-driven sync; discuss **snapshot vs triggered/synced table** options and **ordering** with DLT + inference. |
| **Genie integration** | 10 | `hl7-forecasting-app/pages/8_genie_chat.py`, `hl7-forecasting-app/utils/genie_client.py`, `hl7-forecasting-app/app.yaml` (`GENIE_SPACE_ID`), `notebooks/12_genie_uc_grants.py` | NL → SQL over a **curated space**; runtime = **app service principal**; show **2–3 canned questions** that you know work; mention UC + warehouse grants. |
| **Databricks App quality** | 10 | `hl7-forecasting-app/` (multipage Streamlit), `utils/theme.py`, `utils/ui.py`, platform pages under `hl7-forecasting-app/pages/` | Themed multipage app, clear IA (**Clinical / Platform / Genie**), interactivity (filters, refresh, jobs, load test). Optional second surface: `hl7-appkit-app/` + `bundles/hl7_appkit/`. |
| **Security & governance** | 10 | `docs/ARCHITECTURE.md` (“Security and identity”), UC grant notebooks above, `app.yaml` env (no static DB passwords for Lakebase) | **UC least privilege**, **OAuth** to Lakebase, **Genie** scoped to space + grants, secrets in **scopes** (not Git) for things like email jobs. |
| **CI/CD & deployment (DAB)** | 10 | `databricks.yml` (targets: `dev` / `staging` / `prod`, variables), `resources/*.yml`, `hl7-forecasting-app/app.yaml`, `scripts/run_full_stack_full_refresh.sh` | **Databricks Asset Bundle** defines pipelines/jobs/vars; same bundle promotes across targets; tie **deploy** to your actual workflow (manual vs CI). |
| **Data engineering quality** | 5 | `resources/hl7_pipeline.yml`, DLT notebooks / pipeline code as referenced in bundle | Medallion **bronze → silver → gold**, HL7 parse/conform; pick **one entity** (e.g. ADT/census) and trace to gold. |
| **Performance & scalability** | 5 | `hl7-forecasting-app/utils/db.py` (connection discipline), load-test page, DLT/Photon in job config where used | Explain **why** short-lived connections + lock under Streamlit; **load test** for Lakebase; batch inference for ML scale. |
| **Presentation & use case** | 5 | `README.md`, `docs/SOLUTION_BRIEF_HL7_STREAMING.md` | **ED & ICU operations**: census, trends, forecasts, throughput — business outcome in first 60 seconds. |

---

## 5-minute demo order (suggested)

1. **Diagram** — one slide or whiteboard: ingest → DLT → gold → Lakebase job → App + Genie.  
2. **Clinical** — one Lakebase-backed page (e.g. real-time ops or trends).  
3. **Platform** — System status or Run jobs / DLT live (proves operability).  
4. **Genie** — two curated questions; if offline, show space + grants story.  
5. **Close** — governance (UC + SP + OAuth) and how you’d run the stack daily (`run_full_stack_full_refresh.sh` or workflow job).

---

## Honest framing (avoids rubric mismatch)

- **Lakebase (15 pts):** Strength is **interactive dashboard latency** and **Postgres ergonomics** on a **governed read replica** of gold. If asked about “transactions / user state,” either show a small transactional use case or explicitly scope to **analytics serving layer**.  
- **Reverse ETL (10 pts):** You have both **synced-table preparation** (`09_lakebase_sync.py`) and **job-based snapshot load** (`10_lakebase_load.py`); name which is primary in your workspace and why.

---

*Last updated: aligns with repo layout at doc creation time; adjust job/page names if you rename resources.*
