# Genie: API access, RBAC with OAuth, and factory rollout

This document answers three questions for teams standardizing **Databricks AI/BI Genie** behind APIs:

1. How to call Genie through **APIs** (REST and SDK).
2. How **RBAC** applies when using OAuth (and what Genie does *not* do for you).
3. **Factory-style** procedures to stamp out the same pattern across workspaces and apps.

It aligns with this repository’s Streamlit integration (`hl7-forecasting-app/utils/genie_client.py`, `pages/8_genie_chat.py`) and grant notebook (`notebooks/12_genie_uc_grants.py`). Official behavior always wins over this doc; verify against current Databricks documentation for your cloud and plan.

---

## 1. Accessing Genie through API calls

Databricks exposes Genie in two API families (see [Use the Genie API](https://docs.databricks.com/en/genie/conversation-api)):

| API family | Purpose |
|------------|---------|
| **Conversation APIs** | Natural-language Q&A in *your* app: start a thread, send follow-ups, retrieve answers (stateful conversations). |
| **Management APIs** | Create/update Genie **spaces**, wire **data sources**, instructions, benchmarks — suitable for CI/CD and “factory” provisioning. |

### 1.1 Authentication (required for every call)

All workspace REST calls use your **workspace host** (from the browser URL) and a **Bearer token**:

- **User in the browser (interactive / delegated):** [OAuth for users (U2M)](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-u2m) — typical for local dev or tools that open a browser once.
- **Automation / server / Databricks App default runtime:** [OAuth for service principals (M2M)](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m) — client ID + secret (or managed identity where supported) to obtain a token, then `Authorization: Bearer <token>`.

The token’s **subject** (user or service principal) is what Unity Catalog and the SQL warehouse evaluate for **every** SQL Genie generates.

### 1.2 REST (pattern)

- Base URL: `https://<workspace-host>/`
- Header: `Authorization: Bearer <access_token>`
- Conversation endpoints live under the workspace **Genie** REST API (see [Genie API reference](https://docs.databricks.com/api/workspace/genie)). Typical flow:
  1. **Start conversation** for a space (returns a conversation id).
  2. **Create message** (user prompt) on that conversation; poll or wait for completion depending on API variant.
  3. Read the assistant message payload (text, SQL, attachments, optional statement id for result preview).

Exact paths and request bodies change with API versions; use the linked reference pages rather than hard-coding URLs from blog posts.

### 1.3 Python SDK (pattern used in this repo)

Inside Databricks (notebook, job, or **Databricks App**), the [Databricks SDK for Python](https://docs.databricks.com/aws/en/dev-tools/sdks) `WorkspaceClient()` resolves credentials from the runtime environment:

```python
from databricks.sdk import WorkspaceClient

client = WorkspaceClient()
# Conversation helpers (names may vary slightly by SDK version):
client.genie.start_conversation_and_wait(space_id, prompt)
client.genie.create_message_and_wait(space_id, conversation_id, prompt)
```

This repository wraps that in `hl7-forecasting-app/utils/genie_client.py` (`ask_genie`, conversation id handling, and optional **statement execution** preview for tabular results).

**Space id:** configured as `GENIE_SPACE_ID` (or `DATABRICKS_GENIE_SPACE_ID`) — UUID from the Genie room URL `.../genie/rooms/<SPACE_ID>`.

### 1.4 Prerequisites (easy to forget)

From Databricks’ own prerequisites ([conversation API doc](https://docs.databricks.com/en/genie/conversation-api)):

- Workspace with **Databricks SQL** entitlement where required.
- The calling identity needs **`CAN USE`** on the **SQL warehouse** bound to the Genie space (Pro / Serverless per product rules).
- A **curated Genie space** (tables, instructions, benchmarks) — poor space quality cannot be fixed by API wiring alone.

---

## 2. Ensuring Genie follows RBAC when using OAuth

**Core rule:** Genie does **not** bypass Unity Catalog. Generated SQL runs in the warehouse using credentials associated with the **same identity that obtained the OAuth token** (unless your product integration explicitly uses a different documented model).

### 2.1 What “RBAC” means here

| Layer | What enforces access |
|-------|----------------------|
| **Unity Catalog** | `USE CATALOG`, `USE SCHEMA`, table/view `SELECT`, row filters, column masks, etc., on the identity executing the query. |
| **SQL warehouse** | `CAN USE` (and related warehouse permissions) on the identity. |
| **Genie space configuration** | **Logical** scope: which tables, metric views, instructions, and examples the model may use. This reduces *confusion* and *prompt injection surface* but is **not** a substitute for UC denies. |

If the identity cannot `SELECT` a table, Genie cannot “magically” return its rows through the API.

### 2.2 Service principal vs end-user (two supported models)

**A. App or integration runs as a service principal (common in Databricks Apps)**

- OAuth **M2M** for the SP.
- All Genie answers are limited to what that SP can read in UC + the curated space.
- **Pros:** Simple operations, easy to grant consistently, good for a **shared analytics** space (e.g. “Healthcare operations — gold read model”).
- **Cons:** Not row-level “who is the analyst” unless you add **separate** controls (separate spaces, separate apps, or dynamic identity — see below).

This repo’s README states that the Streamlit Genie page uses `WorkspaceClient()` in the app runtime, so calls run as the **app’s service principal**, not each browser user’s personal identity.

**B. On-behalf-of user (when product supports it)**

Some deployments use **Databricks Apps** `user_api_scopes` so the app can call workspace APIs **as the signed-in user** (subject to Databricks product support and your bundle configuration). Example in this monorepo: `bundles/hl7_appkit/resources/hl7_appkit_app_resource.yml` includes `dashboards.genie` under `user_api_scopes` (AppKit path). The main `hl7-forecasting-app` bundle currently lists `sql` only under `resources/hl7_pipeline.yml` — align scopes with what your integration actually calls.

**Pros:** UC row/column policies and table grants can reflect **the human user**.  
**Cons:** More moving parts: OAuth for users, consent, and stricter testing per profile.

Pick one model per product surface and document it in your runbook.

### 2.3 Checklist: RBAC + OAuth for Genie

1. **Choose identity model** (SP M2M vs user delegation) and record it in your architecture doc.
2. **Least privilege on UC:** grant only the catalog/schema/tables needed for that Genie space (prefer `SELECT` on views that already encode joins).
3. **Warehouse:** grant **`CAN USE`** on the exact warehouse the space uses.
4. **Curate the space:** limit tables, add instructions, add benchmarks ([best practices](https://docs.databricks.com/aws/en/genie/best-practices)).
5. **Secrets:** store SP client secrets in Databricks **secret scopes**, not in Git.
6. **Audit:** use UC audit lineage and warehouse query history to verify who ran which SQL pattern in production.

### 2.4 Common misconceptions

- **“OAuth fixes compliance by itself.”** OAuth proves *who* the caller is; **UC + warehouse** still decide *what* they can read.
- **“Genie space replaces UC grants.”** The space guides the model; **denies** still come from UC.
- **“One mega-space for all domains.”** Easiest to ship, hardest to govern — prefer **domain spaces** (ED, finance, research) with narrow grants per integration identity.

---

## 3. Factory-style rollout on Databricks

“Factory” means the same **repeatable** steps in every environment (dev / stage / prod) with naming, IaC, and verification.

### 3.1 Standard building blocks

| Artifact | Convention |
|----------|--------------|
| **UC catalog/schema** | e.g. `corp_analytics.hl7_gold_prod` — separate per env or single catalog with `_dev` / `_prod` schemas. |
| **Genie space** | One space per **domain + env**, e.g. `genie-hl7-ops-dev`, `genie-hl7-ops-prod`. |
| **SQL warehouse** | Dedicated “Genie warehouse” per tier or shared — document **warehouse id** next to the space. |
| **Runtime identity** | One **service principal per app** (or per tier) — never share one SP across unrelated data domains if you can avoid it. |
| **Configuration** | `GENIE_SPACE_ID` in `app.yaml`, bundle `env`, or Apps UI env — single source of truth per deploy target. |

### 3.2 Automation options

1. **Management APIs** — Create/update spaces from CI using [Create Genie space](https://docs.databricks.com/api/workspace/genie/createspace) and related endpoints; store `serialized_space` JSON in version control (treat as config, not secrets).
2. **Jobs / notebooks for grants** — Same pattern as `notebooks/12_genie_uc_grants.py`: parameterized `GRANT` SQL for `USE CATALOG`, `USE SCHEMA`, `SELECT ON SCHEMA`, `CAN USE ON WAREHOUSE`, run as a principal that can grant.
3. **Databricks Asset Bundles** — Declare apps, env vars, and optionally jobs; promote the same bundle across targets with different variable files (`databricks.yml` targets).

### 3.3 Factory runbook (copy per new workspace)

1. Create **UC** catalog/schema (or confirm existing) and **views** exposed to Genie.
2. Create **SQL warehouse**; note id; grant `CAN USE` to the app SP (and to admins who curate the space).
3. Create **Genie space**; attach warehouse; add **tables**, **instructions**, **≥5 benchmark questions** ([benchmarks](https://docs.databricks.com/aws/en/genie/benchmarks)).
4. Run **grant job** (e.g. clone `12_genie_uc_grants.py` with widgets: catalog, schema, `app_service_principal`, `warehouse_name`).
5. Deploy **Databricks App** with `GENIE_SPACE_ID` and correct **`user_api_scopes`** if using delegated Genie.
6. **Smoke test:** 3 canned questions that must succeed in each env; fail the deploy if any fail.
7. **Documentation:** link space URL, SP id (`databricks apps get <app> -o json`), warehouse name, and owning team in your internal wiki.

### 3.4 How this repo fits the factory pattern

| Piece | Location |
|-------|----------|
| Genie chat UI + space id resolution | `hl7-forecasting-app/pages/8_genie_chat.py` |
| SDK conversation helpers | `hl7-forecasting-app/utils/genie_client.py` |
| Example env var | `hl7-forecasting-app/app.yaml` → `GENIE_SPACE_ID` |
| UC + warehouse grants for app SP | `notebooks/12_genie_uc_grants.py`, job `hl7_genie_uc_grants` in `resources/hl7_pipeline.yml` |
| Architecture summary | `docs/ARCHITECTURE.md` (Security and identity) |

---

## Official references (bookmark these)

- [Use the Genie API (conversation + management overview)](https://docs.databricks.com/en/genie/conversation-api)  
- [Genie API (REST reference)](https://docs.databricks.com/api/workspace/genie)  
- [OAuth for users (U2M)](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-u2m)  
- [OAuth for service principals (M2M)](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m)  
- [Genie in Databricks Apps](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/genie)  
- [Curate an effective Genie space](https://docs.databricks.com/aws/en/genie/best-practices)  
- [Set up and manage a Genie space](https://docs.databricks.com/aws/en/genie/set-up)

---

## Revision

Document introduced for HL7 Streaming reference implementation; update when Databricks changes Genie API paths, OAuth flows, or Apps `user_api_scopes` behavior.
