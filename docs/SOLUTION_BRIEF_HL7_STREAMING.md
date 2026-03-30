Usecase 1
HL7 Streaming — ED/ICU Operations, Clinical Gold & Forecasting on Databricks (Reference implementation)

Problem Statement
HL7 v2.x feeds in healthcare environments are often:

high-volume and continuous (ADT, ORU, ORM, and related messages in large batches or streams, multi-message files, inconsistent formatting)
semi-structured and error-prone (segment layout varies by vendor and site; manual parsing does not scale)
hard to reconcile (linking encounters, patients, observations, orders, and allergies into analytics-ready tables needs repeatable medallion logic)
slow to turn into action (operations need near–real-time ED/ICU census and flow; planners want forecasts and accuracy tracking without bespoke ETL for every question)

The app solves:

"How do we reliably ingest HL7 v2.x into a governed Lakehouse, serve low-latency operations and clinical dashboards, and support forecasting—with optional natural-language access to the same gold data?"

Target Users and Value Proposition
Target User
Pain Point
Value Proposition
Hospital / health system operations
Limited visibility into ED/ICU census, hourly flow, and pipeline health across facilities.
Operational clarity: Real-time and trend views on census, arrivals/discharges, message throughput, and data freshness from curated gold tables.
Clinical & quality analysts
Spreadsheets and one-off SQL on raw or siloed extracts; weak governance.
Governed analytics: Patient, encounter, diagnosis, observation, allergy, and order facts in Unity Catalog, with Lakeview and Streamlit for cohort exploration.
Capacity & planning teams
Reactive staffing; weak short-horizon signals for arrivals and load.
Forecasting loop: Feature tables in Delta, AutoML-trained models in MLflow (UC), batch inference into gold_forecast_predictions, with accuracy in gold_forecast_accuracy.
Platform / data engineering
Fragile pipelines and environment drift.
Repeatable delivery: Delta Live Tables (DLT) plus Databricks Asset Bundles for IaC, Photon-enabled pipelines, and jobs for training, inference, and Lakebase sync.



Context
Reference architecture for batch-landed HL7 v2.x on Databricks: funke-based parsing, DLT medallion layers (Bronze → Silver → Gold), reporting and forecasting notebooks, Lakebase Postgres for low-latency reads in a Databricks App (Streamlit HL7App), and Lakeview dashboards on SQL warehouses over the same gold schema. AI/BI Genie can be attached to the app for governed natural-language Q&A over Unity Catalog tables that back the Genie space.

Key Messages:

What: An end-to-end HL7 v2.x to Delta Lake pipeline with gold dimensional and fact models, ED/ICU operational metrics, ML forecasting (features, AutoML, inference), Lakeview dashboards, and an HL7App Streamlit experience on Lakebase, plus optional Genie in-app chat.

Why: To reduce manual parsing and ad hoc extracts, improve trust through Unity Catalog governance, and give operations and analytics one place to monitor flow, clinical activity, and model performance—without locking insights in a single BI tool.

How: Ingest raw files to a Unity Catalog Volume; run DLT (notebooks 01–05) for parse, structure, gold, reports, and forecast features; run jobs for AutoML (07), inference (08), and Lakebase load (10); deploy the Streamlit app (hl7-forecasting-app) and Lakeview JSON dashboards via the bundle; configure a Genie space and GENIE_SPACE_ID for conversational analytics.

Ideas that can be built
Real-time alerting on ADT or census thresholds:
What: Notifications when ED census, arrivals, or message backlog crosses policy limits.
How: DLT or streaming triggers, Lakeflow Jobs or webhooks to email or ticketing, with thresholds in Delta config tables.

Cross-facility command center:
What: A single pane comparing hospitals, units, and message-type mix with drill-down to patient class and location.
How: Extend gold encounter and message metrics models; Lakeview parameters plus Streamlit filters (pattern in hl7-forecasting-app utils/filters.py).

Forecast explainability and champion promotion:
What: UI to compare model versions, promote MLflow aliases, and show feature importance for arrival forecasts.
How: MLflow Tracking and Model Registry APIs in a notebook or App page; read gold_model_training_metadata and prediction history.

OMOP / research bridge:
What: Map HL7-derived facts to a standard analytics model for research networks.
How: Additional Gold notebook layer emitting OMOP-shaped Delta tables; Unity Catalog grants for a separate research catalog.

Patient timeline / encounter narrative (governed):
What: Authorized users see a time-ordered clinical timeline from gold facts.
How: Streamlit page on Lakebase with row-level or column-masking in Unity Catalog; audit via system tables.


Usecase 2
Ask Your Data on Governed HL7 Gold — Genie + HL7App (Reference implementation)

Problem Statement
Analysts and clinical informaticists often know the business question but not the exact SQL or table layout, and still need answers on governed data without exporting sensitive data to unmanaged tools.

The app solves:

"How can users ask questions in natural language against curated HL7 gold tables—with Unity Catalog enforcement and the same warehouse semantics as dashboards?"

Target Users and Value Proposition
Target User
Pain Point
Value Proposition
Analysts / informaticists
Rewriting similar SQL and hunting table and column documentation.
Faster exploration: Genie proposes SQL against an approved Genie space (for example Healthcare Operations and Patient Analytics).
Operations leadership
Ad hoc questions during incidents without waiting for a full report refresh.
Conversational insights: In-app Ask your data page (8_genie_chat.py) on Databricks Apps.
Governance owners
Shadow IT and ungoverned LLM tools outside UC boundaries.
UC-bound context: Genie spaces, SELECT grants for the app service principal, warehouse CAN USE—aligned with 12_genie_uc_grants and app resources.



Context
N/A (same pattern as lightweight self-service assistants: conversational surface on a governed Lakehouse backend.)

Key Messages:

What: A Genie Conversation API–backed chat page in HL7App, with GENIE_SPACE_ID from a Databricks App Genie space resource (valueFrom: genie-space) or an explicit environment variable.

Why: To complement fixed dashboards with flexible Q&A without giving up Unity Catalog and SQL warehouse governance.

How: Databricks Apps, databricks-sdk (WorkspaceClient, genie.start_conversation_and_wait / create_message_and_wait), optional Lakeview genieSpace on dashboards; Unity Catalog grants via 12_genie_uc_grants.py and job hl7_genie_uc_grants.

Ideas that can be built
Genie benchmark pack for HL7 gold:
What: Curated sample questions and reference SQL for encounters, diagnoses, observations, and forecast tables to improve answer quality.
How: Versioned markdown or Delta table of Q&A pairs; review cycles with subject-matter experts.

Blended experience: dashboard tile to Genie follow-up:
What: Deep links from Lakeview into the App Genie page with context such as department filter.
How: URL parameters or app routing where the product supports passing session hints.

Audit log of Genie-suggested SQL:
What: A compliance-friendly record of suggested statements and result metadata.
How: Custom wrapper around Genie API responses appending to a Delta audit table for review.


Repository: HL7 Streaming (Databricks Asset Bundle, notebooks, hl7-forecasting-app). Detailed runbooks: README.md at repo root.
