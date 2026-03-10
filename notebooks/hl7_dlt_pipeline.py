# Databricks notebook source
# MAGIC %md
# MAGIC # HL7 Streaming Pipeline - Delta Live Tables
# MAGIC 
# MAGIC Complete HL7 v2.x streaming pipeline using Delta Live Tables and the **funke** library.
# MAGIC 
# MAGIC ## Architecture (Medallion)
# MAGIC ```
# MAGIC ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
# MAGIC │   Bronze    │───▶│   Silver    │───▶│    Gold     │
# MAGIC │  01_bronze  │    │  02_silver  │    │  03_gold    │
# MAGIC └─────────────┘    └─────────────┘    └─────────────┘
# MAGIC ```
# MAGIC 
# MAGIC ## Notebooks
# MAGIC | Notebook | Layer | Description |
# MAGIC |----------|-------|-------------|
# MAGIC | `01_bronze.py` | Bronze | Raw ingestion, multi-message splitting, funke parsing |
# MAGIC | `02_silver.py` | Silver | Segment extraction (MSH, PID, PV1, OBX, DG1, AL1, IN1, EVN, OBR) |
# MAGIC | `03_gold.py` | Gold | Dimension/Fact tables + monitoring metrics |
# MAGIC 
# MAGIC ## Tables
# MAGIC 
# MAGIC ### Bronze
# MAGIC - `bronze_hl7_raw` - Raw HL7 files (one row per file, may contain multiple messages)
# MAGIC - `bronze_hl7_split` - Individual messages split from multi-message files
# MAGIC - `bronze_hl7_parsed` - Messages parsed with funke
# MAGIC 
# MAGIC ### Silver
# MAGIC - `silver_hl7_parsed` - Key identifiers extracted
# MAGIC - `silver_msh` - Message Header
# MAGIC - `silver_pid` - Patient Identification
# MAGIC - `silver_pv1` - Patient Visit
# MAGIC - `silver_evn` - Event Type
# MAGIC - `silver_obr` - Observation Request
# MAGIC - `silver_obx` - Observation/Result
# MAGIC - `silver_dg1` - Diagnosis
# MAGIC - `silver_al1` - Allergy
# MAGIC - `silver_in1` - Insurance
# MAGIC 
# MAGIC ### Gold
# MAGIC - `gold_patient_dim` - Patient dimension (SCD Type 1)
# MAGIC - `gold_encounter_fact` - Patient encounters
# MAGIC - `gold_observation_fact` - Lab results / vitals
# MAGIC - `gold_diagnosis_fact` - Diagnosis codes
# MAGIC - `gold_allergy_fact` - Allergies
# MAGIC - `gold_order_fact` - Lab/procedure orders
# MAGIC - `gold_message_metrics` - Processing metrics
# MAGIC - `gold_patient_activity` - Activity summary
# MAGIC 
# MAGIC ## funke Field Access Pattern
# MAGIC ```
# MAGIC hl7.SEGMENT[segment_rep].fields[field_num][field_rep][component][subcomponent]
# MAGIC ```
# MAGIC Example: `hl7.MSH[0].fields[9][0][1][1]` → Message Type (MSH-9-1)
# MAGIC 
# MAGIC ## Configuration
# MAGIC Set these in the DLT pipeline settings:
# MAGIC - `hl7.source_path` - Path to raw HL7 message files
# MAGIC - `hl7.checkpoint_path` - Checkpoint location for streaming
