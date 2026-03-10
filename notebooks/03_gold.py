# Databricks notebook source
# MAGIC %md
# MAGIC # HL7 Streaming Pipeline - Gold Layer
# MAGIC 
# MAGIC Analytics-ready dimension and fact tables built from Silver layer segments.
# MAGIC 
# MAGIC ## Dimension Tables
# MAGIC - `gold_patient_dim` - Patient dimension (SCD Type 1)
# MAGIC 
# MAGIC ## Fact Tables
# MAGIC - `gold_encounter_fact` - Patient encounters/visits
# MAGIC - `gold_observation_fact` - Lab results and vitals
# MAGIC - `gold_diagnosis_fact` - Diagnosis codes (ICD)
# MAGIC - `gold_allergy_fact` - Patient allergies
# MAGIC - `gold_order_fact` - Lab/procedure orders
# MAGIC 
# MAGIC ## Monitoring Tables
# MAGIC - `gold_message_metrics` - Hourly message processing metrics
# MAGIC - `gold_patient_activity` - Daily patient activity summary
# MAGIC 
# MAGIC ## Related Notebooks
# MAGIC - `04_reports.py` - Real-time ED & ICU census, arrivals, discharges
# MAGIC - `05_forecasting.py` - Predictive feature tables & forecast outputs

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### gold_patient_dim
# MAGIC Patient dimension maintaining latest state (SCD Type 1).

# COMMAND ----------

@dlt.table(
    name="gold_patient_dim",
    comment="Patient dimension table - latest state per patient"
)
def gold_patient_dim():
    """Patient dimension maintaining latest state (SCD Type 1)."""
    return (
        dlt.read("silver_pid")
        .withColumn(
            "patient_key",
            F.md5(F.concat_ws("|", F.col("patient_id"), F.col("patient_id_authority")))
        )
        .withColumn(
            "patient_name_full",
            F.concat_ws(" ", F.col("patient_name_given"), F.col("patient_name_family"))
        )
        .withColumn(
            "address_full",
            F.concat_ws(", ",
                F.col("address_street"),
                F.col("address_city"),
                F.col("address_state"),
                F.col("address_zip")
            )
        )
        .groupBy("patient_key", "patient_id", "patient_id_authority")
        .agg(
            F.last("patient_name_full").alias("patient_name_full"),
            F.last("patient_name_family").alias("patient_name_family"),
            F.last("patient_name_given").alias("patient_name_given"),
            F.last("date_of_birth").alias("date_of_birth"),
            F.last("sex").alias("sex"),
            F.last("race").alias("race"),
            F.last("address_full").alias("address_full"),
            F.last("address_city").alias("address_city"),
            F.last("address_state").alias("address_state"),
            F.last("address_zip").alias("address_zip"),
            F.last("phone_home").alias("phone_primary"),
            F.last("marital_status").alias("marital_status"),
            F.min("parsed_at").alias("first_seen_at"),
            F.max("parsed_at").alias("last_updated_at"),
            F.last("sending_facility").alias("source_system"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### gold_encounter_fact
# MAGIC All patient visits/encounters from PV1 segments.

# COMMAND ----------

@dlt.table(
    name="gold_encounter_fact",
    comment="Encounter fact table - all patient visits/encounters"
)
def gold_encounter_fact():
    """Encounter fact table with visit details."""
    return (
        dlt.read("silver_pv1")
        .withColumn(
            "encounter_key",
            F.md5(F.concat_ws("|", F.col("message_id"), F.col("patient_id"), F.col("visit_number")))
        )
        .withColumn(
            "patient_key",
            F.md5(F.col("patient_id"))
        )
        .withColumn(
            "patient_class_desc",
            F.when(F.col("patient_class") == "I", "Inpatient")
            .when(F.col("patient_class") == "O", "Outpatient")
            .when(F.col("patient_class") == "E", "Emergency")
            .when(F.col("patient_class") == "P", "Preadmit")
            .otherwise(F.col("patient_class"))
        )
        .select(
            "encounter_key",
            "patient_key",
            "patient_id",
            "visit_number",
            "patient_class",
            "patient_class_desc",
            "admission_type",
            "admit_source",
            "location_facility",
            "location_point_of_care",
            "location_room",
            "location_bed",
            "hospital_service",
            "attending_doctor_id",
            "attending_doctor_name",
            "admit_datetime",
            "discharge_datetime",
            "message_type",
            "trigger_event",
            "message_id",
            F.col("sending_facility").alias("source_system"),
            F.col("parsed_at").alias("created_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### gold_observation_fact
# MAGIC Lab results and vitals from OBX segments.

# COMMAND ----------

@dlt.table(
    name="gold_observation_fact",
    comment="Observation/Lab result fact table"
)
def gold_observation_fact():
    """Observation fact table with lab results and vitals."""
    return (
        dlt.read("silver_obx")
        .withColumn(
            "observation_key",
            F.md5(F.concat_ws("|", F.col("message_id"), F.col("observation_id"), F.col("set_id")))
        )
        .withColumn(
            "patient_key",
            F.md5(F.col("patient_id"))
        )
        .withColumn(
            "observation_value_numeric",
            F.when(
                F.col("value_type") == "NM",
                F.regexp_extract(F.col("observation_value"), r"^-?[\d.]+", 0).cast("double")
            )
        )
        .withColumn(
            "is_abnormal",
            F.when(
                F.col("abnormal_flags").isin("H", "HH", "L", "LL", "A", "AA", "C"),
                F.lit(True)
            ).otherwise(F.lit(False))
        )
        .select(
            "observation_key",
            "patient_key",
            "patient_id",
            "observation_id",
            "observation_text",
            "value_type",
            F.col("observation_value").alias("observation_value_text"),
            "observation_value_numeric",
            "units",
            "reference_range",
            "abnormal_flags",
            "is_abnormal",
            F.col("result_status").alias("observation_result_status"),
            "observation_datetime",
            "message_id",
            F.col("sending_facility").alias("source_system"),
            F.col("parsed_at").alias("created_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### gold_diagnosis_fact
# MAGIC Diagnosis codes from DG1 segments.

# COMMAND ----------

@dlt.table(
    name="gold_diagnosis_fact",
    comment="Diagnosis fact table"
)
def gold_diagnosis_fact():
    """Diagnosis fact table with ICD codes."""
    return (
        dlt.read("silver_dg1")
        .withColumn(
            "diagnosis_key",
            F.md5(F.concat_ws("|", F.col("message_id"), F.col("diagnosis_code"), F.col("set_id")))
        )
        .withColumn(
            "patient_key",
            F.md5(F.col("patient_id"))
        )
        .select(
            "diagnosis_key",
            "patient_key",
            "patient_id",
            "diagnosis_code",
            "diagnosis_description",
            "diagnosis_coding_system",
            "diagnosis_datetime",
            "diagnosis_type",
            "diagnosis_priority",
            "message_id",
            F.col("sending_facility").alias("source_system"),
            F.col("parsed_at").alias("created_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### gold_allergy_fact
# MAGIC Patient allergies from AL1 segments.

# COMMAND ----------

@dlt.table(
    name="gold_allergy_fact",
    comment="Patient allergy fact table"
)
def gold_allergy_fact():
    """Allergy fact table with allergen details."""
    return (
        dlt.read("silver_al1")
        .withColumn(
            "allergy_key",
            F.md5(F.concat_ws("|", F.col("message_id"), F.col("allergen_code"), F.col("set_id")))
        )
        .withColumn(
            "patient_key",
            F.md5(F.col("patient_id"))
        )
        .withColumn(
            "is_severe",
            F.when(
                F.col("severity_code").isin("SV", "SE"),
                F.lit(True)
            ).otherwise(F.lit(False))
        )
        .select(
            "allergy_key",
            "patient_key",
            "patient_id",
            "allergen_type_code",
            "allergen_type_text",
            "allergen_code",
            "allergen_description",
            "severity_code",
            "severity_text",
            "is_severe",
            "reaction",
            "identification_date",
            "message_id",
            F.col("sending_facility").alias("source_system"),
            F.col("parsed_at").alias("created_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### gold_order_fact
# MAGIC Lab/procedure orders from OBR segments.

# COMMAND ----------

@dlt.table(
    name="gold_order_fact",
    comment="Order/Lab request fact table"
)
def gold_order_fact():
    """Order fact table from OBR segments."""
    return (
        dlt.read("silver_obr")
        .withColumn(
            "order_key",
            F.md5(F.concat_ws("|", 
                F.col("message_id"), 
                F.coalesce(F.col("placer_order_number"), F.col("filler_order_number")),
                F.col("set_id")
            ))
        )
        .withColumn(
            "patient_key",
            F.md5(F.col("patient_id"))
        )
        .select(
            "order_key",
            "patient_key",
            "patient_id",
            "placer_order_number",
            "filler_order_number",
            "universal_service_id",
            "universal_service_text",
            "universal_service_coding_system",
            "priority",
            "requested_datetime",
            "observation_datetime",
            "observation_end_datetime",
            "ordering_provider_id",
            "ordering_provider_name",
            "result_status",
            "message_id",
            F.col("sending_facility").alias("source_system"),
            F.col("parsed_at").alias("created_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### gold_message_metrics
# MAGIC Hourly message processing metrics for operational monitoring.

# COMMAND ----------

@dlt.table(
    name="gold_message_metrics",
    comment="Aggregated message processing metrics"
)
def gold_message_metrics():
    """Hourly message processing metrics for monitoring."""
    return (
        dlt.read("silver_msh")
        .withColumn("processing_hour", F.date_trunc("hour", F.col("parsed_at")))
        .groupBy("processing_hour", "message_type", "sending_facility")
        .agg(
            F.count("*").alias("message_count"),
            F.countDistinct("message_control_id").alias("unique_messages"),
            F.min("parsed_at").alias("first_message_at"),
            F.max("parsed_at").alias("last_message_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### gold_patient_activity
# MAGIC Daily patient activity summary across facilities.

# COMMAND ----------

@dlt.table(
    name="gold_patient_activity",
    comment="Patient activity summary"
)
def gold_patient_activity():
    """Daily patient activity summary."""
    return (
        dlt.read("gold_encounter_fact")
        .withColumn("activity_date", F.to_date(F.col("created_at")))
        .groupBy("activity_date", "location_facility", "patient_class_desc")
        .agg(
            F.countDistinct("patient_id").alias("unique_patients"),
            F.count("*").alias("encounter_count"),
            F.countDistinct("attending_doctor_id").alias("unique_providers")
        )
    )
