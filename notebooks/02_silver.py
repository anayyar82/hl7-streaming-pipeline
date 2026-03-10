# Databricks notebook source
# MAGIC %md
# MAGIC # HL7 Streaming Pipeline - Silver Layer
# MAGIC 
# MAGIC Segment extraction and data quality enforcement.
# MAGIC Reads from `bronze_hl7_parsed` and produces one table per HL7 segment.
# MAGIC 
# MAGIC ## Tables
# MAGIC - `silver_hl7_parsed` - Key identifiers extracted (message_type, patient_id, etc.)
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
# MAGIC ## funke Field Access Pattern
# MAGIC ```
# MAGIC hl7.SEGMENT[segment_rep].fields[field_num][field_rep][component][subcomponent]
# MAGIC ```

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_hl7_parsed
# MAGIC Core staging table with key identifiers extracted for downstream use.

# COMMAND ----------

@dlt.table(
    name="silver_hl7_parsed",
    comment="Parsed HL7 messages with key identifiers extracted"
)
@dlt.expect_or_drop("valid_message", "hl7.MSH IS NOT NULL")
def silver_hl7_parsed():
    """Extract key message identifiers from parsed HL7 messages."""
    return (
        dlt.read_stream("bronze_hl7_parsed")
        # MSH-9-1: Message Type, MSH-9-2: Trigger Event
        .withColumn("message_type", F.col("hl7.MSH")[0]["fields"][9][0][1][1])
        .withColumn("trigger_event", F.col("hl7.MSH")[0]["fields"][9][0][2][1])
        # MSH-4: Sending Facility
        .withColumn("sending_facility", F.col("hl7.MSH")[0]["fields"][4][0][1][1])
        # MSH-10: Message Control ID
        .withColumn("message_control_id", F.col("hl7.MSH")[0]["fields"][10][0][1][1])
        # PID-3-1: Patient ID
        .withColumn("patient_id", F.col("hl7.PID")[0]["fields"][3][0][1][1])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_msh - Message Header

# COMMAND ----------

@dlt.table(
    name="silver_msh",
    comment="Message Header (MSH) segments"
)
@dlt.expect_or_drop("msh_message_type_not_null", "message_type IS NOT NULL")
@dlt.expect_or_drop("msh_control_id_not_null", "message_control_id IS NOT NULL")
@dlt.expect("msh_sending_facility_not_null", "sending_facility IS NOT NULL")
def silver_msh():
    """
    MSH-3:  Sending Application
    MSH-4:  Sending Facility
    MSH-5:  Receiving Application
    MSH-6:  Receiving Facility
    MSH-7:  Message Date/Time
    MSH-9:  Message Type (9-1) + Trigger Event (9-2)
    MSH-10: Message Control ID
    MSH-11: Processing ID
    MSH-12: Version ID
    """
    return (
        dlt.read_stream("silver_hl7_parsed")
        .select(
            "message_id",
            F.col("hl7.MSH")[0]["fields"][3][0][1][1].alias("sending_application"),
            F.col("hl7.MSH")[0]["fields"][4][0][1][1].alias("sending_facility"),
            F.col("hl7.MSH")[0]["fields"][5][0][1][1].alias("receiving_application"),
            F.col("hl7.MSH")[0]["fields"][6][0][1][1].alias("receiving_facility"),
            F.col("hl7.MSH")[0]["fields"][7][0][1][1].alias("message_datetime"),
            F.col("hl7.MSH")[0]["fields"][9][0][1][1].alias("message_type"),
            F.col("hl7.MSH")[0]["fields"][9][0][2][1].alias("trigger_event"),
            F.col("hl7.MSH")[0]["fields"][10][0][1][1].alias("message_control_id"),
            F.col("hl7.MSH")[0]["fields"][11][0][1][1].alias("processing_id"),
            F.col("hl7.MSH")[0]["fields"][12][0][1][1].alias("version_id"),
            "parsed_at"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_pid - Patient Identification

# COMMAND ----------

@dlt.table(
    name="silver_pid",
    comment="Patient Identification (PID) segments"
)
@dlt.expect_or_drop("pid_patient_id_not_null", "patient_id IS NOT NULL AND patient_id != ''")
@dlt.expect("pid_patient_name_present", "patient_name_family IS NOT NULL OR patient_name_given IS NOT NULL")
@dlt.expect("pid_valid_sex", "sex IS NULL OR sex IN ('M', 'F', 'U', 'O', 'A', 'N')")
def silver_pid():
    """
    PID-3:  Patient Identifier List (3-1 = ID, 3-4 = Authority)
    PID-5:  Patient Name (5-1 = Family, 5-2 = Given, 5-3 = Middle)
    PID-7:  Date of Birth
    PID-8:  Administrative Sex
    PID-10: Race
    PID-11: Patient Address (11-1 = Street, 11-3 = City, 11-4 = State, 11-5 = ZIP)
    PID-13: Phone Number - Home
    PID-16: Marital Status
    PID-18: Patient Account Number
    """
    return (
        dlt.read_stream("silver_hl7_parsed")
        .filter(F.col("hl7.PID").isNotNull())
        .select(
            "message_id",
            F.col("hl7.PID")[0]["fields"][3][0][1][1].alias("patient_id"),
            F.col("hl7.PID")[0]["fields"][3][0][4][1].alias("patient_id_authority"),
            F.col("hl7.PID")[0]["fields"][5][0][1][1].alias("patient_name_family"),
            F.col("hl7.PID")[0]["fields"][5][0][2][1].alias("patient_name_given"),
            F.col("hl7.PID")[0]["fields"][5][0][3][1].alias("patient_name_middle"),
            F.col("hl7.PID")[0]["fields"][7][0][1][1].alias("date_of_birth"),
            F.col("hl7.PID")[0]["fields"][8][0][1][1].alias("sex"),
            F.col("hl7.PID")[0]["fields"][10][0][1][1].alias("race"),
            F.col("hl7.PID")[0]["fields"][11][0][1][1].alias("address_street"),
            F.col("hl7.PID")[0]["fields"][11][0][3][1].alias("address_city"),
            F.col("hl7.PID")[0]["fields"][11][0][4][1].alias("address_state"),
            F.col("hl7.PID")[0]["fields"][11][0][5][1].alias("address_zip"),
            F.col("hl7.PID")[0]["fields"][13][0][1][1].alias("phone_home"),
            F.col("hl7.PID")[0]["fields"][16][0][1][1].alias("marital_status"),
            F.col("hl7.PID")[0]["fields"][18][0][1][1].alias("patient_account_number"),
            "message_type",
            "sending_facility",
            "parsed_at"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_pv1 - Patient Visit

# COMMAND ----------

@dlt.table(
    name="silver_pv1",
    comment="Patient Visit (PV1) segments"
)
@dlt.expect("pv1_patient_class_present", "patient_class IS NOT NULL")
@dlt.expect("pv1_valid_patient_class", "patient_class IS NULL OR patient_class IN ('I', 'O', 'E', 'P', 'R', 'B', 'C', 'N', 'U')")
def silver_pv1():
    """
    PV1-2:  Patient Class
    PV1-3:  Assigned Patient Location (3-1 POC, 3-2 Room, 3-3 Bed, 3-4 Facility)
    PV1-4:  Admission Type
    PV1-7:  Attending Doctor (7-1 ID, 7-2 Family, 7-3 Given)
    PV1-10: Hospital Service
    PV1-14: Admit Source
    PV1-19: Visit Number
    PV1-44: Admit Date/Time
    PV1-45: Discharge Date/Time
    """
    return (
        dlt.read_stream("silver_hl7_parsed")
        .filter(F.col("hl7.PV1").isNotNull())
        .select(
            "message_id",
            "patient_id",
            F.col("hl7.PV1")[0]["fields"][2][0][1][1].alias("patient_class"),
            F.col("hl7.PV1")[0]["fields"][3][0][1][1].alias("location_point_of_care"),
            F.col("hl7.PV1")[0]["fields"][3][0][2][1].alias("location_room"),
            F.col("hl7.PV1")[0]["fields"][3][0][3][1].alias("location_bed"),
            F.col("hl7.PV1")[0]["fields"][3][0][4][1].alias("location_facility"),
            F.col("hl7.PV1")[0]["fields"][4][0][1][1].alias("admission_type"),
            F.col("hl7.PV1")[0]["fields"][7][0][1][1].alias("attending_doctor_id"),
            F.concat_ws(" ",
                F.col("hl7.PV1")[0]["fields"][7][0][3][1],
                F.col("hl7.PV1")[0]["fields"][7][0][2][1]
            ).alias("attending_doctor_name"),
            F.col("hl7.PV1")[0]["fields"][10][0][1][1].alias("hospital_service"),
            F.col("hl7.PV1")[0]["fields"][14][0][1][1].alias("admit_source"),
            F.col("hl7.PV1")[0]["fields"][19][0][1][1].alias("visit_number"),
            F.col("hl7.PV1")[0]["fields"][44][0][1][1].alias("admit_datetime"),
            F.col("hl7.PV1")[0]["fields"][45][0][1][1].alias("discharge_datetime"),
            "message_type",
            "trigger_event",
            "sending_facility",
            "parsed_at"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_evn - Event Type

# COMMAND ----------

@dlt.table(
    name="silver_evn",
    comment="Event Type (EVN) segments"
)
def silver_evn():
    """
    EVN-1: Event Type Code
    EVN-2: Recorded Date/Time
    EVN-3: Date/Time Planned Event
    EVN-4: Event Reason Code
    EVN-5: Operator ID
    EVN-6: Event Occurred
    """
    return (
        dlt.read_stream("silver_hl7_parsed")
        .filter(F.col("hl7.EVN").isNotNull())
        .select(
            "message_id",
            "patient_id",
            F.col("hl7.EVN")[0]["fields"][1][0][1][1].alias("event_type_code"),
            F.col("hl7.EVN")[0]["fields"][2][0][1][1].alias("recorded_datetime"),
            F.col("hl7.EVN")[0]["fields"][3][0][1][1].alias("planned_event_datetime"),
            F.col("hl7.EVN")[0]["fields"][4][0][1][1].alias("event_reason_code"),
            F.col("hl7.EVN")[0]["fields"][5][0][1][1].alias("operator_id"),
            F.col("hl7.EVN")[0]["fields"][6][0][1][1].alias("event_occurred"),
            "message_type",
            "trigger_event",
            "sending_facility",
            "parsed_at"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_obr - Observation Request

# COMMAND ----------

@dlt.table(
    name="silver_obr",
    comment="Observation Request (OBR) segments"
)
@dlt.expect("obr_order_number_present", "placer_order_number IS NOT NULL OR filler_order_number IS NOT NULL")
def silver_obr():
    """
    OBR-2:  Placer Order Number
    OBR-3:  Filler Order Number
    OBR-4:  Universal Service ID (4-1 ID, 4-2 Text, 4-3 Coding System)
    OBR-5:  Priority
    OBR-6:  Requested Date/Time
    OBR-7:  Observation Date/Time
    OBR-8:  Observation End Date/Time
    OBR-16: Ordering Provider (16-1 ID, 16-2 Family, 16-3 Given)
    OBR-25: Result Status
    """
    return (
        dlt.read_stream("silver_hl7_parsed")
        .filter(F.col("hl7.OBR").isNotNull())
        .filter(F.size(F.col("hl7.OBR")) > 0)
        .withColumn("obr_exploded", F.explode(F.col("hl7.OBR")))
        .select(
            "message_id",
            "patient_id",
            F.col("obr_exploded.index").alias("set_id"),
            F.col("obr_exploded.fields")[2][0][1][1].alias("placer_order_number"),
            F.col("obr_exploded.fields")[3][0][1][1].alias("filler_order_number"),
            F.col("obr_exploded.fields")[4][0][1][1].alias("universal_service_id"),
            F.col("obr_exploded.fields")[4][0][2][1].alias("universal_service_text"),
            F.col("obr_exploded.fields")[4][0][3][1].alias("universal_service_coding_system"),
            F.col("obr_exploded.fields")[5][0][1][1].alias("priority"),
            F.col("obr_exploded.fields")[6][0][1][1].alias("requested_datetime"),
            F.col("obr_exploded.fields")[7][0][1][1].alias("observation_datetime"),
            F.col("obr_exploded.fields")[8][0][1][1].alias("observation_end_datetime"),
            F.col("obr_exploded.fields")[16][0][1][1].alias("ordering_provider_id"),
            F.concat_ws(" ",
                F.col("obr_exploded.fields")[16][0][3][1],
                F.col("obr_exploded.fields")[16][0][2][1]
            ).alias("ordering_provider_name"),
            F.col("obr_exploded.fields")[25][0][1][1].alias("result_status"),
            "message_type",
            "sending_facility",
            "parsed_at"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_obx - Observation/Result

# COMMAND ----------

@dlt.table(
    name="silver_obx",
    comment="Observation/Result (OBX) segments"
)
@dlt.expect_or_drop("obx_observation_id_not_null", "observation_id IS NOT NULL AND observation_id != ''")
@dlt.expect("obx_value_type_present", "value_type IS NOT NULL")
def silver_obx():
    """
    OBX-2:  Value Type
    OBX-3:  Observation Identifier (3-1 ID, 3-2 Text, 3-3 Coding System)
    OBX-4:  Observation Sub-ID
    OBX-5:  Observation Value
    OBX-6:  Units
    OBX-7:  Reference Range
    OBX-8:  Abnormal Flags
    OBX-11: Observation Result Status
    OBX-14: Date/Time of Observation
    OBX-15: Producer's ID
    """
    return (
        dlt.read_stream("silver_hl7_parsed")
        .filter(F.col("hl7.OBX").isNotNull())
        .filter(F.size(F.col("hl7.OBX")) > 0)
        .withColumn("obx_exploded", F.explode(F.col("hl7.OBX")))
        .select(
            "message_id",
            "patient_id",
            F.col("obx_exploded.index").alias("set_id"),
            F.col("obx_exploded.fields")[2][0][1][1].alias("value_type"),
            F.col("obx_exploded.fields")[3][0][1][1].alias("observation_id"),
            F.col("obx_exploded.fields")[3][0][2][1].alias("observation_text"),
            F.col("obx_exploded.fields")[3][0][3][1].alias("observation_coding_system"),
            F.col("obx_exploded.fields")[4][0][1][1].alias("observation_sub_id"),
            F.col("obx_exploded.fields")[5][0][1][1].alias("observation_value"),
            F.col("obx_exploded.fields")[6][0][1][1].alias("units"),
            F.col("obx_exploded.fields")[7][0][1][1].alias("reference_range"),
            F.col("obx_exploded.fields")[8][0][1][1].alias("abnormal_flags"),
            F.col("obx_exploded.fields")[11][0][1][1].alias("result_status"),
            F.col("obx_exploded.fields")[14][0][1][1].alias("observation_datetime"),
            F.col("obx_exploded.fields")[15][0][1][1].alias("producer_id"),
            "message_type",
            "sending_facility",
            "parsed_at"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_dg1 - Diagnosis

# COMMAND ----------

@dlt.table(
    name="silver_dg1",
    comment="Diagnosis (DG1) segments"
)
@dlt.expect_or_drop("dg1_code_not_null", "diagnosis_code IS NOT NULL AND diagnosis_code != ''")
def silver_dg1():
    """
    DG1-2:  Diagnosis Coding Method
    DG1-3:  Diagnosis Code (3-1 Code, 3-2 Description, 3-3 Coding System)
    DG1-5:  Diagnosis Date/Time
    DG1-6:  Diagnosis Type
    DG1-15: Diagnosis Priority
    """
    return (
        dlt.read_stream("silver_hl7_parsed")
        .filter(F.col("hl7.DG1").isNotNull())
        .filter(F.size(F.col("hl7.DG1")) > 0)
        .withColumn("dg1_exploded", F.explode(F.col("hl7.DG1")))
        .select(
            "message_id",
            "patient_id",
            F.col("dg1_exploded.index").alias("set_id"),
            F.col("dg1_exploded.fields")[2][0][1][1].alias("diagnosis_coding_method"),
            F.col("dg1_exploded.fields")[3][0][1][1].alias("diagnosis_code"),
            F.col("dg1_exploded.fields")[3][0][2][1].alias("diagnosis_description"),
            F.col("dg1_exploded.fields")[3][0][3][1].alias("diagnosis_coding_system"),
            F.col("dg1_exploded.fields")[5][0][1][1].alias("diagnosis_datetime"),
            F.col("dg1_exploded.fields")[6][0][1][1].alias("diagnosis_type"),
            F.col("dg1_exploded.fields")[15][0][1][1].alias("diagnosis_priority"),
            "message_type",
            "sending_facility",
            "parsed_at"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_al1 - Allergy

# COMMAND ----------

@dlt.table(
    name="silver_al1",
    comment="Allergy (AL1) segments"
)
def silver_al1():
    """
    AL1-2: Allergen Type Code (2-1 Code, 2-2 Text)
    AL1-3: Allergen Code (3-1 Code, 3-2 Description)
    AL1-4: Allergy Severity Code (4-1 Code, 4-2 Text)
    AL1-5: Allergy Reaction Code
    AL1-6: Identification Date
    """
    return (
        dlt.read_stream("silver_hl7_parsed")
        .filter(F.col("hl7.AL1").isNotNull())
        .filter(F.size(F.col("hl7.AL1")) > 0)
        .withColumn("al1_exploded", F.explode(F.col("hl7.AL1")))
        .select(
            "message_id",
            "patient_id",
            F.col("al1_exploded.index").alias("set_id"),
            F.col("al1_exploded.fields")[2][0][1][1].alias("allergen_type_code"),
            F.col("al1_exploded.fields")[2][0][2][1].alias("allergen_type_text"),
            F.col("al1_exploded.fields")[3][0][1][1].alias("allergen_code"),
            F.col("al1_exploded.fields")[3][0][2][1].alias("allergen_description"),
            F.col("al1_exploded.fields")[4][0][1][1].alias("severity_code"),
            F.col("al1_exploded.fields")[4][0][2][1].alias("severity_text"),
            F.col("al1_exploded.fields")[5][0][1][1].alias("reaction"),
            F.col("al1_exploded.fields")[6][0][1][1].alias("identification_date"),
            "message_type",
            "sending_facility",
            "parsed_at"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_in1 - Insurance

# COMMAND ----------

@dlt.table(
    name="silver_in1",
    comment="Insurance (IN1) segments"
)
def silver_in1():
    """
    IN1-2:  Insurance Plan ID
    IN1-3:  Insurance Company ID
    IN1-4:  Insurance Company Name
    IN1-8:  Group Number
    IN1-9:  Group Name
    IN1-12: Plan Effective Date
    IN1-13: Plan Expiration Date
    IN1-15: Plan Type
    IN1-16: Name of Insured (16-1 Family, 16-2 Given)
    IN1-17: Insured's Relationship to Patient
    IN1-36: Policy Number
    """
    return (
        dlt.read_stream("silver_hl7_parsed")
        .filter(F.col("hl7.IN1").isNotNull())
        .filter(F.size(F.col("hl7.IN1")) > 0)
        .withColumn("in1_exploded", F.explode(F.col("hl7.IN1")))
        .select(
            "message_id",
            "patient_id",
            F.col("in1_exploded.index").alias("set_id"),
            F.col("in1_exploded.fields")[2][0][1][1].alias("insurance_plan_id"),
            F.col("in1_exploded.fields")[3][0][1][1].alias("insurance_company_id"),
            F.col("in1_exploded.fields")[4][0][1][1].alias("insurance_company_name"),
            F.col("in1_exploded.fields")[8][0][1][1].alias("group_number"),
            F.col("in1_exploded.fields")[9][0][1][1].alias("group_name"),
            F.col("in1_exploded.fields")[12][0][1][1].alias("plan_effective_date"),
            F.col("in1_exploded.fields")[13][0][1][1].alias("plan_expiration_date"),
            F.col("in1_exploded.fields")[15][0][1][1].alias("plan_type"),
            F.col("in1_exploded.fields")[16][0][1][1].alias("insured_name_family"),
            F.col("in1_exploded.fields")[16][0][2][1].alias("insured_name_given"),
            F.col("in1_exploded.fields")[17][0][1][1].alias("insured_relationship"),
            F.col("in1_exploded.fields")[36][0][1][1].alias("policy_number"),
            "message_type",
            "sending_facility",
            "parsed_at"
        )
    )
