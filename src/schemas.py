"""
HL7 Schema Definitions for Delta Live Tables

Defines PySpark schemas for HL7 segments and message types.
Used for type-safe processing in the streaming pipeline.

Note: This pipeline uses the funke library for HL7v2 parsing:
    from funke.parsing.functions import parse_hl7v2_msg
    from funke.parsing.hl7 import HL7v2Schema

The funke library provides automatic schema inference and parsing.
These schema definitions are provided for reference and can be used
for custom transformations or validation.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    ArrayType,
    MapType,
    BooleanType,
    DoubleType,
)


class HL7Schemas:
    """Schema definitions for HL7 segments."""
    
    @staticmethod
    def get_raw_message_schema() -> StructType:
        """Schema for raw HL7 messages from streaming source."""
        return StructType([
            StructField("message_id", StringType(), False),
            StructField("raw_message", StringType(), False),
            StructField("source_system", StringType(), True),
            StructField("received_at", TimestampType(), False),
            StructField("file_path", StringType(), True),
            StructField("file_modification_time", TimestampType(), True),
        ])
    
    @staticmethod
    def get_msh_schema() -> StructType:
        """Schema for MSH (Message Header) segment."""
        return StructType([
            StructField("message_id", StringType(), False),
            StructField("encoding_characters", StringType(), True),
            StructField("sending_application", StringType(), True),
            StructField("sending_facility", StringType(), True),
            StructField("receiving_application", StringType(), True),
            StructField("receiving_facility", StringType(), True),
            StructField("message_datetime", TimestampType(), True),
            StructField("security", StringType(), True),
            StructField("message_type", StringType(), True),
            StructField("trigger_event", StringType(), True),
            StructField("message_type_full", StringType(), True),
            StructField("message_control_id", StringType(), True),
            StructField("processing_id", StringType(), True),
            StructField("version_id", StringType(), True),
            StructField("processed_at", TimestampType(), False),
        ])
    
    @staticmethod
    def get_pid_schema() -> StructType:
        """Schema for PID (Patient Identification) segment."""
        return StructType([
            StructField("message_id", StringType(), False),
            StructField("patient_id", StringType(), True),
            StructField("patient_id_authority", StringType(), True),
            StructField("patient_id_external", StringType(), True),
            StructField("alternate_patient_id", StringType(), True),
            StructField("patient_name_family", StringType(), True),
            StructField("patient_name_given", StringType(), True),
            StructField("patient_name_middle", StringType(), True),
            StructField("patient_name_suffix", StringType(), True),
            StructField("date_of_birth", TimestampType(), True),
            StructField("sex", StringType(), True),
            StructField("race", StringType(), True),
            StructField("address_street", StringType(), True),
            StructField("address_city", StringType(), True),
            StructField("address_state", StringType(), True),
            StructField("address_zip", StringType(), True),
            StructField("address_country", StringType(), True),
            StructField("phone_home", StringType(), True),
            StructField("phone_business", StringType(), True),
            StructField("language", StringType(), True),
            StructField("marital_status", StringType(), True),
            StructField("religion", StringType(), True),
            StructField("patient_account_number", StringType(), True),
            StructField("ssn", StringType(), True),
            StructField("ethnicity", StringType(), True),
            StructField("death_indicator", StringType(), True),
            StructField("processed_at", TimestampType(), False),
        ])
    
    @staticmethod
    def get_pv1_schema() -> StructType:
        """Schema for PV1 (Patient Visit) segment."""
        return StructType([
            StructField("message_id", StringType(), False),
            StructField("patient_id", StringType(), True),
            StructField("set_id", StringType(), True),
            StructField("patient_class", StringType(), True),
            StructField("assigned_location_point_of_care", StringType(), True),
            StructField("assigned_location_room", StringType(), True),
            StructField("assigned_location_bed", StringType(), True),
            StructField("assigned_location_facility", StringType(), True),
            StructField("admission_type", StringType(), True),
            StructField("preadmit_number", StringType(), True),
            StructField("prior_location", StringType(), True),
            StructField("attending_doctor_id", StringType(), True),
            StructField("attending_doctor_family", StringType(), True),
            StructField("attending_doctor_given", StringType(), True),
            StructField("hospital_service", StringType(), True),
            StructField("admit_source", StringType(), True),
            StructField("ambulatory_status", StringType(), True),
            StructField("vip_indicator", StringType(), True),
            StructField("patient_type", StringType(), True),
            StructField("visit_number", StringType(), True),
            StructField("financial_class", StringType(), True),
            StructField("discharge_disposition", StringType(), True),
            StructField("admit_datetime", TimestampType(), True),
            StructField("discharge_datetime", TimestampType(), True),
            StructField("processed_at", TimestampType(), False),
        ])
    
    @staticmethod
    def get_obx_schema() -> StructType:
        """Schema for OBX (Observation/Result) segment."""
        return StructType([
            StructField("message_id", StringType(), False),
            StructField("patient_id", StringType(), True),
            StructField("set_id", StringType(), True),
            StructField("value_type", StringType(), True),
            StructField("observation_id", StringType(), True),
            StructField("observation_text", StringType(), True),
            StructField("observation_coding_system", StringType(), True),
            StructField("observation_sub_id", StringType(), True),
            StructField("observation_value", StringType(), True),
            StructField("observation_value_numeric", DoubleType(), True),
            StructField("units", StringType(), True),
            StructField("reference_range", StringType(), True),
            StructField("abnormal_flags", StringType(), True),
            StructField("observation_result_status", StringType(), True),
            StructField("observation_datetime", TimestampType(), True),
            StructField("producer_id", StringType(), True),
            StructField("responsible_observer", StringType(), True),
            StructField("processed_at", TimestampType(), False),
        ])
    
    @staticmethod
    def get_dg1_schema() -> StructType:
        """Schema for DG1 (Diagnosis) segment."""
        return StructType([
            StructField("message_id", StringType(), False),
            StructField("patient_id", StringType(), True),
            StructField("set_id", StringType(), True),
            StructField("diagnosis_coding_method", StringType(), True),
            StructField("diagnosis_code", StringType(), True),
            StructField("diagnosis_description", StringType(), True),
            StructField("diagnosis_coding_system", StringType(), True),
            StructField("diagnosis_datetime", TimestampType(), True),
            StructField("diagnosis_type", StringType(), True),
            StructField("major_diagnostic_category", StringType(), True),
            StructField("diagnostic_related_group", StringType(), True),
            StructField("diagnosis_priority", StringType(), True),
            StructField("diagnosing_clinician", StringType(), True),
            StructField("processed_at", TimestampType(), False),
        ])
    
    @staticmethod
    def get_in1_schema() -> StructType:
        """Schema for IN1 (Insurance) segment."""
        return StructType([
            StructField("message_id", StringType(), False),
            StructField("patient_id", StringType(), True),
            StructField("set_id", StringType(), True),
            StructField("insurance_plan_id", StringType(), True),
            StructField("insurance_company_id", StringType(), True),
            StructField("insurance_company_name", StringType(), True),
            StructField("group_number", StringType(), True),
            StructField("group_name", StringType(), True),
            StructField("plan_effective_date", TimestampType(), True),
            StructField("plan_expiration_date", TimestampType(), True),
            StructField("plan_type", StringType(), True),
            StructField("insured_name", StringType(), True),
            StructField("insured_relationship", StringType(), True),
            StructField("policy_number", StringType(), True),
            StructField("processed_at", TimestampType(), False),
        ])
    
    @staticmethod
    def get_patient_dim_schema() -> StructType:
        """Schema for Patient dimension (Gold layer)."""
        return StructType([
            StructField("patient_key", StringType(), False),
            StructField("patient_id", StringType(), False),
            StructField("patient_id_authority", StringType(), True),
            StructField("patient_name_full", StringType(), True),
            StructField("patient_name_family", StringType(), True),
            StructField("patient_name_given", StringType(), True),
            StructField("date_of_birth", TimestampType(), True),
            StructField("age_years", IntegerType(), True),
            StructField("sex", StringType(), True),
            StructField("race", StringType(), True),
            StructField("ethnicity", StringType(), True),
            StructField("address_full", StringType(), True),
            StructField("address_city", StringType(), True),
            StructField("address_state", StringType(), True),
            StructField("address_zip", StringType(), True),
            StructField("phone_primary", StringType(), True),
            StructField("language", StringType(), True),
            StructField("marital_status", StringType(), True),
            StructField("death_indicator", BooleanType(), True),
            StructField("first_seen_at", TimestampType(), True),
            StructField("last_updated_at", TimestampType(), True),
            StructField("source_system", StringType(), True),
        ])
    
    @staticmethod
    def get_encounter_fact_schema() -> StructType:
        """Schema for Encounter fact table (Gold layer)."""
        return StructType([
            StructField("encounter_key", StringType(), False),
            StructField("patient_key", StringType(), False),
            StructField("patient_id", StringType(), True),
            StructField("visit_number", StringType(), True),
            StructField("patient_class", StringType(), True),
            StructField("admission_type", StringType(), True),
            StructField("admit_source", StringType(), True),
            StructField("location_facility", StringType(), True),
            StructField("location_point_of_care", StringType(), True),
            StructField("location_room", StringType(), True),
            StructField("location_bed", StringType(), True),
            StructField("hospital_service", StringType(), True),
            StructField("attending_doctor_id", StringType(), True),
            StructField("attending_doctor_name", StringType(), True),
            StructField("admit_datetime", TimestampType(), True),
            StructField("discharge_datetime", TimestampType(), True),
            StructField("length_of_stay_hours", DoubleType(), True),
            StructField("discharge_disposition", StringType(), True),
            StructField("financial_class", StringType(), True),
            StructField("message_type", StringType(), True),
            StructField("source_message_id", StringType(), True),
            StructField("created_at", TimestampType(), True),
        ])
    
    @staticmethod
    def get_observation_fact_schema() -> StructType:
        """Schema for Observation fact table (Gold layer)."""
        return StructType([
            StructField("observation_key", StringType(), False),
            StructField("patient_key", StringType(), False),
            StructField("encounter_key", StringType(), True),
            StructField("patient_id", StringType(), True),
            StructField("observation_id", StringType(), True),
            StructField("observation_code", StringType(), True),
            StructField("observation_text", StringType(), True),
            StructField("observation_coding_system", StringType(), True),
            StructField("value_type", StringType(), True),
            StructField("observation_value_text", StringType(), True),
            StructField("observation_value_numeric", DoubleType(), True),
            StructField("units", StringType(), True),
            StructField("reference_range_low", DoubleType(), True),
            StructField("reference_range_high", DoubleType(), True),
            StructField("abnormal_flag", StringType(), True),
            StructField("is_abnormal", BooleanType(), True),
            StructField("result_status", StringType(), True),
            StructField("observation_datetime", TimestampType(), True),
            StructField("producer_id", StringType(), True),
            StructField("source_message_id", StringType(), True),
            StructField("created_at", TimestampType(), True),
        ])
