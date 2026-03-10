"""
HL7 Data Transformations

Transformation functions for converting parsed HL7 data between pipeline layers.
Implements the Bronze -> Silver -> Gold medallion architecture transformations.
"""

from typing import Dict, Any, Optional, List
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, BooleanType, TimestampType
import hashlib
import uuid


class HL7Transformations:
    """Transformation utilities for HL7 data processing."""
    
    @staticmethod
    def generate_message_id(raw_message: str, timestamp: str) -> str:
        """Generate a unique message ID based on content hash."""
        content = f"{raw_message}{timestamp}"
        return hashlib.sha256(content.encode()).hexdigest()[:32]
    
    @staticmethod
    def generate_surrogate_key(*args) -> str:
        """Generate a surrogate key from multiple fields."""
        content = "|".join(str(arg) for arg in args if arg)
        if content:
            return hashlib.md5(content.encode()).hexdigest()
        return str(uuid.uuid4()).replace("-", "")
    
    @staticmethod
    def clean_patient_id(patient_id: Optional[str]) -> Optional[str]:
        """Standardize patient ID format."""
        if not patient_id:
            return None
        return patient_id.strip().upper()
    
    @staticmethod
    def parse_reference_range(ref_range: Optional[str]) -> Dict[str, Optional[float]]:
        """Parse reference range string into low/high values."""
        result = {"low": None, "high": None}
        
        if not ref_range:
            return result
        
        ref_range = ref_range.strip()
        
        if "-" in ref_range and not ref_range.startswith("-"):
            parts = ref_range.split("-")
            if len(parts) == 2:
                try:
                    result["low"] = float(parts[0].strip())
                    result["high"] = float(parts[1].strip())
                except ValueError:
                    pass
        elif ref_range.startswith("<"):
            try:
                result["high"] = float(ref_range[1:].strip())
            except ValueError:
                pass
        elif ref_range.startswith(">"):
            try:
                result["low"] = float(ref_range[1:].strip())
            except ValueError:
                pass
        
        return result
    
    @staticmethod
    def try_parse_numeric(value: Optional[str]) -> Optional[float]:
        """Attempt to parse a string value as numeric."""
        if not value:
            return None
        
        try:
            cleaned = "".join(c for c in value if c.isdigit() or c in ".-")
            return float(cleaned)
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def calculate_age(dob: Optional[datetime], reference_date: Optional[datetime] = None) -> Optional[int]:
        """Calculate age in years from date of birth."""
        if not dob:
            return None
        
        ref = reference_date or datetime.utcnow()
        age = ref.year - dob.year
        
        if (ref.month, ref.day) < (dob.month, dob.day):
            age -= 1
        
        return age if age >= 0 else None
    
    @staticmethod
    def format_full_name(family: Optional[str], given: Optional[str], middle: Optional[str] = None) -> Optional[str]:
        """Format a full name from components."""
        parts = [p for p in [given, middle, family] if p]
        return " ".join(parts) if parts else None
    
    @staticmethod
    def format_full_address(
        street: Optional[str],
        city: Optional[str],
        state: Optional[str],
        zip_code: Optional[str],
        country: Optional[str] = None
    ) -> Optional[str]:
        """Format a full address from components."""
        parts = []
        if street:
            parts.append(street)
        city_state_zip = ", ".join(filter(None, [city, state, zip_code]))
        if city_state_zip:
            parts.append(city_state_zip)
        if country and country.upper() != "USA":
            parts.append(country)
        return ", ".join(parts) if parts else None
    
    @staticmethod
    def is_abnormal_result(abnormal_flags: Optional[str]) -> bool:
        """Determine if observation result is abnormal based on flags."""
        if not abnormal_flags:
            return False
        
        abnormal_indicators = ["H", "HH", "L", "LL", "A", "AA", "C", "CR"]
        return abnormal_flags.upper() in abnormal_indicators
    
    @staticmethod
    def calculate_length_of_stay(
        admit_datetime: Optional[datetime],
        discharge_datetime: Optional[datetime]
    ) -> Optional[float]:
        """Calculate length of stay in hours."""
        if not admit_datetime or not discharge_datetime:
            return None
        
        delta = discharge_datetime - admit_datetime
        return delta.total_seconds() / 3600
    
    @staticmethod
    def standardize_sex(sex: Optional[str]) -> Optional[str]:
        """Standardize sex/gender values."""
        if not sex:
            return None
        
        sex_map = {
            "M": "Male",
            "F": "Female",
            "U": "Unknown",
            "O": "Other",
            "A": "Ambiguous",
            "N": "Not Applicable",
        }
        
        return sex_map.get(sex.upper(), sex)
    
    @staticmethod
    def standardize_patient_class(patient_class: Optional[str]) -> Optional[str]:
        """Standardize patient class values."""
        if not patient_class:
            return None
        
        class_map = {
            "I": "Inpatient",
            "O": "Outpatient",
            "E": "Emergency",
            "P": "Preadmit",
            "R": "Recurring Patient",
            "B": "Observation",
            "C": "Commercial Account",
            "N": "Not Applicable",
            "U": "Unknown",
        }
        
        return class_map.get(patient_class.upper(), patient_class)


def add_processing_metadata(df: DataFrame) -> DataFrame:
    """Add standard processing metadata columns to a DataFrame."""
    return df.withColumn(
        "processed_at", F.current_timestamp()
    ).withColumn(
        "processing_date", F.current_date()
    ).withColumn(
        "pipeline_id", F.lit(str(uuid.uuid4())[:8])
    )


def add_surrogate_key(df: DataFrame, key_column: str, *source_columns: str) -> DataFrame:
    """Add a surrogate key column based on source columns."""
    key_expr = F.md5(F.concat_ws("|", *[F.coalesce(F.col(c), F.lit("")) for c in source_columns]))
    return df.withColumn(key_column, key_expr)


def deduplicate_by_key(df: DataFrame, key_columns: List[str], order_column: str, ascending: bool = False) -> DataFrame:
    """Deduplicate DataFrame keeping the latest record per key."""
    from pyspark.sql.window import Window
    
    window = Window.partitionBy(*key_columns).orderBy(
        F.col(order_column).asc() if ascending else F.col(order_column).desc()
    )
    
    return df.withColumn(
        "_row_num", F.row_number().over(window)
    ).filter(
        F.col("_row_num") == 1
    ).drop("_row_num")


def explode_repeated_segments(df: DataFrame, segment_column: str, output_alias: str) -> DataFrame:
    """Explode repeated HL7 segments (like OBX, DG1) into separate rows."""
    return df.withColumn(
        output_alias,
        F.explode_outer(F.col(segment_column))
    )


def create_udf_functions(spark: SparkSession):
    """Register UDFs for use in Spark SQL and DataFrames."""
    
    @F.udf(returnType=StringType())
    def generate_key_udf(*args):
        return HL7Transformations.generate_surrogate_key(*args)
    
    @F.udf(returnType=DoubleType())
    def try_numeric_udf(value):
        return HL7Transformations.try_parse_numeric(value)
    
    @F.udf(returnType=BooleanType())
    def is_abnormal_udf(flags):
        return HL7Transformations.is_abnormal_result(flags)
    
    @F.udf(returnType=StringType())
    def format_name_udf(family, given, middle):
        return HL7Transformations.format_full_name(family, given, middle)
    
    @F.udf(returnType=StringType())
    def standardize_sex_udf(sex):
        return HL7Transformations.standardize_sex(sex)
    
    @F.udf(returnType=StringType())
    def standardize_patient_class_udf(patient_class):
        return HL7Transformations.standardize_patient_class(patient_class)
    
    spark.udf.register("generate_key", generate_key_udf)
    spark.udf.register("try_numeric", try_numeric_udf)
    spark.udf.register("is_abnormal", is_abnormal_udf)
    spark.udf.register("format_name", format_name_udf)
    spark.udf.register("standardize_sex", standardize_sex_udf)
    spark.udf.register("standardize_patient_class", standardize_patient_class_udf)
