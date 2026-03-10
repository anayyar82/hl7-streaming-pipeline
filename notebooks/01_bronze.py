# Databricks notebook source
import subprocess, sys, os

bundle_files_path = spark.conf.get("hl7.bundle_files_path")
wheel_name = spark.conf.get("hl7.funke_wheel_name", "funke-0.1.0a1-py3-none-any.whl")
wheel_path = os.path.join(bundle_files_path, "libraries", wheel_name)
subprocess.check_call([sys.executable, "-m", "pip", "install", wheel_path, "-q"])

# COMMAND ----------

# MAGIC %md
# MAGIC # HL7 Streaming Pipeline - Bronze Layer
# MAGIC 
# MAGIC Raw HL7 message ingestion, splitting, and parsing using the **funke** library.
# MAGIC 
# MAGIC ## Tables
# MAGIC | Table | Description |
# MAGIC |-------|-------------|
# MAGIC | `bronze_hl7_raw` | Whole files ingested via Auto Loader (one row per file) |
# MAGIC | `bronze_hl7_split` | Individual HL7 messages split from multi-message files |
# MAGIC | `bronze_hl7_parsed` | Messages parsed into funke's nested structure |
# MAGIC 
# MAGIC ## Why Split?
# MAGIC Real-world HL7 feeds often deliver **multiple messages per file**.
# MAGIC Messages are separated by MLLP framing characters (`\x0b`, `\x1c\x0d`),
# MAGIC blank lines, or simply consecutive `MSH` segments.
# MAGIC 
# MAGIC Keeping ingestion and splitting as separate steps means:
# MAGIC - Auto Loader handles file discovery (no custom code)
# MAGIC - The splitter handles any message-per-file ratio
# MAGIC - funke only ever sees a single well-formed message
# MAGIC - Each step is independently testable and restartable

# COMMAND ----------

import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, StringType

from funke.parsing.functions import parse_hl7v2_msg
from funke.parsing.hl7 import HL7v2Schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC All paths are resolved dynamically from pipeline configuration.

# COMMAND ----------

catalog = spark.conf.get("hl7.catalog")
volume = spark.conf.get("hl7.source_volume", "landing")

schema = spark.conf.get("hl7.schema")

SOURCE_PATH = f"/Volumes/{catalog}/{schema}/{volume}"
CHECKPOINT_PATH = f"{SOURCE_PATH}/_checkpoints"

hl7v2_schema = HL7v2Schema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## HL7 Message Splitter UDF
# MAGIC 
# MAGIC Splits a file that may contain multiple HL7 messages into individual messages.
# MAGIC 
# MAGIC Handles common real-world formats:
# MAGIC - **MLLP framing**: Messages wrapped in `\x0b` (VT) ... `\x1c\x0d` (FS+CR)
# MAGIC - **Blank-line separated**: Messages separated by one or more empty lines
# MAGIC - **Consecutive MSH**: Multiple `MSH|` segments back-to-back with no separator
# MAGIC - **Single message per file**: Passed through as-is

# COMMAND ----------

@F.udf(returnType=ArrayType(StringType()))
def split_hl7_messages(raw_content):
    """
    Split raw file content into individual HL7 messages.
    
    Supports:
    - MLLP framing (VT/FS characters)
    - Blank-line separation
    - Consecutive MSH segment detection
    - Single message pass-through
    """
    if not raw_content:
        return []
    
    # Strip MLLP framing characters: VT (\x0b) and FS+CR (\x1c\x0d)
    content = raw_content.replace("\x0b", "").replace("\x1c\x0d", "\n").replace("\x1c", "\n")
    
    # Normalize line endings
    content = content.replace("\r\n", "\n").replace("\r", "\n")
    
    lines = content.split("\n")
    
    messages = []
    current_message_lines = []
    
    for line in lines:
        stripped = line.strip()
        
        if not stripped:
            # Blank line: if we have accumulated lines, close the current message
            if current_message_lines:
                messages.append("\r".join(current_message_lines))
                current_message_lines = []
            continue
        
        if stripped.startswith("MSH|") and current_message_lines:
            # New MSH while we already have a message in progress → close previous
            messages.append("\r".join(current_message_lines))
            current_message_lines = [stripped]
        else:
            current_message_lines.append(stripped)
    
    # Don't forget the last message
    if current_message_lines:
        messages.append("\r".join(current_message_lines))
    
    # Filter out anything that doesn't look like a valid HL7 message
    return [m for m in messages if m.startswith("MSH|")]

# COMMAND ----------

# MAGIC %md
# MAGIC ## bronze_hl7_raw
# MAGIC Ingest raw HL7 files from cloud storage using Auto Loader.
# MAGIC Each row = one file (may contain multiple HL7 messages).

# COMMAND ----------

@dlt.table(
    name="bronze_hl7_raw",
    comment="Raw HL7 files from streaming source (one row per file)",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_hl7_raw():
    """Ingest raw HL7 files from cloud storage using Auto Loader."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("wholetext", "true")
        .load(SOURCE_PATH)
        .withColumn("file_id", F.md5(F.concat(F.col("value"), F.col("_metadata.file_modification_time").cast("string"))))
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("file_modification_time", F.col("_metadata.file_modification_time"))
        .withColumn("ingested_at", F.current_timestamp())
        .select(
            "file_id",
            F.col("value").alias("raw_content"),
            "source_file",
            "file_modification_time",
            "ingested_at"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## bronze_hl7_split
# MAGIC Split multi-message files into individual HL7 messages.
# MAGIC Each row = one HL7 message, ready for funke parsing.
# MAGIC 
# MAGIC This is the key step that enables parallel processing:
# MAGIC - A file with 500 messages becomes 500 rows
# MAGIC - Spark distributes the rows across workers
# MAGIC - funke parses each message independently

# COMMAND ----------

@dlt.table(
    name="bronze_hl7_split",
    comment="Individual HL7 messages split from multi-message files",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_drop("valid_hl7_start", "raw_message LIKE 'MSH|%'")
def bronze_hl7_split():
    """Split multi-message files into individual HL7 messages."""
    return (
        dlt.read_stream("bronze_hl7_raw")
        .withColumn("messages", split_hl7_messages(F.col("raw_content")))
        .withColumn("file_message_count", F.size(F.col("messages")))
        .withColumn("raw_message", F.explode(F.col("messages")))
        .withColumn("message_id", F.md5(F.col("raw_message")))
        .select(
            "message_id",
            "raw_message",
            "file_id",
            "source_file",
            "file_message_count",
            "file_modification_time",
            "ingested_at"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## bronze_hl7_parsed
# MAGIC Parse each individual HL7 message using funke's `parse_hl7v2_msg`.
# MAGIC 
# MAGIC The parsed `hl7` column contains a nested map structure:
# MAGIC ```
# MAGIC hl7 -> Map[String, Array[Struct(index, segment, fields)]]
# MAGIC ```
# MAGIC 
# MAGIC Access pattern: `hl7.SEGMENT[segment_rep].fields[field_num][field_rep][component][subcomponent]`

# COMMAND ----------

@dlt.table(
    name="bronze_hl7_parsed",
    comment="HL7 messages parsed using funke library",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_hl7_parsed():
    """Parse individual HL7 messages using funke's parse_hl7v2_msg function."""
    return (
        dlt.read_stream("bronze_hl7_split")
        .withColumn("hl7", parse_hl7v2_msg(hl7v2_schema)(F.col("raw_message")))
        .withColumn("parsed_at", F.current_timestamp())
    )
