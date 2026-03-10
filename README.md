# HL7 Streaming Pipeline with Databricks Delta Live Tables

A production-ready HL7 v2.x streaming pipeline built on Databricks using Delta Live Tables (DLT) for declarative data processing.

**Powered by [funke](https://github.com/databricks-field-eng/funke)** - Databricks Field Engineering library for HL7v2 message parsing.

## Architecture

This pipeline follows the **Medallion Architecture**:

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Source    │───▶│   Bronze    │───▶│   Silver    │───▶│    Gold     │
│  (HL7 Raw)  │    │ (Raw JSON)  │    │ (Cleaned)   │    │  (Analytics)│
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

### Layers

- **Bronze**: Raw HL7 messages ingested from streaming source (Kafka, Event Hubs, or cloud storage)
- **Silver**: Parsed and validated HL7 segments (MSH, PID, PV1, OBX, etc.)
- **Gold**: Business-level aggregations and analytics-ready tables

## Supported HL7 Message Types

- **ADT** (Admit/Discharge/Transfer): A01, A02, A03, A04, A08, etc.
- **ORU** (Observation Results): R01
- **ORM** (Orders): O01
- **SIU** (Scheduling): S12, S14, S15

## Project Structure

```
HL7 Streaming/
├── README.md                         # This file
├── requirements.txt                  # Python dependencies
├── notebooks/
│   ├── hl7_dlt_pipeline.py          # Pipeline overview & documentation
│   ├── 01_bronze.py                 # Bronze layer - raw ingestion + funke parsing
│   ├── 02_silver.py                 # Silver layer - segment extraction & validation
│   └── 03_gold.py                   # Gold layer - analytics tables & metrics
├── src/
│   ├── __init__.py
│   ├── hl7_parser.py                # HL7 message parsing utilities
│   ├── transformations.py           # Data transformation functions
│   ├── schemas.py                   # Schema definitions for HL7 segments
│   └── quality_rules.py             # Data quality expectations
├── config/
│   ├── pipeline_config.json         # Pipeline configuration
│   └── hl7_mappings.json            # HL7 field mappings
├── tests/
│   ├── __init__.py
│   ├── test_hl7_parser.py
│   └── sample_messages/
│       └── sample_adt_a01.hl7       # Sample HL7 messages for testing
└── deploy/
    └── dlt_pipeline_settings.json   # DLT deployment settings
```

## Setup Instructions

### 1. Prerequisites

- Databricks workspace with Unity Catalog enabled
- Databricks Runtime 13.0+ with Delta Live Tables
- Access to streaming source (Kafka, Event Hubs, or ADLS/S3)
- **funke** library installed on your Databricks cluster

### 2. Install funke Library

Install the funke library on your Databricks cluster:

```python
# Option 1: Install via pip in a notebook cell
%pip install git+https://github.com/databricks-field-eng/funke.git

# Option 2: Add to cluster libraries via Databricks UI
# Go to Cluster > Libraries > Install New > PyPI > funke
```

Or add to your `requirements.txt` for cluster initialization:
```
git+https://github.com/databricks-field-eng/funke.git
```

### 3. Configure the Pipeline

Edit `config/pipeline_config.json` to set your:
- Source connection details
- Target catalog and schema
- Checkpoint locations

### 4. Deploy the DLT Pipeline

1. Upload the project to your Databricks workspace
2. Create a new DLT pipeline using `deploy/dlt_pipeline_settings.json`
3. Start the pipeline in development or production mode

## Usage

### Running the Pipeline

```python
# In Databricks, the pipeline runs automatically via DLT
# The funke library provides native HL7v2 parsing:

from funke.parsing.functions import parse_hl7v2_msg
from funke.parsing.hl7 import HL7v2Schema

# Parse a single message
raw_message = open("tests/sample_messages/sample_adt_a01.hl7").read()

# In Spark DataFrame context:
df = spark.createDataFrame([(raw_message,)], ["raw_message"])
parsed_df = df.withColumn("parsed", parse_hl7v2_msg(F.col("raw_message")))

# Access parsed segments
parsed_df.select(
    "parsed.MSH.message_type.message_code",
    "parsed.PID.patient_identifier_list[0].id_number",
    "parsed.PV1.patient_class.identifier"
).show()
```

### funke Library Key Functions

| Function | Description |
|----------|-------------|
| `parse_hl7v2_msg(schema)` | Returns a UDF that parses raw HL7v2 messages |
| `HL7v2Schema` | Provides schema definitions for HL7v2 segments |

### funke Field Access Pattern

The funke library uses a nested structure for accessing HL7 fields:

```
hl7.SEGMENT[segment_rep].fields[field_num][field_rep][component][subcomponent]
```

**Examples:**

| HL7 Field | funke Access Pattern | Description |
|-----------|---------------------|-------------|
| MSH-9-1 | `hl7.MSH[0].fields[9][0][1][1]` | Message Type |
| MSH-9-2 | `hl7.MSH[0].fields[9][0][2][1]` | Trigger Event |
| MSH-4-1 | `hl7.MSH[0].fields[4][0][1][1]` | Sending Facility |
| MSH-10-1 | `hl7.MSH[0].fields[10][0][1][1]` | Message Control ID |
| PID-3-1 | `hl7.PID[0].fields[3][0][1][1]` | Patient ID |
| PID-5-1 | `hl7.PID[0].fields[5][0][1][1]` | Patient Last Name |
| PID-5-2 | `hl7.PID[0].fields[5][0][2][1]` | Patient First Name |
| PID-7-1 | `hl7.PID[0].fields[7][0][1][1]` | Date of Birth |
| PID-8-1 | `hl7.PID[0].fields[8][0][1][1]` | Gender |
| PV1-2-1 | `hl7.PV1[0].fields[2][0][1][1]` | Patient Class |
| PV1-3-1 | `hl7.PV1[0].fields[3][0][1][1]` | Location (Point of Care) |

**Index Reference:**
- `SEGMENT[n]` - Segment repetition (0-indexed)
- `fields[n]` - Field number (1-indexed per HL7 standard)
- `[n]` - Field repetition (0-indexed)
- `[n]` - Component (1-indexed)
- `[n]` - Subcomponent (1-indexed)

### Data Quality

The pipeline includes built-in data quality checks:
- Required field validation (patient ID, message type)
- Format validation (dates, identifiers)
- Referential integrity checks

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `HL7_SOURCE_PATH` | Path to streaming source | `/mnt/hl7/raw` |
| `HL7_CHECKPOINT_PATH` | Checkpoint location | `/mnt/hl7/checkpoints` |
| `HL7_CATALOG` | Unity Catalog name | `healthcare` |
| `HL7_SCHEMA` | Target schema name | `hl7_streaming` |

## License

Internal use only - Healthcare Data Platform
