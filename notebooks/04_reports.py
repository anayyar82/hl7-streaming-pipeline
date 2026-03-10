# Databricks notebook source
# MAGIC %md
# MAGIC # HL7 Streaming Pipeline - Real-Time ED & ICU Reports
# MAGIC 
# MAGIC Operational reporting tables for Emergency Department (ED) and
# MAGIC Intensive Care Unit (ICU) monitoring. These tables power dashboards
# MAGIC that track arrivals, discharges, and live census in real time.
# MAGIC 
# MAGIC ## Signal Mapping (ADT Trigger Events)
# MAGIC | Trigger | Meaning | Effect |
# MAGIC |---------|---------|--------|
# MAGIC | A01 | Admit | +1 arrival |
# MAGIC | A03 | Discharge | +1 discharge |
# MAGIC | A04 | Register (ED walk-in) | +1 ED arrival |
# MAGIC | A02 | Transfer | +1 arrival at destination, +1 discharge at origin |
# MAGIC | A08 | Update patient info | Census unchanged |
# MAGIC 
# MAGIC ## Department Classification
# MAGIC - **ED**: `patient_class = 'E'` OR `location_point_of_care LIKE '%ED%'` OR `location_point_of_care LIKE '%ER%'`
# MAGIC - **ICU**: `location_point_of_care LIKE '%ICU%'` OR `hospital_service IN ('ICU','CCU','MICU','SICU','NICU','PICU')`
# MAGIC 
# MAGIC ## Tables
# MAGIC | Table | Grain | Description |
# MAGIC |-------|-------|-------------|
# MAGIC | `gold_ed_hourly_census` | hour × facility | ED arrivals, discharges, net census change |
# MAGIC | `gold_icu_hourly_census` | hour × facility | ICU arrivals, discharges, net census change |
# MAGIC | `gold_ed_daily_summary` | day × facility | Daily ED volume, avg LOS, peak hour |
# MAGIC | `gold_icu_daily_summary` | day × facility | Daily ICU volume, avg LOS, peak hour |
# MAGIC | `gold_department_census_current` | facility × dept | Latest point-in-time census snapshot |

# COMMAND ----------

import dlt
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: Classify Department from Encounter Data

# COMMAND ----------

def classify_department(df):
    """Tag each encounter with its department (ED, ICU, or Other)."""
    return df.withColumn(
        "department",
        F.when(
            (F.col("patient_class") == "E") |
            (F.upper(F.coalesce(F.col("location_point_of_care"), F.lit(""))).like("%ED%")) |
            (F.upper(F.coalesce(F.col("location_point_of_care"), F.lit(""))).like("%ER%")) |
            (F.upper(F.coalesce(F.col("location_point_of_care"), F.lit(""))).like("%EMER%")),
            F.lit("ED")
        ).when(
            (F.upper(F.coalesce(F.col("location_point_of_care"), F.lit(""))).like("%ICU%")) |
            (F.upper(F.coalesce(F.col("hospital_service"), F.lit(""))).isin(
                "ICU", "CCU", "MICU", "SICU", "NICU", "PICU"
            )),
            F.lit("ICU")
        ).otherwise(F.lit("OTHER"))
    )

def parse_hl7_ts(col_name):
    """Parse HL7 timestamps (yyyyMMddHHmm or yyyyMMddHHmmss) to proper timestamps."""
    return F.coalesce(
        F.to_timestamp(F.col(col_name), "yyyyMMddHHmmss"),
        F.to_timestamp(F.col(col_name), "yyyyMMddHHmm"),
        F.to_timestamp(F.col(col_name), "yyyyMMdd"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_ed_hourly_census
# MAGIC Hourly ED arrivals, discharges, and net change per facility.
# MAGIC Arrivals = A01 + A04 (admits and registrations).
# MAGIC Discharges = A03.

# COMMAND ----------

@dlt.table(
    name="gold_ed_hourly_census",
    comment="Hourly ED arrivals, discharges, and net census change by facility"
)
def gold_ed_hourly_census():
    encounters = classify_department(dlt.read("gold_encounter_fact"))
    ed = encounters.filter(F.col("department") == "ED")

    arrivals = (
        ed.filter(F.col("trigger_event").isin("A01", "A04", "A02"))
        .withColumn("event_ts", parse_hl7_ts("admit_datetime"))
        .withColumn("event_hour", F.date_trunc("hour", F.coalesce("event_ts", "created_at")))
        .groupBy("event_hour", "location_facility")
        .agg(F.count("*").alias("arrivals"))
    )

    discharges = (
        ed.filter(F.col("trigger_event") == "A03")
        .withColumn("event_ts", parse_hl7_ts("discharge_datetime"))
        .withColumn("event_hour", F.date_trunc("hour", F.coalesce("event_ts", "created_at")))
        .groupBy("event_hour", "location_facility")
        .agg(F.count("*").alias("discharges"))
    )

    return (
        arrivals
        .join(discharges, ["event_hour", "location_facility"], "full_outer")
        .withColumn("arrivals", F.coalesce("arrivals", F.lit(0)))
        .withColumn("discharges", F.coalesce("discharges", F.lit(0)))
        .withColumn("net_change", F.col("arrivals") - F.col("discharges"))
        .withColumn("hour_of_day", F.hour("event_hour"))
        .withColumn("day_of_week", F.dayofweek("event_hour"))
        .withColumn("is_weekend", F.col("day_of_week").isin(1, 7).cast("int"))
        .select(
            "event_hour", "location_facility",
            "arrivals", "discharges", "net_change",
            "hour_of_day", "day_of_week", "is_weekend"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_icu_hourly_census
# MAGIC Hourly ICU arrivals, discharges, and net change per facility.

# COMMAND ----------

@dlt.table(
    name="gold_icu_hourly_census",
    comment="Hourly ICU arrivals, discharges, and net census change by facility"
)
def gold_icu_hourly_census():
    encounters = classify_department(dlt.read("gold_encounter_fact"))
    icu = encounters.filter(F.col("department") == "ICU")

    arrivals = (
        icu.filter(F.col("trigger_event").isin("A01", "A02"))
        .withColumn("event_ts", parse_hl7_ts("admit_datetime"))
        .withColumn("event_hour", F.date_trunc("hour", F.coalesce("event_ts", "created_at")))
        .groupBy("event_hour", "location_facility")
        .agg(F.count("*").alias("arrivals"))
    )

    discharges = (
        icu.filter(F.col("trigger_event").isin("A03", "A02"))
        .withColumn("event_ts", parse_hl7_ts("discharge_datetime"))
        .withColumn("event_hour", F.date_trunc("hour", F.coalesce("event_ts", "created_at")))
        .groupBy("event_hour", "location_facility")
        .agg(F.count("*").alias("discharges"))
    )

    return (
        arrivals
        .join(discharges, ["event_hour", "location_facility"], "full_outer")
        .withColumn("arrivals", F.coalesce("arrivals", F.lit(0)))
        .withColumn("discharges", F.coalesce("discharges", F.lit(0)))
        .withColumn("net_change", F.col("arrivals") - F.col("discharges"))
        .withColumn("hour_of_day", F.hour("event_hour"))
        .withColumn("day_of_week", F.dayofweek("event_hour"))
        .withColumn("is_weekend", F.col("day_of_week").isin(1, 7).cast("int"))
        .select(
            "event_hour", "location_facility",
            "arrivals", "discharges", "net_change",
            "hour_of_day", "day_of_week", "is_weekend"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_ed_daily_summary
# MAGIC Daily ED operational summary: total volume, average LOS, peak arrival hour.

# COMMAND ----------

@dlt.table(
    name="gold_ed_daily_summary",
    comment="Daily ED operational summary with volume, avg LOS, and peak hour"
)
def gold_ed_daily_summary():
    encounters = classify_department(dlt.read("gold_encounter_fact"))
    ed = encounters.filter(F.col("department") == "ED")

    return (
        ed.withColumn("admit_ts", parse_hl7_ts("admit_datetime"))
        .withColumn("discharge_ts", parse_hl7_ts("discharge_datetime"))
        .withColumn("activity_date", F.to_date(F.coalesce("admit_ts", "created_at")))
        .withColumn("admit_hour", F.hour(F.coalesce("admit_ts", "created_at")))
        .withColumn(
            "los_minutes",
            (F.unix_timestamp("discharge_ts") - F.unix_timestamp("admit_ts")) / 60
        )
        .groupBy("activity_date", "location_facility")
        .agg(
            F.count("*").alias("total_encounters"),
            F.sum(F.when(F.col("trigger_event").isin("A01", "A04"), 1).otherwise(0)).alias("total_arrivals"),
            F.sum(F.when(F.col("trigger_event") == "A03", 1).otherwise(0)).alias("total_discharges"),
            F.countDistinct("patient_id").alias("unique_patients"),
            F.round(F.avg("los_minutes"), 1).alias("avg_los_minutes"),
            F.round(F.percentile_approx("los_minutes", 0.5), 1).alias("median_los_minutes"),
            F.max("los_minutes").alias("max_los_minutes"),
            F.mode("admit_hour").alias("peak_arrival_hour"),
            F.countDistinct("attending_doctor_id").alias("unique_providers"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_icu_daily_summary
# MAGIC Daily ICU operational summary.

# COMMAND ----------

@dlt.table(
    name="gold_icu_daily_summary",
    comment="Daily ICU operational summary with volume, avg LOS, and peak hour"
)
def gold_icu_daily_summary():
    encounters = classify_department(dlt.read("gold_encounter_fact"))
    icu = encounters.filter(F.col("department") == "ICU")

    return (
        icu.withColumn("admit_ts", parse_hl7_ts("admit_datetime"))
        .withColumn("discharge_ts", parse_hl7_ts("discharge_datetime"))
        .withColumn("activity_date", F.to_date(F.coalesce("admit_ts", "created_at")))
        .withColumn("admit_hour", F.hour(F.coalesce("admit_ts", "created_at")))
        .withColumn(
            "los_hours",
            (F.unix_timestamp("discharge_ts") - F.unix_timestamp("admit_ts")) / 3600
        )
        .groupBy("activity_date", "location_facility")
        .agg(
            F.count("*").alias("total_encounters"),
            F.sum(F.when(F.col("trigger_event").isin("A01", "A02"), 1).otherwise(0)).alias("total_arrivals"),
            F.sum(F.when(F.col("trigger_event").isin("A03", "A02"), 1).otherwise(0)).alias("total_discharges"),
            F.countDistinct("patient_id").alias("unique_patients"),
            F.round(F.avg("los_hours"), 1).alias("avg_los_hours"),
            F.round(F.percentile_approx("los_hours", 0.5), 1).alias("median_los_hours"),
            F.max("los_hours").alias("max_los_hours"),
            F.mode("admit_hour").alias("peak_arrival_hour"),
            F.countDistinct("attending_doctor_id").alias("unique_providers"),
            F.countDistinct("location_bed").alias("beds_used"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_department_census_current
# MAGIC Point-in-time census: cumulative arrivals minus cumulative discharges
# MAGIC gives approximate current occupancy per department per facility.

# COMMAND ----------

@dlt.table(
    name="gold_department_census_current",
    comment="Approximate current census per department per facility"
)
def gold_department_census_current():
    encounters = classify_department(dlt.read("gold_encounter_fact"))

    arrivals = (
        encounters
        .filter(F.col("trigger_event").isin("A01", "A04", "A02"))
        .groupBy("location_facility", "department")
        .agg(F.count("*").alias("total_arrivals"))
    )

    discharges = (
        encounters
        .filter(F.col("trigger_event") == "A03")
        .groupBy("location_facility", "department")
        .agg(F.count("*").alias("total_discharges"))
    )

    return (
        arrivals
        .join(discharges, ["location_facility", "department"], "full_outer")
        .withColumn("total_arrivals", F.coalesce("total_arrivals", F.lit(0)))
        .withColumn("total_discharges", F.coalesce("total_discharges", F.lit(0)))
        .withColumn("estimated_census", F.col("total_arrivals") - F.col("total_discharges"))
        .withColumn("snapshot_at", F.current_timestamp())
    )
