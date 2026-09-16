from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    lit,
    lower,
    trim,
    when,
    array,
    array_join,
    to_json
)


# ============================================================
# CONFIGURATION
# ============================================================

BRONZE_PATH = "fixtures/dq_test.json"

SILVER_PATH = "output/silver/university_chapters"

QUARANTINE_PATH = "output/quarantine/university_chapters"


# ============================================================
# START SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("UniversityChapters_Silver")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 1. READ BRONZE
# ============================================================

bronze_df = (
    spark.read
    .option("multiline", "true")
    .json(BRONZE_PATH)
)

print("\n===== BRONZE RECORD COUNT =====")
print(bronze_df.count())


# ============================================================
# 2. GET INGESTION METADATA
# ============================================================

ingest_timestamp = datetime.now(timezone.utc).isoformat()

ingest_run_id = datetime.now(timezone.utc).strftime(
    "%Y%m%d_%H%M%S"
)

print("\n===== INGESTION METADATA =====")
print("ingest_run_id:", ingest_run_id)
print("ingest_timestamp:", ingest_timestamp)


# ============================================================
# 3. FLATTEN FEATURES
# ============================================================

features_df = bronze_df.select(
    explode("features").alias("feature")
)


silver_base_df = features_df.select(
    col("feature.attributes.ChapterID")
        .alias("chapter_id"),

    col("feature.attributes.University_Chapter")
        .alias("chapter_name"),

    col("feature.attributes.City")
        .alias("city"),

    col("feature.attributes.State")
        .alias("state"),

    col("feature.attributes.OBJECTID")
        .cast("long")
        .alias("source_object_id"),

    col("feature.geometry.x")
        .cast("double")
        .alias("longitude"),

    col("feature.geometry.y")
        .cast("double")
        .alias("latitude"),

    lit(ingest_run_id)
        .alias("ingest_run_id"),

    lit(ingest_timestamp)
        .alias("ingest_timestamp"),

    # Preserve the complete source feature
    # for quarantine/debugging.
    to_json(col("feature"))
        .alias("raw_feature")
)


# ============================================================
# 4. APPLY REQUIRED STATE SCOPE
# ============================================================

silver_base_df = silver_base_df.filter(
    col("state").isin("CA", "OR", "WA")
)


# ============================================================
# 5. DEDUPLICATE
# ============================================================

silver_base_df = silver_base_df.dropDuplicates(
    ["chapter_id"]
)


# ============================================================
# 6. DQ-Q1
#
# HARD FAILURE:
#
# Longitude:
#   NULL
#   < -180
#   > 180
#
# Latitude:
#   NULL
#   < -90
#   > 90
#
# Invalid records go ONLY to quarantine.
# ============================================================

invalid_coordinates = (
    col("longitude").isNull()
    | col("latitude").isNull()
    | (col("longitude") < -180)
    | (col("longitude") > 180)
    | (col("latitude") < -90)
    | (col("latitude") > 90)
)


quarantine_df = (
    silver_base_df
    .filter(invalid_coordinates)
    .select(
        "chapter_id",
        "chapter_name",
        "city",
        "state",
        "longitude",
        "latitude",
        "source_object_id",
        "ingest_run_id",
        "ingest_timestamp",
        "raw_feature",
        lit("INVALID_COORDINATES")
            .alias("reason_code")
    )
)


# ============================================================
# 7. VALID COORDINATES → CONTINUE TO SILVER
# ============================================================

valid_df = silver_base_df.filter(
    ~invalid_coordinates
)


# ============================================================
# 8. DQ-W1
#
# WARNING:
#
# city is:
#   NULL
#   blank
#   UNKNOWN
#
# These records ARE published.
# ============================================================

missing_city = (
    col("city").isNull()
    | (trim(col("city")) == "")
    | (lower(trim(col("city"))) == "unknown")
)


silver_df = (
    valid_df

    .withColumn(
        "dq_status",
        when(
            missing_city,
            lit("WARNING")
        ).otherwise(
            lit("OK")
        )
    )

    .withColumn(
        "dq_warnings",
        when(
            missing_city,
            lit("MISSING_OR_UNKNOWN_CITY")
        ).otherwise(
            lit("")
        )
    )

    .drop("raw_feature")
)


# ============================================================
# 9. WRITE SILVER
# ============================================================

(
    silver_df
    .write
    .mode("overwrite")
    .parquet(SILVER_PATH)
)


# ============================================================
# 10. WRITE QUARANTINE
# ============================================================

(
    quarantine_df
    .write
    .mode("overwrite")
    .parquet(QUARANTINE_PATH)
)


# ============================================================
# 11. DQ / BATCH METRICS
# ============================================================

rows_in = silver_base_df.count()

rows_quarantined = quarantine_df.count()

rows_warned = (
    silver_df
    .filter(col("dq_status") == "WARNING")
    .count()
)

rows_ok = (
    silver_df
    .filter(col("dq_status") == "OK")
    .count()
)


print("\n==========================================")
print("UNIVERSITY CHAPTERS - SILVER DQ SUMMARY")
print("==========================================")

print(f"rows_in          : {rows_in}")
print(f"rows_quarantined : {rows_quarantined}")
print(f"rows_warned      : {rows_warned}")
print(f"rows_ok          : {rows_ok}")

print("==========================================")


# ============================================================
# 12. SHOW SILVER
# ============================================================

print("\n===== SILVER DATA =====")

silver_df.select(
    "chapter_id",
    "chapter_name",
    "city",
    "state",
    "longitude",
    "latitude",
    "source_object_id",
    "ingest_run_id",
    "ingest_timestamp",
    "dq_status",
    "dq_warnings"
).show(
    truncate=False
)


# ============================================================
# 13. SHOW QUARANTINE
# ============================================================

print("\n===== QUARANTINE DATA =====")

quarantine_df.show(
    truncate=False
)


spark.stop()