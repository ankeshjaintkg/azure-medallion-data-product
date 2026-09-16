from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# --------------------------------------------------
# Spark session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("UniversityChapters-Gold")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# Paths
# --------------------------------------------------

SILVER_PATH = "output/silver/university_chapters"
GOLD_PATH = "output/gold/university_chapters/v1"

# --------------------------------------------------
# Read Silver
# --------------------------------------------------

silver_df = spark.read.parquet(SILVER_PATH)

print("\n===== SILVER RECORD COUNT =====")
print(silver_df.count())

# --------------------------------------------------
# Gold publishing rule
#
# OK       -> Publish
# WARNING  -> Publish
# QUARANTINED -> Never publish
# --------------------------------------------------

gold_df = (
    silver_df
    .filter(col("dq_status").isin("OK", "WARNING"))
)

# --------------------------------------------------
# Select consumer-facing columns
# --------------------------------------------------

gold_df = gold_df.select(
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
)

# --------------------------------------------------
# Write Gold
# --------------------------------------------------

(
    gold_df
    .write
    .mode("overwrite")
    .parquet(GOLD_PATH)
)

# --------------------------------------------------
# Validation
# --------------------------------------------------

print("\n===== GOLD RECORD COUNT =====")
print(gold_df.count())

print("\n===== GOLD DATA =====")
gold_df.show(truncate=False)

print("\n===== GOLD DQ STATUS =====")
gold_df.groupBy("dq_status").count().show()

print("\nGold layer successfully created.")

spark.stop()