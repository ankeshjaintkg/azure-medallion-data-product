import os
import subprocess
from pathlib import Path

from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SILVER_PATH = PROJECT_ROOT / "output" / "silver" / "university_chapters"
QUARANTINE_PATH = PROJECT_ROOT / "output" / "quarantine" / "university_chapters"


def run_dq_pipeline():
    """
    Run the Silver DQ test pipeline using the controlled DQ fixture.
    """

    env = os.environ.copy()

    # Ensure Hadoop can be found when pytest launches Spark
    env["HADOOP_HOME"] = r"C:\hadoop"
    env["PATH"] = env["PATH"] + r";C:\hadoop\bin"

    result = subprocess.run(
        [
            "python",
            str(PROJECT_ROOT / "src" / "silver_dq_test.py")
        ],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True
    )

    print(result.stdout)

    if result.returncode != 0:
        print(result.stderr)

    assert result.returncode == 0, (
        f"Silver DQ pipeline failed.\n"
        f"STDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )


def create_spark():
    return (
        SparkSession.builder
        .appName("UniversityChapters-DQ-Tests")
        .master("local[*]")
        .getOrCreate()
    )


def test_invalid_coordinates_are_quarantined():
    """
    DQ-Q1:
    Invalid longitude/latitude must be quarantined.
    """

    run_dq_pipeline()

    spark = create_spark()

    quarantine_df = spark.read.parquet(str(QUARANTINE_PATH))

    invalid_row = (
        quarantine_df
        .filter("chapter_id = 'TEST-INVALID-001'")
        .collect()
    )

    assert len(invalid_row) == 1

    row = invalid_row[0]

    assert row["reason_code"] == "INVALID_COORDINATES"

    spark.stop()


def test_unknown_city_is_warning_and_published():
    """
    DQ-W1:
    UNKNOWN city must produce WARNING and remain in Silver.
    """

    run_dq_pipeline()

    spark = create_spark()

    silver_df = spark.read.parquet(str(SILVER_PATH))

    warning_row = (
        silver_df
        .filter("chapter_id = 'TEST-WARNING-001'")
        .collect()
    )

    assert len(warning_row) == 1

    row = warning_row[0]

    assert row["dq_status"] == "WARNING"
    assert row["dq_warnings"] == "MISSING_OR_UNKNOWN_CITY"

    spark.stop()


def test_quarantined_record_does_not_enter_silver():
    """
    Quarantined records must never enter Silver.
    """

    run_dq_pipeline()

    spark = create_spark()

    silver_df = spark.read.parquet(str(SILVER_PATH))

    invalid_row_count = (
        silver_df
        .filter("chapter_id = 'TEST-INVALID-001'")
        .count()
    )

    assert invalid_row_count == 0

    spark.stop()