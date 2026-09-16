import os
import subprocess
from pathlib import Path

from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SILVER_PATH = PROJECT_ROOT / "output" / "silver" / "university_chapters"
GOLD_PATH = PROJECT_ROOT / "output" / "gold" / "university_chapters" / "v1"


def run_dq_pipeline():
    """
    Generate Silver data from the controlled DQ fixture.
    """

    env = os.environ.copy()

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


def run_gold_pipeline():
    """
    Generate Gold from the DQ-tested Silver data.
    """

    env = os.environ.copy()

    env["HADOOP_HOME"] = r"C:\hadoop"
    env["PATH"] = env["PATH"] + r";C:\hadoop\bin"

    result = subprocess.run(
        [
            "python",
            str(PROJECT_ROOT / "src" / "gold_publish.py")
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
        f"Gold pipeline failed.\n"
        f"STDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )


def create_spark():
    return (
        SparkSession.builder
        .appName("UniversityChapters-Gold-DQ-Tests")
        .master("local[*]")
        .getOrCreate()
    )


def test_quarantined_record_is_excluded_from_gold():
    """
    DQ-Q1:
    A record with invalid coordinates must not appear in Gold.
    """

    run_dq_pipeline()
    run_gold_pipeline()

    spark = create_spark()

    gold_df = spark.read.parquet(str(GOLD_PATH))

    quarantined_record_count = (
        gold_df
        .filter("chapter_id = 'TEST-INVALID-001'")
        .count()
    )

    assert quarantined_record_count == 0

    spark.stop()


def test_warning_record_is_present_in_gold():
    """
    DQ-W1:
    A record with UNKNOWN city must remain publishable and appear in Gold
    with the correct warning.
    """

    run_dq_pipeline()
    run_gold_pipeline()

    spark = create_spark()

    gold_df = spark.read.parquet(str(GOLD_PATH))

    warning_rows = (
        gold_df
        .filter("chapter_id = 'TEST-WARNING-001'")
        .collect()
    )

    assert len(warning_rows) == 1

    row = warning_rows[0]

    assert row["dq_status"] == "WARNING"
    assert row["dq_warnings"] == "MISSING_OR_UNKNOWN_CITY"

    spark.stop()