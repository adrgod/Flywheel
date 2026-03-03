from pathlib import Path
import os
import sys
import subprocess

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    try:
        if not os.environ.get("JAVA_HOME"):
            java_home_cmd = ["/usr/libexec/java_home", "-v", "17"]
            java_home = subprocess.check_output(java_home_cmd, text=True).strip()
            os.environ["JAVA_HOME"] = java_home

        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        os.environ.setdefault("SPARK_USER", "pytest")

        spark = (
            SparkSession.builder.master("local[2]")
            .appName("flywheel-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.ansi.enabled", "false")
            .getOrCreate()
        )
    except Exception as exc:
        pytest.skip(f"Spark session is unavailable in this environment: {exc}")
    yield spark
    spark.stop()


@pytest.fixture
def input_output_paths(tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    return str(input_dir), str(output_dir)
