from pathlib import Path

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    try:
        spark = (
            SparkSession.builder.master("local[2]")
            .appName("flywheel-tests")
            .config("spark.ui.enabled", "false")
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
