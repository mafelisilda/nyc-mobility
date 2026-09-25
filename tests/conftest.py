import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture(scope="session")
def spark():
    builder = SparkSession.builder.appName(
        "nyc-mobility-tests"
    )

    if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
        builder = builder.master("local[1]")

    spark_session = builder.getOrCreate()

    yield spark_session

    if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
        spark_session.stop()