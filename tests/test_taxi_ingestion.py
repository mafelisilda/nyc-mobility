
import pytest
from pyspark.sql import SparkSession

from src.ingestion.taxi import add_bronze_metadata


@pytest.fixture(scope="session")
def spark():
    """
    Create one local Spark session for the test suite.
    """

    spark_session = (
        SparkSession.builder
        .master("local[1]")
        .appName("nyc-mobility-tests")
        .getOrCreate()
    )

    yield spark_session

    spark_session.stop()


def test_add_bronze_metadata(spark):
    """
    Verify that Bronze provenance columns are added correctly.
    """

    input_df = spark.createDataFrame(
        [
            (
                1,
                75,
                236,
            )
        ],
        [
            "VendorID",
            "PULocationID",
            "DOLocationID",
        ],
    )

    result_df = add_bronze_metadata(
        df=input_df,
        source_file="green_tripdata_2026-03.parquet",
        source_month="2026-03",
        batch_id="taxi_2026_03",
    )

    result = result_df.collect()[0]

    assert result["source_system"] == "nyc_tlc_green_taxi"
    assert result["source_file"] == "green_tripdata_2026-03.parquet"
    assert result["source_month"] == "2026-03"
    assert result["batch_id"] == "taxi_2026_03"
    assert result["ingested_at"] is not None