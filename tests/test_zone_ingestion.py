import pytest
from pyspark.sql import SparkSession

from src.ingestion.zones import add_zone_bronze_metadata


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder
        .master("local[1]")
        .appName("nyc-mobility-tests")
        .getOrCreate()
    )

    yield spark_session

    spark_session.stop()


def test_add_zone_bronze_metadata(spark):
    input_df = spark.createDataFrame(
        [
            (
                "1",
                "EWR",
                "Newark Airport",
                "EWR",
            )
        ],
        [
            "LocationID",
            "Borough",
            "Zone",
            "service_zone",
        ],
    )

    result_df = add_zone_bronze_metadata(
        df=input_df,
        source_url="https://example.com/zones.csv",
        batch_id="taxi_zones_lookup",
    )

    result = result_df.collect()[0]

    assert result["source_system"] == "nyc_tlc_taxi_zones"
    assert result["source_url"] == "https://example.com/zones.csv"
    assert result["batch_id"] == "taxi_zones_lookup"
    assert result["ingested_at"] is not None