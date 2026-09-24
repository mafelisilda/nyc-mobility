import pytest
from pyspark.sql import SparkSession

from src.ingestion.weather import (
    add_weather_bronze_metadata,
    build_weather_url,
)


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


def test_build_weather_url():
    url = build_weather_url(
        latitude=40.7128,
        longitude=-74.0060,
        start_date="2026-03-01",
        end_date="2026-03-31",
    )

    assert "latitude=40.7128" in url
    assert "longitude=-74.006" in url
    assert "start_date=2026-03-01" in url
    assert "end_date=2026-03-31" in url
    assert "temperature_2m" in url
    assert "precipitation" in url
    assert "weather_code" in url


def test_add_weather_bronze_metadata(spark):
    input_df = spark.createDataFrame(
        [
            (
                "2026-03-01T00:00",
                5.2,
                0.0,
                1,
            )
        ],
        [
            "time",
            "temperature_2m",
            "precipitation",
            "weather_code",
        ],
    )

    result_df = add_weather_bronze_metadata(
        df=input_df,
        source_url="https://example.com/weather",
        batch_id="weather_2026_03",
    )

    result = result_df.collect()[0]

    assert result["source_system"] == "open_meteo"
    assert result["source_url"] == "https://example.com/weather"
    assert result["batch_id"] == "weather_2026_03"
    assert result["ingested_at"] is not None