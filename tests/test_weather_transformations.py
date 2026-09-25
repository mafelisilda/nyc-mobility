import pytest

from src.transformations.weather import transform_weather_silver

def test_transform_weather_silver_standardizes_columns(spark):
    input_df = spark.createDataFrame(
        [
            (
                "2026-03-01T00:00",
                "5.2",
                "0.0",
                "1",
            ),
        ],
        [
            "time",
            "temperature_2m",
            "precipitation",
            "weather_code",
        ],
    )

    result = transform_weather_silver(input_df)

    row = result.collect()[0]

    assert row["weather_datetime"] is not None
    assert row["temperature_c"] == 5.2
    assert row["precipitation_mm"] == 0.0
    assert row["weather_code"] == 1


def test_transform_weather_silver_removes_duplicate_hours(spark):
    input_df = spark.createDataFrame(
        [
            (
                "2026-03-01T00:00",
                5.2,
                0.0,
                1,
            ),
            (
                "2026-03-01T00:00",
                5.2,
                0.0,
                1,
            ),
        ],
        [
            "time",
            "temperature_2m",
            "precipitation",
            "weather_code",
        ],
    )

    result = transform_weather_silver(input_df)

    assert result.count() == 1


def test_transform_weather_silver_filters_invalid_timestamp(spark):
    input_df = spark.createDataFrame(
        [
            (
                "not-a-date",
                5.2,
                0.0,
                1,
            ),
        ],
        [
            "time",
            "temperature_2m",
            "precipitation",
            "weather_code",
        ],
    )

    result = transform_weather_silver(input_df)

    assert result.count() == 0
