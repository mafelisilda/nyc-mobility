from src.transformations.taxi import transform_taxi_silver

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

def test_transform_taxi_silver_filters_invalid_rows(spark):
    input_df = spark.createDataFrame(
        [
            # Valid row
            (
                "2026-03-01 10:00:00",
                "2026-03-01 10:30:00",
                75,
                236,
                2.5,
                20.0,
                "2026-03",
            ),

            # Invalid: dropoff before pickup
            (
                "2026-03-01 11:00:00",
                "2026-03-01 10:30:00",
                75,
                236,
                2.5,
                20.0,
                "2026-03",
            ),

            # Invalid: pickup month != source_month
            (
                "2026-04-01 10:00:00",
                "2026-04-01 10:30:00",
                75,
                236,
                2.5,
                20.0,
                "2026-03",
            ),
        ],
        [
            "lpep_pickup_datetime",
            "lpep_dropoff_datetime",
            "PULocationID",
            "DOLocationID",
            "trip_distance",
            "total_amount",
            "source_month",
        ],
    )

    result = transform_taxi_silver(input_df)

    assert result.count() == 1

    row = result.collect()[0]

    assert row["pickup_location_id"] == 75
    assert row["dropoff_location_id"] == 236
    assert row["trip_duration_minutes"] == 30.0
    assert row["trip_hash"] is not None


def test_transform_taxi_silver_removes_duplicate_trips(spark):
    input_df = spark.createDataFrame(
        [
            (
                "2026-03-01 10:00:00",
                "2026-03-01 10:30:00",
                75,
                236,
                2.5,
                20.0,
                "2026-03",
            ),
            (
                "2026-03-01 10:00:00",
                "2026-03-01 10:30:00",
                75,
                236,
                2.5,
                20.0,
                "2026-03",
            ),
        ],
        [
            "lpep_pickup_datetime",
            "lpep_dropoff_datetime",
            "PULocationID",
            "DOLocationID",
            "trip_distance",
            "total_amount",
            "source_month",
        ],
    )

    result = transform_taxi_silver(input_df)

    assert result.count() == 1


def test_transform_taxi_silver_filters_null_locations(spark):
    schema = StructType(
        [
            StructField(
                "lpep_pickup_datetime",
                StringType(),
                True,
            ),
            StructField(
                "lpep_dropoff_datetime",
                StringType(),
                True,
            ),
            StructField(
                "PULocationID",
                IntegerType(),
                True,
            ),
            StructField(
                "DOLocationID",
                IntegerType(),
                True,
            ),
            StructField(
                "trip_distance",
                DoubleType(),
                True,
            ),
            StructField(
                "total_amount",
                DoubleType(),
                True,
            ),
            StructField(
                "source_month",
                StringType(),
                True,
            ),
        ]
    )

    input_df = spark.createDataFrame(
        [
            (
                "2026-03-01 10:00:00",
                "2026-03-01 10:30:00",
                None,
                236,
                2.5,
                20.0,
                "2026-03",
            ),
        ],
        schema=schema,
    )

    result = transform_taxi_silver(input_df)

    assert result.count() == 0