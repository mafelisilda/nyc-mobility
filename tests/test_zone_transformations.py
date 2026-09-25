from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

from src.transformations.zones import transform_zones_silver

def test_transform_zones_silver_standardizes_columns(spark):
    input_df = spark.createDataFrame(
        [
            (
                "1",
                "EWR",
                "Newark Airport",
                "EWR",
            ),
        ],
        [
            "LocationID",
            "Borough",
            "Zone",
            "service_zone",
        ],
    )

    result = transform_zones_silver(input_df)

    row = result.collect()[0]

    assert row["location_id"] == 1
    assert row["borough"] == "EWR"
    assert row["zone"] == "Newark Airport"
    assert row["service_zone"] == "EWR"


def test_transform_zones_silver_removes_duplicate_location_ids(spark):
    input_df = spark.createDataFrame(
        [
            (
                "1",
                "EWR",
                "Newark Airport",
                "EWR",
            ),
            (
                "1",
                "EWR",
                "Newark Airport",
                "EWR",
            ),
        ],
        [
            "LocationID",
            "Borough",
            "Zone",
            "service_zone",
        ],
    )

    result = transform_zones_silver(input_df)

    assert result.count() == 1


def test_transform_zones_silver_filters_null_location_id(spark):
    schema = StructType(
        [
            StructField(
                "LocationID",
                StringType(),
                True,
            ),
            StructField(
                "Borough",
                StringType(),
                True,
            ),
            StructField(
                "Zone",
                StringType(),
                True,
            ),
            StructField(
                "service_zone",
                StringType(),
                True,
            ),
        ]
    )

    input_df = spark.createDataFrame(
        [
            (
                None,
                "Unknown",
                "Unknown",
                "Unknown",
            ),
        ],
        schema=schema,
    )

    result = transform_zones_silver(input_df)

    assert result.count() == 0