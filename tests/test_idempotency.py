import pytest

from src.ingestion.taxi import file_already_processed

def test_file_already_processed_returns_true_for_existing_file(
    spark,
):
    table_name = "default.test_taxi_idempotency"

    df = spark.createDataFrame(
        [
            (
                "green_tripdata_2026-03.parquet",
                "2026-03",
                "taxi_2026_03",
            ),
        ],
        [
            "source_file",
            "source_month",
            "batch_id",
        ],
    )

    (
        df.write
        .mode("overwrite")
        .saveAsTable(table_name)
    )

    result = file_already_processed(
        spark,
        table_name,
        "green_tripdata_2026-03.parquet",
    )

    assert result is True

    spark.sql(
        f"DROP TABLE IF EXISTS {table_name}"
    )


def test_file_already_processed_returns_false_for_new_file(
    spark,
):
    table_name = "default.test_taxi_idempotency"

    df = spark.createDataFrame(
        [
            (
                "green_tripdata_2026-03.parquet",
                "2026-03",
                "taxi_2026_03",
            ),
        ],
        [
            "source_file",
            "source_month",
            "batch_id",
        ],
    )

    (
        df.write
        .mode("overwrite")
        .saveAsTable(table_name)
    )

    result = file_already_processed(
        spark,
        table_name,
        "green_tripdata_2026-04.parquet",
    )

    assert result is False

    spark.sql(
        f"DROP TABLE IF EXISTS {table_name}"
    )


def test_file_already_processed_returns_false_when_table_missing(
    spark,
):
    table_name = "default.nonexistent_taxi_table"

    spark.sql(
        f"DROP TABLE IF EXISTS {table_name}"
    )

    result = file_already_processed(
        spark,
        table_name,
        "green_tripdata_2026-03.parquet",
    )

    assert result is False
