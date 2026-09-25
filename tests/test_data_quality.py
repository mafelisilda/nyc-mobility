from src.quality.checks import (
    count_duplicates,
    count_nulls,
    count_rows_outside_date_range,
)

def test_count_nulls(spark):
    df = spark.createDataFrame(
        [
            (1, "A"),
            (2, None),
            (None, "C"),
        ],
        [
            "id",
            "value",
        ],
    )

    result = count_nulls(
        df,
        [
            "id",
            "value",
        ],
    )

    assert result["id"] == 1
    assert result["value"] == 1


def test_count_duplicates(spark):
    df = spark.createDataFrame(
        [
            ("trip_001",),
            ("trip_001",),
            ("trip_002",),
        ],
        [
            "trip_hash",
        ],
    )

    result = count_duplicates(
        df,
        ["trip_hash"],
    )

    assert result == 1


def test_count_duplicates_returns_zero_when_unique(spark):
    df = spark.createDataFrame(
        [
            ("trip_001",),
            ("trip_002",),
            ("trip_003",),
        ],
        [
            "trip_hash",
        ],
    )

    result = count_duplicates(
        df,
        ["trip_hash"],
    )

    assert result == 0


def test_count_rows_outside_date_range(spark):
    df = spark.createDataFrame(
        [
            ("2026-02-28 23:59:00",),
            ("2026-03-01 00:00:00",),
            ("2026-03-15 12:00:00",),
            ("2026-03-31 23:59:00",),
            ("2026-04-01 00:00:00",),
        ],
        [
            "event_time",
        ],
    )

    result = count_rows_outside_date_range(
        df,
        timestamp_column="event_time",
        start_date="2026-03-01",
        end_date="2026-03-31",
    )

    assert result == 2


def test_count_rows_outside_date_range_returns_zero_when_valid(
    spark,
):
    df = spark.createDataFrame(
        [
            ("2026-03-01 00:00:00",),
            ("2026-03-15 12:00:00",),
            ("2026-03-31 23:59:00",),
        ],
        [
            "event_time",
        ],
    )

    result = count_rows_outside_date_range(
        df,
        timestamp_column="event_time",
        start_date="2026-03-01",
        end_date="2026-03-31",
    )

    assert result == 0