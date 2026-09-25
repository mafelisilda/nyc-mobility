from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def prepare_taxi_records(df: DataFrame) -> DataFrame:
    """
    Standardize Green Taxi Bronze records and assign a quarantine reason
    to rows that violate row-level data-quality rules.
    """

    return (
        df
        .withColumnRenamed(
            "PULocationID",
            "pickup_location_id",
        )
        .withColumnRenamed(
            "DOLocationID",
            "dropoff_location_id",
        )
        .withColumn(
            "pickup_datetime",
            F.col("lpep_pickup_datetime").cast("timestamp"),
        )
        .withColumn(
            "dropoff_datetime",
            F.col("lpep_dropoff_datetime").cast("timestamp"),
        )
        .withColumn(
            "trip_duration_minutes",
            (
                F.unix_timestamp("dropoff_datetime")
                - F.unix_timestamp("pickup_datetime")
            )
            / 60.0,
        )
        .withColumn(
            "trip_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("lpep_pickup_datetime").cast("string"),
                    F.col("lpep_dropoff_datetime").cast("string"),
                    F.col("pickup_location_id").cast("string"),
                    F.col("dropoff_location_id").cast("string"),
                    F.col("trip_distance").cast("string"),
                    F.col("total_amount").cast("string"),
                ),
                256,
            ),
        )
        .withColumn(
            "quarantine_reason",
            F.when(
                F.col("pickup_datetime").isNull(),
                F.lit("NULL_OR_INVALID_PICKUP_TIMESTAMP"),
            )
            .when(
                F.col("dropoff_datetime").isNull(),
                F.lit("NULL_OR_INVALID_DROPOFF_TIMESTAMP"),
            )
            .when(
                F.col("dropoff_datetime")
                < F.col("pickup_datetime"),
                F.lit("DROPOFF_BEFORE_PICKUP"),
            )
            .when(
                F.col("pickup_location_id").isNull(),
                F.lit("NULL_PICKUP_LOCATION"),
            )
            .when(
                F.col("dropoff_location_id").isNull(),
                F.lit("NULL_DROPOFF_LOCATION"),
            )
            .when(
                F.col("source_month").isNull(),
                F.lit("NULL_SOURCE_MONTH"),
            )
            .when(
                F.date_format(
                    F.col("pickup_datetime"),
                    "yyyy-MM",
                )
                != F.col("source_month"),
                F.lit("PICKUP_MONTH_MISMATCH"),
            ),
        )
    )


def split_taxi_records(
    df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """
    Split Taxi Bronze records into valid Silver records and
    quarantined records.

    Returns:
        valid_df:
            Clean records suitable for the Silver taxi table.

        quarantine_df:
            Invalid records with a quarantine reason and timestamp.
    """

    prepared_df = prepare_taxi_records(df)

    valid_df = (
        prepared_df
        .filter(
            F.col("quarantine_reason").isNull()
        )
        .drop("quarantine_reason")
        .dropDuplicates(["trip_hash"])
    )

    quarantine_df = (
        prepared_df
        .filter(
            F.col("quarantine_reason").isNotNull()
        )
        .withColumn(
            "quarantined_at",
            F.current_timestamp(),
        )
    )

    return valid_df, quarantine_df


def transform_taxi_silver(df: DataFrame) -> DataFrame:
    """
    Clean and standardize Green Taxi Bronze data for Silver.

    Invalid row-level records are excluded from Silver.
    Use split_taxi_records() when the quarantined records
    also need to be persisted.
    """

    valid_df, _ = split_taxi_records(df)

    return valid_df