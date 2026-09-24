from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform_taxi_silver(df: DataFrame) -> DataFrame:
    """
    Clean and standardize Green Taxi Bronze data for Silver.
    """

    return (
        df
        .withColumnRenamed("PULocationID", "pickup_location_id")
        .withColumnRenamed("DOLocationID", "dropoff_location_id")
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
            ) / 60.0,
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
        .filter(
            F.col("pickup_datetime").isNotNull()
            & F.col("dropoff_datetime").isNotNull()
            & (F.col("dropoff_datetime") >= F.col("pickup_datetime"))
            & F.col("pickup_location_id").isNotNull()
            & F.col("dropoff_location_id").isNotNull()
        )
        .dropDuplicates(["trip_hash"])
    )
