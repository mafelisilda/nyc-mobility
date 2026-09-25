from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform_zones_silver(df: DataFrame) -> DataFrame:
    """
    Clean and standardize NYC Taxi Zone lookup data.
    """

    return (
        df
        .withColumn(
            "location_id",
            F.col("LocationID").cast("int"),
        )
        .withColumnRenamed("Borough", "borough")
        .withColumnRenamed("Zone", "zone")
        .filter(F.col("location_id").isNotNull())
        .dropDuplicates(["location_id"])
    )
