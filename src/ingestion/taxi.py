from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_bronze_metadata(
    df: DataFrame,
    source_file: str,
    source_month: str,
    batch_id: str,
) -> DataFrame:
    """
    Add Bronze-layer provenance metadata to NYC Green Taxi data.
    """

    return (
        df
        .withColumn(
            "source_system",
            F.lit("nyc_tlc_green_taxi"),
        )
        .withColumn(
            "source_file",
            F.lit(source_file),
        )
        .withColumn(
            "source_month",
            F.lit(source_month),
        )
        .withColumn(
            "batch_id",
            F.lit(batch_id),
        )
        .withColumn(
            "ingested_at",
            F.current_timestamp(),
        )
    )


def file_already_processed(
    spark,
    table_name: str,
    source_file: str,
) -> bool:
    """
    Return True if the source file already exists in Bronze.
    Return False only when the Bronze table does not yet exist.
    """

    catalog_name, schema_name, short_table_name = (
        table_name.split(".")
    )

    table_exists = (
        spark.sql(
            f"""
            SHOW TABLES IN `{catalog_name}`.`{schema_name}`
            LIKE '{short_table_name}'
            """
        )
        .limit(1)
        .count()
        > 0
    )

    if not table_exists:
        return False

    return (
        spark.table(table_name)
        .filter(
            F.col("source_file") == source_file
        )
        .limit(1)
        .count()
        > 0
    )