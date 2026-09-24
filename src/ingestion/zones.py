import csv
import io
import requests

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def fetch_zone_rows(source_url: str) -> list[dict]:
    """
    Download the NYC Taxi Zone lookup CSV and return its rows.
    """

    response = requests.get(source_url, timeout=30)
    response.raise_for_status()

    reader = csv.DictReader(
        io.StringIO(response.text)
    )

    return list(reader)


def add_zone_bronze_metadata(
    df: DataFrame,
    source_url: str,
    batch_id: str,
) -> DataFrame:
    """
    Add Bronze provenance metadata to taxi-zone records.
    """

    return (
        df
        .withColumn(
            "source_system",
            F.lit("nyc_tlc_taxi_zones"),
        )
        .withColumn(
            "source_url",
            F.lit(source_url),
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


def batch_already_processed(
    spark,
    table_name: str,
    batch_id: str,
) -> bool:
    """
    Return True if the taxi-zone batch already exists.
    Return False if the Bronze table does not exist yet.
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
        .filter(F.col("batch_id") == batch_id)
        .limit(1)
        .count()
        > 0
    )
