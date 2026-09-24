
import requests
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_weather_url(
    latitude: float,
    longitude: float,
    start_date: str,
    end_date: str,
) -> str:
    """
    Build the Open-Meteo historical weather API URL.
    """

    return (
        "https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={latitude}"
        f"&longitude={longitude}"
        f"&start_date={start_date}"
        f"&end_date={end_date}"
        "&hourly=temperature_2m,precipitation,weather_code"
        "&timezone=America%2FNew_York"
    )


def fetch_weather_json(url: str) -> dict:
    """
    Fetch historical weather data from Open-Meteo.
    """

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    return response.json()


def add_weather_bronze_metadata(
    df: DataFrame,
    source_url: str,
    batch_id: str,
) -> DataFrame:
    """
    Add Bronze provenance metadata to weather records.
    """

    return (
        df
        .withColumn("source_system", F.lit("open_meteo"))
        .withColumn("source_url", F.lit(source_url))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("ingested_at", F.current_timestamp())
    )


def batch_already_processed(
    spark,
    table_name: str,
    batch_id: str,
) -> bool:
    """
    Return True if the weather batch already exists in Bronze.
    Return False if the Bronze table does not exist yet.
    """

    catalog_name, schema_name, short_table_name = table_name.split(".")

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