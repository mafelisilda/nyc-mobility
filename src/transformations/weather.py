from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform_weather_silver(df: DataFrame) -> DataFrame:
    """
    Clean and standardize hourly Open-Meteo weather data.
    """

    return (
        df
        .withColumn(
            "weather_datetime",
            F.to_timestamp("time", "yyyy-MM-dd'T'HH:mm"),
        )
        .withColumn(
            "temperature_c",
            F.col("temperature_2m").cast("double"),
        )
        .withColumn(
            "precipitation_mm",
            F.col("precipitation").cast("double"),
        )
        .withColumn(
            "weather_code",
            F.col("weather_code").cast("int"),
        )
        .filter(F.col("weather_datetime").isNotNull())
        .dropDuplicates(["weather_datetime"])
    )