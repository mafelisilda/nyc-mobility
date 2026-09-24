from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_dim_date(taxi_df: DataFrame) -> DataFrame:
    """
    Build a conformed date dimension from taxi pickup and dropoff dates.
    """

    pickup_dates = (
        taxi_df
        .select(
            F.to_date("pickup_datetime").alias("full_date")
        )
    )

    dropoff_dates = (
        taxi_df
        .select(
            F.to_date("dropoff_datetime").alias("full_date")
        )
    )

    return (
        pickup_dates
        .union(dropoff_dates)
        .dropDuplicates(["full_date"])
        .filter(F.col("full_date").isNotNull())
        .withColumn(
            "date_key",
            F.date_format("full_date", "yyyyMMdd").cast("int"),
        )
        .withColumn(
            "year",
            F.year("full_date"),
        )
        .withColumn(
            "quarter",
            F.quarter("full_date"),
        )
        .withColumn(
            "month",
            F.month("full_date"),
        )
        .withColumn(
            "month_name",
            F.date_format("full_date", "MMMM"),
        )
        .withColumn(
            "day_of_month",
            F.dayofmonth("full_date"),
        )
        .withColumn(
            "day_of_week",
            F.dayofweek("full_date"),
        )
        .withColumn(
            "day_name",
            F.date_format("full_date", "EEEE"),
        )
        .withColumn(
            "week_of_year",
            F.weekofyear("full_date"),
        )
        .withColumn(
            "is_weekend",
            F.dayofweek("full_date").isin(1, 7),
        )
        .select(
            "date_key",
            "full_date",
            "year",
            "quarter",
            "month",
            "month_name",
            "day_of_month",
            "day_of_week",
            "day_name",
            "week_of_year",
            "is_weekend",
        )
        .orderBy("full_date")
    )


def build_dim_hour(spark) -> DataFrame:
    """
    Build a 24-row hour-of-day dimension.
    """

    hour_df = spark.range(0, 24).toDF("hour_of_day")

    return (
        hour_df
        .withColumn(
            "hour_key",
            F.col("hour_of_day").cast("int"),
        )
        .withColumn(
            "hour_label",
            F.format_string(
                "%02d:00",
                F.col("hour_of_day"),
            ),
        )
        .withColumn(
            "day_period",
            F.when(
                F.col("hour_of_day").between(0, 5),
                "Late Night",
            )
            .when(
                F.col("hour_of_day").between(6, 11),
                "Morning",
            )
            .when(
                F.col("hour_of_day").between(12, 17),
                "Afternoon",
            )
            .otherwise("Evening"),
        )
        .select(
            "hour_key",
            "hour_of_day",
            "hour_label",
            "day_period",
        )
    )


def build_dim_zone(zones_df: DataFrame) -> DataFrame:
    """
    Build the taxi-zone dimension.

    location_id is the source/business key.
    zone_key is the warehouse surrogate key.
    """

    return (
        zones_df
        .select(
            "location_id",
            "borough",
            "zone",
            "service_zone",
        )
        .dropDuplicates(["location_id"])
        .withColumn(
            "zone_key",
            F.abs(
                F.xxhash64(
                    F.col("location_id").cast("string")
                )
            ),
        )
        .select(
            "zone_key",
            "location_id",
            "borough",
            "zone",
            "service_zone",
        )
    )


def add_weather_categories(
    weather_df: DataFrame,
) -> DataFrame:
    """
    Add analytical weather categories while retaining exact measurements.
    """

    return (
        weather_df
        .withColumn(
            "weather_condition",
            F.when(
                F.col("weather_code") == 0,
                "Clear",
            )
            .when(
                F.col("weather_code").isin(1, 2, 3),
                "Cloudy",
            )
            .when(
                F.col("weather_code").isin(45, 48),
                "Fog",
            )
            .when(
                F.col("weather_code").between(51, 67),
                "Rain",
            )
            .when(
                F.col("weather_code").between(71, 77),
                "Snow",
            )
            .when(
                F.col("weather_code").between(80, 82),
                "Rain",
            )
            .when(
                F.col("weather_code").between(85, 86),
                "Snow",
            )
            .when(
                F.col("weather_code").between(95, 99),
                "Thunderstorm",
            )
            .otherwise("Other"),
        )
        .withColumn(
            "temperature_band",
            F.when(
                F.col("temperature_c") < 0,
                "Below 0 C",
            )
            .when(
                F.col("temperature_c") < 10,
                "0-10 C",
            )
            .when(
                F.col("temperature_c") < 20,
                "10-20 C",
            )
            .when(
                F.col("temperature_c") < 30,
                "20-30 C",
            )
            .otherwise("30+ C"),
        )
        .withColumn(
            "precipitation_band",
            F.when(
                F.col("precipitation_mm") == 0,
                "None",
            )
            .when(
                F.col("precipitation_mm") < 2.5,
                "Light",
            )
            .when(
                F.col("precipitation_mm") < 7.5,
                "Moderate",
            )
            .otherwise("Heavy"),
        )
        .withColumn(
            "is_raining",
            F.col("precipitation_mm") > 0,
        )
    )


def build_dim_weather(
    weather_df: DataFrame,
) -> DataFrame:
    """
    Build a descriptive weather dimension.
    """

    categorized = add_weather_categories(weather_df)

    return (
        categorized
        .select(
            "weather_condition",
            "temperature_band",
            "precipitation_band",
            "is_raining",
        )
        .dropDuplicates()
        .withColumn(
            "weather_key",
            F.abs(
                F.xxhash64(
                    "weather_condition",
                    "temperature_band",
                    "precipitation_band",
                    F.col("is_raining").cast("string"),
                )
            ),
        )
        .select(
            "weather_key",
            "weather_condition",
            "temperature_band",
            "precipitation_band",
            "is_raining",
        )
    )