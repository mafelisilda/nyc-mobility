# Databricks notebook source

# COMMAND ----------
# 1. IMPORTS

from pyspark.sql import functions as F

from src.transformations.gold import (
    add_weather_categories,
    build_dim_date,
    build_dim_hour,
    build_dim_weather,
    build_dim_zone,
)


# COMMAND ----------
# 2. TABLE CONFIGURATION

SILVER_TAXI = "nyc_mobility.silver.taxi_trips"
SILVER_WEATHER = "nyc_mobility.silver.weather"
SILVER_ZONES = "nyc_mobility.silver.taxi_zones"

GOLD_DIM_DATE = "nyc_mobility.gold.dim_date"
GOLD_DIM_HOUR = "nyc_mobility.gold.dim_hour"
GOLD_DIM_ZONE = "nyc_mobility.gold.dim_zone"
GOLD_DIM_WEATHER = "nyc_mobility.gold.dim_weather"
GOLD_FACT_TRIP = "nyc_mobility.gold.fact_trip"


# COMMAND ----------
# 3. LOAD SILVER SOURCES

taxi = spark.table(SILVER_TAXI)
weather = spark.table(SILVER_WEATHER)
zones = spark.table(SILVER_ZONES)

print(f"Taxi Silver rows: {taxi.count():,}")
print(f"Weather Silver rows: {weather.count():,}")
print(f"Zone Silver rows: {zones.count():,}")


# COMMAND ----------
# 4. BUILD DIM_DATE

dim_date = build_dim_date(taxi)

(
    dim_date.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_DIM_DATE)
)

print(f"dim_date rows: {dim_date.count():,}")


# COMMAND ----------
# 5. BUILD DIM_HOUR

dim_hour = build_dim_hour(spark)

(
    dim_hour.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_DIM_HOUR)
)

print(f"dim_hour rows: {dim_hour.count():,}")


# COMMAND ----------
# 6. BUILD DIM_ZONE

dim_zone = build_dim_zone(zones)

(
    dim_zone.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_DIM_ZONE)
)

print(f"dim_zone rows: {dim_zone.count():,}")


# COMMAND ----------
# 7. BUILD DIM_WEATHER

dim_weather = build_dim_weather(weather)

(
    dim_weather.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_DIM_WEATHER)
)

print(f"dim_weather rows: {dim_weather.count():,}")


# COMMAND ----------
# 8. PREPARE WEATHER FOR HOURLY JOIN
#
# Taxi trips are matched to weather at the pickup hour.

weather_enriched = add_weather_categories(weather)

weather_hourly = (
    weather_enriched
    .withColumn(
        "weather_hour",
        F.date_trunc(
            "hour",
            F.col("weather_datetime"),
        ),
    )
    .join(
        dim_weather,
        on=[
            "weather_condition",
            "temperature_band",
            "precipitation_band",
            "is_raining",
        ],
        how="left",
    )
    .select(
        "weather_hour",
        "weather_key",
        "temperature_c",
        "precipitation_mm",
        "weather_code",
    )
)


# COMMAND ----------
# 9. PREPARE TAXI FACT SOURCE

fact_source = (
    taxi
    .withColumn(
        "pickup_date_key",
        F.date_format(
            "pickup_datetime",
            "yyyyMMdd",
        ).cast("int"),
    )
    .withColumn(
        "dropoff_date_key",
        F.date_format(
            "dropoff_datetime",
            "yyyyMMdd",
        ).cast("int"),
    )
    .withColumn(
        "pickup_hour_key",
        F.hour("pickup_datetime"),
    )
    .withColumn(
        "dropoff_hour_key",
        F.hour("dropoff_datetime"),
    )
    .withColumn(
        "pickup_weather_hour",
        F.date_trunc(
            "hour",
            F.col("pickup_datetime"),
        ),
    )
)


# COMMAND ----------
# 10. JOIN PICKUP ZONE

pickup_zone = (
    dim_zone
    .select(
        F.col("location_id").alias(
            "pickup_location_id"
        ),
        F.col("zone_key").alias(
            "pickup_zone_key"
        ),
    )
)

fact_source = (
    fact_source
    .join(
        pickup_zone,
        on="pickup_location_id",
        how="left",
    )
)


# COMMAND ----------
# 11. JOIN DROPOFF ZONE

dropoff_zone = (
    dim_zone
    .select(
        F.col("location_id").alias(
            "dropoff_location_id"
        ),
        F.col("zone_key").alias(
            "dropoff_zone_key"
        ),
    )
)

fact_source = (
    fact_source
    .join(
        dropoff_zone,
        on="dropoff_location_id",
        how="left",
    )
)


# COMMAND ----------
# 12. JOIN HOURLY WEATHER

fact_source = (
    fact_source
    .join(
        weather_hourly,
        fact_source["pickup_weather_hour"]
        == weather_hourly["weather_hour"],
        how="left",
    )
)


# COMMAND ----------
# 13. BUILD FACT_TRIP

fact_trip = (
    fact_source
    .withColumn(
        "trip_key",
        F.abs(
            F.xxhash64("trip_hash")
        ),
    )
    .withColumn(
        "trip_count",
        F.lit(1),
    )
    .select(
        "trip_key",
        "trip_hash",
        "pickup_date_key",
        "dropoff_date_key",
        "pickup_hour_key",
        "dropoff_hour_key",
        "pickup_zone_key",
        "dropoff_zone_key",
        "weather_key",
        "passenger_count",
        "trip_distance",
        "trip_duration_minutes",
        "fare_amount",
        "total_amount",
        "temperature_c",
        "precipitation_mm",
        "trip_count",
    )
)


# COMMAND ----------
# 14. WRITE FACT_TRIP

(
    fact_trip.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_FACT_TRIP)
)

print(f"fact_trip rows: {fact_trip.count():,}")


# COMMAND ----------
# 15. GOLD VALIDATION

print("=== GOLD VALIDATION ===")

print(
    "dim_date:",
    spark.table(GOLD_DIM_DATE).count(),
)

print(
    "dim_hour:",
    spark.table(GOLD_DIM_HOUR).count(),
)

print(
    "dim_zone:",
    spark.table(GOLD_DIM_ZONE).count(),
)

print(
    "dim_weather:",
    spark.table(GOLD_DIM_WEATHER).count(),
)

print(
    "fact_trip:",
    spark.table(GOLD_FACT_TRIP).count(),
)


# COMMAND ----------
# 16. CHECK FOREIGN KEYS

fact = spark.table(GOLD_FACT_TRIP)

null_foreign_keys = (
    fact
    .filter(
        F.col("pickup_date_key").isNull()
        | F.col("dropoff_date_key").isNull()
        | F.col("pickup_hour_key").isNull()
        | F.col("dropoff_hour_key").isNull()
        | F.col("pickup_zone_key").isNull()
        | F.col("dropoff_zone_key").isNull()
        | F.col("weather_key").isNull()
    )
    .count()
)

print(
    f"Fact rows with null foreign keys: "
    f"{null_foreign_keys:,}"
)


# COMMAND ----------
# 17. CHECK FACT GRAIN

duplicate_trip_keys = (
    fact
    .groupBy("trip_key")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    f"Duplicate trip keys: "
    f"{duplicate_trip_keys:,}"
)

assert spark.table(GOLD_DIM_HOUR).count() == 24
assert fact.count() == taxi.count()
assert duplicate_trip_keys == 0
assert null_foreign_keys == 0

print("Gold dimensional model build passed.")
