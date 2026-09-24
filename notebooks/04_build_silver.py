# Databricks notebook source

# COMMAND ----------
# 1. IMPORTS

from src.transformations.taxi import transform_taxi_silver
from src.transformations.weather import transform_weather_silver
from src.transformations.zones import transform_zones_silver


# COMMAND ----------
# 2. TABLE CONFIGURATION

BRONZE_TAXI = "nyc_mobility.bronze.green_taxi"
BRONZE_WEATHER = "nyc_mobility.bronze.weather"
BRONZE_ZONES = "nyc_mobility.bronze.taxi_zones"

SILVER_TAXI = "nyc_mobility.silver.taxi_trips"
SILVER_WEATHER = "nyc_mobility.silver.weather"
SILVER_ZONES = "nyc_mobility.silver.taxi_zones"


# COMMAND ----------
# 3. BUILD TAXI SILVER

taxi_bronze = spark.table(BRONZE_TAXI)

taxi_silver = transform_taxi_silver(taxi_bronze)

print(f"Taxi Bronze rows: {taxi_bronze.count():,}")
print(f"Taxi Silver rows: {taxi_silver.count():,}")

(
    taxi_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TAXI)
)


# COMMAND ----------
# 4. BUILD WEATHER SILVER

weather_bronze = spark.table(BRONZE_WEATHER)

weather_silver = transform_weather_silver(weather_bronze)

print(f"Weather Bronze rows: {weather_bronze.count():,}")
print(f"Weather Silver rows: {weather_silver.count():,}")

(
    weather_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_WEATHER)
)


# COMMAND ----------
# 5. BUILD ZONES SILVER

zones_bronze = spark.table(BRONZE_ZONES)

zones_silver = transform_zones_silver(zones_bronze)

print(f"Zones Bronze rows: {zones_bronze.count():,}")
print(f"Zones Silver rows: {zones_silver.count():,}")

(
    zones_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_ZONES)
)


# COMMAND ----------
# 6. VALIDATION

display(
    spark.table(SILVER_TAXI)
    .select(
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_location_id",
        "dropoff_location_id",
        "trip_distance",
        "trip_duration_minutes",
        "total_amount",
        "trip_hash",
    )
    .limit(10)
)

display(
    spark.table(SILVER_WEATHER)
    .select(
        "weather_datetime",
        "temperature_c",
        "precipitation_mm",
        "weather_code",
    )
    .limit(10)
)

display(
    spark.table(SILVER_ZONES)
    .select(
        "location_id",
        "borough",
        "zone",
        "service_zone",
    )
    .limit(10)
)

print("Silver build finished.")
