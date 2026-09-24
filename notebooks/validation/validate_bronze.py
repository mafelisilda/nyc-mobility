# Databricks notebook source

# COMMAND ----------
# 1. IMPORTS

from pyspark.sql import functions as F


# COMMAND ----------
# 2. TABLE CONFIGURATION

BRONZE_TAXI = "nyc_mobility.bronze.green_taxi"
BRONZE_WEATHER = "nyc_mobility.bronze.weather"
BRONZE_ZONES = "nyc_mobility.bronze.taxi_zones"


# COMMAND ----------
# 3. LOAD BRONZE TABLES

taxi = spark.table(BRONZE_TAXI)
weather = spark.table(BRONZE_WEATHER)
zones = spark.table(BRONZE_ZONES)

print(f"Taxi Bronze rows: {taxi.count():,}")
print(f"Weather Bronze rows: {weather.count():,}")
print(f"Zones Bronze rows: {zones.count():,}")


# COMMAND ----------
# 4. TAXI BRONZE VALIDATION

print("=== TAXI BRONZE VALIDATION ===")

expected_taxi_months = {
    "2026-03",
    "2026-04",
    "2026-05",
}

actual_taxi_months = {
    row["source_month"]
    for row in (
        taxi
        .select("source_month")
        .distinct()
        .collect()
    )
}

print(f"Taxi months found: {actual_taxi_months}")

taxi_batch_counts = (
    taxi
    .groupBy(
        "source_month",
        "source_file",
        "batch_id",
    )
    .count()
    .orderBy("source_month")
)

display(taxi_batch_counts)

duplicate_taxi_files = (
    taxi
    .select(
        "source_file",
        "batch_id",
    )
    .distinct()
    .groupBy("source_file")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

print(
    f"Taxi files associated with multiple batch IDs: "
    f"{duplicate_taxi_files}"
)


# COMMAND ----------
# 5. WEATHER BRONZE VALIDATION

print("=== WEATHER BRONZE VALIDATION ===")

expected_weather_batches = {
    "weather_2026_03": 744,
    "weather_2026_04": 720,
    "weather_2026_05": 744,
}

actual_weather_batches = {
    row["batch_id"]: row["count"]
    for row in (
        weather
        .groupBy("batch_id")
        .count()
        .collect()
    )
}

print(
    f"Weather batch counts: "
    f"{actual_weather_batches}"
)


# COMMAND ----------
# 6. ZONE BRONZE VALIDATION

print("=== TAXI ZONE BRONZE VALIDATION ===")

zone_count = zones.count()

duplicate_zone_ids = (
    zones
    .groupBy("LocationID")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

print(f"Zone row count: {zone_count:,}")
print(
    f"Duplicate LocationIDs: "
    f"{duplicate_zone_ids}"
)


# COMMAND ----------
# 7. ASSERT BRONZE QUALITY RULES

assert taxi.count() > 0
assert actual_taxi_months == expected_taxi_months
assert duplicate_taxi_files == 0

assert weather.count() == 2208
assert actual_weather_batches == expected_weather_batches

assert zone_count == 265
assert duplicate_zone_ids == 0

print("All Bronze validation checks passed.")