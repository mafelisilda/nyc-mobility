# Databricks notebook source

# COMMAND ----------
# 1. IMPORTS

from src.quality.checks import (
    count_duplicates,
    count_nulls,
    count_rows_outside_date_range,
)

from pyspark.sql import functions as F


# COMMAND ----------
# 2. TABLE CONFIGURATION

SILVER_TAXI = "nyc_mobility.silver.taxi_trips"
SILVER_WEATHER = "nyc_mobility.silver.weather"
SILVER_ZONES = "nyc_mobility.silver.taxi_zones"


# COMMAND ----------
# 3. LOAD SILVER TABLES

taxi = spark.table(SILVER_TAXI)
weather = spark.table(SILVER_WEATHER)
zones = spark.table(SILVER_ZONES)


# COMMAND ----------
# 4. TAXI DATA QUALITY CHECKS

print("=== TAXI DATA QUALITY ===")

taxi_row_count = taxi.count()

print(f"Taxi row count: {taxi_row_count:,}")

taxi_nulls = count_nulls(
    taxi,
    [
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_location_id",
        "dropoff_location_id",
        "trip_hash",
    ],
)

print(f"Taxi null counts: {taxi_nulls}")

taxi_duplicate_hashes = count_duplicates(
    taxi,
    ["trip_hash"],
)

print(
    f"Duplicate trip hashes: "
    f"{taxi_duplicate_hashes}"
)

invalid_duration_count = (
    taxi
    .filter(
        F.col("trip_duration_minutes") < 0
    )
    .count()
)

print(
    f"Negative trip durations: "
    f"{invalid_duration_count}"
)

taxi_outside_range = count_rows_outside_date_range(
    taxi,
    "pickup_datetime",
    "2026-03-01",
    "2026-05-31",
)

print(
    f"Taxi rows outside expected date range: "
    f"{taxi_outside_range}"
)


# COMMAND ----------
# 5. WEATHER DATA QUALITY CHECKS

print("=== WEATHER DATA QUALITY ===")

weather_row_count = weather.count()

print(
    f"Weather row count: "
    f"{weather_row_count:,}"
)

weather_nulls = count_nulls(
    weather,
    [
        "weather_datetime",
        "temperature_c",
        "precipitation_mm",
        "weather_code",
    ],
)

print(f"Weather null counts: {weather_nulls}")

weather_duplicate_hours = count_duplicates(
    weather,
    ["weather_datetime"],
)

print(
    f"Duplicate weather hours: "
    f"{weather_duplicate_hours}"
)

weather_outside_range = count_rows_outside_date_range(
    weather,
    "weather_datetime",
    "2026-03-01",
    "2026-05-31",
)

print(
    f"Weather rows outside expected date range: "
    f"{weather_outside_range}"
)

# Check hourly record counts by month

weather_month_counts = (
    weather
    .withColumn(
        "weather_month",
        F.date_format(
            "weather_datetime",
            "yyyy-MM",
        ),
    )
    .groupBy("weather_month")
    .count()
    .orderBy("weather_month")
)

display(weather_month_counts)

expected_weather_counts = {
    "2026-03": 744,
    "2026-04": 720,
    "2026-05": 744,
}

actual_weather_counts = {
    row["weather_month"]: row["count"]
    for row in weather_month_counts.collect()
}

print(f"Weather counts by month: {actual_weather_counts}")

# COMMAND ----------
# 6. ZONE DATA QUALITY CHECKS

print("=== TAXI ZONE DATA QUALITY ===")

zone_row_count = zones.count()

print(
    f"Zone row count: "
    f"{zone_row_count:,}"
)

zone_nulls = count_nulls(
    zones,
    [
        "location_id",
        "borough",
        "zone",
    ],
)

print(f"Zone null counts: {zone_nulls}")

duplicate_location_ids = count_duplicates(
    zones,
    ["location_id"],
)

print(
    f"Duplicate location IDs: "
    f"{duplicate_location_ids}"
)


# COMMAND ----------
# 7. ASSERT EXPECTED QUALITY RULES

assert taxi_row_count > 0
assert weather_row_count == 2208
assert zone_row_count == 265

assert all(
    value == 0
    for value in taxi_nulls.values()
)

assert taxi_duplicate_hashes == 0
assert invalid_duration_count == 0
assert taxi_outside_range == 0

assert all(
    value == 0
    for value in weather_nulls.values()
)

assert weather_duplicate_hours == 0
assert weather_outside_range == 0
assert actual_weather_counts == expected_weather_counts

assert zone_nulls["location_id"] == 0
assert zone_nulls["borough"] == 0
assert zone_nulls["zone"] == 0
assert duplicate_location_ids == 0

print("All Silver data-quality checks passed.")
