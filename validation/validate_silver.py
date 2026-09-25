# Databricks notebook source

# COMMAND ----------
# 1. IMPORTS

from calendar import monthrange

from pyspark.sql import functions as F

from src.quality.checks import (
    count_duplicates,
    count_nulls,
)


# COMMAND ----------
# 2. TABLE CONFIGURATION

BRONZE_TAXI = "nyc_mobility.bronze.green_taxi"

SILVER_TAXI = "nyc_mobility.silver.taxi_trips"
SILVER_WEATHER = "nyc_mobility.silver.weather"
SILVER_ZONES = "nyc_mobility.silver.taxi_zones"


# COMMAND ----------
# 3. LOAD TABLES

bronze_taxi = spark.table(BRONZE_TAXI)

taxi = spark.table(SILVER_TAXI)
weather = spark.table(SILVER_WEATHER)
zones = spark.table(SILVER_ZONES)


# COMMAND ----------
# 4. DISCOVER EXPECTED MONTHS

expected_months = sorted(
    {
        row["source_month"]
        for row in (
            bronze_taxi
            .select("source_month")
            .distinct()
            .collect()
        )
        if row["source_month"] is not None
    }
)

print(
    f"Expected Silver months: "
    f"{expected_months}"
)

assert len(expected_months) > 0


# COMMAND ----------
# 5. TAXI DATA QUALITY CHECKS

print("=== TAXI SILVER VALIDATION ===")

taxi_row_count = taxi.count()

print(
    f"Taxi Silver row count: "
    f"{taxi_row_count:,}"
)

taxi_nulls = count_nulls(
    taxi,
    [
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_location_id",
        "dropoff_location_id",
        "trip_hash",
        "source_month",
    ],
)

print(
    f"Taxi null counts: "
    f"{taxi_nulls}"
)

taxi_duplicate_hashes = (
    count_duplicates(
        taxi,
        ["trip_hash"],
    )
)

print(
    "Duplicate trip hashes: "
    f"{taxi_duplicate_hashes}"
)

negative_trip_durations = (
    taxi
    .filter(
        F.col("trip_duration_minutes") < 0
    )
    .count()
)

print(
    "Negative trip durations: "
    f"{negative_trip_durations}"
)


# COMMAND ----------
# 6. VALIDATE TAXI SOURCE MONTH ALIGNMENT

taxi_month_mismatches = (
    taxi
    .filter(
        F.date_format(
            F.col("pickup_datetime"),
            "yyyy-MM",
        )
        != F.col("source_month")
    )
    .count()
)

print(
    "Taxi rows where pickup month "
    "does not match source_month: "
    f"{taxi_month_mismatches}"
)


actual_taxi_months = sorted(
    {
        row["source_month"]
        for row in (
            taxi
            .select("source_month")
            .distinct()
            .collect()
        )
        if row["source_month"] is not None
    }
)

print(
    f"Taxi Silver months: "
    f"{actual_taxi_months}"
)


# COMMAND ----------
# 7. WEATHER DATA QUALITY CHECKS

print("=== WEATHER SILVER VALIDATION ===")

weather_row_count = weather.count()

print(
    f"Weather Silver row count: "
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

print(
    f"Weather null counts: "
    f"{weather_nulls}"
)

weather_duplicate_hours = (
    count_duplicates(
        weather,
        ["weather_datetime"],
    )
)

print(
    "Duplicate weather hours: "
    f"{weather_duplicate_hours}"
)


# COMMAND ----------
# 8. VALIDATE WEATHER COUNTS BY MONTH

weather_month_counts_df = (
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

display(weather_month_counts_df)

actual_weather_counts = {
    row["weather_month"]: row["count"]
    for row in (
        weather_month_counts_df.collect()
    )
}

expected_weather_counts = {}

for source_month in expected_months:

    year, month = map(
        int,
        source_month.split("-"),
    )

    expected_weather_counts[
        source_month
    ] = (
        monthrange(year, month)[1]
        * 24
    )

print(
    "Expected weather counts: "
    f"{expected_weather_counts}"
)

print(
    "Actual weather counts: "
    f"{actual_weather_counts}"
)


# Compare only the months expected from taxi Bronze.
actual_required_weather_counts = {
    source_month: actual_weather_counts.get(
        source_month
    )
    for source_month in expected_months
}


# COMMAND ----------
# 9. TAXI ZONE DATA QUALITY CHECKS

print("=== TAXI ZONE SILVER VALIDATION ===")

zone_row_count = zones.count()

zone_nulls = count_nulls(
    zones,
    [
        "location_id",
        "borough",
        "zone",
    ],
)

duplicate_location_ids = (
    count_duplicates(
        zones,
        ["location_id"],
    )
)

print(
    f"Zone row count: "
    f"{zone_row_count:,}"
)

print(
    f"Zone null counts: "
    f"{zone_nulls}"
)

print(
    "Duplicate location IDs: "
    f"{duplicate_location_ids}"
)


# COMMAND ----------
# 10. ASSERT SILVER QUALITY RULES

assert taxi_row_count > 0

assert all(
    value == 0
    for value in taxi_nulls.values()
)

assert taxi_duplicate_hashes == 0
assert negative_trip_durations == 0
assert taxi_month_mismatches == 0

assert (
    actual_taxi_months
    == expected_months
)

assert all(
    value == 0
    for value in weather_nulls.values()
)

assert weather_duplicate_hours == 0

assert (
    actual_required_weather_counts
    == expected_weather_counts
), (
    "Weather Silver does not contain "
    "the expected hourly records for "
    "all taxi source months."
)

assert zone_row_count == 265

assert all(
    value == 0
    for value in zone_nulls.values()
)

assert duplicate_location_ids == 0

print("All Silver validation checks passed.")