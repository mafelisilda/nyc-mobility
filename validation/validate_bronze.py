# Databricks notebook source

# COMMAND ----------
# 1. IMPORTS

from calendar import monthrange

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
# 4. DISCOVER TAXI MONTHS DYNAMICALLY

taxi_months = sorted(
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

print(f"Taxi months found: {taxi_months}")

assert len(taxi_months) > 0, (
    "No taxi source months were found in Bronze."
)


# COMMAND ----------
# 5. VALIDATE TAXI FILE UNIQUENESS

print("=== TAXI BRONZE VALIDATION ===")

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


# One source file should map to one batch ID.
duplicate_taxi_files = (
    taxi
    .select(
        "source_file",
        "batch_id",
    )
    .distinct()
    .groupBy("source_file")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    "Taxi files associated with multiple batch IDs: "
    f"{duplicate_taxi_files}"
)


# One source month should map to one source file.
taxi_files_per_month = (
    taxi
    .select(
        "source_month",
        "source_file",
    )
    .distinct()
    .groupBy("source_month")
    .count()
)

months_with_multiple_files = (
    taxi_files_per_month
    .filter(F.col("count") > 1)
    .count()
)

print(
    "Taxi months associated with multiple source files: "
    f"{months_with_multiple_files}"
)


# COMMAND ----------
# 6. BUILD EXPECTED WEATHER COUNTS DYNAMICALLY

expected_weather_batches = {}

for source_month in taxi_months:

    year, month = map(
        int,
        source_month.split("-"),
    )

    expected_hours = (
        monthrange(year, month)[1] * 24
    )

    batch_id = (
        f"weather_"
        f"{source_month.replace('-', '_')}"
    )

    expected_weather_batches[
        batch_id
    ] = expected_hours

print(
    "Expected weather batches: "
    f"{expected_weather_batches}"
)


# COMMAND ----------
# 7. VALIDATE WEATHER BATCHES

print("=== WEATHER BRONZE VALIDATION ===")

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
    "Actual weather batches: "
    f"{actual_weather_batches}"
)


# Only compare weather batches that correspond to taxi months.
actual_required_weather_batches = {
    batch_id: actual_weather_batches.get(
        batch_id
    )
    for batch_id in expected_weather_batches
}

print(
    "Required weather batch counts: "
    f"{actual_required_weather_batches}"
)


# COMMAND ----------
# 8. VALIDATE TAXI ZONES

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

null_zone_ids = (
    zones
    .filter(
        F.col("LocationID").isNull()
    )
    .count()
)

print(f"Zone row count: {zone_count:,}")
print(
    f"Duplicate LocationIDs: "
    f"{duplicate_zone_ids}"
)
print(
    f"Null LocationIDs: "
    f"{null_zone_ids}"
)


# COMMAND ----------
# 9. ASSERT BRONZE QUALITY RULES

assert taxi.count() > 0

assert duplicate_taxi_files == 0
assert months_with_multiple_files == 0

assert (
    actual_required_weather_batches
    == expected_weather_batches
), (
    "Weather batches are missing or have "
    "unexpected hourly counts."
)

assert zone_count == 265
assert duplicate_zone_ids == 0
assert null_zone_ids == 0

print("All Bronze validation checks passed.")