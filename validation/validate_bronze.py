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
# 5. VALIDATE TAXI FILE / BATCH STRUCTURE

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


# One source file should map to exactly one batch ID.
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


# One source month should map to exactly one source file.
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
# 6. VALIDATE TAXI PROVENANCE METADATA

taxi_metadata_nulls = {
    column: (
        taxi
        .filter(F.col(column).isNull())
        .count()
    )
    for column in [
        "source_system",
        "source_file",
        "source_month",
        "batch_id",
        "ingested_at",
    ]
}

print(
    f"Taxi metadata null counts: "
    f"{taxi_metadata_nulls}"
)


# COMMAND ----------
# 7. BUILD EXPECTED WEATHER COUNTS

expected_weather_counts = {}

for source_month in taxi_months:

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
    f"Expected weather counts: "
    f"{expected_weather_counts}"
)


# COMMAND ----------
# 8. DISCOVER WEATHER MONTHS

print("=== WEATHER BRONZE VALIDATION ===")

weather_month_counts = (
    weather
    .withColumn(
        "weather_month",
        F.concat_ws(
            "-",
            F.regexp_extract(
                "batch_id",
                r"weather_(\d{4})_(\d{2})",
                1,
            ),
            F.regexp_extract(
                "batch_id",
                r"weather_(\d{4})_(\d{2})",
                2,
            ),
        ),
    )
    .groupBy("weather_month")
    .count()
    .orderBy("weather_month")
)

display(weather_month_counts)

actual_weather_counts = {
    row["weather_month"]: row["count"]
    for row in weather_month_counts.collect()
}

weather_months = sorted(
    actual_weather_counts.keys()
)

print(
    f"Weather months found: "
    f"{weather_months}"
)

print(
    f"Actual weather counts: "
    f"{actual_weather_counts}"
)


# COMMAND ----------
# 9. VALIDATE WEATHER PROVENANCE METADATA

weather_metadata_nulls = {
    column: (
        weather
        .filter(F.col(column).isNull())
        .count()
    )
    for column in [
        "source_system",
        "source_url",
        "batch_id",
        "ingested_at",
    ]
}

print(
    f"Weather metadata null counts: "
    f"{weather_metadata_nulls}"
)


# COMMAND ----------
# 10. VALIDATE TAXI ZONES

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

zone_metadata_nulls = {
    column: (
        zones
        .filter(F.col(column).isNull())
        .count()
    )
    for column in [
        "source_system",
        "source_url",
        "batch_id",
        "ingested_at",
    ]
}

print(f"Zone row count: {zone_count:,}")
print(f"Duplicate LocationIDs: {duplicate_zone_ids}")
print(f"Null LocationIDs: {null_zone_ids}")
print(
    f"Zone metadata null counts: "
    f"{zone_metadata_nulls}"
)


# COMMAND ----------
# 11. ASSERT BRONZE QUALITY RULES

# Taxi
assert taxi.count() > 0
assert duplicate_taxi_files == 0
assert months_with_multiple_files == 0

assert all(
    value == 0
    for value in taxi_metadata_nulls.values()
), (
    "Taxi Bronze contains null provenance metadata."
)


# Taxi and Weather must cover the exact same months.
assert weather_months == taxi_months, (
    "Taxi and Weather Bronze months do not match exactly. "
    f"Taxi months: {taxi_months}. "
    f"Weather months: {weather_months}."
)


# Each weather month must contain the expected number of hourly rows.
assert (
    actual_weather_counts
    == expected_weather_counts
), (
    "Weather Bronze contains missing, extra, "
    "or incomplete monthly batches."
)

assert all(
    value == 0
    for value in weather_metadata_nulls.values()
), (
    "Weather Bronze contains null provenance metadata."
)


# Zones
assert zone_count == 265
assert duplicate_zone_ids == 0
assert null_zone_ids == 0

assert all(
    value == 0
    for value in zone_metadata_nulls.values()
), (
    "Taxi Zone Bronze contains null provenance metadata."
)


print("All Bronze validation checks passed.")