# Databricks notebook source

# COMMAND ----------
# 1. IMPORTS

from pyspark.sql import functions as F


# COMMAND ----------
# 2. TABLE CONFIGURATION

SILVER_TAXI = "nyc_mobility.silver.taxi_trips"

GOLD_FACT_TRIP = "nyc_mobility.gold.fact_trip"
GOLD_DIM_DATE = "nyc_mobility.gold.dim_date"
GOLD_DIM_HOUR = "nyc_mobility.gold.dim_hour"
GOLD_DIM_ZONE = "nyc_mobility.gold.dim_zone"
GOLD_DIM_WEATHER = "nyc_mobility.gold.dim_weather"


# COMMAND ----------
# 3. LOAD TABLES

silver_taxi = spark.table(SILVER_TAXI)

fact = spark.table(GOLD_FACT_TRIP)
dim_date = spark.table(GOLD_DIM_DATE)
dim_hour = spark.table(GOLD_DIM_HOUR)
dim_zone = spark.table(GOLD_DIM_ZONE)
dim_weather = spark.table(GOLD_DIM_WEATHER)

print(f"Silver taxi rows: {silver_taxi.count():,}")
print(f"Gold fact rows: {fact.count():,}")
print(f"dim_date rows: {dim_date.count():,}")
print(f"dim_hour rows: {dim_hour.count():,}")
print(f"dim_zone rows: {dim_zone.count():,}")
print(f"dim_weather rows: {dim_weather.count():,}")


# COMMAND ----------
# 4. FACT GRAIN VALIDATION

print("=== FACT GRAIN VALIDATION ===")

duplicate_trip_keys = (
    fact
    .groupBy("trip_key")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

duplicate_trip_hashes = (
    fact
    .groupBy("trip_hash")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

print(
    f"Duplicate trip keys: "
    f"{duplicate_trip_keys}"
)

print(
    f"Duplicate trip hashes: "
    f"{duplicate_trip_hashes}"
)


# COMMAND ----------
# 5. NULL FOREIGN KEY VALIDATION

print("=== FOREIGN KEY NULL VALIDATION ===")

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
    f"Rows with null foreign keys: "
    f"{null_foreign_keys}"
)


# COMMAND ----------
# 6. DIMENSION UNIQUENESS

print("=== DIMENSION UNIQUENESS VALIDATION ===")

duplicate_date_keys = (
    dim_date
    .groupBy("date_key")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

duplicate_hour_keys = (
    dim_hour
    .groupBy("hour_key")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

duplicate_zone_keys = (
    dim_zone
    .groupBy("zone_key")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

duplicate_weather_keys = (
    dim_weather
    .groupBy("weather_key")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

print(f"Duplicate date keys: {duplicate_date_keys}")
print(f"Duplicate hour keys: {duplicate_hour_keys}")
print(f"Duplicate zone keys: {duplicate_zone_keys}")
print(f"Duplicate weather keys: {duplicate_weather_keys}")


# COMMAND ----------
# 7. REFERENTIAL INTEGRITY

print("=== REFERENTIAL INTEGRITY VALIDATION ===")

missing_pickup_dates = (
    fact.alias("f")
    .join(
        dim_date.alias("d"),
        F.col("f.pickup_date_key") == F.col("d.date_key"),
        how="left_anti",
    )
    .count()
)

missing_dropoff_dates = (
    fact.alias("f")
    .join(
        dim_date.alias("d"),
        F.col("f.dropoff_date_key") == F.col("d.date_key"),
        how="left_anti",
    )
    .count()
)

missing_pickup_hours = (
    fact.alias("f")
    .join(
        dim_hour.alias("h"),
        F.col("f.pickup_hour_key") == F.col("h.hour_key"),
        how="left_anti",
    )
    .count()
)

missing_dropoff_hours = (
    fact.alias("f")
    .join(
        dim_hour.alias("h"),
        F.col("f.dropoff_hour_key") == F.col("h.hour_key"),
        how="left_anti",
    )
    .count()
)

missing_pickup_zones = (
    fact.alias("f")
    .join(
        dim_zone.alias("z"),
        F.col("f.pickup_zone_key") == F.col("z.zone_key"),
        how="left_anti",
    )
    .count()
)

missing_dropoff_zones = (
    fact.alias("f")
    .join(
        dim_zone.alias("z"),
        F.col("f.dropoff_zone_key") == F.col("z.zone_key"),
        how="left_anti",
    )
    .count()
)

missing_weather = (
    fact.alias("f")
    .join(
        dim_weather.alias("w"),
        F.col("f.weather_key") == F.col("w.weather_key"),
        how="left_anti",
    )
    .count()
)

print(f"Missing pickup dates: {missing_pickup_dates}")
print(f"Missing dropoff dates: {missing_dropoff_dates}")
print(f"Missing pickup hours: {missing_pickup_hours}")
print(f"Missing dropoff hours: {missing_dropoff_hours}")
print(f"Missing pickup zones: {missing_pickup_zones}")
print(f"Missing dropoff zones: {missing_dropoff_zones}")
print(f"Missing weather keys: {missing_weather}")


# COMMAND ----------
# 8. FACT MEASURE VALIDATION

print("=== FACT MEASURE VALIDATION ===")

invalid_trip_count = (
    fact
    .filter(
        F.col("trip_count") != 1
    )
    .count()
)

negative_trip_duration = (
    fact
    .filter(
        F.col("trip_duration_minutes") < 0
    )
    .count()
)

print(
    f"Rows where trip_count is not 1: "
    f"{invalid_trip_count}"
)

print(
    f"Negative trip durations: "
    f"{negative_trip_duration}"
)


# COMMAND ----------
# 9. ASSERT GOLD QUALITY RULES

assert fact.count() == silver_taxi.count()

assert duplicate_trip_keys == 0
assert duplicate_trip_hashes == 0

assert null_foreign_keys == 0

assert dim_hour.count() == 24
assert dim_zone.count() == 265

assert duplicate_date_keys == 0
assert duplicate_hour_keys == 0
assert duplicate_zone_keys == 0
assert duplicate_weather_keys == 0

assert missing_pickup_dates == 0
assert missing_dropoff_dates == 0
assert missing_pickup_hours == 0
assert missing_dropoff_hours == 0
assert missing_pickup_zones == 0
assert missing_dropoff_zones == 0
assert missing_weather == 0

assert invalid_trip_count == 0
assert negative_trip_duration == 0

print("All Gold validation checks passed.")