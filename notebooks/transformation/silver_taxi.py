# Databricks notebook source

# COMMAND ----------
# 1. IMPORTS

from src.transformations.taxi import transform_taxi_silver

# COMMAND ----------
# 2. TABLE CONFIGURATION

BRONZE_TABLE = "nyc_mobility.bronze.green_taxi"
SILVER_TABLE = "nyc_mobility.silver.taxi_trips"


# COMMAND ----------
# 3. LOAD BRONZE TAXI DATA

bronze_taxi = spark.table(BRONZE_TABLE)

print(
    f"Bronze taxi rows: "
    f"{bronze_taxi.count():,}"
)


# COMMAND ----------
# 4. TRANSFORM TO SILVER

silver_taxi = transform_taxi_silver(
    bronze_taxi
)

print(
    f"Silver taxi rows after transformation: "
    f"{silver_taxi.count():,}"
)


# COMMAND ----------
# 5. WRITE SILVER TABLE

(
    silver_taxi.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

print(
    f"Written to: {SILVER_TABLE}"
)


# COMMAND ----------
# 6. VERIFY RESULT

result = spark.table(SILVER_TABLE)

print(
    f"Final Silver taxi rows: "
    f"{result.count():,}"
)

display(
    result
    .select(
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_location_id",
        "dropoff_location_id",
        "trip_distance",
        "trip_duration_minutes",
        "fare_amount",
        "total_amount",
        "source_month",
        "trip_hash",
    )
    .limit(10)
)

print(
    "Silver taxi build completed successfully."
)