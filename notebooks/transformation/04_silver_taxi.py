# Databricks notebook source

# COMMAND ----------
# 1. IMPORTS

from src.transformations.taxi import split_taxi_records


# COMMAND ----------
# 2. TABLE CONFIGURATION

BRONZE_TABLE = "nyc_mobility.bronze.green_taxi"
SILVER_TABLE = "nyc_mobility.silver.taxi_trips"
QUARANTINE_TABLE = "nyc_mobility.silver.taxi_trips_quarantine"


# COMMAND ----------
# 3. LOAD BRONZE TAXI DATA

bronze_taxi = spark.table(
    BRONZE_TABLE
)

bronze_row_count = bronze_taxi.count()

print(
    f"Bronze taxi rows: "
    f"{bronze_row_count:,}"
)


# COMMAND ----------
# 4. SPLIT VALID AND INVALID RECORDS

silver_taxi, quarantine_taxi = split_taxi_records(
    bronze_taxi
)

silver_row_count = silver_taxi.count()
quarantine_row_count = quarantine_taxi.count()

print(
    f"Valid Silver taxi rows: "
    f"{silver_row_count:,}"
)

print(
    f"Quarantined taxi rows: "
    f"{quarantine_row_count:,}"
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
    f"Written valid records to: "
    f"{SILVER_TABLE}"
)


# COMMAND ----------
# 6. WRITE QUARANTINE TABLE

(
    quarantine_taxi.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(QUARANTINE_TABLE)
)

print(
    f"Written quarantined records to: "
    f"{QUARANTINE_TABLE}"
)


# COMMAND ----------
# 7. VERIFY SILVER RESULT

silver_result = spark.table(
    SILVER_TABLE
)

print(
    f"Final Silver taxi rows: "
    f"{silver_result.count():,}"
)

display(
    silver_result
    .select(
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_location_id",
        "dropoff_location_id",
        "trip_duration_minutes",
        "trip_hash",
        "source_month",
    )
    .limit(10)
)


# COMMAND ----------
# 8. VERIFY QUARANTINE RESULT

quarantine_result = spark.table(
    QUARANTINE_TABLE
)

print(
    f"Final quarantined taxi rows: "
    f"{quarantine_result.count():,}"
)

display(
    quarantine_result
    .select(
        "lpep_pickup_datetime",
        "lpep_dropoff_datetime",
        "pickup_location_id",
        "dropoff_location_id",
        "source_month",
        "source_file",
        "quarantine_reason",
        "quarantined_at",
    )
    .orderBy(
        "quarantine_reason"
    )
)


# COMMAND ----------
# 9. QUARANTINE SUMMARY

quarantine_summary = (
    quarantine_result
    .groupBy(
        "quarantine_reason"
    )
    .count()
    .orderBy(
        "quarantine_reason"
    )
)

print(
    "Quarantine counts by reason:"
)

display(
    quarantine_summary
)


# COMMAND ----------
# 10. RECONCILIATION CHECK

print(
    f"Bronze rows: "
    f"{bronze_row_count:,}"
)

print(
    f"Silver rows: "
    f"{silver_row_count:,}"
)

print(
    f"Quarantine rows: "
    f"{quarantine_row_count:,}"
)

print(
    "Silver taxi build and quarantine "
    "processing completed successfully."
)