# Databricks notebook source

# COMMAND ----------
# 1. IMPORTS
# PySpark functions are used to add metadata columns and query the Bronze table.

from src.ingestion.taxi import (
    add_bronze_metadata,
    file_already_processed,
)


# COMMAND ----------
# 2. RUN PARAMETER
# The same notebook can process March, April, or May.
# Examples: 2026-03, 2026-04, 2026-05
# The default is March because this is our initial load.

dbutils.widgets.text(
    "process_month",
    "2026-03",
    "Process Month"
)

process_month = dbutils.widgets.get("process_month")

print(f"Processing month: {process_month}")


# COMMAND ----------
# 3. SOURCE CONFIGURATION
# This is the folder containing the monthly NYC Green Taxi Parquet files.

GREEN_TAXI_BASE_PATH = (
    "r2://ftw-b12-dataengineering@6338489909d41c2f78a0a2345a684267."
    "r2.cloudflarestorage.com/groups/week-08/group-c/landing/green_taxi"
)

# Build the source filename dynamically from the process_month parameter.
source_file = f"green_tripdata_{process_month}.parquet"

# Build the complete path to the monthly Parquet file.
source_path = f"{GREEN_TAXI_BASE_PATH}/{source_file}"

# Create a readable batch identifier for lineage and debugging.
# Example: 2026-03 -> taxi_2026_03
batch_id = f"taxi_{process_month.replace('-', '_')}"

print(f"Source file: {source_file}")
print(f"Source path: {source_path}")
print(f"Batch ID: {batch_id}")


# COMMAND ----------
# 4. BRONZE DESTINATION
# All Green Taxi source records will eventually be stored in one Delta table.
#
# March is loaded first.
# April and May will later be appended incrementally.

BRONZE_TABLE = "nyc_mobility.bronze.green_taxi"

print(f"Bronze table: {BRONZE_TABLE}")


# COMMAND ----------
# 5. HELPER FUNCTION: CHECK WHETHER A FILE WAS ALREADY PROCESSED
# For the taxi source, we use the source filename as our ingestion signal.

def file_already_processed(table_name: str, file_name: str) -> bool:
    """
    Return True if the source file has already been loaded
    into the Bronze table.
    """

    try:
        return (
            spark.table(table_name)
            .filter(F.col("source_file") == file_name)
            .limit(1)
            .count()
            > 0
        )
    except Exception:
        return False


# Check whether the requested monthly file has already been loaded.
already_processed = file_already_processed(
    spark,
    BRONZE_TABLE,
    source_file
)

print(f"Already processed: {already_processed}")


# COMMAND ----------
# 6. INGEST THE FILE ONLY IF IT HAS NOT BEEN PROCESSED
# This gives us file-level idempotency:

if already_processed:

    print(
        f"Skipping {source_file}: "
        "this file has already been processed."
    )

else:

    # ------------------------------------------------------------------
    # 6A. READ THE RAW PARQUET FILE
    # ------------------------------------------------------------------
    # At the Bronze layer, we preserve the source columns as received.

    raw_df = spark.read.parquet(source_path)

    source_row_count = raw_df.count()

    print(f"Source row count: {source_row_count:,}")

    # Show a small preview of the source.
    display(raw_df.limit(10))


    # ------------------------------------------------------------------
    # 6B. ADD PROVENANCE / INGESTION METADATA
    # ------------------------------------------------------------------
    # Bronze should preserve the source data while adding information
    # that allows us to trace where each row came from.

    bronze_df = add_bronze_metadata(
        df=raw_df,
        source_file=source_file,
        source_month=process_month,
        batch_id=batch_id,
    )


    # ------------------------------------------------------------------
    # 6C. VERIFY THE BRONZE DATA BEFORE WRITING
    # ------------------------------------------------------------------

    print("Bronze schema:")
    bronze_df.printSchema()

    print(f"Bronze rows to ingest: {bronze_df.count():,}")

    # Preview important source and metadata columns.
    display(
        bronze_df.select(
            "lpep_pickup_datetime",
            "lpep_dropoff_datetime",
            "PULocationID",
            "DOLocationID",
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "total_amount",
            "source_system",
            "source_file",
            "source_month",
            "batch_id",
            "ingested_at"
        ).limit(10)
    )


    # ------------------------------------------------------------------
    # 6D. WRITE TO THE BRONZE DELTA TABLE
    # ------------------------------------------------------------------
    # Append is appropriate because each month represents a new batch.

    (
        bronze_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(BRONZE_TABLE)
    )

    print(
        f"Successfully loaded {source_file} "
        f"into {BRONZE_TABLE}."
    )


# COMMAND ----------
# 7. VALIDATE THE BRONZE TABLE
# This section runs whether the file was newly loaded or skipped.

try:
    # Read the Bronze table directly.
    bronze = spark.table(BRONZE_TABLE)

    # Count all rows currently stored in Bronze.
    bronze_row_count = bronze.count()

    print(f"Total Bronze rows: {bronze_row_count:,}")

    # Show the batches currently stored in Bronze.
    display(
        bronze
        .groupBy(
            "source_month",
            "source_file",
            "batch_id"
        )
        .count()
        .orderBy("source_month")
    )

    # Preview provenance metadata.
    display(
        bronze.select(
            "source_system",
            "source_file",
            "source_month",
            "batch_id",
            "ingested_at"
        ).limit(10)
    )

except Exception as e:
    print(f"Unable to read Bronze table {BRONZE_TABLE}.")
    print(f"Error: {e}")


# COMMAND ----------
# 8. FINAL RUN SUMMARY

print("Green Taxi Bronze ingestion finished.")
print(f"Process month: {process_month}")
print(f"Source file: {source_file}")
print(f"Already processed before this run: {already_processed}")