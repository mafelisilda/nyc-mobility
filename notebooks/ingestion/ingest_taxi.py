# Databricks notebook source

# COMMAND ----------
# 1. IMPORTS

import re

from src.ingestion.taxi import (
    add_bronze_metadata,
    file_already_processed,
)


# COMMAND ----------
# 2. CONFIGURATION

GREEN_TAXI_BASE_PATH = (
    "r2://ftw-b12-dataengineering@6338489909d41c2f78a0a2345a684267."
    "r2.cloudflarestorage.com/groups/week-08/group-c/landing/green_taxi"
)

BRONZE_TABLE = "nyc_mobility.bronze.green_taxi"

# Ignore source files before the project start month.
MIN_SOURCE_MONTH = "2026-03"

# Expected filename format:
# green_tripdata_2026-03.parquet
FILE_PATTERN = re.compile(
    r"^green_tripdata_(\d{4}-\d{2})\.parquet$"
)


# COMMAND ----------
# 3. DISCOVER AVAILABLE TAXI FILES

landing_entries = dbutils.fs.ls(
    GREEN_TAXI_BASE_PATH
)

available_files = []

for entry in landing_entries:

    match = FILE_PATTERN.match(entry.name)

    if not match:
        continue

    source_month = match.group(1)

    if source_month < MIN_SOURCE_MONTH:
        continue

    available_files.append(
        {
            "source_file": entry.name,
            "source_month": source_month,
            "source_path": entry.path,
        }
    )

available_files = sorted(
    available_files,
    key=lambda x: x["source_month"],
)

print("Discovered taxi source files:")

for item in available_files:
    print(
        f"  {item['source_month']} -> "
        f"{item['source_file']}"
    )

assert len(available_files) > 0, (
    "No valid Green Taxi Parquet files were found."
)


# COMMAND ----------
# 4. IDENTIFY NEW FILES

new_files = []

for item in available_files:

    already_processed = file_already_processed(
        spark,
        BRONZE_TABLE,
        item["source_file"],
    )

    if already_processed:
        print(
            f"SKIP: {item['source_file']} "
            "has already been processed."
        )

    else:
        new_files.append(item)

print(
    f"New taxi files to ingest: "
    f"{len(new_files)}"
)

for item in new_files:
    print(f"  {item['source_file']}")


# COMMAND ----------
# 5. INGEST NEW FILES

for item in new_files:

    source_file = item["source_file"]
    source_month = item["source_month"]
    source_path = item["source_path"]

    batch_id = (
        f"taxi_{source_month.replace('-', '_')}"
    )

    print(
        f"===== INGESTING {source_file} ====="
    )

    raw_df = spark.read.parquet(
        source_path
    )

    source_row_count = raw_df.count()

    print(
        f"Source rows: "
        f"{source_row_count:,}"
    )

    assert source_row_count > 0, (
        f"{source_file} contains no records."
    )

    bronze_df = add_bronze_metadata(
        df=raw_df,
        source_file=source_file,
        source_month=source_month,
        batch_id=batch_id,
    )

    (
        bronze_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(BRONZE_TABLE)
    )

    print(
        f"Loaded {source_file}: "
        f"{source_row_count:,} rows"
    )


# COMMAND ----------
# 6. FINAL TAXI BRONZE VALIDATION

bronze = spark.table(
    BRONZE_TABLE
)

print(
    f"Total Taxi Bronze rows: "
    f"{bronze.count():,}"
)

taxi_batches = (
    bronze
    .groupBy(
        "source_month",
        "source_file",
        "batch_id",
    )
    .count()
    .orderBy("source_month")
)

display(taxi_batches)

print(
    "Automatic Green Taxi ingestion "
    "completed successfully."
)