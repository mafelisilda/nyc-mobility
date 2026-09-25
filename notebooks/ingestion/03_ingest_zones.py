# Databricks notebook source

# COMMAND ----------
# 1. IMPORTS

from src.ingestion.zones import (
    add_zone_bronze_metadata,
    batch_already_processed,
    fetch_zone_rows,
)


# COMMAND ----------
# 2. SOURCE CONFIGURATION

SOURCE_URL = (
    "https://d37ci6vzurychx.cloudfront.net/"
    "misc/taxi_zone_lookup.csv"
)

BRONZE_TABLE = "nyc_mobility.bronze.taxi_zones"

# The taxi-zone lookup is a static reference dataset.
# We use one fixed batch identifier.
batch_id = "taxi_zones_lookup"

print(f"Source URL: {SOURCE_URL}")
print(f"Batch ID: {batch_id}")
print(f"Bronze table: {BRONZE_TABLE}")


# COMMAND ----------
# 3. CHECK WHETHER THIS LOOKUP WAS ALREADY PROCESSED

already_processed = batch_already_processed(
    spark,
    BRONZE_TABLE,
    batch_id,
)

print(f"Already processed: {already_processed}")


# COMMAND ----------
# 4. INGEST TAXI ZONES

if already_processed:

    print(
        f"Skipping {batch_id}: "
        "the taxi-zone lookup has already been processed."
    )

else:

    # --------------------------------------------------------------
    # 4A. DOWNLOAD THE CSV
    # --------------------------------------------------------------

    zone_rows = fetch_zone_rows(SOURCE_URL)

    print(
        f"Taxi-zone records received: "
        f"{len(zone_rows):,}"
    )


    # --------------------------------------------------------------
    # 4B. CREATE SPARK DATAFRAME
    # --------------------------------------------------------------
    #
    # At Bronze we preserve source column names.
    #
    # Expected columns:
    # LocationID
    # Borough
    # Zone
    # service_zone

    zones_df = spark.createDataFrame(zone_rows)

    print("Source schema:")
    zones_df.printSchema()

    display(zones_df.limit(10))


    # --------------------------------------------------------------
    # 4C. ADD BRONZE METADATA
    # --------------------------------------------------------------

    bronze_df = add_zone_bronze_metadata(
        df=zones_df,
        source_url=SOURCE_URL,
        batch_id=batch_id,
    )


    # --------------------------------------------------------------
    # 4D. VERIFY BEFORE WRITING
    # --------------------------------------------------------------

    print(
        f"Taxi-zone rows to ingest: "
        f"{bronze_df.count():,}"
    )

    display(
        bronze_df.select(
            "LocationID",
            "Borough",
            "Zone",
            "service_zone",
            "source_system",
            "batch_id",
            "ingested_at",
        ).limit(10)
    )


    # --------------------------------------------------------------
    # 4E. WRITE TO BRONZE
    # --------------------------------------------------------------

    (
        bronze_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(BRONZE_TABLE)
    )

    print(
        f"Successfully loaded {batch_id} "
        f"into {BRONZE_TABLE}."
    )


# COMMAND ----------
# 5. VALIDATE BRONZE TAXI ZONES

try:

    bronze = spark.table(BRONZE_TABLE)

    print(
        f"Total Bronze taxi-zone rows: "
        f"{bronze.count():,}"
    )

    display(
        bronze
        .groupBy("batch_id")
        .count()
    )

    display(
        bronze.select(
            "LocationID",
            "Borough",
            "Zone",
            "service_zone",
            "source_system",
            "batch_id",
            "ingested_at",
        ).limit(10)
    )

except Exception as e:

    print(
        f"Unable to read Bronze table "
        f"{BRONZE_TABLE}."
    )

    print(f"Error: {e}")


# COMMAND ----------
# 6. FINAL RUN SUMMARY

print("Taxi Zone Bronze ingestion finished.")
print(f"Batch ID: {batch_id}")
print(
    f"Already processed before this run: "
    f"{already_processed}"
)
