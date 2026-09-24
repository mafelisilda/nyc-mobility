# Databricks notebook source

# COMMAND ----------
# 1. IMPORTS

from src.ingestion.weather import (
    add_weather_bronze_metadata,
    batch_already_processed,
    build_weather_url,
    fetch_weather_json,
)


# COMMAND ----------
# 2. RUN PARAMETER
# The weather notebook processes the same month used by the taxi pipeline.

dbutils.widgets.text(
    "process_month",
    "2026-03",
    "Process Month"
)

process_month = dbutils.widgets.get("process_month")

print(f"Processing month: {process_month}")


# COMMAND ----------
# 3. WEATHER CONFIGURATION
#
# We use one representative point for New York City.
# This keeps the weather source at one hourly observation per hour.

NYC_LATITUDE = 40.7128
NYC_LONGITUDE = -74.0060

BRONZE_TABLE = "nyc_mobility.bronze.weather"

print(f"Bronze table: {BRONZE_TABLE}")


# COMMAND ----------
# 4. BUILD DATE RANGE
#
# Convert the month parameter into the first and last calendar dates
# needed by the historical weather API.

year, month = process_month.split("-")

year = int(year)
month = int(month)

if month == 12:
    next_year = year + 1
    next_month = 1
else:
    next_year = year
    next_month = month + 1


from datetime import date, timedelta


start_date = date(
    year,
    month,
    1
)

next_month_start = date(
    next_year,
    next_month,
    1
)

end_date = next_month_start - timedelta(days=1)

start_date_str = start_date.isoformat()
end_date_str = end_date.isoformat()

print(f"Start date: {start_date_str}")
print(f"End date: {end_date_str}")


# COMMAND ----------
# 5. BUILD API REQUEST

source_url = build_weather_url(
    latitude=NYC_LATITUDE,
    longitude=NYC_LONGITUDE,
    start_date=start_date_str,
    end_date=end_date_str,
)

batch_id = f"weather_{process_month.replace('-', '_')}"

print(f"Batch ID: {batch_id}")
print(f"Source URL: {source_url}")


# COMMAND ----------
# 6. CHECK WHETHER THIS WEATHER BATCH WAS ALREADY PROCESSED

already_processed = batch_already_processed(
    spark,
    BRONZE_TABLE,
    batch_id,
)

print(f"Already processed: {already_processed}")


# COMMAND ----------
# 7. INGEST WEATHER DATA

if already_processed:

    print(
        f"Skipping {batch_id}: "
        "this weather batch has already been processed."
    )

else:

    # --------------------------------------------------------------
    # 7A. CALL THE OPEN-METEO API
    # --------------------------------------------------------------

    weather_json = fetch_weather_json(source_url)

    print("Weather API response received.")


    # --------------------------------------------------------------
    # 7B. EXTRACT HOURLY ARRAYS
    # --------------------------------------------------------------
    #
    # Open-Meteo returns hourly values as parallel arrays.
    # Example:
    #
    # time            -> [hour1, hour2, ...]
    # temperature_2m  -> [value1, value2, ...]
    # precipitation   -> [value1, value2, ...]
    # weather_code    -> [value1, value2, ...]
    #
    # We convert them into one record per hour.

    hourly = weather_json["hourly"]

    rows = list(
        zip(
            hourly["time"],
            hourly["temperature_2m"],
            hourly["precipitation"],
            hourly["weather_code"],
        )
    )

    print(f"Hourly weather records received: {len(rows):,}")


    # --------------------------------------------------------------
    # 7C. CREATE SPARK DATAFRAME
    # --------------------------------------------------------------

    weather_df = spark.createDataFrame(
        rows,
        [
            "time",
            "temperature_2m",
            "precipitation",
            "weather_code",
        ],
    )

    display(weather_df.limit(10))


    # --------------------------------------------------------------
    # 7D. ADD BRONZE METADATA
    # --------------------------------------------------------------

    bronze_df = add_weather_bronze_metadata(
        df=weather_df,
        source_url=source_url,
        batch_id=batch_id,
    )


    # --------------------------------------------------------------
    # 7E. VERIFY BEFORE WRITING
    # --------------------------------------------------------------

    print("Bronze weather schema:")
    bronze_df.printSchema()

    print(
        f"Weather rows to ingest: "
        f"{bronze_df.count():,}"
    )

    display(
        bronze_df.select(
            "time",
            "temperature_2m",
            "precipitation",
            "weather_code",
            "source_system",
            "batch_id",
            "ingested_at",
        ).limit(10)
    )


    # --------------------------------------------------------------
    # 7F. WRITE TO BRONZE
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
# 8. VALIDATE BRONZE WEATHER

try:
    bronze = spark.table(BRONZE_TABLE)

    print(
        f"Total Bronze weather rows: "
        f"{bronze.count():,}"
    )

    display(
        bronze
        .groupBy("batch_id")
        .count()
        .orderBy("batch_id")
    )

    display(
        bronze.select(
            "time",
            "temperature_2m",
            "precipitation",
            "weather_code",
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
# 9. FINAL RUN SUMMARY

print("Weather Bronze ingestion finished.")
print(f"Process month: {process_month}")
print(f"Batch ID: {batch_id}")
print(
    f"Already processed before this run: "
    f"{already_processed}"
)
