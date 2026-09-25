# Databricks notebook source

# COMMAND ----------
# 1. IMPORTS

from calendar import monthrange
from datetime import date

from src.ingestion.weather import (
    add_weather_bronze_metadata,
    batch_already_processed,
    build_weather_url,
    fetch_weather_json,
)


# COMMAND ----------
# 2. CONFIGURATION

TAXI_BRONZE_TABLE = (
    "nyc_mobility.bronze.green_taxi"
)

WEATHER_BRONZE_TABLE = (
    "nyc_mobility.bronze.weather"
)

NYC_LATITUDE = 40.7128
NYC_LONGITUDE = -74.0060

MIN_SOURCE_MONTH = "2026-03"


# COMMAND ----------
# 3. DISCOVER TAXI MONTHS

taxi = spark.table(
    TAXI_BRONZE_TABLE
)

taxi_months = sorted(
    {
        row["source_month"]
        for row in (
            taxi
            .select("source_month")
            .distinct()
            .collect()
        )
        if (
            row["source_month"]
            and row["source_month"]
            >= MIN_SOURCE_MONTH
        )
    }
)

print(
    f"Taxi months available: "
    f"{taxi_months}"
)

assert len(taxi_months) > 0, (
    "No taxi source months are available."
)


# COMMAND ----------
# 4. FIND WEATHER MONTHS THAT ARE MISSING

missing_weather_months = []

for process_month in taxi_months:

    batch_id = (
        f"weather_"
        f"{process_month.replace('-', '_')}"
    )

    already_processed = (
        batch_already_processed(
            spark,
            WEATHER_BRONZE_TABLE,
            batch_id,
        )
    )

    if already_processed:
        print(
            f"SKIP: {batch_id} "
            "has already been processed."
        )

    else:
        missing_weather_months.append(
            process_month
        )

print(
    "Weather months to ingest: "
    f"{missing_weather_months}"
)


# COMMAND ----------
# 5. FETCH MISSING WEATHER MONTHS

for process_month in missing_weather_months:

    print(
        f"===== INGESTING WEATHER "
        f"{process_month} ====="
    )

    year, month = map(
        int,
        process_month.split("-"),
    )

    days_in_month = monthrange(
        year,
        month,
    )[1]

    start_date = date(
        year,
        month,
        1,
    )

    end_date = date(
        year,
        month,
        days_in_month,
    )

    start_date_str = (
        start_date.isoformat()
    )

    end_date_str = (
        end_date.isoformat()
    )

    source_url = build_weather_url(
        latitude=NYC_LATITUDE,
        longitude=NYC_LONGITUDE,
        start_date=start_date_str,
        end_date=end_date_str,
    )

    batch_id = (
        f"weather_"
        f"{process_month.replace('-', '_')}"
    )

    weather_json = fetch_weather_json(
        source_url
    )

    hourly = weather_json["hourly"]

    rows = list(
        zip(
            hourly["time"],
            hourly["temperature_2m"],
            hourly["precipitation"],
            hourly["weather_code"],
        )
    )

    expected_hours = (
        days_in_month * 24
    )

    print(
        f"Weather records received: "
        f"{len(rows):,}"
    )

    print(
        f"Expected hourly records: "
        f"{expected_hours:,}"
    )

    assert len(rows) == expected_hours, (
        f"Incomplete weather batch for "
        f"{process_month}: "
        f"expected {expected_hours}, "
        f"received {len(rows)}."
    )

    weather_df = spark.createDataFrame(
        rows,
        [
            "time",
            "temperature_2m",
            "precipitation",
            "weather_code",
        ],
    )

    bronze_df = (
        add_weather_bronze_metadata(
            df=weather_df,
            source_url=source_url,
            batch_id=batch_id,
        )
    )

    (
        bronze_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(
            WEATHER_BRONZE_TABLE
        )
    )

    print(
        f"Loaded {batch_id}: "
        f"{len(rows):,} rows"
    )


# COMMAND ----------
# 6. FINAL WEATHER VALIDATION

weather = spark.table(
    WEATHER_BRONZE_TABLE
)

weather_batch_counts = (
    weather
    .groupBy("batch_id")
    .count()
    .orderBy("batch_id")
)

display(weather_batch_counts)

print(
    f"Total Weather Bronze rows: "
    f"{weather.count():,}"
)

print(
    "Automatic weather ingestion "
    "completed successfully."
)