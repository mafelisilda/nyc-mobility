# NYC Mobility Data Pipeline Architecture

## Overview

This project implements an end-to-end data engineering pipeline for NYC mobility analysis using Databricks and Delta Lake.

The pipeline integrates three data sources:

1. NYC Green Taxi trip data
2. Open-Meteo historical hourly weather data
3. NYC Taxi Zone lookup data

The architecture follows a Medallion pattern:

INGEST → BRONZE → CLEAN / STANDARDIZE → SILVER → INTEGRATE → GOLD

## Data Sources

### NYC Green Taxi

Source format: Parquet

Current source files:

- `green_tripdata_2026-03.parquet`
- `green_tripdata_2026-04.parquet`
- `green_tripdata_2026-05.parquet`

Source grain:

One row represents one NYC Green Taxi trip.

### Open-Meteo

Source format: REST API / JSON

Weather variables:

- hourly timestamp
- temperature
- precipitation
- weather code

Source grain:

One row represents one hourly weather observation for New York City.

### NYC Taxi Zones

Source format: CSV

Fields include:

- LocationID
- Borough
- Zone
- service_zone

Source grain:

One row represents one NYC taxi zone.

## Bronze Layer

Bronze preserves source data with minimal transformation.

Tables:

- `nyc_mobility.bronze.green_taxi`
- `nyc_mobility.bronze.weather`
- `nyc_mobility.bronze.taxi_zones`

Bronze also stores ingestion metadata such as:

- `source_system`
- `source_file` or `source_url`
- `source_month`
- `batch_id`
- `ingested_at`

The Bronze ingestion processes are idempotent. Previously processed files or batches are detected before writing additional records.

## Silver Layer

Silver contains cleaned and standardized records.

Tables:

- `nyc_mobility.silver.taxi_trips`
- `nyc_mobility.silver.weather`
- `nyc_mobility.silver.taxi_zones`

Main transformations include:

- standardized column names
- timestamp conversion
- location ID normalization
- trip-duration calculation
- deterministic trip hashing
- duplicate removal
- invalid timestamp filtering
- source-month validation
- standardized weather measurements

## Data Quality

Silver data quality checks validate:

- required fields are not null
- trip hashes are unique
- trip durations are non-negative
- trip pickup dates match the expected source month
- weather timestamps are unique
- weather records are within the expected date range
- taxi zone location IDs are unique

For March 2026:

- Bronze taxi rows: 44,208
- Silver taxi rows: 44,198
- Weather rows: 744
- Taxi zone rows: 265

Ten taxi records were excluded during Silver processing:

- 1 trip had a dropoff timestamp earlier than its pickup timestamp
- 9 trips had pickup dates outside the source month

## Gold Layer

Gold implements a dimensional model for mobility analytics.

Tables:

- `nyc_mobility.gold.fact_trip`
- `nyc_mobility.gold.dim_date`
- `nyc_mobility.gold.dim_hour`
- `nyc_mobility.gold.dim_zone`
- `nyc_mobility.gold.dim_weather`

The grain of `fact_trip` is one valid NYC Green Taxi trip.

Weather is joined to each trip using the pickup hour.

Pickup and dropoff dates, hours, and taxi zones use role-playing dimensions.

## Pipeline Flow

```text
NYC Green Taxi Parquet ──────┐
                             │
Open-Meteo REST API ─────────┼──> Bronze
                             │       │
NYC Taxi Zones CSV ──────────┘       ▼
                                   Silver
                                     │
                                     ▼
                              Data Quality Checks
                                     │
                                     ▼
                                    Gold
                                     │
                                     ▼
                             Analytics / BI
