# Engineering Decisions

## 1. Medallion Architecture

The project uses Bronze, Silver, and Gold layers.

Bronze preserves source data and ingestion metadata.

Silver standardizes and validates records.

Gold provides analytics-ready dimensional tables.

## 2. Idempotent Ingestion

Taxi ingestion checks whether a source file has already been processed.

Weather and taxi-zone ingestion check whether a batch ID has already been processed.

Repeated execution therefore does not append the same source batch multiple times.

## 3. Provenance Metadata

Bronze records retain metadata including:

- source system
- source file or URL
- ingestion timestamp
- batch ID

Taxi data also stores `source_month`.

This supports lineage, troubleshooting, and incremental processing.

## 4. Taxi Trip Identity

The NYC Green Taxi source does not provide a unique trip identifier suitable for the project.

A deterministic `trip_hash` is therefore generated using selected trip attributes:

- pickup timestamp
- dropoff timestamp
- pickup location ID
- dropoff location ID
- trip distance
- total amount

This hash is used for deduplication and technical trip identity.

It is a deterministic engineering key rather than a source-provided business identifier.

## 5. Invalid Trip Filtering

Trips are excluded from Silver when:

- pickup timestamp is null
- dropoff timestamp is null
- dropoff occurs before pickup
- pickup location is null
- dropoff location is null
- pickup month does not match the source month

During March processing:

- 1 row was excluded because dropoff occurred before pickup
- 9 rows were excluded because pickup timestamps did not belong to March 2026

## 6. Weather Integration

Open-Meteo weather is stored at hourly grain.

Each taxi trip is associated with weather at the hour of pickup.

The pickup timestamp is truncated to the hour and joined to `weather_datetime`.

## 7. Weather Dimension

Continuous measurements such as exact temperature and precipitation remain in the fact table.

The weather dimension contains descriptive analytical categories:

- weather condition
- temperature band
- precipitation band
- raining indicator

This supports both categorical analysis and precise numeric analysis.

## 8. Surrogate Keys

Gold dimensions use deterministic surrogate keys.

`zone_key` is generated from the taxi-zone business key.

`weather_key` is generated from the combination of descriptive weather attributes.

`trip_key` is generated from `trip_hash`.

## 9. Role-Playing Dimensions

The same date, hour, and zone dimensions are reused for pickup and dropoff relationships.

This avoids unnecessary duplication of dimension tables.

## 10. Silver Rebuild Strategy

Silver tables currently use deterministic overwrite builds from Bronze.

Because Silver is derived entirely from Bronze, rebuilding ensures consistent transformation results during project development.

Bronze remains the persistent ingestion layer and retains incremental source history.
