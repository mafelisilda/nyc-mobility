# Data Dictionary

This document describes the primary data tables and fields used in the NYC Mobility pipeline.

The project follows a Medallion architecture:

```text
Bronze → Silver → Gold
```

Bronze preserves source data and ingestion metadata. Silver contains standardized and validated records. Gold contains analytics-ready dimensional tables.

---

# Bronze Layer

## `nyc_mobility.bronze.green_taxi`

### Grain

One row represents one NYC Green Taxi trip as received from the monthly source Parquet file.

### Main Source Fields

| Column | Description |
|---|---|
| `VendorID` | Identifier for the taxi technology provider that submitted the trip record |
| `lpep_pickup_datetime` | Original taxi pickup timestamp |
| `lpep_dropoff_datetime` | Original taxi dropoff timestamp |
| `RatecodeID` | Final rate code applied to the trip |
| `PULocationID` | TLC Taxi Zone identifier for the pickup location |
| `DOLocationID` | TLC Taxi Zone identifier for the dropoff location |
| `passenger_count` | Number of passengers reported for the trip |
| `trip_distance` | Trip distance reported by the taxi meter |
| `fare_amount` | Base fare amount |
| `extra` | Additional miscellaneous fare charges |
| `mta_tax` | MTA tax charged on the trip |
| `tip_amount` | Tip amount |
| `tolls_amount` | Toll charges |
| `ehail_fee` | Electronic hail fee when applicable |
| `improvement_surcharge` | Improvement surcharge |
| `total_amount` | Total amount charged to the passenger |
| `payment_type` | Payment method code |
| `trip_type` | Street-hail or dispatch trip classification |
| `congestion_surcharge` | Congestion surcharge |
| `cbd_congestion_fee` | Central Business District congestion fee when applicable |

### Ingestion Metadata

| Column | Description |
|---|---|
| `source_system` | Source system identifier |
| `source_file` | Source Parquet filename |
| `source_month` | Expected source month in `YYYY-MM` format |
| `batch_id` | Ingestion batch identifier |
| `ingested_at` | Timestamp when the record was ingested into Bronze |

---

## `nyc_mobility.bronze.weather`

### Grain

One row represents one hourly Open-Meteo weather observation for the representative New York City location.

| Column | Description |
|---|---|
| `time` | Hourly observation timestamp returned by Open-Meteo |
| `temperature_2m` | Air temperature at 2 meters above ground level in degrees Celsius |
| `precipitation` | Hourly precipitation amount in millimeters |
| `weather_code` | Open-Meteo weather condition code |
| `source_system` | Source system identifier |
| `source_url` | API URL used to retrieve the observation batch |
| `batch_id` | Monthly weather ingestion batch identifier |
| `ingested_at` | Ingestion timestamp |

---

## `nyc_mobility.bronze.taxi_zones`

### Grain

One row represents one NYC TLC Taxi Zone.

| Column | Description |
|---|---|
| `LocationID` | TLC Taxi Zone identifier |
| `Borough` | NYC borough associated with the zone |
| `Zone` | Taxi zone name |
| `service_zone` | TLC service-zone classification |
| `source_system` | Source system identifier |
| `source_url` | Source CSV URL |
| `batch_id` | Ingestion batch identifier |
| `ingested_at` | Ingestion timestamp |

---

# Silver Layer

## `nyc_mobility.silver.taxi_trips`

### Grain

One row represents one cleaned and valid NYC Green Taxi trip.

The Silver Taxi transformation standardizes fields, calculates trip duration, generates a deterministic `trip_hash`, routes invalid row-level records to quarantine, and deduplicates valid trips.

### Standardized Fields

| Column | Description |
|---|---|
| `pickup_location_id` | Standardized pickup Taxi Zone identifier derived from `PULocationID` |
| `dropoff_location_id` | Standardized dropoff Taxi Zone identifier derived from `DOLocationID` |
| `pickup_datetime` | Pickup timestamp standardized to Spark timestamp type |
| `dropoff_datetime` | Dropoff timestamp standardized to Spark timestamp type |
| `trip_duration_minutes` | Trip duration in minutes |
| `trip_hash` | Deterministic SHA-256 technical identifier used for deduplication |
| `passenger_count` | Number of passengers |
| `trip_distance` | Reported trip distance |
| `fare_amount` | Base fare amount |
| `total_amount` | Total passenger charge |
| `source_system` | Original Bronze source-system metadata |
| `source_file` | Original source filename |
| `source_month` | Expected source month |
| `batch_id` | Original ingestion batch identifier |
| `ingested_at` | Original ingestion timestamp |

### Trip Hash Inputs

`trip_hash` is generated from:

- pickup timestamp
- dropoff timestamp
- pickup location ID
- dropoff location ID
- trip distance
- total amount

### Valid Record Rules

A Taxi record remains in the valid Silver table when:

- pickup timestamp is valid and non-null
- dropoff timestamp is valid and non-null
- dropoff timestamp is greater than or equal to pickup timestamp
- pickup location ID is not null
- dropoff location ID is not null
- `source_month` is not null
- pickup month matches `source_month`

Valid rows are deduplicated using `trip_hash`.

---

## `nyc_mobility.silver.taxi_trips_quarantine`

### Purpose

Stores Taxi records that fail row-level data-quality rules during the Bronze-to-Silver transformation.

Quarantined records are retained for auditing, troubleshooting, and monitoring. They do not continue into the Gold analytical model.

### Grain

One row represents one rejected NYC Green Taxi record.

### Main Fields

| Column | Description |
|---|---|
| `lpep_pickup_datetime` | Original pickup timestamp from Bronze |
| `lpep_dropoff_datetime` | Original dropoff timestamp from Bronze |
| `pickup_location_id` | Standardized pickup Taxi Zone identifier |
| `dropoff_location_id` | Standardized dropoff Taxi Zone identifier |
| `pickup_datetime` | Standardized pickup timestamp |
| `dropoff_datetime` | Standardized dropoff timestamp |
| `trip_duration_minutes` | Calculated trip duration in minutes |
| `trip_hash` | Deterministic technical trip identifier |
| `passenger_count` | Number of passengers when available |
| `trip_distance` | Reported trip distance |
| `fare_amount` | Base fare amount |
| `total_amount` | Total passenger charge |
| `source_system` | Original Bronze source system |
| `source_file` | Original source filename |
| `source_month` | Expected source month |
| `batch_id` | Original ingestion batch identifier |
| `ingested_at` | Original Bronze ingestion timestamp |
| `quarantine_reason` | Primary row-level quality rule that caused the record to be quarantined |
| `quarantined_at` | Timestamp when the record was written to quarantine |

### Supported Quarantine Reasons

| Quarantine Reason | Meaning |
|---|---|
| `NULL_OR_INVALID_PICKUP_TIMESTAMP` | Pickup timestamp is missing or invalid |
| `NULL_OR_INVALID_DROPOFF_TIMESTAMP` | Dropoff timestamp is missing or invalid |
| `DROPOFF_BEFORE_PICKUP` | Dropoff occurs before pickup |
| `NULL_PICKUP_LOCATION` | Pickup Taxi Zone identifier is missing |
| `NULL_DROPOFF_LOCATION` | Dropoff Taxi Zone identifier is missing |
| `NULL_SOURCE_MONTH` | `source_month` is missing |
| `PICKUP_MONTH_MISMATCH` | Pickup timestamp month does not match `source_month` |

Each quarantined record is assigned one primary quarantine reason.

---

## `nyc_mobility.silver.weather`

### Grain

One row represents one standardized hourly weather observation.

| Column | Description |
|---|---|
| `weather_datetime` | Weather observation timestamp converted to Spark timestamp |
| `temperature_c` | Temperature in degrees Celsius |
| `precipitation_mm` | Precipitation amount in millimeters |
| `weather_code` | Integer Open-Meteo weather condition code |
| `source_system` | Original Bronze source system |
| `source_url` | Original Open-Meteo request URL |
| `batch_id` | Weather ingestion batch identifier |
| `ingested_at` | Original ingestion timestamp |

`weather_datetime` is unique in the Silver weather table.

---

## `nyc_mobility.silver.taxi_zones`

### Grain

One row represents one standardized NYC Taxi Zone.

| Column | Description |
|---|---|
| `location_id` | Integer TLC Taxi Zone business key |
| `borough` | NYC borough |
| `zone` | Taxi zone name |
| `service_zone` | TLC service-zone classification |
| `source_system` | Original Bronze source system |
| `source_url` | Source CSV URL |
| `batch_id` | Taxi-zone ingestion batch identifier |
| `ingested_at` | Original ingestion timestamp |

`location_id` is unique in the Silver Taxi Zone table.

---

# Gold Layer

The Gold layer uses a dimensional model centered on Taxi trips.

Only valid records from `nyc_mobility.silver.taxi_trips` flow into Gold. Quarantined Taxi records do not enter the analytical model.

---

## `nyc_mobility.gold.fact_trip`

### Grain

One row represents one valid NYC Green Taxi trip.

| Column | Type | Key | Description |
|---|---|---|---|
| `trip_key` | BIGINT | PK | Deterministic surrogate key generated from `trip_hash` |
| `trip_hash` | STRING | Technical Key | Deterministic SHA-256 trip identifier inherited from Silver |
| `pickup_date_key` | INT | FK | References `dim_date.date_key` for pickup date |
| `dropoff_date_key` | INT | FK | References `dim_date.date_key` for dropoff date |
| `pickup_hour_key` | INT | FK | References `dim_hour.hour_key` for pickup hour |
| `dropoff_hour_key` | INT | FK | References `dim_hour.hour_key` for dropoff hour |
| `pickup_zone_key` | BIGINT | FK | References `dim_zone.zone_key` for pickup zone |
| `dropoff_zone_key` | BIGINT | FK | References `dim_zone.zone_key` for dropoff zone |
| `weather_key` | BIGINT | FK | References `dim_weather.weather_key` using weather at pickup hour |
| `passenger_count` | BIGINT | Measure | Number of passengers |
| `trip_distance` | DOUBLE | Measure | Trip distance |
| `trip_duration_minutes` | DOUBLE | Measure | Trip duration in minutes |
| `fare_amount` | DOUBLE | Measure | Base fare amount |
| `total_amount` | DOUBLE | Measure | Total passenger charge |
| `temperature_c` | DOUBLE | Measure | Exact temperature at pickup hour |
| `precipitation_mm` | DOUBLE | Measure | Exact precipitation at pickup hour |
| `trip_count` | INT | Measure | Constant value `1` used for aggregation |

---

## `nyc_mobility.gold.dim_date`

### Grain

One row represents one calendar date appearing in valid Taxi pickup or dropoff data.

| Column | Type | Key | Description |
|---|---|---|---|
| `date_key` | INT | PK | Date represented as `YYYYMMDD` |
| `full_date` | DATE |  | Calendar date |
| `year` | INT |  | Calendar year |
| `quarter` | INT |  | Calendar quarter |
| `month` | INT |  | Month number |
| `month_name` | STRING |  | Full month name |
| `day_of_month` | INT |  | Day number within the month |
| `day_of_week` | INT |  | Spark day-of-week value |
| `day_name` | STRING |  | Full weekday name |
| `week_of_year` | INT |  | Week number within the year |
| `is_weekend` | BOOLEAN |  | Indicates whether the date is Saturday or Sunday |

`dim_date` is reused for pickup and dropoff dates.

---

## `nyc_mobility.gold.dim_hour`

### Grain

One row represents one hour of the day. The table contains exactly 24 rows.

| Column | Type | Key | Description |
|---|---|---|---|
| `hour_key` | INT | PK | Hour value from 0 to 23 |
| `hour_of_day` | BIGINT |  | Hour number from 0 to 23 |
| `hour_label` | STRING |  | Readable label such as `08:00` |
| `day_period` | STRING |  | Broad time-of-day category |

### Day Period Rules

| Hours | Day Period |
|---|---|
| 00:00-05:59 | Late Night |
| 06:00-11:59 | Morning |
| 12:00-17:59 | Afternoon |
| 18:00-23:59 | Evening |

`dim_hour` is reused for pickup and dropoff hours.

---

## `nyc_mobility.gold.dim_zone`

### Grain

One row represents one NYC Taxi Zone.

| Column | Type | Key | Description |
|---|---|---|---|
| `zone_key` | BIGINT | PK | Deterministic surrogate key generated from `location_id` |
| `location_id` | INT | Business Key | Original TLC Taxi Zone identifier |
| `borough` | STRING |  | NYC borough |
| `zone` | STRING |  | Taxi zone name |
| `service_zone` | STRING |  | TLC service-zone classification |

`dim_zone` is reused for pickup and dropoff locations.

---

## `nyc_mobility.gold.dim_weather`

### Grain

One row represents one distinct combination of descriptive weather attributes.

| Column | Type | Key | Description |
|---|---|---|---|
| `weather_key` | BIGINT | PK | Deterministic surrogate key generated from weather categories |
| `weather_condition` | STRING |  | High-level weather condition |
| `temperature_band` | STRING |  | Temperature range category |
| `precipitation_band` | STRING |  | Precipitation intensity category |
| `is_raining` | BOOLEAN |  | Indicates whether precipitation is greater than zero |

### Weather Condition Categories

- Clear
- Cloudy
- Fog
- Rain
- Snow
- Thunderstorm
- Other

### Temperature Bands

| Rule | Category |
|---|---|
| temperature < 0°C | `Below 0 C` |
| 0°C ≤ temperature < 10°C | `0-10 C` |
| 10°C ≤ temperature < 20°C | `10-20 C` |
| 20°C ≤ temperature < 30°C | `20-30 C` |
| temperature ≥ 30°C | `30+ C` |

### Precipitation Bands

| Rule | Category |
|---|---|
| precipitation = 0 mm | `None` |
| 0 mm < precipitation < 2.5 mm | `Light` |
| 2.5 mm ≤ precipitation < 7.5 mm | `Moderate` |
| precipitation ≥ 7.5 mm | `Heavy` |

Exact `temperature_c` and `precipitation_mm` values are retained in `fact_trip`.

---

# Data Quality Handling Summary

| Dataset / Issue | Action |
|---|---|
| Taxi row-level defect | Quarantine |
| Taxi duplicate trip | Deduplicate |
| Weather completeness or integrity failure | Fail validation |
| Taxi Zone completeness or key-integrity failure | Fail validation |
| Gold key or referential-integrity failure | Fail validation |
| All required checks satisfied | Pass |

---

# Key Conventions

## Primary Keys

Gold tables use deterministic keys:

- `trip_key`
- `date_key`
- `hour_key`
- `zone_key`
- `weather_key`

## Business Keys

Source-system identifiers are retained where appropriate.

For example:

- `dim_zone.location_id` is the original TLC Taxi Zone identifier.
- `dim_zone.zone_key` is the warehouse surrogate key.

## Foreign Keys

`fact_trip` contains:

- `pickup_date_key`
- `dropoff_date_key`
- `pickup_hour_key`
- `dropoff_hour_key`
- `pickup_zone_key`
- `dropoff_zone_key`
- `weather_key`

Gold validation verifies these relationships.

## Metadata

Ingestion metadata is retained through Bronze and Silver for lineage and troubleshooting.

Taxi quarantine additionally records:

- `quarantine_reason`
- `quarantined_at`

Gold focuses on analytical attributes, measures, and dimensional relationships.
