# Data Dictionary

This document describes the main tables and fields used in the NYC Mobility data pipeline.

The project follows a Medallion architecture:

Bronze → Silver → Gold

Bronze preserves source data and ingestion metadata, Silver contains cleaned and standardized records, and Gold contains analytics-ready dimensional tables.

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
| `source_system` | Source system identifier, currently `nyc_tlc_green_taxi` |
| `source_file` | Name of the Parquet file from which the record was ingested |
| `source_month` | Expected source month in `YYYY-MM` format |
| `batch_id` | Identifier for the ingestion batch |
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
| `source_system` | Source system identifier, currently `open_meteo` |
| `source_url` | API URL used to retrieve the observation batch |
| `batch_id` | Identifier for the monthly weather ingestion batch |
| `ingested_at` | Timestamp when the record was ingested into Bronze |

---

## `nyc_mobility.bronze.taxi_zones`

### Grain

One row represents one NYC TLC Taxi Zone.

| Column | Description |
|---|---|
| `LocationID` | TLC Taxi Zone identifier |
| `Borough` | NYC borough associated with the taxi zone |
| `Zone` | Taxi zone name |
| `service_zone` | TLC service-zone classification |
| `source_system` | Source system identifier, currently `nyc_tlc_taxi_zones` |
| `source_url` | URL of the Taxi Zone lookup CSV |
| `batch_id` | Identifier for the taxi-zone ingestion batch |
| `ingested_at` | Timestamp when the lookup data was ingested |

---

# Silver Layer

## `nyc_mobility.silver.taxi_trips`

### Grain

One row represents one cleaned and valid NYC Green Taxi trip.

The Silver transformation removes invalid records and duplicate trips.

### Standardized Fields

| Column | Description |
|---|---|
| `pickup_location_id` | Standardized pickup Taxi Zone identifier derived from `PULocationID` |
| `dropoff_location_id` | Standardized dropoff Taxi Zone identifier derived from `DOLocationID` |
| `pickup_datetime` | Pickup timestamp standardized to Spark timestamp type |
| `dropoff_datetime` | Dropoff timestamp standardized to Spark timestamp type |
| `trip_duration_minutes` | Trip duration calculated as dropoff time minus pickup time, in minutes |
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

The hash is used as a deterministic technical trip identifier because the source does not provide a unique trip ID suitable for the project.

### Record Validation Rules

A taxi record must satisfy the following conditions to remain in Silver:

- pickup timestamp is not null
- dropoff timestamp is not null
- dropoff timestamp is greater than or equal to pickup timestamp
- pickup location ID is not null
- dropoff location ID is not null
- pickup month matches `source_month`
- `trip_hash` is unique after deduplication

---

## `nyc_mobility.silver.weather`

### Grain

One row represents one standardized hourly weather observation.

| Column | Description |
|---|---|
| `weather_datetime` | Weather observation timestamp converted to Spark timestamp type |
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

`location_id` is unique in the Silver taxi-zone table.

---

# Gold Layer

The Gold layer uses a dimensional model centered on taxi trips.

The grain of `fact_trip` is:

**one row per valid NYC Green Taxi trip**

---

## `nyc_mobility.gold.fact_trip`

### Purpose

Stores trip-level facts and foreign keys used to analyze mobility, location, time, and weather.

| Column | Type | Key | Description |
|---|---|---|---|
| `trip_key` | BIGINT | PK | Deterministic surrogate key generated from `trip_hash` |
| `trip_hash` | STRING | Technical Key | Deterministic SHA-256 trip identifier inherited from Silver |
| `pickup_date_key` | INT | FK | References `dim_date.date_key` for the pickup date |
| `dropoff_date_key` | INT | FK | References `dim_date.date_key` for the dropoff date |
| `pickup_hour_key` | INT | FK | References `dim_hour.hour_key` for the pickup hour |
| `dropoff_hour_key` | INT | FK | References `dim_hour.hour_key` for the dropoff hour |
| `pickup_zone_key` | BIGINT | FK | References `dim_zone.zone_key` for the pickup zone |
| `dropoff_zone_key` | BIGINT | FK | References `dim_zone.zone_key` for the dropoff zone |
| `weather_key` | BIGINT | FK | References `dim_weather.weather_key` based on weather at the pickup hour |
| `passenger_count` | BIGINT | Measure | Number of passengers |
| `trip_distance` | DOUBLE | Measure | Trip distance |
| `trip_duration_minutes` | DOUBLE | Measure | Duration of the trip in minutes |
| `fare_amount` | DOUBLE | Measure | Base fare charged |
| `total_amount` | DOUBLE | Measure | Total amount charged to the passenger |
| `temperature_c` | DOUBLE | Measure | Exact temperature at the pickup hour |
| `precipitation_mm` | DOUBLE | Measure | Exact precipitation amount at the pickup hour |
| `trip_count` | INT | Measure | Constant value of `1`, used for aggregation |

### Fact Relationships

`fact_trip` relates to:

- `dim_date` twice, for pickup and dropoff dates
- `dim_hour` twice, for pickup and dropoff hours
- `dim_zone` twice, for pickup and dropoff zones
- `dim_weather` once, using weather at the pickup hour

---

## `nyc_mobility.gold.dim_date`

### Grain

One row represents one calendar date appearing in valid taxi pickup or dropoff data.

| Column | Type | Key | Description |
|---|---|---|---|
| `date_key` | INT | PK | Date represented as `YYYYMMDD` |
| `full_date` | DATE |  | Calendar date |
| `year` | INT |  | Calendar year |
| `quarter` | INT |  | Calendar quarter |
| `month` | INT |  | Month number from 1 to 12 |
| `month_name` | STRING |  | Full month name |
| `day_of_month` | INT |  | Day number within the month |
| `day_of_week` | INT |  | Spark day-of-week value |
| `day_name` | STRING |  | Full weekday name |
| `week_of_year` | INT |  | Week number within the year |
| `is_weekend` | BOOLEAN |  | Indicates whether the date is Saturday or Sunday |

`dim_date` is a role-playing dimension used by both pickup and dropoff date foreign keys.

---

## `nyc_mobility.gold.dim_hour`

### Grain

One row represents one hour of the day.

The dimension contains exactly 24 rows.

| Column | Type | Key | Description |
|---|---|---|---|
| `hour_key` | INT | PK | Hour value from 0 to 23 |
| `hour_of_day` | BIGINT |  | Hour number from 0 to 23 |
| `hour_label` | STRING |  | Readable hourly label such as `08:00` |
| `day_period` | STRING |  | Broad time-of-day category |

### Day Period Rules

| Hours | Day Period |
|---|---|
| 00:00–05:59 | Late Night |
| 06:00–11:59 | Morning |
| 12:00–17:59 | Afternoon |
| 18:00–23:59 | Evening |

`dim_hour` is a role-playing dimension used by both pickup and dropoff hour foreign keys.

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

`dim_zone` is a role-playing dimension used for both pickup and dropoff locations.

---

## `nyc_mobility.gold.dim_weather`

### Grain

One row represents one distinct combination of descriptive weather attributes.

| Column | Type | Key | Description |
|---|---|---|---|
| `weather_key` | BIGINT | PK | Deterministic surrogate key generated from weather categories |
| `weather_condition` | STRING |  | High-level weather condition category |
| `temperature_band` | STRING |  | Temperature range category |
| `precipitation_band` | STRING |  | Precipitation intensity category |
| `is_raining` | BOOLEAN |  | Indicates whether precipitation is greater than zero |

### Weather Condition Categories

Open-Meteo `weather_code` values are grouped into:

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

Exact temperature and precipitation values are retained in `fact_trip`. The dimension contains categorical versions intended for grouping and analytical slicing.

---

# Key Conventions

## Primary Keys

Gold dimensional tables use deterministic keys:

- `date_key`
- `hour_key`
- `zone_key`
- `weather_key`
- `trip_key`

## Business Keys

Source-system identifiers are retained where appropriate.

For example:

- `dim_zone.location_id`

is the original TLC Taxi Zone identifier, while:

- `dim_zone.zone_key`

is the warehouse surrogate key.

## Foreign Keys

Foreign keys in `fact_trip` reference Gold dimensions:

- `pickup_date_key`
- `dropoff_date_key`
- `pickup_hour_key`
- `dropoff_hour_key`
- `pickup_zone_key`
- `dropoff_zone_key`
- `weather_key`

The Gold validation process verifies that these foreign keys are populated.

## Metadata

Ingestion metadata is retained through Bronze and Silver to support lineage and troubleshooting.

Gold focuses on analytical attributes, measures, and dimensional relationships.
