# NYC Mobility Data Engineering Pipeline

An end-to-end data engineering project for analyzing NYC Green Taxi mobility patterns using Databricks, Delta Lake, PySpark, SQL, pytest, and GitHub Actions.

The project integrates Taxi trip data, historical weather data, and NYC Taxi Zone reference data into a validated Medallion architecture and analytics-ready Gold model.

---

## Project Objectives

The pipeline is designed to answer three business questions:

1. When and where is Taxi demand highest?
2. How does weather affect Taxi demand and trip behavior?
3. Which areas show the strongest mobility patterns and opportunities?

The project also demonstrates core data engineering practices including:

- incremental ingestion
- idempotent processing
- Bronze, Silver, and Gold data layers
- data-quality validation gates
- Taxi record quarantine
- dimensional modeling
- workflow orchestration
- safe reruns and recovery
- automated testing
- CI with GitHub Actions
- operational monitoring
- business analytics

---

## Data Sources

### NYC Green Taxi

Monthly Green Taxi trip data in Parquet format.

Current project coverage:

- March 2026
- April 2026
- May 2026

Source grain:

> One row represents one NYC Green Taxi trip.

### Open-Meteo

Historical hourly weather data retrieved through the Open-Meteo REST API.

Fields used include:

- timestamp
- temperature
- precipitation
- weather code

Source grain:

> One row represents one hourly weather observation for the representative New York City location.

### NYC Taxi Zones

NYC TLC Taxi Zone reference data in CSV format.

Main source fields include:

- `LocationID`
- `Borough`
- `Zone`
- `service_zone`

Source grain:

> One row represents one NYC TLC Taxi Zone.

---

## Architecture

The project follows a Medallion architecture:

```text
INGEST
  ↓
BRONZE
  ↓
BRONZE VALIDATION
  ↓
SILVER
  ↓
SILVER VALIDATION
  ↓
GOLD
  ↓
GOLD VALIDATION
  ↓
ANALYTICS
```

The Databricks workflow is orchestrated as:

```text
ingest_taxi_into_bronze ───────┐
ingest_weather_into_bronze ────┼──> bronze_validation
ingest_zones_into_bronze ──────┘
                                      ↓
                         ┌────────────┼────────────┐
                         ↓            ↓            ↓
                    silver_taxi  silver_weather  silver_zones
                         └────────────┼────────────┘
                                      ↓
                               silver_validation
                                      ↓
                                     gold
                                      ↓
                               gold_validation
                                      ↓
                                  analytics
```

The three ingestion tasks can run in parallel.

---

## Medallion Layers

### Bronze

Bronze preserves source data with minimal transformation and adds ingestion metadata.

Tables:

```text
nyc_mobility.bronze.green_taxi
nyc_mobility.bronze.weather
nyc_mobility.bronze.taxi_zones
```

Common metadata includes:

- `source_system`
- `source_file` or `source_url`
- `source_month`
- `batch_id`
- `ingested_at`

Bronze ingestion is designed to be incremental and idempotent.

Taxi files are skipped when the same source file has already been processed.

Weather batches are skipped when the corresponding monthly batch already exists.

Taxi Zones use a fixed batch identifier and are not reloaded unnecessarily.

### Silver

Silver contains cleaned, standardized, validated, and deduplicated records.

Tables:

```text
nyc_mobility.silver.taxi_trips
nyc_mobility.silver.taxi_trips_quarantine
nyc_mobility.silver.weather
nyc_mobility.silver.taxi_zones
```

Taxi transformations include:

- timestamp standardization
- location ID normalization
- trip-duration calculation
- deterministic `trip_hash` generation
- row-level data-quality classification
- valid-record routing
- invalid-record quarantine
- source-month validation
- duplicate removal

Weather transformations include:

- timestamp standardization
- numeric casting
- hourly deduplication
- invalid timestamp handling

Taxi Zone transformations include:

- standardized field names
- integer location IDs
- null filtering
- location ID deduplication

---

## Taxi Quarantine

Invalid Taxi records are preserved instead of silently discarded.

The Silver transformation separates Taxi rows into:

```text
Bronze Taxi
    ↓
Row-level quality checks
    ├── valid   → silver.taxi_trips
    └── invalid → silver.taxi_trips_quarantine
```

Supported quarantine reasons include:

- `NULL_OR_INVALID_PICKUP_TIMESTAMP`
- `NULL_OR_INVALID_DROPOFF_TIMESTAMP`
- `DROPOFF_BEFORE_PICKUP`
- `NULL_PICKUP_LOCATION`
- `NULL_DROPOFF_LOCATION`
- `NULL_SOURCE_MONTH`
- `PICKUP_MONTH_MISMATCH`

Each quarantined row also records a `quarantined_at` timestamp.

The project applies the following data-quality policy:

| Issue | Action |
|---|---|
| Taxi row-level defect | Quarantine |
| Taxi duplicate | Deduplicate |
| Weather completeness or integrity failure | Fail validation |
| Taxi Zone completeness or key-integrity failure | Fail validation |
| Gold referential-integrity failure | Fail validation |

Quarantined Taxi records do not flow into the Gold analytical model.

---

## Gold Dimensional Model

The Gold layer provides an analytics-ready star schema.

Tables:

```text
nyc_mobility.gold.fact_trip
nyc_mobility.gold.dim_date
nyc_mobility.gold.dim_hour
nyc_mobility.gold.dim_zone
nyc_mobility.gold.dim_weather
```

![NYC Mobility Gold Star Schema](docs/img/nyc_data_model.jpeg)

### Fact Table

`fact_trip`

Grain:

> One row represents one valid NYC Green Taxi trip.

Main measures include:

- `passenger_count`
- `trip_distance`
- `trip_duration_minutes`
- `fare_amount`
- `total_amount`
- `temperature_c`
- `precipitation_mm`
- `trip_count`

Foreign keys include:

- `pickup_date_key`
- `dropoff_date_key`
- `pickup_hour_key`
- `dropoff_hour_key`
- `pickup_zone_key`
- `dropoff_zone_key`
- `weather_key`

### Dimensions

- `dim_date` provides calendar attributes
- `dim_hour` contains the 24 hours of the day
- `dim_zone` contains NYC TLC Taxi Zones
- `dim_weather` contains categorical weather attributes

Date, hour, and zone dimensions are role-playing dimensions used for both pickup and dropoff relationships.

Weather is associated with each trip using the pickup hour.

---

## Data Quality Gates

Validation is performed after each major data layer.

### Bronze Validation

Checks include:

- expected Taxi months are present
- source metadata is populated
- Taxi files are mapped to valid batches
- Weather coverage matches Taxi source months
- expected Weather hourly counts are present
- Taxi Zone count equals 265
- Taxi Zone IDs are unique and non-null

A Bronze validation failure prevents Silver tasks from running.

### Silver Validation

Checks include:

- required Taxi fields are populated
- `trip_hash` is unique
- trip durations are valid
- Taxi pickup month matches `source_month`
- Weather timestamps are unique
- required Weather hourly coverage is preserved
- Taxi Zone IDs remain unique and non-null
- quarantine metadata is populated
- quarantine reasons are supported
- no trip exists in both valid Silver and quarantine

A Silver validation failure prevents Gold from running.

### Gold Validation

Checks include:

- Gold fact count matches valid Silver Taxi count
- `trip_key` and `trip_hash` are unique
- foreign keys are non-null
- dimension keys are unique
- fact-to-dimension referential integrity is valid
- trip duration is non-negative
- `trip_count = 1`
- `dim_hour` contains 24 rows
- `dim_zone` contains 265 rows

A Gold validation failure prevents downstream Analytics from running.

---

## Incremental Loading and Idempotency

Bronze ingestion is incremental.

Taxi ingestion checks whether each source file has already been processed.

Weather ingestion checks whether each monthly Weather batch already exists.

Taxi Zone ingestion checks whether the reference batch has already been loaded.

Silver and Gold are deterministic rebuilds from validated upstream data.

This allows failed workflows to be rerun safely without repeatedly appending the same source batches.

---

## Recovery Strategy

Pipeline validation tasks act as quality gates.

```text
bronze_validation fails
→ Silver does not run

silver_validation fails
→ Gold does not run

gold_validation fails
→ Analytics does not run
```

After the underlying issue is corrected, the workflow can be rerun safely because ingestion is idempotent and downstream tables are deterministically rebuilt.

---

## Business Analytics

Business analytics queries are stored under:

```text
analytics/
```

The three dashboard datasets correspond to the project business questions:

```text
business_analytics_question_1.dbquery.ipynb
business_analytics_question_2.dbquery.ipynb
business_analytics_question_3.dbquery.ipynb
```

### Question 1: Taxi Demand

Analyzes Taxi demand across:

- date
- day of week
- hour
- day period
- pickup borough
- pickup zone

Metrics include:

- trip count
- total fare
- total distance
- average trip distance

### Question 2: Weather Impact

Analyzes Taxi activity using:

- temperature band
- weather condition
- Rain vs No Rain
- hour of day

Weather exposure is calculated from actual hourly Silver Weather observations so that trip volume can be normalized as trips per weather hour.

### Question 3: Mobility Patterns

Analyzes:

- pickups
- dropoffs
- net flow
- absolute flow imbalance
- zone activity
- average trip distance
- average fare
- average trip duration

Pickup activity uses actual pickup date and hour.

Dropoff activity uses actual dropoff date and hour.

Weather associated with dropoff activity still represents pickup weather because `fact_trip` stores one weather relationship based on pickup time.

---

## Monitoring

The project includes a Databricks monitoring dashboard:

```text
dashboard/
└── NYC Mobility — Production Monitoring.lvdash.json
```

Monitoring focuses on:

- job execution
- task execution
- failures
- data freshness
- data quality

The monitoring design supports visibility into:

- pipeline status
- failed tasks
- task duration
- recent executions
- validation results
- data freshness
- quality-check outcomes

---

## Testing

The project uses `pytest` for code-level tests.

The current test suite contains 23 tests covering:

- data-quality helper functions
- ingestion metadata
- idempotency behavior
- Taxi ingestion
- Taxi transformations
- Taxi quarantine behavior
- Weather ingestion
- Weather transformations
- Taxi Zone ingestion
- Taxi Zone transformations

Run tests locally with:

```bash
python -m pytest
```

---

## Continuous Integration

GitHub Actions runs the test suite for pushes and pull requests targeting `main`.

Workflow:

```text
Change
  ↓
Test
  ↓
Commit
  ↓
Push
  ↓
GitHub Actions CI
  ↓
Deploy / Run in Databricks
  ↓
Verify
```

CI configuration:

```text
.github/workflows/ci.yml
```

---

## Repository Structure

```text
nyc-mobility/
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── analytics/
│   ├── business_analytics_question_1.dbquery.ipynb
│   ├── business_analytics_question_2.dbquery.ipynb
│   └── business_analytics_question_3.dbquery.ipynb
│
├── dashboard/
│   └── NYC Mobility — Production Monitoring.lvdash.json
│
├── docs/
│   ├── architecture.md
│   ├── data_dictionary.md
│   ├── data_model.md
│   ├── engineering_decisions.md
│   └── img/
│       └── nyc_data_model.jpeg
│
├── notebooks/
│   ├── 00_setup.sql
│   │
│   ├── ingestion/
│   │   ├── 01_ingest_taxi.py
│   │   ├── 02_ingest_weather.py
│   │   └── 03_ingest_zones.py
│   │
│   └── transformation/
│       ├── 04_silver_taxi.py
│       ├── 05_silver_weather.py
│       ├── 06_silver_zones.py
│       ├── 07_build_gold.py
│       └── 08_analytics.py
│
├── src/
│   ├── ingestion/
│   ├── quality/
│   ├── transformations/
│   └── utils/
│
├── tests/
│   ├── conftest.py
│   ├── test_data_quality.py
│   ├── test_idempotency.py
│   ├── test_taxi_ingestion.py
│   ├── test_taxi_transformations.py
│   ├── test_weather_ingestion.py
│   ├── test_weather_transformations.py
│   ├── test_zone_ingestion.py
│   └── test_zone_transformations.py
│
├── validation/
│   ├── validate_bronze.py
│   ├── validate_silver.py
│   └── validate_gold.py
│
├── pytest.ini
├── requirements.txt
└── README.md
```

---

## Documentation

Additional project documentation is available in:

- [`docs/architecture.md`](docs/architecture.md) – pipeline architecture, orchestration, monitoring, and governance
- [`docs/data_dictionary.md`](docs/data_dictionary.md) – table and field definitions
- [`docs/data_model.md`](docs/data_model.md) – Gold dimensional model
- [`docs/engineering_decisions.md`](docs/engineering_decisions.md) – major engineering design decisions

---

## Technology Stack

- Databricks
- Apache Spark / PySpark
- Spark SQL
- Delta Lake
- Python
- REST APIs
- pytest
- Git
- GitHub
- GitHub Actions

---

## Key Engineering Decisions

The project intentionally uses:

- Bronze as the persistent incremental ingestion layer
- deterministic Silver and Gold rebuilds
- deterministic `trip_hash` values for Taxi trip identity
- quarantine for isolated Taxi row-level defects
- validation failure for incomplete Weather and Taxi Zone datasets
- role-playing dimensions for date, hour, and zone
- pickup-hour Weather enrichment
- pytest for unit-level behavior
- runtime validation notebooks for actual Databricks tables
- GitHub Actions for continuous integration

See [`docs/engineering_decisions.md`](docs/engineering_decisions.md) for more detail.

---

## Current Dataset Scope

The current analytical dataset covers:

```text
March 2026
April 2026
May 2026
```

The implementation is designed so that additional monthly Taxi and Weather batches can be discovered and ingested incrementally.
