# NYC Mobility Data Pipeline Architecture

## Overview

This project implements an end-to-end data engineering pipeline for NYC Green Taxi mobility analysis using Databricks, Delta Lake, PySpark, GitHub Actions, and pytest.

The pipeline integrates three data sources:

1. NYC Green Taxi monthly Parquet files
2. Open-Meteo historical hourly weather data
3. NYC Taxi Zone reference data

The processing architecture follows a Medallion pattern:

```text
INGEST → BRONZE → VALIDATE → SILVER → VALIDATE → GOLD → VALIDATE → ANALYTICS
```

---

## Data Sources

### NYC Green Taxi

**Source format:** Parquet

The ingestion process scans the landing location for monthly Green Taxi files matching:

```text
green_tripdata_YYYY-MM.parquet
```

The current project dataset contains:

- March 2026
- April 2026
- May 2026

**Source grain:** One row represents one NYC Green Taxi trip as provided by the source file.

### Open-Meteo

**Source format:** REST API / JSON

Hourly variables used:

- timestamp
- temperature
- precipitation
- weather code

Weather months are derived from the available Green Taxi landing files so that Taxi and Weather ingestion can run independently in parallel.

**Source grain:** One row represents one hourly weather observation for the representative New York City location.

### NYC Taxi Zones

**Source format:** CSV

Main fields:

- `LocationID`
- `Borough`
- `Zone`
- `service_zone`

**Source grain:** One row represents one NYC TLC Taxi Zone.

---

## Orchestration

The Databricks Job executes the pipeline as a dependency graph.

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

The three ingestion tasks can execute in parallel because they do not depend on one another.

---

## Bronze Layer

Bronze preserves source data with minimal transformation and adds ingestion metadata.

Tables:

- `nyc_mobility.bronze.green_taxi`
- `nyc_mobility.bronze.weather`
- `nyc_mobility.bronze.taxi_zones`

Metadata includes:

- `source_system`
- `source_file` or `source_url`
- `source_month` where applicable
- `batch_id`
- `ingested_at`

### Incremental and Idempotent Ingestion

Taxi ingestion checks whether each source file has already been processed before appending records.

Weather ingestion checks whether each monthly weather batch already exists before calling the API.

Taxi Zone ingestion uses a fixed batch identifier and skips the reference load if it has already been processed.

This makes repeated Bronze execution safe and prevents duplicate ingestion of the same source batch.

---

## Bronze Validation

Bronze validation verifies that source ingestion completed correctly before Silver transformations are allowed to run.

Checks include:

- expected Taxi source months are present
- each Taxi source file maps to one batch
- Taxi provenance metadata is populated
- Weather coverage matches the Taxi source months
- each Weather month contains the expected number of hourly observations
- Weather provenance metadata is populated
- Taxi Zone count is 265
- Taxi Zone IDs are unique and non-null
- Taxi Zone provenance metadata is populated

If Bronze validation fails, the Silver transformation tasks do not proceed.

---

## Silver Layer

Silver contains cleaned, standardized, and deduplicated records.

Tables:

- `nyc_mobility.silver.taxi_trips`
- `nyc_mobility.silver.taxi_trips_quarantine`
- `nyc_mobility.silver.weather`
- `nyc_mobility.silver.taxi_zones`

### Taxi Transformations

Taxi transformations include:

- timestamp standardization
- location ID normalization
- trip-duration calculation
- deterministic `trip_hash` generation
- row-level data-quality classification
- valid-record routing to `nyc_mobility.silver.taxi_trips`
- invalid-record routing to `nyc_mobility.silver.taxi_trips_quarantine`
- source-month validation
- duplicate removal for valid trips

### Weather Transformations

Weather transformations include:

- timestamp standardization
- numeric casting
- invalid timestamp filtering
- hourly deduplication

### Taxi Zone Transformations

Taxi Zone transformations include:

- field standardization
- integer location ID conversion
- null filtering
- location ID deduplication

Silver tables are rebuilt deterministically from Bronze using overwrite mode.

---

## Silver Validation

Silver validation verifies:

- required fields are populated
- Taxi trip hashes are unique
- trip durations are valid
- pickup timestamps match the expected source month
- Weather timestamps are unique
- expected Weather coverage is preserved
- Taxi Zone IDs remain unique and non-null

If Silver validation fails, the Gold dimensional build does not run.

---

## Gold Layer

Gold provides an analytics-ready dimensional model.

Tables:

- `nyc_mobility.gold.fact_trip`
- `nyc_mobility.gold.dim_date`
- `nyc_mobility.gold.dim_hour`
- `nyc_mobility.gold.dim_zone`
- `nyc_mobility.gold.dim_weather`

The grain of `fact_trip` is one valid NYC Green Taxi trip.

Role-playing dimensions are used for:

- pickup and dropoff date
- pickup and dropoff hour
- pickup and dropoff Taxi Zone

Weather is joined to each trip using the pickup hour.

The exact temperature and precipitation measurements are retained in `fact_trip`, while `dim_weather` contains descriptive weather categories.

---

## Gold Validation

Gold validation verifies:

- fact row count matches Silver Taxi row count
- `trip_key` and `trip_hash` are unique
- foreign keys are non-null
- dimension primary keys are unique
- fact-to-dimension relationships are valid
- trip duration is non-negative
- `trip_count = 1`
- `dim_hour` contains exactly 24 rows
- `dim_zone` contains exactly 265 rows

If Gold validation fails, the Analytics task does not proceed.

---

## Recovery and Safe Reruns

The pipeline is designed so that downstream tasks depend on successful completion of upstream validation gates.

Examples:

```text
bronze_validation failure
→ Silver tasks do not run

silver_validation failure
→ Gold does not run

gold_validation failure
→ Analytics does not run
```

Rerunning the workflow is safe because:

- Taxi Bronze ingestion skips previously processed source files
- Weather Bronze ingestion skips previously processed monthly batches
- Taxi Zone ingestion skips the already-loaded reference batch
- Silver tables are deterministically rebuilt from Bronze
- Gold tables are deterministically rebuilt from Silver

This design reduces the risk of duplicate ingestion and allows failed runs to be retried safely after the underlying issue is fixed.

---

## Testing and CI

Code-level behavior is tested using pytest.

The current unit-test suite covers:

- data-quality helper functions
- ingestion metadata
- ingestion idempotency
- Taxi Silver transformations
- Weather Silver transformations
- Taxi Zone Silver transformations

The current suite contains **23 tests**.

GitHub Actions runs:

```bash
python -m pytest
```

for each push and pull request to `main`.

The development lifecycle is:

```text
Change
→ Test
→ Commit
→ Push
→ CI
→ Deploy / Run
→ Verify
```

This separates code-level testing from runtime pipeline validation.

### Unit Testing vs Pipeline Validation

Unit tests verify whether individual functions behave correctly using small controlled inputs.

Examples include:

- duplicate removal
- null handling
- timestamp filtering
- metadata creation
- idempotency logic

Pipeline validation verifies whether the actual Bronze, Silver, and Gold tables satisfy expected data-quality rules after execution.

---

## Monitoring

The project monitors four main areas:

1. Execution
2. Failures
3. Freshness
4. Data quality

### Execution Monitoring

Execution monitoring focuses on whether the Databricks Job runs successfully and whether each task completes as expected.

Key indicators include:

- overall Job run status
- individual task status
- task start and end times
- task duration
- end-to-end pipeline completion

The main pipeline tasks include:

- ingestion tasks
- Bronze validation
- Silver transformations
- Silver validation
- Gold build
- Gold validation
- Analytics

### Failure Monitoring

Failure monitoring focuses on identifying where the pipeline failed and whether downstream processing was blocked.

Key indicators include:

- failed task name
- error message
- failed run timestamp
- skipped or blocked downstream tasks
- successful recovery after rerun

Validation tasks act as quality gates.

For example:

```text
bronze_validation fails
→ Silver does not proceed

silver_validation fails
→ Gold does not proceed

gold_validation fails
→ Analytics does not proceed
```

### Freshness Monitoring

Freshness monitoring checks whether recently expected source data has been loaded.

For Taxi data, useful indicators include:

- latest `source_month`
- latest `ingested_at`
- latest source file
- whether expected monthly source files are present

For Weather data, useful indicators include:

- latest `weather_datetime`
- latest Weather batch
- latest `ingested_at`
- expected number of hourly observations per month

For Taxi Zones, freshness is less time-sensitive because the table is reference data.

The primary checks are:

- table exists
- expected 265 zones are present

### Data Quality Monitoring

Data-quality monitoring uses the project's validation scripts and checks.

Bronze monitoring includes:

- provenance metadata completeness
- duplicate source detection
- expected source-month coverage
- expected Weather hourly counts
- Taxi Zone uniqueness
- Taxi Zone row count

Silver monitoring includes:

- required field completeness
- duplicate trip detection
- invalid timestamp detection
- source-month consistency
- Weather timestamp uniqueness
- Taxi Zone uniqueness

Gold monitoring includes:

- fact row count consistency
- duplicate `trip_key` detection
- duplicate `trip_hash` detection
- null foreign-key detection
- dimension primary-key uniqueness
- referential integrity
- `trip_count = 1`
- expected dimension sizes

### Monitoring Dashboard

A Databricks monitoring dashboard should surface the most important operational indicators.

Recommended sections include:

#### Execution

- latest pipeline status
- recent pipeline runs
- task duration
- execution history

#### Failures

- recent failed tasks
- failure count
- failure timestamps
- downstream task impact

#### Freshness

- latest Taxi source month
- latest Taxi ingestion timestamp
- latest Weather observation
- Weather hourly completeness

#### Data Quality

- Bronze row counts
- Silver row counts
- Gold fact row count
- null checks
- duplicate checks
- key-integrity checks

---

## Governance

### Naming Conventions

The project uses consistent naming conventions across layers.

#### Bronze

Bronze tables use source-oriented names:

- `green_taxi`
- `weather`
- `taxi_zones`

#### Silver

Silver tables use standardized analytical names:

- `taxi_trips`
- `weather`
- `taxi_zones`

#### Gold

Gold tables use dimensional-model prefixes:

- `fact_` for fact tables
- `dim_` for dimension tables

Examples:

- `fact_trip`
- `dim_date`
- `dim_hour`
- `dim_zone`
- `dim_weather`

Primary and foreign keys use the `_key` suffix where appropriate.

Examples:

- `trip_key`
- `date_key`
- `zone_key`
- `weather_key`

Source-system business identifiers retain source terminology where useful.

Example:

- `location_id`

### Ownership

The project repository and Databricks pipeline are maintained as part of the NYC Mobility data engineering project.

Ownership includes responsibility for:

- pipeline code
- data transformations
- validation logic
- documentation
- monitoring
- GitHub CI
- Databricks orchestration

### Permissions and Access

Data access is managed through Databricks catalog, schema, and table permissions.

The project uses:

- `nyc_mobility.bronze`
- `nyc_mobility.silver`
- `nyc_mobility.gold`

Recommended access principles:

- pipeline execution identities require write access to the required schemas
- project maintainers require development access
- analytical consumers should receive read-only access to the layers they need
- Gold should be the preferred layer for analytics and BI consumption

### Lineage

Lineage is preserved across the pipeline.

Bronze contains source provenance fields such as:

- source system
- source file or URL
- source month
- batch ID
- ingestion timestamp

Silver preserves relevant provenance metadata and deterministic identifiers such as `trip_hash`.

Gold preserves dimensional relationships through foreign keys.

Pipeline task dependencies also provide operational lineage between stages.

The high-level lineage is:

```text
Source
→ Bronze
→ Silver
→ Gold
→ Analytics
```

### Key Data-Quality Rules

Key project rules include:

- source metadata must not be null
- Taxi source files must not be loaded more than once
- Weather batches must not be loaded more than once
- Taxi pickup and dropoff timestamps must be valid
- dropoff must not occur before pickup
- pickup and dropoff location IDs must be populated
- pickup month must match `source_month`
- `trip_hash` must be unique after deduplication
- Weather timestamps must be unique
- Taxi Zone `location_id` must be unique
- Gold foreign keys must be populated
- Gold dimension keys must be unique
- Gold fact-to-dimension relationships must be valid
- `dim_hour` must contain 24 rows
- `dim_zone` must contain 265 rows

---

## Data Lineage Summary

```text
NYC Green Taxi Parquet ──────────────┐
                                     │
Open-Meteo REST API ─────────────────┼──> BRONZE
                                     │
NYC Taxi Zones CSV ──────────────────┘
                                             ↓
                                      Bronze Validation
                                             ↓
                                           SILVER
                                             ↓
                                      Silver Validation
                                             ↓
                                            GOLD
                                             ↓
                                       Gold Validation
                                             ↓
                                          Analytics
```

---

## Development and Deployment Flow

The project follows a source-controlled engineering workflow.

```text
Code Change
    ↓
Unit Tests
    ↓
Commit
    ↓
Push to GitHub
    ↓
GitHub Actions CI
    ↓
Databricks Pipeline
    ↓
Validation
    ↓
Analytics
```

This workflow helps ensure that code is tested before it is used in the data pipeline and that each data layer is validated before downstream processing continues.
