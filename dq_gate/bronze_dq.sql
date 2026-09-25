-- ============================================================
-- NYC MOBILITY - BRONZE DATA QUALITY GATE
-- ============================================================
-- Purpose:
--   Validate Bronze ingestion before Silver transformation.
--
-- Tables checked:
--   1. nyc_mobility.bronze.green_taxi
--   2. nyc_mobility.bronze.weather
--   3. nyc_mobility.bronze.taxi_zones
--
-- Bronze principles:
--   - Data arrived
--   - Critical source fields are present
--   - Ingestion metadata is present
--   - Reference-table identifiers are usable
--   - No heavy business cleaning is performed here
--
-- Expected:
--   All checks should return PASS
-- ============================================================


WITH

-- ============================================================
-- GREEN TAXI CHECKS
-- ============================================================

green_taxi_checks AS (

    -- --------------------------------------------------------
    -- 1. Table must not be empty
    -- --------------------------------------------------------
    SELECT
        'BRONZE' AS layer,
        'green_taxi' AS table_name,
        'ROW_COUNT' AS check_type,
        'table_not_empty' AS check_name,
        COUNT(*) AS total_rows,
        CASE
            WHEN COUNT(*) > 0 THEN 0
            ELSE 1
        END AS failed_rows,
        CASE
            WHEN COUNT(*) > 0 THEN 'PASS'
            ELSE 'FAIL'
        END AS status,
        'Green Taxi Bronze table must contain records.' AS description
    FROM nyc_mobility.bronze.green_taxi


    UNION ALL


    -- --------------------------------------------------------
    -- 2. Pickup datetime must not be NULL
    -- --------------------------------------------------------
    SELECT
        'BRONZE',
        'green_taxi',
        'NULL',
        'pickup_datetime_not_null',
        COUNT(*),
        COALESCE(SUM(
            CASE
                WHEN lpep_pickup_datetime IS NULL THEN 1
                ELSE 0
            END
        ), 0),
        CASE
            WHEN COALESCE(SUM(
                CASE
                    WHEN lpep_pickup_datetime IS NULL THEN 1
                    ELSE 0
                END
            ), 0) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,
        'Pickup datetime is required for downstream trip processing.'
    FROM nyc_mobility.bronze.green_taxi


    UNION ALL


    -- --------------------------------------------------------
    -- 3. Dropoff datetime must not be NULL
    -- --------------------------------------------------------
    SELECT
        'BRONZE',
        'green_taxi',
        'NULL',
        'dropoff_datetime_not_null',
        COUNT(*),
        COALESCE(SUM(
            CASE
                WHEN lpep_dropoff_datetime IS NULL THEN 1
                ELSE 0
            END
        ), 0),
        CASE
            WHEN COALESCE(SUM(
                CASE
                    WHEN lpep_dropoff_datetime IS NULL THEN 1
                    ELSE 0
                END
            ), 0) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,
        'Dropoff datetime is required for downstream trip processing.'
    FROM nyc_mobility.bronze.green_taxi


    UNION ALL


    -- --------------------------------------------------------
    -- 4. Pickup Location ID must not be NULL
    -- --------------------------------------------------------
    SELECT
        'BRONZE',
        'green_taxi',
        'NULL',
        'pickup_location_not_null',
        COUNT(*),
        COALESCE(SUM(
            CASE
                WHEN PULocationID IS NULL THEN 1
                ELSE 0
            END
        ), 0),
        CASE
            WHEN COALESCE(SUM(
                CASE
                    WHEN PULocationID IS NULL THEN 1
                    ELSE 0
                END
            ), 0) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,
        'Pickup Location ID is required for zone enrichment.'
    FROM nyc_mobility.bronze.green_taxi


    UNION ALL


    -- --------------------------------------------------------
    -- 5. Dropoff Location ID must not be NULL
    -- --------------------------------------------------------
    SELECT
        'BRONZE',
        'green_taxi',
        'NULL',
        'dropoff_location_not_null',
        COUNT(*),
        COALESCE(SUM(
            CASE
                WHEN DOLocationID IS NULL THEN 1
                ELSE 0
            END
        ), 0),
        CASE
            WHEN COALESCE(SUM(
                CASE
                    WHEN DOLocationID IS NULL THEN 1
                    ELSE 0
                END
            ), 0) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,
        'Dropoff Location ID is required for zone enrichment.'
    FROM nyc_mobility.bronze.green_taxi


    UNION ALL


    -- --------------------------------------------------------
    -- 6. Source system metadata
    -- --------------------------------------------------------
    SELECT
        'BRONZE',
        'green_taxi',
        'METADATA',
        'source_system_not_null',
        COUNT(*),
        COALESCE(SUM(
            CASE
                WHEN source_system IS NULL
                     OR TRIM(source_system) = ''
                THEN 1
                ELSE 0
            END
        ), 0),
        CASE
            WHEN COALESCE(SUM(
                CASE
                    WHEN source_system IS NULL
                         OR TRIM(source_system) = ''
                    THEN 1
                    ELSE 0
                END
            ), 0) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,
        'Every Bronze trip should identify its source system.'
    FROM nyc_mobility.bronze.green_taxi


    UNION ALL


    -- --------------------------------------------------------
    -- 7. Source file metadata
    -- --------------------------------------------------------
    SELECT
        'BRONZE',
        'green_taxi',
        'METADATA',
        'source_file_not_null',
        COUNT(*),
        COALESCE(SUM(
            CASE
                WHEN source_file IS NULL
                     OR TRIM(source_file) = ''
                THEN 1
                ELSE 0
            END
        ), 0),
        CASE
            WHEN COALESCE(SUM(
                CASE
                    WHEN source_file IS NULL
                         OR TRIM(source_file) = ''
                    THEN 1
                    ELSE 0
                END
            ), 0) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,
        'Each Bronze trip should be traceable to its source file.'
    FROM nyc_mobility.bronze.green_taxi


    UNION ALL


    -- --------------------------------------------------------
    -- 8. Source month metadata
    -- --------------------------------------------------------
    SELECT
        'BRONZE',
        'green_taxi',
        'METADATA',
        'source_month_not_null',
        COUNT(*),
        COALESCE(SUM(
            CASE
                WHEN source_month IS NULL THEN 1
                ELSE 0
            END
        ), 0),
        CASE
            WHEN COALESCE(SUM(
                CASE
                    WHEN source_month IS NULL THEN 1
                    ELSE 0
                END
            ), 0) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,
        'Each Bronze trip should identify its source month.'
    FROM nyc_mobility.bronze.green_taxi


    UNION ALL


    -- --------------------------------------------------------
    -- 9. Batch ID metadata
    -- --------------------------------------------------------
    SELECT
        'BRONZE',
        'green_taxi',
        'METADATA',
        'batch_id_not_null',
        COUNT(*),
        COALESCE(SUM(
            CASE
                WHEN batch_id IS NULL
                     OR TRIM(batch_id) = ''
                THEN 1
                ELSE 0
            END
        ), 0),
        CASE
            WHEN COALESCE(SUM(
                CASE
                    WHEN batch_id IS NULL
                         OR TRIM(batch_id) = ''
                    THEN 1
                    ELSE 0
                END
            ), 0) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,
        'Each Bronze trip should be traceable to an ingestion batch.'
    FROM nyc_mobility.bronze.green_taxi


    UNION ALL


    -- --------------------------------------------------------
    -- 10. Ingestion timestamp
    -- --------------------------------------------------------
    SELECT
        'BRONZE',
        'green_taxi',
        'METADATA',
        'ingested_at_not_null',
        COUNT(*),
        COALESCE(SUM(
            CASE
                WHEN ingested_at IS NULL THEN 1
                ELSE 0
            END
        ), 0),
        CASE
            WHEN COALESCE(SUM(
                CASE
                    WHEN ingested_at IS NULL THEN 1
                    ELSE 0
                END
            ), 0) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,
        'Every Bronze trip should have an ingestion timestamp.'
    FROM nyc_mobility.bronze.green_taxi

),


-- ============================================================
-- WEATHER CHECKS
-- ============================================================

weather_checks AS (

    -- --------------------------------------------------------
    -- 1. Table must not be empty
    -- --------------------------------------------------------
    SELECT
        'BRONZE' AS layer,
        'weather' AS table_name,
        'ROW_COUNT' AS check_type,
        'table_not_empty' AS check_name,
        COUNT(*) AS total_rows,
        CASE
            WHEN COUNT(*) > 0 THEN 0
            ELSE 1
        END AS failed_rows,
        CASE
            WHEN COUNT(*) > 0 THEN 'PASS'
            ELSE 'FAIL'
        END AS status,
        'Weather Bronze table must contain records.' AS description
    FROM nyc_mobility.bronze.weather


    UNION ALL


    -- --------------------------------------------------------
    -- 2. Weather timestamp
    -- --------------------------------------------------------
    SELECT
        'BRONZE',
        'weather',
        'NULL',
        'time_not_null',
        COUNT(*),
        COALESCE(SUM(
            CASE
                WHEN time IS NULL
                     OR TRIM(time) = ''
                THEN 1
                ELSE 0
            END
        ), 0),
        CASE
            WHEN COALESCE(SUM(
                CASE
                    WHEN time IS NULL
                         OR TRIM(time) = ''
                    THEN 1
                    ELSE 0
                END
            ), 0) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,
        'Weather observation time is required.'
    FROM nyc_mobility.bronze.weather


    UNION ALL


    -- --------------------------------------------------------
    -- 3. Temperature
    -- --------------------------------------------------------
    SELECT
        'BRONZE',
        'weather',
        'NULL',
        'temperature_not_null',
        COUNT(*),
        COALESCE(SUM(
            CASE
                WHEN temperature_2m IS NULL THEN 1
                ELSE 0
            END
        ), 0),
        CASE
            WHEN COALESCE(SUM(
                CASE
                    WHEN temperature_2m IS NULL THEN 1
                    ELSE 0
                END
            ), 0) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,
        'Temperature observations must be available.'
    FROM nyc_mobility.bronze.weather


    UNION ALL


    -- --------------------------------------------------------
    -- 4. Precipitation
    -- --------------------------------------------------------
    SELECT
        'BRONZE',
        'weather',
        'NULL',
        'precipitation_not_null',
        COUNT(*),
        COALESCE(SUM(
            CASE
                WHEN precipitation IS NULL THEN 1
                ELSE 0
            END
        ), 0),
        CASE
            WHEN COALESCE(SUM(
                CASE
                    WHEN precipitation IS NULL THEN 1
                    ELSE 0
                END
            ), 0) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,
        'Precipitation observations must be available.'
    FROM nyc_mobility.bronze.weather


    UNION ALL


    -- --------------------------------------------------------
    -- 5. Weather code
    -- --------------------------------------------------------
    SELECT
        'BRONZE',
        'weather',
        'NULL',
        'weather_code_not_null',
        COUNT(*),
        COALESCE(SUM(
            CASE
                WHEN weather_code IS NULL THEN 1
                ELSE 0
            END
        ), 0),
        CASE
            WHEN COALESCE(SUM(
                CASE
                    WHEN weather_code IS NULL THEN 1
                    ELSE 0
                END
            ), 0) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,
        'Weather code should be available for each observation.'
    FROM nyc_mobility.bronze.weather


    UNION ALL


    -- --------------------------------------------------------
    -- 6. Duplicate weather timestamp
    -- --------------------------------------------------------
    SELECT
        'BRONZE',
        'weather',
        'DUPLICATE',
        'time_unique',
        COUNT(*),
        COUNT(*) - COUNT(DISTINCT time),
        CASE
            WHEN COUNT(*) = COUNT(DISTINCT time)
            THEN 'PASS'
            ELSE 'FAIL'
        END,
        'Weather observations should not contain duplicate timestamps.'
    FROM nyc_mobility.bronze.weather


    UNION ALL


    -- --------------------------------------------------------
    -- 7. Source system
    -- --------------------------------------------------------
    SELECT
        'BRONZE',
        'weather',
        'METADATA',
        'source_system_not_null',
        COUNT(*),
        COALESCE(SUM(
            CASE
                WHEN source_system IS NULL
                     OR TRIM(source_system) = ''
                THEN 1
                ELSE 0
            END
        ), 0),
        CASE
            WHEN COALESCE(SUM(
                CASE
                    WHEN source_system IS NULL
                         OR TRIM(source_system) = ''
                    THEN 1
                    ELSE 0
                END
            ), 0) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,
        'Every Bronze weather record should identify its source system.'
    FROM nyc_mobility.bronze.weather


    UNION ALL


    -- --------------------------------------------------------
    -- 8. Source URL
    -- --------------------------------------------------------
    SELECT
        'BRONZE',
        'weather',
        'METADATA',
        'source_url_not_null',
        COUNT(*),
        COALESCE(SUM(
            CASE
                WHEN source_url IS NULL
                     OR TRIM(source_url) = ''
                THEN 1
                ELSE 0
            END
        ), 0),
        CASE
            WHEN COALESCE(SUM(
                CASE
                    WHEN source_url IS NULL
                         OR TRIM(source_url) = ''
                    THEN 1
                    ELSE 0
                END
            ), 0) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,
        'Every Bronze weather record should be traceable to its source.'
    FROM nyc_mobility.bronze.weather


    UNION ALL


    -- --------------------------------------------------------
    -- 9. Batch ID
    -- --------------------------------------------------------
    SELECT
        'BRONZE',
        'weather',
        'METADATA',
        'batch_id_not_null',
        COUNT(*),
        COALESCE(SUM(
            CASE
                WHEN batch_id IS NULL
                     OR TRIM(batch_id) = ''
                THEN 1
                ELSE 0
            END
        ), 0),
        CASE
            WHEN COALESCE(SUM(
                CASE
                    WHEN batch_id IS NULL
                         OR TRIM(batch_id) = ''
                    THEN 1
                    ELSE 0
                END
            ), 0) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,
        'Every Bronze weather record should belong to an ingestion batch.'
    FROM nyc_mobility.bronze.weather


    UNION ALL


    -- --------------------------------------------------------
    -- 10. Ingestion timestamp
    -- --------------------------------------------------------
    SELECT
        'BRONZE',
        'weather',
        'METADATA',
        'ingested_at_not_null',
        COUNT(*),
        COALESCE(SUM(
            CASE
                WHEN ingested_at IS NULL THEN 1
                ELSE 0
            END
        ), 0),
        CASE
            WHEN COALESCE(SUM(
                CASE
                    WHEN ingested_at IS NULL THEN 1
                    ELSE 0
                END
            ), 0) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,
        'Every Bronze weather record should have an ingestion timestamp.'
    FROM nyc_mobility.bronze.weather

),


-- ============================================================
-- TAXI ZONE CHECKS
-- ============================================================

taxi_zone_checks AS (

    -- --------------------------------------------------------
    -- 1. Table must not be empty
    -- --------------------------------------------------------
    SELECT
        'BRONZE' AS layer,
        'taxi_zones' AS table_name,
        'ROW_COUNT' AS check_type,
        'table_not_empty' AS check_name,
        COUNT(*) AS total_rows,
        CASE
            WHEN COUNT(*) > 0 THEN 0
            ELSE 1
        END AS failed_rows,
        CASE
            WHEN COUNT(*) > 0 THEN 'PASS'
            ELSE 'FAIL'
        END AS status,
        'Taxi Zones Bronze table must contain records.' AS description
    FROM nyc_mobility.bronze.taxi_zones


    UNION ALL


    -- --------------------------------------------------------
    -- 2. LocationID must not be NULL
    -- --------------------------------------------------------
    SELECT
        'BRONZE',
        'taxi_zones',
        'NULL',
        'location_id_not_null',
        COUNT(*),
        COALESCE(SUM(
            CASE
                WHEN LocationID IS NULL THEN 1
                ELSE 0
            END
        ), 0),
        CASE
            WHEN COALESCE(SUM(
                CASE
                    WHEN LocationID IS NULL THEN 1
                    ELSE 0
                END
            ), 0) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,
        'LocationID is required to identify each taxi zone.'
    FROM nyc_mobility.bronze.taxi_zones


    UNION ALL


    -- --------------------------------------------------------
    -- 3. LocationID must be unique
    -- --------------------------------------------------------
    SELECT
        'BRONZE',
        'taxi_zones',
        'UNIQUE',
        'location_id_unique',
        COUNT(*),
        COUNT(*) - COUNT(DISTINCT LocationID),
        CASE
            WHEN COUNT(*) = COUNT(DISTINCT LocationID)
            THEN 'PASS'
            ELSE 'FAIL'
        END,
        'LocationID should uniquely identify each taxi zone.'
    FROM nyc_mobility.bronze.taxi_zones


    UNION ALL


    -- --------------------------------------------------------
    -- 4. Borough must not be NULL / blank
    -- --------------------------------------------------------
    SELECT
        'BRONZE',
        'taxi_zones',
        'NULL',
        'borough_not_null',
        COUNT(*),
        COALESCE(SUM(
            CASE
                WHEN Borough IS NULL
                     OR TRIM(Borough) = ''
                THEN 1
                ELSE 0
            END
        ), 0),
        CASE
            WHEN COALESCE(SUM(
                CASE
                    WHEN Borough IS NULL
                         OR TRIM(Borough) = ''
                    THEN 1
                    ELSE 0
                END
            ), 0) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,
        'Borough should be populated for each taxi zone.'
    FROM nyc_mobility.bronze.taxi_zones


    UNION ALL


    -- --------------------------------------------------------
    -- 5. Zone must not be NULL / blank
    -- --------------------------------------------------------
    SELECT
        'BRONZE',
        'taxi_zones',
        'NULL',
        'zone_not_null',
        COUNT(*),
        COALESCE(SUM(
            CASE
                WHEN Zone IS NULL
                     OR TRIM(Zone) = ''
                THEN 1
                ELSE 0
            END
        ), 0),
        CASE
            WHEN COALESCE(SUM(
                CASE
                    WHEN Zone IS NULL
                         OR TRIM(Zone) = ''
                    THEN 1
                    ELSE 0
                END
            ), 0) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,
        'Zone name should be populated for each taxi zone.'
    FROM nyc_mobility.bronze.taxi_zones


    UNION ALL


    -- --------------------------------------------------------
    -- 6. Source system
    -- --------------------------------------------------------
    SELECT
        'BRONZE',
        'taxi_zones',
        'METADATA',
        'source_system_not_null',
        COUNT(*),
        COALESCE(SUM(
            CASE
                WHEN source_system IS NULL
                     OR TRIM(source_system) = ''
                THEN 1
                ELSE 0
            END
        ), 0),
        CASE
            WHEN COALESCE(SUM(
                CASE
                    WHEN source_system IS NULL
                         OR TRIM(source_system) = ''
                    THEN 1
                    ELSE 0
                END
            ), 0) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,
        'Every Taxi Zone record should identify its source system.'
    FROM nyc_mobility.bronze.taxi_zones


    UNION ALL


    -- --------------------------------------------------------
    -- 7. Source URL
    -- --------------------------------------------------------
    SELECT
        'BRONZE',
        'taxi_zones',
        'METADATA',
        'source_url_not_null',
        COUNT(*),
        COALESCE(SUM(
            CASE
                WHEN source_url IS NULL
                     OR TRIM(source_url) = ''
                THEN 1
                ELSE 0
            END
        ), 0),
        CASE
            WHEN COALESCE(SUM(
                CASE
                    WHEN source_url IS NULL
                         OR TRIM(source_url) = ''
                    THEN 1
                    ELSE 0
                END
            ), 0) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,
        'Every Taxi Zone record should be traceable to its source.'
    FROM nyc_mobility.bronze.taxi_zones


    UNION ALL


    -- --------------------------------------------------------
    -- 8. Batch ID
    -- --------------------------------------------------------
    SELECT
        'BRONZE',
        'taxi_zones',
        'METADATA',
        'batch_id_not_null',
        COUNT(*),
        COALESCE(SUM(
            CASE
                WHEN batch_id IS NULL
                     OR TRIM(batch_id) = ''
                THEN 1
                ELSE 0
            END
        ), 0),
        CASE
            WHEN COALESCE(SUM(
                CASE
                    WHEN batch_id IS NULL
                         OR TRIM(batch_id) = ''
                    THEN 1
                    ELSE 0
                END
            ), 0) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,
        'Every Taxi Zone record should belong to an ingestion batch.'
    FROM nyc_mobility.bronze.taxi_zones


    UNION ALL


    -- --------------------------------------------------------
    -- 9. Ingestion timestamp
    -- --------------------------------------------------------
    SELECT
        'BRONZE',
        'taxi_zones',
        'METADATA',
        'ingested_at_not_null',
        COUNT(*),
        COALESCE(SUM(
            CASE
                WHEN ingested_at IS NULL THEN 1
                ELSE 0
            END
        ), 0),
        CASE
            WHEN COALESCE(SUM(
                CASE
                    WHEN ingested_at IS NULL THEN 1
                    ELSE 0
                END
            ), 0) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,
        'Every Taxi Zone record should have an ingestion timestamp.'
    FROM nyc_mobility.bronze.taxi_zones

),


-- ============================================================
-- COMBINE ALL CHECKS
-- ============================================================

all_checks AS (

    SELECT * FROM green_taxi_checks

    UNION ALL

    SELECT * FROM weather_checks

    UNION ALL

    SELECT * FROM taxi_zone_checks

)


-- ============================================================
-- FINAL RESULT
-- ============================================================

SELECT
    layer,
    table_name,
    check_type,
    check_name,
    total_rows,
    failed_rows,
    status,
    description

FROM all_checks

ORDER BY
    CASE
        WHEN status = 'FAIL' THEN 1
        ELSE 2
    END,
    table_name,
    check_type,
    check_name;