-- ============================================================
-- NYC MOBILITY
-- BRONZE -> SILVER DATA QUALITY GATE
-- ============================================================
--
-- PURPOSE
-- Validate that Bronze data was transformed correctly into
-- Silver before Gold transformation is allowed to run.
--
-- SOURCES
-- nyc_mobility.bronze.green_taxi
-- nyc_mobility.bronze.weather
-- nyc_mobility.bronze.taxi_zones
--
-- TARGETS
-- nyc_mobility.silver.taxi_trips
-- nyc_mobility.silver.weather
-- nyc_mobility.silver.taxi_zones
--
-- CHECK TYPES
-- 1. ROW COUNT / RECONCILIATION
-- 2. NULL
-- 3. UNIQUE
-- 4. RANGE
-- 5. ACCEPTED VALUES
-- 6. REFERENTIAL INTEGRITY
-- 7. TRANSFORMATION COVERAGE
--
-- ============================================================


WITH taxi_trip_checks AS (

    -- ========================================================
    -- 1. TAXI TRIPS - SILVER TABLE NOT EMPTY
    -- ========================================================

    SELECT
        'BRONZE_TO_SILVER' AS stage,
        'taxi_trips' AS dataset,
        'CRITICAL' AS severity,
        'ROW_COUNT' AS check_type,
        'silver_not_empty' AS check_name,
        COUNT(*) AS total_rows,
        CASE
            WHEN COUNT(*) > 0 THEN 0
            ELSE 1
        END AS failed_rows,
        CASE
            WHEN COUNT(*) > 0 THEN 'PASS'
            ELSE 'FAIL'
        END AS status,
        'Silver Taxi Trips must contain transformed records.'
            AS description
    FROM nyc_mobility.silver.taxi_trips


    UNION ALL


    -- ========================================================
    -- 2. TAXI TRIPS - SILVER SHOULD NOT EXCEED BRONZE
    -- ========================================================

    SELECT
        'BRONZE_TO_SILVER',
        'taxi_trips',
        'CRITICAL',
        'RECONCILIATION',
        'silver_not_greater_than_bronze',

        (SELECT COUNT(*)
         FROM nyc_mobility.silver.taxi_trips),

        CASE
            WHEN
                (SELECT COUNT(*)
                 FROM nyc_mobility.silver.taxi_trips)
                >
                (SELECT COUNT(*)
                 FROM nyc_mobility.bronze.green_taxi)

            THEN
                (SELECT COUNT(*)
                 FROM nyc_mobility.silver.taxi_trips)
                -
                (SELECT COUNT(*)
                 FROM nyc_mobility.bronze.green_taxi)

            ELSE 0
        END,

        CASE
            WHEN
                (SELECT COUNT(*)
                 FROM nyc_mobility.silver.taxi_trips)
                <=
                (SELECT COUNT(*)
                 FROM nyc_mobility.bronze.green_taxi)

            THEN 'PASS'
            ELSE 'FAIL'
        END,

        'Silver Taxi Trips should not unexpectedly contain more rows than Bronze.'


    UNION ALL


    -- ========================================================
    -- 3. TAXI TRIPS - PICKUP DATETIME NOT NULL
    -- ========================================================

    SELECT
        'BRONZE_TO_SILVER',
        'taxi_trips',
        'CRITICAL',
        'NULL',
        'pickup_datetime_not_null',
        COUNT(*),

        COALESCE(
            SUM(
                CASE
                    WHEN pickup_datetime IS NULL THEN 1
                    ELSE 0
                END
            ),
            0
        ),

        CASE
            WHEN COALESCE(
                SUM(
                    CASE
                        WHEN pickup_datetime IS NULL THEN 1
                        ELSE 0
                    END
                ),
                0
            ) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,

        'Every Silver taxi trip must have a standardized pickup datetime.'

    FROM nyc_mobility.silver.taxi_trips


    UNION ALL


    -- ========================================================
    -- 4. TAXI TRIPS - DROPOFF DATETIME NOT NULL
    -- ========================================================

    SELECT
        'BRONZE_TO_SILVER',
        'taxi_trips',
        'CRITICAL',
        'NULL',
        'dropoff_datetime_not_null',
        COUNT(*),

        COALESCE(
            SUM(
                CASE
                    WHEN dropoff_datetime IS NULL THEN 1
                    ELSE 0
                END
            ),
            0
        ),

        CASE
            WHEN COALESCE(
                SUM(
                    CASE
                        WHEN dropoff_datetime IS NULL THEN 1
                        ELSE 0
                    END
                ),
                0
            ) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,

        'Every Silver taxi trip must have a standardized dropoff datetime.'

    FROM nyc_mobility.silver.taxi_trips


    UNION ALL


    -- ========================================================
    -- 5. TAXI TRIPS - DROPOFF AFTER PICKUP
    -- ========================================================

    SELECT
        'BRONZE_TO_SILVER',
        'taxi_trips',
        'CRITICAL',
        'RANGE',
        'dropoff_after_pickup',
        COUNT(*),

        COALESCE(
            SUM(
                CASE
                    WHEN dropoff_datetime < pickup_datetime THEN 1
                    ELSE 0
                END
            ),
            0
        ),

        CASE
            WHEN COALESCE(
                SUM(
                    CASE
                        WHEN dropoff_datetime < pickup_datetime THEN 1
                        ELSE 0
                    END
                ),
                0
            ) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,

        'Dropoff datetime must not occur before pickup datetime.'

    FROM nyc_mobility.silver.taxi_trips


    UNION ALL


    -- ========================================================
    -- 6. TAXI TRIPS - TRIP DURATION NON-NEGATIVE
    -- ========================================================

    SELECT
        'BRONZE_TO_SILVER',
        'taxi_trips',
        'CRITICAL',
        'RANGE',
        'trip_duration_non_negative',
        COUNT(*),

        COALESCE(
            SUM(
                CASE
                    WHEN trip_duration_minutes < 0 THEN 1
                    ELSE 0
                END
            ),
            0
        ),

        CASE
            WHEN COALESCE(
                SUM(
                    CASE
                        WHEN trip_duration_minutes < 0 THEN 1
                        ELSE 0
                    END
                ),
                0
            ) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,

        'Calculated trip duration must not be negative.'

    FROM nyc_mobility.silver.taxi_trips


    UNION ALL


    -- ========================================================
    -- 7. TAXI TRIPS - PICKUP LOCATION NOT NULL
    -- ========================================================

    SELECT
        'BRONZE_TO_SILVER',
        'taxi_trips',
        'CRITICAL',
        'NULL',
        'pickup_location_not_null',
        COUNT(*),

        COALESCE(
            SUM(
                CASE
                    WHEN pickup_location_id IS NULL THEN 1
                    ELSE 0
                END
            ),
            0
        ),

        CASE
            WHEN COALESCE(
                SUM(
                    CASE
                        WHEN pickup_location_id IS NULL THEN 1
                        ELSE 0
                    END
                ),
                0
            ) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,

        'Pickup location is required for zone enrichment.'

    FROM nyc_mobility.silver.taxi_trips


    UNION ALL


    -- ========================================================
    -- 8. TAXI TRIPS - DROPOFF LOCATION NOT NULL
    -- ========================================================

    SELECT
        'BRONZE_TO_SILVER',
        'taxi_trips',
        'CRITICAL',
        'NULL',
        'dropoff_location_not_null',
        COUNT(*),

        COALESCE(
            SUM(
                CASE
                    WHEN dropoff_location_id IS NULL THEN 1
                    ELSE 0
                END
            ),
            0
        ),

        CASE
            WHEN COALESCE(
                SUM(
                    CASE
                        WHEN dropoff_location_id IS NULL THEN 1
                        ELSE 0
                    END
                ),
                0
            ) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,

        'Dropoff location is required for zone enrichment.'

    FROM nyc_mobility.silver.taxi_trips


    UNION ALL


    -- ========================================================
    -- 9. TAXI TRIPS - TRIP DISTANCE NON-NEGATIVE
    -- ========================================================

    SELECT
        'BRONZE_TO_SILVER',
        'taxi_trips',
        'CRITICAL',
        'RANGE',
        'trip_distance_non_negative',
        COUNT(*),

        COALESCE(
            SUM(
                CASE
                    WHEN trip_distance < 0 THEN 1
                    ELSE 0
                END
            ),
            0
        ),

        CASE
            WHEN COALESCE(
                SUM(
                    CASE
                        WHEN trip_distance < 0 THEN 1
                        ELSE 0
                    END
                ),
                0
            ) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,

        'Trip distance must not be negative after Silver transformation.'

    FROM nyc_mobility.silver.taxi_trips


    UNION ALL


    -- ========================================================
    -- 10. TAXI TRIPS - FARE NON-NEGATIVE
    -- ========================================================

    SELECT
        'BRONZE_TO_SILVER',
        'taxi_trips',
        'WARNING',
        'RANGE',
        'fare_amount_non_negative',
        COUNT(*),

        COALESCE(
            SUM(
                CASE
                    WHEN fare_amount < 0 THEN 1
                    ELSE 0
                END
            ),
            0
        ),

        CASE
            WHEN COALESCE(
                SUM(
                    CASE
                        WHEN fare_amount < 0 THEN 1
                        ELSE 0
                    END
                ),
                0
            ) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,

        'Negative fares are flagged for review.'

    FROM nyc_mobility.silver.taxi_trips


    UNION ALL


    -- ========================================================
    -- 11. TAXI TRIPS - PAYMENT TYPE ACCEPTED VALUES
    -- ========================================================

    SELECT
        'BRONZE_TO_SILVER',
        'taxi_trips',
        'WARNING',
        'ACCEPTED_VALUES',
        'payment_type_valid',
        COUNT(*),

        COALESCE(
            SUM(
                CASE
                    WHEN payment_type IS NOT NULL
                         AND payment_type NOT IN (1,2,3,4,5,6)
                    THEN 1
                    ELSE 0
                END
            ),
            0
        ),

        CASE
            WHEN COALESCE(
                SUM(
                    CASE
                        WHEN payment_type IS NOT NULL
                             AND payment_type NOT IN (1,2,3,4,5,6)
                        THEN 1
                        ELSE 0
                    END
                ),
                0
            ) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,

        'Non-null payment_type values must use accepted TLC codes.'

    FROM nyc_mobility.silver.taxi_trips


    UNION ALL


    -- ========================================================
    -- 12. TAXI TRIPS - TRIP HASH NOT NULL
    -- ========================================================

    SELECT
        'BRONZE_TO_SILVER',
        'taxi_trips',
        'CRITICAL',
        'NULL',
        'trip_hash_not_null',
        COUNT(*),

        COALESCE(
            SUM(
                CASE
                    WHEN trip_hash IS NULL
                         OR TRIM(trip_hash) = ''
                    THEN 1
                    ELSE 0
                END
            ),
            0
        ),

        CASE
            WHEN COALESCE(
                SUM(
                    CASE
                        WHEN trip_hash IS NULL
                             OR TRIM(trip_hash) = ''
                        THEN 1
                        ELSE 0
                    END
                ),
                0
            ) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,

        'Every Silver taxi trip should have a trip hash.'

    FROM nyc_mobility.silver.taxi_trips


    UNION ALL


    -- ========================================================
    -- 13. TAXI TRIPS - TRIP HASH UNIQUE
    -- ========================================================

    SELECT
        'BRONZE_TO_SILVER',
        'taxi_trips',
        'CRITICAL',
        'UNIQUE',
        'trip_hash_unique',
        COUNT(*),

        COUNT(*) - COUNT(DISTINCT trip_hash),

        CASE
            WHEN COUNT(*) = COUNT(DISTINCT trip_hash)
            THEN 'PASS'
            ELSE 'FAIL'
        END,

        'trip_hash should uniquely identify each Silver taxi trip.'

    FROM nyc_mobility.silver.taxi_trips


    UNION ALL


    -- ========================================================
    -- 14. TAXI TRIPS - PICKUP LOCATION REFERENTIAL INTEGRITY
    -- ========================================================

    SELECT
        'BRONZE_TO_SILVER',
        'taxi_trips',
        'CRITICAL',
        'REFERENTIAL_INTEGRITY',
        'pickup_location_exists',
        COUNT(*),

        COALESCE(
            SUM(
                CASE
                    WHEN z.LocationID IS NULL THEN 1
                    ELSE 0
                END
            ),
            0
        ),

        CASE
            WHEN COALESCE(
                SUM(
                    CASE
                        WHEN z.LocationID IS NULL THEN 1
                        ELSE 0
                    END
                ),
                0
            ) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,

        'Every pickup_location_id must exist in Silver Taxi Zones.'

    FROM nyc_mobility.silver.taxi_trips t

    LEFT JOIN nyc_mobility.silver.taxi_zones z
        ON t.pickup_location_id = z.LocationID


    UNION ALL


    -- ========================================================
    -- 15. TAXI TRIPS - DROPOFF LOCATION REFERENTIAL INTEGRITY
    -- ========================================================

    SELECT
        'BRONZE_TO_SILVER',
        'taxi_trips',
        'CRITICAL',
        'REFERENTIAL_INTEGRITY',
        'dropoff_location_exists',
        COUNT(*),

        COALESCE(
            SUM(
                CASE
                    WHEN z.LocationID IS NULL THEN 1
                    ELSE 0
                END
            ),
            0
        ),

        CASE
            WHEN COALESCE(
                SUM(
                    CASE
                        WHEN z.LocationID IS NULL THEN 1
                        ELSE 0
                    END
                ),
                0
            ) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,

        'Every dropoff_location_id must exist in Silver Taxi Zones.'

    FROM nyc_mobility.silver.taxi_trips t

    LEFT JOIN nyc_mobility.silver.taxi_zones z
        ON t.dropoff_location_id = z.LocationID

),


-- ============================================================
-- WEATHER CHECKS
-- Bronze Weather -> Silver Weather
-- ============================================================

weather_checks AS (

    -- ========================================================
    -- 16. WEATHER - SILVER TABLE NOT EMPTY
    -- ========================================================

    SELECT
        'BRONZE_TO_SILVER' AS stage,
        'weather' AS dataset,
        'CRITICAL' AS severity,
        'ROW_COUNT' AS check_type,
        'silver_not_empty' AS check_name,
        COUNT(*) AS total_rows,

        CASE
            WHEN COUNT(*) > 0 THEN 0
            ELSE 1
        END AS failed_rows,

        CASE
            WHEN COUNT(*) > 0 THEN 'PASS'
            ELSE 'FAIL'
        END AS status,

        'Silver Weather must contain transformed records.'
            AS description

    FROM nyc_mobility.silver.weather


    UNION ALL


    -- ========================================================
    -- 17. WEATHER - BRONZE/SILVER ROW RECONCILIATION
    -- ========================================================

    SELECT
        'BRONZE_TO_SILVER',
        'weather',
        'CRITICAL',
        'RECONCILIATION',
        'weather_row_count_matches_bronze',

        (SELECT COUNT(*)
         FROM nyc_mobility.silver.weather),

        ABS(
            (SELECT COUNT(*)
             FROM nyc_mobility.bronze.weather)
            -
            (SELECT COUNT(*)
             FROM nyc_mobility.silver.weather)
        ),

        CASE
            WHEN
                (SELECT COUNT(*)
                 FROM nyc_mobility.bronze.weather)
                =
                (SELECT COUNT(*)
                 FROM nyc_mobility.silver.weather)

            THEN 'PASS'
            ELSE 'FAIL'
        END,

        'Silver Weather should preserve the expected Bronze weather population.'


    UNION ALL


    -- ========================================================
    -- 18. WEATHER - TIME NOT NULL
    -- ========================================================

    SELECT
        'BRONZE_TO_SILVER',
        'weather',
        'CRITICAL',
        'NULL',
        'time_not_null',
        COUNT(*),

        COALESCE(
            SUM(
                CASE
                    WHEN time IS NULL THEN 1
                    ELSE 0
                END
            ),
            0
        ),

        CASE
            WHEN COALESCE(
                SUM(
                    CASE
                        WHEN time IS NULL THEN 1
                        ELSE 0
                    END
                ),
                0
            ) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,

        'Every Silver weather record must have a timestamp.'

    FROM nyc_mobility.silver.weather


    UNION ALL


    -- ========================================================
    -- 19. WEATHER - TIME UNIQUE
    -- ========================================================

    SELECT
        'BRONZE_TO_SILVER',
        'weather',
        'CRITICAL',
        'UNIQUE',
        'time_unique',
        COUNT(*),

        COUNT(*) - COUNT(DISTINCT time),

        CASE
            WHEN COUNT(*) = COUNT(DISTINCT time)
            THEN 'PASS'
            ELSE 'FAIL'
        END,

        'Silver Weather should contain one record per timestamp.'

    FROM nyc_mobility.silver.weather


    UNION ALL


    -- ========================================================
    -- 20. WEATHER - TEMPERATURE NOT NULL
    -- ========================================================

    SELECT
        'BRONZE_TO_SILVER',
        'weather',
        'CRITICAL',
        'NULL',
        'temperature_not_null',
        COUNT(*),

        COALESCE(
            SUM(
                CASE
                    WHEN temperature_2m IS NULL THEN 1
                    ELSE 0
                END
            ),
            0
        ),

        CASE
            WHEN COALESCE(
                SUM(
                    CASE
                        WHEN temperature_2m IS NULL THEN 1
                        ELSE 0
                    END
                ),
                0
            ) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,

        'Temperature must be populated in Silver Weather.'

    FROM nyc_mobility.silver.weather


    UNION ALL


    -- ========================================================
    -- 21. WEATHER - PRECIPITATION NOT NULL
    -- ========================================================

    SELECT
        'BRONZE_TO_SILVER',
        'weather',
        'CRITICAL',
        'NULL',
        'precipitation_not_null',
        COUNT(*),

        COALESCE(
            SUM(
                CASE
                    WHEN precipitation IS NULL THEN 1
                    ELSE 0
                END
            ),
            0
        ),

        CASE
            WHEN COALESCE(
                SUM(
                    CASE
                        WHEN precipitation IS NULL THEN 1
                        ELSE 0
                    END
                ),
                0
            ) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,

        'Precipitation must be populated in Silver Weather.'

    FROM nyc_mobility.silver.weather


    UNION ALL


    -- ========================================================
    -- 22. WEATHER - PRECIPITATION NON-NEGATIVE
    -- ========================================================

    SELECT
        'BRONZE_TO_SILVER',
        'weather',
        'CRITICAL',
        'RANGE',
        'precipitation_non_negative',
        COUNT(*),

        COALESCE(
            SUM(
                CASE
                    WHEN precipitation < 0 THEN 1
                    ELSE 0
                END
            ),
            0
        ),

        CASE
            WHEN COALESCE(
                SUM(
                    CASE
                        WHEN precipitation < 0 THEN 1
                        ELSE 0
                    END
                ),
                0
            ) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,

        'Precipitation cannot be negative.'

    FROM nyc_mobility.silver.weather


    UNION ALL


    -- ========================================================
    -- 23. WEATHER - WEATHER CODE NOT NULL
    -- ========================================================

    SELECT
        'BRONZE_TO_SILVER',
        'weather',
        'WARNING',
        'NULL',
        'weather_code_not_null',
        COUNT(*),

        COALESCE(
            SUM(
                CASE
                    WHEN weather_code IS NULL THEN 1
                    ELSE 0
                END
            ),
            0
        ),

        CASE
            WHEN COALESCE(
                SUM(
                    CASE
                        WHEN weather_code IS NULL THEN 1
                        ELSE 0
                    END
                ),
                0
            ) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,

        'Missing weather codes should be reviewed.'

    FROM nyc_mobility.silver.weather

),


-- ============================================================
-- TAXI ZONE CHECKS
-- Bronze Taxi Zones -> Silver Taxi Zones
-- ============================================================

taxi_zone_checks AS (

    -- ========================================================
    -- 24. TAXI ZONES - SILVER TABLE NOT EMPTY
    -- ========================================================

    SELECT
        'BRONZE_TO_SILVER' AS stage,
        'taxi_zones' AS dataset,
        'CRITICAL' AS severity,
        'ROW_COUNT' AS check_type,
        'silver_not_empty' AS check_name,
        COUNT(*) AS total_rows,

        CASE
            WHEN COUNT(*) > 0 THEN 0
            ELSE 1
        END AS failed_rows,

        CASE
            WHEN COUNT(*) > 0 THEN 'PASS'
            ELSE 'FAIL'
        END AS status,

        'Silver Taxi Zones must contain reference records.'
            AS description

    FROM nyc_mobility.silver.taxi_zones


    UNION ALL


    -- ========================================================
    -- 25. TAXI ZONES - ROW RECONCILIATION
    -- ========================================================

    SELECT
        'BRONZE_TO_SILVER',
        'taxi_zones',
        'CRITICAL',
        'RECONCILIATION',
        'zone_row_count_matches_bronze',

        (SELECT COUNT(*)
         FROM nyc_mobility.silver.taxi_zones),

        ABS(
            (SELECT COUNT(*)
             FROM nyc_mobility.bronze.taxi_zones)
            -
            (SELECT COUNT(*)
             FROM nyc_mobility.silver.taxi_zones)
        ),

        CASE
            WHEN
                (SELECT COUNT(*)
                 FROM nyc_mobility.bronze.taxi_zones)
                =
                (SELECT COUNT(*)
                 FROM nyc_mobility.silver.taxi_zones)

            THEN 'PASS'
            ELSE 'FAIL'
        END,

        'Silver Taxi Zones should preserve the expected Bronze reference population.'


    UNION ALL


    -- ========================================================
    -- 26. TAXI ZONES - LOCATION ID NOT NULL
    -- ========================================================

    SELECT
        'BRONZE_TO_SILVER',
        'taxi_zones',
        'CRITICAL',
        'NULL',
        'location_id_not_null',
        COUNT(*),

        COALESCE(
            SUM(
                CASE
                    WHEN LocationID IS NULL THEN 1
                    ELSE 0
                END
            ),
            0
        ),

        CASE
            WHEN COALESCE(
                SUM(
                    CASE
                        WHEN LocationID IS NULL THEN 1
                        ELSE 0
                    END
                ),
                0
            ) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,

        'LocationID is required for Silver Taxi Zones.'

    FROM nyc_mobility.silver.taxi_zones


    UNION ALL


    -- ========================================================
    -- 27. TAXI ZONES - LOCATION ID UNIQUE
    -- ========================================================

    SELECT
        'BRONZE_TO_SILVER',
        'taxi_zones',
        'CRITICAL',
        'UNIQUE',
        'location_id_unique',
        COUNT(*),

        COUNT(*) - COUNT(DISTINCT LocationID),

        CASE
            WHEN COUNT(*) = COUNT(DISTINCT LocationID)
            THEN 'PASS'
            ELSE 'FAIL'
        END,

        'Each LocationID must identify only one Silver Taxi Zone.'

    FROM nyc_mobility.silver.taxi_zones


    UNION ALL


    -- ========================================================
    -- 28. TAXI ZONES - BOROUGH NOT NULL
    -- ========================================================

    SELECT
        'BRONZE_TO_SILVER',
        'taxi_zones',
        'CRITICAL',
        'NULL',
        'borough_not_null',
        COUNT(*),

        COALESCE(
            SUM(
                CASE
                    WHEN Borough IS NULL
                         OR TRIM(Borough) = ''
                    THEN 1
                    ELSE 0
                END
            ),
            0
        ),

        CASE
            WHEN COALESCE(
                SUM(
                    CASE
                        WHEN Borough IS NULL
                             OR TRIM(Borough) = ''
                        THEN 1
                        ELSE 0
                    END
                ),
                0
            ) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,

        'Borough must be populated in Silver Taxi Zones.'

    FROM nyc_mobility.silver.taxi_zones


    UNION ALL


    -- ========================================================
    -- 29. TAXI ZONES - ZONE NAME NOT NULL
    -- ========================================================

    SELECT
        'BRONZE_TO_SILVER',
        'taxi_zones',
        'CRITICAL',
        'NULL',
        'zone_not_null',
        COUNT(*),

        COALESCE(
            SUM(
                CASE
                    WHEN Zone IS NULL
                         OR TRIM(Zone) = ''
                    THEN 1
                    ELSE 0
                END
            ),
            0
        ),

        CASE
            WHEN COALESCE(
                SUM(
                    CASE
                        WHEN Zone IS NULL
                             OR TRIM(Zone) = ''
                        THEN 1
                        ELSE 0
                    END
                ),
                0
            ) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END,

        'Zone name must be populated in Silver Taxi Zones.'

    FROM nyc_mobility.silver.taxi_zones

),


-- ============================================================
-- COMBINE ALL QUALITY CHECKS
-- ============================================================

all_checks AS (

    SELECT * FROM taxi_trip_checks

    UNION ALL

    SELECT * FROM weather_checks

    UNION ALL

    SELECT * FROM taxi_zone_checks

),


-- ============================================================
-- CALCULATE OVERALL QUALITY GATE STATUS
-- ============================================================

final_results AS (

    SELECT
        stage,
        dataset,
        severity,
        check_type,
        check_name,
        total_rows,
        failed_rows,
        status,
        description,

        CASE
            WHEN SUM(
                CASE
                    WHEN severity = 'CRITICAL'
                         AND status = 'FAIL'
                    THEN 1
                    ELSE 0
                END
            ) OVER () > 0

            THEN 'BLOCK SILVER -> GOLD'

            ELSE 'PASS SILVER -> GOLD'
        END AS overall_gate_status

    FROM all_checks

)


-- ============================================================
-- FINAL OUTPUT
-- Critical failures appear first.
-- ============================================================

SELECT
    stage,
    dataset,
    severity,
    check_type,
    check_name,
    total_rows,
    failed_rows,
    status,
    overall_gate_status,
    description

FROM final_results

ORDER BY

    CASE
        WHEN status = 'FAIL'
             AND severity = 'CRITICAL'
        THEN 1

        WHEN status = 'FAIL'
             AND severity = 'WARNING'
        THEN 2

        ELSE 3
    END,

    dataset,
    check_type,
    check_name;