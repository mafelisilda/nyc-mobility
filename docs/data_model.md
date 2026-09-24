# Gold Data Model

## Business Process

The Gold model represents NYC Green Taxi mobility activity enriched with taxi-zone and weather information.

## Fact Grain

`fact_trip` has one row per valid NYC Green Taxi trip.

## Fact Table

### fact_trip

Primary key:

- `trip_key`

Technical identifier:

- `trip_hash`

Foreign keys:

- `pickup_date_key`
- `dropoff_date_key`
- `pickup_hour_key`
- `dropoff_hour_key`
- `pickup_zone_key`
- `dropoff_zone_key`
- `weather_key`

Measures:

- `passenger_count`
- `trip_distance`
- `trip_duration_minutes`
- `fare_amount`
- `total_amount`
- `temperature_c`
- `precipitation_mm`
- `trip_count`

## Dimensions

### dim_date

Conformed date dimension used by both pickup and dropoff dates.

Primary key:

- `date_key`

Attributes:

- `full_date`
- `year`
- `quarter`
- `month`
- `month_name`
- `day_of_month`
- `day_of_week`
- `day_name`
- `week_of_year`
- `is_weekend`

### dim_hour

Conformed hour dimension with 24 rows.

Primary key:

- `hour_key`

Attributes:

- `hour_of_day`
- `hour_label`
- `day_period`

### dim_zone

Taxi-zone dimension.

Primary key:

- `zone_key`

Business key:

- `location_id`

Attributes:

- `borough`
- `zone`
- `service_zone`

### dim_weather

Descriptive weather dimension.

Primary key:

- `weather_key`

Attributes:

- `weather_condition`
- `temperature_band`
- `precipitation_band`
- `is_raining`

Exact temperature and precipitation measurements remain in `fact_trip`.

## Role-Playing Dimensions

The model reuses the same physical dimensions for multiple business roles.

`dim_date`:

- pickup date
- dropoff date

`dim_hour`:

- pickup hour
- dropoff hour

`dim_zone`:

- pickup zone
- dropoff zone

Separate pickup and dropoff dimension tables are not required.
