# NYC Mobility Gold Star Schema

### Overview

The Gold layer uses one trip fact table and four dimensions. It supports analysis of NYC Green Taxi trips by pickup or dropoff date, hour, zone, and weather at pickup.
![NYC Star Schema](img/nyc_data_model.jpeg)
### Table grain

| Table | One row represents | Primary key |
| --- | --- | --- |
| `fact_trip` | One valid NYC Green Taxi trip | `trip_key` |
| `dim_date` | One calendar date found in valid pickup or dropoff data | `date_key` |
| `dim_hour` | One hour of the day; 24 rows total | `hour_key` |
| `dim_zone` | One NYC TLC taxi zone | `zone_key` |
| `dim_weather` | One distinct combination of descriptive weather attributes | `weather_key` |

### Fact table: `fact_trip`

Each trip has separate dimension keys for its pickup and dropoff:

| Foreign key | Dimension | Role |
| --- | --- | --- |
| `pickup_date_key` | `dim_date` | Pickup date |
| `dropoff_date_key` | `dim_date` | Dropoff date |
| `pickup_hour_key` | `dim_hour` | Pickup hour |
| `dropoff_hour_key` | `dim_hour` | Dropoff hour |
| `pickup_zone_key` | `dim_zone` | Pickup zone |
| `dropoff_zone_key` | `dim_zone` | Dropoff zone |
| `weather_key` | `dim_weather` | Weather associated with the pickup hour |

The fact table contains these measures: `passenger_count`, `trip_distance`, `trip_duration_minutes`, `fare_amount`, `tip_amount`, `total_amount`, `temperature_c`, `precipitation_mm`, and `trip_count`.

### Dimensions

- **`dim_date`:** Full date, year, quarter, month, month name, day of month, day of week, day name, week of year, and weekend flag.
- **`dim_hour`:** Hour of day, hour label, and day period.
- **`dim_zone`:** TLC `location_id`, borough, zone, and service zone.
- **`dim_weather`:** Weather condition, temperature band, precipitation band, and rain flag.

### How to query the model

- Count trips with `COUNT(*)` or `SUM(trip_count)`.
- Use the **pickup** or **dropoff** key that matches the question. For example, use `pickup_zone_key` to group trips by pickup zone.
- Use `dim_weather` for weather categories. Use `fact_trip.temperature_c` and `fact_trip.precipitation_mm` for numeric weather analysis.
- When joining the same dimension for pickup and dropoff, use separate aliases such as `pickup_zone` and `dropoff_zone`.

### Implementation check

Confirm in the Gold loading code how hourly weather is matched to each trip's pickup and how unmatched weather is handled. The diagram identifies the relationship but does not define that loading rule.