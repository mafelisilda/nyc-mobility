# Databricks notebook source

# COMMAND ----------
# 1. IMPORTS

from pyspark.sql import functions as F

from src.transformations.gold import add_weather_categories


# COMMAND ----------
# 2. LOAD GOLD TABLES

FACT_TRIP = "nyc_mobility.gold.fact_trip"
DIM_DATE = "nyc_mobility.gold.dim_date"
DIM_HOUR = "nyc_mobility.gold.dim_hour"
DIM_ZONE = "nyc_mobility.gold.dim_zone"
DIM_WEATHER = "nyc_mobility.gold.dim_weather"

SILVER_WEATHER = "nyc_mobility.silver.weather"

fact = spark.table(FACT_TRIP)
dim_date = spark.table(DIM_DATE)
dim_hour = spark.table(DIM_HOUR)
dim_zone = spark.table(DIM_ZONE)
dim_weather = spark.table(DIM_WEATHER)

print(f"fact_trip rows: {fact.count():,}")
print(f"dim_date rows: {dim_date.count():,}")
print(f"dim_hour rows: {dim_hour.count():,}")
print(f"dim_zone rows: {dim_zone.count():,}")
print(f"dim_weather rows: {dim_weather.count():,}")


# COMMAND ----------
# 3. PREPARE ROLE-PLAYING ZONE DIMENSIONS

pickup_zone = (
    dim_zone
    .select(
        F.col("zone_key").alias("pickup_zone_key"),
        F.col("borough").alias("pickup_borough"),
        F.col("zone").alias("pickup_zone"),
    )
)

dropoff_zone = (
    dim_zone
    .select(
        F.col("zone_key").alias("dropoff_zone_key"),
        F.col("borough").alias("dropoff_borough"),
        F.col("zone").alias("dropoff_zone"),
    )
)


# COMMAND ----------
# 4. BUSINESS QUESTION 1
# When and where is taxi demand highest?

print("=== QUESTION 1: WHEN AND WHERE IS TAXI DEMAND HIGHEST? ===")


# COMMAND ----------
# 4A. DEMAND BY PICKUP ZONE

demand_by_zone = (
    fact
    .join(
        pickup_zone,
        on="pickup_zone_key",
        how="left",
    )
    .groupBy(
        "pickup_borough",
        "pickup_zone",
    )
    .agg(
        F.sum("trip_count").alias("trip_count"),
    )
    .orderBy(
        F.desc("trip_count")
    )
)

print("Top pickup zones by trip volume:")
display(demand_by_zone.limit(20))


# COMMAND ----------
# 4B. DEMAND BY HOUR

demand_by_hour = (
    fact
    .join(
        dim_hour,
        fact["pickup_hour_key"] == dim_hour["hour_key"],
        how="left",
    )
    .groupBy(
        "hour_of_day",
        "hour_label",
        "day_period",
    )
    .agg(
        F.sum("trip_count").alias("trip_count"),
    )
    .orderBy("hour_of_day")
)

print("Trip demand by pickup hour:")
display(demand_by_hour)


# COMMAND ----------
# 4C. DEMAND BY DAY OF WEEK

demand_by_day = (
    fact
    .join(
        dim_date,
        fact["pickup_date_key"] == dim_date["date_key"],
        how="left",
    )
    .groupBy(
        "day_of_week",
        "day_name",
    )
    .agg(
        F.sum("trip_count").alias("trip_count"),
    )
    .orderBy("day_of_week")
)

print("Trip demand by day of week:")
display(demand_by_day)


# COMMAND ----------
# 4D. DEMAND BY DAY, HOUR, AND PICKUP ZONE

demand_by_day_hour_zone = (
    fact
    .join(
        dim_date,
        fact["pickup_date_key"] == dim_date["date_key"],
        how="left",
    )
    .join(
        dim_hour,
        fact["pickup_hour_key"] == dim_hour["hour_key"],
        how="left",
    )
    .join(
        pickup_zone,
        on="pickup_zone_key",
        how="left",
    )
    .groupBy(
        "day_name",
        "hour_of_day",
        "hour_label",
        "pickup_borough",
        "pickup_zone",
    )
    .agg(
        F.sum("trip_count").alias("trip_count"),
    )
    .orderBy(
        F.desc("trip_count")
    )
)

print("Highest-demand day, hour, and pickup-zone combinations:")
display(demand_by_day_hour_zone.limit(30))


# COMMAND ----------
# 5. BUSINESS QUESTION 2
# How does weather affect volume and trip behavior?

print("=== QUESTION 2: HOW DOES WEATHER AFFECT TAXI ACTIVITY? ===")


# COMMAND ----------
# 5A. TRIP BEHAVIOR BY WEATHER CONDITION

weather_analysis = (
    fact
    .join(
        dim_weather,
        on="weather_key",
        how="left",
    )
    .groupBy(
        "weather_condition",
        "is_raining",
    )
    .agg(
        F.sum("trip_count").alias("trip_count"),
        F.round(
            F.avg("trip_distance"),
            2,
        ).alias("avg_trip_distance"),
        F.round(
            F.avg("trip_duration_minutes"),
            2,
        ).alias("avg_trip_duration_minutes"),
        F.round(
            F.avg("fare_amount"),
            2,
        ).alias("avg_fare_amount"),
        F.round(
            F.avg("total_amount"),
            2,
        ).alias("avg_total_amount"),
    )
    .orderBy(
        F.desc("trip_count")
    )
)

print("Trip behavior by weather condition:")
display(weather_analysis)


# COMMAND ----------
# 5B. RAIN VS NO RAIN

rain_analysis = (
    fact
    .join(
        dim_weather,
        on="weather_key",
        how="left",
    )
    .groupBy(
        "is_raining",
    )
    .agg(
        F.sum("trip_count").alias("trip_count"),
        F.round(
            F.avg("trip_distance"),
            2,
        ).alias("avg_trip_distance"),
        F.round(
            F.avg("trip_duration_minutes"),
            2,
        ).alias("avg_trip_duration_minutes"),
        F.round(
            F.avg("total_amount"),
            2,
        ).alias("avg_total_amount"),
    )
    .orderBy("is_raining")
)

print("Rain vs no-rain trip behavior:")
display(rain_analysis)


# COMMAND ----------
# 5C. NORMALIZE DEMAND BY WEATHER EXPOSURE
#
# Raw trip totals can be misleading because some weather conditions
# occur for more hours than others.
#
# We calculate the number of weather hours in each category, then
# divide trip volume by those hours.

silver_weather = spark.table(SILVER_WEATHER)

weather_with_categories = add_weather_categories(
    silver_weather
)

weather_hours = (
    weather_with_categories
    .groupBy(
        "weather_condition",
        "is_raining",
    )
    .agg(
        F.count("*").alias("weather_hours")
    )
)

trip_weather_counts = (
    fact
    .join(
        dim_weather,
        on="weather_key",
        how="left",
    )
    .groupBy(
        "weather_condition",
        "is_raining",
    )
    .agg(
        F.sum("trip_count").alias("trip_count")
    )
)

weather_normalized_demand = (
    trip_weather_counts
    .join(
        weather_hours,
        on=[
            "weather_condition",
            "is_raining",
        ],
        how="left",
    )
    .withColumn(
        "trips_per_weather_hour",
        F.round(
            F.col("trip_count")
            / F.col("weather_hours"),
            2,
        ),
    )
    .orderBy(
        F.desc("trips_per_weather_hour")
    )
)

print("Taxi demand normalized by weather exposure:")
display(weather_normalized_demand)


# COMMAND ----------
# 5D. NORMALIZED RAIN VS NO RAIN DEMAND

rain_weather_hours = (
    weather_with_categories
    .groupBy("is_raining")
    .agg(
        F.count("*").alias("weather_hours")
    )
)

rain_trip_counts = (
    fact
    .join(
        dim_weather,
        on="weather_key",
        how="left",
    )
    .groupBy("is_raining")
    .agg(
        F.sum("trip_count").alias("trip_count")
    )
)

rain_normalized_demand = (
    rain_trip_counts
    .join(
        rain_weather_hours,
        on="is_raining",
        how="left",
    )
    .withColumn(
        "trips_per_weather_hour",
        F.round(
            F.col("trip_count")
            / F.col("weather_hours"),
            2,
        ),
    )
    .orderBy("is_raining")
)

print("Normalized demand during rain vs no rain:")
display(rain_normalized_demand)


# COMMAND ----------
# 6. BUSINESS QUESTION 3
# Which areas show the strongest mobility patterns or opportunities?

print("=== QUESTION 3: WHICH AREAS SHOW THE STRONGEST MOBILITY PATTERNS? ===")


# COMMAND ----------
# 6A. MOST COMMON PICKUP-DROPOFF ROUTES

route_patterns = (
    fact
    .join(
        pickup_zone,
        on="pickup_zone_key",
        how="left",
    )
    .join(
        dropoff_zone,
        on="dropoff_zone_key",
        how="left",
    )
    .groupBy(
        "pickup_borough",
        "pickup_zone",
        "dropoff_borough",
        "dropoff_zone",
    )
    .agg(
        F.sum("trip_count").alias("trip_count"),
        F.round(
            F.avg("trip_distance"),
            2,
        ).alias("avg_trip_distance"),
        F.round(
            F.avg("trip_duration_minutes"),
            2,
        ).alias("avg_trip_duration_minutes"),
    )
    .orderBy(
        F.desc("trip_count")
    )
)

print("Most common pickup-dropoff routes:")
display(route_patterns.limit(30))


# COMMAND ----------
# 6B. PICKUP AND DROPOFF COUNTS BY ZONE

pickups = (
    fact
    .groupBy("pickup_zone_key")
    .agg(
        F.sum("trip_count").alias("pickups")
    )
)

dropoffs = (
    fact
    .groupBy("dropoff_zone_key")
    .agg(
        F.sum("trip_count").alias("dropoffs")
    )
)

zone_balance = (
    dim_zone
    .join(
        pickups,
        dim_zone["zone_key"]
        == pickups["pickup_zone_key"],
        how="left",
    )
    .join(
        dropoffs,
        dim_zone["zone_key"]
        == dropoffs["dropoff_zone_key"],
        how="left",
    )
    .fillna(
        {
            "pickups": 0,
            "dropoffs": 0,
        }
    )
    .withColumn(
        "net_flow",
        F.col("dropoffs")
        - F.col("pickups"),
    )
    .withColumn(
        "absolute_flow_imbalance",
        F.abs(
            F.col("net_flow")
        ),
    )
    .select(
        "borough",
        "zone",
        "pickups",
        "dropoffs",
        "net_flow",
        "absolute_flow_imbalance",
    )
    .orderBy(
        F.desc("absolute_flow_imbalance")
    )
)

print("Zones with the largest pickup-dropoff imbalance:")
display(zone_balance.limit(30))


# COMMAND ----------
# 6C. HIGHEST PICKUP-DEMAND ZONES

pickup_opportunities = (
    zone_balance
    .orderBy(
        F.desc("pickups")
    )
)

print("Zones with the highest pickup demand:")
display(pickup_opportunities.limit(20))


# COMMAND ----------
# 6D. STRONGEST NET INBOUND ZONES

net_inbound = (
    zone_balance
    .filter(
        F.col("net_flow") > 0
    )
    .orderBy(
        F.desc("net_flow")
    )
)

print("Zones with strongest net inbound flow:")
display(net_inbound.limit(20))


# COMMAND ----------
# 6E. STRONGEST NET OUTBOUND ZONES

net_outbound = (
    zone_balance
    .filter(
        F.col("net_flow") < 0
    )
    .orderBy(
        F.asc("net_flow")
    )
)

print("Zones with strongest net outbound flow:")
display(net_outbound.limit(20))


# COMMAND ----------
# 7. MONTHLY TREND

monthly_demand = (
    fact
    .join(
        dim_date,
        fact["pickup_date_key"]
        == dim_date["date_key"],
        how="left",
    )
    .groupBy(
        "year",
        "month",
        "month_name",
    )
    .agg(
        F.sum("trip_count").alias("trip_count"),
        F.round(
            F.avg("trip_distance"),
            2,
        ).alias("avg_trip_distance"),
        F.round(
            F.avg("trip_duration_minutes"),
            2,
        ).alias("avg_trip_duration_minutes"),
    )
    .orderBy(
        "year",
        "month",
    )
)

print("Monthly taxi activity:")
display(monthly_demand)


# COMMAND ----------
# 8. ANALYTICS VALIDATION

print("=== ANALYTICS VALIDATION ===")

assert fact.count() > 0
assert demand_by_zone.count() > 0
assert demand_by_hour.count() == 24
assert weather_analysis.count() > 0
assert route_patterns.count() > 0
assert zone_balance.count() == dim_zone.count()

print("Business-question analytics completed successfully.")