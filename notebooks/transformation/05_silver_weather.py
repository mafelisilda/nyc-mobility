# Databricks notebook source

# COMMAND ----------
# 1. IMPORTS

from src.transformations.weather import transform_weather_silver

# COMMAND ----------
# 2. TABLE CONFIGURATION

BRONZE_TABLE = "nyc_mobility.bronze.weather"
SILVER_TABLE = "nyc_mobility.silver.weather"


# COMMAND ----------
# 3. LOAD BRONZE WEATHER DATA

bronze_weather = spark.table(
    BRONZE_TABLE
)

print(
    f"Bronze weather rows: "
    f"{bronze_weather.count():,}"
)


# COMMAND ----------
# 4. TRANSFORM TO SILVER

silver_weather = transform_weather_silver(
    bronze_weather
)

print(
    f"Silver weather rows after transformation: "
    f"{silver_weather.count():,}"
)


# COMMAND ----------
# 5. WRITE SILVER TABLE

(
    silver_weather.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

print(
    f"Written to: {SILVER_TABLE}"
)


# COMMAND ----------
# 6. VERIFY RESULT

result = spark.table(
    SILVER_TABLE
)

print(
    f"Final Silver weather rows: "
    f"{result.count():,}"
)

display(
    result
    .select(
        "weather_datetime",
        "temperature_c",
        "precipitation_mm",
        "weather_code",
    )
    .orderBy("weather_datetime")
    .limit(10)
)

print(
    "Silver weather build completed successfully."
)