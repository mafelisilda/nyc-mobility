# Databricks notebook source

# COMMAND ----------
# 1. IMPORTS

from src.transformations.zones import transform_zones_silver

# COMMAND ----------
# 2. TABLE CONFIGURATION

BRONZE_TABLE = "nyc_mobility.bronze.taxi_zones"
SILVER_TABLE = "nyc_mobility.silver.taxi_zones"


# COMMAND ----------
# 3. LOAD BRONZE TAXI ZONES

bronze_zones = spark.table(
    BRONZE_TABLE
)

print(
    f"Bronze zone rows: "
    f"{bronze_zones.count():,}"
)


# COMMAND ----------
# 4. TRANSFORM TO SILVER

silver_zones = transform_zones_silver(
    bronze_zones
)

print(
    f"Silver zone rows after transformation: "
    f"{silver_zones.count():,}"
)


# COMMAND ----------
# 5. WRITE SILVER TABLE

(
    silver_zones.write
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
    f"Final Silver zone rows: "
    f"{result.count():,}"
)

display(
    result
    .select(
        "location_id",
        "borough",
        "zone",
        "service_zone",
    )
    .orderBy("location_id")
    .limit(20)
)

print(
    "Silver taxi zone build completed successfully."
)