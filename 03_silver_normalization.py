# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC #### Silver: normalize CDC into flat records

# COMMAND ----------

bronze = spark.table("bronze_trips_cdc")

# COMMAND ----------

silver = (bronze
    .where(F.col("op").isin("u", "c"))
    .select(
        F.col("after.trip_id").alias("trip_id"),
        F.col("after.tpep_pickup_datetime").alias("tpep_pickup_datetime"),
        F.col("after.tpep_dropoff_datetime").alias("tpep_dropoff_datetime"),
        F.col("after.trip_distance").alias("trip_distance"),
        F.col("after.fare_amount").alias("fare_amount"),
        F.col("after.pickup_zip").alias("pickup_zip"),
        F.col("after.dropoff_zip").alias("dropoff_zip"),
        (F.col("ts_ms")/1000).alias("event_ts")
    )
)

# COMMAND ----------

(silver
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver_trips_cdc")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- REFRESH TABLE silver_trips_cdc;
# MAGIC SELECT * FROM silver_trips_cdc;