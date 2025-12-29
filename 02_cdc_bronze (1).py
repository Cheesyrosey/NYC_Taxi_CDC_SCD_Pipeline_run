# Databricks notebook source
batch_id = dbutils.widgets.get("batch_id")

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType
from pyspark.sql import Row

# COMMAND ----------

before_after_schema = StructType([
        StructField("trip_id", StringType(), True), 
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("pickup_zip", StringType(), True),
        StructField("dropoff_zip", StringType(), True),
])

bronze_schema = StructType([
        StructField("op", StringType(), True),
        StructField("before", before_after_schema, True),
        StructField("after", before_after_schema, True),
        StructField("ts_ms", LongType(), True), ])

# COMMAND ----------

def to_bronze_dict(gold_row):
    d = gold_row.asDict()
    return {
        "trip_id": d["trip_id"],
        "tpep_pickup_datetime": d["tpep_pickup_datetime"],
        "tpep_dropoff_datetime": d["tpep_dropoff_datetime"],
        "fare_amount": float(d["fare_amount"]),
        "trip_distance": float(d["trip_distance"]),
        "pickup_zip": str(d["pickup_zip"]),
        "dropoff_zip": str(d["dropoff_zip"])
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Bronze: Simulate CDC events

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Batch 1 CDC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Pick some samples trips from fact_trips to simulate CDC
# MAGIC

# COMMAND ----------

if batch_id == "1":
        # Simulate cdc events using fact table
        spark.sql("DROP TABLE IF EXISTS bronze_trips_cdc")
        sample_trips = (spark.table("fact_trips")
                        .orderBy('trip_id')
                        .limit(2)
                        .collect() 
                        )
        if len(sample_trips) < 2:
            raise ValueError("Not enpough trips in fact_trips to simulate CDC. Insert more data first.")
        tripA, tripB = sample_trips[0], sample_trips[1]

        # For Trip A: add fare_amount by 2.5
        beforeA_dict = {
                    "trip_id" : tripA["trip_id"],
                    "tpep_pickup_datetime" : tripA["tpep_pickup_datetime"],
                    "tpep_dropoff_datetime" : tripA["tpep_dropoff_datetime"],
                    "fare_amount" : float(tripA["fare_amount"]),
                    "trip_distance" : float(tripA["trip_distance"]),
                    "pickup_zip" : str(tripA["pickup_zip"]),
                    "dropoff_zip" : str(tripA["dropoff_zip"])
        }

        afterA_dict = beforeA_dict.copy()
        afterA_dict["fare_amount"] = afterA_dict["fare_amount"] + 2.5

        beforeA = Row(**beforeA_dict)
        afterA = Row(**afterA_dict)

        # For Trip B: add distance by 0.8
        beforeB_dict = {
                    "trip_id" : tripB["trip_id"],
                    "tpep_pickup_datetime" : tripB["tpep_pickup_datetime"],
                    "tpep_dropoff_datetime" : tripB["tpep_dropoff_datetime"],
                    "fare_amount" : float(tripB["fare_amount"]),
                    "trip_distance" : float(tripB["trip_distance"]),
                    "pickup_zip" : str(tripB["pickup_zip"]),
                    "dropoff_zip" : str(tripB["dropoff_zip"])
        }

        afterB_dict = beforeB_dict.copy()
        afterB_dict["trip_distance"] = afterB_dict["trip_distance"] + 0.8

        beforeB = Row(**beforeB_dict)
        afterB = Row(**afterB_dict)

        batch1 = [
            Row(op = "u", before = beforeA, after = afterA, ts_ms = 1710000000000),
            Row(op = "u", before = beforeB, after = afterB, ts_ms = 1710005000000)
        ]
        spark.createDataFrame(batch1, schema = bronze_schema).write.mode("overwrite").format("delta").saveAsTable("bronze_trips_cdc")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Batch 2 CDC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Define the schema, simulate cdc and write

# COMMAND ----------

if batch_id == "2":
        # Simulate cdc events using already created gold table
        gold_current = spark.table("trips_history_scd").orderBy('trip_id').filter("is_current = true").collect()
        tripA2, tripB2 = gold_current[0], gold_current[1]

        # For Trip A2: add trip distance by 0.5
        beforeA2 = Row(**to_bronze_dict(tripA2))
        afterA2_dict = to_bronze_dict(tripA2)
        afterA2_dict["trip_distance"] += 0.5
        afterA2 = Row(**afterA2_dict)

        # For Trip B2: add fare amount by 1.0
        beforeB2 = Row(**to_bronze_dict(tripB2))
        afterB2_dict = to_bronze_dict(tripB2)
        afterB2_dict["fare_amount"] += 1.0
        afterB2 = Row(**afterB2_dict)

        batch2 = [
            Row(op="u", before=beforeA2, after=afterA2, ts_ms=1710020000000),
            Row(op="u", before=beforeB2, after=afterB2, ts_ms=1710025000000)
        ]
        spark.createDataFrame(batch2, schema = bronze_schema).write.mode("append").format("delta").saveAsTable("bronze_trips_cdc")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Batch 3 CDC

# COMMAND ----------

if batch_id == "3":
        gold_current = spark.table("trips_history_scd").filter("is_current = true").collect()
        tripA3, tripB3 = gold_current[0], gold_current[1]

        # For Trip A3: add pickup_zip by 1
        beforeA3 = Row(**to_bronze_dict(tripA3))
        afterA3_dict = to_bronze_dict(tripA3)
        afterA3_dict["pickup_zip"] = str(int(afterA3_dict["pickup_zip"]) + 1)
        afterA3 = Row(**afterA3_dict)

        # For Trip B3: add dropoff_zip by 1
        beforeB3 = Row(**to_bronze_dict(tripB3))
        afterB3_dict = to_bronze_dict(tripB3)
        afterB3_dict["dropoff_zip"] = str(int(afterB3_dict["dropoff_zip"]) + 1)
        afterB3 = Row(**afterB3_dict)

        batch3 = [
            Row(op="u", before=beforeA3, after=afterA3, ts_ms=1713000000000),
            Row(op="u", before=beforeB3, after=afterB3, ts_ms=1713500000000)
        ]
        spark.createDataFrame(batch3, schema = bronze_schema).write.mode("append").format("delta").saveAsTable("bronze_trips_cdc")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- REFRESH TABLE bronze_trips_cdc;
# MAGIC SELECT * FROM bronze_trips_cdc