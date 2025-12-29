# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, BooleanType
import pyspark.sql.functions as F

# COMMAND ----------

batch_id = dbutils.widgets.get("batch_id")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Create empty SCD Gold table

# COMMAND ----------

# Only create the Gold table if it doesn't exist yet
if batch_id == "1" or not spark.catalog.tableExists('trips_history_scd'):
    schema = StructType([
        StructField('trip_id', StringType(), True),
        StructField('tpep_pickup_datetime', TimestampType(), True),
        StructField('tpep_dropoff_datetime', TimestampType(), True),
        StructField('fare_amount', DoubleType(), True),
        StructField('trip_distance', DoubleType(), True),
        StructField('pickup_zip', StringType(), True),
        StructField('dropoff_zip', StringType(), True),
        StructField('valid_from', TimestampType(), True),
        StructField('valid_to', TimestampType(), True),
        StructField('is_current', BooleanType(), True)
    ])
    empty_table = spark.createDataFrame([], schema=schema)
    (empty_table
        .write
        .mode("overwrite")
        .format("delta")
        .saveAsTable("trips_history_scd")
    )
spark.table("trips_history_scd").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Apply SCD type 2 logic

# COMMAND ----------

spark.sql("""
 SELECT * FROM 
    (
        SELECT *, 
        ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY event_ts DESC) AS rn 
        FROM silver_trips_cdc
    ) 
    WHERE rn = 1
""").createOrReplaceTempView("silver_updated")

# COMMAND ----------

merge_sql = f"""
MERGE INTO trips_history_scd t
USING silver_updated s
ON t.trip_id = s.trip_id AND t.is_current = true

-- Step 1: Expire the old record if the data has changed
WHEN MATCHED AND (
    t.fare_amount <> s.fare_amount 
    OR t.trip_distance <> s.trip_distance 
    OR t.pickup_zip <> s.pickup_zip 
    OR t.dropoff_zip <> s.dropoff_zip
) 
THEN UPDATE SET
    t.valid_to = s.event_ts,
    t.is_current = false

-- Step 2: Insert new rows for completely new trip_ids
WHEN NOT MATCHED THEN INSERT (
    trip_id,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    fare_amount,
    trip_distance,
    pickup_zip,
    dropoff_zip,
    valid_from,
    valid_to,
    is_current
)
VALUES (
    s.trip_id,
    s.tpep_pickup_datetime,
    s.tpep_dropoff_datetime,
    s.fare_amount,
    s.trip_distance,
    s.pickup_zip,
    s.dropoff_zip,
    s.event_ts,
    NULL,
    true
)
"""

# Execute the merge SQL statement with Spark
spark.sql(merge_sql)

# COMMAND ----------

spark.sql("""
--- Find the rows for Step3: insert updated trip info
SELECT
  s.trip_id,
  s.tpep_pickup_datetime,
  s.tpep_dropoff_datetime,
  s.fare_amount,
  s.trip_distance,
  s.pickup_zip,
  s.dropoff_zip,
  s.event_ts,
  NULL,
  true
FROM silver_updated s
INNER JOIN trips_history_scd t
  ON s.trip_id = t.trip_id 
    AND t.is_current = false 
    AND (float(s.fare_amount) <> float(t.fare_amount) 
        OR float(s.trip_distance) <> float(t.trip_distance) 
        OR string(s.pickup_zip <> string(t.pickup_zip) 
        OR string(s.dropoff_zip) <> string(t.dropoff_zip)))
    AND t.valid_to = DATEADD(SECOND,s.event_ts,'1970-1-1')
""").createOrReplaceTempView('scd_step3')

# COMMAND ----------

merge_sql_1 = """
--- Step3: Insert the updated trip info 
INSERT INTO trips_history_scd
(
    trip_id,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    fare_amount,
    trip_distance,
    pickup_zip,
    dropoff_zip,
    valid_from,
    valid_to,
    is_current
) 
SELECT * FROM scd_step3
"""
spark.sql(merge_sql_1)

# COMMAND ----------

# spark.sql("REPLACE table trips_history_scd as select * from trips_history_scd version as of 2")

# COMMAND ----------

# %sql
# REFRESH TABLE trips_history_scd

# COMMAND ----------

assert spark.table("trips_history_scd").where("is_current = True").groupBy("trip_id").agg(F.count("*").alias("cnc")).filter("cnc > 1").count() == 0, "More than 1 current version of same trip"

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Optimize

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE trips_history_scd ZORDER BY (trip_id, valid_from);
# MAGIC SELECT * FROM trips_history_scd