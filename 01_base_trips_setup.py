# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS fact_trips;
# MAGIC DROP TABLE IF EXISTS bronze_trips_cdc;
# MAGIC DROP TABLE IF EXISTS silver_trips_cdc;
# MAGIC DROP TABLE IF EXISTS trips_history_scd;

# COMMAND ----------

source_df = spark.table('samples.nyctaxi.trips')

# COMMAND ----------

trips_with_id_df = (source_df
                    .withColumn(
                        'trip_id',
                        F.sha2(F.concat_ws("|", 
                                           F.col('tpep_pickup_datetime'), 
                                           F.col('tpep_dropoff_datetime'), 
                                           F.col('trip_distance'), 
                                           F.col('fare_amount'), 
                                           F.col('pickup_zip'), 
                                           F.col('dropoff_zip')
                                    ), 256)
                    ))

# COMMAND ----------

(trips_with_id_df.select('trip_id', *source_df.columns)
                   .write
                   .format('delta')
                   .mode('overwrite')
                   .saveAsTable('fact_trips'))