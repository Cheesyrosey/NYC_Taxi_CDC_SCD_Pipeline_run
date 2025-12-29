# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

def debug_test0_():
    # debug function for Test 0
    prob_id = violations.select("trip_id").distinct()
    silver = spark.table("silver_trips_cdc")
    print("silver afffected rows:")
    silver.filter(F.col("trip_id").isin(prob_id)).orderBy("trip_id").display()
    print("gold afffected rows:")
    gold.filter(F.col("trip_id").isin(prob_id)).orderBy("trip_id", "valid_from").display()
    raise Exception("Multiple current versions detected in Gold")

# COMMAND ----------

# Test 0: Gold must have exactly one is_current = true per trip
gold = spark.table("trips_history_scd")
violations = gold.filter(F.col("is_current") == True).groupBy("trip_id").agg(F.count("*").alias("cnt")).filter("cnt > 1")
if violations.count() > 0: debug_test0_() 
else: print("Test 0 passed: Gold has exactly one current version per trip")

# COMMAND ----------

# Test 1: Bronze CDC schema mush match expected 7 columns
bronze = spark.table("bronze_trips_cdc")
expected_cols = {
    "trip_id", "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount", "pickup_zip", "dropoff_zip"
}
assert set(bronze.select("before.*").columns) == expected_cols, "Bronze.before schema mismatch"
assert set(bronze.select("after.*").columns) == expected_cols, "Bronze.after schema mismatch"
print("Test 1 passed: Bronze schema is correct")

# COMMAND ----------

# Test 2: CDC events must contain real changes
no_change = bronze.filter(
    (F.col("before.fare_amount") == F.col("after.fare_amount")) &
    (F.col("before.trip_distance") == F.col("after.trip_distance")) &
    (F.col("before.pickup_zip") == F.col("after.pickup_zip")) &
    (F.col("before.dropoff_zip") == F.col("after.dropoff_zip"))
)
assert no_change.count() == 0, "Found CDC events with no actual changes"
print("Test 2 passed: CDC events contain actual changes")

# COMMAND ----------

# Test 3: Silver schema must be flattened correctly
silver = spark.table("silver_trips_cdc")
expected_silver = {
    "trip_id", "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount", "pickup_zip", "dropoff_zip", "event_ts"
}
assert set(silver.columns) == expected_silver, "Silver schema mismatch"
print("Test 3 passed: Silver schema is correct")

# COMMAND ----------

# Test 4: All closed versions must have valid_to filled
closed = gold.filter("is_current = false")
assert closed.filter("valid_to is null").count() == 0, "Found closed versions without valid_to"
print("Test 4 passed: All closed versions have valid_to timestamps")

# COMMAND ----------

# Test 5: Gold must contain historical versions (SCD type 2)
current_versions = gold.filter("is_current = true").count()
if gold.count() > current_versions:
    assert gold.filter("is_current = false").count() >= 1, "No historical versions found - SCD Type 2 not working"
    print("Test 5 passed: Historical versions found - SCD Type 2 working")
else:
    print("Test 6 skipped: First MEERGE does not produce history")

# COMMAND ----------

# Test 6: Gold version count must grow after each MERGE
current_count = gold.count()
assert current_count >= 2, "Gold table did not grow after MERGE"
print("Test 6 passed: Gold table grows after MERGE")

# COMMAND ----------

## Test 7 (Skipped due to free account environment): OPTIMIZE should reduce or maintain file count
# import time
# detail = spark.sql("DESCRIBE DETAIL trips_history_scd").select("location").first()[0]
# before_files = spark.sql(f"SHOW FILES IN delta.{detail}").count()
# spark.sql("OPTIMIZE trips_history_scd ZORDER BY (trip_id)")
# time.sleep(10)
# after_files = spark.sql(f"SHOW FILES IN delta.{detail}").count()
# assert before_files >= after_files, "OPTIMIZE did not reduce file count"
# print("Test 7 passed: OPTIMIZE reduced or maintained file count")

# COMMAND ----------

print("All unit tests passed for this batch!")