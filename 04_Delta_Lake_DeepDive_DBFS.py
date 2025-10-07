# Databricks notebook source
# MAGIC %md
# MAGIC # 04. Delta Lake Deep Dive (DBFS)
# MAGIC
# MAGIC **Purpose:** Explore and demonstrate key features of Delta Lake using the tables created in the previous steps, which are backed by data stored exclusively on DBFS.
# MAGIC
# MAGIC **Features Covered:**
# MAGIC * ACID Transactions (Implicit)
# MAGIC * Schema Enforcement & Evolution
# MAGIC * Time Travel (Querying previous table versions)
# MAGIC * `MERGE` statement (Upserts: Updates, Inserts, Deletes)
# MAGIC * `OPTIMIZE` (Compaction) & `ZORDER` (Data Skipping)
# MAGIC * `VACUUM` (Garbage Collection)
# MAGIC * `CLONE` (Shallow and Deep copies)
# MAGIC * `RESTORE` (Reverting table state)
# MAGIC
# MAGIC **DBFS Focus:**
# MAGIC * All Delta operations are performed on tables whose underlying data files reside in the project's DBFS structure (`dbfs:/Users/...`).
# MAGIC * Commands like `OPTIMIZE`, `VACUUM`, `MERGE`, `CLONE`, `RESTORE` work seamlessly with Delta tables regardless of whether they are managed or external (pointing to DBFS `LOCATION`).
# MAGIC * We'll primarily use the Silver table (`silver_taxi_trips`) for demonstrations.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC * Notebooks `00` through `03` have been run successfully.
# MAGIC * The `silver_taxi_trips` table exists in the specified database.

# COMMAND ----------

# MAGIC %run ../DE_Cert_Prep_Project_DBFS/_Helper_Config_DBFS

# COMMAND ----------

# DBTITLE 1,Load Configuration from Spark Conf
import re
from pyspark.sql.functions import col, lit, expr, current_timestamp, coalesce, year, month # Import necessary functions

# Retrieve necessary configuration
try:
    db_name = get_config_value("db_name")
    silver_table_name = "silver_taxi_trips"
    silver_path_dbfs = get_config_value("silver_path_dbfs") # Get Silver path for direct path operations if needed
    project_base_dbfs = get_config_value("project_base_dbfs") # For clone destination
except ValueError as e:
    print(f"Configuration Error: {e}")
    dbutils.notebook.exit("Failed to load configuration.")

# Set the current database context
spark.sql(f"USE {db_name}")

print(f"--- Configuration Loaded ---")
print(f"Database Name:        {db_name}")
print(f"Using Silver Table:   {db_name}.{silver_table_name}")
print(f"Silver Path (DBFS):   {silver_path_dbfs}")
print(f"Project Base (DBFS):  {project_base_dbfs}")

# Define names for cloned tables
silver_clone_shallow_name = f"{silver_table_name}_clone_shallow"
silver_clone_deep_name = f"{silver_table_name}_clone_deep"
silver_clone_deep_path = f"{project_base_dbfs}/delta/silver_clone_deep" # Path for deep clone data

# COMMAND ----------

# DBTITLE 1,ACID Transactions (Implicit Demonstration)
# MAGIC %md
# MAGIC ### ACID Transactions
# MAGIC
# MAGIC Delta Lake ensures Atomicity, Consistency, Isolation, and Durability (ACID) for all operations. Every write (`INSERT`, `UPDATE`, `DELETE`, `MERGE`, `OVERWRITE`) is a transaction logged in the `_delta_log`. If an operation fails partway, Delta Lake guarantees the table remains in its previous consistent state.
# MAGIC
# MAGIC We've already implicitly used ACID transactions when creating the Bronze, Silver, and Gold tables in the previous notebooks. The `write.format("delta").save(...)` or `saveAsTable(...)` operations were atomic.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Schema Enforcement & Evolution
# MAGIC %md
# MAGIC ### Schema Enforcement & Evolution
# MAGIC
# MAGIC * **Schema Enforcement (Default):** Delta Lake prevents writing data that doesn't match the table's current schema (column names, data types). This ensures data integrity.
# MAGIC * **Schema Evolution:** You can allow the schema to evolve using the `mergeSchema` option during writes (`.option("mergeSchema", "true")`). This allows adding new columns or changing nullable properties safely.
# MAGIC

# COMMAND ----------

# MAGIC %md #### Attempt 1: Write with Incorrect Schema (Enforcement)
# MAGIC

# COMMAND ----------

# Create a sample DataFrame with a missing column compared to the silver table
print(f"Attempting to append data missing a column ('vendor_id') to '{silver_table_name}'...")
try:
    sample_df_missing_col = spark.table(f"{db_name}.{silver_table_name}").limit(5).drop("vendor_id")
    # Attempt to append this incompatible DataFrame
    sample_df_missing_col.write \
        .format("delta") \
        .mode("append") \
        .save(silver_path_dbfs) # Appending directly to the path also enforces schema
    print("Error: Append with incorrect schema unexpectedly succeeded.")
except Exception as e:
    print(f"SUCCESS: Append failed as expected due to schema mismatch.")
    print(f"Error message snippet: {str(e)[:300]}...") # Show part of the error

# COMMAND ----------

# MAGIC %md #### Attempt 2: Write with New Column (Evolution)

# COMMAND ----------

# Create a sample DataFrame with a new column added
print(f"\nAttempting to append data with a new column ('data_source_quality') using schema evolution...")
try:
    silver_df_current = spark.table(f"{db_name}.{silver_table_name}")
    new_col_df = silver_df_current.limit(10).withColumn("data_source_quality", lit("Good"))

    # Append using the mergeSchema option
    new_col_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(silver_path_dbfs) # Append to the path

    print("SUCCESS: Append with new column and mergeSchema=true completed.")

    # Verify the new schema by reading the table again
    print("\nVerifying new schema:")
    final_silver_df = spark.table(f"{db_name}.{silver_table_name}")
    final_silver_df.printSchema()
    # Check if the new column exists
    assert "data_source_quality" in final_silver_df.columns
    print("Column 'data_source_quality' successfully added to the schema.")

    # Show data - older rows will have null for the new column
    print("\nSample data showing new column (may be null for older rows):")
    display(final_silver_df.select("pickup_location_id", "tip_amount", "data_source_quality").limit(20))

except Exception as e:
    print(f"\nERROR during append with schema evolution: {e}")
    dbutils.notebook.exit("Failed schema evolution step.")

# COMMAND ----------

# DBTITLE 1,Time Travel (Querying Older Versions)
# MAGIC %md
# MAGIC ### Time Travel
# MAGIC
# MAGIC Delta Lake automatically versions every operation on the table. You can query the table as it existed at a specific point in time using either a version number or a timestamp.
# MAGIC

# COMMAND ----------

# 1. Show Table History
# The history shows each transaction (version), timestamp, operation type, etc.
print(f"Displaying history for table: {db_name}.{silver_table_name}")
history_df = spark.sql(f"DESCRIBE HISTORY {db_name}.{silver_table_name}")
display(history_df)

# COMMAND ----------

# 2. Get Version Numbers
# Find the latest version and the version before our schema evolution append
try:
    latest_version = history_df.selectExpr("max(version)").first()[0]
    # Find the version number just before the APPEND operation (schema evolution)
    previous_version_df = history_df.filter("operation = 'WRITE' AND version < {}".format(latest_version)).selectExpr("max(version) as version")
    previous_version = previous_version_df.first()["version"] if previous_version_df.count() > 0 else latest_version -1

    if previous_version is None or previous_version < 0:
         previous_version = 0 # Handle case with very few versions

    print(f"\nLatest version: {latest_version}")
    print(f"Version before last append (schema evolution): {previous_version}")
except Exception as e:
    print(f"Error getting version numbers from history: {e}")
    latest_version = 0
    previous_version = 0

# COMMAND ----------

# 3. Query by Version Number
# Query the table as it was *before* the schema evolution (should not have the new column)
if latest_version > previous_version:
    print(f"\nQuerying table state at version {previous_version}...")
    try:
        silver_df_previous_version = spark.read.format("delta") \
            .option("versionAsOf", previous_version) \
            .load(silver_path_dbfs) # Use the DBFS path

        print(f"Schema of version {previous_version}:")
        silver_df_previous_version.printSchema()
        assert "data_source_quality" not in silver_df_previous_version.columns
        print(f"SUCCESS: Column 'data_source_quality' is not present in version {previous_version}, as expected.")
        print(f"Record count in version {previous_version}: {silver_df_previous_version.count()}")

    except Exception as e:
        print(f"ERROR querying version {previous_version}: {e}")
else:
    print("\nNot enough history to query a distinct previous version.")

# COMMAND ----------

# 4. Query by Timestamp
# Find the timestamp associated with the previous version from the history
if latest_version > previous_version:
    try:
        previous_version_timestamp_row = history_df.filter(col("version") == previous_version).select("timestamp").first()
        if previous_version_timestamp_row:
            previous_version_timestamp = previous_version_timestamp_row[0]
            # Format timestamp string precisely as required by timestampAsOf
            ts_string = previous_version_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")
            print(f"\nTimestamp of version {previous_version}: {ts_string}")

            print(f"Querying table state at timestamp '{ts_string}'...")
            silver_df_timestamp_version = spark.read.format("delta") \
                .option("timestampAsOf", ts_string) \
                .load(silver_path_dbfs) # Use the DBFS path

            print(f"Schema queried using timestamp '{ts_string}':")
            silver_df_timestamp_version.printSchema()
            assert "data_source_quality" not in silver_df_timestamp_version.columns
            print(f"SUCCESS: Column 'data_source_quality' is not present at timestamp '{ts_string}'.")
            print(f"Record count using timestamp '{ts_string}': {silver_df_timestamp_version.count()}")
        else:
            print(f"Could not find timestamp for version {previous_version} in history.")

    except Exception as e:
        print(f"ERROR querying using timestamp: {e}")

# COMMAND ----------

# DBTITLE 1,MERGE Statement (Upserts)
# MAGIC %md
# MAGIC ### MERGE Statement
# MAGIC
# MAGIC The `MERGE` command performs "upserts" - efficiently inserting new rows, updating existing rows, and optionally deleting rows based on a match condition between a source dataset and the target Delta table, all within a single atomic transaction. This is fundamental for incremental data processing and Change Data Capture (CDC) scenarios.
# MAGIC
# MAGIC **Syntax:**
# MAGIC ```sql
# MAGIC MERGE INTO target_table_alias
# MAGIC USING source_table_alias
# MAGIC ON merge_condition
# MAGIC WHEN MATCHED [AND condition] THEN UPDATE SET col1 = val1 [, ...]
# MAGIC WHEN MATCHED [AND condition] THEN DELETE
# MAGIC WHEN NOT MATCHED [AND condition] THEN INSERT (col1 [, ...]) VALUES (val1 [, ...])
# MAGIC ```

# COMMAND ----------

# 1. Create Sample Source Data for MERGE
# Simulate receiving new trip data: some are updates to existing trips, some are brand new.
# IMPORTANT: A reliable unique key is crucial for MERGE. The taxi dataset lacks a perfect one.
# We'll *simulate* a key using a combination of pickup time, dropoff time, and location IDs.
# THIS IS NOT ROBUST FOR REAL-WORLD TAXI DATA but demonstrates the MERGE concept.

print("Creating sample source data for MERGE operation...")

# Get a few existing records to simulate updates
try:
    # Simulate an increased tip and adjust total accordingly
    updates_source_df = spark.table(f"{db_name}.{silver_table_name}") \
        .filter("pickup_location_id = 48 AND passenger_count = 1 AND pickup_year = 2023 AND pickup_month = 1") \
        .limit(2) \
        .withColumn("tip_amount", col("tip_amount") + 5.0) \
        .withColumn("total_amount", col("total_amount") + 5.0) \
        .withColumn("merge_operation", lit("UPDATE")) # Add flag for clarity

    # Create a couple of 'new' records mimicking the schema
    new_record_df_1 = spark.table(f"{db_name}.{silver_table_name}") \
        .limit(1) \
        .withColumn("tpep_pickup_datetime", expr("current_timestamp() - interval 2 hours")) \
        .withColumn("tpep_dropoff_datetime", expr("current_timestamp() - interval 1 hour 30 minutes")) \
        .withColumn("passenger_count", lit(3)) \
        .withColumn("pickup_location_id", lit(101)) \
        .withColumn("dropoff_location_id", lit(202)) \
        .withColumn("tip_amount", lit(8.88)) \
        .withColumn("fare_amount", lit(60.00)) \
        .withColumn("total_amount", lit(75.0)) \
        .withColumn("trip_distance", lit(18.2)) \
        .withColumn("trip_duration_minutes", lit(30.0)) \
        .withColumn("pickup_year", year(expr("current_timestamp() - interval 2 hours"))) \
        .withColumn("pickup_month", month(expr("current_timestamp() - interval 2 hours"))) \
        .withColumn("data_source_quality", lit("New")) \
        .withColumn("merge_operation", lit("INSERT"))

    new_record_df_2 = new_record_df_1 \
        .withColumn("tpep_pickup_datetime", expr("current_timestamp() - interval 3 hours")) \
        .withColumn("tpep_dropoff_datetime", expr("current_timestamp() - interval 2 hour 15 minutes")) \
        .withColumn("pickup_location_id", lit(105)) \
        .withColumn("tip_amount", lit(4.44))

    # Combine updates and new records into a single source DataFrame
    # Use unionByName to handle potential schema differences (like the merge_operation flag)
    merge_source_df = updates_source_df.unionByName(new_record_df_1, allowMissingColumns=True) \
                                     .unionByName(new_record_df_2, allowMissingColumns=True)

    print(f"Created source DataFrame with {merge_source_df.count()} records for MERGE.")
    print("Sample source data:")
    display(merge_source_df.select("merge_operation", "tpep_pickup_datetime", "pickup_location_id", "dropoff_location_id", "tip_amount", "total_amount", "data_source_quality"))

    # Register the source DataFrame as a temporary view to use in SQL MERGE
    merge_source_df.createOrReplaceTempView("silver_taxi_updates_source")

except Exception as e:
    print(f"Error creating sample data for MERGE: {e}")
    dbutils.notebook.exit("Failed MERGE setup.")

# COMMAND ----------

# 2. Perform the MERGE operation using SQL
print("Executing MERGE statement...")

# Define the MERGE SQL statement
# The ON condition uses our simulated key. Remember this is fragile for this dataset.
# WHEN MATCHED: Updates the tip and total amount for records found in both source and target.
# WHEN NOT MATCHED: Inserts the new records from the source into the target.
merge_sql = f"""
MERGE INTO {db_name}.{silver_table_name} AS target
USING silver_taxi_updates_source AS source
ON  target.tpep_pickup_datetime = source.tpep_pickup_datetime
AND target.tpep_dropoff_datetime = source.tpep_dropoff_datetime -- Adding dropoff time makes key slightly more unique
AND target.pickup_location_id = source.pickup_location_id
AND target.dropoff_location_id = source.dropoff_location_id
AND target.passenger_count = source.passenger_count -- Using multiple fields to approximate a unique key

WHEN MATCHED AND source.merge_operation = 'UPDATE' THEN
  UPDATE SET
    target.tip_amount = source.tip_amount,
    target.total_amount = source.total_amount,
    target.data_source_quality = 'Updated Tip' -- Update the quality flag too

-- Example of a MATCHED DELETE condition (optional)
-- WHEN MATCHED AND source.merge_operation = 'DELETE' THEN
--   DELETE

WHEN NOT MATCHED AND source.merge_operation = 'INSERT' THEN
  INSERT (
    vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
    trip_distance, rate_code_id, store_and_fwd_flag, pickup_location_id, dropoff_location_id,
    payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
    improvement_surcharge, total_amount, congestion_surcharge, airport_fee,
    pickup_year, pickup_month, trip_duration_seconds, trip_duration_minutes, average_speed_mph,
    data_source_quality
    -- Ensure all columns in the target table are listed or handled by schema evolution if applicable
  )
  VALUES (
    source.vendor_id, source.tpep_pickup_datetime, source.tpep_dropoff_datetime, source.passenger_count,
    source.trip_distance, source.rate_code_id, source.store_and_fwd_flag, source.pickup_location_id, source.dropoff_location_id,
    source.payment_type, source.fare_amount, source.extra, source.mta_tax, source.tip_amount, source.tolls_amount,
    source.improvement_surcharge, source.total_amount, source.congestion_surcharge, source.airport_fee,
    source.pickup_year, source.pickup_month, source.trip_duration_seconds, source.trip_duration_minutes, source.average_speed_mph,
    source.data_source_quality
  )
"""

try:
    merge_result = spark.sql(merge_sql)
    print("MERGE statement executed successfully.")
    # Display the metrics DataFrame returned by MERGE
    print("MERGE operation metrics:")
    display(merge_result)
    # Look for num_inserted_rows, num_updated_rows etc.
except Exception as e:
    print(f"ERROR executing MERGE statement: {e}")
    dbutils.notebook.exit("MERGE operation failed.")

# 3. Verify MERGE Results
print("\nVerifying MERGE results...")
# Check if the 'new' records were inserted (look for their specific tip amounts)
display(spark.table(f"{db_name}.{silver_table_name}").filter("tip_amount = 8.88 OR tip_amount = 4.44").select("tpep_pickup_datetime", "pickup_location_id", "tip_amount", "total_amount", "data_source_quality"))
# Check if the 'updated' records show the 'Updated Tip' quality flag and increased tip
display(spark.table(f"{db_name}.{silver_table_name}").filter("data_source_quality = 'Updated Tip'").select("tpep_pickup_datetime", "pickup_location_id", "tip_amount", "total_amount", "data_source_quality"))

# COMMAND ----------

# DBTITLE 1,OPTIMIZE and ZORDER (Performance Tuning)
# MAGIC %md
# MAGIC ### OPTIMIZE and ZORDER
# MAGIC
# MAGIC These commands help improve query performance on Delta tables, especially large ones.
# MAGIC
# MAGIC * **`OPTIMIZE`**: Compacts small data files into larger ones. Reading fewer, larger files is much more efficient for Spark than reading many small files. This is particularly useful after many small writes (like streaming or frequent MERGE operations).
# MAGIC * **`ZORDER BY (col1, col2, ...)`**: A technique used *with* `OPTIMIZE`. It reorganizes the data within the compacted files based on the values in the specified `ZORDER` columns. This allows Spark to skip reading irrelevant files when queries filter on the `ZORDER` columns, significantly speeding up those queries. Choose columns frequently used in `WHERE` clauses (high cardinality columns often benefit most). Do **not** ZORDER by partition columns.
# MAGIC

# COMMAND ----------

# 1. Show File Count Before OPTIMIZE
# Describe detail shows metrics including number of files
print(f"Table details before OPTIMIZE for: {db_name}.{silver_table_name}")
display(spark.sql(f"DESCRIBE DETAIL {db_name}.{silver_table_name}"))
# Note the 'numFiles' metric.


# COMMAND ----------

# 2. Run OPTIMIZE (Compaction)
print("\nRunning OPTIMIZE to compact small files...")
try:
    # Optimize the entire table. For very large tables, you can add a WHERE clause
    # to optimize specific partitions, e.g., WHERE pickup_year = 2023 AND pickup_month = 1
    optimize_result = spark.sql(f"OPTIMIZE {db_name}.{silver_table_name}")
    print("OPTIMIZE command completed.")
    print("OPTIMIZE metrics:")
    display(optimize_result) # Shows files added/removed, size etc.
except Exception as e:
    print(f"ERROR running OPTIMIZE: {e}")

# Check file count again (should ideally decrease if compaction occurred)
print(f"\nTable details after OPTIMIZE:")
display(spark.sql(f"DESCRIBE DETAIL {db_name}.{silver_table_name}"))

# COMMAND ----------

# 3. Run OPTIMIZE with ZORDER
# Choose columns frequently used in WHERE clauses for ZORDERing.
# Good candidates: High cardinality IDs, timestamps often filtered on range, categorical codes.
# Example: Z-order by pickup location and the pickup timestamp.
zorder_cols = ["pickup_location_id", "tpep_pickup_datetime"]
print(f"\nRunning OPTIMIZE with ZORDER BY ({', '.join(zorder_cols)})...")
# Note: ZORDER incurs extra cost as it rewrites data, so run strategically.
try:
    zorder_result = spark.sql(f"OPTIMIZE {db_name}.{silver_table_name} ZORDER BY ({', '.join(zorder_cols)})")
    print("OPTIMIZE ZORDER command completed.")
    print("ZORDER metrics:")
    display(zorder_result)
except Exception as e:
    print(f"ERROR running OPTIMIZE ZORDER: {e}")

# Check table details again (file count might change again)
print(f"\nTable details after OPTIMIZE ZORDER:")
display(spark.sql(f"DESCRIBE DETAIL {db_name}.{silver_table_name}"))

# COMMAND ----------

# MAGIC %md **Note:** The benefits of `OPTIMIZE` and `ZORDER` are most apparent on larger datasets and with specific query patterns that filter on the ZORDERed columns.
# MAGIC

# COMMAND ----------

# DBTITLE 1,VACUUM (Garbage Collection)
# MAGIC %md
# MAGIC ### VACUUM
# MAGIC
# MAGIC Removes data files from the table's DBFS directory that are no longer referenced by the Delta transaction log AND are older than a retention threshold.
# MAGIC
# MAGIC * **Purpose:** Clean up old, unreferenced data files (e.g., from `OVERWRITE`, `MERGE` updates, aborted writes) to save storage costs. It also permanently removes data older than the retention period, which can be necessary for compliance (like GDPR).
# MAGIC * **Retention Period:** Controlled by the table property `delta.logRetentionDuration` (default is 7 days). `VACUUM` will not delete files within this retention period unless explicitly forced.
# MAGIC * **WARNING:** Running `VACUUM` with a short retention period (e.g., `RETAIN 0 HOURS`) **permanently deletes** the history required for Time Travel beyond that period. Use extreme caution. Ensure no queries or users need to time travel to versions older than the retention period you specify.
# MAGIC * **Safety Check:** By default, Spark prevents `VACUUM` with less than 7 days retention (`spark.databricks.delta.retentionDurationCheck.enabled`). We need to disable this check for immediate demonstration, but **do not disable it in production** without fully understanding the consequences.
# MAGIC

# COMMAND ----------

# 1. Check Default Retention Period
print(f"Checking table properties for {db_name}.{silver_table_name}...")
display(spark.sql(f"SHOW TBLPROPERTIES {db_name}.{silver_table_name}"))
# Look for 'delta.logRetentionDuration' (default is 'interval 7 days')

# COMMAND ----------

# 2. Disable Retention Check (FOR DEMO ONLY - NOT RECOMMENDED IN PROD)
print("Disabling VACUUM retention check for demonstration purposes...")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
print("Check disabled. VACUUM with < 7 days retention is now allowed.")

# COMMAND ----------

# 3. Run VACUUM (Dry Run First!)
# DRY RUN shows which files *would* be deleted without actually deleting them. Always recommended first.
print("\nRunning VACUUM (DRY RUN) with 0 hours retention...")
try:
   # Use RETAIN 0 HOURS to see files eligible for immediate deletion (beyond the transaction log's active references)
   vacuum_dry_run_result = spark.sql(f"VACUUM {db_name}.{silver_table_name} RETAIN 0 HOURS DRY RUN")
   print("VACUUM DRY RUN completed.")
   # Display the list of files that would be deleted
   display(vacuum_dry_run_result)
except Exception as e:
   print(f"ERROR during VACUUM DRY RUN: {e}")

# COMMAND ----------

# 4. Run VACUUM for Real (USE WITH EXTREME CAUTION with RETAIN 0 HOURS)
# This will PERMANENTLY delete files older than 0 hours not needed for the current table state.
# Time travel beyond this point for deleted versions will fail.
# Consider commenting this out unless you specifically want to test the permanent deletion.

# print("\nRunning actual VACUUM with RETAIN 0 HOURS (PERMANENTLY DELETES HISTORY)...")
# try:
#    spark.sql(f"VACUUM {db_name}.{silver_table_name} RETAIN 0 HOURS")
#    print("Actual VACUUM command completed.")
#    # Verify by checking the DBFS path - some files might be gone
#    # display(dbutils.fs.ls(silver_path_dbfs))
# except Exception as e:
#    print(f"ERROR during actual VACUUM: {e}")


# COMMAND ----------

# 5. IMPORTANT: Re-enable the Safety Check
print("\nRe-enabling VACUUM retention check...")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
print("Check re-enabled. Default safety restored.")

# COMMAND ----------

# DBTITLE 1,CLONE (Shallow and Deep Copies)
# MAGIC %md
# MAGIC ### CLONE
# MAGIC
# MAGIC Creates a copy of an existing Delta table at a specific version. Useful for testing, development, archiving, or creating isolated snapshots.
# MAGIC
# MAGIC * **Shallow Clone:** Creates a copy of the *metadata* (transaction log) of the source table. The clone points to the *same data files* as the source. It's very fast and storage-efficient, great for quick tests or dev copies. Changes to the source *after* the clone are not reflected. Changes to the clone do not affect the source.
# MAGIC * **Deep Clone:** Creates a copy of both the metadata *and* the data files. The clone is completely independent of the source table. Takes longer and consumes more storage but provides full isolation.

# COMMAND ----------

# 1. Shallow Clone
# Creates only a metadata copy, pointing to the original data files. Very fast.
print(f"Creating SHALLOW clone '{silver_clone_shallow_name}' from '{silver_table_name}'...")
try:
    spark.sql(f"DROP TABLE IF EXISTS {db_name}.{silver_clone_shallow_name}") # Drop if exists from previous run
    spark.sql(f"""
        CREATE TABLE {db_name}.{silver_clone_shallow_name}
        SHALLOW CLONE {db_name}.{silver_table_name}
        -- Optional: Clone a specific version or timestamp
        -- VERSION AS OF {previous_version}
        -- TIMESTAMP AS OF '{ts_string}'
    """)
    print(f"SUCCESS: Shallow clone '{silver_clone_shallow_name}' created.")

    # Verify: Describe detail should show it's a shallow clone and point to original files initially
    display(spark.sql(f"DESCRIBE DETAIL {db_name}.{silver_clone_shallow_name}"))
    display(spark.sql(f"DESCRIBE EXTENDED {db_name}.{silver_clone_shallow_name}")) # Location might be same as source

except Exception as e:
    print(f"ERROR creating shallow clone: {e}")

# COMMAND ----------

# 2. Deep Clone
# Copies both metadata and data files to a new location. Slower, uses more storage, fully independent.
print(f"\nCreating DEEP clone '{silver_clone_deep_name}' from '{silver_table_name}'...")
# Ensure the target path for the deep clone data is clean
print(f"Removing any existing data at deep clone path: {silver_clone_deep_path}")
dbutils.fs.rm(silver_clone_deep_path, recurse=True)

try:
    spark.sql(f"DROP TABLE IF EXISTS {db_name}.{silver_clone_deep_name}") # Drop table definition if exists
    spark.sql(f"""
        CREATE TABLE {db_name}.{silver_clone_deep_name}
        DEEP CLONE {db_name}.{silver_table_name}
        LOCATION '{silver_clone_deep_path}' -- Specify a new location for the copied data
    """)
    print(f"SUCCESS: Deep clone '{silver_clone_deep_name}' created.")
    print(f"Data files copied to: {silver_clone_deep_path}")

    # Verify: Describe detail should show it's NOT a shallow clone. Location should be the new path.
    display(spark.sql(f"DESCRIBE DETAIL {db_name}.{silver_clone_deep_name}"))
    display(spark.sql(f"DESCRIBE EXTENDED {db_name}.{silver_clone_deep_name}"))

    # Check counts match original table at time of clone
    original_count = spark.table(f"{db_name}.{silver_table_name}").count()
    deep_clone_count = spark.table(f"{db_name}.{silver_clone_deep_name}").count()
    print(f"Original count: {original_count}, Deep clone count: {deep_clone_count}")
    assert original_count == deep_clone_count

except Exception as e:
    print(f"ERROR creating deep clone: {e}")

# COMMAND ----------

# DBTITLE 1,RESTORE (Reverting Table State)
# MAGIC %md
# MAGIC ### RESTORE
# MAGIC
# MAGIC Reverts a Delta table back to an earlier state, specified by version number or timestamp. This is like "committing" a time travel operation. It creates a new table version recording the restore.
# MAGIC
# MAGIC **Use Cases:** Recovering from accidental bad writes (DELETEs, UPDATEs), rolling back faulty pipeline runs.

# COMMAND ----------

# 1. Get Current and Target Restore Version
# Let's restore the original silver table back to the version *before* the MERGE operation.
print(f"Preparing to RESTORE table '{silver_table_name}'...")
try:
    history_df_restore = spark.sql(f"DESCRIBE HISTORY {db_name}.{silver_table_name}")
    current_version_restore = history_df_restore.selectExpr("max(version)").first()[0]

    # Find the version just before the MERGE operation
    merge_version_df = history_df_restore.filter("operation = 'MERGE'").selectExpr("min(version) as version") # Find first MERGE version
    if merge_version_df.count() > 0:
        merge_version = merge_version_df.first()["version"]
        target_restore_version = merge_version - 1 # Version before the merge
        print(f"Current table version: {current_version_restore}")
        print(f"Target restore version (before MERGE): {target_restore_version}")

        # Check if target version is valid
        if target_restore_version < 0:
             print("Cannot restore to version less than 0.")
             target_restore_version = None # Prevent restore attempt

    else:
        print("MERGE operation not found in history. Cannot determine pre-MERGE version.")
        target_restore_version = None # Prevent restore attempt

except Exception as e:
    print(f"Error getting versions for RESTORE: {e}")
    target_restore_version = None

# COMMAND ----------

# 2. Execute RESTORE
if target_restore_version is not None:
    print(f"\nExecuting RESTORE to version {target_restore_version}...")
    try:
        spark.sql(f"RESTORE TABLE {db_name}.{silver_table_name} VERSION AS OF {target_restore_version}")
        # Or use TIMESTAMP AS OF: RESTORE TABLE table_name TIMESTAMP AS OF 'yyyy-MM-dd HH:mm:ss'
        print(f"SUCCESS: Table '{silver_table_name}' restored to version {target_restore_version}.")

        # Verify: Check history - a new RESTORE entry should exist.
        print("\nChecking table history after RESTORE:")
        display(spark.sql(f"DESCRIBE HISTORY {db_name}.{silver_table_name}"))

        # Verify: Check data - the records added/updated by MERGE should be gone.
        print("\nVerifying data state after RESTORE (checking for MERGE artifacts)...")
        restored_count = spark.table(f"{db_name}.{silver_table_name}").count()
        print(f"Record count after restore: {restored_count}")
        # Check if the 'Updated Tip' flag from MERGE is gone
        updated_rows_after_restore = spark.table(f"{db_name}.{silver_table_name}").filter("data_source_quality = 'Updated Tip'").count()
        print(f"Count of rows with 'Updated Tip' flag: {updated_rows_after_restore}")
        assert updated_rows_after_restore == 0 # Should be 0 after restore

    except Exception as e:
        print(f"ERROR during RESTORE operation: {e}")
else:
    print("\nSkipping RESTORE operation as target version could not be determined or was invalid.")

# COMMAND ----------

# DBTITLE 1,Cleanup Cloned Tables (Optional)
# MAGIC %md ### Optional Cleanup: Drop Cloned Tables

# COMMAND ----------

# Set to True to drop the tables created by CLONE
cleanup_clones = False

if cleanup_clones:
    print("Cleaning up cloned tables...")
    try:
        spark.sql(f"DROP TABLE IF EXISTS {db_name}.{silver_clone_shallow_name}")
        print(f"Dropped shallow clone: {db_name}.{silver_clone_shallow_name}")
    except Exception as e:
        print(f"Error dropping shallow clone: {e}")

    try:
        spark.sql(f"DROP TABLE IF EXISTS {db_name}.{silver_clone_deep_name}")
        # IMPORTANT: Dropping the table definition for a deep clone DOES NOT automatically delete the data files
        # at the specified LOCATION unless it was a managed table clone (no LOCATION specified).
        # Since we specified a LOCATION for the deep clone, we need to manually remove the data files.
        print(f"Dropped deep clone table definition: {db_name}.{silver_clone_deep_name}")
        print(f"Removing deep clone data files at: {silver_clone_deep_path}")
        dbutils.fs.rm(silver_clone_deep_path, recurse=True)
        print(f"Removed deep clone data files.")
    except Exception as e:
        print(f"Error dropping deep clone or removing its data: {e}")
    print("Clone cleanup finished.")
else:
    print("Skipping clone cleanup.")

# COMMAND ----------

