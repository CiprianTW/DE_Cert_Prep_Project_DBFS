# Databricks notebook source
# MAGIC %md
# MAGIC # 05. Structured Streaming with Auto Loader & Aggregations (DBFS)
# MAGIC
# MAGIC **Purpose:** Demonstrate incremental data ingestion from a DBFS directory using Structured Streaming and Auto Loader, and then perform stateful streaming aggregations with windowing and watermarking. Write outputs to Delta tables on DBFS.
# MAGIC
# MAGIC **Key Concepts:**
# MAGIC * **Structured Streaming:** Spark's engine for processing data streams incrementally and fault-tolerantly.
# MAGIC * **Auto Loader (`cloudFiles` format):** Optimized source for ingesting files, handling file tracking and schema evolution.
# MAGIC * **Checkpoint Location:** Essential for fault-tolerance and state management in streaming queries.
# MAGIC * **Schema Location:** Used by Auto Loader for schema inference and evolution.
# MAGIC * **Triggers:** Control processing frequency (`availableNow`, `processingTime`).
# MAGIC * **Stream Sink:** Destination for streaming data (Delta tables on DBFS).
# MAGIC * **Event Time:** Using timestamps embedded in the data (e.g., `tpep_pickup_datetime`) for time-based operations.
# MAGIC * **Windowing:** Aggregating data over specific time intervals (e.g., 10-minute windows).
# MAGIC * **Watermarking (`withWatermark`):** A mechanism to handle late-arriving data based on event time. It allows Spark to know how late data is expected, enabling it to drop old state for aggregations and bound memory usage.
# MAGIC * **Stateful Aggregation:** Aggregations (like counts, sums over windows) that require Spark to maintain state across micro-batches.
# MAGIC * **Output Modes:** How results of streaming aggregations are written (`Append`, `Update`, `Complete`).
# MAGIC
# MAGIC **DBFS Focus:**
# MAGIC * **Source:** Auto Loader monitors a DBFS directory.
# MAGIC * **Sinks:** Streams write to Delta tables located in DBFS paths.
# MAGIC * **Checkpoint & Schema Location:** Configured to use DBFS paths.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC * Notebooks `00` through `04` have been run successfully.
# MAGIC * The raw data file exists in the main DBFS landing zone (`landing_path_dbfs`).

# COMMAND ----------

# MAGIC %run ../DE_Cert_Prep_Project_DBFS/_Helper_Config_DBFS

# COMMAND ----------

# DBTITLE 1,Load Configuration from Spark Conf
import re
import time
# Import necessary functions, including window and watermark related ones
from pyspark.sql.functions import col, current_timestamp, input_file_name, window, count, expr
from pyspark.sql.types import TimestampType

# Retrieve necessary configuration
try:
    db_name = get_config_value("db_name")
    landing_path_dbfs = get_config_value("landing_path_dbfs") # Main landing zone
    raw_data_file = get_config_value("raw_data_file")
    # Specific paths for this streaming example
    streaming_source_dir_dbfs = get_config_value("streaming_source_dir_dbfs") # Dedicated input dir for streaming
    checkpoint_base_path_dbfs = get_config_value("checkpoint_base_path_dbfs")
    project_base_dbfs = get_config_value("project_base_dbfs")

    # --- Paths and names for the initial Bronze stream ---
    streaming_bronze_table_name = "streaming_bronze_taxi_trips"
    streaming_bronze_delta_path = f"{project_base_dbfs}/delta/streaming_bronze"
    streaming_bronze_checkpoint_path = f"{checkpoint_base_path_dbfs}/streaming_bronze_checkpoint"
    streaming_bronze_schema_location = f"{checkpoint_base_path_dbfs}/streaming_bronze_schema"

    # --- Paths and names for the windowed aggregation stream ---
    streaming_agg_table_name = "streaming_agg_windowed_pickup_counts"
    streaming_agg_delta_path = f"{project_base_dbfs}/delta/streaming_agg_windowed"
    streaming_agg_checkpoint_path = f"{checkpoint_base_path_dbfs}/streaming_agg_windowed_checkpoint"

except ValueError as e:
    print(f"Configuration Error: {e}")
    dbutils.notebook.exit("Failed to load configuration.")

# Set the current database context
spark.sql(f"USE {db_name}")

print(f"--- Configuration Loaded ---")
print(f"Database Name:                 {db_name}")
print(f"Streaming Source Dir (DBFS):   {streaming_source_dir_dbfs}")
print(f"Streaming Bronze Path (DBFS):  {streaming_bronze_delta_path}")
print(f"Streaming Bronze Table:        {db_name}.{streaming_bronze_table_name}")
print(f"Bronze Checkpoint (DBFS):      {streaming_bronze_checkpoint_path}")
print(f"Bronze Schema Loc (DBFS):      {streaming_bronze_schema_location}")
print(f"Streaming Agg Path (DBFS):     {streaming_agg_delta_path}")
print(f"Streaming Agg Table:           {db_name}.{streaming_agg_table_name}")
print(f"Agg Checkpoint (DBFS):         {streaming_agg_checkpoint_path}")
print(f"Original Raw File Source:      {landing_path_dbfs}/{raw_data_file}")

# COMMAND ----------

# DBTITLE 1,Prepare Streaming Source Directory & Simulate File Arrival
# MAGIC %md
# MAGIC ### Prepare Streaming Source Directory & Simulate File Arrival
# MAGIC
# MAGIC ### Prepare Streaming Source Directory
# MAGIC (Same as before - ensure the source directory exists and copy the sample file if needed)
# MAGIC

# COMMAND ----------

# 1. Ensure the streaming source directory exists
print(f"Ensuring streaming source directory exists: {streaming_source_dir_dbfs}")
dbutils.fs.mkdirs(streaming_source_dir_dbfs)

# 2. Simulate file arrival by copying the raw data file
source_file_path = f"{landing_path_dbfs}/{raw_data_file}"
target_file_path = f"{streaming_source_dir_dbfs}/{raw_data_file}"

print(f"\nSimulating file arrival by copying:")
print(f"  Source: {source_file_path}")
print(f"  Target: {target_file_path}")

try:
    dbutils.fs.ls(target_file_path)
    print(f"INFO: File '{raw_data_file}' already exists in the streaming source directory. Skipping copy.")
except Exception as e:
     if 'java.io.FileNotFoundException' in str(e):
         try:
             dbutils.fs.cp(source_file_path, target_file_path)
             print(f"SUCCESS: Copied sample file to streaming source directory.")
         except Exception as copy_e:
             print(f"ERROR copying file: {copy_e}")
             dbutils.notebook.exit("Failed to prepare streaming source file.")
     else:
        print(f"ERROR checking target file path: {e}")
        dbutils.notebook.exit("Failed during streaming source preparation.")

print("\nCurrent contents of streaming source directory:")
display(dbutils.fs.ls(streaming_source_dir_dbfs))

# COMMAND ----------

# DBTITLE 1,Define and Run: Auto Loader Stream (Bronze Layer)
# MAGIC %md
# MAGIC ### Stream 1: Ingest Raw Data using Auto Loader
# MAGIC This stream reads raw files as they land and writes them to a Bronze Delta table, adding ingestion metadata.

# COMMAND ----------

print("Defining the Auto Loader stream (Bronze Ingestion)...")

# Configure the streaming read using 'cloudFiles'
raw_streaming_df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", streaming_bronze_schema_location) \
    .option("cloudFiles.inferColumnTypes", "true") \
    .option("pathGlobFilter", "*.parquet") \
    .load(streaming_source_dir_dbfs)

# Add ingestion metadata
bronze_stream_df = raw_streaming_df \
    .withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast(TimestampType())) \
    .withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast(TimestampType())) \
    .withColumnRenamed("PULocationID", "pickup_location_id") \
    .withColumn("streaming_ingestion_timestamp", current_timestamp()) \
    .withColumn("streaming_source_file", input_file_name())

print("Bronze Streaming DataFrame defined.")
bronze_stream_df.printSchema()

# Write the Bronze stream to its Delta table on DBFS
print(f"\nSetting up Bronze streaming write to Delta table '{db_name}.{streaming_bronze_table_name}'...")
bronze_stream_query_name = "autoloader_dbfs_to_delta_bronze"

bronze_streaming_writer = (bronze_stream_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", streaming_bronze_checkpoint_path)
    .option("path", streaming_bronze_delta_path) # Specify path for external table data
    .queryName(bronze_stream_query_name)
    .trigger(availableNow=True) # Process available files and stop
    .toTable(f"{db_name}.{streaming_bronze_table_name}")
)

print(f"Bronze Streaming query '{bronze_stream_query_name}' defined and will run now (availableNow=True).")
# Let this stream run to completion to populate the bronze table for the next step
# No awaitTermination needed due to availableNow=True

# COMMAND ----------

# DBTITLE 1,Define and Run: Streaming Aggregation with Windowing & Watermarking
# MAGIC %md
# MAGIC ### Stream 2: Perform Windowed Aggregation on Bronze Data
# MAGIC This stream reads from the Bronze Delta table (created by the previous stream), performs a time-windowed aggregation (e.g., count trips per location per 10 minutes), uses watermarking to handle late data, and writes the results to an aggregated Delta table.

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Explanation: Windowing, Watermarking, and Late Data
# MAGIC * **Event Time:** We use the `tpep_pickup_datetime` column from the data itself as the basis for time-based operations.
# MAGIC * **Tumbling Windows:** We'll use `window(event_time, windowDuration)` to group data into fixed, non-overlapping time intervals (e.g., 10 minutes).
# MAGIC * **Watermarking (`withWatermark`):** This is crucial for stateful streaming aggregations.
# MAGIC     * It tells Spark how late data (based on event time) is expected to arrive. Syntax: `.withWatermark("event_time_column", "delay_threshold")`.
# MAGIC     * Example: `withWatermark("tpep_pickup_datetime", "20 minutes")` means Spark will wait up to 20 minutes (in event time) for late data to arrive for a particular window before finalizing the aggregation for that window and dropping its state.
# MAGIC     * **Why?** Without watermarking, Spark would have to keep the state for *all* windows indefinitely, potentially running out of memory. Watermarking allows Spark to bound the state it needs to maintain.
# MAGIC     * Data arriving later than the watermark for its window will be dropped (considered too late).
# MAGIC * **Output Mode:** For aggregations with watermarking, `append` mode is commonly used. It outputs results only after a window is finalized (i.e., after the watermark passes the window's end time). `update` mode can also be used to emit intermediate results.

# COMMAND ----------

print("Defining the Windowed Aggregation stream...")

# Read from the Bronze streaming table (which is just a regular Delta table now)
try:
    # It's important that the bronze stream (Stream 1) has finished writing
    # before we start reading from its output table for Stream 2.
    # A small delay helps ensure metadata is updated, especially in interactive notebooks.
    print("Waiting briefly for Bronze table metadata to update...")
    time.sleep(15)
    bronze_table_df = spark.readStream.table(f"{db_name}.{streaming_bronze_table_name}")
    print(f"Successfully set up readStream from {db_name}.{streaming_bronze_table_name}")
except Exception as e:
    print(f"ERROR reading from streaming bronze table {db_name}.{streaming_bronze_table_name}: {e}")
    dbutils.notebook.exit("Failed to read input for aggregation stream.")


# Define watermark and aggregation
# Watermark: Allow data to be up to 20 minutes late based on pickup time.
# Window: Aggregate counts over 10-minute tumbling windows.
aggregation_stream_df = bronze_table_df \
    .withWatermark("tpep_pickup_datetime", "20 minutes") \
    .groupBy(
        window("tpep_pickup_datetime", "10 minutes"), # 10-minute tumbling window
        col("pickup_location_id")
    ).agg(
        count("*").alias("trip_count")
    )

print("Windowed Aggregation DataFrame defined.")
aggregation_stream_df.printSchema()


# Write the aggregated stream to its Delta table on DBFS
print(f"\nSetting up Aggregation streaming write to Delta table '{db_name}.{streaming_agg_table_name}'...")
agg_stream_query_name = "streaming_aggregation_dbfs"

aggregation_streaming_writer = (aggregation_stream_df.writeStream
    .format("delta")
    .outputMode("append") # Append finalized window results
    # .outputMode("update") # Use update mode to see intermediate counts per microbatch
    .option("checkpointLocation", streaming_agg_checkpoint_path)
    .option("path", streaming_agg_delta_path) # Specify path for external table data
    .queryName(agg_stream_query_name)
    .trigger(availableNow=True) # Process available data and stop
    .toTable(f"{db_name}.{streaming_agg_table_name}")
)

print(f"Aggregation Streaming query '{agg_stream_query_name}' defined and will run now (availableNow=True).")
# This stream reads from the output of the first stream.

# COMMAND ----------

# DBTITLE 1,Verify the Output Delta Tables
# MAGIC %md
# MAGIC After both `availableNow=True` streams complete, verify the contents of both the Bronze and the aggregated Gold tables.

# COMMAND ----------

# Give the second stream write a moment to fully commit metadata.
print("Waiting a few seconds for aggregation stream commit...")
time.sleep(15) # Increased wait time as aggregation might take longer

# --- Verify Bronze Streaming Table ---
print(f"\n--- Verifying Bronze Streaming Table: {db_name}.{streaming_bronze_table_name} ---")
try:
    bronze_target_df = spark.table(f"{db_name}.{streaming_bronze_table_name}")
    bronze_record_count = bronze_target_df.count()
    print(f"SUCCESS: Bronze table exists and contains {bronze_record_count} records.")
    if bronze_record_count > 0:
        display(bronze_target_df.limit(5))
    # Describe the table to confirm its location
    print("\nDescribing Bronze table (Extended):")
    display(spark.sql(f"DESCRIBE EXTENDED {db_name}.{streaming_bronze_table_name}"))
except Exception as e:
    print(f"\nERROR accessing Bronze target table '{db_name}.{streaming_bronze_table_name}': {e}")

# --- Verify Aggregated Streaming Table ---
print(f"\n--- Verifying Aggregated Streaming Table: {db_name}.{streaming_agg_table_name} ---")
try:
    agg_target_df = spark.table(f"{db_name}.{streaming_agg_table_name}")
    agg_record_count = agg_target_df.count()
    print(f"SUCCESS: Aggregated table exists and contains {agg_record_count} records (rows represent finalized window counts).")

    if agg_record_count > 0:
        print("\nSchema of the aggregated table:")
        agg_target_df.printSchema()
        print("\nSample data from aggregated table (limit 10):")
        # Order by window start time for clarity
        display(agg_target_df.orderBy("window.start").limit(10))
    else:
        print("WARNING: Aggregated table is empty. This might be expected if using append mode and the watermark hasn't advanced past the first window, or if the input data timeframe is short.")
        print("Consider using 'update' output mode or processing more data over a longer time span to see results with 'append' mode.")

    # Describe the table to confirm its location
    print("\nDescribing Aggregated table (Extended):")
    display(spark.sql(f"DESCRIBE EXTENDED {db_name}.{streaming_agg_table_name}"))
except Exception as e:
    print(f"\nERROR accessing Aggregated target table '{db_name}.{streaming_agg_table_name}': {e}")
    print("Ensure the aggregation streaming query ran successfully.")

# COMMAND ----------

# DBTITLE 1,Stopping Continuous Streams (Informational)
# MAGIC %md
# MAGIC ### Stopping Continuous Streams
# MAGIC (Same informational content as before regarding stopping streams if using continuous triggers)
# MAGIC ```python
# MAGIC # Example code (do not run now as we used availableNow=True)
# MAGIC # for s in spark.streams.active: print(f"Found active stream: Name='{s.name}', ID='{s.id}'")
# MAGIC # stream_name_to_stop = "your_stream_query_name"
# MAGIC # for s in spark.streams.active:
# MAGIC #     if s.name == stream_name_to_stop:
# MAGIC #         print(f"Stopping stream '{s.name}'..."); s.stop(); print("Stopped.")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Cleanup Streaming Assets (Optional)
# MAGIC %md
# MAGIC ### Optional Cleanup
# MAGIC Run the following cell to remove **all** assets created by **both** streaming examples in this notebook (Bronze stream and Aggregation stream).

# COMMAND ----------

# # Set to True to perform cleanup of ALL streaming assets from this notebook
# cleanup_streaming_all = False

# if cleanup_streaming_all:
#     print("--- Cleaning up ALL streaming assets from this notebook ---")

#     # Assets from Stream 1 (Bronze Ingestion)
#     print("\nCleaning up Bronze stream assets...")
#     try: spark.sql(f"DROP TABLE IF EXISTS {db_name}.{streaming_bronze_table_name}")
#     except Exception as e: print(f"Warn: {e}")
#     try: dbutils.fs.rm(streaming_bronze_delta_path, recurse=True)
#     except Exception as e: print(f"Warn: {e}")
#     try: dbutils.fs.rm(streaming_bronze_checkpoint_path, recurse=True)
#     except Exception as e: print(f"Warn: {e}")
#     try: dbutils.fs.rm(streaming_bronze_schema_location, recurse=True)
#     except Exception as e: print(f"Warn: {e}")
#     try: dbutils.fs.rm(streaming_source_dir_dbfs, recurse=True) # Source dir used by Bronze stream
#     except Exception as e: print(f"Warn: {e}")
#     print("Bronze stream assets cleanup attempted.")

#     # Assets from Stream 2 (Aggregation)
#     print("\nCleaning up Aggregation stream assets...")
#     try: spark.sql(f"DROP TABLE IF EXISTS {db_name}.{streaming_agg_table_name}")
#     except Exception as e: print(f"Warn: {e}")
#     try: dbutils.fs.rm(streaming_agg_delta_path, recurse=True)
#     except Exception as e: print(f"Warn: {e}")
#     try: dbutils.fs.rm(streaming_agg_checkpoint_path, recurse=True)
#     except Exception as e: print(f"Warn: {e}")
#     print("Aggregation stream assets cleanup attempted.")

#     print("\n--- Streaming cleanup finished ---")
# else:
#     print("Skipping cleanup of streaming assets.")

# COMMAND ----------

