# Databricks notebook source
# MAGIC %md
# MAGIC # 03. Gold Layer - Aggregation (DBFS)
# MAGIC
# MAGIC **Purpose:** Create aggregated, business-focused tables optimized for reporting and analytics. This involves reading from the cleaned Silver layer table, performing aggregations (e.g., daily summaries, summaries by location), and writing the results to the Gold DBFS path. Corresponding external Gold tables will be created in the metastore.
# MAGIC
# MAGIC **Gold Layer Characteristics:**
# MAGIC * Aggregated data (SUM, COUNT, AVG, etc.).
# MAGIC * Business-centric view, often de-normalized for BI tools.
# MAGIC * Lower granularity than Silver/Bronze.
# MAGIC * Schema tailored for specific reporting use cases.
# MAGIC * Stored in Delta Lake format, potentially optimized (ZORDER) for query performance on common filter columns.
# MAGIC
# MAGIC **DBFS Focus:**
# MAGIC * Reading from the Silver table (defined in the previous notebook).
# MAGIC * Performing aggregations using Spark SQL or DataFrame API.
# MAGIC * Writing aggregated Delta files to specific subdirectories within `dbfs:/.../delta/gold/`.
# MAGIC * Creating multiple **external** Gold tables (e.g., `gold_daily_trip_summary`, `gold_pickup_zone_summary`) using the `LOCATION` clause pointing to their respective Gold DBFS paths.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC * `00_Setup_Data_DBFS`, `01_Ingestion_Bronze_DBFS`, and `02_Cleansing_Silver_DBFS` notebooks have been run successfully.
# MAGIC * The `silver_taxi_trips` table exists in the specified database.

# COMMAND ----------

# MAGIC %run ../DE_Cert_Prep_Project_DBFS/_Helper_Config_DBFS

# COMMAND ----------

# DBTITLE 1,Load Configuration from Spark Conf
import re
from pyspark.sql.functions import col, count, sum, avg, date_trunc, countDistinct, round # Import necessary functions

# Retrieve necessary configuration
try:
    db_name = get_config_value("db_name")
    # Silver table name is fixed based on the previous notebook
    silver_table_name = "silver_taxi_trips"
    gold_path_dbfs_base = get_config_value("gold_path_dbfs_base") # Base path for Gold tables
except ValueError as e:
    print(f"Configuration Error: {e}")
    dbutils.notebook.exit("Failed to load configuration.")

# Define Gold table names and their specific DBFS paths within the Gold base path
gold_table_daily_summary = "gold_daily_trip_summary"
gold_daily_path_dbfs = f"{gold_path_dbfs_base}/{gold_table_daily_summary}" # Specific path for this table's data

gold_table_zone_summary = "gold_pickup_zone_summary"
gold_zone_path_dbfs = f"{gold_path_dbfs_base}/{gold_table_zone_summary}" # Specific path for this table's data

# Set the current database context
spark.sql(f"USE {db_name}")

print(f"--- Configuration Loaded ---")
print(f"Database Name:          {db_name}")
print(f"Reading From Table:     {db_name}.{silver_table_name}")
print(f"Gold Base Path (DBFS):  {gold_path_dbfs_base}")
print(f"Daily Summary Path:     {gold_daily_path_dbfs}")
print(f"Zone Summary Path:      {gold_zone_path_dbfs}")

# COMMAND ----------

# DBTITLE 1,Read from Silver Layer Table
print(f"Reading data from Silver table: '{db_name}.{silver_table_name}'...")
try:
    # Read the cleaned data from the Silver table
    silver_df = spark.table(f"{db_name}.{silver_table_name}")

    # Verify read
    silver_count = silver_df.count()
    print(f"Successfully read {silver_count} records from Silver table.")

    # Optional: Cache if performing multiple aggregations on Silver data
    # silver_df.cache()
    # print("Cached Silver DataFrame for potentially faster subsequent aggregations.")

except Exception as e:
    print(f"\nERROR reading Silver table '{db_name}.{silver_table_name}': {e}")
    print(f"Troubleshooting:")
    print(f"  - Ensure '02_Cleansing_Silver_DBFS' notebook ran successfully.")
    print(f"  - Verify the table exists using `SHOW TABLES IN {db_name};`.")
    dbutils.notebook.exit("Failed to read Silver table.")

# COMMAND ----------

# DBTITLE 1,Gold Table 1: Daily Trip Summary Aggregation
# Create a summary table aggregating key metrics by day.
print("Calculating daily trip summary aggregation...")

try:
    daily_summary_df = silver_df.groupBy(
            # Truncate the pickup timestamp to the day level
            date_trunc("day", col("tpep_pickup_datetime")).alias("trip_date")
        ).agg(
            # Define aggregations: count trips, average distance, sum fares/tips, average duration
            count("*").alias("total_trips"),
            # Use pyspark.sql.functions.round (imported as round)
            round(avg("trip_distance"), 2).alias("average_trip_distance_miles"),
            round(sum("fare_amount"), 2).alias("total_fare_amount"),
            round(sum("tip_amount"), 2).alias("total_tip_amount"),
            round(avg("trip_duration_minutes"), 2).alias("average_trip_duration_minutes")
        ).orderBy("trip_date") # Order by date for clarity

    print("Daily summary calculation complete.")
    print("Schema of Daily Summary:")
    daily_summary_df.printSchema()
    print("\nSample Daily Summary Data:")
    display(daily_summary_df.limit(10))

except Exception as e:
    print(f"\nERROR calculating daily summary: {e}")
    dbutils.notebook.exit("Failed during daily summary aggregation.")

# COMMAND ----------

# DBTITLE 1,Write Daily Summary Gold Table to DBFS & Create External Table
# Write the aggregated daily summary data to its specific Gold DBFS path.
print(f"Writing Daily Summary Gold data to Delta path: {gold_daily_path_dbfs}")

try:
    daily_summary_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(gold_daily_path_dbfs) # Save to the specific DBFS path for this table
    print(f"Successfully wrote Daily Summary Delta files to {gold_daily_path_dbfs}")
except Exception as e:
    print(f"\nERROR writing Daily Summary Gold data: {e}")
    dbutils.notebook.exit("Failed writing daily summary to DBFS.")

# Create the external table definition in the metastore pointing to the data path.
print(f"\nCreating external table '{db_name}.{gold_table_daily_summary}'...")
daily_summary_create_sql = f"""
CREATE TABLE IF NOT EXISTS {db_name}.{gold_table_daily_summary}
USING DELTA
LOCATION '{gold_daily_path_dbfs}'
COMMENT 'Gold layer: Daily aggregated trip summary. Data stored in DBFS at {gold_daily_path_dbfs}'
"""
try:
    spark.sql(daily_summary_create_sql)
    print(f"Table '{db_name}.{gold_table_daily_summary}' created or already exists.")
except Exception as e:
    print(f"\nERROR creating external table {gold_table_daily_summary}: {e}")
    dbutils.notebook.exit("Failed to create daily summary external table.")

# COMMAND ----------

# DBTITLE 1,Gold Table 2: Pickup Location Zone Summary Aggregation
# Create another summary, this time aggregating metrics by pickup location zone and potentially time period.
# Note: For actual zone names, you'd typically join with a separate Zone Lookup table.
# Here, we'll just aggregate by the pickup_location_id.
print("\nCalculating pickup location zone summary aggregation...")

# Determine partitioning strategy for this Gold table (e.g., by month)
gold_zone_partition_cols = []
if "pickup_year" in silver_df.columns and "pickup_month" in silver_df.columns:
   gold_zone_partition_cols = ["pickup_year", "pickup_month"]
   print(f"Zone summary will be partitioned by: {gold_zone_partition_cols}")
else:
   print("Zone summary will not be partitioned.")

try:
    # Group by partition columns (if any) and the location ID
    grouping_cols = gold_zone_partition_cols + ["pickup_location_id"]

    zone_summary_df = silver_df.groupBy(*grouping_cols) \
        .agg(
            count("*").alias("total_trips"),
            round(avg("total_amount"), 2).alias("average_total_amount"),
            round(avg("tip_amount"), 2).alias("average_tip_amount"),
            round(avg("trip_distance"), 2).alias("average_trip_distance_miles"),
            countDistinct("vendor_id").alias("distinct_vendors_count") # Example distinct count
        ).orderBy(*gold_zone_partition_cols, col("total_trips").desc()) # Order for clarity

    print("Zone summary calculation complete.")
    print("Schema of Zone Summary:")
    zone_summary_df.printSchema()
    print("\nSample Zone Summary Data:")
    display(zone_summary_df.limit(10))

except Exception as e:
    print(f"\nERROR calculating zone summary: {e}")
    dbutils.notebook.exit("Failed during zone summary aggregation.")

# COMMAND ----------

# DBTITLE 1,Write Zone Summary Gold Table to DBFS & Create External Table
print(f"\nWriting Zone Summary Gold data to Delta path: {gold_zone_path_dbfs}")

try:
    zone_writer = zone_summary_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true")

    if gold_zone_partition_cols:
        zone_writer = zone_writer.partitionBy(gold_zone_partition_cols)
        print(f"Writing Zone Summary partitioned by: {gold_zone_partition_cols}")

    zone_writer.save(gold_zone_path_dbfs)
    print(f"Successfully wrote Zone Summary Delta files to {gold_zone_path_dbfs}")
except Exception as e:
    print(f"\nERROR writing Zone Summary Gold data: {e}")
    dbutils.notebook.exit("Failed writing zone summary to DBFS.")

print(f"\nCreating external table '{db_name}.{gold_table_zone_summary}'...")
zone_summary_create_sql = f"""
CREATE TABLE IF NOT EXISTS {db_name}.{gold_table_zone_summary}
USING DELTA
LOCATION '{gold_zone_path_dbfs}'
COMMENT 'Gold layer: Aggregated trip summary by pickup zone ID and month. Data stored in DBFS at {gold_zone_path_dbfs}'
"""
# The following lines for TBLPROPERTIES are REMOVED/COMMENTED OUT to resolve the error:
# if gold_zone_partition_cols:
#     partition_spec_str = ", ".join(gold_zone_partition_cols)
#     # For external tables, TBLPROPERTIES for partition_columns is informational
#     # Delta infers partitioning from the directory structure at LOCATION.
#     # zone_summary_create_sql += f"\nTBLPROPERTIES ('partition_columns' = '{partition_spec_str}')"

try:
    spark.sql(zone_summary_create_sql)
    print(f"Table '{db_name}.{gold_table_zone_summary}' created or already exists.")
except Exception as e:
    print(f"\nERROR creating external table {gold_table_zone_summary}: {e}")
    dbutils.notebook.exit("Failed to create zone summary external table.")

# COMMAND ----------

# DBTITLE 1,Verify Gold Tables Access via SQL
print(f"\n--- Verifying Gold Table: {db_name}.{gold_table_daily_summary} ---")
try:
    print("Describing Extended:")
    display(spark.sql(f"DESCRIBE EXTENDED {db_name}.{gold_table_daily_summary}"))
    print("\nSample Data:")
    display(spark.table(f"{db_name}.{gold_table_daily_summary}").limit(5))
    print("\nRecord Count:")
    display(spark.sql(f"SELECT COUNT(*) as count FROM {db_name}.{gold_table_daily_summary}"))
except Exception as e:
    print(f"ERROR verifying {gold_table_daily_summary}: {e}")


print(f"\n--- Verifying Gold Table: {db_name}.{gold_table_zone_summary} ---")
try:
    print("Describing Extended:")
    display(spark.sql(f"DESCRIBE EXTENDED {db_name}.{gold_table_zone_summary}"))
    print("\nSample Data:")
    display(spark.table(f"{db_name}.{gold_table_zone_summary}").limit(5))
    print("\nRecord Count:")
    display(spark.sql(f"SELECT COUNT(*) as count FROM {db_name}.{gold_table_zone_summary}"))
    if gold_zone_partition_cols:
        print("\nPartitions:")
        display(spark.sql(f"SHOW PARTITIONS {db_name}.{gold_table_zone_summary}"))
except Exception as e:
    print(f"ERROR verifying {gold_table_zone_summary}: {e}")

# Optional: Uncache Silver DataFrame if it was cached earlier
if silver_df.is_cached:
   silver_df.unpersist()
   print("\nUnpersisted Silver DataFrame.")

print(f"\n--- Gold Layer Processing and Verification Complete ---")

# COMMAND ----------

