# Databricks notebook source
# MAGIC %md
# MAGIC # 01. Bronze Layer Ingestion (DBFS)
# MAGIC
# MAGIC **Purpose:** Read raw data from the DBFS landing zone and write it as a Delta table to the designated Bronze DBFS path. Create an *unmanaged (external)* table pointing to this path.
# MAGIC
# MAGIC **DBFS Focus:**
# MAGIC * Reading source data (Parquet) directly from a `dbfs:/` path.
# MAGIC * Writing Delta Lake formatted files to a specific `dbfs:/` path for the Bronze layer.
# MAGIC * Creating an **external** Hive Metastore table using the `LOCATION` clause pointing to the Bronze DBFS path. This separates the table metadata from the underlying data files on DBFS.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC * `00_Setup_Data_DBFS` notebook has been run successfully.
# MAGIC * Raw data file (e.g., `yellow_tripdata_2023_01.parquet`) exists in the project's DBFS landing zone.

# COMMAND ----------

# MAGIC %run ../DE_Cert_Prep_Project_DBFS/_Helper_Config_DBFS

# COMMAND ----------

# DBTITLE 1,Load Configuration from Spark Conf
try:
    db_name = get_config_value("db_name")
    landing_path_dbfs = get_config_value("landing_path_dbfs")
    bronze_path_dbfs = get_config_value("bronze_path_dbfs")
    raw_data_file = get_config_value("raw_data_file")
    full_raw_data_path = f"{landing_path_dbfs}/{raw_data_file}" # Construct full path
except ValueError as e:
    print(f"FATAL Configuration Error: {e}")
    dbutils.notebook.exit("Failed to load essential configuration using helper. Cannot proceed.")

# Set the current database context
try:
    spark.sql(f"USE {db_name}")
    print(f"--- Configuration Loaded Successfully via Helper ---")
    print(f"Database Name:        {db_name}")
    print(f"Reading From (DBFS):  {full_raw_data_path}")
    print(f"Writing To (DBFS):    {bronze_path_dbfs}")
except Exception as e_sql:
    print(f"ERROR setting database context with db_name='{db_name}': {e_sql}")
    dbutils.notebook.exit("Failed to set database context.")

# COMMAND ----------

# DBTITLE 1,Read Raw Data (Parquet from DBFS Landing Zone)
from pyspark.sql.functions import col, year, month, current_timestamp, input_file_name # Ensure all functions are imported

print(f"Reading raw Parquet data from: {full_raw_data_path}")
try:
    # Use spark.read.parquet to read the data directly from the DBFS path
    raw_df = spark.read.format("parquet").load(full_raw_data_path)

    # Perform an action (like count) to trigger the read and verify basic access
    record_count = raw_df.count()
    print(f"Successfully read {record_count} records from the source file: {raw_data_file}")

    # Display schema and sample data for inspection (optional, can be commented out for faster runs)
    # print("\nInferred Schema:")
    # raw_df.printSchema()
    # print("\nSample Data (limit 3):")
    # display(raw_df.limit(3))

except Exception as e:
    print(f"\nERROR reading Parquet file from DBFS path '{full_raw_data_path}': {e}")
    print(f"Troubleshooting:")
    print(f"  - Verify the path is correct and the file exists (check output of 00_Setup notebook).")
    print(f"  - Ensure the cluster has permissions to read from this DBFS path.")
    # dbutils.notebook.exit(f"Failed to read source data. See error above.")

# COMMAND ----------

# DBTITLE 1,Add Ingestion Metadata Columns

# Enhance the raw data with metadata common in Bronze layers:
# - ingestion_timestamp: Records when the data was processed into Bronze.
# - source_file: Records the specific source file the data came from. input_file_name() works with DBFS paths.

print("Adding ingestion metadata columns ('ingestion_timestamp', 'source_file')...")
bronze_df = raw_df.withColumn("ingestion_timestamp", current_timestamp()) \
                                .withColumn("source_file", input_file_name())

print("Schema after adding metadata:")
bronze_df.printSchema()
print("\nSample Data with Metadata (limit 5):")
display(bronze_df.select("ingestion_timestamp", "source_file", *raw_df.columns[:3]).limit(5)) # Show new cols + first few original


# COMMAND ----------

# DBTITLE 1,Define Bronze Table Name and Partitioning Strategy
# Define the name for the Bronze table in the metastore
bronze_table_name = "bronze_taxi_trips"

# Determine partitioning strategy based on available columns
# Partitioning helps optimize queries that filter on the partition columns.
# Common strategies include date/time components (year, month, day).
partition_columns = [] # Default to no partitioning

# Check if a suitable timestamp column exists for partitioning
# The NYC Taxi dataset often has 'tpep_pickup_datetime'
pickup_timestamp_col = "tpep_pickup_datetime"
if pickup_timestamp_col in bronze_df.columns:
    from pyspark.sql.functions import year, month
    print(f"Found '{pickup_timestamp_col}', deriving 'pickup_year' and 'pickup_month' for partitioning.")
    # Add year and month columns derived from the pickup timestamp
    bronze_df = bronze_df.withColumn("pickup_year", year(col(pickup_timestamp_col))) \
                         .withColumn("pickup_month", month(col(pickup_timestamp_col)))
    # Define the columns to use for partitioning the Delta table on disk
    partition_columns = ["pickup_year", "pickup_month"]
    print(f"Partition columns set to: {partition_columns}")
else:
    print(f"Column '{pickup_timestamp_col}' not found. Writing Bronze table without partitioning.")

# COMMAND ----------

# DBTITLE 1,Write to Bronze Delta Path on DBFS
# Write the DataFrame containing raw data + metadata to the designated Bronze DBFS path.
# The data will be stored in Delta Lake format.

print(f"Starting write operation to Delta path: {bronze_path_dbfs}")

# Configure the DataFrame writer for Delta format
writer = bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") # Allows schema changes if the source schema evolves between runs (useful for overwrite mode)

# Apply partitioning if partition columns were defined
if partition_columns:
    writer = writer.partitionBy(partition_columns)
    print(f"Writing data partitioned by: {partition_columns}")
else:
    print("Writing data without partitioning.")

# Execute the write operation to the specified DBFS path
try:
    writer.save(bronze_path_dbfs)
    print(f"\nSUCCESS: Data written successfully to Delta path: {bronze_path_dbfs}")
except Exception as e:
    print(f"\nERROR writing Delta table to DBFS path '{bronze_path_dbfs}': {e}")
    dbutils.notebook.exit("Failed during Delta write operation.")

# Verify the contents of the DBFS path (should contain _delta_log and data files)
print("\nListing contents of the Bronze Delta path:")
display(dbutils.fs.ls(bronze_path_dbfs))

# COMMAND ----------

# DBTITLE 1,Create External Bronze Table in Metastore
# Create an 'external' table definition in the Hive Metastore.
# This table definition points to the Delta files we just wrote to the DBFS path.
# 'External' means dropping the table metadata DOES NOT delete the underlying data files in DBFS.
# 'Managed' tables (default if LOCATION is omitted) store data in a default warehouse path,
# and dropping a managed table *does* delete the data. We prefer external tables for explicit path control.

print(f"\nCreating external table '{db_name}.{bronze_table_name}'...")
print(f"Table will point to DBFS LOCATION: '{bronze_path_dbfs}'")

# Construct the CREATE TABLE statement using Delta Lake syntax and the LOCATION clause.
# Using `CREATE TABLE IF NOT EXISTS` prevents errors if the table already exists from a previous run.
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {db_name}.{bronze_table_name}
USING DELTA
LOCATION '{bronze_path_dbfs}'
COMMENT 'External Bronze layer table for NYC Taxi Trips. Data stored in DBFS at {bronze_path_dbfs}'
"""

# Note: For Delta tables, the PARTITIONED BY clause in CREATE TABLE is not typically used
# as Delta automatically infers partitioning from the directory structure at the specified LOCATION.
# However, you *can* add TBLPROPERTIES for documentation or specific Delta features.
# Example: TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')

try:
    spark.sql(create_table_sql)
    print(f"\nSUCCESS: Table '{db_name}.{bronze_table_name}' created or already exists.")
except Exception as e:
    print(f"\nERROR creating external table '{db_name}.{bronze_table_name}': {e}")
    print(f"Troubleshooting:")
    print(f"  - Check if a table/view with the same name already exists with an incompatible definition.")
    print(f"  - Ensure you have CREATE TABLE privileges on the database '{db_name}'.")
    dbutils.notebook.exit("Failed to create external table.")

# Optional: Repair table (MSCK REPAIR TABLE) - Generally NOT needed for Delta tables created with LOCATION,
# as Delta manages its own file index via the transaction log (_delta_log).
# It doesn't hurt but is usually redundant for Delta.
# print("Running MSCK REPAIR TABLE (optional, usually not needed for Delta)...")
# spark.sql(f"MSCK REPAIR TABLE {db_name}.{bronze_table_name}")

# COMMAND ----------

# DBTITLE 1,Verify Bronze Table Access via SQL
# Final verification steps to ensure the table is registered correctly and queryable.

print(f"\n--- Verifying Bronze Table: {db_name}.{bronze_table_name} ---")

# 1. Describe Table Extended: Shows metadata, including the DBFS Location.
print("\n1. Describing table (Extended):")
display(spark.sql(f"DESCRIBE EXTENDED {db_name}.{bronze_table_name}"))
# Look for the 'Location' field in the output - it should match bronze_path_dbfs.

# 2. Describe Table Detail: Shows Delta-specific information (format, numFiles, size).
print("\n2. Describing table detail (Delta specific):")
display(spark.sql(f"DESCRIBE DETAIL {db_name}.{bronze_table_name}"))

# 3. Count Records: Query the table using its name to count records.
try:
    count_df = spark.sql(f"SELECT COUNT(*) as record_count FROM {db_name}.{bronze_table_name}")
    count_result = count_df.first()["record_count"]
    print(f"\n3. Record count via SQL query: {count_result}")
    assert count_result == record_count # Verify count matches earlier DataFrame count
except Exception as e:
    print(f"\nERROR counting records via SQL: {e}")

# 4. Show Sample Data: Query the table using its name.
print("\n4. Sample data via SQL query (limit 10):")
display(spark.sql(f"SELECT * FROM {db_name}.{bronze_table_name} LIMIT 10"))

# 5. Show Partitions (if partitioned): Verify the metastore recognizes the partitions.
if partition_columns:
    print("\n5. Showing partitions detected by metastore:")
    try:
        display(spark.sql(f"SHOW PARTITIONS {db_name}.{bronze_table_name}"))
    except Exception as e:
        print(f"\nERROR showing partitions: {e}") # Might happen if table created without partition info synced

print(f"\n--- Verification Complete for {db_name}.{bronze_table_name} ---")

# COMMAND ----------

