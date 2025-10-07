# Databricks notebook source
# MAGIC %md
# MAGIC # 02. Silver Layer - Cleansing and Transformation (DBFS)
# MAGIC
# MAGIC **Purpose:** Read data from the Bronze Delta table (via its table name, which resolves to the Bronze DBFS `LOCATION`), apply data quality rules, cleaning steps, and transformations, then write the enriched data to the Silver DBFS path as a new Delta table. Create an unmanaged (external) Silver table pointing to this DBFS location.
# MAGIC
# MAGIC **Silver Layer Characteristics:**
# MAGIC * Data is cleaned (nulls handled, types corrected, invalid records filtered).
# MAGIC * Data is transformed (columns renamed, features derived like trip duration).
# MAGIC * Schema is more enforced and consistent than Bronze.
# MAGIC * Still typically granular, but ready for more complex analysis or aggregation.
# MAGIC
# MAGIC **DBFS Focus:**
# MAGIC * Reading from the Bronze table (defined in the previous notebook).
# MAGIC * Applying transformations using Spark DataFrame API.
# MAGIC * Writing cleaned Delta files to a specific `dbfs:/.../delta/silver/` path.
# MAGIC * Creating an **external** table `silver_taxi_trips` using the `LOCATION` clause pointing to the Silver DBFS path.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC * `00_Setup_Data_DBFS` and `01_Ingestion_Bronze_DBFS` notebooks have been run successfully.
# MAGIC * The `bronze_taxi_trips` table exists in the specified database.

# COMMAND ----------

# MAGIC %run ../DE_Cert_Prep_Project_DBFS/_Helper_Config_DBFS

# COMMAND ----------

# DBTITLE 1,Load Configuration from Spark Conf
from pyspark.sql.functions import col, unix_timestamp, round, expr, coalesce, lit, year, month, when # Import necessary functions

# Retrieve necessary configuration
try:
    db_name = get_config_value("db_name")
    # Bronze table name is fixed based on the previous notebook
    bronze_table_name = "bronze_taxi_trips"
    silver_path_dbfs = get_config_value("silver_path_dbfs")
    # Define Silver table name
    silver_table_name = "silver_taxi_trips"
except ValueError as e:
    print(f"Configuration Error: {e}")
    dbutils.notebook.exit("Failed to load configuration.")

# Set the current database context
spark.sql(f"USE {db_name}")

print(f"--- Configuration Loaded ---")
print(f"Database Name:        {db_name}")
print(f"Reading From Table:   {db_name}.{bronze_table_name}")
print(f"Writing To (DBFS):    {silver_path_dbfs}")
print(f"Silver Table Name:    {silver_table_name}")

# COMMAND ----------

# DBTITLE 1,Read from Bronze Delta Table (via Table Name)
print(f"Reading data from Bronze table: '{db_name}.{bronze_table_name}'...")
try:
    # Read data using the table name registered in the metastore.
    # Spark resolves this to the DBFS LOCATION specified when the Bronze table was created.
    bronze_df = spark.table(f"{db_name}.{bronze_table_name}")

    # Verify read operation
    bronze_count = bronze_df.count()
    print(f"Successfully read {bronze_count} records from Bronze table.")

    # Display schema to understand input columns
    print("\nBronze Table Schema:")
    bronze_df.printSchema()

except Exception as e:
    print(f"\nERROR reading Bronze table '{db_name}.{bronze_table_name}': {e}")
    print(f"Troubleshooting:")
    print(f"  - Ensure '01_Ingestion_Bronze_DBFS' notebook ran successfully and created the table.")
    print(f"  - Verify the table exists using `SHOW TABLES IN {db_name};` in a SQL cell.")
    dbutils.notebook.exit("Failed to read Bronze table.")

# COMMAND ----------

# DBTITLE 1,Data Cleaning and Transformation Logic
print("Applying cleaning and transformations for Silver layer...")

# 1. Select relevant columns (or drop unnecessary ones)
# Often drop metadata columns added during ingestion if not needed downstream
silver_df_selected = bronze_df.drop("source_file", "ingestion_timestamp")

# 2. Correct Data Types
# Ensure timestamps are TimestampType, numerical columns are appropriate numeric types (Double, Integer, etc.)
# Casting is crucial for accurate calculations and compatibility with BI tools.
silver_df_typed = silver_df_selected \
    .withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast("timestamp")) \
    .withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast("timestamp")) \
    .withColumn("passenger_count", col("passenger_count").cast("integer")) \
    .withColumn("trip_distance", col("trip_distance").cast("double")) \
    .withColumn("RatecodeID", col("RatecodeID").cast("integer")) \
    .withColumn("PULocationID", col("PULocationID").cast("integer")) \
    .withColumn("DOLocationID", col("DOLocationID").cast("integer")) \
    .withColumn("fare_amount", col("fare_amount").cast("double")) \
    .withColumn("extra", col("extra").cast("double")) \
    .withColumn("mta_tax", col("mta_tax").cast("double")) \
    .withColumn("tip_amount", col("tip_amount").cast("double")) \
    .withColumn("tolls_amount", col("tolls_amount").cast("double")) \
    .withColumn("improvement_surcharge", col("improvement_surcharge").cast("double")) \
    .withColumn("total_amount", col("total_amount").cast("double")) \
    .withColumn("congestion_surcharge", col("congestion_surcharge").cast("double")) \
    .withColumn("airport_fee", col("airport_fee").cast("double")) \
    .withColumn("VendorID", col("VendorID").cast("integer")) # Keep original partition columns type if they exist
    # Note: pickup_year, pickup_month were likely created as INT in Bronze, keep them as INT

print("Step 2/6: Corrected data types.")

# 3. Handle Nulls / Missing Values
# Strategies: Fill with default values (0, "Unknown", mean), or filter rows with critical nulls.
silver_df_filled = silver_df_typed \
    .na.fill({"passenger_count": 1}) \
    .na.fill({"tip_amount": 0.0}) \
    .na.fill({"RatecodeID": 99}) \
    .na.fill({"congestion_surcharge": 0.0}) \
    .na.fill({"airport_fee": 0.0}) \
    # Add more fill logic as needed based on data analysis

print("Step 3/6: Handled null values.")

# 4. Filter Invalid Data (Apply Business Rules)
# Remove records that don't make sense based on domain knowledge.
silver_df_filtered = silver_df_filled.filter(
    (col("trip_distance") >= 0) & \
    (col("fare_amount") >= 0) & \
    (col("total_amount") >= 0) & \
    (col("passenger_count") > 0) & \
    # Ensure pickup time is not after dropoff time
    (col("tpep_pickup_datetime") <= col("tpep_dropoff_datetime")) & \
    # Filter out trips that are excessively long or short (e.g., > 24 hours or < 10 seconds)
    ((unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) > 10) & \
    ((unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) < (24 * 60 * 60))
)
print("Step 4/6: Filtered invalid records based on business rules.")

# 5. Rename Columns (Improve Clarity / Conform to Standards)
# Use more descriptive or standardized names.
silver_df_renamed = silver_df_filtered \
    .withColumnRenamed("VendorID", "vendor_id") \
    .withColumnRenamed("RatecodeID", "rate_code_id") \
    .withColumnRenamed("PULocationID", "pickup_location_id") \
    .withColumnRenamed("DOLocationID", "dropoff_location_id")
print("Step 5/6: Renamed columns for clarity.")

# 6. Derive New Features (Enrichment)
# Calculate useful metrics not present in the source data.
silver_df_enriched = silver_df_renamed.withColumn(
    "trip_duration_seconds",
    unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")
)
silver_df_enriched = silver_df_enriched.withColumn(
    "trip_duration_minutes",
    round(col("trip_duration_seconds") / 60, 2) # Calculate minutes from seconds
)
# Calculate average speed, handling potential division by zero (if duration is very short)
silver_df_enriched = silver_df_enriched.withColumn(
    "average_speed_mph",
    when(col("trip_duration_seconds") > 0, # Avoid division by zero
         round(col("trip_distance") / (col("trip_duration_seconds") / 3600), 2) # distance / (duration_in_hours)
        ).otherwise(0.0) # Assign 0 speed if duration is zero or negative (should be filtered already)
)
print("Step 6/6: Derived new features (trip duration, average speed).")

# Assign the final transformed DataFrame
silver_df_final = silver_df_enriched

# Verify the transformations
final_silver_count = silver_df_final.count()
print(f"\nTransformations complete. Record count changed from {bronze_count} (Bronze) to {final_silver_count} (Silver).")
print("\nFinal Silver DataFrame Schema:")
silver_df_final.printSchema()
print("\nSample Transformed Data (limit 10):")
display(silver_df_final.limit(10))

# COMMAND ----------

# DBTITLE 1,Determine Silver Partitioning Strategy
# Decide how to partition the Silver table. Often, inheriting the Bronze partitioning is suitable,
# but you might choose different columns based on common query patterns for the Silver layer.
silver_partition_columns = []

# Check if the partitioning columns from Bronze still exist in the final Silver DataFrame
bronze_partition_cols_exist = all(col_name in silver_df_final.columns for col_name in ["pickup_year", "pickup_month"])

if bronze_partition_cols_exist:
    silver_partition_columns = ["pickup_year", "pickup_month"]
    print(f"Silver table will be partitioned by inherited columns: {silver_partition_columns}")
else:
    # If Bronze wasn't partitioned or columns were dropped, decide on new partitioning or none.
    # Example: Partition by pickup_date if needed, but requires deriving it first.
    # For this example, we'll assume inheriting or none.
    print("Silver table will not be partitioned (or Bronze partition columns were dropped).")

# COMMAND ----------

# DBTITLE 1,Write to Silver Delta Path on DBFS
print(f"\nCreating external table '{db_name}.{silver_table_name}'...")
print(f"Table will point to DBFS LOCATION: '{silver_path_dbfs}'")

# Construct the CREATE TABLE statement
# For external Delta tables where data already exists at the LOCATION,
# explicitly defining partition columns in TBLPROPERTIES can sometimes cause conflicts
# if it doesn't perfectly match what Delta has already inferred from the directory structure.
# It's often safer to omit it and let Delta handle partition discovery.
silver_create_sql = f"""
CREATE TABLE IF NOT EXISTS {db_name}.{silver_table_name}
USING DELTA
LOCATION '{silver_path_dbfs}'
COMMENT 'External Silver layer table for cleaned NYC Taxi Trips. Data stored in DBFS at {silver_path_dbfs}'
"""

# The following lines for TBLPROPERTIES are REMOVED/COMMENTED OUT to resolve the error:
# if silver_partition_columns:
#     partition_spec_str = ", ".join(silver_partition_columns)
#     silver_create_sql += f"\nTBLPROPERTIES ('partition_columns' = '{partition_spec_str}')"

try:
    spark.sql(silver_create_sql)
    print(f"\nSUCCESS: Table '{db_name}.{silver_table_name}' created or already exists.")
except Exception as e:
    print(f"\nERROR creating external Silver table '{db_name}.{silver_table_name}': {e}")
    dbutils.notebook.exit("Failed to create external Silver table.")

# COMMAND ----------

# DBTITLE 1,Create External Silver Table in Metastore
# Create the external table definition in the metastore for the Silver layer.
# This table points to the Delta files just written to the Silver DBFS path.

print(f"\nCreating external table '{db_name}.{silver_table_name}'...")
print(f"Table will point to DBFS LOCATION: '{silver_path_dbfs}'")

# Construct the CREATE TABLE statement
silver_create_sql = f"""
CREATE TABLE IF NOT EXISTS {db_name}.{silver_table_name}
USING DELTA
LOCATION '{silver_path_dbfs}'
COMMENT 'External Silver layer table for cleaned NYC Taxi Trips. Data stored in DBFS at {silver_path_dbfs}'
"""

# Optional: Add TBLPROPERTIES for documentation if partitioned
if silver_partition_columns:
    partition_spec_str = ", ".join(silver_partition_columns)
    silver_create_sql += f"\nTBLPROPERTIES ('partition_columns' = '{partition_spec_str}')"

# Execute the CREATE TABLE command
try:
    spark.sql(silver_create_sql)
    print(f"\nSUCCESS: Table '{db_name}.{silver_table_name}' created or already exists.")
except Exception as e:
    print(f"\nERROR creating external Silver table '{db_name}.{silver_table_name}': {e}")
    dbutils.notebook.exit("Failed to create external Silver table.")

# COMMAND ----------

# DBTITLE 1,Verify Silver Table Access via SQL
# Perform checks to ensure the Silver table is correctly defined and accessible.

print(f"\n--- Verifying Silver Table: {db_name}.{silver_table_name} ---")

# 1. Describe Table Extended: Check metadata and Location.
print("\n1. Describing table (Extended):")
display(spark.sql(f"DESCRIBE EXTENDED {db_name}.{silver_table_name}"))

# 2. Describe Table Detail: Check Delta-specific properties.
print("\n2. Describing table detail (Delta specific):")
display(spark.sql(f"DESCRIBE DETAIL {db_name}.{silver_table_name}"))

# 3. Count Records: Ensure count matches the DataFrame count after transformations.
try:
    count_df_sql = spark.sql(f"SELECT COUNT(*) as record_count FROM {db_name}.{silver_table_name}")
    count_result_sql = count_df_sql.first()["record_count"]
    print(f"\n3. Record count via SQL query: {count_result_sql}")
    assert count_result_sql == final_silver_count
except Exception as e:
    print(f"\nERROR counting records via SQL or count mismatch: {e}")

# 4. Show Sample Data: Query the table via SQL.
print("\n4. Sample data via SQL query (limit 10):")
display(spark.sql(f"SELECT * FROM {db_name}.{silver_table_name} LIMIT 10"))

# 5. Show Partitions (if partitioned):
if silver_partition_columns:
    print("\n5. Showing partitions detected by metastore:")
    try:
        display(spark.sql(f"SHOW PARTITIONS {db_name}.{silver_table_name}"))
    except Exception as e:
        print(f"\nERROR showing partitions: {e}")

print(f"\n--- Verification Complete for {db_name}.{silver_table_name} ---")