# Databricks notebook source
# MAGIC %md
# MAGIC # 06. Delta Live Tables (DLT) Concepts & Syntax (DBFS Context)
# MAGIC
# MAGIC **Purpose:** Introduce the concepts and syntax of Delta Live Tables (DLT), focusing on how it operates within an environment primarily using DBFS for storage. This notebook contains *examples* of DLT pipeline code (Python and SQL).
# MAGIC
# MAGIC **Important Note:** You **cannot run** a DLT pipeline directly from a standard interactive notebook like this one. DLT pipelines are defined in notebooks or Python/SQL scripts, but they are configured, executed, and monitored through the **Databricks Workflows UI** (`Workflows` -> `Delta Live Tables`).
# MAGIC
# MAGIC **DLT Key Concepts:**
# MAGIC * **Declarative Pipelines:** You define the *end state* of your data (the tables and their dependencies), and DLT automatically figures out the execution order, orchestration, error handling, and retries. This contrasts with imperative approaches where you explicitly define each step's execution.
# MAGIC * **Data Quality Expectations:** Define data quality rules directly in your pipeline code (e.g., `expect("col > 0", "Value must be positive")`). DLT tracks metrics for these expectations and allows you to configure actions on violations (e.g., allow, drop record, fail pipeline).
# MAGIC * **Automatic Orchestration & Scaling:** DLT manages the cluster lifecycle (creating, scaling, terminating job clusters) based on the pipeline's needs.
# MAGIC * **Simplified Development:** Handles common tasks like schema evolution, checkpointing, and backfills automatically.
# MAGIC * **Event Log:** Provides detailed, queryable logs of all pipeline events, including data quality metrics, lineage information, and performance diagnostics.
# MAGIC * **Incremental or Full Processing:** DLT automatically determines whether to perform incremental updates or full refreshes based on dependencies and data changes.
# MAGIC * **Python & SQL Interfaces:** Define pipelines using familiar Python decorators or SQL DDL extensions.
# MAGIC
# MAGIC **DLT and DBFS:**
# MAGIC * **Storage Location:** When you create a DLT pipeline, you can specify a **Storage Location** (a DBFS path or external cloud storage path). If omitted, DLT uses a default DBFS path (typically under `/pipelines/<pipeline_id>/`). All pipeline artifacts, including the data for managed DLT tables, checkpoints, system files, and event logs, reside in this location.
# MAGIC * **Reading from DBFS:** DLT pipelines can easily read source data from DBFS paths using Auto Loader (`cloud_files()` in SQL, `spark.readStream.format("cloudFiles")` in Python).
# MAGIC * **Writing to DBFS:** DLT writes output Delta tables to its designated storage location on DBFS. You can query these tables directly after the pipeline runs. You can also publish tables to a specific database (Hive Metastore or Unity Catalog if enabled).
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example DLT Pipeline using Python Decorators
# MAGIC
# MAGIC Below is an example demonstrating the Bronze -> Silver -> Gold flow using DLT Python syntax. You would typically save this code (starting from `import dlt`) in a **separate Python notebook or `.py` file** and then select that file when creating a DLT Pipeline in the UI.
# MAGIC
# MAGIC **DO NOT RUN THE CELLS BELOW IN THIS INTERACTIVE NOTEBOOK.**

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC # Example DLT Python Code - Save in a separate notebook/file for DLT Pipeline
# MAGIC
# MAGIC import dlt
# MAGIC from pyspark.sql.functions import *
# MAGIC from pyspark.sql.types import *
# MAGIC
# MAGIC # --- Configuration ---
# MAGIC # These paths would typically be passed via DLT Pipeline Settings JSON
# MAGIC # Example: spark.conf.get("conf.source_data_path", "/default/dbfs/path")
# MAGIC # We use spark.conf.get for demonstration; in DLT use spark.conf.get("conf.<key>")
# MAGIC
# MAGIC # Assume these are set in the DLT Pipeline Configuration UI:
# MAGIC # {
# MAGIC #   "conf.source_data_path": "dbfs:/Users/user@example.com/DE_Cert_Prep_Project_DBFS/landing/streaming_input",
# MAGIC #   "conf.checkpoint_base_path": "dbfs:/Users/user@example.com/DE_Cert_Prep_Project_DBFS/checkpoints"
# MAGIC # }
# MAGIC source_data_path = spark.conf.get("conf.source_data_path")
# MAGIC checkpoint_base_path = spark.conf.get("conf.checkpoint_base_path")
# MAGIC
# MAGIC # Define paths for DLT schema locations within the checkpoint area
# MAGIC bronze_schema_location = f"{checkpoint_base_path}/dlt_bronze_schema_py"
# MAGIC
# MAGIC # --- Bronze Layer (Reading from DBFS using Auto Loader) ---
# MAGIC
# MAGIC @dlt.table(
# MAGIC   name="dlt_bronze_taxi_trips_py",
# MAGIC   comment="Raw taxi trip data ingested from DBFS using Auto Loader (Python DLT).",
# MAGIC   table_properties={
# MAGIC     "quality": "bronze",
# MAGIC     "pipelines.autoOptimize.managed": "true" # Enable auto-optimize features
# MAGIC   }
# MAGIC   # path= "..." # Optional: Specify DBFS path for *managed* table data within DLT storage
# MAGIC )
# MAGIC def bronze_taxi_trips_py():
# MAGIC   """Ingests raw Parquet files from the configured DBFS source path."""
# MAGIC   return (
# MAGIC     spark.readStream.format("cloudFiles")
# MAGIC       .option("cloudFiles.format", "parquet")
# MAGIC       .option("cloudFiles.schemaLocation", bronze_schema_location) # Schema location on DBFS
# MAGIC       .option("cloudFiles.inferColumnTypes", "true")
# MAGIC       # Add other Auto Loader options if needed (e.g., schema hints)
# MAGIC       .load(source_data_path) # Read from the DBFS path specified in config
# MAGIC       .withColumn("dlt_ingestion_timestamp", current_timestamp())
# MAGIC       .withColumn("dlt_source_file", input_file_name())
# MAGIC   )
# MAGIC
# MAGIC # --- Silver Layer (Cleaning and Transformation with Expectations) ---
# MAGIC
# MAGIC @dlt.table(
# MAGIC   name="dlt_silver_taxi_trips_py",
# MAGIC   comment="Cleaned and validated taxi trip data (Python DLT).",
# MAGIC   table_properties={"quality": "silver"}
# MAGIC   # partition_cols=["pickup_year", "pickup_month"] # Optional partitioning
# MAGIC )
# MAGIC # Define Data Quality Rules using DLT Expectations
# MAGIC @dlt.expect_or_drop("valid_trip_distance", col("trip_distance") >= 0) # Drop rows with negative distance
# MAGIC @dlt.expect("valid_fare", col("fare_amount") >= 0) # Track violations if fare is negative, but keep row
# MAGIC @dlt.expect_all_or_fail({ # Fail pipeline if critical rules violated
# MAGIC     "valid_pickup_time": col("tpep_pickup_datetime").isNotNull(),
# MAGIC     "plausible_duration": col("trip_duration_minutes") < (24*60) # Duration < 1 day
# MAGIC })
# MAGIC def silver_taxi_trips_py():
# MAGIC   """Cleans Bronze data, applies quality rules, and derives features."""
# MAGIC   # Read from the upstream Bronze table (referenced by its function/table name)
# MAGIC   bronze_df = dlt.read_stream("dlt_bronze_taxi_trips_py")
# MAGIC
# MAGIC   # Apply transformations (similar logic to notebook 02, adapted for DLT)
# MAGIC   silver_transformed_df = bronze_df \
# MAGIC     .filter(col("tpep_pickup_datetime") <= col("tpep_dropoff_datetime")) \
# MAGIC     .withColumn("vendor_id", col("VendorID").cast("integer")) \
# MAGIC     .withColumn("passenger_count", coalesce(col("passenger_count").cast("integer"), lit(1))) \
# MAGIC     .withColumn("tip_amount", coalesce(col("tip_amount").cast("double"), lit(0.0))) \
# MAGIC     .withColumn("trip_duration_minutes", round((unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60, 2)) \
# MAGIC     .withColumn("pickup_year", year(col("tpep_pickup_datetime"))) \
# MAGIC     .withColumn("pickup_month", month(col("tpep_pickup_datetime"))) \
# MAGIC     .select( # Select and potentially rename columns for the Silver layer
# MAGIC         "vendor_id",
# MAGIC         col("tpep_pickup_datetime"),
# MAGIC         col("tpep_dropoff_datetime"),
# MAGIC         "passenger_count",
# MAGIC         col("trip_distance").cast("double"),
# MAGIC         col("PULocationID").alias("pickup_location_id"),
# MAGIC         col("DOLocationID").alias("dropoff_location_id"),
# MAGIC         col("fare_amount").cast("double"),
# MAGIC         "tip_amount",
# MAGIC         col("total_amount").cast("double"),
# MAGIC         "trip_duration_minutes",
# MAGIC         "pickup_year",
# MAGIC         "pickup_month",
# MAGIC         "dlt_ingestion_timestamp", # Carry forward relevant metadata if needed
# MAGIC         "dlt_source_file"
# MAGIC     )
# MAGIC   return silver_transformed_df
# MAGIC
# MAGIC # --- Gold Layer (Aggregation) ---
# MAGIC
# MAGIC @dlt.table(
# MAGIC   name="dlt_gold_daily_summary_py",
# MAGIC   comment="Aggregated daily taxi trip summary (Python DLT).",
# MAGIC   table_properties={"quality": "gold"}
# MAGIC )
# MAGIC def gold_daily_summary_py():
# MAGIC   """Calculates daily summary metrics from the Silver table."""
# MAGIC   # Read from the Silver table (use dlt.read() for batch-like processing from Silver)
# MAGIC   silver_df = dlt.read("dlt_silver_taxi_trips_py")
# MAGIC
# MAGIC   # Perform aggregation
# MAGIC   gold_df = silver_df.groupBy(
# MAGIC       date_trunc("day", col("tpep_pickup_datetime")).alias("trip_date")
# MAGIC     ).agg(
# MAGIC       count("*").alias("total_trips"),
# MAGIC       round(avg("trip_distance"), 2).alias("average_trip_distance"),
# MAGIC       round(sum("total_amount"), 2).alias("total_revenue")
# MAGIC     )
# MAGIC   return gold_df
# MAGIC
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example DLT Pipeline using SQL
# MAGIC
# MAGIC Below is the equivalent pipeline defined using DLT SQL syntax. You would save this in a **separate SQL notebook or `.sql` file** for use in a DLT Pipeline.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- Example DLT SQL Code - Save in a separate SQL notebook/file for DLT Pipeline
# MAGIC
# MAGIC -- --- Configuration (Set these in DLT Pipeline Settings UI JSON) ---
# MAGIC -- Example JSON Configuration:
# MAGIC -- {
# MAGIC --   "conf.source_data_path": "dbfs:/Users/user@example.com/DE_Cert_Prep_Project_DBFS/landing/streaming_input",
# MAGIC --   "conf.checkpoint_base_path": "dbfs:/Users/user@example.com/DE_Cert_Prep_Project_DBFS/checkpoints"
# MAGIC -- }
# MAGIC
# MAGIC -- --- Bronze Layer (Reading from DBFS using Auto Loader) ---
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE dlt_bronze_taxi_trips_sql (
# MAGIC   -- Optional: Define schema explicitly if needed, or let Auto Loader infer/evolve
# MAGIC   -- VendorID INT, tpep_pickup_datetime TIMESTAMP, ...
# MAGIC )
# MAGIC COMMENT "Raw taxi trip data ingested from DBFS using Auto Loader (SQL DLT)."
# MAGIC TBLPROPERTIES (
# MAGIC   "quality" = "bronze",
# MAGIC   "pipelines.autoOptimize.managed" = "true"
# MAGIC )
# MAGIC AS SELECT *, current_timestamp() AS dlt_ingestion_timestamp, input_file_name() AS dlt_source_file
# MAGIC FROM cloud_files(
# MAGIC   "${conf.source_data_path}", -- Use config variable for DBFS source path
# MAGIC   "parquet",                 -- Specify file format
# MAGIC   map(                       -- Map containing Auto Loader options
# MAGIC      "cloudFiles.schemaLocation", "${conf.checkpoint_base_path}/dlt_bronze_schema_sql", -- Schema location on DBFS
# MAGIC      "cloudFiles.inferColumnTypes", "true"
# MAGIC      -- Add other options like 'schemaHints' if necessary
# MAGIC      )
# MAGIC );
# MAGIC
# MAGIC -- --- Silver Layer (Cleaning and Transformation with Constraints) ---
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE dlt_silver_taxi_trips_sql (
# MAGIC   -- Define Data Quality Rules using SQL Constraints
# MAGIC   CONSTRAINT valid_trip_distance EXPECT (trip_distance >= 0) ON VIOLATION DROP ROW, -- Drop invalid rows
# MAGIC   CONSTRAINT valid_fare EXPECT (fare_amount >= 0), -- Keep row, track violation count
# MAGIC   CONSTRAINT valid_pickup_time EXPECT (tpep_pickup_datetime IS NOT NULL) ON VIOLATION FAIL UPDATE, -- Fail pipeline
# MAGIC   CONSTRAINT plausible_duration EXPECT ( (unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) < (24*60*60) )
# MAGIC
# MAGIC   -- Optional: Define schema explicitly for Silver table
# MAGIC   -- vendor_id INT, tpep_pickup_datetime TIMESTAMP, ... pickup_year INT, pickup_month INT
# MAGIC )
# MAGIC PARTITIONED BY (pickup_year, pickup_month) -- Optional partitioning
# MAGIC COMMENT "Cleaned and validated taxi trip data (SQL DLT)."
# MAGIC TBLPROPERTIES ("quality" = "silver")
# MAGIC AS SELECT
# MAGIC   CAST(VendorID AS INT) AS vendor_id,
# MAGIC   tpep_pickup_datetime,
# MAGIC   tpep_dropoff_datetime,
# MAGIC   COALESCE(CAST(passenger_count AS INT), 1) AS passenger_count,
# MAGIC   CAST(trip_distance AS DOUBLE) AS trip_distance,
# MAGIC   PULocationID AS pickup_location_id,
# MAGIC   DOLocationID AS dropoff_location_id,
# MAGIC   CAST(fare_amount AS DOUBLE) AS fare_amount,
# MAGIC   COALESCE(CAST(tip_amount AS DOUBLE), 0.0) AS tip_amount,
# MAGIC   CAST(total_amount AS DOUBLE) AS total_amount,
# MAGIC   -- Calculate trip duration in minutes
# MAGIC   round((unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 60, 2) AS trip_duration_minutes,
# MAGIC   -- Add partitioning columns
# MAGIC   year(tpep_pickup_datetime) AS pickup_year,
# MAGIC   month(tpep_pickup_datetime) AS pickup_month,
# MAGIC   dlt_ingestion_timestamp, -- Carry forward metadata if needed
# MAGIC   dlt_source_file
# MAGIC FROM STREAM(LIVE.dlt_bronze_taxi_trips_sql) -- Read incrementally from the upstream Bronze LIVE table
# MAGIC WHERE tpep_pickup_datetime <= tpep_dropoff_datetime; -- Basic filtering (additional handled by constraints)
# MAGIC
# MAGIC
# MAGIC -- --- Gold Layer (Aggregation) ---
# MAGIC
# MAGIC CREATE OR REFRESH LIVE TABLE dlt_gold_daily_summary_sql
# MAGIC COMMENT "Aggregated daily taxi trip summary (SQL DLT)."
# MAGIC TBLPROPERTIES ("quality" = "gold")
# MAGIC AS SELECT
# MAGIC   date_trunc('DAY', tpep_pickup_datetime) AS trip_date,
# MAGIC   count(*) AS total_trips,
# MAGIC   round(avg(trip_distance), 2) AS average_trip_distance,
# MAGIC   round(sum(total_amount), 2) AS total_revenue
# MAGIC FROM LIVE.dlt_silver_taxi_trips_sql -- Read (batch) from the upstream Silver LIVE table
# MAGIC GROUP BY trip_date;
# MAGIC
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating and Running a DLT Pipeline in the UI
# MAGIC
# MAGIC 1.  **Save Code:** Save one of the examples above (either Python or SQL) as a notebook or a `.py`/`.sql` file in your Databricks Workspace (e.g., within your `DE_Cert_Prep_Project_DBFS` folder).
# MAGIC 2.  **Go to Workflows:** Navigate to the `Workflows` section in the left-hand sidebar of Databricks.
# MAGIC 3.  **Select Delta Live Tables:** Click on the `Delta Live Tables` tab.
# MAGIC 4.  **Create Pipeline:** Click `Create Pipeline`.
# MAGIC 5.  **Configure Pipeline Settings:**
# MAGIC     * **Pipeline name:** Give your pipeline a descriptive name (e.g., `de_cert_dlt_pipeline_dbfs`).
# MAGIC     * **Product Edition:** Choose `Advanced` or `Pro` to use features like Expectations.
# MAGIC     * **Pipeline Mode:** Select `Triggered` (runs once when started) or `Continuous`. `Triggered` is often sufficient for batch-like updates.
# MAGIC     * **Notebook Libraries:** Under `Paths`, browse and select the notebook or file you saved in Step 1.
# MAGIC     * **Storage location:** (Optional but Recommended) Specify a DBFS path where DLT will store all its data (tables, checkpoints, logs). Example: `dbfs:/Users/your_email@domain.com/DE_Cert_Prep_Project_DBFS/dlt_storage`. If blank, a default path under `/pipelines/` will be used.
# MAGIC     * **Target:** (Optional) Specify a database (schema) name (e.g., `your_dlt_output_db`) where the defined DLT tables (`dlt_bronze_...`, `dlt_silver_...`, `dlt_gold_...`) will be published in the Hive Metastore for easy querying after the pipeline runs.
# MAGIC     * **Compute:** Configure cluster settings.
# MAGIC         * You can often leave Policy as default.
# MAGIC         * Choose worker/driver types.
# MAGIC         * Enable **Autoscaling**.
# MAGIC         * **Important:** DLT manages its own job cluster(s). These settings define that cluster.
# MAGIC     * **Configuration:** Add **Key-Value pairs** needed by your code. This is where you pass DBFS paths.
# MAGIC         * Click `Add configuration`.
# MAGIC         * **Key:** `conf.source_data_path` -> **Value:** `dbfs:/Users/your_email@domain.com/DE_Cert_Prep_Project_DBFS/landing/streaming_input` (Use the actual path from your setup!)
# MAGIC         * **Key:** `conf.checkpoint_base_path` -> **Value:** `dbfs:/Users/your_email@domain.com/DE_Cert_Prep_Project_DBFS/checkpoints` (Use the actual path!)
# MAGIC     * **Channel:** (Optional) Select `Current` (recommended) or `Preview`.
# MAGIC 6.  **Start:** Click `Start` to initiate the pipeline run.
# MAGIC 7.  **Monitor:** Observe the pipeline graph build in the DLT UI. You can see data flowing, monitor processing steps, and check data quality results. Once complete (for Triggered mode), you can query the target tables (if published) or explore the data in the specified DLT storage location on DBFS.
# MAGIC