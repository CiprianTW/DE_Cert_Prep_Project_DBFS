# Databricks notebook source
# MAGIC %md
# MAGIC # 00. Setup Project Environment and Ingest Data to DBFS
# MAGIC
# MAGIC **Purpose:**
# MAGIC 1.  Define unique DBFS paths for this project within your user directory to avoid conflicts.
# MAGIC 2.  Define a unique database name for this project.
# MAGIC 3.  Provide clear instructions to upload the source data file (e.g., `yellow_tripdata_2023_01.parquet`) directly to DBFS using the Databricks UI.
# MAGIC 4.  Move the uploaded data from its initial upload location to a designated 'landing' zone within the project's DBFS structure.
# MAGIC 5.  Create the database in the metastore.

# COMMAND ----------

# DBTITLE 1,Define Project Configuration (DBFS Paths & Database)
import re
import time
import json # Import json library

# Get the current user's email or username to create isolated paths
try:
    user_name_raw = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
except Exception as e:
    print(f"Warning: Could not get username automatically ({e}). Using a default.")
    user_name_raw = f"default_user_{int(time.time())}"

user_name_safe = re.sub(r'[^a-zA-Z0-9_]', '_', user_name_raw)
project_base_dbfs = f"dbfs:/Users/{user_name_raw}/DE_Cert_Prep_Project_DBFS"
# Define a unique prefix for Spark configuration settings related to this project
# This will also be used as the base for keys in our JSON config file
project_config_prefix_nosprk = f"project.de_cert_prep_dbfs.{user_name_safe}"

db_name = f"de_cert_prep_dbfs_{user_name_safe}"
landing_path_dbfs = f"{project_base_dbfs}/landing"
bronze_path_dbfs = f"{project_base_dbfs}/delta/bronze"
silver_path_dbfs = f"{project_base_dbfs}/delta/silver"
gold_path_dbfs_base = f"{project_base_dbfs}/delta/gold"
checkpoint_base_path_dbfs = f"{project_base_dbfs}/checkpoints"
streaming_source_dir_dbfs = f"{landing_path_dbfs}/streaming_input"
raw_data_file = "yellow_tripdata_2023_01.parquet"
config_file_path_dbfs = f"{project_base_dbfs}/project_config.json" # Path for the config JSON file

print(f"--- Project Configuration ---")
print(f"Username (Raw):       {user_name_raw}")
print(f"Username (Safe):      {user_name_safe}")
print(f"Project DBFS Path:    {project_base_dbfs}")
print(f"Config JSON Path:     {config_file_path_dbfs}") # Print new config file path
print(f"Database Name:        {db_name}")
# ... (rest of the print statements from before) ...
print(f"Landing Zone (DBFS):  {landing_path_dbfs}")
print(f"Bronze Path (DBFS):   {bronze_path_dbfs}")
print(f"Silver Path (DBFS):   {silver_path_dbfs}")
print(f"Gold Base Path (DBFS):{gold_path_dbfs_base}")
print(f"Checkpoints (DBFS):   {checkpoint_base_path_dbfs}")
print(f"Streaming Src (DBFS): {streaming_source_dir_dbfs}")
print(f"Raw Data Filename:    {raw_data_file}")
print(f"Key Prefix for JSON:  {project_config_prefix_nosprk}")


# Store configuration values in a dictionary to be saved as JSON
project_configs = {
    f"{project_config_prefix_nosprk}.db_name": db_name,
    f"{project_config_prefix_nosprk}.project_base_dbfs": project_base_dbfs,
    f"{project_config_prefix_nosprk}.landing_path_dbfs": landing_path_dbfs,
    f"{project_config_prefix_nosprk}.bronze_path_dbfs": bronze_path_dbfs,
    f"{project_config_prefix_nosprk}.silver_path_dbfs": silver_path_dbfs,
    f"{project_config_prefix_nosprk}.gold_path_dbfs_base": gold_path_dbfs_base,
    f"{project_config_prefix_nosprk}.checkpoint_base_path_dbfs": checkpoint_base_path_dbfs,
    f"{project_config_prefix_nosprk}.streaming_source_dir_dbfs": streaming_source_dir_dbfs,
    f"{project_config_prefix_nosprk}.raw_data_file": raw_data_file,
    f"{project_config_prefix_nosprk}.user_name_safe": user_name_safe, # Keep this for potential use
    f"{project_config_prefix_nosprk}.config_file_path_dbfs": config_file_path_dbfs # Self-reference
}

# Set in Spark conf for this session (optional now, but doesn't hurt)
for key, value in project_configs.items():
    spark.conf.set(key, str(value)) # Ensure value is string for spark.conf
print("\n(Optional) Set configs in current Spark session.")

# COMMAND ----------

# DBTITLE 1,Create Project Directories in DBFS
print(f"Creating project directories under {project_base_dbfs}...")
dbutils.fs.mkdirs(landing_path_dbfs)
dbutils.fs.mkdirs(bronze_path_dbfs)
dbutils.fs.mkdirs(silver_path_dbfs)
dbutils.fs.mkdirs(gold_path_dbfs_base)
dbutils.fs.mkdirs(checkpoint_base_path_dbfs)
dbutils.fs.mkdirs(streaming_source_dir_dbfs)
print(f"Project directories created (or already existed).")
display(dbutils.fs.ls(project_base_dbfs))

# COMMAND ----------

# MAGIC %md
# MAGIC Data Upload Instructions (DBFS UI) - CRITICAL STEP
# MAGIC
# MAGIC ## IMPORTANT: Upload Source Data to DBFS
# MAGIC
# MAGIC **Action Required by You:**
# MAGIC
# MAGIC 1.  **Download Data:**
# MAGIC      * Go to the [NYC TLC Trip Record Data website](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
# MAGIC      * Find the "Yellow Taxi Trip Records" section.
# MAGIC      * Download the **Parquet** file for a specific month (e.g., January 2023). The filename should be similar to `yellow_tripdata_2023_01.parquet`. **Using Parquet is strongly recommended** over CSV for performance with Spark.
# MAGIC
# MAGIC 2.  **Upload to DBFS using Databricks UI:**
# MAGIC      * In the Databricks workspace UI, navigate to **Data** in the left sidebar.
# MAGIC      * Click the **DBFS** button near the top (or look for **Browse DBFS** or similar).
# MAGIC      * Navigate to a temporary upload location. A common place is `/FileStore/tables/`. If this path doesn't exist, you might need to use the `Upload Data` button first and select `DBFS` as the target to create the initial folder structure.
# MAGIC      * Inside your chosen temporary location (e.g., `dbfs:/FileStore/tables/`), click the **Upload** button.
# MAGIC      * Select the `.parquet` file you downloaded in Step 1 and upload it.
# MAGIC
# MAGIC 3.  **Verify Upload Location:**
# MAGIC      * Make a note of the **exact DBFS path** where the file was uploaded (e.g., `dbfs:/FileStore/tables/yellow_tripdata_2023_01.parquet`). You will need this exact path for the next command cell.
# MAGIC
# MAGIC **The next cell will move the file from this temporary location to the project's designated landing zone.**

# COMMAND ----------

# DBTITLE 1,Move Uploaded File to Project Landing Zone
# --- !!! ACTION REQUIRED: Verify and potentially modify the SOURCE path below !!! ---
uploaded_file_dbfs_path = f"dbfs:/FileStore/tables/{raw_data_file}"
target_landing_file_path = f"{landing_path_dbfs}/{raw_data_file}"

print(f"Attempting to move file...")
print(f"  Source:      {uploaded_file_dbfs_path}")
print(f"  Destination: {target_landing_file_path}")
try:
    dbutils.fs.ls(target_landing_file_path)
    print(f"\nINFO: File already exists at the target location: {target_landing_file_path}.")
except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
        print(f"\nFile not found at target. Proceeding with move...")
        try:
            dbutils.fs.mv(uploaded_file_dbfs_path, target_landing_file_path)
            print(f"\nSUCCESS: Moved file from '{uploaded_file_dbfs_path}' to '{target_landing_file_path}'")
        except Exception as move_e:
            print(f"\nERROR moving file: {move_e}")
            # dbutils.notebook.exit("Failed to move raw data file to landing zone.")
    else:
        print(f"\nERROR checking target file path: {e}")
        # dbutils.notebook.exit("Failed to check target file path.")

# COMMAND ----------

# DBTITLE 1,Verify Data File in Project Landing Zone
print(f"Verifying file presence in project landing zone: {target_landing_file_path}")
try:
    file_info = dbutils.fs.ls(target_landing_file_path)
    print(f"SUCCESS: File '{raw_data_file}' found in project landing zone.")
except Exception as e:
    print(f"ERROR: File '{raw_data_file}' not found in the project landing zone ({landing_path_dbfs}).")
    dbutils.notebook.exit("Raw data file not found in the designated project landing zone.")

# COMMAND ----------

# DBTITLE 1,Write Configuration to JSON File on DBFS
# Since we are using a Min Cluster Policy, we need to manually set the configuration for the project in a json file instead of spark config.
print(f"\nWriting project configuration to DBFS file: {config_file_path_dbfs}")
try:
    # Convert the project_configs dictionary to a JSON string
    config_json_string = json.dumps(project_configs, indent=4)

    # Use dbutils.fs.put() to write the string to the DBFS file, overwriting if it exists
    dbutils.fs.put(config_file_path_dbfs, config_json_string, overwrite=True)
    print(f"SUCCESS: Configuration written to {config_file_path_dbfs}")

    # Verify by reading it back (optional)
    print("\nVerifying written config file content (first 500 chars):")
    read_config_string = dbutils.fs.head(config_file_path_dbfs, 500)
    print(read_config_string)

except Exception as e:
    print(f"ERROR writing configuration file to DBFS: {e}")
    dbutils.notebook.exit("Failed to persist project configuration.")

# COMMAND ----------

# DBTITLE 1,Create Project Database
# Create the database defined earlier if it doesn't already exist.
# This database will store the metadata (table definitions) for our project tables.
print(f"\nAttempting to create database '{db_name}' if it doesn't exist...")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} COMMENT 'Database for DBFS-based DE Cert Prep Project: {user_name_raw}'")
print(f"Database '{db_name}' created or already exists.")

# Switch the current session's active database to our project database.
# This means subsequent SQL commands (like CREATE TABLE) without a database prefix
# will operate within this database.
spark.sql(f"USE {db_name}")
print(f"Set current database context to '{db_name}'.")

# COMMAND ----------

# MAGIC %md
# MAGIC Cleanup (Optional - Run to Reset Entire Project)
# MAGIC
# MAGIC ### Optional Cleanup Cell
# MAGIC
# MAGIC **WARNING:** Running the following cell will:
# MAGIC 1.  Drop the database (`{db_name}`) created for this project, removing all associated table definitions from the metastore.
# MAGIC 2.  Recursively delete the entire project directory (`{project_base_dbfs}`) from DBFS, including all landing data, Delta tables (Bronze, Silver, Gold), checkpoints, etc.
# MAGIC
# MAGIC

# COMMAND ----------

# # Set this flag to True and run the cell to perform cleanup.
# # KEEP IT FALSE unless you are absolutely sure you want to delete everything.
perform_cleanup = False

if perform_cleanup:
    print(f"--- WARNING: CLEANUP ENABLED ---")
    print(f"This will permanently delete:")
    print(f"  - Database: '{db_name}' (and all its tables)")
    print(f"  - DBFS Directory: '{project_base_dbfs}' (and all its contents)")

    # Add a final confirmation step to prevent accidental deletion
    confirm = input("Type 'DELETE MY PROJECT' exactly to confirm: ")

    if confirm == "DELETE MY PROJECT":
        print(f"\nProceeding with cleanup...")

        # Drop the database
        try:
            print(f"Dropping database {db_name}...")
            spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
            print(f"Database {db_name} dropped.")
        except Exception as e:
            print(f"Error dropping database {db_name}: {e}")

        # Remove the DBFS directory
        try:
            print(f"Recursively removing DBFS directory {project_base_dbfs}...")
            dbutils.fs.rm(project_base_dbfs, recurse=True)
            print(f"DBFS directory {project_base_dbfs} removed.")
        except Exception as e:
            print(f"Error removing DBFS directory {project_base_dbfs}: {e}")

        print("\nCleanup complete.")
    else:
        print("\nCleanup cancelled. Confirmation phrase did not match.")
else:
    print("Cleanup is disabled. To enable, set 'perform_cleanup = True' in the cell above and rerun.")
