# Databricks notebook source
# MAGIC %md
# MAGIC # 10. Databricks Utilities for DBFS (`dbutils.fs`)
# MAGIC
# MAGIC **Purpose:** Demonstrate common and useful `dbutils.fs` commands for interacting with the Databricks File System (DBFS) directly from a notebook. This is particularly relevant for managing the files and directories created throughout this DBFS-based project.
# MAGIC
# MAGIC **Key `dbutils.fs` Commands:**
# MAGIC * `ls(path)`: Lists the contents of a directory.
# MAGIC * `mkdirs(path)`: Creates directories (including parent directories if needed).
# MAGIC * `cp(src_path, dest_path, recurse=False)`: Copies a file or directory.
# MAGIC * `mv(src_path, dest_path, recurse=False)`: Moves (renames) a file or directory.
# MAGIC * `put(path, contents, overwrite=False)`: Writes a string to a file on DBFS.
# MAGIC * `head(path, max_bytes=65536)`: Reads the first few bytes of a file. Useful for peeking into text files.
# MAGIC * `rm(path, recurse=False)`: Removes a file or directory.
# MAGIC
# MAGIC **DBFS Focus:**
# MAGIC * All examples operate on the DBFS paths defined and used within this project (`dbfs:/Users/.../DE_Cert_Prep_Project_DBFS/...`).
# MAGIC * Demonstrates practical file system management tasks relevant to data engineering workflows.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC * Notebook `00_Setup_Data_DBFS` has been run successfully to define paths and create the basic directory structure.
# MAGIC * Other notebooks (01-03) might need to have run to populate directories like `bronze`, `silver`, `gold` for `ls` examples.
# MAGIC

# COMMAND ----------

# MAGIC %run ../DE_Cert_Prep_Project_DBFS/_Helper_Config_DBFS

# COMMAND ----------

# DBTITLE 1,Load Configuration from Spark Conf
import re

# Retrieve necessary configuration
try:
    db_name = get_config_value("db_name")
    project_base_dbfs = get_config_value("project_base_dbfs")
    landing_path_dbfs = get_config_value("landing_path_dbfs")
    bronze_path_dbfs = get_config_value("bronze_path_dbfs")
    silver_path_dbfs = get_config_value("silver_path_dbfs")
    gold_path_dbfs_base = get_config_value("gold_path_dbfs_base")
    raw_data_file = get_config_value("raw_data_file")
except ValueError as e:
    print(f"Configuration Error: {e}")
    dbutils.notebook.exit("Failed to load configuration.")

# Set the current database context (though not strictly needed for dbutils.fs)
spark.sql(f"USE {db_name}")

print(f"--- Configuration Loaded ---")
print(f"Project Base Path (DBFS): {project_base_dbfs}")
print(f"Landing Path (DBFS):      {landing_path_dbfs}")
print(f"Bronze Path (DBFS):       {bronze_path_dbfs}")
print(f"Silver Path (DBFS):       {silver_path_dbfs}")
print(f"Gold Base Path (DBFS):    {gold_path_dbfs_base}")

# Define a path for temporary utility tests
util_test_path = f"{project_base_dbfs}/utility_tests"
print(f"Utility Test Path (DBFS): {util_test_path}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### `dbutils.fs.ls(path)`
# MAGIC Lists files and directories within a given DBFS path. Returns a list of `FileInfo` objects, each containing `path`, `name`, `size`, and `modificationTime`.
# MAGIC

# COMMAND ----------

print(f"Listing contents of project base directory: {project_base_dbfs}")
try:
    contents = dbutils.fs.ls(project_base_dbfs)
    # Display using Databricks display() for nice formatting
    display(contents)
except Exception as e:
    print(f"Error listing {project_base_dbfs}: {e}")


print(f"\nListing contents of Bronze Delta table path: {bronze_path_dbfs}")
try:
    # Useful for inspecting the structure of Delta tables (_delta_log, data files, partition dirs)
    bronze_contents = dbutils.fs.ls(bronze_path_dbfs)
    display(bronze_contents)
except Exception as e:
    print(f"Error listing {bronze_path_dbfs}: {e} (Did notebook 01 run?)")

# Example listing a specific partition (if partitioned by year=2023, month=1)
bronze_partition_path = f"{bronze_path_dbfs}/pickup_year=2023/pickup_month=1"
print(f"\nAttempting to list contents of a specific Bronze partition: {bronze_partition_path}")
try:
    partition_contents = dbutils.fs.ls(bronze_partition_path)
    display(partition_contents)
except Exception as e:
    print(f"Could not list partition path: {e} (Partition might not exist or path is incorrect)")


# COMMAND ----------

# MAGIC %md
# MAGIC ### `dbutils.fs.mkdirs(path)`
# MAGIC Creates the specified directory path, including any necessary parent directories. If the directory already exists, it does nothing (no error).

# COMMAND ----------

print(f"Creating utility test directory (and parents if needed): {util_test_path}")
try:
    success = dbutils.fs.mkdirs(util_test_path)
    if success:
        print(f"Directory '{util_test_path}' created or already existed.")
        # Verify by listing the parent directory
        display(dbutils.fs.ls(project_base_dbfs))
    else:
        # Note: mkdirs usually returns True even if dir exists, failure is rare (e.g., permissions)
        print(f"Failed to create directory '{util_test_path}'.")
except Exception as e:
    print(f"Error creating directory: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### `dbutils.fs.put(path, contents, overwrite=False)`
# MAGIC Writes the given string `contents` to a file at the specified DBFS `path`.
# MAGIC * `overwrite=True`: Allows overwriting an existing file. Defaults to `False` (throws error if file exists).

# COMMAND ----------

test_file_path = f"{util_test_path}/my_test_file.txt"
file_content = "This is line 1.\nHello from dbutils.fs.put!\nLine 3."

print(f"Writing content to file: {test_file_path}")
try:
    dbutils.fs.put(test_file_path, file_content, overwrite=True)
    print("File written successfully.")
    # Verify by listing the directory
    display(dbutils.fs.ls(util_test_path))
except Exception as e:
    print(f"Error writing file: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### `dbutils.fs.head(path, max_bytes=65536)`
# MAGIC Reads and returns the first `max_bytes` of a file as a string. Useful for quickly inspecting the contents of text files without reading the whole file into memory.

# COMMAND ----------

print(f"Reading the head of file: {test_file_path}")
try:
    # Read the first 100 bytes
    head_content = dbutils.fs.head(test_file_path, 100)
    print("--- File Head (max 100 bytes) ---")
    print(head_content)
    print("---------------------------------")
except Exception as e:
    print(f"Error reading head of file: {e}")

# Example: Try reading head of a Parquet file (will show binary/garbled text)
raw_file_path_landing = f"{landing_path_dbfs}/{raw_data_file}"
print(f"\nReading the head of a Parquet file: {raw_file_path_landing}")
try:
    parquet_head = dbutils.fs.head(raw_file_path_landing, 200)
    print("--- Parquet File Head (max 200 bytes) ---")
    print(parquet_head) # Output will likely look like garbage characters
    print("---------------------------------------")
    print("(Note: Output is binary representation, not human-readable text)")
except Exception as e:
    print(f"Error reading head of Parquet file: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### `dbutils.fs.cp(src_path, dest_path, recurse=False)`
# MAGIC Copies a file or directory from `src_path` to `dest_path`.
# MAGIC * `recurse=True`: Required to copy directories.

# COMMAND ----------

copy_dest_file = f"{util_test_path}/my_test_file_copy.txt"
copy_dest_dir = f"{util_test_path}/landing_copy"

# 1. Copy a single file
print(f"Copying file from {test_file_path} to {copy_dest_file}")
try:
    success = dbutils.fs.cp(test_file_path, copy_dest_file)
    if success:
        print("File copied successfully.")
        display(dbutils.fs.ls(util_test_path))
    else:
        print("File copy failed.")
except Exception as e:
    print(f"Error copying file: {e}")


# 2. Copy a directory (requires recurse=True)
# Copy the landing zone directory (containing the raw parquet file) to the test area
print(f"\nCopying directory from {landing_path_dbfs} to {copy_dest_dir} (recursive)")
try:
    # Ensure destination doesn't exist first if you want a clean copy
    dbutils.fs.rm(copy_dest_dir, recurse=True) # Remove if exists from previous run
    print(f"Removed existing directory {copy_dest_dir} if present.")

    success = dbutils.fs.cp(landing_path_dbfs, copy_dest_dir, recurse=True)
    if success:
        print("Directory copied successfully.")
        print(f"Contents of {copy_dest_dir}:")
        display(dbutils.fs.ls(copy_dest_dir))
    else:
        print("Directory copy failed.")
except Exception as e:
    print(f"Error copying directory: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### `dbutils.fs.mv(src_path, dest_path, recurse=False)`
# MAGIC Moves (renames) a file or directory from `src_path` to `dest_path`.
# MAGIC * `recurse=True`: Seems to be implicitly handled for directories in `mv` unlike `cp`.
# MAGIC * Useful for renaming files or reorganizing directories. We used this in `00_Setup` to move the uploaded file.

# COMMAND ----------

move_src_file = copy_dest_file # Use the copied file from previous step
move_dest_file = f"{util_test_path}/my_test_file_moved.txt"

print(f"Moving (renaming) file from {move_src_file} to {move_dest_file}")
try:
    success = dbutils.fs.mv(move_src_file, move_dest_file)
    if success:
        print("File moved successfully.")
        print("\nVerifying source is gone and destination exists:")
        try:
            dbutils.fs.ls(move_src_file)
            print(f"ERROR: Source file {move_src_file} still exists after move!")
        except:
            print(f"OK: Source file {move_src_file} correctly removed.")
        display(dbutils.fs.ls(move_dest_file)) # Check destination
    else:
        print("File move failed.")
except Exception as e:
    print(f"Error moving file: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### `dbutils.fs.rm(path, recurse=False)`
# MAGIC Removes (deletes) a file or directory.
# MAGIC * `recurse=True`: **Required** to remove a non-empty directory. Use with caution!

# COMMAND ----------

# 1. Remove a single file
file_to_remove = move_dest_file # Use the moved file
print(f"Removing single file: {file_to_remove}")
try:
    success = dbutils.fs.rm(file_to_remove, recurse=False) # recurse=False for single file
    if success:
        print("File removed successfully.")
        # Verify it's gone
        try:
            dbutils.fs.ls(file_to_remove)
            print(f"ERROR: File {file_to_remove} still exists after rm!")
        except:
            print(f"OK: File {file_to_remove} correctly removed.")
    else:
        print("File removal failed.")
except Exception as e:
    print(f"Error removing file: {e}")


# 2. Remove a directory (requires recurse=True)
dir_to_remove = copy_dest_dir # Use the copied directory
print(f"\nRemoving directory (recursive): {dir_to_remove}")
try:
    success = dbutils.fs.rm(dir_to_remove, recurse=True) # MUST use recurse=True for directories
    if success:
        print("Directory removed successfully.")
        # Verify it's gone by listing parent
        print(f"Contents of {util_test_path} after removing subdir:")
        display(dbutils.fs.ls(util_test_path))
    else:
        print("Directory removal failed.")
except Exception as e:
    print(f"Error removing directory: {e}")


# 3. Clean up the main utility test directory
print(f"\nCleaning up main utility test directory: {util_test_path}")
try:
    success = dbutils.fs.rm(util_test_path, recurse=True)
    if success:
        print("Utility test directory removed successfully.")
        # Verify by listing parent
        print(f"Contents of {project_base_dbfs} after cleanup:")
        display(dbutils.fs.ls(project_base_dbfs))
    else:
        print("Utility test directory removal failed.")
except Exception as e:
    print(f"Error removing utility test directory: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC These `dbutils.fs` commands provide essential tools for managing your project's files and directories directly within Databricks notebooks when working with DBFS storage.