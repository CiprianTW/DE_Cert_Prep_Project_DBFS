# Databricks notebook source
# MAGIC %md
# MAGIC # _Helper_Config_DBFS
# MAGIC
# MAGIC **Purpose:** This helper notebook contains functions to load project configuration
# MAGIC from the `project_config.json` file stored on DBFS. This file is expected
# MAGIC to be created by the `00_Setup_Data_DBFS` notebook.
# MAGIC
# MAGIC This notebook is intended to be run by other notebooks using the `%run` command
# MAGIC to make these utility functions available.
# MAGIC
# MAGIC **Functions:**
# MAGIC * `load_project_config_from_dbfs_once()`: Reads and caches the project configuration JSON.
# MAGIC * `get_config_value(key_suffix)`: Retrieves a specific configuration value.

# COMMAND ----------

import re
import json

# Global variable to store loaded configuration to avoid re-reading the file multiple times
# This cache will be specific to the session that runs this helper notebook.
_PROJECT_CONFIG_CACHE_HELPER = None

def load_project_config_from_dbfs_once():
    """
    Reads the project_config.json file from DBFS if not already loaded into the cache.
    The path to the config file is constructed based on the current user's context.
    """
    global _PROJECT_CONFIG_CACHE_HELPER
    if _PROJECT_CONFIG_CACHE_HELPER is not None:
        # print("DEBUG (Helper): Project config already loaded in cache.")
        return _PROJECT_CONFIG_CACHE_HELPER

    # Determine the current user to construct the expected config file path
    try:
        current_user_name_raw = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        expected_project_base_dbfs = f"dbfs:/Users/{current_user_name_raw}/DE_Cert_Prep_Project_DBFS"
        expected_config_file_path = f"{expected_project_base_dbfs}/project_config.json"
        # print(f"DEBUG (Helper): Attempting to load config file from: {expected_config_file_path}")
    except Exception as e:
        print(f"ERROR (Helper): Could not determine current user to locate config file: {e}")
        raise ValueError("Cannot locate config file without user context to build its path.")

    try:
        config_json_string = dbutils.fs.head(expected_config_file_path)
        _PROJECT_CONFIG_CACHE_HELPER = json.loads(config_json_string)
        print(f"SUCCESS (Helper): Project configuration loaded from {expected_config_file_path}")
        return _PROJECT_CONFIG_CACHE_HELPER
    except Exception as e:
        print(f"ERROR (Helper): Failed to read or parse configuration file from {expected_config_file_path}")
        print(f"  Details: {e}")
        _PROJECT_CONFIG_CACHE_HELPER = None 
        raise ValueError(f"Could not load project configuration from {expected_config_file_path}. See errors above.")

# COMMAND ----------

def get_config_value(key_suffix):
    """
    Retrieves a specific configuration value by its suffix (e.g., "db_name").
    The full key is constructed using the user-specific prefix found within the loaded config.
    """
    configs = load_project_config_from_dbfs_once() # Ensure config is loaded (or attempted)

    # Determine the user-specific prefix from the loaded config itself
    # This relies on 'user_name_safe' being stored in the config by notebook 00
    user_name_safe_from_config = None
    # Iterate through keys to find one that ends with '.user_name_safe' to get the value
    for k, v in configs.items():
        if k.endswith(".user_name_safe"):
            user_name_safe_from_config = v
            break
    
    if not user_name_safe_from_config:
        # Fallback: if .user_name_safe key itself isn't in the JSON, try to reconstruct from current context
        # This is a secondary measure and might indicate an issue with how config was saved.
        print("WARNING: '.user_name_safe' key not found in config JSON. Attempting reconstruction for prefix.")
        try:
            current_user_name_raw = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
            user_name_safe_from_config = re.sub(r'[^a-zA-Z0-9_]', '_', current_user_name_raw)
            if not user_name_safe_from_config: # Should not happen if context is available
                 raise ValueError("Could not reconstruct user_name_safe from current context.")
        except Exception as e_ctx:
            print(f"ERROR: Could not determine user_name_safe for key construction: {e_ctx}")
            raise ValueError("Configuration key prefix cannot be determined.")

    # Construct the full key name as it was stored in the JSON file
    full_key_in_json = f"project.de_cert_prep_dbfs.{user_name_safe_from_config}.{key_suffix}"
    
    value = configs.get(full_key_in_json)
    
    if value is None:
        print(f"DEBUG: Key '{full_key_in_json}' not found. Available keys in loaded config: {list(configs.keys())}")
        raise ValueError(f"Configuration key '{full_key_in_json}' not found in the loaded project_config.json.")
    
    # print(f"DEBUG: Retrieved value for '{key_suffix}' (full key '{full_key_in_json}'): '{value}'")
    return value

# COMMAND ----------

# MAGIC %md
# MAGIC Helper functions defined. This notebook can now be called using `%run /path/to/_Helper_Config_DBFS`.
# MAGIC
