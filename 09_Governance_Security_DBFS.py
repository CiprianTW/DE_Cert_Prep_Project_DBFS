# Databricks notebook source
# MAGIC %md
# MAGIC # 09. Governance & Security (DBFS Context)
# MAGIC
# MAGIC **Purpose:** Discuss governance and security considerations when working primarily with DBFS and the standard Hive Metastore in Databricks. This contrasts with the centralized governance features offered by Unity Catalog (UC).
# MAGIC
# MAGIC **Key Concepts (DBFS/Hive Metastore Context):**
# MAGIC * **Access Control:** Managing who can access workspaces, clusters, jobs, notebooks, and data.
# MAGIC * **Data Encryption:** How data is protected at rest and in transit.
# MAGIC * **Auditing:** Tracking user activities and operations.
# MAGIC * **PII Handling:** Strategies for managing Personally Identifiable Information.
# MAGIC * **DBFS Security:** Specific considerations for data stored directly in DBFS.
# MAGIC
# MAGIC **DBFS Focus:**
# MAGIC * Access control relies heavily on workspace-level permissions and potentially Table Access Control Lists (Table ACLs) if enabled on the cluster.
# MAGIC * Data lineage and fine-grained (column/row level) security are generally *not* available without Unity Catalog.
# MAGIC * Auditing is primarily through workspace audit logs.
# MAGIC * PII handling requires manual implementation within ETL code.
# MAGIC
# MAGIC **Disclaimer:** Security configurations can vary significantly based on your specific Databricks deployment tier (Standard, Premium, Enterprise) and cloud provider settings. This notebook covers general concepts. Always consult official Databricks documentation and your security team for specifics.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Access Control Mechanisms (Non-UC Environment)
# MAGIC %md
# MAGIC ### Access Control Mechanisms
# MAGIC
# MAGIC Without Unity Catalog, access control is managed through several distinct mechanisms:
# MAGIC
# MAGIC 1.  **Workspace Permissions:**
# MAGIC     * Admins control user/group access to the workspace itself.
# MAGIC     * Permissions can be set on workspace objects like Notebooks, Folders, Experiments, Models using Access Control Lists (ACLs) in the UI.
# MAGIC     * Example: Granting `Can Edit` or `Can View` permission on the `DE_Cert_Prep_Project_DBFS` folder to specific users or groups.
# MAGIC
# MAGIC 2.  **Cluster Permissions (Cluster ACLs):**
# MAGIC     * Admins or cluster creators can control who can attach to, restart, or manage specific clusters.
# MAGIC     * Found under `Compute` -> `Cluster Name` -> `Permissions`.
# MAGIC     * This implicitly controls who can run code and potentially access data the cluster's service principal/instance profile can reach.
# MAGIC
# MAGIC 3.  **Job Permissions (Job ACLs):**
# MAGIC     * Admins or job creators can control who can view, manage, or run specific jobs.
# MAGIC     * Found under `Workflows` -> `Jobs` -> `Job Name` -> `Permissions`.
# MAGIC     * Controls who can trigger the execution of the notebooks defined in the job tasks.
# MAGIC
# MAGIC 4.  **Table Access Control Lists (Table ACLs - Cluster Dependent):**
# MAGIC     * **Availability:** This feature is available on **High Concurrency clusters** or clusters with **Table Access Control enabled** in the cluster settings (requires Premium tier or above). It is *not* the same as Unity Catalog's `GRANT`/`REVOKE`.
# MAGIC     * **How it Works:** Allows granting `SELECT`, `MODIFY` permissions on specific *tables* registered in the Hive Metastore to users/groups.
# MAGIC     * **Limitations:**
# MAGIC         * Only applies to SQL commands (`SELECT`, `INSERT`, `UPDATE`, etc.).
# MAGIC         * **Does NOT protect the underlying data files on DBFS directly.** A user with direct DBFS access or permissions to run arbitrary code on a cluster might still be able to bypass Table ACLs by reading the files directly (e.g., `spark.read.format("delta").load("dbfs:/path/to/table/data")`).
# MAGIC         * No support for row/column level security or data masking.
# MAGIC     * **Syntax (SQL):**
# MAGIC       ```sql
# MAGIC       -- GRANT SELECT ON TABLE database_name.table_name TO `user@example.com`;
# MAGIC       -- GRANT MODIFY ON TABLE database_name.table_name TO `group_name`;
# MAGIC       -- REVOKE SELECT ON TABLE database_name.table_name FROM `user@example.com`;
# MAGIC       -- SHOW GRANTS ON TABLE database_name.table_name;
# MAGIC       ```
# MAGIC
# MAGIC 5.  **DBFS Access:**
# MAGIC     * Access to DBFS paths often depends on the cluster configuration (instance profiles, service principals) and workspace settings.
# MAGIC     * User isolation in DBFS can be complex. By default, users might have broad access within `/Users/`, but access to other paths like `/mnt/` or the root depends on setup.
# MAGIC     * **Recommendation:** Avoid storing sensitive data in easily accessible DBFS locations if fine-grained control is needed. Use dedicated service principals or instance profiles with least-privilege access to underlying cloud storage if possible (though this project assumes DBFS-only).
# MAGIC
# MAGIC **Contrast with Unity Catalog:** UC provides a centralized, SQL-standard `GRANT`/`REVOKE` model for fine-grained permissions on Catalogs, Schemas, Tables, Views, Functions, and External Locations, which *does* protect the underlying data regardless of API used (SQL, Python). It also adds features like attribute-based access, row/column masking, and data lineage.
# MAGIC

# COMMAND ----------

# MAGIC %run ../DE_Cert_Prep_Project_DBFS/_Helper_Config_DBFS

# COMMAND ----------

# DBTITLE 1,Checking Table ACLs (If Enabled)
# This command will only work if Table ACLs are enabled on the cluster you are running this on.
# Otherwise, it will likely throw a security or parse exception.

db_name = get_config_value("db_name")
silver_table_name = "silver_taxi_trips"

try:
    print(f"Attempting to show grants on table {db_name}.{silver_table_name} (Requires Table ACLs enabled cluster)...")
    display(spark.sql(f"SHOW GRANTS ON TABLE {db_name}.{silver_table_name}"))
    print("Command executed. If output is shown, Table ACLs are likely enabled.")
except Exception as e:
    print(f"\nCommand failed. Table ACLs might not be enabled on this cluster or you lack permissions.")
    print(f"Error: {e}")

# COMMAND ----------

# DBTITLE 1,Data Encryption
# MAGIC %md
# MAGIC ### Data Encryption
# MAGIC
# MAGIC * **Encryption at Rest:**
# MAGIC     * Data stored on DBFS (which resides on cloud storage attached to the control plane or worker nodes' volumes) is typically encrypted at rest by the cloud provider (AWS S3, Azure Blob/ADLS, GCP GCS).
# MAGIC     * Databricks may offer configuration options for customer-managed keys (CMK) for enhanced control over encryption (Premium feature).
# MAGIC * **Encryption in Transit:**
# MAGIC     * Communication between Spark driver and executors, and between your browser and the Databricks workspace, is generally secured using TLS/SSL.
# MAGIC
# MAGIC **Note:** Specific encryption details depend heavily on your cloud provider and Databricks subscription level.

# COMMAND ----------

# DBTITLE 1,Auditing
# MAGIC %md
# MAGIC ### Auditing
# MAGIC
# MAGIC * **Workspace Audit Logs:** Databricks captures detailed audit logs recording activities within the workspace.
# MAGIC * **Content:** Logs include events like notebook commands executed, cluster start/stop, job runs, permission changes, configuration updates, DBFS interactions, table access (if Table ACLs used), etc.
# MAGIC * **Access:** Audit logs can typically be configured to deliver to a specified location (like cloud storage - S3/ADLS) for analysis and retention (Premium feature). Admins can access recent logs via the Admin Console.
# MAGIC * **Importance:** Essential for security monitoring, compliance, and troubleshooting.
# MAGIC
# MAGIC **Contrast with Unity Catalog:** UC provides additional, centralized audit logs specifically for data access events governed by UC (who accessed what table/schema/catalog).

# COMMAND ----------

# DBTITLE 1,PII Handling Strategies (Manual Implementation)
# MAGIC %md
# MAGIC ### PII Handling Strategies
# MAGIC
# MAGIC Protecting Personally Identifiable Information (PII) is critical. Without UC's built-in masking capabilities, you need to implement PII handling manually within your ETL pipelines.
# MAGIC
# MAGIC **Common Strategies:**
# MAGIC
# MAGIC 1.  **Exclusion:** Don't ingest PII into Databricks if it's not needed for analysis.
# MAGIC 2.  **Pseudonymization/Tokenization:** Replace PII values with irreversible tokens or pseudonyms. Requires a secure lookup mechanism if re-identification is ever needed (usually managed outside Databricks).
# MAGIC 3.  **Hashing:** Apply cryptographic hash functions (e.g., SHA-256) to PII columns. This is one-way; you cannot recover the original value. Good for joining or grouping on PII without exposing it, but vulnerable to rainbow table attacks if not salted.
# MAGIC 4.  **Masking:** Partially or fully obscure PII values (e.g., show only last 4 digits of an ID, replace characters with 'X').
# MAGIC 5.  **Generalization/Suppression:** Reduce the precision of data (e.g., replace exact age with an age range, suppress rare combinations).
# MAGIC
# MAGIC **Implementation:** These techniques are typically applied during the Silver or Gold layer transformations using Spark functions.

# COMMAND ----------

# DBTITLE 1,Example: Simple Hashing/Masking in Spark
from pyspark.sql.functions import sha2, lit, expr, udf, rand, col, concat
from pyspark.sql.types import StringType

# Assume we have a DataFrame with hypothetical PII
data = [("Alice", "alice@example.com", "555-1234"), ("Bob", "bob@example.com", "555-5678")]
columns = ["name", "email", "phone"]
pii_df = spark.createDataFrame(data, columns)

print("Original DataFrame with PII:")
display(pii_df)

# --- Hashing Example (using SHA-256) ---
# Add a salt for better security against rainbow tables
salt = "a_random_secret_salt_string" # In practice, manage salts securely
hashed_df = pii_df.withColumn("email_hash", sha2(col("email"), 256)) \
                  .withColumn("phone_hash_salted", sha2(concat(col("phone"), lit(salt)), 256))

print("\nDataFrame with Hashed PII:")
display(hashed_df.select("name", "email_hash", "phone_hash_salted"))


# --- Masking Example ---
# Mask email: show first char, domain, replace middle with '***'
# Mask phone: show last 4 digits, replace rest with 'X'
masked_df = pii_df \
    .withColumn("email_masked", expr("regexp_replace(email, '(?<=.).*?(?=@)', '***')")) \
    .withColumn("phone_masked", expr("concat(regexp_replace(phone, '[0-9](?=[0-9]{4})', 'X'), substr(phone, -4, 4))"))

print("\nDataFrame with Masked PII:")
display(masked_df.select("name", "email_masked", "phone_masked"))

# --- Tokenization Example (Conceptual - Requires External System) ---
# This requires a secure external service/database for token mapping.
# We simulate it here with a simple UDF generating random IDs (NOT secure/persistent).
@udf(StringType())
def simple_tokenize(value):
  import uuid
  # In reality, this would call an external tokenization service
  # or look up/create a token in a secure database.
  # DO NOT USE THIS UDF FOR REAL TOKENIZATION.
  return str(uuid.uuid4())

tokenized_df = pii_df.withColumn("name_token", simple_tokenize(col("name")))

print("\nDataFrame with Simulated Tokens (NOT FOR PRODUCTION):")
display(tokenized_df.select("name_token", "email", "phone"))

# COMMAND ----------

# DBTITLE 1,DBFS Security Considerations
# MAGIC %md
# MAGIC ### DBFS Security Considerations
# MAGIC
# MAGIC * **User Isolation:** While `/Users/<user_email>/` provides some isolation, users on the same cluster might still be able to access each other's user directories depending on cluster settings and permissions. Shared clusters increase this risk.
# MAGIC * **`/FileStore/` and Root (`/`):** These locations are often accessible by many users. Avoid storing sensitive raw data or intermediate files here without careful permission management (which is limited on DBFS itself).
# MAGIC * **Mount Points (`/mnt/`):** Access to data mounted from external storage depends entirely on how the mount point was configured (e.g., service principal credentials, passthrough authentication). Ensure mounts are configured securely.
# MAGIC * **Secrets:** Never hardcode credentials or sensitive information directly in notebooks. Use Databricks Secrets (`dbutils.secrets.get(scope="...", key="...")`) to store and retrieve secrets securely. Job clusters need appropriate permissions to access secret scopes.
# MAGIC * **Direct File Access:** As mentioned under Table ACLs, users who can run arbitrary code on a cluster might bypass table-level permissions by directly reading the underlying Delta files from their DBFS path if they have filesystem access. This highlights the importance of controlling *cluster access* and *code execution permissions*.
# MAGIC
# MAGIC **Recommendation:** For sensitive workloads in non-UC environments, using Job Clusters with specific service principals or instance profiles configured with least-privilege access to necessary DBFS paths or external locations is a more robust approach than relying solely on Table ACLs or workspace permissions.

# COMMAND ----------

# MAGIC %md
# MAGIC Governance and security in a non-UC environment require careful configuration across multiple layers (workspace, cluster, jobs, potentially Table ACLs) and often involve manual implementation for tasks like PII handling. Unity Catalog significantly simplifies and centralizes many of these aspects.
# MAGIC