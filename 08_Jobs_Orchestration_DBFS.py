# Databricks notebook source
# MAGIC %md
# MAGIC # 08. Job Orchestration (DBFS Context)
# MAGIC
# MAGIC **Purpose:** Explain how to operationalize the DBFS-based ETL pipeline (notebooks 01-03 and potentially others) using Databricks Jobs for scheduling and orchestration.
# MAGIC
# MAGIC **Key Concepts (Databricks Jobs):**
# MAGIC * **Job:** A container for one or more tasks.
# MAGIC * **Task:** A unit of work within a job. Can run a Notebook, Python script, JAR, SQL query, DLT Pipeline, etc.
# MAGIC * **Dependencies:** Define the execution order of tasks, creating a workflow (DAG - Directed Acyclic Graph). Tasks run only after their dependencies complete successfully.
# MAGIC * **Clusters:** Configure the compute resources for job tasks.
# MAGIC     * **Job Cluster (Recommended):** A cluster created specifically for a job run, which terminates automatically upon completion. Ideal for isolation, cost management, and using specific configurations per job.
# MAGIC     * **All-Purpose Cluster:** An existing interactive cluster. Can be used for jobs, but generally not recommended for production due to potential resource conflicts and manual termination requirements.
# MAGIC * **Scheduling:** Define schedules (e.g., nightly, hourly, cron syntax) to run jobs automatically. Jobs can also be triggered manually or via API/CLI.
# MAGIC * **Parameters:** Pass parameters (e.g., via Notebook Widgets) to tasks, making jobs configurable and reusable without changing the underlying code.
# MAGIC * **Alerts & Notifications:** Configure email, webhook, or Slack notifications for job events (start, success, failure).
# MAGIC * **Repair and Rerun:** Rerun only failed tasks or the entire job run.
# MAGIC * **Monitoring:** Track job run history, view logs per task, analyze performance, and identify bottlenecks in the Jobs UI.
# MAGIC * **Task Values (`dbutils.jobs.taskValues`):** Allow tasks to pass small amounts of data (strings) to downstream tasks within the same job run.
# MAGIC
# MAGIC **DBFS Focus:**
# MAGIC * The job will orchestrate notebooks that read from and write to DBFS paths.
# MAGIC * Job clusters will access DBFS just like interactive clusters.
# MAGIC * Parameters can be used to specify DBFS paths if needed (though our current setup uses Spark conf derived from user context).
# MAGIC
# MAGIC **Goal:** Outline the steps to create a Databricks Job to run the Bronze -> Silver -> Gold pipeline notebooks sequentially.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating a Multi-Task Job for the DBFS Pipeline
# MAGIC
# MAGIC Follow these steps in the Databricks UI to create a job that runs notebooks `01_Ingestion_Bronze_DBFS`, `02_Cleansing_Silver_DBFS`, and `03_Aggregation_Gold_DBFS` in sequence.
# MAGIC
# MAGIC 1.  **Navigate to Workflows:**
# MAGIC     * Click the **Workflows** icon in the left sidebar.
# MAGIC 2.  **Go to Jobs Tab:**
# MAGIC     * Select the **Jobs** tab.
# MAGIC 3.  **Create Job:**
# MAGIC     * Click the **Create Job** button.
# MAGIC 4.  **Configure Job Details (Top Panel):**
# MAGIC     * **Job Name:** Enter a descriptive name, e.g., `DE Cert Prep ETL (DBFS)`.
# MAGIC     * **Tags:** (Optional) Add tags for organization.
# MAGIC
# MAGIC 5.  **Configure Task 1: Bronze Ingestion**
# MAGIC     * **Task name:** `Ingest_Bronze_DBFS`
# MAGIC     * **Type:** Select `Notebook`.
# MAGIC     * **Source:** Select `Workspace`.
# MAGIC     * **Path:** Browse to and select the `01_Ingestion_Bronze_DBFS` notebook within your project folder (e.g., `/Workspace/Users/your_email@domain.com/DE_Cert_Prep_Project_DBFS/01_Ingestion_Bronze_DBFS`).
# MAGIC     * **Cluster:**
# MAGIC         * **Highly Recommended:** Select `New job cluster` from the dropdown.
# MAGIC         * **Configure Job Cluster:**
# MAGIC             * **Policy:** Use a suitable cluster policy if available, or configure manually.
# MAGIC             * **Worker type / Driver type:** Choose appropriate instance types (e.g., Standard_DS3_v2 or similar general-purpose nodes).
# MAGIC             * **Workers:** Set Min/Max workers for autoscaling (e.g., Min 1, Max 2 or 4 depending on expected load).
# MAGIC             * **Photon acceleration:** **Enable** this checkbox for better performance.
# MAGIC             * **Terminate after:** Set an inactivity timeout (e.g., 10-15 minutes) for the job cluster.
# MAGIC     * **Parameters:** (Optional) If the notebook used widgets (`dbutils.widgets`), you could override them here. Our setup currently relies on Spark conf derived from context, which usually works in jobs too, but widgets are the standard way to parameterize job notebooks.
# MAGIC     * **Depends on:** Leave blank for the first task.
# MAGIC
# MAGIC 6.  **Configure Task 2: Silver Cleansing**
# MAGIC     * Click the **+ Add task** button below the first task.
# MAGIC     * **Task name:** `Process_Silver_DBFS`
# MAGIC     * **Type:** `Notebook`
# MAGIC     * **Source:** `Workspace`
# MAGIC     * **Path:** Browse to and select the `02_Cleansing_Silver_DBFS` notebook.
# MAGIC     * **Cluster:** **Select the *same* Job Cluster** defined for Task 1 (e.g., it will be named something like `Job cluster for DE Cert Prep ETL (DBFS)`). This reuses the same cluster, saving startup time and cost.
# MAGIC     * **Parameters:** (Optional)
# MAGIC     * **Depends on:** Select `Ingest_Bronze_DBFS` from the dropdown. This ensures this task only runs if Task 1 succeeds.
# MAGIC
# MAGIC 7.  **Configure Task 3: Gold Aggregation**
# MAGIC     * Click the **+ Add task** button.
# MAGIC     * **Task name:** `Aggregate_Gold_DBFS`
# MAGIC     * **Type:** `Notebook`
# MAGIC     * **Source:** `Workspace`
# MAGIC     * **Path:** Browse to and select the `03_Aggregation_Gold_DBFS` notebook.
# MAGIC     * **Cluster:** Select the *same* Job Cluster again.
# MAGIC     * **Parameters:** (Optional)
# MAGIC     * **Depends on:** Select `Process_Silver_DBFS`.
# MAGIC
# MAGIC 8.  **(Optional) Add Further Tasks:**
# MAGIC     * You could add subsequent tasks for:
# MAGIC         * **Optimization:** Running `OPTIMIZE` and `VACUUM` commands (perhaps from notebook `04` or a dedicated utility notebook). Depends on `Aggregate_Gold_DBFS`.
# MAGIC         * **Streaming:** Triggering the streaming notebook (`05`). Note that `availableNow=True` streams fit well in triggered jobs. Continuous streams require different job configurations (e.g., continuous mode jobs). Depends on `Ingest_Bronze_DBFS` (if reading Bronze) or runs independently.
# MAGIC         * **Data Quality Checks:** Running validation notebooks.
# MAGIC
# MAGIC 9.  **Configure Job-Level Settings (Right Panel):**
# MAGIC     * **Schedule:** (Optional)
# MAGIC         * Click `Edit schedule`.
# MAGIC         * Select `Scheduled`.
# MAGIC         * Choose frequency (e.g., Daily, Hourly) and time, or enter a Cron syntax.
# MAGIC     * **Job Clusters:** Review the job cluster definition created in Task 1.
# MAGIC     * **Parameters:** (Optional) Define job-level parameters accessible by all tasks.
# MAGIC     * **Notifications:** (Recommended)
# MAGIC         * Click `Add` under Notifications.
# MAGIC         * Add your email address for `Start`, `Success`, and especially `Failure` events.
# MAGIC     * **Maximum concurrent runs:** Usually `1` for ETL jobs to prevent conflicts, unless designed for parallel independent runs.
# MAGIC
# MAGIC 10. **Create the Job:**
# MAGIC     * Click the **Create** button.
# MAGIC
# MAGIC 11. **Run and Monitor:**
# MAGIC     * You can run the job manually using the **Run Now** button in the top right of the Job details page.
# MAGIC     * Monitor the progress in the **Job runs** tab. Click on a specific run to see the task graph, status of each task, links to logs, and cluster details.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameterization using Widgets
# MAGIC
# MAGIC While our current notebooks derive configuration from Spark conf, the more standard way to pass parameters into Job Notebook tasks is via **Widgets**.
# MAGIC
# MAGIC **Example in a Notebook:**
# MAGIC ```python
# MAGIC # In notebook 01_Ingestion_Bronze_DBFS (or others)
# MAGIC dbutils.widgets.text("source_data_path", "/default/path/to/data.parquet", "Source Data Path")
# MAGIC dbutils.widgets.text("target_db_name", "dev_db", "Target Database Name")
# MAGIC
# MAGIC # Get widget values
# MAGIC source_path = dbutils.widgets.get("source_data_path")
# MAGIC target_db = dbutils.widgets.get("target_db_name")
# MAGIC
# MAGIC print(f"Reading from: {source_path}")
# MAGIC print(f"Writing to DB: {target_db}")
# MAGIC # ... rest of the notebook logic using these variables ...
# MAGIC ```
# MAGIC
# MAGIC **In the Job Task Configuration:**
# MAGIC * When configuring the notebook task (e.g., `Ingest_Bronze_DBFS`), the **Parameters** section will automatically detect the widgets.
# MAGIC * You can then provide values for these parameters specific to this job run.
# MAGIC     * Key: `source_data_path`, Value: `dbfs:/mnt/prod/landing/taxi_2023_01.parquet`
# MAGIC     * Key: `target_db_name`, Value: `prod_taxi_db`
# MAGIC * This allows reusing the same notebook code for different environments (dev, staging, prod) or different input datasets just by changing the job parameters.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passing Values Between Tasks (`dbutils.jobs.taskValues`)
# MAGIC
# MAGIC Sometimes, a downstream task needs a small piece of information computed by an upstream task (e.g., a record count, a generated file path, a status flag). `dbutils.jobs.taskValues` allows this.
# MAGIC
# MAGIC **Example:**
# MAGIC
# MAGIC **Task 1: `Ingest_Bronze_DBFS`**
# MAGIC ```python
# MAGIC # ... (code to write bronze_df) ...
# MAGIC record_count = bronze_df.count()
# MAGIC print(f"Bronze ingestion complete. Count: {record_count}")
# MAGIC
# MAGIC # Set a task value for downstream tasks
# MAGIC dbutils.jobs.taskValues.set(key="bronze_record_count", value=str(record_count))
# MAGIC print(f"Set task value 'bronze_record_count' to: {record_count}")
# MAGIC ```
# MAGIC
# MAGIC **Task 2: `Process_Silver_DBFS` (Depends on Task 1)**
# MAGIC ```python
# MAGIC # Get the value set by the upstream task 'Ingest_Bronze_DBFS'
# MAGIC # Provide a default value for interactive runs outside a job context
# MAGIC upstream_count_str = dbutils.jobs.taskValues.get(taskKey="Ingest_Bronze_DBFS", key="bronze_record_count", default="0", debugValue="0")
# MAGIC upstream_count = int(upstream_count_str)
# MAGIC print(f"Received record count from upstream task 'Ingest_Bronze_DBFS': {upstream_count}")
# MAGIC
# MAGIC # Use the value if needed
# MAGIC if upstream_count == 0:
# MAGIC     print("Warning: Upstream task reported 0 records.")
# MAGIC     # dbutils.notebook.exit("Skipping Silver processing due to 0 input records.") # Optional exit
# MAGIC # ... (rest of Silver processing) ...
# MAGIC ```
# MAGIC **Note:** Task values are limited in size (typically strings) and are meant for passing small control-flow information, not large datasets.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC By using Databricks Jobs, you can reliably automate the execution of your DBFS-based ETL pipeline, ensuring tasks run in the correct order and leveraging dedicated compute resources for efficiency and cost management.