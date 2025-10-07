# Databricks Data Engineer Cert Prep Project (DBFS Focus)

**Goal:** Provide hands-on examples for the Databricks Data Engineer Professional certification, using **DBFS exclusively** for data storage. This project is designed for environments where direct access to external cloud storage (like S3, ADLS, GCS) is not available or desired for the exercise.

**Data:** NYC Yellow Taxi Trip Records (January 2023 Parquet file recommended). Download and upload as per instructions in `00_Setup_Data_DBFS`.

**Architecture:** Medallion Architecture (Bronze -> Silver -> Gold) implemented on DBFS paths within your user directory. Tables created will primarily be *unmanaged (external)* tables, explicitly pointing to these DBFS locations via the `LOCATION` clause.

**Key Topics Covered (DBFS Context):**
* Databricks Platform Basics (Workspace, DBFS, Clusters, Jobs, Repos)
* Reading/Writing Spark DataFrames to/from DBFS paths.
* Delta Lake on DBFS (ACID, Time Travel, Schema Evolution/Enforcement, MERGE, OPTIMIZE, ZORDER, VACUUM, CLONE).
* Creating Managed and Unmanaged (External) Delta tables pointing to specific DBFS locations.
* ETL Pipeline Development (Bronze, Silver, Gold layers built entirely on DBFS).
* Structured Streaming with Auto Loader reading from DBFS directories, Checkpointing to DBFS.
* Delta Live Tables concepts (understanding how DLT manages storage, which defaults to or can be configured for DBFS).
* Performance Tuning (Partitioning strategies on DBFS, Caching, Adaptive Query Execution, Photon Engine, Query Plan Analysis).
* Job Scheduling for DBFS-based notebooks, Parameterization using Widgets and Task Values.
* Governance & Security concepts (Table Access Control Lists (legacy, non-UC), PII Handling). Discussion on Unity Catalog interaction with DBFS if applicable.
* `dbutils` utilities for DBFS interaction (`dbutils.fs`), Widgets, and Notebook workflows.

**Instructions:**
1.  **Run `00_Setup_Data_DBFS` first.** Carefully follow the data upload instructions within that notebook to get the source data into your DBFS landing zone.
2.  Execute notebooks 01 through 05 sequentially (`01_Ingestion...` to `05_Streaming...`). These build the core pipeline and explore Delta/Streaming features using DBFS.
3.  Review notebooks 06 through 10 (`06_DLT...` to `10_Utilities...`). These cover important conceptual topics (DLT, Optimization, Jobs, Governance) and practical utilities, all within the context of using DBFS for storage.

**Important Note on DBFS:** While DBFS is fully functional and suitable for this learning project, large-scale production systems often leverage direct integration with dedicated cloud storage (ADLS Gen2, S3, GCS). This provides advantages in performance, scalability, cost management, and fine-grained access control compared to relying solely on workspace-integrated DBFS. This project focuses on demonstrating the core Databricks engineering concepts using the readily available DBFS.
