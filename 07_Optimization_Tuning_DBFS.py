# Databricks notebook source
# MAGIC %md
# MAGIC # 07. Optimization and Tuning Techniques (DBFS Context)
# MAGIC
# MAGIC **Purpose:** Demonstrate and discuss common performance optimization techniques in Databricks relevant to data engineering workloads, particularly when data resides on DBFS.
# MAGIC
# MAGIC **Techniques Covered:**
# MAGIC * **Partitioning Review:** Recap partition strategy and pruning benefits for DBFS-based tables.
# MAGIC * **`OPTIMIZE` & `ZORDER` Review:** Recap compaction and data skipping for Delta tables on DBFS.
# MAGIC * **Caching (`.cache()`, `.persist()`):** Storing intermediate results in memory/disk.
# MAGIC * **Adaptive Query Execution (AQE):** Spark's runtime query re-optimization framework.
# MAGIC * **Photon Engine:** Databricks' native vectorized execution engine.
# MAGIC * **Broadcast Hash Joins:** Efficient join strategy for large-table-to-small-table joins.
# MAGIC * **Analyzing Query Plans (`.explain()`):** Understanding Spark's execution strategy.
# MAGIC
# MAGIC **DBFS Focus:**
# MAGIC * While some techniques (like Photon, AQE) are engine-level, others like partitioning, `OPTIMIZE`, and `ZORDER` directly impact how data is stored and accessed on DBFS.
# MAGIC * Caching behavior might be slightly influenced by DBFS I/O characteristics compared to direct cloud storage, but the concept remains the same.
# MAGIC * Query plan analysis helps identify bottlenecks related to DBFS scans or shuffles.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC * Notebooks `00` through `04` have been run successfully.
# MAGIC * Tables like `silver_taxi_trips`, `gold_daily_trip_summary`, `gold_pickup_zone_summary` exist.
# MAGIC

# COMMAND ----------

# MAGIC %run ../DE_Cert_Prep_Project_DBFS/_Helper_Config_DBFS

# COMMAND ----------

# DBTITLE 1,Load Configuration from Spark Conf
import re, time
from pyspark.sql.functions import col, avg, count, broadcast, date_trunc # Import necessary functions
from pyspark.storagelevel import StorageLevel # For specific persist levels

# Retrieve necessary configuration
try:
    db_name = get_config_value("db_name")
    # Define table names used in this notebook
    silver_table_name = "silver_taxi_trips"
    gold_table_daily_summary = "gold_daily_trip_summary"
    gold_table_zone_summary = "gold_pickup_zone_summary"
except ValueError as e:
    print(f"Configuration Error: {e}")
    dbutils.notebook.exit("Failed to load configuration.")

# Set the current database context
spark.sql(f"USE {db_name}")

print(f"--- Configuration Loaded ---")
print(f"Database Name:        {db_name}")
print(f"Using Tables:         {silver_table_name}, {gold_table_daily_summary}, {gold_table_zone_summary}")


# COMMAND ----------

# DBTITLE 1,Review: Partitioning and OPTIMIZE/ZORDER (DBFS Context)
# MAGIC %md
# MAGIC ### Review: Partitioning and `OPTIMIZE`/`ZORDER`
# MAGIC
# MAGIC These Delta Lake features, covered practically in notebook `04`, are fundamental for performance, especially with data on DBFS.
# MAGIC
# MAGIC * **Partitioning:**
# MAGIC     * **How:** We partitioned tables (e.g., Silver by `pickup_year`, `pickup_month`) during the `write` operation. This creates subdirectories within the main table's DBFS path (e.g., `.../delta/silver/pickup_year=2023/pickup_month=1/`).
# MAGIC     * **Benefit (Partition Pruning):** When you filter on partition columns in your queries (`WHERE pickup_year = 2023 AND pickup_month = 1`), Spark intelligently reads *only* the data within the relevant DBFS subdirectories, drastically reducing the amount of data scanned compared to reading the entire table path. This is highly effective for speeding up queries on large tables stored on DBFS.
# MAGIC     * **Strategy:** Choose low-to-moderate cardinality columns frequently used in `WHERE` clauses. Avoid over-partitioning (which creates too many small files and directories, potentially hurting performance on DBFS). Aim for partition sizes > 100MB (ideally ~1GB).
# MAGIC
# MAGIC * **`OPTIMIZE`:**
# MAGIC     * **How:** `OPTIMIZE table_name` or `OPTIMIZE delta.`dbfs:/path/to/delta``.
# MAGIC     * **Benefit (Compaction):** Merges small data files within partitions (or the whole table) into fewer, larger files. Reduces metadata overhead and improves scan performance, as opening/closing many small files on DBFS can be slow. Run periodically (e.g., daily or weekly) on tables with frequent small writes (streaming, merges).
# MAGIC
# MAGIC * **`ZORDER BY (col1, col2, ...)`:**
# MAGIC     * **How:** Used with `OPTIMIZE`: `OPTIMIZE table_name ZORDER BY (col1, col2)`.
# MAGIC     * **Benefit (Data Skipping):** Reorganizes data *within* the files so that rows with similar values in the ZORDER columns are physically co-located. When filtering on these columns, Delta can use file statistics (min/max values per file) to skip reading entire files that don't contain relevant data, even within a partition. Very effective for high-cardinality columns used in filters. Complements partitioning.
# MAGIC

# COMMAND ----------

# Example: Query benefiting from partition pruning (assuming Silver is partitioned by year/month)
print("Running query likely using partition pruning on Silver table...")
start_time = time.time()
display(spark.sql(f"""
    SELECT pickup_location_id, avg(tip_amount) as avg_tip
    FROM {db_name}.{silver_table_name}
    WHERE pickup_year = 2023 AND pickup_month = 1 -- Filter on partition columns
    GROUP BY pickup_location_id
    ORDER BY pickup_location_id
"""))
end_time = time.time()
print(f"Partitioned query duration: {end_time - start_time:.2f} seconds")

# Compare with a query potentially scanning more (though Spark might still push down filters)
print("\nRunning query potentially scanning more partitions (if filtering on non-partition col)...")
start_time = time.time()
display(spark.sql(f"""
    SELECT pickup_location_id, avg(tip_amount) as avg_tip
    FROM {db_name}.{silver_table_name}
    WHERE tip_amount > 50 -- Filter on non-partition column
    GROUP BY pickup_location_id
    ORDER BY pickup_location_id
"""))
end_time = time.time()
print(f"Non-partition filter query duration: {end_time - start_time:.2f} seconds")

# COMMAND ----------

# DBTITLE 1,Caching DataFrames (`.cache()`, `.persist()`)
# MAGIC %md
# MAGIC ### Caching (`.cache()`, `.persist()`)
# MAGIC
# MAGIC Caching stores the contents of a DataFrame or RDD in the cluster's memory (and potentially disk), allowing subsequent actions on the *same* DataFrame to reuse the computed results instead of recomputing them from the source (e.g., re-reading from DBFS and re-applying transformations).
# MAGIC
# MAGIC * `.cache()`: A shorthand for `.persist(StorageLevel.MEMORY_AND_DISK)`. Tries to store data in memory, spills to disk if memory is full. (Note: Default might vary slightly across Spark versions/Databricks runtimes, but MEMORY_AND_DISK is common).
# MAGIC * `.persist(StorageLevel)`: Allows fine-grained control over storage:
# MAGIC     * `MEMORY_ONLY`: Store in memory only. If not enough memory, partitions won't be cached (or may cause OOM). Fastest access.
# MAGIC     * `MEMORY_ONLY_SER`: Like `MEMORY_ONLY` but stores serialized objects (saves memory space, higher CPU cost for deserialization).
# MAGIC     * `MEMORY_AND_DISK`: Store in memory, spill to disk if needed. Good balance.
# MAGIC     * `MEMORY_AND_DISK_SER`: Like `MEMORY_AND_DISK` but serialized.
# MAGIC     * `DISK_ONLY`: Store only on disk. Slower access but saves memory.
# MAGIC     * Replication levels (`_2`) are usually not needed/used in typical Databricks setups.
# MAGIC * **When to Use:** Cache a DataFrame *if you plan to perform multiple actions* on it (e.g., multiple aggregations, using it for ML training and evaluation, writing to multiple locations).
# MAGIC * **How it Works:** Caching is *lazy*. The data is only computed and stored when the first *action* (like `.count()`, `.show()`, `.write()`, `.collect()`) is called on the cached DataFrame.
# MAGIC * **Cost:** Caching consumes cluster resources (memory, disk). Over-caching can evict useful data or even slow down jobs.
# MAGIC * **Cleanup:** Always call `.unpersist()` on the DataFrame when you are finished with it to release the cached resources, especially in long-running applications or memory-constrained environments.
# MAGIC

# COMMAND ----------

# Example: Using caching when performing multiple aggregations on the Silver table

print(f"Reading Silver table: {db_name}.{silver_table_name}")
silver_df = spark.table(f"{db_name}.{silver_table_name}")

# --- Scenario WITHOUT Caching ---
print("\n--- Running aggregations WITHOUT caching ---")

# Action 1
start_time = time.time()
avg_fare_by_passenger = silver_df.groupBy("passenger_count").agg(avg("fare_amount").alias("avg_fare"))
avg_fare_by_passenger.count() # Action to trigger computation
end_time = time.time()
print(f"Action 1 duration (no cache): {end_time - start_time:.2f} seconds")
# display(avg_fare_by_passenger)

# Action 2 (re-reads and re-computes from source)
start_time = time.time()
trips_by_vendor = silver_df.groupBy("vendor_id").count()
trips_by_vendor.count() # Action to trigger computation
end_time = time.time()
print(f"Action 2 duration (no cache): {end_time - start_time:.2f} seconds")
# display(trips_by_vendor)

# --- Scenario WITH Caching ---
print("\n--- Running aggregations WITH caching ---")

# Cache the DataFrame (using default MEMORY_AND_DISK)
print("Persisting Silver DataFrame...")
silver_df.persist(StorageLevel.MEMORY_AND_DISK)
# Or use: silver_df.cache()

# Trigger an action to materialize the cache. Count is cheap.
print("Triggering action (count) to populate cache...")
start_time = time.time()
initial_count = silver_df.count()
end_time = time.time()
print(f"Initial count and cache population took: {end_time - start_time:.2f} seconds. Count: {initial_count}")
print("Silver DataFrame should now be cached (partially or fully).")

# Action 1 (reads from cache)
start_time = time.time()
avg_fare_by_passenger_cached = silver_df.groupBy("passenger_count").agg(avg("fare_amount").alias("avg_fare"))
avg_fare_by_passenger_cached.count() # Action
end_time = time.time()
print(f"Action 1 duration (with cache): {end_time - start_time:.2f} seconds")
# display(avg_fare_by_passenger_cached)

# Action 2 (reads from cache)
start_time = time.time()
trips_by_vendor_cached = silver_df.groupBy("vendor_id").count()
trips_by_vendor_cached.count() # Action
end_time = time.time()
print(f"Action 2 duration (with cache): {end_time - start_time:.2f} seconds")
# display(trips_by_vendor_cached)

# IMPORTANT: Unpersist the DataFrame when done
print("\nUnpersisting the Silver DataFrame...")
silver_df.unpersist()
print("Cache released.")

# COMMAND ----------

# DBTITLE 1,Adaptive Query Execution (AQE)
# MAGIC %md
# MAGIC ### Adaptive Query Execution (AQE)
# MAGIC
# MAGIC AQE is a powerful Spark SQL optimization framework **enabled by default** in Databricks Runtime. It allows Spark to re-optimize query plans *during execution* based on actual runtime statistics, rather than relying solely on potentially inaccurate compile-time estimates. This often leads to significant performance improvements without manual tuning.
# MAGIC
# MAGIC **Key Features:**
# MAGIC
# MAGIC 1.  **Dynamically Coalescing Shuffle Partitions:**
# MAGIC     * **Problem:** Standard Spark shuffles can create many small partitions, leading to overhead in task scheduling and management, especially if the default `spark.sql.shuffle.partitions` (often 200) is too high for the data size.
# MAGIC     * **AQE Solution:** After a shuffle, AQE can automatically merge adjacent small shuffle partitions into larger, more reasonably sized partitions *before* the next stage starts processing them. This reduces task overhead and can improve resource utilization.
# MAGIC
# MAGIC 2.  **Dynamically Switching Join Strategies:**
# MAGIC     * **Problem:** Spark initially plans a join strategy (e.g., Sort Merge Join) based on estimated table/relation sizes. If these estimates are wrong (e.g., due to complex filters), the chosen strategy might be suboptimal.
# MAGIC     * **AQE Solution:** AQE can monitor the actual size of join relations at runtime. If one side of a join turns out to be much smaller than initially estimated (e.g., below the broadcast threshold), AQE can dynamically switch the join strategy from, say, Sort Merge Join to the more efficient Broadcast Hash Join mid-query.
# MAGIC
# MAGIC 3.  **Dynamically Optimizing Skew Joins:**
# MAGIC     * **Problem:** Data skew occurs when certain join keys have disproportionately more rows than others. In a standard shuffle join, the tasks processing these skewed keys become stragglers, slowing down the entire stage.
# MAGIC     * **AQE Solution:** AQE can detect highly skewed partitions during a shuffle join. It can then split these skewed partitions into smaller sub-partitions and replicate the corresponding rows from the other side of the join if needed, allowing the skewed data to be processed in parallel by multiple tasks, mitigating the straggler effect.
# MAGIC
# MAGIC **How to Observe AQE:**
# MAGIC * AQE operates automatically behind the scenes.
# MAGIC * The best way to see its effects is in the **Spark UI**:
# MAGIC     * Go to the `SQL / DataFrame` tab for your completed query.
# MAGIC     * Click on the query description to view the details.
# MAGIC     * Examine the **query plan graph**. AQE modifications often introduce `AQEShuffleRead` or `AQEQueryStage` nodes. The final executed plan might differ from the initial plan shown.
# MAGIC     * Look at the node details. You might see evidence of coalesced partitions (fewer tasks than initial shuffle partitions) or dynamically switched joins (e.g., a `BroadcastHashJoin` node appearing where a `SortMergeJoin` might have been initially planned). Skew join optimization details might also be visible in task metrics or node descriptions.
# MAGIC

# COMMAND ----------

# Run a query that might benefit from AQE (e.g., a join where filtering might change relation sizes)
print("Running a join query potentially benefiting from AQE...")

# Example: Join Silver data with the daily summary Gold table
query_for_aqe = f"""
SELECT
    s.tpep_pickup_datetime,
    s.pickup_location_id,
    s.total_amount,
    gds.total_trips AS trips_on_pickup_day -- Get total trips for the day from Gold table
FROM {db_name}.{silver_table_name} s
JOIN {db_name}.{gold_table_daily_summary} gds
    ON date_trunc('DAY', s.tpep_pickup_datetime) = gds.trip_date
WHERE s.pickup_year = 2023 AND s.pickup_month = 1 AND s.tip_amount > 10 -- Add some filters
LIMIT 5000 -- Limit output for demo speed
"""

start_time = time.time()
result_df_aqe = spark.sql(query_for_aqe)
result_df_aqe.count() # Action to trigger execution
end_time = time.time()
print(f"AQE Example Query Duration: {end_time - start_time:.2f} seconds")
# display(result_df_aqe)

print("\nACTION REQUIRED: Check the Spark UI -> SQL/DataFrame tab for the query above.")
print("Look for AQE indicators in the query plan graph and node details (e.g., AQEShuffleRead, dynamically switched joins).")


# COMMAND ----------

# DBTITLE 1,Photon Engine
# MAGIC %md
# MAGIC ### Photon Engine
# MAGIC
# MAGIC Photon is Databricks' high-performance, native vectorized query engine, rewritten in C++ to take full advantage of modern CPU architectures. It significantly accelerates Spark SQL and DataFrame workloads by replacing parts of the standard Spark execution engine.
# MAGIC
# MAGIC * **Benefit:** Faster execution times and lower total cost of ownership (TCO) due to reduced cluster usage for the same workload. Performance gains vary depending on the query and data, but often range from 2x to 10x or more. Particularly effective for CPU-intensive operations (aggregations, joins, filters).
# MAGIC * **How to Enable:** Photon is enabled at the **cluster level**. When creating or editing a cluster in the Databricks UI, simply check the **"Use Photon Acceleration"** checkbox. No code changes are required.
# MAGIC * **Compatibility:** Designed to be seamlessly compatible with existing Spark APIs (SQL, DataFrames, Datasets) and Delta Lake. Most operations are automatically accelerated if Photon is enabled.
# MAGIC * **Verification:** In the **Spark UI**, look at the query plan graph for executed queries. Photon-accelerated operations will appear as nodes with "Photon" in their name (e.g., `PhotonScan`, `PhotonHashAggregate`, `PhotonShuffleExchange`). If you see these nodes, Photon is active for those parts of your query.
# MAGIC

# COMMAND ----------

# Check if Photon is enabled for the current cluster session
is_photon_enabled = spark.conf.get("spark.databricks.io.photon.enabled", "false") == "true"

if is_photon_enabled:
    print("Photon Acceleration appears to be ENABLED for this cluster.")
    print("Queries run on this cluster will likely benefit from Photon.")
    print("Run any query (like the AQE example above) and check the Spark UI -> SQL/DataFrame tab for 'Photon' nodes in the query plan.")
else:
    print("Photon Acceleration appears to be DISABLED for this cluster.")
    print("Consider enabling Photon in the cluster configuration for potential performance improvements.")


# COMMAND ----------

# DBTITLE 1,Broadcast Hash Joins (BHJ)
# MAGIC %md
# MAGIC ### Broadcast Hash Joins (BHJ)
# MAGIC
# MAGIC A highly efficient join algorithm used when one side of the join (typically a dimension table) is small enough to fit entirely into the memory of each executor node on the cluster.
# MAGIC
# MAGIC * **How it Works:**
# MAGIC     1.  The smaller table is collected by the Spark driver.
# MAGIC     2.  The driver *broadcasts* this entire small table to every executor node working on the larger table.
# MAGIC     3.  Each executor builds an in-memory hash table from the broadcasted small table.
# MAGIC     4.  It then streams through its local partitions of the *large* table, probing the hash table for matches using the join key.
# MAGIC * **Benefit:** Avoids a costly *shuffle* operation on the large table, which is often the most expensive part of other join strategies like Sort Merge Join.
# MAGIC * **Automatic Broadcasting:** Spark automatically attempts to use BHJ if it estimates one side of the join is smaller than the threshold defined by `spark.sql.autoBroadcastJoinThreshold` (default is typically 10MB, but can be configured). AQE can also dynamically switch to BHJ at runtime if estimates were initially wrong.
# MAGIC * **Manual Hints:** If you know a table is small enough to broadcast, but Spark might not realize it (e.g., stale statistics), you can explicitly request a broadcast join using hints:
# MAGIC     * **Python DataFrame API:** Use the `broadcast()` function around the small DataFrame in the `join()` call: `large_df.join(broadcast(small_df), "join_key")`.
# MAGIC     * **SQL:** Use a hint comment: `SELECT /*+ BROADCAST(small_table_alias) */ * FROM large_table l JOIN small_table s ON l.key = s.key`. (Replace `small_table_alias` with the actual alias or table name).
# MAGIC

# COMMAND ----------

# Example: Joining Silver (large) with Gold Daily Summary (likely small)
# Assume gold_daily_summary is small enough to broadcast.

large_table = f"{db_name}.{silver_table_name}"
small_table = f"{db_name}.{gold_table_daily_summary}"

print(f"Attempting join between {large_table} (large) and {small_table} (small).")

# --- Method 1: Using SQL Hint ---
print("\nRunning join with SQL BROADCAST hint...")
sql_hint_query = f"""
SELECT /*+ BROADCAST(gds) */
    s.tpep_pickup_datetime,
    s.pickup_location_id,
    s.total_amount,
    gds.total_trips AS total_trips_on_pickup_day
FROM {large_table} s
JOIN {small_table} gds -- Alias 'gds' used in hint
    ON date_trunc('DAY', s.tpep_pickup_datetime) = gds.trip_date
WHERE s.pickup_year = 2023 AND s.pickup_month = 1 AND s.passenger_count = 1 -- Add filters
LIMIT 1000
"""
start_time = time.time()
spark.sql(sql_hint_query).count() # Action
end_time = time.time()
print(f"SQL Hint Query Duration: {end_time - start_time:.2f} seconds")
# display(spark.sql(sql_hint_query))
print("Check Spark UI -> SQL/DataFrame tab. The join node should indicate 'BroadcastHashJoin'.")


# --- Method 2: Using Python DataFrame API Hint ---
print("\nRunning join with Python broadcast() hint...")
large_df = spark.table(large_table).filter("pickup_year = 2023 AND pickup_month = 1 AND passenger_count = 2") # Filter large table first!
small_df = spark.table(small_table)

start_time = time.time()
# Apply broadcast() hint to the small DataFrame
joined_df_broadcast = large_df.join(
        broadcast(small_df),
        date_trunc('DAY', large_df["tpep_pickup_datetime"]) == small_df["trip_date"],
        "inner"
    ).select(
        large_df.tpep_pickup_datetime,
        large_df.pickup_location_id,
        large_df.total_amount,
        small_df.total_trips.alias("total_trips_on_pickup_day")
    ).limit(1000)

joined_df_broadcast.count() # Action
end_time = time.time()
print(f"Python Hint Query Duration: {end_time - start_time:.2f} seconds")
# display(joined_df_broadcast)
print("Check Spark UI -> SQL/DataFrame tab. The join node should indicate 'BroadcastHashJoin'.")


# COMMAND ----------

# DBTITLE 1,Analyzing Query Plans (`.explain()`)
# MAGIC %md
# MAGIC ### Analyzing Query Plans (`.explain()`)
# MAGIC
# MAGIC Understanding how Spark intends to execute your query is crucial for diagnosing performance issues and verifying optimizations. The `.explain()` method on a DataFrame or `EXPLAIN` command in SQL reveals the query plan.
# MAGIC
# MAGIC * **Usage:**
# MAGIC     * DataFrame API: `your_dataframe.explain(mode="<mode>")`
# MAGIC     * SQL: `EXPLAIN [FORMATTED | EXTENDED | ...] SELECT ...`
# MAGIC * **Explain Modes:**
# MAGIC     * `simple` (Default if mode omitted): Shows the physical plan.
# MAGIC     * `extended`: Shows logical plans (parsed, analyzed, optimized) and the physical plan. Very detailed.
# MAGIC     * `formatted`: Provides a more readable, structured view of the physical plan, often including node details and metrics if available after execution (e.g., via `spark.sql("EXPLAIN FORMATTED SELECT ...")`).
# MAGIC     * `codegen`: Shows the generated Java bytecode (advanced).
# MAGIC     * `cost`: Shows the optimized logical plan along with cost estimates (if statistics are available).
# MAGIC * **What to Look For in the Physical Plan (`simple` or `formatted`):**
# MAGIC     * **Scan Nodes (e.g., `FileScan parquet`, `DeltaScan`):** Check for applied filters:
# MAGIC         * `PartitionFilters: [...]`: Shows filters applied to partition columns. Indicates partition pruning is working. Essential for performance on partitioned tables.
# MAGIC         * `PushedFilters: [...]`: Shows filters pushed down to the data source (e.g., Parquet reader, Delta reader). Indicates filtering is happening efficiently *before* data is loaded into memory.
# MAGIC     * **Filter Nodes:** Operations that couldn't be pushed down.
# MAGIC     * **Join Nodes:** Identify the type: `BroadcastHashJoin`, `SortMergeJoin`, `ShuffleHashJoin`, `BroadcastNestedLoopJoin`. Ensure the most efficient strategy is used (often BHJ or SMJ).
# MAGIC     * **Exchange Nodes (e.g., `ShuffleExchange`, `BroadcastExchange`):** Indicate data shuffling across the network (expensive) or broadcasting. Minimize shuffles where possible. AQE might add `AQEShuffleRead`.
# MAGIC     * **Aggregate Nodes (e.g., `HashAggregate`, `SortAggregate`):** Show how aggregations are performed.
# MAGIC

# COMMAND ----------

# Example 1: Explain the broadcast join query from before
print("--- Explain Plan for Broadcast Join Query (Formatted) ---")
# Using SQL EXPLAIN FORMATTED
spark.sql(f"EXPLAIN FORMATTED {sql_hint_query}").show(truncate=False, n=1000)
# Look for BroadcastHashJoin, BroadcastExchange, PushedFilters, PartitionFilters in the output.

# COMMAND ----------

# Example 2: Explain a simple filter query on the Silver table
print("\n--- Explain Plan for Filter Query on Silver Table (Extended) ---")
filter_query_df = spark.table(silver_table_name)\
    .filter("pickup_year = 2023 AND pickup_month = 1 AND trip_distance > 10")\
    .select("vendor_id", "trip_distance", "total_amount", "pickup_location_id")

# Using DataFrame explain("extended")
filter_query_df.explain(mode="extended")
# Look through the different plan stages (Parsed, Analyzed, Optimized, Physical).
# In the Physical Plan, look for DeltaScan or FileScan nodes and check PartitionFilters and PushedFilters.


# COMMAND ----------

