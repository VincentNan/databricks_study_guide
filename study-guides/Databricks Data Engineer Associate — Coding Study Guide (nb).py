# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Overview — Coding Study Guide
# MAGIC %md
# MAGIC # Databricks Certified Data Engineer Associate — Coding Study Guide
# MAGIC
# MAGIC Runnable notebook covering **Python, PySpark & SQL** coding objectives from the official exam guide (May 4, 2026 version): ingestion, data cleaning, DataFrame transformations, joins, aggregations, Delta tables, Lakeflow Jobs, CI/CD, performance tuning, and Unity Catalog security.
# MAGIC
# MAGIC All examples use the existing **`lab_uc`** catalog in this workspace (schemas `bronze`, `silver`, `gold`), with fresh table names so earlier lab outputs are not overwritten.
# MAGIC
# MAGIC ## Study priorities
# MAGIC
# MAGIC | Priority | Topics |
# MAGIC | --- | --- |
# MAGIC | Highest | PySpark DataFrames, SQL transformations, joins, aggregations, deduplication |
# MAGIC | High | Auto Loader, COPY INTO, Delta Lake, incremental loading |
# MAGIC | High | Lakeflow Jobs, notebook parameters, task dependencies |
# MAGIC | Medium | Databricks CLI, bundle YAML, Git |
# MAGIC | Medium | Performance configuration, Unity Catalog SQL permissions |
# MAGIC
# MAGIC ## Contents
# MAGIC
# MAGIC 1. Setup — sample data
# MAGIC 2. Notebook essentials
# MAGIC 3. Write modes
# MAGIC 4. COPY INTO and Auto Loader ingestion
# MAGIC 5. Data cleaning — Bronze to Silver
# MAGIC 6. Joins
# MAGIC 7. Aggregations and window functions
# MAGIC 8. Nested JSON and arrays
# MAGIC 9. Delta Lake — MERGE, UPDATE, DELETE, history
# MAGIC 10. Gold layer — views, materialized views, streaming tables
# MAGIC 11. Data quality checks
# MAGIC 12. Notebook parameters and Lakeflow Jobs
# MAGIC 13. CI/CD — bundles and CLI
# MAGIC 14. Performance tuning
# MAGIC 15. Unity Catalog — SQL security
# MAGIC 16. Hands-on exercises
# MAGIC 17. Seven-day study plan
# MAGIC 18. Optional cleanup
# MAGIC
# MAGIC **Run the cells top to bottom.** Cells that require a job context, a declarative pipeline, or workspace groups are included as reference and are not executed; the task-values cell 12.2 runs but safely self-annotates outside a job run. The materialized view cell (10.2) executes but self-annotates on serverless notebook compute, where MV creation requires a serverless SQL warehouse instead.
# MAGIC
# MAGIC **Validated on serverless compute.** Cells 1 and 5.1 create their tables only when absent (nothing pre-existing is overwritten); drop the study tables first to rebuild them on a re-run.

# COMMAND ----------

# DBTITLE 1,1. Setup — sample data
# Ensure schemas exist
spark.sql("CREATE SCHEMA IF NOT EXISTS lab_uc.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS lab_uc.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS lab_uc.gold")

from pyspark.sql import functions as F

# Bronze orders_raw: raw strings, messy statuses, a NULL amount, a NULL key, and a duplicate order_id
bronze_orders = spark.createDataFrame(
    [
        (1001, 501, "125.50", " completed "),
        (1002, 502, "invalid", "PENDING"),
        (1003, 503, None, "completed"),
        (1003, 503, "99.99", "completed"),
        (1004, 504, "75.25", "Completed"),
        (None, 505, "10.00", "completed"),
    ],
    "order_id BIGINT, customer_id BIGINT, amount STRING, status STRING",
)


# Create the study tables only if they do not exist yet (nothing pre-existing is overwritten)
def save_fresh(df, name):
    schema = ".".join(name.split(".")[:-1])
    tbl = name.split(".")[-1]
    if spark.sql(f"SHOW TABLES IN {schema} LIKE '{tbl}'").count():
        print(f"{name} already exists — left unchanged")
    else:
        # 'ignore' = write only if absent. (Note: 'errorifexists' is not
        # supported by saveAsTable on Spark Connect / serverless compute.)
        df.write.format("delta").mode("ignore").saveAsTable(name)


save_fresh(bronze_orders, "lab_uc.bronze.orders_raw")

# Silver customers — small dimension table, later used for joins and broadcast
customers = spark.createDataFrame(
    [
        (501, "Alice", "EAST"),
        (502, "Bao", "WEST"),
        (503, "Carla", "EAST"),
        (504, "Dan", "WEST"),
        (505, "Eve", "NORTH"),  # no orders — useful for join demos
    ],
    "customer_id BIGINT, customer_name STRING, region STRING",
)
save_fresh(customers, "lab_uc.silver.customers")

# Bronze customer updates — multiple rows per customer, used for window functions and MERGE
customer_updates = spark.createDataFrame(
    [
        (501, "Alice Smith", "2026-10-10 12:00:00"),
        (501, "Alice (old)", "2026-09-01 08:00:00"),
        (506, "Frank", "2026-10-11 09:00:00"),  # brand-new customer
    ],
    "customer_id BIGINT, customer_name STRING, updated_at STRING",
)
save_fresh(customer_updates, "lab_uc.bronze.customer_updates")

display(spark.table("lab_uc.bronze.orders_raw"))

# COMMAND ----------

# DBTITLE 1,2. Notebook essentials
# MAGIC %md
# MAGIC ## 2. Databricks notebook essentials
# MAGIC
# MAGIC | Syntax | Purpose |
# MAGIC | --- | --- |
# MAGIC | `%python` | Run a Python cell |
# MAGIC | `%sql` | Run a SQL cell |
# MAGIC | `%md` | Render Markdown documentation |
# MAGIC | `spark.sql(...)` | Execute SQL from Python |
# MAGIC | `spark.table(...)` | Read a table as a DataFrame |
# MAGIC | `display(df)` | Display a DataFrame in a notebook |
# MAGIC | `dbutils.widgets` | Read notebook parameters |
# MAGIC
# MAGIC **Exam tip:** `spark.sql()` returns a **Spark DataFrame** — it does not automatically convert results into a Python list or pandas DataFrame.

# COMMAND ----------

# DBTITLE 1,2.1 Reading tables — three equivalent ways
# All three read the same governed table as a DataFrame
df1 = spark.table("lab_uc.bronze.orders_raw")
df2 = spark.read.table("lab_uc.bronze.orders_raw")
df3 = spark.sql("SELECT * FROM lab_uc.bronze.orders_raw")

print("All equal:", df1.count() == df2.count() == df3.count())
display(df1.limit(10))

# COMMAND ----------

# DBTITLE 1,2.2 Run SQL from Python
df = spark.sql("""
    SELECT customer_id, COUNT(*) AS order_count
    FROM lab_uc.bronze.orders_raw
    GROUP BY customer_id
""")
display(df)
print(type(df))  # PySpark DataFrame, not pandas

# COMMAND ----------

# DBTITLE 1,3. Write modes
# MAGIC %md
# MAGIC ## 3. Write modes — reading and writing Delta tables
# MAGIC
# MAGIC | Mode | Behavior |
# MAGIC | --- | --- |
# MAGIC | `append` | Add rows to an existing table |
# MAGIC | `overwrite` | Replace existing table data |
# MAGIC | `errorifexists` | Fail if the destination already exists |
# MAGIC | `ignore` | Do nothing if the destination already exists |
# MAGIC
# MAGIC **Remember:** `overwrite` is **not** an upsert. For updating existing business keys while inserting new ones, use `MERGE`.

# COMMAND ----------

# DBTITLE 1,3.1 Write modes in action
demo = spark.table("lab_uc.bronze.orders_raw").limit(2)

# overwrite: creates the table, replacing any existing data
demo.write.format("delta").mode("overwrite").saveAsTable(
    "lab_uc.silver.write_mode_demo"
)

# append: adds the 2 rows again (now 4)
demo.write.format("delta").mode("append").saveAsTable("lab_uc.silver.write_mode_demo")

# ignore: table exists, so nothing happens (still 4)
demo.write.format("delta").mode("ignore").saveAsTable("lab_uc.silver.write_mode_demo")

# errorifexists: fails when the destination already exists.
# (On serverless / Spark Connect compute, saveAsTable does not support this
# mode at all and raises UNSUPPORTED_OPERATION — either way, no data is written.)
try:
    demo.write.format("delta").mode("errorifexists").saveAsTable(
        "lab_uc.silver.write_mode_demo"
    )
    print("No failure — the table did not exist")
except Exception as e:
    print("errorifexists did not write ->", type(e).__name__)

display(spark.sql("SELECT COUNT(*) AS rows FROM lab_uc.silver.write_mode_demo"))

# COMMAND ----------

# DBTITLE 1,4. COPY INTO and Auto Loader ingestion
# MAGIC %md
# MAGIC ## 4. COPY INTO and Auto Loader ingestion
# MAGIC
# MAGIC Two incremental file-ingestion approaches, both idempotent:
# MAGIC
# MAGIC | Mechanism | Use |
# MAGIC | --- | --- |
# MAGIC | `COPY INTO` | Idempotent SQL command — loads new files, skips already-loaded ones |
# MAGIC | Auto Loader (`cloudFiles`) | Scalable incremental ingestion built on Structured Streaming |
# MAGIC | `availableNow=True` | Process available files, then terminate |
# MAGIC | Lakeflow Connect | Managed connectors for supported external systems |
# MAGIC
# MAGIC **Exam tip:** `COPY INTO` suits smaller numbers of files; Auto Loader scales better for large or continuously arriving data.

# COMMAND ----------

# DBTITLE 1,4.1 COPY INTO — stage CSV files
# 4.1 COPY INTO — stage CSV files

# Stage two small CSV batches in the existing landing volume
csv_path = "/Volumes/lab_uc/bronze/landing/study_orders_csv/"
dbutils.fs.mkdirs(csv_path)

dbutils.fs.put(
    csv_path + "batch1.csv",
    "order_id,customer_id,amount\n2001,501,40.00\n2002,502,15.25\n",
    True,
)
dbutils.fs.put(
    csv_path + "batch2.csv",
    "order_id,customer_id,amount\n2003,503,60.75\n2004,504,22.10\n",
    True,
)
display(dbutils.fs.ls(csv_path))

# COMMAND ----------

# DBTITLE 1,4.2 COPY INTO — incremental file loading
# MAGIC %sql
# MAGIC -- 4.2 COPY INTO — incremental file loading
# MAGIC CREATE TABLE IF NOT EXISTS lab_uc.bronze.orders_csv (
# MAGIC     order_id BIGINT,
# MAGIC     customer_id BIGINT,
# MAGIC     amount DOUBLE
# MAGIC   ) USING DELTA;
# MAGIC
# MAGIC -- With 'inferSchema' = 'false' the CSV values arrive as STRING, and COPY INTO
# MAGIC -- refuses to merge STRING into the typed columns (DELTA_FAILED_TO_MERGE_FIELDS).
# MAGIC -- Read from a SELECT that casts each column explicitly instead.
# MAGIC COPY INTO lab_uc.bronze.orders_csv FROM (
# MAGIC   SELECT
# MAGIC     CAST(order_id AS BIGINT) AS order_id,
# MAGIC     CAST(customer_id AS BIGINT) AS customer_id,
# MAGIC     CAST(amount AS DOUBLE) AS amount FROM '/Volumes/lab_uc/bronze/landing/study_orders_csv/'
# MAGIC ) FILEFORMAT = CSV FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false') COPY_OPTIONS (
# MAGIC   'mergeSchema' = 'false'
# MAGIC );
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   lab_uc.bronze.orders_csv;
# MAGIC
# MAGIC -- Run the COPY INTO again: it is idempotent — previously loaded files are skipped
# MAGIC -- (the SELECT below still shows only 4 rows)
# MAGIC COPY INTO lab_uc.bronze.orders_csv FROM (
# MAGIC   SELECT
# MAGIC     CAST(order_id AS BIGINT) AS order_id,
# MAGIC     CAST(customer_id AS BIGINT) AS customer_id,
# MAGIC     CAST(amount AS DOUBLE) AS amount FROM '/Volumes/lab_uc/bronze/landing/study_orders_csv/'
# MAGIC ) FILEFORMAT = CSV FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false') COPY_OPTIONS (
# MAGIC   'mergeSchema' = 'false'
# MAGIC );
# MAGIC
# MAGIC SELECT
# MAGIC   COUNT(*) AS rows_after_second_copy
# MAGIC FROM
# MAGIC   lab_uc.bronze.orders_csv;

# COMMAND ----------

# DBTITLE 1,4.3 COPY INTO vs Auto Loader — key distinction
# 4.3 COPY INTO vs Auto Loader — key distinction

# COPY INTO:       convenient, idempotent incremental loading via a SQL command;
#                 skips previously loaded files.
# Auto Loader:     scalable incremental file discovery and processing using
#                 Structured Streaming (the `cloudFiles` source).
# Lakeflow Connect: managed and standard connector options for supported sources.

# Live check of both approaches from 4.2 / 4.4:
print("orders_csv (COPY INTO, idempotent) ->", spark.table("lab_uc.bronze.orders_csv").count(), "rows")
print("events_raw (Auto Loader stream)    ->", spark.table("lab_uc.bronze.events_raw").count(), "rows")

# Auto Loader options to know (used live in 4.4):
#   cloudFiles.format              -> source file format
#   cloudFiles.schemaLocation      -> persist inferred/evolved schema
#   cloudFiles.schemaEvolutionMode -> control behavior when new columns appear
#   checkpointLocation             -> persist streaming execution progress
#   availableNow=True              -> process available data, then terminate

# COMMAND ----------

# DBTITLE 1,4.4 Auto Loader — availableNow trigger
# 4.4 Auto Loader — availableNow trigger

# Stage JSON files for the Auto Loader example
json_path = "/Volumes/lab_uc/bronze/landing/study_orders_json/"
schema_path = "/Volumes/lab_uc/bronze/landing/study_schema/"
checkpoint_path = "/Volumes/lab_uc/bronze/landing/study_checkpoint/"

dbutils.fs.mkdirs(json_path)
dbutils.fs.put(json_path + "event1.json", '{"event": "login", "user": "alice"}', True)
dbutils.fs.put(json_path + "event2.json", '{"event": "purchase", "user": "bao"}', True)

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_path)
    .load(json_path)
)

query = (
    df.writeStream.format("delta")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)  # process available data, then stop
    .toTable("lab_uc.bronze.events_raw")
)

query.awaitTermination()
display(spark.table("lab_uc.bronze.events_raw"))

# COMMAND ----------

# DBTITLE 1,4.5 Schema evolution (reference)
# 4.5 Schema evolution (reference)

json_path = "/Volumes/lab_uc/bronze/landing/study_orders_json/"
schema_path = "/Volumes/lab_uc/bronze/landing/study_schema/"

# Same Auto Loader source as 4.4, but with schema evolution enabled.
# Building the streaming DataFrame does not start a query or write data.
evolving = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(json_path)
)

print("streaming DataFrame:", evolving.isStreaming)
print("inferred schema:", evolving.schema.simpleString())

# With addNewColumns, a new field generally causes Auto Loader to UPDATE the tracked
# schema and STOP the stream; a restart continues with the updated schema.
# Important distinction: schema evolution does NOT rewrite historical records.

# The destination table and paths must be accessible to the compute identity.
# In production, use dedicated, persistent checkpoint and schema directories.

# COMMAND ----------

# DBTITLE 1,5. Data cleaning — Bronze to Silver
# MAGIC %md
# MAGIC ## 5. Data cleaning — Bronze to Silver
# MAGIC
# MAGIC The Bronze table `lab_uc.bronze.orders_raw` contains raw strings:
# MAGIC
# MAGIC | order_id | customer_id | amount | status |
# MAGIC | --- | --- | --- | --- |
# MAGIC | 1001 | 501 | `"125.50"` | `" completed "` |
# MAGIC | 1002 | 502 | `"invalid"` | `"PENDING"` |
# MAGIC | 1003 | 503 | `NULL` | `"completed"` |
# MAGIC | 1003 | 503 | `"99.99"` | `"completed"` (duplicate key) |
# MAGIC | ... | ... | ... | ... |
# MAGIC
# MAGIC PySpark cleaning methods to recognize:
# MAGIC
# MAGIC | Syntax | Meaning |
# MAGIC | --- | --- |
# MAGIC | `withColumn()` | Add or replace a column |
# MAGIC | `col()` | Reference a column |
# MAGIC | `trim()` | Remove leading/trailing spaces |
# MAGIC | `upper()` | Convert text to uppercase |
# MAGIC | `filter()` | Keep rows satisfying a condition |
# MAGIC | `isNotNull()` | Check for non-NULL values |
# MAGIC | `fillna()` | Replace NULL values |
# MAGIC | `dropDuplicates()` | Remove duplicate rows by selected columns |
# MAGIC
# MAGIC **Exam trap:** `dropDuplicates(["order_id"])` does **not** guarantee the latest record is kept. Use a window function when ordering matters.

# COMMAND ----------

# DBTITLE 1,5.1 Clean with PySpark
bronze = spark.table("lab_uc.bronze.orders_raw")

silver = (
    bronze.withColumn(
        "amount", F.expr("try_cast(amount AS DOUBLE)")
    )  # NULL instead of error for bad values
    .withColumn("status", F.upper(F.trim(F.col("status"))))
    .filter(F.col("order_id").isNotNull())
    .fillna({"amount": 0.0})
    .dropDuplicates(["order_id"])
)

# Create orders_clean only if it does not exist yet (nothing pre-existing is overwritten).
# On a re-run, drop lab_uc.silver.orders_clean first to rebuild it.
if spark.sql("SHOW TABLES IN lab_uc.silver LIKE 'orders_clean'").count():
    print("lab_uc.silver.orders_clean already exists — left unchanged")
else:
    silver.write.format("delta").mode("ignore").saveAsTable(
        "lab_uc.silver.orders_clean"
    )
display(spark.table("lab_uc.silver.orders_clean"))

# COMMAND ----------

# DBTITLE 1,5.2 Equivalent SQL cleaning
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lab_uc.silver.orders_sql AS
# MAGIC SELECT DISTINCT
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   COALESCE(TRY_CAST(amount AS DOUBLE), 0.0) AS amount,
# MAGIC   UPPER(TRIM(status)) AS status
# MAGIC FROM
# MAGIC   lab_uc.bronze.orders_raw
# MAGIC WHERE
# MAGIC   order_id IS NOT NULL;
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   lab_uc.silver.orders_sql
# MAGIC ORDER BY
# MAGIC   order_id;
# MAGIC
# MAGIC -- Note: DISTINCT removes identical selected rows; it does NOT guarantee one row per order_id
# MAGIC -- if other column values differ.

# COMMAND ----------

# DBTITLE 1,5.3 Column operations cheat sheet — live
df = spark.table("lab_uc.silver.customers")

display(
    df.select("customer_id", "customer_name", "region")  # select
    .withColumnRenamed("customer_name", "name")  # rename
    .withColumn("name_parts", F.split(F.col("name"), " "))  # split -> array
    .withColumn("first_name", F.col("name_parts").getItem(0))
    .drop("name_parts")  # drop column
    .withColumn("region_lower", F.col("region").cast("string"))  # cast
    .orderBy(F.col("customer_id").desc())  # sort
)

# filter, withColumn with a literal, explode — see the nested-JSON section below

# COMMAND ----------

# DBTITLE 1,6. Joins
# MAGIC %md
# MAGIC ## 6. Joins
# MAGIC
# MAGIC | Join | Returned rows |
# MAGIC | --- | --- |
# MAGIC | `inner` | Matching rows from both sides |
# MAGIC | `left` | All left rows and matching right rows |
# MAGIC | `right` | All right rows and matching left rows |
# MAGIC | `full` | All rows from either side |
# MAGIC | `left_semi` | Left rows **having** a match |
# MAGIC | `left_anti` | Left rows **without** a match |
# MAGIC | `cross` | Cartesian product of both datasets |

# COMMAND ----------

# DBTITLE 1,6.1 PySpark join demos
orders = spark.table("lab_uc.silver.orders_clean")
customers = spark.table("lab_uc.silver.customers")

inner_join = orders.join(customers, on="customer_id", how="inner")
left_join = orders.join(customers, "customer_id", "left")
full_join = orders.join(customers, "customer_id", "full")
semi_join = orders.join(
    customers, "customer_id", "left_semi"
)  # orders WITH a matching customer
anti_join = orders.join(
    customers, "customer_id", "left_anti"
)  # orders WITHOUT a matching customer

print(
    "inner:",
    inner_join.count(),
    "| left:",
    left_join.count(),
    "| full:",
    full_join.count(),
)
print("left_semi:", semi_join.count(), "| left_anti:", anti_join.count())
display(inner_join)

# COMMAND ----------

# DBTITLE 1,6.2 Multiple keys and broadcast join
from pyspark.sql.functions import broadcast

# Multiple join keys: pass a list to `on`
result = orders.join(
    customers, on=["customer_id"], how="inner"
)  # extend the list for multi-key joins

# Broadcast join: force a small table to every executor to avoid a shuffle
result = orders.join(
    broadcast(customers),  # customers is tiny; orders could be millions of rows
    "customer_id",
    "inner",
)
display(result)

# COMMAND ----------

# DBTITLE 1,6.3 SQL join, UNION vs UNION ALL
# MAGIC %sql
# MAGIC SELECT
# MAGIC   o.order_id,
# MAGIC   o.amount,
# MAGIC   c.customer_name
# MAGIC FROM
# MAGIC   lab_uc.silver.orders_clean AS o
# MAGIC     INNER JOIN lab_uc.silver.customers AS c
# MAGIC       ON o.customer_id = c.customer_id
# MAGIC ORDER BY
# MAGIC   o.order_id;
# MAGIC
# MAGIC -- UNION removes duplicate result rows
# MAGIC SELECT
# MAGIC   customer_id
# MAGIC FROM
# MAGIC   lab_uc.bronze.orders_raw
# MAGIC UNION
# MAGIC SELECT
# MAGIC   customer_id
# MAGIC FROM
# MAGIC   lab_uc.silver.customers
# MAGIC ORDER BY
# MAGIC   customer_id;
# MAGIC
# MAGIC -- UNION ALL preserves duplicates
# MAGIC SELECT
# MAGIC   customer_id
# MAGIC FROM
# MAGIC   lab_uc.bronze.orders_raw
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   customer_id
# MAGIC FROM
# MAGIC   lab_uc.silver.customers
# MAGIC ORDER BY
# MAGIC   customer_id;

# COMMAND ----------

# DBTITLE 1,6.4 PySpark union trap
df1 = spark.createDataFrame([(1,), (1,), (2,)], "customer_id BIGINT")
df2 = spark.createDataFrame([(1,), (3,)], "customer_id BIGINT")

combined = df1.unionByName(df2)  # preserves duplicates (5 rows)
distinct_combined = combined.distinct()  # 3 rows — like SQL UNION

print(
    "unionByName:",
    combined.count(),
    "rows | after distinct:",
    distinct_combined.count(),
    "rows",
)

# Exam trap: DataFrame.union() and unionByName() preserve duplicates.
# They behave like SQL UNION ALL — apply .distinct() to behave like SQL UNION.

# COMMAND ----------

# DBTITLE 1,7. Aggregations and window functions
# MAGIC %md
# MAGIC ## 7. Aggregations and window functions
# MAGIC
# MAGIC | Syntax | Purpose |
# MAGIC | --- | --- |
# MAGIC | `groupBy(...).agg(...)` | Aggregate per group |
# MAGIC | `count / sum / avg / min / max` | Standard aggregate functions |
# MAGIC | `approx_count_distinct` | Faster approximate distinct count |
# MAGIC | `Window.partitionBy(...).orderBy(...)` | Define a window specification |
# MAGIC | `row_number / rank / dense_rank / lag / lead` | Window functions |
# MAGIC
# MAGIC **Exam trap:** `dropDuplicates()` does not guarantee the latest row is kept. To get the latest record per key, use `row_number()` over a window ordered by the timestamp descending and keep `rn = 1` (see 7.3).

# COMMAND ----------

# DBTITLE 1,7.1 PySpark aggregations
# 7.1 PySpark aggregations

summary = (
    spark.table("lab_uc.silver.orders_clean")
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("order_count"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount"),
        F.max("amount").alias("max_amount"),
        F.approx_count_distinct("order_id").alias("approx_orders"),
    )
)
display(summary)

# COMMAND ----------

# DBTITLE 1,7.2 SQL aggregation equivalent
# MAGIC %sql
# MAGIC -- 7.2 SQL aggregation equivalent
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   COUNT(*) AS order_count,
# MAGIC   SUM(amount) AS total_amount,
# MAGIC   AVG(amount) AS avg_amount,
# MAGIC   MAX(amount) AS max_amount,
# MAGIC   APPROX_COUNT_DISTINCT(order_id) AS approx_orders
# MAGIC FROM
# MAGIC   lab_uc.silver.orders_clean
# MAGIC GROUP BY
# MAGIC   customer_id
# MAGIC ORDER BY
# MAGIC   customer_id;

# COMMAND ----------

# DBTITLE 1,7.3 Window function — latest record per customer
# 7.3 Window function — latest record per customer

from pyspark.sql.window import Window

updates = spark.table("lab_uc.bronze.customer_updates")

w = Window.partitionBy("customer_id").orderBy(
    F.col("updated_at").desc(), F.col("sequence_id").desc()
)

# sequence_id is a deterministic tie-breaker; add it because updated_at alone can tie
updates = updates.withColumn("sequence_id", F.monotonically_increasing_id())

latest = (
    updates.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
)

display(latest)

# Function comparison:
#   ROW_NUMBER() -> unique sequence number (ties broken arbitrarily)
#   RANK()       -> same rank for ties, gaps afterward (1,1,3)
#   DENSE_RANK() -> same rank for ties, no gaps (1,1,2)
#   LAG()/LEAD() -> access previous/following row in the window

# COMMAND ----------

# DBTITLE 1,7.4 SQL window equivalent
# MAGIC %sql
# MAGIC -- 7.4 SQL window equivalent
# MAGIC WITH ranked AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS rn
# MAGIC   FROM
# MAGIC     lab_uc.bronze.customer_updates
# MAGIC )
# MAGIC SELECT
# MAGIC   * EXCEPT (rn)
# MAGIC FROM
# MAGIC   ranked
# MAGIC WHERE
# MAGIC   rn = 1;

# COMMAND ----------

# DBTITLE 1,8. Nested JSON and arrays
# MAGIC %md
# MAGIC ## 8. Nested JSON and arrays
# MAGIC
# MAGIC | Syntax | Purpose |
# MAGIC | --- | --- |
# MAGIC | `explode()` | Expand an array into one row per element |
# MAGIC | `F.col("item.product_id")` | Access a field inside a struct |
# MAGIC | `LATERAL VIEW EXPLODE` | SQL equivalent of `explode()` |
# MAGIC | `ARRAY<STRUCT<...>>` | Spark SQL type for nested data |
# MAGIC
# MAGIC **Exam tip:** `explode()` produces one row per array element; the struct fields inside each element are then accessed with dot notation.

# COMMAND ----------

# DBTITLE 1,8.1 Nested JSON and arrays — explode
# 8.1 Nested JSON and arrays — explode

# One order with a nested items array — as it would arrive from JSON
nested = spark.createDataFrame(
    [
        (
            1001,
            501,
            [
                {"product_id": 10, "quantity": 2},
                {"product_id": 20, "quantity": 1},
            ],
        )
    ],
    "order_id BIGINT, customer_id BIGINT, items ARRAY<STRUCT<product_id INT, quantity INT>>",
)

# Persist so the SQL LATERAL VIEW example can read it
nested.write.format("delta").mode("overwrite").saveAsTable(
    "lab_uc.bronze.orders_nested"
)

# explode() expands an array into rows — one row per item
flattened = nested.select(
    "order_id", "customer_id", F.explode("items").alias("item")
).select(
    "order_id",
    "customer_id",
    F.col("item.product_id").alias("product_id"),
    F.col("item.quantity").alias("quantity"),
)
display(flattened)

# Remember: explode() expands an array or map into rows.
# split() is different — it turns a STRING into an array.

# COMMAND ----------

# DBTITLE 1,8.2 SQL — LATERAL VIEW EXPLODE
# MAGIC %sql
# MAGIC -- 8.2 SQL — LATERAL VIEW EXPLODE
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   item.product_id,
# MAGIC   item.quantity
# MAGIC FROM
# MAGIC   lab_uc.bronze.orders_nested
# MAGIC   LATERAL VIEW EXPLODE(items) exploded AS item;

# COMMAND ----------

# DBTITLE 1,9. Delta Lake — MERGE, UPDATE, DELETE, history
# MAGIC %md
# MAGIC ## 9. Delta Lake — MERGE, UPDATE, DELETE, history
# MAGIC
# MAGIC * **`MERGE INTO`** is the upsert: update matching business keys, insert new ones and perform a DELETE action.
# MAGIC * Before merging, ensure the source has **at most one applicable update for each target row** (see the window-function dedup in 7.3).
# MAGIC * Time travel requires the relevant historical log and data files to remain available — `VACUUM` can remove files needed by older versions.

# COMMAND ----------

# DBTITLE 1,9.1 MERGE — upsert customers
# MAGIC %sql
# MAGIC MERGE INTO
# MAGIC   lab_uc.silver.customers AS target
# MAGIC USING (
# MAGIC   -- dedupe updates first: at most one row per customer_id
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     updated_at
# MAGIC   FROM
# MAGIC     (
# MAGIC       SELECT
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS rn
# MAGIC       FROM
# MAGIC         lab_uc.bronze.customer_updates
# MAGIC     )
# MAGIC   WHERE
# MAGIC     rn = 1
# MAGIC ) AS source
# MAGIC ON
# MAGIC   target.customer_id = source.customer_id
# MAGIC WHEN MATCHED THEN UPDATE SET target.customer_name = source.customer_name
# MAGIC WHEN NOT MATCHED THEN INSERT (customer_id, customer_name)
# MAGIC   VALUES (source.customer_id, source.customer_name);
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   lab_uc.silver.customers
# MAGIC ORDER BY
# MAGIC   customer_id;
# MAGIC -- Alice -> updated to 'Alice Smith' (501); Frank (506) -> inserted

# COMMAND ----------

# DBTITLE 1,9.2 UPDATE and DELETE
# MAGIC %sql
# MAGIC UPDATE
# MAGIC   lab_uc.silver.orders_clean
# MAGIC SET
# MAGIC   status = 'CANCELLED'
# MAGIC WHERE
# MAGIC   order_id = 1001;
# MAGIC
# MAGIC DELETE FROM
# MAGIC   lab_uc.silver.orders_clean
# MAGIC WHERE
# MAGIC   amount < 20;
# MAGIC
# MAGIC -- removes order 1002 (invalid -> 0.0) — small orders gone
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   lab_uc.silver.orders_clean
# MAGIC ORDER BY
# MAGIC   order_id;

# COMMAND ----------

# DBTITLE 1,9.3 History and time travel
# MAGIC %sql
# MAGIC DESCRIBE HISTORY lab_uc.silver.orders_clean;
# MAGIC
# MAGIC -- Time travel: query an earlier version of the table
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   lab_uc.silver.orders_clean VERSION AS OF 2
# MAGIC ORDER BY
# MAGIC   order_id;
# MAGIC
# MAGIC -- RESTORE rolls the table back (left commented — it is a destructive operation):
# MAGIC -- RESTORE TABLE lab_uc.silver.orders_clean TO VERSION AS OF 0;

# COMMAND ----------

# DBTITLE 1,9.4 OPTIMIZE and VACUUM
# MAGIC %sql
# MAGIC -- OPTIMIZE compacts files to improve layout (safe to run)
# MAGIC OPTIMIZE
# MAGIC   lab_uc.silver.orders_clean;
# MAGIC
# MAGIC -- VACUUM removes eligible obsolete files per retention rules (left commented).
# MAGIC -- It does NOT compact files — that is OPTIMIZE's job.
# MAGIC -- VACUUM lab_uc.silver.orders_clean;

# COMMAND ----------

# DBTITLE 1,9.5 Liquid clustering
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lab_uc.gold.sales (store_id BIGINT, sale_date DATE, revenue DOUBLE)
# MAGIC   USING DELTA
# MAGIC   CLUSTER BY (store_id, sale_date);
# MAGIC
# MAGIC INSERT INTO lab_uc.gold.sales
# MAGIC   VALUES
# MAGIC     (1, DATE '2026-10-01', 100.0),
# MAGIC     (1, DATE '2026-10-02', 150.0),
# MAGIC     (2, DATE '2026-10-01', 200.0);
# MAGIC
# MAGIC DESCRIBE HISTORY lab_uc.gold.sales;
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   lab_uc.gold.sales;
# MAGIC
# MAGIC -- Liquid clustering organizes data for efficient access patterns,
# MAGIC -- particularly when filtering frequently on selected columns.

# COMMAND ----------

# DBTITLE 1,10. Gold layer — views, materialized views, streaming tables
# MAGIC %md
# MAGIC ## 10. Gold layer — views, materialized views, streaming tables
# MAGIC
# MAGIC | Object | Key distinction |
# MAGIC | --- | --- |
# MAGIC | Table | Stores data |
# MAGIC | View | Stores a **query definition** (no separate copy of results) |
# MAGIC | Materialized view | Stores **refreshed query results** |
# MAGIC | Streaming table | Supports **incremental stream processing** |
# MAGIC
# MAGIC A streaming table is created with `CREATE OR REFRESH STREAMING TABLE ... AS SELECT ... FROM STREAM read_files(...)` — this is **declarative pipeline SQL** and must run in a Lakeflow Spark Declarative Pipelines context, not ad hoc SQL.

# COMMAND ----------

# DBTITLE 1,10.1 Regular view
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW lab_uc.gold.active_orders AS
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   amount,
# MAGIC   status
# MAGIC FROM
# MAGIC   lab_uc.silver.orders_clean
# MAGIC WHERE
# MAGIC   status = 'COMPLETED';
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   lab_uc.gold.active_orders;
# MAGIC
# MAGIC -- A view stores the query definition only — querying it re-runs the query.

# COMMAND ----------

# DBTITLE 1,10.2 Materialized view (reference on serverless compute)
# 10.2 Materialized view — kept as a runnable REFERENCE cell.
# Creating a materialized view requires a serverless SQL warehouse; doing it
# from serverless notebook compute fails with
# MATERIALIZED_VIEW_OPERATION_NOT_ALLOWED (MV_NOT_ENABLED_ON_SERVERLESS_GENERIC_COMPUTE).
mv_sql = """
CREATE OR REPLACE MATERIALIZED VIEW lab_uc.gold.daily_sales AS
SELECT
    customer_id,
    SUM(amount) AS total_sales
FROM lab_uc.silver.orders_clean
GROUP BY customer_id
"""

try:
    spark.sql(mv_sql)
    display(spark.sql("SELECT * FROM lab_uc.gold.daily_sales"))
except Exception as e:
    print("Materialized view creation is not available on this serverless compute:")
    print("  " + str(e).splitlines()[0])
    print()
    print("A materialized view stores computed results and is refreshed to incorporate")
    print("changes in its source data. To see it succeed, run the same")
    print(
        "CREATE MATERIALIZED VIEW statement from a serverless SQL warehouse (SQL editor)"
    )
    print("or from a pipeline. The equivalent plain view lives in 10.1.")

# COMMAND ----------

# DBTITLE 1,10.3 Materialized view — SQL version (reference)
# MAGIC %sql
# MAGIC -- 10.3 Materialized view — SQL version (reference)
# MAGIC
# MAGIC -- The same statement as 10.2, in pure SQL. On this serverless notebook compute
# MAGIC -- it fails with MATERIALIZED_VIEW_OPERATION_NOT_ALLOWED
# MAGIC -- (MV_NOT_ENABLED_ON_SERVERLESS_GENERIC_COMPUTE) — the CREATE must run from a
# MAGIC -- serverless SQL warehouse (SQL editor) or a pipeline, so it is kept as reference:
# MAGIC
# MAGIC -- CREATE OR REPLACE MATERIALIZED VIEW lab_uc.gold.daily_sales AS
# MAGIC -- SELECT
# MAGIC --     customer_id,
# MAGIC --     SUM(amount) AS total_sales
# MAGIC -- FROM lab_uc.silver.orders_clean
# MAGIC -- GROUP BY customer_id;
# MAGIC
# MAGIC -- Runnable check: list the gold objects that actually exist.
# MAGIC -- Until the statement above is run from a warehouse, daily_sales is absent:
# MAGIC SELECT
# MAGIC   table_name,
# MAGIC   table_type
# MAGIC FROM
# MAGIC   lab_uc.information_schema.tables
# MAGIC WHERE
# MAGIC   table_schema = 'gold'
# MAGIC ORDER BY
# MAGIC   table_name;
# MAGIC

# COMMAND ----------

# DBTITLE 1,11. Data quality checks
# MAGIC %md
# MAGIC ## 11. Data quality checks
# MAGIC
# MAGIC Two ways to enforce data quality:
# MAGIC
# MAGIC | Mechanism | Purpose |
# MAGIC | --- | --- |
# MAGIC | Ad hoc checks (PySpark / SQL) | Validate rows anywhere in a pipeline; raise an error on failure |
# MAGIC | `CONSTRAINT ... EXPECT (condition) ON VIOLATION DROP ROW` | Declarative expectation inside a streaming-table definition |
# MAGIC | `ON VIOLATION WARN` | Track violations in metrics without dropping rows |
# MAGIC
# MAGIC A declarative expectation requires a supported Lakeflow Spark Declarative Pipelines context — it cannot run as ad hoc SQL in a notebook.

# COMMAND ----------

# DBTITLE 1,11.1 Ad hoc data quality checks
# 11.1 Ad hoc data quality checks

# Validate rows in a DataFrame and raise an error if any fail the check.
# This is the simplest form of ad hoc data-quality enforcement.

orders = spark.table("lab_uc.silver.orders_clean")

display(orders)

invalid_orders = orders.filter(F.col("order_id").isNull() | (F.col("amount") < 0))

invalid_count = invalid_orders.count()
if invalid_count > 0:
    raise ValueError(f"Found {invalid_count} invalid orders")
else:
    print("Data quality check passed: 0 invalid orders")

# SQL equivalent:
# SELECT COUNT(*) AS invalid_count FROM lab_uc.silver.orders_clean
# WHERE order_id IS NULL OR amount < 0;

# COMMAND ----------

# DBTITLE 1,11.2 Python error handling — try/except and assert
# 11.2 Python error handling — try/except and assert

# Two Python constructs commonly used in data pipelines for validation and
# error handling:
#
#   try / except  Catch and handle exceptions; keep the pipeline running or
#                 fail gracefully with context
#   assert        Boolean sanity check; raises AssertionError if False
#   finally       Runs cleanup code regardless of whether an exception occurred
#   raise         Re-raise or raise a new exception with a custom message
#
# Exam tip: assert statements can be disabled globally with python -O
# (optimize flag) — never rely on assert as the ONLY data-quality gate in
# production. Use explicit if checks with raise for critical validation.

# COMMAND ----------

# DBTITLE 1,11.3 try/except and assert — live examples
# 11.3 try/except and assert — live examples

# Demonstrate each construct with runnable examples against orders_clean.

# --- try/except: graceful error handling ---
# Attempt a risky cast that would fail on bad input
def safe_cast(value, target_type=float):
    """Return the casted value or None if the cast fails."""
    try:
        return target_type(value)
    except (ValueError, TypeError):
        return None

print("safe_cast('42.5') ->", safe_cast("42.5"))
print("safe_cast('abc')   ->", safe_cast("abc"))

# try/except/finally — finally always runs
try:
    bad_division = 1 / 0
except ZeroDivisionError:
    print("Caught ZeroDivisionError — handled gracefully")
finally:
    print("finally block always executes (cleanup goes here)")

# --- assert: quick inline sanity checks ---
# Useful during development and EDA to catch obvious mistakes early.
assert safe_cast("100") == 100.0, "safe_cast should return 100.0"

# assert with a message — raises AssertionError if False
row_count = spark.table("lab_uc.silver.orders_clean").count()
assert row_count > 0, "orders_clean should have at least one row"
print(f"Assertion passed: orders_clean has {row_count} rows")

# assert can check schema expectations
expected_cols = {"order_id", "customer_id", "amount", "status"}
actual_cols = set(spark.table("lab_uc.silver.orders_clean").columns)
assert expected_cols.issubset(actual_cols), f"Missing columns: {expected_cols - actual_cols}"
print("Assertion passed: all expected columns present")

# --- PySpark testing assertions: assertDataFrameEqual and assertSchemaEqual ---
# pyspark.testing provides DataFrame-aware assertion helpers that compare
# rows and schemas directly — more precise than manual .collect() comparisons.

from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual

# assertSchemaEqual — compare only the schema (field names and types)
schema1 = spark.createDataFrame([], "id INT, name STRING").schema
schema2 = spark.createDataFrame([], "id INT, name STRING").schema
assertSchemaEqual(schema1, schema2)
print("assertSchemaEqual passed: schemas match")

# assertDataFrameEqual — compare row data between two DataFrames
# (order-sensitive by default; set checkRowOrder=False to ignore row order)
df1 = spark.createDataFrame([(1, "Alice"), (2, "Bob")], "id INT, name STRING")
df2 = spark.createDataFrame([(1, "Alice"), (2, "Bob")], "id INT, name STRING")

assertDataFrameEqual(df1, df2)
print("assertDataFrameEqual passed: DataFrames are equal")

# A mismatch raises AssertionError with a clear diff — ideal for test suites:
#   df3 = spark.createDataFrame([(1, "Alice"), (3, "Carol")], "id INT, name STRING")
#   assertDataFrameEqual(df1, df3)  # fails — row 2 differs

# Exam trap: assert is disabled when Python runs with -O (optimize) flag.
# For production data-quality gates, use if-checks with raise instead:
if row_count == 0:
    raise ValueError("orders_clean is empty — aborting pipeline")
else:
    print("Production-style check passed")

# COMMAND ----------

# DBTITLE 1,11.4 Declarative pipeline expectation (reference)
# 11.4 Declarative pipeline expectation (reference)

# A declarative expectation lives inside a streaming-table definition in a
# Lakeflow Spark Declarative Pipelines context — not ad hoc notebook SQL.
# Like materialized views (10.2), streaming tables cannot be created from this
# serverless notebook compute — demonstrated safely below.

expectation_sql = """
CREATE OR REFRESH STREAMING TABLE valid_orders (
    CONSTRAINT positive_amount
    EXPECT (amount >= 0)
    ON VIOLATION DROP ROW
)
AS SELECT *
FROM STREAM training.bronze.orders_stream
"""

try:
    spark.sql(expectation_sql)
    print("Streaming table created")
except Exception as e:
    print("CREATE OR REFRESH STREAMING TABLE is not available on this serverless notebook compute:")
    print("  " + str(e).splitlines()[0])

# The expectation DROPS rows violating the condition (ON VIOLATION DROP ROW);
# ON VIOLATION WARN keeps violating rows and tracks them in metrics.
# It does not replace broader data-quality monitoring.
#
# See Lab 03 — Lakeflow Declarative Pipelines in this workspace for the full
# declarative pipeline context where expectations run.

# COMMAND ----------

# DBTITLE 1,12. Notebook parameters and Lakeflow Jobs
# MAGIC %md
# MAGIC ## 12. Notebook parameters and Lakeflow Jobs
# MAGIC
# MAGIC The guide includes job tasks, task dependencies, retries, branching, looping, and scheduled or data-driven triggers. The mechanisms to know:
# MAGIC
# MAGIC | Mechanism | Purpose |
# MAGIC | --- | --- |
# MAGIC | `dbutils.widgets` | Read notebook parameters (values arrive as strings) |
# MAGIC | `dbutils.jobs.taskValues` | Pass values between job tasks |
# MAGIC | `dbutils.notebook.exit` | End a notebook and return a string result |
# MAGIC | Job triggers | Scheduled, file-arrival, and table-update triggers |
# MAGIC
# MAGIC Widget values are strings — validate identifiers against an allowlist before interpolating them into queries.

# COMMAND ----------

# DBTITLE 1,12.1 Notebook parameters — widgets
# 12.1 Notebook parameters — widgets

dbutils.widgets.text("catalog", "lab_uc")
dbutils.widgets.text("process_date", "2026-10-01")

catalog = dbutils.widgets.get("catalog")
process_date = dbutils.widgets.get("process_date")

# Validate identifiers against an allowlist before interpolating into queries
allowed_catalogs = {"lab_uc", "training", "development", "production"}
if catalog not in allowed_catalogs:
    raise ValueError("Unsupported catalog")

df = spark.table(f"{catalog}.silver.orders_clean")
print(f"catalog={catalog}, process_date={process_date}, rows={df.count()}")

# COMMAND ----------

# DBTITLE 1,12.2 Jobs — task values, exit, and orchestration (reference)
# 12.2 Jobs — task values, exit, and orchestration (reference)

# These APIs only work inside a job run. Outside one, taskValues.get() raises an
# error (TypeError here) while taskValues.set() may silently do nothing —
# demonstrated safely below instead of just described (runnable-reference style like 10.2).

# Upstream notebook task: set a named value for downstream tasks
try:
    dbutils.jobs.taskValues.set(key="row_count", value=12500)
except Exception as e:
    print("taskValues.set works only inside a job run ->", type(e).__name__)

# Downstream notebook task: read a value set by an upstream task
try:
    row_count = dbutils.jobs.taskValues.get(taskKey="ingest_orders", key="row_count")
    print("row_count:", row_count)
except Exception as e:
    print("taskValues.get works only inside a job run ->", type(e).__name__)

# dbutils.notebook.exit("SUCCESS") ends the notebook and returns a string result
# (a different mechanism from a named task value). Left commented so this notebook keeps running.

# Job orchestration concepts to recognize (job configuration, not notebook functions):
#   depends_on     -> task dependencies
#   retries        -> recover from transient failures
#   for each task  -> iterate over a list of inputs
#   repair run     -> rerun eligible failed or skipped tasks
#   file arrival   -> trigger when files arrive
#   table update   -> trigger on supported table updates

# COMMAND ----------

# DBTITLE 1,13. CI/CD — bundles and CLI (reference)
# MAGIC %md
# MAGIC ## 13. CI/CD — bundles and CLI (reference)
# MAGIC
# MAGIC Declarative Automation Bundles (formerly Databricks Asset Bundles) promote resources across environments. A bundle describes jobs and resources as YAML with per-environment targets:
# MAGIC
# MAGIC ```yaml
# MAGIC bundle:
# MAGIC   name: orders_pipeline
# MAGIC
# MAGIC variables:
# MAGIC   catalog:
# MAGIC     default: development
# MAGIC
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     orders_job:
# MAGIC       name: orders-etl
# MAGIC       tasks:
# MAGIC         - task_key: ingest
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./src/ingest.py
# MAGIC
# MAGIC targets:
# MAGIC   dev:
# MAGIC     default: true
# MAGIC     variables:
# MAGIC       catalog: development
# MAGIC
# MAGIC   prod:
# MAGIC     variables:
# MAGIC       catalog: production
# MAGIC ```
# MAGIC
# MAGIC ### Important CLI commands
# MAGIC
# MAGIC ```sh
# MAGIC databricks bundle validate -t dev
# MAGIC databricks bundle deploy -t dev
# MAGIC databricks bundle run orders_job -t dev
# MAGIC databricks bundle destroy -t dev   # remove bundle-managed deployed resources
# MAGIC ```
# MAGIC
# MAGIC | Command | Purpose |
# MAGIC | --- | --- |
# MAGIC | `bundle validate` | Validate configuration |
# MAGIC | `bundle deploy` | Deploy resources |
# MAGIC | `bundle run` | Run a deployed resource |
# MAGIC | `bundle destroy` | Remove bundle-managed deployed resources |
# MAGIC
# MAGIC **Exam tip:** `validate` ≠ `deploy`, and `deploy` does not automatically execute the job.

# COMMAND ----------

# DBTITLE 1,14. Performance tuning — Spark configuration
# MAGIC %md
# MAGIC ## 14. Performance tuning — Spark configuration
# MAGIC
# MAGIC The exam guide explicitly identifies Spark configuration areas. Know the difference between runtime session settings and compute-startup settings:
# MAGIC
# MAGIC | Setting | Purpose | When to set |
# MAGIC | --- | --- | --- |
# MAGIC | `spark.sql.shuffle.partitions` | Number of shuffle partitions (default 200) | Runtime — `spark.conf.set` |
# MAGIC | `spark.sql.autoBroadcastJoinThreshold` | Automatic broadcast-join threshold (10 MiB = 10485760 bytes) | Runtime, where the key is supported |
# MAGIC | `spark.default.parallelism` | Default parallelism | Compute startup |
# MAGIC | `spark.executor.memory` / `spark.driver.memory` | Executor / driver memory | Compute startup |
# MAGIC
# MAGIC Memory settings are generally configured for compute startup rather than changed dynamically inside a running notebook. `repartition()` redistributes data with a shuffle; `coalesce()` typically reduces partitions with a narrow dependency. Use the Spark UI to identify shuffle, skew, spilling, and slow stages — one task taking far longer than the others may indicate skewed data distribution.

# COMMAND ----------

# DBTITLE 1,14.1 Spark configuration, repartition vs coalesce, and the query plan
# 14.1 Spark configuration, repartition vs coalesce, and the query plan

# Set the number of shuffle partitions (default 200)
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Automatic broadcast-join threshold (10 MiB = 10485760 bytes).
# Not every Spark conf key is settable on serverless compute — set defensively.
try:
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)
except Exception as e:
    print(
        "spark.sql.autoBroadcastJoinThreshold not settable on this compute ->",
        type(e).__name__,
    )

# Other settings to recognize (configured at compute startup, not dynamically):
#   spark.default.parallelism
#   spark.executor.memory
#   spark.driver.memory

orders = spark.table("lab_uc.silver.orders_clean")

# repartition: redistributes into N partitions, generally with a shuffle.
# (spark.range gives enough rows for every partition to be non-empty)
df2 = spark.range(1000).repartition(100)

# coalesce: typically reduces partition count with a narrow dependency
# (may create unevenly sized partitions)
df3 = df2.coalesce(2)

# .rdd.getNumPartitions() is NOT available on serverless / Spark Connect compute.
# Count distinct partition IDs with the spark_partition_id() expression instead.
from pyspark.sql import functions as F


def num_partitions(df):
    return df.groupBy(F.spark_partition_id().alias("pid")).count().count()


print(f"repartition(100) -> {num_partitions(df2)} partitions")
print(f"coalesce(2)      -> {num_partitions(df3)} partitions")

# Examine the query plan — use the Spark UI to find shuffle, skew, spilling, slow stages
orders.join(spark.table("lab_uc.silver.customers"), "customer_id", "inner").explain(
    "formatted"
)

# COMMAND ----------

# DBTITLE 1,15. Unity Catalog — SQL security (reference)
# MAGIC %md
# MAGIC ## 15. Unity Catalog — SQL security (reference)
# MAGIC
# MAGIC Grant statements (not executed here — they require the target groups to exist in the workspace):
# MAGIC
# MAGIC ```sql
# MAGIC GRANT USE CATALOG ON CATALOG lab_uc TO `analysts`;
# MAGIC GRANT USE SCHEMA ON SCHEMA lab_uc.gold TO `analysts`;
# MAGIC GRANT SELECT ON TABLE lab_uc.gold.daily_sales TO `analysts`;
# MAGIC
# MAGIC REVOKE SELECT ON TABLE lab_uc.gold.daily_sales FROM `analysts`;
# MAGIC ```
# MAGIC
# MAGIC **Hierarchy:** `Catalog → Schema → Table`. A table-level SELECT grant does **not** provide the parent namespace privileges (`USE CATALOG`, `USE SCHEMA`) required to access it.
# MAGIC
# MAGIC ### Managed vs external tables
# MAGIC
# MAGIC ```sql
# MAGIC -- Managed: Unity Catalog manages the data lifecycle
# MAGIC CREATE TABLE lab_uc.silver.customers (customer_id BIGINT, customer_name STRING)
# MAGIC USING DELTA;
# MAGIC
# MAGIC -- External: data lives at a LOCATION you control
# MAGIC CREATE TABLE lab_uc.bronze.external_events (event_id BIGINT, event_type STRING)
# MAGIC USING DELTA
# MAGIC LOCATION 's3://your-bucket/events/';
# MAGIC ```
# MAGIC
# MAGIC Dropping a managed table vs dropping an external table have **different implications for the underlying data**.
# MAGIC
# MAGIC ### Column masks and row filters
# MAGIC
# MAGIC Live examples already exist in this workspace from **Lab 02 — Unity Catalog Setup** (`lab_uc.hr.mask_salary`, `lab_uc.hr.region_filter`, applied to `lab_uc.hr.employees`). Pattern:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE FUNCTION lab_uc.security.mask_salary(salary DECIMAL(12,2))
# MAGIC RETURN IF(is_account_group_member('hr_team'), salary, CAST(NULL AS DECIMAL(12,2)));
# MAGIC
# MAGIC ALTER TABLE lab_uc.gold.employees
# MAGIC ALTER COLUMN salary SET MASK lab_uc.security.mask_salary;
# MAGIC
# MAGIC CREATE FUNCTION lab_uc.security.region_filter(region STRING)
# MAGIC RETURN is_account_group_member('regional_admins') OR region = 'EAST';
# MAGIC
# MAGIC ALTER TABLE lab_uc.gold.sales
# MAGIC SET ROW FILTER lab_uc.security.region_filter ON (region);
# MAGIC ```
# MAGIC
# MAGIC **ABAC** extends this approach with centralized policies based on governed attributes and tags — its configuration is not simply a `GRANT` statement.

# COMMAND ----------

# DBTITLE 1,16. Hands-on exercises (answers below)
# MAGIC %md
# MAGIC ## 16. Hands-on exercises (answers below)
# MAGIC
# MAGIC 1. **Data cleaning** — A Bronze table has `amount` as STRING with some invalid values. Write a PySpark expression that safely converts it to DOUBLE.
# MAGIC 2. **Deduplication** — You need the latest record per `customer_id`, ordered by `updated_at` descending. Which SQL window function should you use?
# MAGIC 3. **Joining** — You have 100M order rows and 5,000 product rows. Write the PySpark join with an explicit broadcast hint.
# MAGIC 4. **Incremental ingestion** — Which PySpark streaming source format does Auto Loader use?
# MAGIC 5. **Upsert** — Which Delta SQL statement updates matching `order_id` records and inserts new ones?
# MAGIC 6. **Governance** — An analyst has SELECT on a table but lacks access to the parent catalog and schema. Which privileges are generally also needed?
# MAGIC
# MAGIC ### Answers
# MAGIC
# MAGIC 1. `F.expr("try_cast(amount AS DOUBLE)")` — `try_cast` returns NULL for invalid values instead of failing.
# MAGIC 2. `ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC)`, then filter `rn = 1`. Use `RANK()`/`DENSE_RANK()` only when you want ties to share ranks.
# MAGIC 3. `large_orders.join(F.broadcast(products), "product_id", "inner")`.
# MAGIC 4. `spark.readStream.format("cloudFiles")` with `cloudFiles.format` and `cloudFiles.schemaLocation` options.
# MAGIC 5. `MERGE INTO ... USING ... ON ... WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ...`.
# MAGIC 6. `USE CATALOG` on the parent catalog and `USE SCHEMA` on the parent schema — the hierarchy is Catalog → Schema → Table.

# COMMAND ----------

# DBTITLE 1,17. Seven-day study plan & final advice
# MAGIC %md
# MAGIC ## 17. Seven-day study plan & final advice
# MAGIC
# MAGIC | Day | Focus |
# MAGIC | --- | --- |
# MAGIC | 1 | Notebook fundamentals, reading/writing Delta tables, COPY INTO |
# MAGIC | 2 | Auto Loader, streaming options, schema evolution |
# MAGIC | 3 | Data cleaning, columns, filters, NULLs, JSON and explode |
# MAGIC | 4 | Joins, union, aggregations, windows, deduplication |
# MAGIC | 5 | Delta MERGE, UPDATE, DELETE, time travel, Gold objects |
# MAGIC | 6 | Lakeflow Jobs, widgets, CLI/bundles, Spark tuning |
# MAGIC | 7 | Unity Catalog SQL security + a complete Bronze → Silver → Gold exercise |
# MAGIC
# MAGIC ## Final advice
# MAGIC
# MAGIC Practice **recognizing, correcting, and completing** notebook code rather than memorizing definitions. Aim to write a complete pipeline from memory: ingest files → clean data → deduplicate by business key → merge into Silver → aggregate into Gold → configure the job to run reliably.

# COMMAND ----------

# DBTITLE 1,18. Optional cleanup (do NOT run by default)
# MAGIC %md
# MAGIC ## 18. Optional cleanup (do NOT run by default)
# MAGIC
# MAGIC Intentionally **not executed** — run these statements only if you really want to drop every table and view this study guide created:
# MAGIC
# MAGIC ```sql
# MAGIC DROP TABLE IF EXISTS lab_uc.bronze.orders_raw;
# MAGIC DROP TABLE IF EXISTS lab_uc.bronze.orders_csv;
# MAGIC DROP TABLE IF EXISTS lab_uc.bronze.orders_nested;
# MAGIC DROP TABLE IF EXISTS lab_uc.bronze.events_raw;
# MAGIC DROP TABLE IF EXISTS lab_uc.bronze.customer_updates;
# MAGIC DROP TABLE IF EXISTS lab_uc.silver.write_mode_demo;
# MAGIC DROP TABLE IF EXISTS lab_uc.silver.orders_clean;
# MAGIC DROP TABLE IF EXISTS lab_uc.silver.orders_sql;
# MAGIC DROP TABLE IF EXISTS lab_uc.gold.sales;
# MAGIC DROP VIEW IF EXISTS lab_uc.gold.active_orders;
# MAGIC DROP VIEW IF EXISTS lab_uc.gold.daily_sales;
# MAGIC ```
# MAGIC
# MAGIC Dropping is irreversible. The earlier lab tables in `lab_uc` (Labs 01–03) are not touched by this notebook and remain available.