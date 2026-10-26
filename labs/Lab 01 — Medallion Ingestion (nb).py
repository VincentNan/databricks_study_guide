# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Lab 01 — Medallion Ingestion
# MAGIC %md
# MAGIC # Lab 01 — Medallion Ingestion (Bronze → Silver → Gold)
# MAGIC
# MAGIC End-to-end walkthrough of the medallion architecture with PySpark + Delta. Land raw events in Bronze, cleanse and dedupe into Silver, aggregate into Gold for analytics consumption.
# MAGIC
# MAGIC **Topics covered:**
# MAGIC * Create three Delta tables in three UC schemas (`bronze`, `silver`, `gold`)
# MAGIC * Ingest synthetic order events into Bronze using `COPY INTO`
# MAGIC * Cleanse + dedupe Silver via `MERGE INTO`
# MAGIC * Aggregate Gold via `CREATE OR REPLACE TABLE AS SELECT`
# MAGIC * Run `OPTIMIZE` + `VACUUM` to see file-management in action

# COMMAND ----------

# DBTITLE 1,Setup — schemas and a UC volume
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Setup — schemas and a UC volume
# MAGIC -- ============================================================
# MAGIC -- This lab uses the 'lab_uc' catalog (created in Lab 02).
# MAGIC CREATE CATALOG IF NOT EXISTS lab_uc COMMENT 'UC lab catalog';
# MAGIC USE CATALOG lab_uc;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS lab_uc.bronze.landing;

# COMMAND ----------

# DBTITLE 1,Step 1 — Synthesise raw events in the landing volume
from pyspark.sql import functions as F
import json
import uuid
import random
from datetime import datetime, timezone, timedelta

# Build 1,000 synthetic order events as JSON lines
rng = random.Random(42)
now = datetime.now(timezone.utc)
events = []
for i in range(1000):
    events.append({
        "order_id": str(uuid.uuid4()),
        "customer_id": rng.randint(1, 100),
        "amount": round(rng.uniform(5, 500), 2),
        "currency": rng.choice(["USD", "EUR", "JPY"]),
        "event_ts": (now - timedelta(seconds=rng.randint(0, 86400))).isoformat(),
    })

# Sprinkle some duplicates and one malformed row to exercise Silver dedup + bad-record handling
events.append(events[7])
events.append({"order_id": "BAD", "customer_id": "not-a-number", "amount": "x", "currency": "USD", "event_ts": now.isoformat()})

# Write as 4 JSON files in the landing volume
chunks = [events[i::4] for i in range(4)]
for i, chunk in enumerate(chunks):
    path = f"/Volumes/lab_uc/bronze/landing/orders_part_{i:02d}.json"
    dbutils.fs.put(path, "\n".join(json.dumps(e) for e in chunk), overwrite=True)

print("Landed 4 files:", dbutils.fs.ls("/Volumes/lab_uc/bronze/landing/"))

# COMMAND ----------

# DBTITLE 1,Step 2 — Bronze: ingest with COPY INTO
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Step 2 — Bronze: ingest with COPY INTO
# MAGIC -- ============================================================
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS lab_uc.bronze.orders (
# MAGIC   order_id     STRING,
# MAGIC   customer_id  STRING,
# MAGIC   amount       STRING,
# MAGIC   currency     STRING,
# MAGIC   event_ts     STRING,
# MAGIC   _ingest_ts   TIMESTAMP,
# MAGIC   _source_file STRING
# MAGIC );
# MAGIC
# MAGIC COPY INTO lab_uc.bronze.orders
# MAGIC FROM (
# MAGIC   SELECT *,
# MAGIC          current_timestamp() AS _ingest_ts,
# MAGIC          _metadata.file_name AS _source_file
# MAGIC   FROM '/Volumes/lab_uc/bronze/landing/'
# MAGIC )
# MAGIC FILEFORMAT = JSON
# MAGIC FORMAT_OPTIONS ('inferSchema' = 'true', 'multiline' = 'false')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');
# MAGIC
# MAGIC SELECT count(*) AS bronze_rows FROM lab_uc.bronze.orders;

# COMMAND ----------

# DBTITLE 1,Step 2 — COPY INTO idempotency tip
# MAGIC %md
# MAGIC > **Tip:** Re-run the `COPY INTO` — it should report 0 new files loaded (idempotency in action).

# COMMAND ----------

# DBTITLE 1,Step 3 — Silver: cleanse, type, dedupe with MERGE
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Step 3 — Silver: cleanse, type, dedupe with MERGE
# MAGIC -- ============================================================
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS lab_uc.silver.orders (
# MAGIC   order_id    STRING NOT NULL,
# MAGIC   customer_id INT,
# MAGIC   amount      DECIMAL(10, 2),
# MAGIC   currency    STRING,
# MAGIC   event_ts    TIMESTAMP,
# MAGIC   _ingest_ts  TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC WITH typed AS (
# MAGIC   SELECT
# MAGIC     order_id,
# MAGIC     CAST(customer_id AS INT) AS customer_id,
# MAGIC     CAST(amount AS DECIMAL(10,2)) AS amount,
# MAGIC     currency,
# MAGIC     CAST(event_ts AS TIMESTAMP) AS event_ts,
# MAGIC     _ingest_ts,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _ingest_ts DESC) AS rn
# MAGIC   FROM lab_uc.bronze.orders
# MAGIC   WHERE order_id IS NOT NULL
# MAGIC     AND TRY_CAST(customer_id AS INT) IS NOT NULL
# MAGIC     AND TRY_CAST(amount      AS DECIMAL(10,2)) IS NOT NULL
# MAGIC     AND TRY_CAST(event_ts    AS TIMESTAMP) IS NOT NULL
# MAGIC ),
# MAGIC deduped AS (
# MAGIC   -- Strip the rn helper column so UPDATE SET * / INSERT * matches the target schema
# MAGIC   SELECT order_id, customer_id, amount, currency, event_ts, _ingest_ts
# MAGIC   FROM typed WHERE rn = 1
# MAGIC )
# MAGIC MERGE INTO lab_uc.silver.orders AS tgt
# MAGIC USING deduped AS src
# MAGIC ON tgt.order_id = src.order_id
# MAGIC WHEN MATCHED AND src._ingest_ts > tgt._ingest_ts THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC
# MAGIC SELECT count(*) AS silver_rows,
# MAGIC        count(DISTINCT order_id) AS distinct_ids
# MAGIC FROM lab_uc.silver.orders;

# COMMAND ----------

# DBTITLE 1,Step 3 — Silver explanation
# MAGIC %md
# MAGIC The two counts should now match — Silver is deduplicated and well-typed. The malformed row was filtered by the `TRY_CAST` predicates.

# COMMAND ----------

# DBTITLE 1,Step 4 — Gold: aggregate for the dashboard
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Step 4 — Gold: aggregate for the dashboard
# MAGIC -- ============================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE lab_uc.gold.daily_revenue AS
# MAGIC SELECT
# MAGIC   date_trunc('day', event_ts) AS event_date,
# MAGIC   currency,
# MAGIC   COUNT(*)        AS orders,
# MAGIC   SUM(amount)     AS revenue,
# MAGIC   AVG(amount)     AS avg_order_value
# MAGIC FROM lab_uc.silver.orders
# MAGIC GROUP BY 1, 2;
# MAGIC
# MAGIC SELECT * FROM lab_uc.gold.daily_revenue ORDER BY event_date DESC, currency;

# COMMAND ----------

# DBTITLE 1,Step 5 — File management (tip)
# MAGIC %md
# MAGIC ## Step 5 — File management
# MAGIC
# MAGIC > **Tip:** For **new** Delta tables in 2026, prefer **liquid clustering** (`CLUSTER BY` on table creation, then `OPTIMIZE table FULL`) over Z-ORDER. Z-ORDER is non-adaptive and locks you into the columns you Z-ORDERed by; liquid clustering adapts as your data grows. The `OPTIMIZE` example below works for both layouts.

# COMMAND ----------

# DBTITLE 1,Step 5 — File management SQL
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Step 5 — File management
# MAGIC -- ============================================================
# MAGIC
# MAGIC -- Compact small files in the Silver layer
# MAGIC OPTIMIZE lab_uc.silver.orders;
# MAGIC
# MAGIC -- Inspect the operation history
# MAGIC DESCRIBE HISTORY lab_uc.silver.orders LIMIT 5;
# MAGIC
# MAGIC -- Drop tombstoned files older than 7 days
# MAGIC -- (in dev only — production should keep the default 7-day retention)
# MAGIC -- NOTE: spark.databricks.delta.retentionDurationCheck.enabled is not available on serverless compute.
# MAGIC -- To run VACUUM with a short retention, use a non-serverless cluster or Delta Lake API instead.
# MAGIC -- SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC -- VACUUM lab_uc.silver.orders RETAIN 0 HOURS;
# MAGIC -- RESET spark.databricks.delta.retentionDurationCheck.enabled;

# COMMAND ----------

# DBTITLE 1,Step 5 — VACUUM warning
# MAGIC %md
# MAGIC > **Warning:** `VACUUM ... RETAIN 0 HOURS` is unsafe in production — it can delete files needed by concurrent readers / time-travel queries. The default 7-day retention exists for a reason. The code above is lab-only.

# COMMAND ----------

# DBTITLE 1,Step 6 — Verify the medallion
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Step 6 — Verify the medallion
# MAGIC -- ============================================================
# MAGIC
# MAGIC -- Row counts at each layer
# MAGIC SELECT 'bronze' AS layer, count(*) AS rows FROM lab_uc.bronze.orders
# MAGIC UNION ALL SELECT 'silver', count(*) FROM lab_uc.silver.orders
# MAGIC UNION ALL SELECT 'gold',   count(*) FROM lab_uc.gold.daily_revenue;
# MAGIC
# MAGIC -- Gold queries should be fast and return aggregated totals
# MAGIC SELECT currency, SUM(revenue) AS total_revenue
# MAGIC FROM lab_uc.gold.daily_revenue
# MAGIC GROUP BY currency;

# COMMAND ----------

# DBTITLE 1,Cleanup
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Cleanup
# MAGIC -- ============================================================
# MAGIC
# MAGIC DROP TABLE IF EXISTS lab_uc.gold.daily_revenue;
# MAGIC DROP TABLE IF EXISTS lab_uc.silver.orders;
# MAGIC DROP TABLE IF EXISTS lab_uc.bronze.orders;
# MAGIC -- Optionally drop the schemas + volume too
# MAGIC -- DROP SCHEMA lab_uc.bronze CASCADE;
# MAGIC -- DROP SCHEMA lab_uc.silver CASCADE;
# MAGIC -- DROP SCHEMA lab_uc.gold   CASCADE;

# COMMAND ----------

# DBTITLE 1,Related Study Material
# MAGIC %md
# MAGIC ## Related Study Material
# MAGIC
# MAGIC * [Medallion Architecture (shared)](../shared/fundamentals/medallion-architecture.md)
# MAGIC * [Delta Lake basics (shared)](../shared/fundamentals/delta-lake-basics.md)
# MAGIC * [DE Pro — Data Modelling](../certifications/data-engineer-professional/09-data-modelling/README.md)
# MAGIC * [DE Pro — COPY INTO](../certifications/data-engineer-professional/07-data-ingestion-and-acquisition/02-copy-into.md)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **[↑ Back to Labs](./README.md) | [Next: Unity Catalog setup →](./02-unity-catalog-setup.md)**