# databricks_study_guide

Date: 10/26/2026

A hands-on study repository for the **Databricks Certified Data Engineer Associate** exam. It covers the full medallion architecture — Bronze → Silver → Gold — with runnable study guides, labs, SQL queries, an SDP (Spark Declarative Pipelines) pipeline, a dashboard, a Genie agent, and an alert.

All notebooks and queries target the **`lab_uc`** catalog in the workspace (schemas: `bronze`, `silver`, `gold`).

## Repository structure

```
databricks_study_guide/
├── study-guides/
├── queries/
├── labs/
├── jobs_pipelines/
├── genie_agents/
├── dashboards/
└── alerts/
```

## study-guides/

| Asset | Type | Description |
| --- | --- | --- |
| Databricks Data Engineer Associate — Study Notes | Notebook | Conceptual notes on compute permission levels, Auto Loader vs COPY INTO, database storage locations, and Databricks Secrets (scopes, keys & CLI). |
| Databricks Data Engineer Associate — Coding Study Guide (nb) | Notebook | 18-section runnable guide covering Python, PySpark & SQL: ingestion, data cleaning, DataFrame transformations, joins, aggregations, Delta Lake (MERGE/UPDATE/DELETE/history), Gold-layer views, data quality, Lakeflow Jobs, notebook parameters, CI/CD bundles, performance tuning, and Unity Catalog security. |
| JSON & Array Processing — Coding Study Guide (nb) | Notebook | Companion guide on JSON parsing (`from_json`, `get_json_object`), nested struct access, array processing (`explode`, `explode_outer`, `posexplode`, `flatten`), higher-order functions, maps, and an end-to-end JSON → Silver → Gold pipeline in both PySpark and SQL. |
| SDP Pipeline Decorators Explained | Notebook | Explains the Spark Declarative Pipelines decorators (`@dp.table`, `@dp.expect_or_drop`, `@dp.materialized_view`) used in the `daily_revenue_medallion_pipeline` project. |

> Each notebook also has a `.py` source-file counterpart in the same folder.

## queries/

| Query | Description |
| --- | --- |
| Silver Layer — Order Status Summary | Aggregates `lab_uc.silver.orders_sql` by status using GROUP BY, COUNT, SUM, AVG, MIN, MAX, and ROUND. |
| Gold Layer — Top Products by Revenue | Ranks products in `lab_uc.gold.study_product_sales` by revenue using `RANK() OVER()`. |
| Gold Layer — Daily Revenue with Rolling Averages | Computes a 7-day rolling average and day-over-day delta from `lab_uc.gold.daily_revenue` using window frames and `LAG()`. |

## labs/

| Lab | Type | Description |
| --- | --- | --- |
| Lab 01 — Medallion Ingestion | Notebook + `.py` | End-to-end medallion walkthrough: creates Delta tables in three UC schemas, ingests synthetic order events with `COPY INTO` (Bronze), cleanses and dedupes with `MERGE INTO` (Silver), aggregates with `CREATE OR REPLACE TABLE AS SELECT` (Gold), and runs `OPTIMIZE` + `VACUUM`. |
| Lab 02 — Unity Catalog Setup | Notebook + `.py` | Unity Catalog governance walkthrough: three-level namespace, managed vs external tables and volumes, `GRANT` / `REVOKE` / `DENY` privilege hierarchy, row filters and column masks as SQL UDFs, and lineage/audit via `system.access.*`. |

## jobs_pipelines/

### `daily_revenue_medallion_pipeline` — Spark Declarative Pipeline

A Bronze → Silver → Gold SDP pipeline that ingests raw JSON order events and produces a daily revenue summary.

| Layer | File | Decorator | Description |
| --- | --- | --- | --- |
| Bronze | `transformations/bronze/orders_raw.py` | `@dp.table` | Auto Loader ingests raw JSON order events from a UC volume (`/Volumes/lab_uc/bronze/landing`). No schema enforcement or transformation — raw data is preserved as-is. |
| Silver | `transformations/silver/orders_clean.py` | `@dp.table` + `@dp.expect_or_drop` | Reads the Bronze streaming table, casts columns to business types with `try_cast`, drops rows that fail five non-null expectations, and deduplicates by `order_id` within a 30-minute event-time watermark. |
| Gold | `transformations/gold/daily_revenue.py` | `@dp.materialized_view` | Batch-reads the Silver table and aggregates into one row per calendar day per currency (order count, total revenue, average order value). Refreshed on schedule or on demand — ideal for reporting. |

## genie_agents/

| Asset | Description |
| --- | --- |
| Daily Revenue Analysis | Genie agent providing a natural-language Q&A interface over the daily revenue dataset. |

## dashboards/

| Dashboard | Description |
| --- | --- |
| Revenue & Product Sales Overview | AI/BI dashboard visualizing revenue and product sales metrics. |

## alerts/

| Alert | Description |
| --- | --- |
| Daily USD Revenue Below Target (1) | SQL alert that triggers when daily USD revenue falls below a configured target. |

