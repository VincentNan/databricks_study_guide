from pyspark import pipelines as dp
from pyspark.sql import functions as F


# --- Silver layer: typed, validated, deduplicated orders ------------------
# This module reads the Bronze streaming table, casts each column to its
# business type, enforces non-null expectations, and removes duplicate
# orders within an event-time watermark. The result is a clean, typed
# dataset ready for Gold-layer aggregation.

@dp.table(
    name="lab_uc.silver.sdp_orders_clean",
    comment=(
        "Silver layer: typed and deduplicated orders. order_id is a non-null "
        "string, customer_id an integer, amount a decimal(10,2), currency a "
        "string, and event_ts a timestamp, plus an ingestion timestamp. Rows "
        "that fail casting are dropped by the expectations below; duplicates "
        "on order_id are removed within the event-time watermark."
    ),
)
# Expectations are evaluated AFTER the select() body runs. Columns that
# fail try_cast become NULL, so these non-null checks drop only the rows
# where a cast failed — not rows that were already NULL in Bronze.
@dp.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_amount", "amount IS NOT NULL")
@dp.expect_or_drop("valid_currency", "currency IS NOT NULL")
@dp.expect_or_drop("valid_event_ts", "event_ts IS NOT NULL")
def orders_clean():
    # Read the Bronze table as a stream so new rows are processed
    # incrementally as Auto Loader lands them.
    bronze = spark.readStream.table("lab_uc.bronze.sdp_orders_raw")

    # Cast every column to its target business type.
    # try_cast returns NULL on failure (instead of failing the query),
    # which lets the expectations above catch and drop bad rows.
    typed = bronze.select(
        F.col("order_id").cast("string").alias("order_id"),
        F.expr("try_cast(customer_id AS INT)").alias("customer_id"),
        F.expr("try_cast(amount AS DECIMAL(10,2))").alias("amount"),
        F.col("currency").cast("string").alias("currency"),
        F.expr("try_cast(event_ts AS TIMESTAMP)").alias("event_ts"),
        # Record when the row was processed in Silver for lineage.
        F.current_timestamp().alias("ingested_at"),
    )

    # withWatermark sets a 30-minute late-tolerance on event_ts.
    # dropDuplicatesWithinWatermark removes duplicate order_id values
    # that arrive within that window — older duplicates past the
    # watermark are kept (the state has already been dropped).
    return (
        typed.withWatermark("event_ts", "30 minutes")
        .dropDuplicatesWithinWatermark(["order_id"])
    )
