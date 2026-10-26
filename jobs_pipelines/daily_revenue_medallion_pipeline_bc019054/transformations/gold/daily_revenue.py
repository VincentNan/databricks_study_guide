from pyspark import pipelines as dp
from pyspark.sql import functions as F


# --- Gold layer: daily revenue summary ------------------------------------
# This module defines a materialized view (not a streaming table) that
# batch-reads the Silver table and aggregates it into one row per calendar
# day per currency. Materialized views are refreshed on a schedule or on
# demand, making them ideal for reporting workloads that don't need
# near-real-time freshness.

@dp.materialized_view(
    name="lab_uc.gold.sdp_daily_revenue",
    comment=(
        "Gold layer: daily revenue summary with exactly one row per calendar "
        "day per currency — number of orders, total revenue, and average order "
        "value (revenue divided by orders). Matches the shape of the Lab 01 "
        "gold table but is a separate, pipeline-owned dataset."
    ),
)
def daily_revenue():
    # Batch read (read, not readStream) — materialized views refresh
    # periodically rather than processing a continuous stream.
    orders = spark.read.table("lab_uc.silver.sdp_orders_clean")

    # Group by currency and calendar day, then compute the three
    # business metrics: order count, total revenue, average order value.
    daily = (
        orders.groupBy(
            F.col("currency"),
            F.to_date("event_ts").alias("event_date"),
        )
        .agg(
            F.count(F.lit(1)).alias("orders"),
            F.sum("amount").alias("revenue"),
            F.avg("amount").alias("avg_order_value"),
        )
    )

    # Match the Lab 01 gold table shape: event_date stored as a TIMESTAMP
    # normalized to midnight of the calendar day.
    return daily.withColumn(
        "event_date", F.expr("CAST(event_date AS TIMESTAMP)")
    )
