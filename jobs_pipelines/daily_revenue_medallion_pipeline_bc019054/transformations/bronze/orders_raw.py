from pyspark import pipelines as dp


# --- Bronze layer: raw JSON ingestion via Auto Loader ---------------------
# This module defines the Bronze streaming table. Auto Loader incrementally
# reads new JSON files as they arrive in the landing volume — no schema
# enforcement or transformation happens here so the raw data is preserved
# exactly as landed.

@dp.table(
    name="lab_uc.bronze.sdp_orders_raw",
    comment=(
        "Bronze layer: raw JSON order events ingested as-is from the landing "
        "volume via Auto Loader. Only orders_part_*.json files at the volume "
        "root are picked up; the study_* subdirectories belonging to other "
        "notebooks are excluded by the filename glob."
    ),
)
def orders_raw():
    return (
        spark.readStream.format("cloudFiles")
        # Auto Loader detects and processes new files incrementally rather
        # than re-scanning the entire directory on every micro-batch.
        .option("cloudFiles.format", "json")
        # The glob filter restricts ingestion to order files only, excluding
        # unrelated subdirectories (e.g. study guide data) in the same volume.
        .option("pathGlobFilter", "orders_part_*.json")
        .load("/Volumes/lab_uc/bronze/landing")
    )
