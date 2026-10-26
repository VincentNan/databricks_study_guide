# Databricks notebook source
# DBTITLE 1,Overview — JSON & Array Coding Study Guide
# MAGIC %md
# MAGIC # JSON and Array Processing — Coding Study Guide
# MAGIC
# MAGIC Runnable companion to the **Databricks Certified Data Engineer Associate** coding guide (May 2026 exam guide): parsing JSON strings, nested struct access, array processing, exploding arrays, higher-order functions, maps, and an end-to-end JSON → Silver → Gold pipeline — in both **PySpark and SQL**.
# MAGIC
# MAGIC All examples run on the existing **`lab_uc`** catalog with fresh study table names (`silver.study_order_items`, `gold.study_product_sales`); earlier lab and study-guide tables are untouched.
# MAGIC
# MAGIC ## Learning goals
# MAGIC
# MAGIC By the end you should be able to parse JSON strings, access nested fields, process arrays, flatten nested records, handle NULL values, and write equivalent PySpark and SQL transformations.
# MAGIC
# MAGIC ## Contents
# MAGIC
# MAGIC 1. Understand JSON data structures
# MAGIC 2. Sample JSON data
# MAGIC 3. Parse JSON strings with from_json()
# MAGIC 4. Extract nested JSON fields
# MAGIC 5. JSON path extraction — get_json_object()
# MAGIC 6. Array processing essentials
# MAGIC 7. explode() — array to rows
# MAGIC 8. explode vs explode_outer vs posexplode
# MAGIC 9. split vs explode vs flatten
# MAGIC 10. Higher-order array functions
# MAGIC 11. MAP data types
# MAGIC 12. End-to-end — JSON to Silver to Gold
# MAGIC 13. Common exam mistakes
# MAGIC 14. Practice questions
# MAGIC 15. Final syntax cheat sheet
# MAGIC 16. Optional cleanup
# MAGIC
# MAGIC **Run the cells top to bottom.** Python cells 2.1, 3.2, and 11.1 register the temp views (`raw_orders`, `parsed_orders`, `product_map`) used by the SQL cells. Cells 12.2–12.4 own and overwrite their study tables on re-run.
# MAGIC
# MAGIC **Validated on serverless compute.**

# COMMAND ----------

# DBTITLE 1,1. Understand JSON data structures
# MAGIC %md
# MAGIC ## 1. Understand JSON data structures
# MAGIC
# MAGIC JSON commonly contains objects, arrays, nested objects, and arrays of objects:
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "order_id": 1001,
# MAGIC   "customer": {
# MAGIC     "id": 501,
# MAGIC     "name": "Alice",
# MAGIC     "address": {"city": "Toronto", "province": "ON"}
# MAGIC   },
# MAGIC   "items": [
# MAGIC     {"product_id": "P100", "quantity": 2, "price": 25.50},
# MAGIC     {"product_id": "P200", "quantity": 1, "price": 40.00}
# MAGIC   ],
# MAGIC   "tags": ["online", "priority"],
# MAGIC   "discount": null
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### JSON to Spark data types
# MAGIC
# MAGIC | JSON value | Spark SQL data type |
# MAGIC | --- | --- |
# MAGIC | `"Alice"` | `STRING` |
# MAGIC | `1001` | `BIGINT` / `INT` |
# MAGIC | `25.50` | `DOUBLE` or `DECIMAL` |
# MAGIC | `true` | `BOOLEAN` |
# MAGIC | `{ "id": 501 }` | `STRUCT` |
# MAGIC | `[1, 2, 3]` | `ARRAY` |
# MAGIC | `["online", "priority"]` | `ARRAY<STRING>` |
# MAGIC | `null` | `NULL` value |
# MAGIC
# MAGIC For monetary values, prefer an explicitly defined `DECIMAL` schema over inferred floating-point types when exact decimal arithmetic matters.

# COMMAND ----------

# DBTITLE 1,2. Sample JSON data
# MAGIC %md
# MAGIC ## 2. Sample JSON data
# MAGIC
# MAGIC The sample DataFrame contains **one JSON string per row** — enough to practice parsing without external files. It has a single column, `raw_json`, of type `STRING`, and registers the `raw_orders` temp view for the SQL cells in later sections.

# COMMAND ----------

# DBTITLE 1,2.1 Create the raw JSON DataFrame
# 2.1 Create the raw JSON DataFrame — one JSON string per row

from pyspark.sql import functions as F
from pyspark.sql import types as T

json_data = [
    '''{
      "order_id": 1001,
      "customer": {
        "id": 501,
        "name": "Alice",
        "address": {"city": "Toronto", "province": "ON"}
      },
      "items": [
        {"product_id": "P100", "quantity": 2, "price": 25.50},
        {"product_id": "P200", "quantity": 1, "price": 40.00}
      ],
      "tags": ["online", "priority"],
      "discount": null
    }''',
    '''{
      "order_id": 1002,
      "customer": {
        "id": 502,
        "name": "Bob",
        "address": {"city": "Ottawa", "province": "ON"}
      },
      "items": [
        {"product_id": "P300", "quantity": 3, "price": 10.00}
      ],
      "tags": ["store"],
      "discount": 5.00
    }''',
]

raw_df = spark.createDataFrame([(x,) for x in json_data], "raw_json STRING")

raw_df.createOrReplaceTempView("raw_orders")  # used by the SQL cells below
display(raw_df)

# COMMAND ----------

# DBTITLE 1,3. Parse JSON strings with from_json()
# MAGIC %md
# MAGIC ## 3. Parse JSON strings with from_json()
# MAGIC
# MAGIC `from_json()` converts a JSON string into a structured Spark column using a schema — one of the most important concepts on the exam.
# MAGIC
# MAGIC | Function | Purpose |
# MAGIC | --- | --- |
# MAGIC | `from_json(col, schema)` | JSON **string** → structured value (STRUCT/ARRAY) |
# MAGIC | `to_json(col)` | Structured value → JSON **string** |
# MAGIC | `get_json_object(col, path)` | Extract one value via a JSON path; returns a **string** |

# COMMAND ----------

# DBTITLE 1,3.1 Define the schema
# 3.1 Define the schema

schema = T.StructType(
    [
        T.StructField("order_id", T.LongType()),
        T.StructField(
            "customer",
            T.StructType(
                [
                    T.StructField("id", T.LongType()),
                    T.StructField("name", T.StringType()),
                    T.StructField(
                        "address",
                        T.StructType(
                            [
                                T.StructField("city", T.StringType()),
                                T.StructField("province", T.StringType()),
                            ]
                        ),
                    ),
                ]
            ),
        ),
        T.StructField(
            "items",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("product_id", T.StringType()),
                        T.StructField("quantity", T.IntegerType()),
                        T.StructField("price", T.DoubleType()),
                    ]
                )
            ),
        ),
        T.StructField("tags", T.ArrayType(T.StringType())),
        T.StructField("discount", T.DoubleType()),
    ]
)

print(schema.simpleString())

# COMMAND ----------

# DBTITLE 1,3.2 Parse the JSON
# 3.2 Parse the JSON — from_json() with the schema

parsed_df = raw_df.withColumn("data", F.from_json(F.col("raw_json"), schema))

# The parsed `data` column now contains nested STRUCT and ARRAY fields
parsed_df.printSchema()

parsed_df.createOrReplaceTempView("parsed_orders")  # used by the SQL cells below
display(parsed_df)

# COMMAND ----------

# DBTITLE 1,3.3 SQL equivalent — from_json
# MAGIC %sql
# MAGIC -- 3.3 SQL equivalent — from_json() with a DDL schema string
# MAGIC -- The schema intentionally declares fewer fields: fields not declared in the
# MAGIC -- schema are NOT included in the resulting struct.
# MAGIC SELECT
# MAGIC     from_json(
# MAGIC         raw_json,
# MAGIC         'STRUCT<
# MAGIC             order_id: BIGINT,
# MAGIC             customer: STRUCT<
# MAGIC                 id: BIGINT,
# MAGIC                 name: STRING
# MAGIC             >
# MAGIC         >'
# MAGIC     ) AS data
# MAGIC FROM raw_orders;

# COMMAND ----------

# DBTITLE 1,4. Extract nested JSON fields
# MAGIC %md
# MAGIC ## 4. Extract nested JSON fields
# MAGIC
# MAGIC Once the JSON is parsed, access nested fields with **dot notation**.
# MAGIC
# MAGIC **Exam tip:** use dot notation to access nested `STRUCT` fields — you do **not** need to explode a struct.

# COMMAND ----------

# DBTITLE 1,4.1 Dot notation — PySpark
# 4.1 Dot notation — PySpark

result = parsed_df.select(
    F.col("data.order_id").alias("order_id"),
    F.col("data.customer.id").alias("customer_id"),
    F.col("data.customer.name").alias("customer_name"),
    F.col("data.customer.address.city").alias("city"),
)
display(result)

# Expected:
#   1001 | 501 | Alice | Toronto
#   1002 | 502 | Bob   | Ottawa

# COMMAND ----------

# DBTITLE 1,4.2 Dot notation — SQL
# MAGIC %sql
# MAGIC -- 4.2 Dot notation — SQL equivalent
# MAGIC SELECT
# MAGIC     data.order_id AS order_id,
# MAGIC     data.customer.id AS customer_id,
# MAGIC     data.customer.name AS customer_name,
# MAGIC     data.customer.address.city AS city
# MAGIC FROM parsed_orders;

# COMMAND ----------

# DBTITLE 1,5. JSON path extraction — get_json_object()
# MAGIC %md
# MAGIC ## 5. JSON path extraction — get_json_object()
# MAGIC
# MAGIC When the source is still a JSON **string**, extract a field directly without first parsing the whole document into a struct. The result is always a **string**.
# MAGIC
# MAGIC | JSON path | Meaning |
# MAGIC | --- | --- |
# MAGIC | `$.order_id` | Root-level field |
# MAGIC | `$.customer.name` | Nested object field |
# MAGIC | `$.customer.address.city` | Deeply nested field |
# MAGIC | `$.items[0].product_id` | First item in array |
# MAGIC | `$.tags[1]` | Second tag |
# MAGIC
# MAGIC **Remember:** JSON array indexing begins at **0**.

# COMMAND ----------

# DBTITLE 1,5.1 JSON paths — PySpark
# 5.1 JSON paths — get_json_object() (source is still a string)

result = raw_df.select(
    F.get_json_object("raw_json", "$.order_id").alias("order_id"),
    F.get_json_object("raw_json", "$.customer.name").alias("customer_name"),
    F.get_json_object("raw_json", "$.customer.address.city").alias("city"),
    F.get_json_object("raw_json", "$.items[0].product_id").alias("first_item"),
    F.get_json_object("raw_json", "$.tags[1]").alias("second_tag"),
)
display(result)

# Note: every extracted value is a STRING — even order_id shows as "1001"/"1002".
# Cast explicitly when you need typed values.

# COMMAND ----------

# DBTITLE 1,5.2 JSON paths — SQL
# MAGIC %sql
# MAGIC -- 5.2 JSON paths — SQL equivalent
# MAGIC SELECT
# MAGIC     get_json_object(raw_json, '$.customer.name') AS customer_name,
# MAGIC     get_json_object(raw_json, '$.customer.address.city') AS city,
# MAGIC     get_json_object(raw_json, '$.items[0].product_id') AS first_item,
# MAGIC     get_json_object(raw_json, '$.tags[1]') AS second_tag
# MAGIC FROM raw_orders;

# COMMAND ----------

# DBTITLE 1,6. Array processing essentials
# MAGIC %md
# MAGIC ## 6. Array processing essentials
# MAGIC
# MAGIC Arrays are ordered collections — e.g. the first order carries `"tags": ["online", "priority"]`.
# MAGIC
# MAGIC | Function | Purpose | Example |
# MAGIC | --- | --- | --- |
# MAGIC | `size()` | Number of elements | `size(tags)` |
# MAGIC | `array_contains()` | Test membership | `array_contains(tags, 'online')` |
# MAGIC | `array_distinct()` | Remove duplicate elements | `array_distinct(tags)` |
# MAGIC | `array_sort()` | Sort elements | `array_sort(tags)` |
# MAGIC | `array_union()` | Union of two arrays | `array_union(a, b)` |
# MAGIC | `array_intersect()` | Common elements | `array_intersect(a, b)` |
# MAGIC | `array_except()` | In first but not second | `array_except(a, b)` |
# MAGIC | `concat()` | Concatenate arrays | `concat(a, b)` |
# MAGIC | `flatten()` | Flatten one level of nested arrays | `flatten(nested_arrays)` |
# MAGIC | `explode()` | Expand array into rows | `explode(items)` |

# COMMAND ----------

# DBTITLE 1,6.1 size, indexing, membership — PySpark
# 6.1 Array size, indexing, and membership — PySpark

arrays = parsed_df.select(
    F.col("data.order_id").alias("order_id"),
    F.size("data.tags").alias("tag_count"),          # number of elements
    F.col("data.tags")[0].alias("first_tag"),        # zero-based indexing
    F.get(F.col("data.tags"), 1).alias("second_tag"),  # NULL when out of bounds
)
display(arrays)

# Exam note: under ANSI SQL, bracket indexing beyond the array length raises
# INVALID_ARRAY_INDEX (order 1002 has only one tag). get() tolerates an
# out-of-bounds index and returns NULL instead; element_at() is 1-based.

# array_contains() as a filter: keep orders tagged 'priority'
priority_orders = parsed_df.filter(F.array_contains(F.col("data.tags"), "priority"))
print("orders tagged 'priority':", priority_orders.count())  # 1 (order 1001)

# COMMAND ----------

# DBTITLE 1,6.2 size, indexing, membership — SQL
# MAGIC %sql
# MAGIC -- 6.2 Array size, indexing, and membership — SQL
# MAGIC SELECT
# MAGIC     data.order_id AS order_id,
# MAGIC     size(data.tags) AS tag_count,
# MAGIC     data.tags[0] AS first_tag,
# MAGIC     get(data.tags, 1) AS second_tag  -- NULL when out of bounds (ANSI-safe)
# MAGIC FROM parsed_orders;
# MAGIC
# MAGIC -- array_contains() in a WHERE clause
# MAGIC SELECT data.order_id AS order_id, data.tags AS tags
# MAGIC FROM parsed_orders
# MAGIC WHERE array_contains(data.tags, 'priority');

# COMMAND ----------

# DBTITLE 1,7. explode() — array to rows
# MAGIC %md
# MAGIC ## 7. explode() — the most important array transformation
# MAGIC
# MAGIC `explode()` transforms each element of an array into a **separate row** — e.g. the two-element `items` array of order 1001 becomes two order-item rows.

# COMMAND ----------

# DBTITLE 1,7.1 Explode items and extract fields
# 7.1 Explode the items array, then extract the struct fields

items_df = (
    parsed_df.select(
        F.col("data.order_id").alias("order_id"),
        F.explode("data.items").alias("item"),  # one row per array element
    )
    .select(
        "order_id",
        F.col("item.product_id").alias("product_id"),
        F.col("item.quantity").alias("quantity"),
        F.col("item.price").alias("price"),
    )
)
display(items_df)

# Expected:
#   1001 | P100 | 2 | 25.5
#   1001 | P200 | 1 | 40.0
#   1002 | P300 | 3 | 10.0

# COMMAND ----------

# DBTITLE 1,7.2 LATERAL VIEW EXPLODE — SQL
# MAGIC %sql
# MAGIC -- 7.2 SQL equivalent — LATERAL VIEW EXPLODE
# MAGIC SELECT
# MAGIC     data.order_id AS order_id,
# MAGIC     item.product_id AS product_id,
# MAGIC     item.quantity AS quantity,
# MAGIC     item.price AS price
# MAGIC FROM parsed_orders
# MAGIC LATERAL VIEW EXPLODE(data.items) exploded AS item;

# COMMAND ----------

# DBTITLE 1,8. explode vs explode_outer vs posexplode
# MAGIC %md
# MAGIC ## 8. explode() vs explode_outer() vs posexplode()
# MAGIC
# MAGIC | Function | Behavior |
# MAGIC | --- | --- |
# MAGIC | `explode()` | One row per element; NULL or empty arrays produce **no rows** |
# MAGIC | `explode_outer()` | Also preserves NULL or empty arrays with a NULL output row |
# MAGIC | `posexplode()` | Produces the element and its zero-based position |
# MAGIC | `posexplode_outer()` | Position-aware outer expansion |
# MAGIC
# MAGIC Given orders `1001 -> [P100, P200]`, `1002 -> []`, `1003 -> NULL`: `explode()` yields rows only for 1001; `explode_outer()` also represents 1002 and 1003 with NULL items.
# MAGIC
# MAGIC **Exam trap:** `explode()` can reduce the number of parent records represented in the output when arrays are empty or NULL.

# COMMAND ----------

# DBTITLE 1,8.1 Live demo — empty and NULL arrays
# 8.1 Live demo — empty and NULL arrays

df = spark.createDataFrame(
    [
        (1001, ["P100", "P200"], "One"),
        (1002, [], "two"),
        (1003, None, None),
    ],
    "order_id BIGINT, items ARRAY<STRING>, desc STRING",
)

explode_rows = df.select("order_id", F.explode("items").alias("item"), "desc")
outer_rows = df.select("order_id", F.explode_outer("items").alias("item"),  "desc")
pose_rows = df.select("order_id", F.posexplode("items").alias("position", "item"),  "desc")

print("explode:       ", explode_rows.count(), "rows (only order 1001)")
print("explode_outer: ", outer_rows.count(), "rows (1002 and 1003 kept with NULL)")
print("posexplode:    ", pose_rows.count(), "rows with zero-based position")
display(outer_rows)

# COMMAND ----------

# DBTITLE 1,9. split vs explode vs flatten
# MAGIC %md
# MAGIC ## 9. split() vs explode() vs flatten()
# MAGIC
# MAGIC Three different operations:
# MAGIC
# MAGIC | Function | Transformation | Example |
# MAGIC | --- | --- | --- |
# MAGIC | `split()` | String → array | `"A,B,C"` → `["A", "B", "C"]` |
# MAGIC | `explode()` | Array → rows | `["A", "B", "C"]` → three rows: A, B, C |
# MAGIC | `flatten()` | Nested arrays → one array | `[["A","B"], ["C"]]` → `["A", "B", "C"]` |

# COMMAND ----------

# DBTITLE 1,9.1 Live demo — split, explode, flatten
# 9.1 Live demo — split(), explode(), flatten()

# split(): string -> array
demo = spark.createDataFrame([("A,B,C",)], "categories STRING")
with_array = demo.select(F.split(F.col("categories"), ",").alias("category_array"))
display(with_array)  # ["A", "B", "C"]

# explode(): array -> rows
display(with_array.select(F.explode("category_array").alias("category")))  # 3 rows: A, B, C

# flatten(): one level of nested arrays -> a single array
nested = spark.createDataFrame(
    [([[["A", "B"], ["C"]]],)],
    "nested_array ARRAY<ARRAY<STRING>>",
)
display(nested.select(F.flatten("nested_array").alias("flat_array")))  # ["A", "B", "C"]

# COMMAND ----------

# DBTITLE 1,10. Higher-order array functions
# MAGIC %md
# MAGIC ## 10. Higher-order array functions — transform(), filter(), aggregate()
# MAGIC
# MAGIC These are **higher-order Spark functions**: they operate on array elements without creating additional rows.

# COMMAND ----------

# DBTITLE 1,10.1 transform, filter, aggregate — PySpark
# 10.1 transform, filter, aggregate — PySpark

numbers = spark.createDataFrame(
    [([10, 20, 30], [5, 15, 25])],
    "numbers ARRAY<INT>, more_numbers ARRAY<INT>",
)

display(
    numbers.select(
        F.transform("numbers", lambda x: x * 2).alias("doubled"),        # [20, 40, 60]
        F.filter("numbers", lambda x: x > 10).alias("filtered"),          # [20, 30]
        F.filter("more_numbers", lambda x: x > 10).alias("filtered2"),    # [15, 25]
        F.aggregate(
            "numbers", 
            F.lit(0), 
            lambda acc, x: acc + x
        ).alias("total"),  # 60
        F.aggregate(
            "numbers",
            initialValue=F.lit(1),  # Start at 1 for multiplication
            merge=lambda acc, x: acc * x,  # Multiply current item (x) by accumulator (acc)
        ).alias("product"),
    )
)

# COMMAND ----------

# DBTITLE 1,10.2 transform, filter, aggregate — SQL
# MAGIC %sql
# MAGIC -- 10.2 transform, filter, aggregate — SQL higher-order functions on literal arrays
# MAGIC SELECT
# MAGIC     transform(array(10, 20, 30), x -> x * 2) AS doubled,          -- [20, 40, 60]
# MAGIC     filter(array(5, 15, 25), x -> x > 10) AS filtered,            -- [15, 25]
# MAGIC     aggregate(array(10, 20, 30), 0, (acc, x) -> acc + x) AS total, -- 60
# MAGIC     aggregate(array(10, 20, 30), 1, (acc, x) -> acc * x) AS product; -- 6000

# COMMAND ----------

# DBTITLE 1,10.3 Order totals without exploding
# MAGIC %sql
# MAGIC
# MAGIC SELECT data.order_id AS order_id, data.items as items FROM parsed_orders;
# MAGIC
# MAGIC -- 10.3 Order totals computed directly from the nested items array — no explode needed
# MAGIC SELECT
# MAGIC     data.order_id AS order_id,
# MAGIC     aggregate(
# MAGIC         data.items,
# MAGIC         CAST(0.0 AS DOUBLE),
# MAGIC         (total, item) -> total + item.quantity * item.price
# MAGIC     ) AS order_total
# MAGIC FROM parsed_orders;
# MAGIC
# MAGIC -- Expected: 1001 -> 91.0 (2*25.5 + 1*40), 1002 -> 30.0 (3*10)

# COMMAND ----------

# DBTITLE 1,11. MAP data types
# MAGIC %md
# MAGIC ## 11. MAP data types
# MAGIC
# MAGIC A map stores key-value pairs, e.g. `"attributes": {"color": "red", "size": "large"}`.
# MAGIC
# MAGIC | Type | Holds |
# MAGIC | --- | --- |
# MAGIC | `STRUCT` | A defined set of named fields |
# MAGIC | `MAP` | Key-value entries, usually of a common key type and value type |
# MAGIC | `ARRAY` | An ordered collection of elements |
# MAGIC
# MAGIC Useful map functions: `map_keys()`, `map_values()`, `element_at()`.

# COMMAND ----------

# DBTITLE 1,11.1 Create and query a MAP
# 11.1 Create and query a MAP — PySpark

products = spark.createDataFrame(
    [
        ("P100", {"color": "red", "size": "large"}),
        ("P200", {"color": "blue", "size": "small"}),
    ],
    "product_id STRING, attributes MAP<STRING, STRING>",
)
products.createOrReplaceTempView("product_map")  # for the SQL cell below

display(
    products.select(
        "product_id",
        F.col("attributes")["color"].alias("color"),   # bracket access
        F.element_at("attributes", "size").alias("size"),  # element_at
        F.map_keys("attributes").alias("keys"),           # [color, size]
        F.map_values("attributes").alias("values"),        # [red, large]
    )
)

# COMMAND ----------

# DBTITLE 1,11.2 SQL map functions
# MAGIC %sql
# MAGIC -- 11.2 SQL equivalent — map subscript, element_at, map_keys, map_values
# MAGIC SELECT
# MAGIC     product_id,
# MAGIC     attributes['color'] AS color,
# MAGIC     element_at(attributes, 'size') AS size,
# MAGIC     map_keys(attributes) AS keys,
# MAGIC     map_values(attributes) AS values
# MAGIC FROM product_map;

# COMMAND ----------

# DBTITLE 1,12. End-to-end — JSON to Silver to Gold
# MAGIC %md
# MAGIC ## 12. End-to-end exercise — JSON to Silver to Gold
# MAGIC
# MAGIC Pipeline: `Raw JSON → Parsed orders → Exploded order items → Gold sales summary`
# MAGIC
# MAGIC Steps: (1) parse the JSON, (2) flatten order items, (3) write to Silver, (4) aggregate into Gold.
# MAGIC
# MAGIC | product_id | units_sold | revenue |
# MAGIC | --- | --- | --- |
# MAGIC | P100 | 2 | 51.00 |
# MAGIC | P200 | 1 | 40.00 |
# MAGIC | P300 | 3 | 30.00 |
# MAGIC
# MAGIC For real production data, account for schema evolution, invalid JSON, duplicates, precision, and incremental updates rather than overwriting the tables on every run.

# COMMAND ----------

# DBTITLE 1,12.1 Parse and flatten order items
# 12.1 Parse the JSON and flatten order items

parsed = raw_df.select(F.from_json("raw_json", schema).alias("data"))

silver_items = (
    parsed.select(
        F.col("data.order_id").alias("order_id"),
        F.col("data.customer.id").alias("customer_id"),
        F.explode("data.items").alias("item"),
    )
    .select(
        "order_id",
        "customer_id",
        F.col("item.product_id").alias("product_id"),
        F.col("item.quantity").alias("quantity"),
        F.col("item.price").alias("price"),
    )
    .withColumn("line_total", F.col("quantity") * F.col("price"))
)
display(silver_items)

# COMMAND ----------

# DBTITLE 1,12.2 Write to Silver
# 12.2 Write to Silver (Delta)

(
    silver_items.write.format("delta")
    .mode("overwrite")  # study table owned by this notebook — safe to rebuild on re-run
    .saveAsTable("lab_uc.silver.study_order_items")
)
print(
    "silver rows:",
    spark.table("lab_uc.silver.study_order_items").count(),
)  # 3 order items

# COMMAND ----------

# DBTITLE 1,12.3 Aggregate into Gold
# 12.3 Aggregate into Gold (Delta)

gold = (
    spark.table("lab_uc.silver.study_order_items")
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("units_sold"),
        F.sum("line_total").alias("revenue"),
    )
)
(
    gold.write.format("delta")
    .mode("overwrite")
    .saveAsTable("lab_uc.gold.study_product_sales")
)
display(spark.table("lab_uc.gold.study_product_sales"))

# Expected: P100 2/51.0, P200 1/40.0, P300 3/30.0

# COMMAND ----------

# DBTITLE 1,12.4 SQL equivalent — CREATE OR REPLACE
# MAGIC %sql
# MAGIC -- 12.4 SQL equivalent — CREATE OR REPLACE TABLE ... AS SELECT
# MAGIC CREATE OR REPLACE TABLE lab_uc.gold.study_product_sales AS
# MAGIC SELECT
# MAGIC     product_id,
# MAGIC     SUM(quantity) AS units_sold,
# MAGIC     SUM(quantity * price) AS revenue
# MAGIC FROM lab_uc.silver.study_order_items
# MAGIC GROUP BY product_id;
# MAGIC
# MAGIC SELECT *
# MAGIC FROM lab_uc.gold.study_product_sales
# MAGIC ORDER BY product_id;

# COMMAND ----------

# DBTITLE 1,13. Common exam mistakes
# MAGIC %md
# MAGIC ## 13. Common exam mistakes
# MAGIC
# MAGIC | Mistake | Correct understanding |
# MAGIC | --- | --- |
# MAGIC | Treating JSON string as STRUCT | Parse it using `from_json()` or an appropriate JSON function |
# MAGIC | Using `explode()` on a STRUCT | Use dot notation to access struct fields |
# MAGIC | Assuming `explode()` preserves empty arrays | Use `explode_outer()` when parent rows must be retained |
# MAGIC | Confusing `split()` with `explode()` | `split` produces an array; `explode` produces rows |
# MAGIC | Assuming array indices begin at 1 | Bracket indexing begins at 0 |
# MAGIC | Using `collect()` on huge datasets | Keep large transformations distributed |
# MAGIC | Using `dropDuplicates()` to retain latest row | Use an ordered window function |
# MAGIC | Using `flatten()` to create multiple rows | `flatten` returns an array; `explode` creates rows |
# MAGIC | Assuming malformed JSON always raises an error | Behavior depends on parsing mode and function |
# MAGIC | Overwriting Silver on every incremental run | Use an appropriate incremental or upsert strategy |

# COMMAND ----------

# DBTITLE 1,14. Practice questions (answers below)
# MAGIC %md
# MAGIC ## 14. Practice questions (answers below)
# MAGIC
# MAGIC 1. Which function converts a JSON string into a STRUCT or ARRAY using a supplied schema? — `to_json()` / `from_json()` / `explode()` / `split()`
# MAGIC 2. What does `explode(array('A','B','C'))` produce? — one row with three columns / one string `A,B,C` / three rows, one per element / a MAP
# MAGIC 3. Which expression extracts the city from a parsed nested STRUCT called `data`? — `data.customer.address.city` / `explode(data.customer)` / `split(data.customer, '.')` / `size(data.customer)`
# MAGIC 4. What is the key difference between `explode` and `explode_outer`? — sorts the array / removes duplicates / converts JSON into a struct / retains NULL or empty arrays with a NULL output row
# MAGIC 5. Which SQL function removes one level of array nesting? — `split()` / `flatten()` / `get_json_object()` / `row_number()`
# MAGIC 6. Which expression checks whether the `tags` array contains `'priority'`? — `size(tags) = 'priority'` / `explode(tags) = 'priority'` / `array_contains(tags, 'priority')` / `to_json(tags)`
# MAGIC 7. You want to double every number inside an array **without creating additional rows**. Which function is most suitable? — `transform()` / `explode()` / `union()` / `groupBy()`
# MAGIC 8. Which operation expands a nested `items` array into order-item rows? — `from_json()` only / `array_sort()` / `flatten()` only / `explode(items)`
# MAGIC
# MAGIC ### Answers
# MAGIC
# MAGIC 1. **B — `from_json()`** takes a JSON string plus a schema and returns a structured value.
# MAGIC 2. **C — three rows, one per element.**
# MAGIC 3. **A — `data.customer.address.city`** — dot notation; no explode needed for structs.
# MAGIC 4. **D — `explode_outer()` retains NULL/empty arrays** with a NULL output row.
# MAGIC 5. **B — `flatten()`** collapses one level of nested arrays into a single array.
# MAGIC 6. **C — `array_contains(tags, 'priority')`.**
# MAGIC 7. **A — `transform()`** modifies each element in place; no row expansion.
# MAGIC 8. **D — `explode(items)`** expands the array into rows.

# COMMAND ----------

# DBTITLE 1,15. Final syntax cheat sheet
# MAGIC %md
# MAGIC ## 15. Final JSON and array syntax cheat sheet
# MAGIC
# MAGIC | Task | PySpark / SQL |
# MAGIC | --- | --- |
# MAGIC | Parse JSON | `from_json(json_col, schema)` |
# MAGIC | Convert to JSON | `to_json(struct_col)` |
# MAGIC | Extract JSON path | `get_json_object(json_col, '$.field')` |
# MAGIC | Access struct | `data.customer.name` |
# MAGIC | Access array index | `items[0]` |
# MAGIC | Array length | `size(items)` |
# MAGIC | Array membership | `array_contains(tags, 'online')` |
# MAGIC | Array to rows | `explode(items)` |
# MAGIC | Preserve empty arrays | `explode_outer(items)` |
# MAGIC | Include array position | `posexplode(items)` |
# MAGIC | String to array | `split(text, ',')` |
# MAGIC | Nested arrays to array | `flatten(nested_array)` |
# MAGIC | Transform elements | `transform(items, x -> ...)` |
# MAGIC | Filter elements | `filter(items, x -> ...)` |
# MAGIC | Reduce array | `aggregate(items, initial, (acc, x) -> ...)` |
# MAGIC | Map keys | `map_keys(attributes)` |
# MAGIC | Map values | `map_values(attributes)` |
# MAGIC
# MAGIC For certification preparation, prioritize `from_json`, nested struct access, `explode`, `explode_outer`, `split`, `flatten`, and array indexing. Then practice higher-order array functions and end-to-end JSON ingestion pipelines.

# COMMAND ----------

# DBTITLE 1,16. Optional cleanup (do NOT run by default)
# MAGIC %md
# MAGIC ## 16. Optional cleanup (do NOT run by default)
# MAGIC
# MAGIC Intentionally **not executed** — run these statements only if you want to drop the tables this study guide created:
# MAGIC
# MAGIC ```sql
# MAGIC DROP TABLE IF EXISTS lab_uc.silver.study_order_items;
# MAGIC DROP TABLE IF EXISTS lab_uc.gold.study_product_sales;
# MAGIC ```
# MAGIC
# MAGIC Dropping is irreversible. The earlier lab tables in `lab_uc` (Labs 01–03) and the first study guide's tables are not touched by this notebook and remain available.