# Databricks notebook source
# DBTITLE 1,Lab 02 — Unity Catalog Setup
# MAGIC %md
# MAGIC # Lab 02 — Unity Catalog Setup
# MAGIC
# MAGIC Walks through UC's full governance surface end-to-end: create a catalog and schema, add a managed table, set up a UC volume, apply GRANT / REVOKE / DENY, attach a row filter and a column mask, and verify enforcement.
# MAGIC
# MAGIC **Topics covered:**
# MAGIC * Three-level namespace: `catalog.schema.object`
# MAGIC * Managed vs external tables and volumes
# MAGIC * `GRANT` / `REVOKE` / `DENY` with privilege hierarchy
# MAGIC * Row filters and column masks as SQL UDFs
# MAGIC * Lineage and audit via `system.access.*`

# COMMAND ----------

# DBTITLE 1,Setup — make a workspace catalog
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Setup — make a workspace catalog
# MAGIC -- ============================================================
# MAGIC -- Replace 'lab_uc' with a catalog name you control
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS lab_uc COMMENT 'UC lab catalog';
# MAGIC USE CATALOG lab_uc;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS hr
# MAGIC   COMMENT 'Employee data — restricted';
# MAGIC -- UC uses the catalog's default managed location when MANAGED LOCATION is omitted.
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS lab_uc.hr.docs;

# COMMAND ----------

# DBTITLE 1,Step 1 — Create a managed Delta table
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Step 1 — Create a managed Delta table
# MAGIC -- ============================================================
# MAGIC
# MAGIC CREATE TABLE lab_uc.hr.employees (
# MAGIC   employee_id INT,
# MAGIC   name        STRING,
# MAGIC   email       STRING,        -- PII: will be column-masked
# MAGIC   salary      DECIMAL(10,2), -- sensitive: will be column-masked
# MAGIC   region      STRING,        -- used by row filter
# MAGIC   is_active   BOOLEAN
# MAGIC );
# MAGIC
# MAGIC INSERT INTO lab_uc.hr.employees VALUES
# MAGIC   (1, 'Alice Anderson', 'alice@example.com', 120000.00, 'AMER', true),
# MAGIC   (2, 'Bao Banh',       'bao@example.com',   95000.00,  'APAC', true),
# MAGIC   (3, 'Carla Costa',    'carla@example.com', 105000.00, 'EMEA', true),
# MAGIC   (4, 'Dan Davies',     'dan@example.com',   110000.00, 'AMER', false);

# COMMAND ----------

# DBTITLE 1,Step 2 — GRANT / REVOKE / DENY (note)
# MAGIC %md
# MAGIC ## Step 2 — GRANT / REVOKE / DENY
# MAGIC
# MAGIC > **Note:** `DENY` is *evaluated independently from GRANT*. If a user is in both `analysts` (grant) and `exec-leadership-blocked` (deny), the deny wins. `REVOKE` only removes an explicit grant; it does not block access.

# COMMAND ----------

# DBTITLE 1,Step 2 (pre) — Create workspace groups
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Create the two groups needed for the GRANT/DENY lab
for group_name in ["analysts", "exec-leadership-blocked"]:
    try:
        result = w.groups.create(display_name=group_name)
        print(f"Created group: {group_name} (id: {result.id})")
    except Exception as e:
        if "already exists" in str(e).lower() or "RESOURCE_ALREADY_EXISTS" in str(e):
            print(f"Group already exists: {group_name}")
        else:
            print(f"Error creating group '{group_name}': {e}")

# COMMAND ----------

# DBTITLE 1,Step 2 — GRANT / REVOKE / DENY
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Step 2 — GRANT / REVOKE / DENY
# MAGIC -- ============================================================
# MAGIC -- Unity Catalog GRANT/DENY only recognizes ACCOUNT-LEVEL groups, not workspace groups.
# MAGIC -- The groups created in cell 5 are workspace-level, so UC reports PRINCIPAL_DOES_NOT_EXIST.
# MAGIC -- For this lab, we GRANT to the current user directly (UC recognizes user principals by email).
# MAGIC -- In production, create account groups via the Account Console and grant to those.
# MAGIC
# MAGIC -- Read access for analysts
# MAGIC GRANT USE CATALOG ON CATALOG lab_uc TO `vincentnan@yahoo.com`;
# MAGIC GRANT USE SCHEMA ON SCHEMA lab_uc.hr TO `vincentnan@yahoo.com`;
# MAGIC GRANT SELECT ON TABLE  lab_uc.hr.employees TO `vincentnan@yahoo.com`;
# MAGIC
# MAGIC -- Hard-deny pattern: DENY to a workspace group also fails (UC can't see it).
# MAGIC -- To demonstrate DENY without blocking your own Step 5 SELECT queries, this is commented out.
# MAGIC -- In a real scenario with account groups: DENY SELECT ON TABLE lab_uc.hr.employees TO `exec-leadership-blocked`;
# MAGIC -- DENY SELECT ON TABLE lab_uc.hr.employees TO 'vincentnan@yahoo.com';

# COMMAND ----------

# DBTITLE 1,Step 3 — Row filter (explanation)
# MAGIC %md
# MAGIC ## Step 3 — Row filter (restrict by region)
# MAGIC
# MAGIC When a member of `emea-admins` runs `SELECT * FROM lab_uc.hr.employees`, they see only `region = 'EMEA'` rows. A member of `global-admins` sees everything.

# COMMAND ----------

# DBTITLE 1,Step 3 — Row filter SQL
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Step 3 — Row filter (restrict by region)
# MAGIC -- ============================================================
# MAGIC
# MAGIC -- Create a UDF that decides which rows the calling user can see
# MAGIC CREATE OR REPLACE FUNCTION lab_uc.hr.region_filter(region_col STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN
# MAGIC   is_account_group_member('emea-admins')   AND region_col = 'EMEA'
# MAGIC   OR is_account_group_member('amer-admins') AND region_col = 'AMER'
# MAGIC   OR is_account_group_member('apac-admins') AND region_col = 'APAC'
# MAGIC   OR is_account_group_member('global-admins');
# MAGIC
# MAGIC -- Attach the row filter to the table
# MAGIC ALTER TABLE lab_uc.hr.employees
# MAGIC   SET ROW FILTER lab_uc.hr.region_filter ON (region);

# COMMAND ----------

# DBTITLE 1,Step 4 — Column mask (explanation)
# MAGIC %md
# MAGIC ## Step 4 — Column mask (hide PII from non-privileged users)
# MAGIC
# MAGIC Non-`pii-cleared` users see `[redacted-<hash>]` instead of an email; non-`hr-comp-admins` see `NULL` for salary. **UC applies the mask at query time** — the underlying data is unchanged.

# COMMAND ----------

# DBTITLE 1,Step 4 — Column mask SQL
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Step 4 — Column mask (hide PII from non-privileged users)
# MAGIC -- ============================================================
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION lab_uc.hr.mask_email(email_col STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE
# MAGIC          WHEN is_account_group_member('pii-cleared') THEN email_col
# MAGIC          ELSE concat('[redacted-', sha2(email_col, 256), ']')
# MAGIC        END;
# MAGIC
# MAGIC ALTER TABLE lab_uc.hr.employees
# MAGIC   ALTER COLUMN email SET MASK lab_uc.hr.mask_email;
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION lab_uc.hr.mask_salary(salary_col DECIMAL(10,2))
# MAGIC RETURNS DECIMAL(10,2)
# MAGIC RETURN CASE
# MAGIC          WHEN is_account_group_member('hr-comp-admins') THEN salary_col
# MAGIC          ELSE NULL
# MAGIC        END;
# MAGIC
# MAGIC ALTER TABLE lab_uc.hr.employees
# MAGIC   ALTER COLUMN salary SET MASK lab_uc.hr.mask_salary;

# COMMAND ----------

# DBTITLE 1,Step 5 — Verify and inspect (warning)
# MAGIC %md
# MAGIC ## Step 5 — Verify and inspect
# MAGIC
# MAGIC > **Warning:** The `system.access.*` tables refresh on a delay (minutes to ~1 hour). Don't expect events from the last 60 seconds to appear immediately.

# COMMAND ----------

# DBTITLE 1,Step 5 — Verify and inspect SQL
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Step 5 — Verify and inspect
# MAGIC -- ============================================================
# MAGIC
# MAGIC SELECT * FROM lab_uc.hr.employees;

# COMMAND ----------

# DBTITLE 1,Step 5 — Show grants
# MAGIC %sql
# MAGIC -- What grants are on the table?
# MAGIC SHOW GRANTS ON TABLE lab_uc.hr.employees;

# COMMAND ----------

# DBTITLE 1,Step 5 — Recent access events
# MAGIC %sql
# MAGIC -- Recent access events for this catalog
# MAGIC SELECT
# MAGIC   event_time,
# MAGIC   user_identity.email AS user_email,
# MAGIC   action_name,
# MAGIC   request_params.full_name_arg AS object
# MAGIC FROM system.access.audit
# MAGIC WHERE service_name = 'unityCatalog'
# MAGIC   AND event_time >= current_timestamp - INTERVAL 1 DAY
# MAGIC   AND request_params.full_name_arg LIKE 'lab_uc.%'
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,Step 5 — Table lineage
# MAGIC %sql
# MAGIC -- Table lineage (who reads/writes this table)
# MAGIC SELECT *
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE source_table_full_name = 'lab_uc.hr.employees'
# MAGIC    OR target_table_full_name = 'lab_uc.hr.employees'
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# DBTITLE 1,Step 6 — External Delta table (explanation)
# MAGIC %md
# MAGIC ## Step 6 — Add an external Delta table for comparison
# MAGIC
# MAGIC External tables: UC owns the metadata; you own the storage. Dropping with `DROP TABLE` removes the metadata but leaves the files behind. Pre-requisite: create a storage credential + external location on your cloud account.

# COMMAND ----------

# DBTITLE 1,Step 6 — External Delta table SQL
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Step 6 — Add an external Delta table for comparison
# MAGIC -- ============================================================
# MAGIC -- Pre-requisite: create a storage credential + external location on your cloud account.
# MAGIC -- The DDL is intentionally illustrative; you'll need to fill in your own external location URL.
# MAGIC
# MAGIC -- CREATE STORAGE CREDENTIAL lab_credential ...;
# MAGIC -- CREATE EXTERNAL LOCATION lab_external_loc
# MAGIC --   URL 's3://my-bucket/lab/'
# MAGIC --   WITH STORAGE CREDENTIAL lab_credential;
# MAGIC
# MAGIC CREATE TABLE lab_uc.hr.archived_employees (
# MAGIC   -- same schema as employees
# MAGIC   employee_id INT,
# MAGIC   name        STRING,
# MAGIC   region      STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://my-bucket/lab/archived_employees';

# COMMAND ----------

# DBTITLE 1,Cleanup
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Cleanup
# MAGIC -- ============================================================
# MAGIC
# MAGIC ALTER TABLE lab_uc.hr.employees DROP ROW FILTER;
# MAGIC ALTER TABLE lab_uc.hr.employees ALTER COLUMN email  DROP MASK;
# MAGIC ALTER TABLE lab_uc.hr.employees ALTER COLUMN salary DROP MASK;
# MAGIC DROP FUNCTION IF EXISTS lab_uc.hr.region_filter;
# MAGIC DROP FUNCTION IF EXISTS lab_uc.hr.mask_email;
# MAGIC DROP FUNCTION IF EXISTS lab_uc.hr.mask_salary;
# MAGIC DROP TABLE IF EXISTS lab_uc.hr.employees;
# MAGIC DROP TABLE IF EXISTS lab_uc.hr.archived_employees;
# MAGIC DROP VOLUME IF EXISTS lab_uc.hr.docs;
# MAGIC DROP SCHEMA IF EXISTS lab_uc.hr CASCADE;
# MAGIC -- Optionally: DROP CATALOG lab_uc CASCADE;

# COMMAND ----------

# DBTITLE 1,Related Study Material
# MAGIC %md
# MAGIC ## Related Study Material
# MAGIC
# MAGIC * [Unity Catalog basics (shared)](../shared/fundamentals/unity-catalog-basics.md)
# MAGIC * [Unity Catalog cheat sheet (shared)](../shared/cheat-sheets/unity-catalog-quick-ref.md)
# MAGIC * [DE Pro — Data Governance](../certifications/data-engineer-professional/08-data-governance/README.md)
# MAGIC * [DE Pro — Ensuring Data Security and Compliance](../certifications/data-engineer-professional/05-ensuring-data-security-and-compliance/README.md)
# MAGIC * [DA — Securing Data](../certifications/data-analyst-associate/07-securing-data/README.md)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **[← Previous: Medallion ingestion](./01-medallion-ingestion.md) | [↑ Back to Labs](./README.md) | [Next: Lakeflow Declarative Pipelines →](./03-lakeflow-declarative-pipelines.md)**