-- Customer master (from SAP KNA1).
-- One row per customer, with classification fields for segmentation.

CREATE OR REPLACE MATERIALIZED VIEW customer
(
  CONSTRAINT customer_id_not_null EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS SELECT
  KUNNR       AS customer_id,
  NAME1       AS customer_name,
  LAND1       AS country,
  ORT01       AS city,
  KTOKD       AS account_group,
  KUKLA       AS customer_class,
  current_timestamp() AS silver_transformation_ts
FROM ${catalog}.${bronze_schema}.kna1;
