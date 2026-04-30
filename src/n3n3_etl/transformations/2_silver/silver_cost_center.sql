-- Cost center master (from SAP CSKS).
-- One row per cost center; date strings cast to proper DATE type.

CREATE OR REPLACE MATERIALIZED VIEW cost_center
(
  CONSTRAINT cost_center_id_not_null EXPECT (cost_center_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_period EXPECT (valid_from <= valid_to)
)
AS SELECT
  KOSTL                           AS cost_center_id,
  KOKRS                           AS controlling_area,
  BUKRS                           AS company_code,
  to_date(DATAB, 'yyyy-MM-dd')    AS valid_from,
  to_date(DATBI, 'yyyy-MM-dd')    AS valid_to,
  KTEXT                           AS cost_center_name,
  LTEXT                           AS cost_center_long_name,
  KOSAR                           AS cost_center_function,
  LAND1                           AS country,
  current_timestamp()             AS silver_transformation_ts
FROM ${catalog}.${bronze_schema}.csks;
