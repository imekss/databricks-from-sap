-- Profit center master (from SAP CEPC).
-- One row per profit center; date strings cast to proper DATE type.

CREATE OR REPLACE MATERIALIZED VIEW profit_center
(
  CONSTRAINT profit_center_id_not_null EXPECT (profit_center_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_period EXPECT (valid_from <= valid_to)
)
AS SELECT
  PRCTR                           AS profit_center_id,
  KOKRS                           AS controlling_area,
  BUKRS                           AS company_code,
  to_date(DATAB, 'yyyy-MM-dd')    AS valid_from,
  to_date(DATBI, 'yyyy-MM-dd')    AS valid_to,
  KTEXT                           AS profit_center_name,
  LTEXT                           AS profit_center_long_name,
  GSBER                           AS business_area,
  LAND1                           AS country,
  current_timestamp()             AS silver_transformation_ts
FROM ${catalog}.${bronze_schema}.cepc;
