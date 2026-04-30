-- Division master (from SAP TSPA).
-- Product divisions (e.g. MEN, WOMEN, KIDS in fashion context).

CREATE OR REPLACE MATERIALIZED VIEW division
(
  CONSTRAINT division_id_not_null EXPECT (division_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS SELECT
  SPART                           AS division_id,
  VTEXT                           AS division_name,
  current_timestamp()             AS silver_transformation_ts
FROM ${catalog}.${bronze_schema}.tspa;
