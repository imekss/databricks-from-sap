-- Plant / store master (from SAP T001W).
-- One row per plant, with sales org and plant type enrichment.

CREATE OR REPLACE MATERIALIZED VIEW plant
(
  CONSTRAINT plant_id_not_null EXPECT (plant_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS SELECT
  WERKS       AS plant_id,
  NAME1       AS plant_name,
  ORT01       AS city,
  LAND1       AS country,
  BUKRS       AS company_code,
  VKORG       AS sales_org_id,
  PTYPE       AS plant_type,
  current_timestamp() AS silver_transformation_ts
FROM ${catalog}.${bronze_schema}.t001w;
