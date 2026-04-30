-- Sales organization master (from SAP TVKO).

CREATE OR REPLACE MATERIALIZED VIEW sales_org
(
  CONSTRAINT sales_org_id_not_null EXPECT (sales_org_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS SELECT
  VKORG                           AS sales_org_id,
  VTEXT                           AS sales_org_name,
  BUKRS                           AS company_code,
  LAND1                           AS country,
  current_timestamp()             AS silver_transformation_ts
FROM ${catalog}.${bronze_schema}.tvko;
