-- Distribution channel master (from SAP TVTW).
-- Examples: RETAIL, WHOLESALE, ECOMMERCE.

CREATE OR REPLACE MATERIALIZED VIEW channel
(
  CONSTRAINT channel_id_not_null EXPECT (channel_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS SELECT
  VTWEG                           AS channel_id,
  VTEXT                           AS channel_name,
  current_timestamp()             AS silver_transformation_ts
FROM ${catalog}.${bronze_schema}.tvtw;
