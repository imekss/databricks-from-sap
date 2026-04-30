-- P&L line definitions (from project-specific DIM_PNL_LINE).
-- Defines the ordering and hierarchy of P&L statement lines (Revenue,
-- COGS, Gross Margin, OpEx, EBITDA, etc). LINE_TYPE = 'component' or 'calc'.

CREATE OR REPLACE MATERIALIZED VIEW pnl_line
(
  CONSTRAINT pnl_line_id_not_null EXPECT (pnl_line_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_line_type      EXPECT (line_type IN ('component', 'calc'))
)
AS SELECT
  PNL_LINE_ID                     AS pnl_line_id,
  PNL_LINE_NAME                   AS pnl_line_name,
  CAST(SORT_ORDER AS INT)         AS sort_order,
  LINE_TYPE                       AS line_type,
  current_timestamp()             AS silver_transformation_ts
FROM ${catalog}.${bronze_schema}.dim_pnl_line;
