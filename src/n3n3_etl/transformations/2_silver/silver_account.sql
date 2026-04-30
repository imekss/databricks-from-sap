-- G/L account master enriched with P&L mapping.
--
-- This is the KEY dimension for financial reporting:
--   - SKA1 (G/L account master) joined with
--   - MAP_ACCOUNT_TO_PNL_LINE (the sign convention + which P&L line it rolls up to) joined with
--   - DIM_PNL_LINE (the human-readable name and sort order of the P&L line)
--
-- Result: every G/L account gets a P&L line it contributes to (if any) and
-- the sign to apply when aggregating. This is what lets gold_profit_and_loss
-- compute a correct P&L statement.

CREATE OR REPLACE MATERIALIZED VIEW account
(
  CONSTRAINT account_id_not_null EXPECT (account_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_account_type  EXPECT (account_type IN ('P&L', 'BS', 'OTHER'))
)
AS SELECT
  s.SAKNR                         AS account_id,
  s.TXT50                         AS account_name,
  s.TXT20                         AS account_short_name,
  s.KTOKS                         AS account_group,
  CASE
      WHEN s.GVTYP = 'X' THEN 'P&L'
      WHEN s.XBILK = 'X' THEN 'BS'
      ELSE 'OTHER'
  END                             AS account_type,
  m.PNL_LINE_ID                   AS pnl_line_id,
  d.PNL_LINE_NAME                 AS pnl_line_name,
  CAST(d.SORT_ORDER AS INT)       AS pnl_sort_order,
  d.LINE_TYPE                     AS pnl_line_type,
  CAST(m.SIGN AS INT)             AS pnl_sign,
  current_timestamp()             AS silver_transformation_ts
FROM ${catalog}.${bronze_schema}.ska1 s
LEFT JOIN ${catalog}.${bronze_schema}.map_account_to_pnl_line m
  ON s.SAKNR = m.SAKNR
LEFT JOIN ${catalog}.${bronze_schema}.dim_pnl_line d
  ON m.PNL_LINE_ID = d.PNL_LINE_ID;
