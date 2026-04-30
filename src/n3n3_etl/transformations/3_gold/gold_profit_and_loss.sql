-- Gold mart: Profit and Loss statement by company / period / channel / division.
--
-- Source: financial_postings (the fully-enriched fact table).
-- Gold does NOT read from bronze — financial_postings already has
-- the account→P&L mapping and sign convention baked in.
--
-- Structure: direct P&L component lines (from SAP accounts mapped to PNL lines)
-- UNION ALL with calculated subtotal lines (Net Sales, Gross Profit, EBITDA, EBIT).
--
-- Sign convention:
--   financial_postings.pnl_sign comes from the MAP_ACCOUNT_TO_PNL_LINE
--   table (typically -1 for all P&L accounts). SAP stores revenue as negative
--   (credit) and costs as positive (debit). Multiplying by pnl_sign flips to the
--   intuitive view: revenue positive, costs negative.

CREATE OR REPLACE MATERIALIZED VIEW profit_and_loss
(
  CONSTRAINT company_code_not_null EXPECT (company_code IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT fiscal_year_valid     EXPECT (fiscal_year BETWEEN 2000 AND 2100),
  CONSTRAINT amount_is_number      EXPECT (amount_eur IS NOT NULL)
)
COMMENT "P&L statement by company × period × version × channel × division, in EUR group currency."
AS
WITH direct_lines AS (
  -- Aggregate P&L component lines using the pre-joined account mapping
  -- in financial_postings. Only P&L accounts contribute.
  SELECT
    s.company_code,
    s.company_name,
    s.company_country,
    s.fiscal_year,
    s.fiscal_period,
    s.version,
    s.distribution_channel_id,
    s.distribution_channel_name,
    s.division_id,
    s.division_name,
    s.sales_org_id,
    s.sales_org_name,
    s.pnl_line_id,
    s.pnl_line_name,
    s.pnl_sort_order                         AS sort_order,
    s.pnl_line_type                          AS line_type,
    SUM(s.amount_group_currency * s.pnl_sign) AS amount_eur
  FROM ${catalog}.${silver_schema}.financial_postings s
  WHERE s.account_type = 'P&L'
    AND s.pnl_line_id IS NOT NULL
  GROUP BY
    s.company_code, s.company_name, s.company_country,
    s.fiscal_year, s.fiscal_period, s.version,
    s.distribution_channel_id, s.distribution_channel_name,
    s.division_id, s.division_name,
    s.sales_org_id, s.sales_org_name,
    s.pnl_line_id, s.pnl_line_name, s.pnl_sort_order, s.pnl_line_type
),

pivoted AS (
  -- Pivot direct lines into wide columns so we can compute subtotals.
  SELECT
    company_code, company_name, company_country,
    fiscal_year, fiscal_period, version,
    distribution_channel_id, distribution_channel_name,
    division_id, division_name,
    sales_org_id, sales_org_name,

    SUM(CASE WHEN pnl_line_id = 'PL01' THEN amount_eur ELSE 0 END) AS pl01,
    SUM(CASE WHEN pnl_line_id = 'PL02' THEN amount_eur ELSE 0 END) AS pl02,
    SUM(CASE WHEN pnl_line_id = 'PL04' THEN amount_eur ELSE 0 END) AS pl04,
    SUM(CASE WHEN pnl_line_id IN ('PL06','PL07','PL08','PL09','PL10','PL11','PL12')
             THEN amount_eur ELSE 0 END)                           AS opex_bundle,
    SUM(CASE WHEN pnl_line_id = 'PL13' THEN amount_eur ELSE 0 END) AS pl13
  FROM direct_lines
  GROUP BY
    company_code, company_name, company_country,
    fiscal_year, fiscal_period, version,
    distribution_channel_id, distribution_channel_name,
    division_id, division_name,
    sales_org_id, sales_org_name
),

calculated_lines AS (
  -- Subtotal definitions:
  --   PL03 Net Sales      = PL01 + PL02
  --   PL05 Gross Profit   = PL03 + PL04
  --   PL14 EBITDA         = PL05 + (PL06..PL12)
  --   PL15 EBIT           = PL14 + PL13
  SELECT
    company_code, company_name, company_country,
    fiscal_year, fiscal_period, version,
    distribution_channel_id, distribution_channel_name,
    division_id, division_name,
    sales_org_id, sales_org_name,

    pl01 + pl02                                AS net_sales,
    pl01 + pl02 + pl04                         AS gross_profit,
    pl01 + pl02 + pl04 + opex_bundle           AS ebitda,
    pl01 + pl02 + pl04 + opex_bundle + pl13    AS ebit
  FROM pivoted
),

final AS (
  -- Direct lines (from SAP account mapping) UNION ALL with calculated subtotals.
  -- Each UNION branch lists columns explicitly to avoid silent shape drift.
  SELECT
    company_code,
    company_name,
    company_country,
    fiscal_year,
    fiscal_period,
    version,
    distribution_channel_id,
    distribution_channel_name,
    division_id,
    division_name,
    sales_org_id,
    sales_org_name,
    pnl_line_id,
    pnl_line_name,
    sort_order,
    line_type,
    amount_eur
  FROM direct_lines

  UNION ALL

  SELECT
    company_code,
    company_name,
    company_country,
    fiscal_year,
    fiscal_period,
    version,
    distribution_channel_id,
    distribution_channel_name,
    division_id,
    division_name,
    sales_org_id,
    sales_org_name,
    'PL03'           AS pnl_line_id,
    'Net Sales'      AS pnl_line_name,
    30               AS sort_order,
    'calc'           AS line_type,
    net_sales        AS amount_eur
  FROM calculated_lines

  UNION ALL

  SELECT
    company_code,
    company_name,
    company_country,
    fiscal_year,
    fiscal_period,
    version,
    distribution_channel_id,
    distribution_channel_name,
    division_id,
    division_name,
    sales_org_id,
    sales_org_name,
    'PL05'           AS pnl_line_id,
    'Gross Profit'   AS pnl_line_name,
    50               AS sort_order,
    'calc'           AS line_type,
    gross_profit     AS amount_eur
  FROM calculated_lines

  UNION ALL

  SELECT
    company_code,
    company_name,
    company_country,
    fiscal_year,
    fiscal_period,
    version,
    distribution_channel_id,
    distribution_channel_name,
    division_id,
    division_name,
    sales_org_id,
    sales_org_name,
    'PL14'           AS pnl_line_id,
    'EBITDA'         AS pnl_line_name,
    140              AS sort_order,
    'calc'           AS line_type,
    ebitda           AS amount_eur
  FROM calculated_lines

  UNION ALL

  SELECT
    company_code,
    company_name,
    company_country,
    fiscal_year,
    fiscal_period,
    version,
    distribution_channel_id,
    distribution_channel_name,
    division_id,
    division_name,
    sales_org_id,
    sales_org_name,
    'PL15'           AS pnl_line_id,
    'EBIT'           AS pnl_line_name,
    150              AS sort_order,
    'calc'           AS line_type,
    ebit             AS amount_eur
  FROM calculated_lines
)

-- Thin wrapper: add the transformation timestamp once, emit the final rowset.
SELECT
  f.*,
  current_timestamp() AS gold_transformation_ts
FROM final f;