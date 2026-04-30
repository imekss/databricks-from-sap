-- Financial postings (the core fact table of the project).
--
-- This is the "single source of truth" — ACDOCA universal journal lines
-- enriched with BKPF document header info and every relevant dimension.
-- Downstream gold marts (gold_profit_and_loss, gold_cost_reporting, etc.)
-- all build from this table.
--
-- Grain: one row per accounting document line item (matches ACDOCA grain).
-- Amount columns follow the SAP triple-currency model (transaction / local / group).
-- Group currency (KSL → amount_group_currency) is used for consolidated reporting.

CREATE OR REPLACE MATERIALIZED VIEW financial_postings
(
  CONSTRAINT document_key_not_null EXPECT (document_number IS NOT NULL AND fiscal_year IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_fiscal_year     EXPECT (fiscal_year BETWEEN 2000 AND 2100),
  CONSTRAINT company_code_not_null EXPECT (company_code IS NOT NULL)
)
AS SELECT
    -- Keys
    a.RLDNR                                          AS ledger,
    a.RBUKRS                                         AS company_code,
    a.BELNR                                          AS document_number,
    a.GJAHR                                          AS fiscal_year,
    CAST(a.BUZEI AS INT)                             AS line_item_number,

    -- Dates & periods
    to_date(a.BUDAT, 'yyyy-MM-dd')                   AS posting_date,
    CAST(a.MONAT AS INT)                             AS fiscal_period,

    -- Account info
    a.RACCT                                          AS account_id,
    s.TXT50                                          AS account_name,
    s.KTOKS                                          AS account_group,
    CASE WHEN s.GVTYP = 'X' THEN 'P&L'
         WHEN s.XBILK = 'X' THEN 'BS'
         ELSE 'OTHER' END                            AS account_type,

    -- Account Mapping to P&L Lines
    map.PNL_LINE_ID                                  AS pnl_line_id,
    pnl.PNL_LINE_NAME                                AS pnl_line_name,
    CAST(pnl.SORT_ORDER AS INT)                      AS pnl_sort_order,
    pnl.LINE_TYPE                                    AS pnl_line_type,
    CAST(map.SIGN AS INT)                            AS pnl_sign,

    -- Amounts & currencies 
    a.RWCUR                                          AS transaction_currency,
    CAST(a.TSL AS DECIMAL(18,2))                     AS amount_transaction_currency,
    a.RHCUR                                          AS local_currency,
    CAST(a.HSL AS DECIMAL(18,2))                     AS amount_local_currency,
    a.KWAER                                          AS group_currency,
    CAST(a.KSL AS DECIMAL(18,2))                     AS amount_group_currency,

    -- Quantities
    CAST(a.MENGE AS DECIMAL(18,2))                   AS quantity,
    a.MEINS                                          AS base_uom,

    -- Posting info
    a.DRCRK                                          AS debit_credit_indicator,
    a.LINETYPE                                       AS line_type,
    a.RVERS                                          AS version,
    a.BSTAT                                          AS document_status,

    -- Company info
    t.BUTXT                                          AS company_name,
    t.LAND1                                          AS company_country,
    t.WAERS                                          AS company_currency,

    -- Organizational dimensions
    a.KOSTL                                          AS cost_center_id,
    c.KTEXT                                          AS cost_center_name,
    c.KOSAR                                          AS cost_center_function,
    c.LAND1                                          AS cost_center_country,

    a.PRCTR                                          AS profit_center_id,
    pc.KTEXT                                         AS profit_center_name,
    pc.GSBER                                         AS business_area,
    pc.LAND1                                         AS profit_center_country,

    a.WERKS                                          AS plant_id,
    p.NAME1                                          AS plant_name,
    p.ORT01                                          AS plant_city,
    p.LAND1                                          AS plant_country,
    p.PTYPE                                          AS plant_type,

    -- Commercial dimensions
    a.VKORG                                          AS sales_org_id,
    vo.VTEXT                                         AS sales_org_name,

    a.VTWEG                                          AS distribution_channel_id,
    vw.VTEXT                                         AS distribution_channel_name,

    a.SPART                                          AS division_id,
    sp.VTEXT                                         AS division_name,

    -- Material info
    a.MATNR                                          AS material_id,
    m.MAKTX                                          AS material_name,
    m.MTART                                          AS material_type,
    m.MATKL                                          AS material_group,
    m.PRDHA                                          AS product_hierarchy,
    m.BRAND                                          AS brand,
    m.SAISO                                          AS season,
    m.COLOR                                          AS color,
    m.SIZE                                           AS size,
    m.STYLE                                          AS style,

    -- Customer info
    a.KUNNR                                          AS customer_id,
    k.NAME1                                          AS customer_name,
    k.LAND1                                          AS customer_country,
    k.ORT01                                          AS customer_city,
    k.KTOKD                                          AS customer_account_group,

    -- Document header info from BKPF
    b.BLART                                          AS document_type,
    b.BKTXT                                          AS document_header_text,
    b.XBLNR                                          AS reference_document_number,
    b.USNAM                                          AS created_by_user,
    b.TCODE                                          AS transaction_code,
    to_date(b.BLDAT, 'yyyy-MM-dd')                   AS document_date,

    -- Metadata
    current_timestamp()                              AS silver_transformation_ts,
    a._ingestion_ts                                  AS bronze_ingestion_ts,
    a._source_file                                   AS bronze_source_file

FROM ${catalog}.${bronze_schema}.acdoca a

LEFT JOIN ${catalog}.${bronze_schema}.bkpf b
    ON a.BELNR  = b.BELNR
   AND a.GJAHR  = b.GJAHR
   AND a.RBUKRS = b.BUKRS

LEFT JOIN ${catalog}.${bronze_schema}.t001 t
    ON a.RBUKRS = t.BUKRS

LEFT JOIN ${catalog}.${bronze_schema}.ska1 s
    ON a.RACCT = s.SAKNR

LEFT JOIN ${catalog}.${bronze_schema}.map_account_to_pnl_line map
    ON a.RACCT = map.SAKNR

LEFT JOIN ${catalog}.${bronze_schema}.dim_pnl_line pnl
    ON map.PNL_LINE_ID = pnl.PNL_LINE_ID

LEFT JOIN ${catalog}.${bronze_schema}.kna1 k
    ON a.KUNNR = k.KUNNR

LEFT JOIN ${catalog}.${bronze_schema}.mara m
    ON a.MATNR = m.MATNR

LEFT JOIN ${catalog}.${bronze_schema}.t001w p
    ON a.WERKS = p.WERKS

LEFT JOIN ${catalog}.${bronze_schema}.csks c
    ON a.KOSTL  = c.KOSTL
   AND a.RBUKRS = c.BUKRS

LEFT JOIN ${catalog}.${bronze_schema}.cepc pc
    ON a.PRCTR  = pc.PRCTR
   AND a.RBUKRS = pc.BUKRS

LEFT JOIN ${catalog}.${bronze_schema}.tvko vo
    ON a.VKORG = vo.VKORG

LEFT JOIN ${catalog}.${bronze_schema}.tvtw vw
    ON a.VTWEG = vw.VTWEG

LEFT JOIN ${catalog}.${bronze_schema}.tspa sp
    ON a.SPART = sp.SPART
;