-- Company code master (from SAP T001).
-- One row per company code, renamed to business-friendly names.

CREATE OR REPLACE MATERIALIZED VIEW company
(
  CONSTRAINT company_code_not_null EXPECT (company_code IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT country_is_iso       EXPECT (length(country) = 2)
)
AS SELECT
  BUKRS       AS company_code,
  BUTXT       AS company_name,
  LAND1       AS country,
  WAERS       AS local_currency,
  KTOPL       AS chart_of_accounts,
  PERIV       AS fiscal_year_variant,
  ORT01       AS city,
  current_timestamp() AS silver_transformation_ts
FROM ${catalog}.${bronze_schema}.t001;
