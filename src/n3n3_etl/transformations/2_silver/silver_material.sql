-- Material master (from SAP MARA).
-- One row per SKU, with fashion-retail attributes (brand, season, color, size, style).

CREATE OR REPLACE MATERIALIZED VIEW material
(
  CONSTRAINT material_id_not_null EXPECT (material_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS SELECT
  MATNR       AS material_id,
  MAKTX       AS material_name,
  MTART       AS material_type,
  MATKL       AS material_group,
  SPART       AS division_id,
  MEINS       AS base_uom,
  PRDHA       AS product_hierarchy,
  BRAND       AS brand,
  SAISO       AS season,
  COLOR       AS color,
  SIZE        AS size,
  STYLE       AS style,
  current_timestamp() AS silver_transformation_ts
FROM ${catalog}.${bronze_schema}.mara;
