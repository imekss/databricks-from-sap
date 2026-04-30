"""
Bronze schema contracts for SAP source tables.

Each StructType below mirrors the shape of an SAP table (or synthesized
analog for this project). The bronze loader enforces these contracts at
read time via FAILFAST mode — if the source file doesn't match, the job
errors immediately instead of silently producing bad data downstream.

Why every field is StringType:
-------------------------------
Bronze is the "raw landing zone." Its job is to preserve the source
exactly, not to interpret it. Type casting (e.g. GJAHR → int, BUDAT → date)
happens at Silver, where business logic lives. Keeping Bronze as-strings:

  1. Preserves leading zeros (e.g. account "0012345")
  2. Preserves dates in their source format (YYYYMMDD strings)
  3. Makes round-tripping back to SAP trivial
  4. Eliminates an entire class of type-inference bugs

The registry at the bottom maps source names (matching CSV filenames,
case-insensitive) to the schemas. The loader looks up schemas by this key.
"""

from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType


# ─────────────────────────────────────────────────────────────────────────────
# ACDOCA — Universal Journal (the fact table)
# SAP source table: ACDOCA
# Grain: one row per accounting document line item
# ─────────────────────────────────────────────────────────────────────────────
ACDOCA_SCHEMA = StructType(
    [
        StructField("RLDNR", StringType(), True),     # Ledger (0L = leading ledger)
        StructField("RBUKRS", StringType(), True),    # Company code
        StructField("BELNR", StringType(), True),     # Document number
        StructField("GJAHR", StringType(), True),     # Fiscal year (cast later at Silver)
        StructField("BUZEI", StringType(), True),     # Line item number within doc
        StructField("BUDAT", StringType(), True),     # Posting date
        StructField("MONAT", StringType(), True),     # Fiscal period (1–12)
        StructField("RACCT", StringType(), True),     # Account number
        StructField("RWCUR", StringType(), True),     # Transaction currency
        StructField("TSL", StringType(), True),       # Amount in transaction currency
        StructField("RHCUR", StringType(), True),     # Local currency (company's)
        StructField("HSL", StringType(), True),       # Amount in local currency
        StructField("KWAER", StringType(), True),     # Group currency (for consolidation)
        StructField("KSL", StringType(), True),       # Amount in group currency
        StructField("KOSTL", StringType(), True),     # Cost center
        StructField("PRCTR", StringType(), True),     # Profit center
        StructField("MATNR", StringType(), True),     # Material number
        StructField("KUNNR", StringType(), True),     # Customer number
        StructField("WERKS", StringType(), True),     # Plant / store
        StructField("VKORG", StringType(), True),     # Sales organization
        StructField("VTWEG", StringType(), True),     # Distribution channel
        StructField("SPART", StringType(), True),     # Division
        StructField("MENGE", StringType(), True),     # Quantity
        StructField("MEINS", StringType(), True),     # Base unit of measure
        StructField("DRCRK", StringType(), True),     # Debit/credit indicator (S/H)
        StructField("BSTAT", StringType(), True),     # Document status
        StructField("LINETYPE", StringType(), True),  # Line type classification
        StructField("RVERS", StringType(), True),     # Version (ACT / BUD / FCT)
    ]
)


# ─────────────────────────────────────────────────────────────────────────────
# BKPF — Accounting Document Header
# Grain: one row per accounting document (matches the ACDOCA BELNR+GJAHR+BUKRS)
# ─────────────────────────────────────────────────────────────────────────────
BKPF_SCHEMA = StructType(
    [
        StructField("BELNR", StringType(), True),    # Document number (PK)
        StructField("BUKRS", StringType(), True),    # Company code
        StructField("GJAHR", StringType(), True),    # Fiscal year
        StructField("BLDAT", StringType(), True),    # Document date
        StructField("BUDAT", StringType(), True),    # Posting date
        StructField("BLART", StringType(), True),    # Document type (SA, KR, DR, RV, etc.)
        StructField("WAERS", StringType(), True),    # Currency
        StructField("BKTXT", StringType(), True),    # Document header text
        StructField("XBLNR", StringType(), True),    # Reference document number
        StructField("USNAM", StringType(), True),    # Posted by (user)
        StructField("CPUDT", StringType(), True),    # Entry date (system timestamp)
        StructField("TCODE", StringType(), True),    # Transaction code used to post
        StructField("MONAT", StringType(), True),    # Fiscal period
        StructField("NUMPG", StringType(), True),    # Number of pages / line items
    ]
)


# ─────────────────────────────────────────────────────────────────────────────
# Master data tables
# ─────────────────────────────────────────────────────────────────────────────

# T001 — Company codes
T001_SCHEMA = StructType(
    [
        StructField("BUKRS", StringType(), True),  # Company code (PK)
        StructField("BUTXT", StringType(), True),  # Company name
        StructField("LAND1", StringType(), True),  # Country key
        StructField("WAERS", StringType(), True),  # Local currency
        StructField("KTOPL", StringType(), True),  # Chart of accounts
        StructField("PERIV", StringType(), True),  # Fiscal year variant
        StructField("SPRAS", StringType(), True),  # Language
        StructField("ORT01", StringType(), True),  # City
    ]
)

# T001W — Plants / stores
T001W_SCHEMA = StructType(
    [
        StructField("WERKS", StringType(), True),  # Plant (PK)
        StructField("NAME1", StringType(), True),  # Plant name
        StructField("ORT01", StringType(), True),  # City
        StructField("LAND1", StringType(), True),  # Country
        StructField("BUKRS", StringType(), True),  # Company code
        StructField("VKORG", StringType(), True),  # Sales organization
        StructField("PTYPE", StringType(), True),  # Plant type (store/outlet/warehouse)
    ]
)

# SKA1 — Chart of accounts (account master, general)
SKA1_SCHEMA = StructType(
    [
        StructField("SAKNR", StringType(), True),  # G/L account number (PK)
        StructField("TXT20", StringType(), True),  # Short text (20 chars)
        StructField("TXT50", StringType(), True),  # Long text (50 chars)
        StructField("KTOKS", StringType(), True),  # Account group
        StructField("XBILK", StringType(), True),  # Balance sheet indicator
        StructField("GVTYP", StringType(), True),  # P&L statement account type
        StructField("KTOPL", StringType(), True),  # Chart of accounts
    ]
)

# SKB1 — Account in company code (chart of accounts extension per company)
SKB1_SCHEMA = StructType(
    [
        StructField("BUKRS", StringType(), True),  # Company code (PK)
        StructField("SAKNR", StringType(), True),  # G/L account (PK)
        StructField("TXT50", StringType(), True),  # Account description
        StructField("KTOKS", StringType(), True),  # Account group
        StructField("XBILK", StringType(), True),  # Balance sheet indicator
        StructField("GVTYP", StringType(), True),  # P&L statement account type
        StructField("WAERS", StringType(), True),  # Account currency
    ]
)

# KNA1 — Customer master (general data)
KNA1_SCHEMA = StructType(
    [
        StructField("KUNNR", StringType(), True),  # Customer number (PK)
        StructField("NAME1", StringType(), True),  # Customer name
        StructField("LAND1", StringType(), True),  # Country
        StructField("ORT01", StringType(), True),  # City
        StructField("KTOKD", StringType(), True),  # Customer account group
        StructField("KUKLA", StringType(), True),  # Customer classification
    ]
)

# MARA — Material master (general data, with fashion-retail extensions)
MARA_SCHEMA = StructType(
    [
        StructField("MATNR", StringType(), True),  # Material number (PK)
        StructField("MAKTX", StringType(), True),  # Material description
        StructField("MTART", StringType(), True),  # Material type
        StructField("MATKL", StringType(), True),  # Material group
        StructField("SPART", StringType(), True),  # Division
        StructField("MEINS", StringType(), True),  # Base unit of measure
        StructField("PRDHA", StringType(), True),  # Product hierarchy
        StructField("BRAND", StringType(), True),  # Brand (fashion retail)
        StructField("SAISO", StringType(), True),  # Season (fashion retail)
        StructField("COLOR", StringType(), True),  # Color (fashion retail)
        StructField("SIZE", StringType(), True),   # Size (fashion retail)
        StructField("STYLE", StringType(), True),  # Style (fashion retail)
    ]
)

# CSKS — Cost centers
CSKS_SCHEMA = StructType(
    [
        StructField("KOSTL", StringType(), True),  # Cost center (PK)
        StructField("KOKRS", StringType(), True),  # Controlling area
        StructField("BUKRS", StringType(), True),  # Company code
        StructField("DATAB", StringType(), True),  # Valid from
        StructField("DATBI", StringType(), True),  # Valid to
        StructField("KTEXT", StringType(), True),  # Short description
        StructField("LTEXT", StringType(), True),  # Long description
        StructField("KOSAR", StringType(), True),  # Cost center category
        StructField("LAND1", StringType(), True),  # Country
    ]
)

# CEPC — Profit centers
CEPC_SCHEMA = StructType(
    [
        StructField("PRCTR", StringType(), True),  # Profit center (PK)
        StructField("KOKRS", StringType(), True),  # Controlling area
        StructField("BUKRS", StringType(), True),  # Company code
        StructField("DATAB", StringType(), True),  # Valid from
        StructField("DATBI", StringType(), True),  # Valid to
        StructField("KTEXT", StringType(), True),  # Short description
        StructField("LTEXT", StringType(), True),  # Long description
        StructField("GSBER", StringType(), True),  # Business area
        StructField("LAND1", StringType(), True),  # Country
    ]
)

# TVKO — Sales organizations
TVKO_SCHEMA = StructType(
    [
        StructField("VKORG", StringType(), True),  # Sales org (PK)
        StructField("VTEXT", StringType(), True),  # Description
        StructField("BUKRS", StringType(), True),  # Company code
        StructField("LAND1", StringType(), True),  # Country
    ]
)

# TVTW — Distribution channels
TVTW_SCHEMA = StructType(
    [
        StructField("VTWEG", StringType(), True),  # Distribution channel (PK)
        StructField("VTEXT", StringType(), True),  # Description
    ]
)

# TSPA — Divisions
TSPA_SCHEMA = StructType(
    [
        StructField("SPART", StringType(), True),  # Division (PK)
        StructField("VTEXT", StringType(), True),  # Description
    ]
)


# ─────────────────────────────────────────────────────────────────────────────
# Project-specific dimension tables (not part of standard SAP)
# ─────────────────────────────────────────────────────────────────────────────

# DIM_PNL_LINE — P&L line ordering and hierarchy
DIM_PNL_LINE_SCHEMA = StructType(
    [
        StructField("PNL_LINE_ID", StringType(), True),    # Line identifier (PK)
        StructField("PNL_LINE_NAME", StringType(), True),  # Display label
        StructField("SORT_ORDER", StringType(), True),     # Display position
        StructField("LINE_TYPE", StringType(), True),      # 'component' or 'calc' (subtotal)
    ]
)

# MAP_ACCOUNT_TO_PNL_LINE — maps G/L accounts to P&L report lines
MAP_ACCOUNT_TO_PNL_LINE_SCHEMA = StructType(
    [
        StructField("SAKNR", StringType(), True),       # G/L account (matches SKA1.SAKNR)
        StructField("PNL_LINE_ID", StringType(), True), # Target P&L line
        StructField("SIGN", StringType(), True),        # +1 or -1 (uniform -1 per ADR-002)
    ]
)


# ─────────────────────────────────────────────────────────────────────────────
# Registry — maps source name (= CSV filename stem, uppercase) to schema.
# The bronze loader calls SCHEMA_REGISTRY.get(source_name.upper()) to look
# up the contract. Missing entries fall back to PERMISSIVE string-type read.
# ─────────────────────────────────────────────────────────────────────────────
SCHEMA_REGISTRY: dict[str, StructType] = {
    "ACDOCA": ACDOCA_SCHEMA,
    "BKPF": BKPF_SCHEMA,
    "T001": T001_SCHEMA,
    "T001W": T001W_SCHEMA,
    "SKA1": SKA1_SCHEMA,
    "SKB1": SKB1_SCHEMA,
    "KNA1": KNA1_SCHEMA,
    "MARA": MARA_SCHEMA,
    "CSKS": CSKS_SCHEMA,
    "CEPC": CEPC_SCHEMA,
    "TVKO": TVKO_SCHEMA,
    "TVTW": TVTW_SCHEMA,
    "TSPA": TSPA_SCHEMA,
    "DIM_PNL_LINE": DIM_PNL_LINE_SCHEMA,
    "MAP_ACCOUNT_TO_PNL_LINE": MAP_ACCOUNT_TO_PNL_LINE_SCHEMA,
}