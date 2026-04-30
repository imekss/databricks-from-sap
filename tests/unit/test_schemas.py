"""Unit tests for n3n3.config.schemas — the SAP schema registry."""

from pyspark.sql.types import StringType, StructType

from n3n3.config.schemas import (
    ACDOCA_SCHEMA,
    BKPF_SCHEMA,
    SCHEMA_REGISTRY,
)


class TestSchemaRegistry:
    """Tests for SCHEMA_REGISTRY as a whole."""

    def test_registry_has_all_expected_sap_tables(self):
        """The registry should cover all 15 SAP sources N3N3 ingests."""
        expected = {
            "ACDOCA",
            "BKPF",
            "T001",
            "T001W",
            "SKA1",
            "SKB1",
            "KNA1",
            "MARA",
            "CSKS",
            "CEPC",
            "TVKO",
            "TVTW",
            "TSPA",
            "DIM_PNL_LINE",
            "MAP_ACCOUNT_TO_PNL_LINE",
        }
        assert set(SCHEMA_REGISTRY.keys()) == expected

    def test_registry_is_not_empty(self):
        assert len(SCHEMA_REGISTRY) == 15

    def test_all_values_are_struct_types(self):
        """Every registered schema must be a proper StructType."""
        for name, schema in SCHEMA_REGISTRY.items():
            assert isinstance(schema, StructType), f"Schema for {name} is not a StructType"


class TestBronzeRawPreservation:
    """Bronze-layer contract: every column is StringType (raw preservation)."""

    def test_all_bronze_fields_are_string_type(self):
        """Bronze preserves raw data as strings; type casts happen at Silver."""
        for name, schema in SCHEMA_REGISTRY.items():
            for field in schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{name}.{field.name} is {field.dataType}, expected StringType"
                )


class TestACDOCASchema:
    """Specific tests for the ACDOCA fact table schema."""

    def test_acdoca_column_count(self):
        """ACDOCA has 28 columns as shipped in the synthetic dataset."""
        assert len(ACDOCA_SCHEMA.fields) == 28

    def test_acdoca_has_critical_columns(self):
        """Columns required for P&L aggregation must be present."""
        col_names = [f.name for f in ACDOCA_SCHEMA.fields]
        for required in ["RBUKRS", "BELNR", "GJAHR", "RACCT", "KSL", "RVERS"]:
            assert required in col_names, f"ACDOCA missing column: {required}"


class TestBKPFSchema:
    """Specific tests for the BKPF document header schema."""

    def test_bkpf_column_count(self):
        """BKPF has 14 columns as shipped."""
        assert len(BKPF_SCHEMA.fields) == 14

    def test_bkpf_has_document_key(self):
        """The primary key (BELNR + BUKRS + GJAHR) must be present."""
        col_names = [f.name for f in BKPF_SCHEMA.fields]
        for required in ["BELNR", "BUKRS", "GJAHR"]:
            assert required in col_names, f"BKPF missing column: {required}"
