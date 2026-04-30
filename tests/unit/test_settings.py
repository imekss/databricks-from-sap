"""Unit tests for n3n3.config.settings — the project config module."""

import pytest
from n3n3.config.settings import (
    CatalogConfig,
    IngestConfig,
    N3N3Config,
    PathConfig,
    config,
)


class TestCatalogConfig:
    """Tests for the CatalogConfig frozen dataclass."""

    def test_default_catalog_is_n3n3(self):
        cfg = CatalogConfig()
        assert cfg.catalog == "n3n3"
        assert cfg.bronze_schema == "bronze"
        assert cfg.silver_schema == "silver"
        assert cfg.gold_schema == "gold"

    def test_bronze_produces_fully_qualified_name(self):
        cfg = CatalogConfig(catalog="n3n3", bronze_schema="bronze")
        assert cfg.bronze("bronze_acdoca") == "n3n3.bronze.bronze_acdoca"

    def test_silver_produces_fully_qualified_name(self):
        cfg = CatalogConfig(catalog="n3n3", silver_schema="dgn_ousmane_silver")
        assert cfg.silver("silver_company") == "n3n3.dgn_ousmane_silver.silver_company"

    def test_gold_produces_fully_qualified_name(self):
        cfg = CatalogConfig(catalog="n3n3", gold_schema="gold")
        assert cfg.gold("gold_profit_and_loss") == "n3n3.gold.gold_profit_and_loss"

    def test_catalog_config_is_immutable(self):
        """Frozen dataclass must refuse attribute mutation."""
        cfg = CatalogConfig()
        with pytest.raises(Exception):  # FrozenInstanceError in practice
            cfg.catalog = "evil"


class TestN3N3Config:
    """Tests for the top-level N3N3Config object."""

    def test_default_singleton_has_expected_values(self):
        """The module-level `config` singleton should have sensible defaults."""
        assert config.catalog.catalog == "n3n3"
        assert config.raw_root == "/Volumes/n3n3/bronze/raw_data"
        assert config.ingest.mode == "overwrite"

    def test_override_from_widgets_returns_new_object(self):
        """override_from_widgets should be pure — original config unchanged."""
        original = N3N3Config()
        updated = original.override_from_widgets(
            bronze_schema="dgn_ousmane_bronze",
        )

        # Original untouched
        assert original.catalog.bronze_schema == "bronze"
        # New object reflects the override
        assert updated.catalog.bronze_schema == "dgn_ousmane_bronze"
        # Other fields preserved
        assert updated.catalog.catalog == "n3n3"
        assert updated.catalog.silver_schema == "silver"

    def test_override_with_all_schemas(self):
        """Overriding all three schemas at once should work."""
        original = N3N3Config()
        updated = original.override_from_widgets(
            bronze_schema="b",
            silver_schema="s",
            gold_schema="g",
        )
        assert updated.catalog.bronze_schema == "b"
        assert updated.catalog.silver_schema == "s"
        assert updated.catalog.gold_schema == "g"


class TestPathConfig:
    """Tests for PathConfig."""

    def test_raw_root_points_to_uc_volume(self):
        result = PathConfig.raw_root_for("n3n3", "bronze")
        assert result.startswith("/Volumes/")
        assert result == "/Volumes/n3n3/bronze/raw_data"


class TestIngestConfig:
    """Tests for IngestConfig."""

    def test_default_mode_is_overwrite(self):
        cfg = IngestConfig()
        assert cfg.mode == "overwrite"

    def test_default_does_not_infer_schema(self):
        """Bronze must enforce contracts — inferring would defeat FAILFAST."""
        cfg = IngestConfig()
        assert cfg.infer_schema is False
