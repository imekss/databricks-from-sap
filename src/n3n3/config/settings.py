"""
N3N3 project configuration.

Centralises catalog, schema, paths, and environment-specific settings.
Notebooks and modules import from here — no hardcoded catalog/schema strings
scattered across the codebase.

Usage:
    from n3n3.config.settings import config

    # Get a fully-qualified table name
    bronze_table = config.catalog.bronze("acdoca")
    # Dev:  "n3n3.dgn_ousmane_bronze.acdoca"
    # Prod: "n3n3.bronze.acdoca"

The config is environment-aware through two mechanisms:
  1. Databricks jobs pass catalog/schema as notebook parameters (widgets),
     which call `config.override_from_widgets(...)`.
  2. Local dev/tests load from conf/dev.yml via `N3N3Config.from_yaml(...)`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass(frozen=True)
class CatalogConfig:
    """
    Unity Catalog namespace configuration.

    One catalog (n3n3) holds ALL schemas. Dev and prod differ only in
    schema names:
        prod → bronze / silver / gold
        dev  → <user>_bronze / <user>_silver / <user>_gold
    """

    catalog: str = "n3n3"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"

    def bronze(self, table: str) -> str:
        """Fully-qualified bronze table name: n3n3.<bronze_schema>.<table>"""
        return f"{self.catalog}.{self.bronze_schema}.{table}"

    def silver(self, table: str) -> str:
        """Fully-qualified silver table name."""
        return f"{self.catalog}.{self.silver_schema}.{table}"

    def gold(self, table: str) -> str:
        """Fully-qualified gold table name."""
        return f"{self.catalog}.{self.gold_schema}.{table}"


@dataclass(frozen=True)
class PathConfig:
    """
    Storage paths for raw data and pipeline state.

    `raw_root` is a UC Volume — the landing zone for SAP CSV extracts.
    Uses `raw_root_for(catalog, bronze_schema)` to resolve per-environment
    paths at runtime (dev users land CSVs in their own <user>_bronze volume;
    prod lands in the shared bronze volume).
    """

    checkpoint_root: str = "/Volumes/n3n3/bronze/_checkpoints"

    @staticmethod
    def raw_root_for(catalog: str, bronze_schema: str) -> str:
        """Build the volume path for a given catalog + bronze schema."""
        return f"/Volumes/{catalog}/{bronze_schema}/raw_data"


@dataclass(frozen=True)
class IngestConfig:
    """
    Bronze ingestion behaviour.

    `mode` controls write semantics:
      - 'overwrite' for full-refresh (dev, initial loads)
      - 'append' for incremental (scheduled prod runs)
    """

    mode: str = "overwrite"
    infer_schema: bool = False
    rescued_data_column: str = "_rescued_data"
    add_metadata: bool = True


@dataclass(frozen=True)
class N3N3Config:
    """Top-level project configuration — composition of the sub-configs above."""

    environment: str = "dev"
    catalog: CatalogConfig = field(default_factory=CatalogConfig)
    paths: PathConfig = field(default_factory=PathConfig)
    ingest: IngestConfig = field(default_factory=IngestConfig)

    @property
    def raw_root(self) -> str:
        """Resolve raw_root for the current catalog + bronze_schema."""
        return PathConfig.raw_root_for(
            catalog=self.catalog.catalog,
            bronze_schema=self.catalog.bronze_schema,
        )

    @classmethod
    def from_yaml(cls, path: str | Path) -> "N3N3Config":
        """
        Load config from a YAML file, merging with defaults.

        Used for local development and tests. In a Databricks job, prefer
        `override_from_widgets(...)` since job parameters come from
        databricks.yml, not a local file.
        """
        with open(path) as f:
            raw = yaml.safe_load(f) or {}

        return cls(
            environment=raw.get("environment", "dev"),
            catalog=CatalogConfig(**raw.get("catalog", {})),
            paths=PathConfig(**raw.get("paths", {})),
            ingest=IngestConfig(**raw.get("ingest", {})),
        )

    def override_from_widgets(
        self,
        catalog: str | None = None,
        bronze_schema: str | None = None,
        silver_schema: str | None = None,
        gold_schema: str | None = None,
    ) -> "N3N3Config":
        """
        Return a new N3N3Config with catalog/schemas overridden by widget values.

        Call from the top of a notebook, after reading `dbutils.widgets`:

            from n3n3.config.settings import config
            config = config.override_from_widgets(
                catalog=dbutils.widgets.get("catalog"),
                bronze_schema=dbutils.widgets.get("bronze_schema"),
                silver_schema=dbutils.widgets.get("silver_schema"),
                gold_schema=dbutils.widgets.get("gold_schema"),
            )

        This is how job parameters from databricks.yml flow into the config.
        """
        new_catalog = CatalogConfig(
            catalog=catalog or self.catalog.catalog,
            bronze_schema=bronze_schema or self.catalog.bronze_schema,
            silver_schema=silver_schema or self.catalog.silver_schema,
            gold_schema=gold_schema or self.catalog.gold_schema,
        )
        return N3N3Config(
            environment=self.environment,
            catalog=new_catalog,
            paths=self.paths,
            ingest=self.ingest,
        )


# Default config instance — importable from anywhere as:
#   from n3n3.config.settings import config
config = N3N3Config()
