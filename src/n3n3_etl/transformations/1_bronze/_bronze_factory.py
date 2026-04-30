"""
Bronze layer — one DLT table per SAP source CSV.

Instead of writing 15 nearly-identical @dp.table functions (one per source),
this factory iterates the SCHEMA_REGISTRY and generates them. Each bronze
table:

  - Reads from a CSV at /Volumes/<catalog>/<bronze_schema>/raw_data/<SOURCE>.csv
    where catalog + bronze_schema come from the pipeline's configuration
    block (set in resources/n3n3_etl_bronze.pipeline.yml). This makes dev
    read from <user>_bronze volume and prod read from the shared bronze volume.
  - Enforces the StructType contract from n3n3.config.schemas (FAILFAST mode)
  - Adds ingestion metadata columns (_ingestion_ts, _source_file)
  - Is materialized as <catalog>.<bronze_schema>.<source>

UC compatibility note:
  `F.input_file_name()` is not supported in Unity Catalog. We use the
  `_metadata.file_path` column instead, which every file-based Spark reader
  exposes automatically (Spark 3.4+).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from n3n3.config.schemas import SCHEMA_REGISTRY
from n3n3.config.settings import PathConfig


def _register_bronze_table(source_name: str, schema: StructType) -> None:
    """
    Register a single DLT bronze table for a source CSV.

    We set `__name__` on the inner function BEFORE applying the decorator,
    because `@dp.table` wraps the function in a DLT-specific object whose
    `__name__` is read-only. `name=` is also passed explicitly to the
    decorator so DLT uses the exact table name we want.
    """
    table_name = f"{source_name.lower()}"

    def _table():
        # Resolve raw path at execution time using the pipeline's
        # configuration values (catalog + bronze_schema). These come from
        # the pipeline YAML's `configuration:` block, so dev and prod
        # each point at their correct volume path.
        catalog = spark.conf.get("catalog")  # noqa: F821
        bronze_schema = spark.conf.get("bronze_schema")  # noqa: F821
        raw_path = f"{PathConfig.raw_root_for(catalog, bronze_schema)}/{source_name}.csv"

        return (
            spark.read.format("csv")  # noqa: F821 — `spark` injected by DLT runtime
            .schema(schema)
            .option("header", True)
            .option("mode", "FAILFAST")
            .load(raw_path)
            .withColumn("_ingestion_ts", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
        )

    # Set __name__ on the raw function BEFORE decoration — once decorated,
    # the return value is a DLT object that doesn't allow attribute writes.
    _table.__name__ = table_name

    decorated = dp.table(
        name=table_name,
        comment=f"Bronze layer: raw ingest of SAP {source_name}.",
    )(_table)

    # Register in module globals so DLT's module scan can find it.
    globals()[table_name] = decorated


# Register every bronze table at module import time.
for _source, _schema in SCHEMA_REGISTRY.items():
    _register_bronze_table(_source, _schema)
