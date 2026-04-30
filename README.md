# N3N3 — SAP → Databricks Fashion Retail Demo

A portfolio project showing how SAP S/4HANA financial data flows into a Databricks medallion architecture (Bronze → Silver → Gold), built for a fictional omnichannel luxury retailer called **N3N3**.

Deployed as a [Databricks Asset Bundle](https://docs.databricks.com/dev-tools/bundles/index.html) with three independent Lakeflow Declarative Pipelines (bronze / silver / gold) chained by an orchestration job. Dev and prod share one Unity Catalog; only the schemas differ.

---

## Architecture

```
┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐
│   Bronze         │   │   Silver         │   │   Gold           │
│   (raw ingest)   │──▶│  (business       │──▶│  (reporting      │
│                  │   │   vocabulary)    │   │   marts)         │
│  15 SAP tables   │   │  11 dims + 1     │   │  P&L by channel  │
│  as Delta        │   │  fact table      │   │  / period /      │
│                  │   │                  │   │  company         │
│  FAILFAST schema │   │  DLT @expect     │   │                  │
│  contracts       │   │  data quality    │   │                  │
└──────────────────┘   └──────────────────┘   └──────────────────┘
   n3n3_etl_bronze       n3n3_etl_silver        n3n3_etl_gold
   (DLT pipeline)        (DLT pipeline)         (DLT pipeline)
          │                     │                     │
          └─────────────────────┴─────────────────────┘
                                │
                      orchestration.job.yml
                       (bronze → silver → gold)
```

Each layer is its own pipeline so refresh cadence, failure boundaries, and compute sizing can differ per layer.

---

## Repository layout

```
n3n3/
├── databricks.yml                        # Bundle manifest (dev/prod targets)
├── pyproject.toml                        # Python deps (uv-managed)
├── uv.lock
├── data/raw/                             # 15 SAP-shaped CSVs
├── dashboards/                           # AI/BI dashboard (.lvdash.json)
├── resources/                            # Bundle resource definitions
│   ├── n3n3_etl_bronze.pipeline.yml
│   ├── n3n3_etl_silver.pipeline.yml
│   ├── n3n3_etl_gold.pipeline.yml
│   └── orchestration.job.yml
├── scripts/
│   └── setup.sh                          # One-time project setup
├── src/
│   ├── n3n3/config/                      # Settings, schema contracts
│   └── n3n3_etl/transformations/
│       ├── 1_bronze/                     # Python — @dp.table factory
│       ├── 2_silver/                     # SQL — materialized views
│       └── 3_gold/                       # SQL — materialized views
├── tests/unit/                           # pytest unit tests
└── .github/workflows/ci.yml              # CI: lint + tests
```

---

## Environments

| Target | Catalog | Schemas |
|---|---|---|
| `dev` | `n3n3` | `<user>_bronze`, `<user>_silver`, `<user>_gold` |
| `prod` | `n3n3` | `bronze`, `silver`, `gold` |

Same code, different target flag. At enterprise scale you'd split into separate catalogs or workspaces — Databricks Free Edition restricts catalog creation, so this project uses schema-level isolation. Promotion semantics are identical.

---

## Tech stack

- **Lakeflow Declarative Pipelines (DLT)** — declarative materialization, data quality expectations, incremental updates
- **Databricks Asset Bundles** — infrastructure-as-code for pipelines and jobs
- **Unity Catalog** — governance, lineage, volume storage
- **uv** — Python deps with lockfile
- **pytest + ruff** — tests and lint
- **GitHub Actions** — CI on every push

---

## Getting started

Developed on macOS. The Databricks CLI is available for Linux and Windows too — see the [install docs](https://docs.databricks.com/dev-tools/cli/install.html) for other platforms.

> **No Databricks account?** [Databricks Free Edition](https://www.databricks.com/learn/free-edition) — zero cost, full Unity Catalog access.

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/getting-started/installation/)
- Databricks CLI: `brew install databricks`
- A Databricks workspace where you have permissions

### Quick start

```bash
# 1. Authenticate to your workspace
databricks auth login --host https://YOUR-WORKSPACE.cloud.databricks.com

# 2. Run setup (defaults to dev)
./scripts/setup.sh
```

The script installs deps, deploys the bundle, creates the schema + volume, uploads CSVs, and runs the pipeline. First run ~5 minutes.

After completion the P&L mart is at:

```
n3n3.<your_username>_gold.profit_and_loss
```

### Running again

`setup.sh` is idempotent — safe to re-run. Schema and volume creation skip if they exist; CSVs are re-uploaded with `--overwrite`; pipeline runs each time.

To just re-run the pipeline:

```bash
databricks bundle run n3n3_full_refresh -t dev
```

Or each layer individually:

```bash
databricks bundle run n3n3_etl_bronze -t dev
databricks bundle run n3n3_etl_silver -t dev
databricks bundle run n3n3_etl_gold   -t dev
```

### Local development

```bash
uv run pytest tests/unit/ -v    # unit tests
uv run ruff check .             # lint
```

### Prod

```bash
./scripts/setup.sh prod
```

Dev and prod currently deploy to the same workspace (different schemas). A real production setup would add:

- Separate workspaces or catalogs for hard isolation
- Service principal deploys, not human users
- CI/CD deploying on merge to `main`
- Secrets in Databricks Secrets / Vault / equivalent

This repo demonstrates the pipeline structure, not the full enterprise deployment model.

---

## Dashboard

A pre-built P&L overview AI/BI dashboard ships at `dashboards/P&L Overview.lvdash.json`. It includes KPI counters (Gross Sales, Net Sales, Gross Profit, EBITDA, margin %), a P&L waterfall, ACT vs BUD variance, monthly trend, channel pie, and global filters (year, period, version, company, division, channel, sales org).

To import:

1. In Databricks: **Dashboards → Create dashboard → File icon → Import dashboard from file**
2. Select `dashboards/P&L Overview.lvdash.json`

The dashboard datasets read from `n3n3.gold.profit_and_loss` (prod schema). If you ran `setup.sh dev`, edit each dataset's SQL `FROM` clause to use your dev gold schema, e.g. `n3n3.<your_username>_gold.profit_and_loss`.

---

## Data

15 SAP-shaped CSVs committed to `data/raw/`:

| Table | Purpose |
|---|---|
| `CEPC.csv` | Profit centers |
| `CSKS.csv` | Cost centers |
| `DIM_PNL_LINE.csv` | P&L line definitions |
| `KNA1.csv` | Customer master |
| `MAP_ACCOUNT_TO_PNL_LINE.csv` | Account → P&L line mapping |
| `MARA.csv` | Material master |
| `SKA1.csv` / `SKB1.csv` | Chart of accounts |
| `T001.csv` / `T001W.csv` | Company codes / plants |
| `TSPA.csv` / `TVKO.csv` / `TVTW.csv` | Divisions / sales orgs / channels |
| `ACDOCA.csv` | Universal journal (GL postings) |
| `BKPF.csv` | Document headers |

`setup.sh` uploads these to `/Volumes/n3n3/<your_username>_bronze/raw_data/`.

---

## Design decisions

- **Three pipelines, not one** — per-layer refresh cadence, failure boundaries, and compute.
- **Python for Bronze, SQL for Silver/Gold** — 15 near-identical bronze sources = a factory loop in Python. Silver/Gold are renames + joins + aggregations, which read clearer as SQL.
- **Bronze columns all `StringType`** — raw preservation. Type casts happen at Silver. Prevents leading-zero loss on SAP codes.
- **P&L sign convention** — every P&L account has `SIGN = -1` in `MAP_ACCOUNT_TO_PNL_LINE`. Multiplying by sign flips SAP's debit/credit storage into the intuitive view (revenue positive, costs negative).

---

## Testing

- **Python unit tests** (pytest) cover `config/settings.py` and `config/schemas.py` — design invariants like "every bronze field is StringType".
- **DLT expectations** (`CONSTRAINT ... EXPECT`) provide runtime data quality checks on silver and gold.