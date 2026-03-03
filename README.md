# NASA CMAPSS Predictive Maintenance Data Platform

An end-to-end **predictive maintenance data platform** built on NASA's CMAPSS turbofan engine dataset. Ingests raw sensor telemetry into a PostgreSQL warehouse, transforms features with dbt, validates data quality with Great Expectations, trains and tracks RUL prediction models on a scheduled Apache Airflow DAG, and surfaces KPIs in a Metabase dashboard.

> ML is the last 15%. Pipelines and schema design are the headline.

---

## Architecture

```
Raw CMAPSS txt files (FD001 — 33,727 rows, 100 engines)
        │
        ▼
[Airflow DAG: rul_pipeline]  ─── daily schedule ───────────────────────────
        │
        ├─► extract_data             unzip + parse txt → staging CSV
        ├─► load_raw_to_postgres     upsert → raw_sensor_readings
        ├─► run_great_expectations   row counts, nulls, sensor bounds
        ├─► dbt_run_transforms       rolling features → features_engine_cycle
        │                            RUL labels      → labels_rul
        ├─► train_rul_model          baseline heuristic + XGBoost → artifacts
        ├─► score_latest_predictions inference → model_predictions
        └─► report_pipeline_metrics  runtime + MAE → pipeline_runs
                │
                ▼
        [PostgreSQL 15 Warehouse — warehouse_db]
                │
                ▼
        [Metabase Dashboard / Jupyter KPI notebook]
```

---

## Tech Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow 2.8 |
| Warehouse | PostgreSQL 15 |
| SQL transforms | dbt-core 1.7 + dbt-postgres |
| Data quality | Great Expectations 0.18 |
| ML | scikit-learn + XGBoost 2.0 |
| Dashboard | Metabase (Docker) · Jupyter fallback |
| Infra | Docker Compose |

---

## Data Model

Five tables in `warehouse_db`:

| Table | Grain | Rows (FD001) | Description |
|---|---|---|---|
| `raw_sensor_readings` | (engine_id, cycle, split) | 33,727 | Raw telemetry, upserted each run |
| `features_engine_cycle` | (engine_id, cycle, split) | 33,727 | Rolling features (dbt mart) |
| `labels_rul` | (engine_id, cycle, split) | 33,727 | Piecewise RUL, capped at 125 |
| `model_predictions` | (engine_id, cycle, split, version) | variable | Scored predictions per model version |
| `pipeline_runs` | run_id | 1 per DAG run | Observability log: rows, runtime, MAE, status |

All time-series tables are indexed on `(engine_id, cycle)` for fast window queries.

### Feature Engineering

14 informative sensors (constant-variance sensors excluded): `s2 s3 s4 s7 s8 s9 s11 s12 s13 s14 s15 s17 s20 s21`

Per sensor, 5 feature types are computed via PostgreSQL window functions in dbt:

| Feature | Description |
|---|---|
| Raw value | Sensor reading at cycle t |
| `rmean_sN_5` | Rolling mean, window = 5 cycles |
| `rstd_sN_5` | Rolling std, window = 5 cycles |
| `rmean_sN_20` | Rolling mean, window = 20 cycles |
| `rstd_sN_20` | Rolling std, window = 20 cycles |
| `delta_sN` | sN(t) − sN(t−1) |

Total: **84 feature columns** per row.

---

## Airflow DAG

```
extract_data → load_raw_to_postgres → run_great_expectations
    → dbt_run_transforms → train_rul_model
        → score_latest_predictions → report_pipeline_metrics
```

- Schedule: `None` (manual) in dev, switch to `@daily` for production
- All tasks: `retries=2, retry_delay=5min`
- Failure callback: logs failed run to `pipeline_runs`
- Metrics passed between tasks via XCom

---

## Model Results

*(Fill in after first pipeline run)*

| Metric | Baseline (mean-by-cycle) | XGBoost v1 | Δ |
|---|---|---|---|
| MAE (cycles) | TBD | TBD | TBD |
| RMSE (cycles) | TBD | TBD | TBD |
| Inference / 1k rows | — | TBD ms | — |

---

## Pipeline Benchmarks

*(Fill in after first pipeline run)*

| Metric | Value |
|---|---|
| Total rows ingested | 33,727 |
| dbt transform runtime | TBD s |
| Query latency (unindexed) | TBD ms |
| Query latency (indexed on engine_id, cycle) | TBD ms |
| GE validation pass rate | TBD / 17 expectations |
| dbt test pass rate | TBD / 8 tests |

---

## Project Structure

```
nasa-cmapss-rul-prediction/
├── docker/
│   ├── docker-compose.yml      # Airflow + Postgres + Metabase
│   └── init-db.sql             # Create airflow/warehouse/metabase databases
├── data/
│   ├── raw/                    # Extracted txt files
│   └── processed/              # Staging CSVs + KPI chart PNGs
├── dags/
│   └── rul_pipeline.py         # 7-task Airflow DAG
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/            # stg_sensor_readings (view)
│       ├── intermediate/       # int_rolling_features (ephemeral)
│       └── marts/              # features_engine_cycle, labels_rul (tables)
├── expectations/
│   └── raw_sensor_readings_suite.json
├── models/
│   ├── train_rul.py            # Train baseline + XGBoost
│   └── artifacts/              # Saved .pkl files (git-ignored)
├── sql/
│   └── schema.sql              # Full DDL for all 5 tables
├── scripts/
│   ├── extract_data.py         # Unzip → staging CSV
│   ├── ingest_raw.py           # CSV → PostgreSQL (upsert)
│   ├── validate_data.py        # Great Expectations runner
│   └── score_latest.py         # Model → model_predictions
├── notebooks/
│   └── dashboard.ipynb         # KPI charts (5 panels)
├── tests/
│   └── test_pipeline.py        # Pytest unit tests
├── requirements.txt
├── plan.md
└── README.md
```

---

## Quickstart

### Prerequisites

- Docker + Docker Compose
- Python 3.11+

### 1. Start infrastructure

```bash
cd docker
docker-compose up -d
# Wait ~60s for Airflow init to complete
# Airflow UI:  http://localhost:8080  (admin / admin)
# Metabase:   http://localhost:3000
# Postgres:   localhost:5432 (warehouse user/pass: warehouse/warehouse)
```

### 2. Install Python dependencies (for local dev)

```bash
pip install -r requirements.txt
```

### 3. Trigger the full pipeline

**Via Airflow UI:** Navigate to `http://localhost:8080`, find `rul_pipeline`, click ▶ Trigger DAG.

**Or run steps manually:**

```bash
# Extract data from zip
python scripts/extract_data.py

# Ingest into PostgreSQL
python scripts/ingest_raw.py

# Validate data quality
python scripts/validate_data.py --fail-on-error

# Run dbt transforms
cd dbt && dbt build --profiles-dir . && cd ..

# Train models
python models/train_rul.py

# Score test set
python scripts/score_latest.py

# Launch KPI notebook
jupyter notebook notebooks/dashboard.ipynb
```

### 4. Run unit tests

```bash
pytest tests/test_pipeline.py -v
```

---

## dbt Models

```
raw_sensor_readings  (PostgreSQL source)
        │
        ▼
stg_sensor_readings          [view]    — 14 informative sensors
        │
        ▼
int_rolling_features         [ephemeral CTE] — window stats + deltas
        │
        ├──► features_engine_cycle  [table] — 84-column feature mart
        └──► labels_rul             [table] — piecewise RUL + failure flag
```

**Tests:** `not_null`, `unique` on grain, `accepted_values` on dataset_split, custom `assert_rul_nonnegative`.

---

## Configuration

| Variable | Default | Description |
|---|---|---|
| `WAREHOUSE_CONN` | `postgresql+psycopg2://warehouse:warehouse@localhost:5432/warehouse_db` | SQLAlchemy connection string |
| `WAREHOUSE_HOST` | `localhost` | Used by dbt profiles.yml |

Set in Docker Compose environment or export before running scripts locally.

---

## Dataset: NASA CMAPSS FD001

- **Source:** [NASA Prognostics Data Repository](https://ti.arc.nasa.gov/tech/dash/groups/pcoe/prognostic-data-repository/)
- **Subset:** FD001 — single fault mode (HPC degradation), single operating condition
- **Train:** 100 engines, 20,631 rows
- **Test:** 100 engines, 13,096 rows
- **RUL target:** Piecewise linear, capped at 125 cycles (Heimes 2008)
- **Other work:** [Research Paper on same topic, different outcomes/graphs](https://www.mdpi.com/2076-3417/15/18/9945)

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
