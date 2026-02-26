# NASA CMAPSS Predictive Maintenance Data Platform — Project Plan

## Problem Statement

Unplanned turbofan engine failures cost airlines significant downtime and maintenance overhead. Using NASA's CMAPSS (Commercial Modular Aero-Propulsion System Simulation) dataset, we build a production-style **predictive maintenance data platform** that:

1. Ingests raw engine sensor telemetry into a PostgreSQL warehouse
2. Transforms data into feature tables via dbt
3. Validates data quality with Great Expectations
4. Trains and scores a Remaining Useful Life (RUL) model on a scheduled Airflow DAG
5. Surfaces KPIs in a Metabase dashboard

ML is the last 15%. Pipelines and schema design are the headline.

---

## Architecture

```
Raw CMAPSS txt files (FD001)
        │
        ▼
[Airflow DAG: rul_pipeline] (daily schedule)
        │
        ├─► extract          → unzip / parse txt files → staging CSV
        ├─► load_raw         → upsert raw_sensor_readings (PostgreSQL)
        ├─► validate         → Great Expectations suite (row counts, nulls, sensor bounds)
        ├─► dbt_run          → features_engine_cycle, labels_rul (rolling windows, RUL calc)
        ├─► train_model      → baseline (heuristic) + improved (XGBoost) → save artifacts
        ├─► score_latest     → write predictions → model_predictions table
        └─► report_metrics   → update pipeline_runs table (rows, runtime, status)
                │
                ▼
        [PostgreSQL Warehouse]
                │
                ▼
        [Metabase Dashboard] — KPIs: MAE, pipeline health, sensor trends
```

---

## Tech Stack

| Layer | Tool | Notes |
|---|---|---|
| Orchestration | Apache Airflow 2.x | Docker-compose setup |
| Warehouse | PostgreSQL 15 | Docker container |
| SQL Transforms | dbt-core + dbt-postgres | Models + tests |
| Data Quality | Great Expectations | Row/column validation suite |
| ML | scikit-learn + XGBoost | Baseline + improved RUL model |
| Dashboard | Metabase (Docker) or Jupyter notebook | KPI display |
| Python | pandas, numpy, sqlalchemy | ETL scripts |
| Infra | Docker Compose | Local development |

---

## Dataset: FD001

From `CMAPSSData.zip`:
- `train_FD001.txt` — 20,631 rows, 26 columns (engine_id, cycle, 3 op settings, 21 sensors)
- `test_FD001.txt`  — 13,096 rows, same schema
- `RUL_FD001.txt`   — 100 true RUL values (one per test engine at last observed cycle)

**Single fault mode (HPC degradation), single operating condition.**

Column layout (space-delimited, no header):
```
engine_id  cycle  op1 op2 op3  s1 s2 ... s21
```

---

## Database Schema (PostgreSQL)

### 1. `raw_sensor_readings`
Grain: `(engine_id, cycle, dataset_split)`
```sql
CREATE TABLE raw_sensor_readings (
    engine_id       INT,
    cycle           INT,
    dataset_split   VARCHAR(10),   -- 'train' or 'test'
    op1 FLOAT, op2 FLOAT, op3 FLOAT,
    s1  FLOAT, s2  FLOAT, s3  FLOAT, s4  FLOAT, s5  FLOAT,
    s6  FLOAT, s7  FLOAT, s8  FLOAT, s9  FLOAT, s10 FLOAT,
    s11 FLOAT, s12 FLOAT, s13 FLOAT, s14 FLOAT, s15 FLOAT,
    s16 FLOAT, s17 FLOAT, s18 FLOAT, s19 FLOAT, s20 FLOAT,
    s21 FLOAT,
    ingested_at     TIMESTAMPTZ DEFAULT NOW(),
    source_file     VARCHAR(50),
    PRIMARY KEY (engine_id, cycle, dataset_split)
);
CREATE INDEX ON raw_sensor_readings (engine_id, cycle);
```

### 2. `features_engine_cycle` (dbt mart)
Grain: `(engine_id, cycle, dataset_split)`

Rolling window features (window = 5 and 20 cycles) on 14 informative sensors
(s2, s3, s4, s7, s8, s9, s11, s12, s13, s14, s15, s17, s20, s21):
```
rolling_mean_s{N}_5, rolling_std_s{N}_5
rolling_mean_s{N}_20, rolling_std_s{N}_20
delta_s{N}    -- s{N} - lag(s{N}, 1)
```

### 3. `labels_rul` (dbt mart)
Grain: `(engine_id, cycle, dataset_split)`
```sql
-- true_rul     = max_cycle_per_engine - current_cycle  (piecewise-capped at 125)
-- failure_flag = 1 if true_rul = 0
```

### 4. `model_predictions`
Grain: `(engine_id, cycle, dataset_split, model_version)`
```sql
CREATE TABLE model_predictions (
    engine_id       INT,
    cycle           INT,
    dataset_split   VARCHAR(10),
    model_version   VARCHAR(50),
    pred_rul        FLOAT,
    prediction_ts   TIMESTAMPTZ DEFAULT NOW()
);
```

### 5. `pipeline_runs`
Grain: `(run_id)` — observability log
```sql
CREATE TABLE pipeline_runs (
    run_id                  SERIAL PRIMARY KEY,
    dag_run_id              VARCHAR(100),
    start_ts                TIMESTAMPTZ,
    end_ts                  TIMESTAMPTZ,
    rows_ingested           INT,
    rows_failed_validation  INT,
    transform_runtime_s     FLOAT,
    model_mae               FLOAT,
    status                  VARCHAR(20),  -- 'success' | 'failed' | 'partial'
    created_at              TIMESTAMPTZ DEFAULT NOW()
);
```

---

## Repository Structure

```
nasa-cmapss-rul-prediction/
├── docker/
│   └── docker-compose.yml          # Airflow + Postgres + Metabase containers
├── data/
│   ├── raw/                        # Extracted CMAPSSData txt files
│   └── processed/                  # Intermediate staging CSVs
├── dags/
│   └── rul_pipeline.py             # Main Airflow DAG (daily schedule)
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml                # Postgres connection
│   ├── models/
│   │   ├── staging/
│   │   │   └── stg_sensor_readings.sql
│   │   ├── intermediate/
│   │   │   └── int_rolling_features.sql
│   │   └── marts/
│   │       ├── features_engine_cycle.sql
│   │       └── labels_rul.sql
│   └── tests/
│       ├── assert_no_nulls.sql
│       └── assert_rul_nonnegative.sql
├── expectations/
│   └── raw_sensor_readings_suite.json
├── models/
│   ├── train_rul.py                # Train baseline + XGBoost, write artifacts
│   └── artifacts/                  # Saved model pkl files (gitignored)
├── sql/
│   └── schema.sql                  # Full DDL for all 5 tables + indexes
├── scripts/
│   ├── extract_data.py             # Unzip + parse txt → staging CSV
│   ├── ingest_raw.py               # CSV → raw_sensor_readings (upsert)
│   └── score_latest.py             # Load model → write to model_predictions
├── notebooks/
│   └── dashboard.ipynb             # KPI summary + sensor trend charts
├── tests/
│   └── test_pipeline.py            # Unit tests for transform logic
├── requirements.txt
├── plan.md                         # This file
└── README.md
```

---

## Airflow DAG: `rul_pipeline`

**Schedule:** `@daily` (manual trigger for development)
**File:** `dags/rul_pipeline.py`

```
extract_data
    │
    ▼
load_raw_to_postgres
    │
    ▼
run_great_expectations
    │
    ▼
dbt_run_transforms          ← runs: stg → int → marts
    │
    ▼
train_rul_model
    │
    ▼
score_latest_predictions
    │
    ▼
report_pipeline_metrics
```

Task configuration:
- All tasks: `retries=2, retry_delay=timedelta(minutes=5)`
- Failure callback: log to `pipeline_runs` with `status='failed'`

---

## dbt Models

| Model | Materialization | Description |
|---|---|---|
| `stg_sensor_readings` | view | Clean + type-cast raw table |
| `int_rolling_features` | ephemeral | Rolling window calculations per engine |
| `features_engine_cycle` | table | Final feature mart |
| `labels_rul` | table | RUL + failure flag |

**dbt tests:**
- `not_null` on all key columns
- `unique` on `(engine_id, cycle, dataset_split)` in feature mart
- `accepted_values` on `dataset_split` ∈ ['train', 'test']
- Custom: `assert_rul_nonnegative.sql`

---

## Great Expectations Suite: `raw_sensor_readings`

| Expectation | Value |
|---|---|
| `expect_table_row_count_to_be_between` | min=20000, max=25000 |
| `expect_column_values_to_not_be_null` | all sensor columns |
| `expect_column_values_to_be_between` | sensor-specific bounds (e.g., s2: 540–645) |
| `expect_column_values_to_be_unique` | `(engine_id, cycle)` within train split |
| `expect_column_min_to_be_between` | `engine_id` ≥ 1, `cycle` ≥ 1 |

---

## ML Models

### Baseline
- **Mean RUL by cycle:** for each cycle number, predict the average RUL seen in training
- Logged as `model_version = 'baseline_mean'`

### Improved
- **XGBoost Regressor** on rolling window features
- Hyperparameters: `n_estimators=200, max_depth=6, learning_rate=0.05`
- Target: piecewise linear RUL (capped at 125)
- Logged as `model_version = 'xgb_v1'`

### Metrics (FD001 test set — fill in after Phase 5)

| Metric | Baseline | XGBoost | Delta |
|---|---|---|---|
| MAE | TBD | TBD | TBD |
| RMSE | TBD | TBD | TBD |
| Inference / 1k rows (ms) | TBD | TBD | — |

---

## Dashboard KPIs (Metabase or notebook)

1. **Pipeline health** — last DAG run status, rows ingested, validation pass rate
2. **RUL prediction vs true RUL** — scatter plot (test set)
3. **MAE over pipeline runs** — from `pipeline_runs` table
4. **Sensor degradation trend** — rolling mean of s2, s4, s11 per engine
5. **Feature build runtime** — bar chart from `pipeline_runs.transform_runtime_s`

---

## Performance Targets (Benchmarks to Document)

| Metric | Target |
|---|---|
| Raw rows ingested | 20,631 (train) + 13,096 (test) = 33,727 total |
| Feature build runtime | < 30s |
| Query latency (unindexed) | Measure baseline before index |
| Query latency (indexed) | Measure after `CREATE INDEX ON raw_sensor_readings (engine_id, cycle)` |
| dbt test pass rate | 100% |
| GE validation pass rate | 100% |
| XGBoost MAE improvement vs baseline | Target ≥ 20% reduction |

---

## Implementation Phases

### Phase 1 — Infrastructure (Day 1, ~2h)
- [ ] Write `docker/docker-compose.yml` (Airflow + Postgres + Metabase)
- [ ] Write `sql/schema.sql` (all 5 table DDLs + indexes)
- [ ] Write `requirements.txt`
- [ ] Verify containers spin up, Postgres accessible on port 5432

### Phase 2 — Ingestion (Day 1–2, ~3h)
- [ ] Write `scripts/extract_data.py` (unzip CMAPSSData, parse txt → CSV)
- [ ] Write `scripts/ingest_raw.py` (upsert CSVs → `raw_sensor_readings`)
- [ ] Verify row counts: 20,631 train + 13,096 test
- [ ] Benchmark query latency before/after index

### Phase 3 — dbt Transforms (Day 2, ~3h)
- [ ] Initialize dbt project (`dbt init`)
- [ ] Write staging, intermediate, and mart models
- [ ] Add dbt schema tests
- [ ] Run `dbt build` — verify all models and tests pass

### Phase 4 — Data Quality (Day 2–3, ~2h)
- [ ] Initialize Great Expectations (`great_expectations init`)
- [ ] Define `raw_sensor_readings` expectation suite
- [ ] Run validation — verify pass rate
- [ ] Integrate GE checkpoint call into Airflow `run_great_expectations` task

### Phase 5 — ML Pipeline (Day 3, ~3h)
- [ ] Write `models/train_rul.py` (baseline + XGBoost, save pkl)
- [ ] Evaluate on FD001 test set — fill in metrics table above
- [ ] Write `scripts/score_latest.py` (load pkl → write to `model_predictions`)
- [ ] Verify predictions written to warehouse

### Phase 6 — Airflow DAG (Day 3–4, ~3h)
- [ ] Write `dags/rul_pipeline.py` with all 7 tasks
- [ ] Add retries + on_failure_callback
- [ ] `report_pipeline_metrics` task → insert row to `pipeline_runs`
- [ ] Run full DAG end-to-end — verify all tasks green

### Phase 7 — Dashboard + README (Day 4–5, ~3h)
- [ ] Write `notebooks/dashboard.ipynb` with 5 KPI charts
- [ ] Fill README: architecture diagram, schema summary, metrics table, how-to-run
- [ ] Back-fill benchmark numbers (latency, runtime, MAE) into this file
- [ ] Take screenshot of DAG graph and dashboard for README

---

## How to Run (Target State)

```bash
# 1. Clone and extract data
git clone <repo>
unzip "6.+Turbofan+Engine+Degradation+Simulation+Data+Set.zip"
# then unzip the nested CMAPSSData.zip into data/raw/

# 2. Start infrastructure
cd docker && docker-compose up -d

# 3. Trigger full pipeline
airflow dags trigger rul_pipeline

# 4. Or run steps manually (outside Airflow)
python scripts/extract_data.py
python scripts/ingest_raw.py
dbt run --project-dir dbt/
python models/train_rul.py
python scripts/score_latest.py
```

---

## Resume Bullets (Fill in after Phase 6–7)

- Built an automated predictive maintenance data platform using **Airflow + PostgreSQL**, ingesting **33K+ sensor readings** across **100 engines**, with reproducible daily pipelines and Great Expectations data-quality validation.
- Designed a 5-table warehouse schema and dbt feature pipeline for engine-cycle telemetry; improved query latency by **X%** via composite indexing and reduced feature build time to **<30s**.
- Trained baseline vs. XGBoost RUL models (**MAE = A → B**, Δ = **C%**) and wrote versioned predictions to the warehouse via a scheduled Airflow scoring task.
