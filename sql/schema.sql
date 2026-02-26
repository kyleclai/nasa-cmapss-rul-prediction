-- ============================================================
-- NASA CMAPSS Predictive Maintenance Warehouse Schema
-- Database: warehouse_db
-- ============================================================

-- ── 1. raw_sensor_readings ────────────────────────────────────────────────
-- Grain: (engine_id, cycle, dataset_split)
-- Populated by: scripts/ingest_raw.py

CREATE TABLE IF NOT EXISTS raw_sensor_readings (
    engine_id       INT         NOT NULL,
    cycle           INT         NOT NULL,
    dataset_split   VARCHAR(10) NOT NULL,   -- 'train' | 'test'
    op1             FLOAT,
    op2             FLOAT,
    op3             FLOAT,
    s1              FLOAT,
    s2              FLOAT,
    s3              FLOAT,
    s4              FLOAT,
    s5              FLOAT,
    s6              FLOAT,
    s7              FLOAT,
    s8              FLOAT,
    s9              FLOAT,
    s10             FLOAT,
    s11             FLOAT,
    s12             FLOAT,
    s13             FLOAT,
    s14             FLOAT,
    s15             FLOAT,
    s16             FLOAT,
    s17             FLOAT,
    s18             FLOAT,
    s19             FLOAT,
    s20             FLOAT,
    s21             FLOAT,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_file     VARCHAR(50),
    PRIMARY KEY (engine_id, cycle, dataset_split)
);

-- Index for time-series window queries (engine history in order)
CREATE INDEX IF NOT EXISTS idx_raw_engine_cycle
    ON raw_sensor_readings (engine_id, cycle);

-- Index for split-level scans
CREATE INDEX IF NOT EXISTS idx_raw_split
    ON raw_sensor_readings (dataset_split);


-- ── 2. features_engine_cycle ──────────────────────────────────────────────
-- Grain: (engine_id, cycle, dataset_split)
-- Populated by: dbt mart model

CREATE TABLE IF NOT EXISTS features_engine_cycle (
    engine_id           INT         NOT NULL,
    cycle               INT         NOT NULL,
    dataset_split       VARCHAR(10) NOT NULL,
    -- Raw sensors (informative subset: s2,s3,s4,s7,s8,s9,s11,s12,s13,s14,s15,s17,s20,s21)
    s2  FLOAT, s3  FLOAT, s4  FLOAT, s7  FLOAT,
    s8  FLOAT, s9  FLOAT, s11 FLOAT, s12 FLOAT,
    s13 FLOAT, s14 FLOAT, s15 FLOAT, s17 FLOAT,
    s20 FLOAT, s21 FLOAT,
    -- Rolling mean (window=5)
    rmean_s2_5  FLOAT, rmean_s3_5  FLOAT, rmean_s4_5  FLOAT,
    rmean_s7_5  FLOAT, rmean_s8_5  FLOAT, rmean_s9_5  FLOAT,
    rmean_s11_5 FLOAT, rmean_s12_5 FLOAT, rmean_s13_5 FLOAT,
    rmean_s14_5 FLOAT, rmean_s15_5 FLOAT, rmean_s17_5 FLOAT,
    rmean_s20_5 FLOAT, rmean_s21_5 FLOAT,
    -- Rolling std (window=5)
    rstd_s2_5   FLOAT, rstd_s3_5   FLOAT, rstd_s4_5   FLOAT,
    rstd_s7_5   FLOAT, rstd_s8_5   FLOAT, rstd_s9_5   FLOAT,
    rstd_s11_5  FLOAT, rstd_s12_5  FLOAT, rstd_s13_5  FLOAT,
    rstd_s14_5  FLOAT, rstd_s15_5  FLOAT, rstd_s17_5  FLOAT,
    rstd_s20_5  FLOAT, rstd_s21_5  FLOAT,
    -- Rolling mean (window=20)
    rmean_s2_20  FLOAT, rmean_s3_20  FLOAT, rmean_s4_20  FLOAT,
    rmean_s7_20  FLOAT, rmean_s8_20  FLOAT, rmean_s9_20  FLOAT,
    rmean_s11_20 FLOAT, rmean_s12_20 FLOAT, rmean_s13_20 FLOAT,
    rmean_s14_20 FLOAT, rmean_s15_20 FLOAT, rmean_s17_20 FLOAT,
    rmean_s20_20 FLOAT, rmean_s21_20 FLOAT,
    -- Rolling std (window=20)
    rstd_s2_20   FLOAT, rstd_s3_20   FLOAT, rstd_s4_20   FLOAT,
    rstd_s7_20   FLOAT, rstd_s8_20   FLOAT, rstd_s9_20   FLOAT,
    rstd_s11_20  FLOAT, rstd_s12_20  FLOAT, rstd_s13_20  FLOAT,
    rstd_s14_20  FLOAT, rstd_s15_20  FLOAT, rstd_s17_20  FLOAT,
    rstd_s20_20  FLOAT, rstd_s21_20  FLOAT,
    -- Lag deltas (s - lag1)
    delta_s2  FLOAT, delta_s3  FLOAT, delta_s4  FLOAT,
    delta_s7  FLOAT, delta_s8  FLOAT, delta_s9  FLOAT,
    delta_s11 FLOAT, delta_s12 FLOAT, delta_s13 FLOAT,
    delta_s14 FLOAT, delta_s15 FLOAT, delta_s17 FLOAT,
    delta_s20 FLOAT, delta_s21 FLOAT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (engine_id, cycle, dataset_split)
);

CREATE INDEX IF NOT EXISTS idx_feat_engine_cycle
    ON features_engine_cycle (engine_id, cycle);


-- ── 3. labels_rul ─────────────────────────────────────────────────────────
-- Grain: (engine_id, cycle, dataset_split)
-- Populated by: dbt mart model

CREATE TABLE IF NOT EXISTS labels_rul (
    engine_id       INT         NOT NULL,
    cycle           INT         NOT NULL,
    dataset_split   VARCHAR(10) NOT NULL,
    max_cycle       INT         NOT NULL,   -- last cycle observed for this engine
    true_rul        INT         NOT NULL,   -- piecewise RUL (capped at 125)
    failure_flag    SMALLINT    NOT NULL DEFAULT 0,  -- 1 if true_rul = 0
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (engine_id, cycle, dataset_split)
);

CREATE INDEX IF NOT EXISTS idx_labels_engine_cycle
    ON labels_rul (engine_id, cycle);


-- ── 4. model_predictions ──────────────────────────────────────────────────
-- Grain: (engine_id, cycle, dataset_split, model_version)
-- Populated by: scripts/score_latest.py

CREATE TABLE IF NOT EXISTS model_predictions (
    id              SERIAL      PRIMARY KEY,
    engine_id       INT         NOT NULL,
    cycle           INT         NOT NULL,
    dataset_split   VARCHAR(10) NOT NULL,
    model_version   VARCHAR(50) NOT NULL,
    pred_rul        FLOAT       NOT NULL,
    prediction_ts   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pred_engine_cycle
    ON model_predictions (engine_id, cycle);

CREATE INDEX IF NOT EXISTS idx_pred_version
    ON model_predictions (model_version);


-- ── 5. pipeline_runs ──────────────────────────────────────────────────────
-- Grain: (run_id) — observability log
-- Populated by: Airflow report_pipeline_metrics task

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id                  SERIAL      PRIMARY KEY,
    dag_run_id              VARCHAR(100),
    start_ts                TIMESTAMPTZ,
    end_ts                  TIMESTAMPTZ,
    rows_ingested           INT,
    rows_failed_validation  INT,
    transform_runtime_s     FLOAT,
    model_mae               FLOAT,
    status                  VARCHAR(20),    -- 'success' | 'failed' | 'partial'
    notes                   TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
