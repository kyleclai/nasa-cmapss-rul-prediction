"""
rul_pipeline.py
~~~~~~~~~~~~~~~
Airflow DAG: end-to-end NASA CMAPSS predictive maintenance pipeline.

Schedule: daily (set to None for manual trigger during development)

Task order:
  extract_data
      → load_raw_to_postgres
          → run_great_expectations
              → dbt_run_transforms
                  → train_rul_model
                      → score_latest_predictions
                          → report_pipeline_metrics
"""

from __future__ import annotations

import importlib.util
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ── Path resolution ────────────────────────────────────────────────────────
# When running inside the Airflow container, PYTHONPATH=/opt/airflow
AIRFLOW_HOME = Path(os.getenv("AIRFLOW_HOME", "/opt/airflow"))
SCRIPTS_DIR  = AIRFLOW_HOME / "scripts"
MODELS_DIR   = AIRFLOW_HOME / "models"
DBT_DIR      = AIRFLOW_HOME / "dbt"

WAREHOUSE_CONN = os.getenv(
    "WAREHOUSE_CONN",
    "postgresql+psycopg2://warehouse:warehouse@postgres:5432/warehouse_db",
)

log = logging.getLogger(__name__)


# ── Helper: dynamic import ─────────────────────────────────────────────────

def _import_script(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ── Task callables ─────────────────────────────────────────────────────────

def task_extract_data(**context) -> None:
    mod = _import_script("extract_data", SCRIPTS_DIR / "extract_data.py")
    mod.main(dataset="FD001")
    log.info("[extract] Data extraction complete.")


def task_load_raw(**context) -> dict:
    mod = _import_script("ingest_raw", SCRIPTS_DIR / "ingest_raw.py")
    result = mod.main(dataset="FD001", conn_str=WAREHOUSE_CONN)
    log.info("[load_raw] %s", result)
    context["ti"].xcom_push(key="ingest_result", value=result)
    return result


def task_validate(**context) -> dict:
    mod = _import_script("validate_data", SCRIPTS_DIR / "validate_data.py")
    result = mod.validate(conn_str=WAREHOUSE_CONN, fail_on_error=True)
    log.info("[validate] %s", result)
    context["ti"].xcom_push(key="validation_result", value=result)
    return result


def task_dbt_run(**context) -> None:
    """Run dbt build inside the container using subprocess."""
    t0 = time.time()
    env = os.environ.copy()
    env["WAREHOUSE_HOST"] = "postgres"

    result = subprocess.run(
        ["dbt", "build", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)],
        capture_output=True,
        text=True,
        env=env,
    )

    log.info("[dbt] stdout:\n%s", result.stdout)
    if result.returncode != 0:
        log.error("[dbt] stderr:\n%s", result.stderr)
        raise RuntimeError(f"dbt build failed (exit {result.returncode})")

    elapsed = round(time.time() - t0, 2)
    log.info("[dbt] Build complete in %ss", elapsed)
    context["ti"].xcom_push(key="dbt_runtime_s", value=elapsed)


def task_train_model(**context) -> dict:
    mod = _import_script("train_rul", MODELS_DIR / "train_rul.py")
    result = mod.main(conn_str=WAREHOUSE_CONN, dataset="FD001")
    log.info("[train] %s", result)
    context["ti"].xcom_push(key="train_result", value=result)
    return result


def task_score_latest(**context) -> dict:
    mod = _import_script("score_latest", SCRIPTS_DIR / "score_latest.py")
    result = mod.score(conn_str=WAREHOUSE_CONN, model_version="xgb_v1", split="test")
    log.info("[score] %s", result)
    context["ti"].xcom_push(key="score_result", value=result)
    return result


def task_report_metrics(**context) -> None:
    """Gather XCom results and write a row to pipeline_runs."""
    from sqlalchemy import create_engine, text

    ti = context["ti"]
    dag_run = context["dag_run"]

    ingest = ti.xcom_pull(task_ids="load_raw_to_postgres", key="ingest_result") or {}
    valid  = ti.xcom_pull(task_ids="run_great_expectations", key="validation_result") or {}
    train  = ti.xcom_pull(task_ids="train_rul_model", key="train_result") or {}
    dbt_rt = ti.xcom_pull(task_ids="dbt_run_transforms", key="dbt_runtime_s") or 0

    best_mae = None
    if train and "metrics" in train:
        xgb_m = next((m for m in train["metrics"] if m["model"] == "xgb_v1"), None)
        if xgb_m:
            best_mae = xgb_m["mae"]

    engine = create_engine(WAREHOUSE_CONN, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO pipeline_runs
                    (dag_run_id, start_ts, end_ts, rows_ingested,
                     rows_failed_validation, transform_runtime_s,
                     model_mae, status, notes)
                VALUES
                    (:dag_run_id, :start_ts, :end_ts, :rows_ingested,
                     :rows_failed_validation, :transform_runtime_s,
                     :model_mae, :status, :notes)
            """),
            {
                "dag_run_id":               str(dag_run.run_id),
                "start_ts":                 dag_run.start_date,
                "end_ts":                   datetime.utcnow(),
                "rows_ingested":            ingest.get("rows_ingested"),
                "rows_failed_validation":   valid.get("failed", 0),
                "transform_runtime_s":      dbt_rt,
                "model_mae":                best_mae,
                "status":                   "success",
                "notes":                    json.dumps({
                    "mae_improvement_pct": train.get("mae_improvement_pct"),
                    "validation_passed": valid.get("success"),
                }),
            },
        )
    log.info("[report] Pipeline run logged to pipeline_runs.")


def on_failure_callback(context) -> None:
    """Log a failed run to pipeline_runs."""
    try:
        from sqlalchemy import create_engine, text as sqla_text

        engine = create_engine(WAREHOUSE_CONN, pool_pre_ping=True)
        dag_run = context.get("dag_run")
        with engine.begin() as conn:
            conn.execute(
                sqla_text("""
                    INSERT INTO pipeline_runs (dag_run_id, start_ts, end_ts, status, notes)
                    VALUES (:dag_run_id, :start_ts, :end_ts, 'failed', :notes)
                """),
                {
                    "dag_run_id": str(dag_run.run_id) if dag_run else "unknown",
                    "start_ts":   dag_run.start_date if dag_run else None,
                    "end_ts":     datetime.utcnow(),
                    "notes":      f"Task failed: {context.get('task_instance_key_str', '')}",
                },
            )
    except Exception as exc:
        log.warning("[on_failure_callback] Could not write to pipeline_runs: %s", exc)


# ── DAG definition ─────────────────────────────────────────────────────────

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id="rul_pipeline",
    description="NASA CMAPSS predictive maintenance: ingest → validate → dbt → train → score → report",
    schedule_interval=None,          # Set to "@daily" for production
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["cmapss", "predictive-maintenance", "rul"],
) as dag:

    t_extract = PythonOperator(
        task_id="extract_data",
        python_callable=task_extract_data,
    )

    t_load_raw = PythonOperator(
        task_id="load_raw_to_postgres",
        python_callable=task_load_raw,
    )

    t_validate = PythonOperator(
        task_id="run_great_expectations",
        python_callable=task_validate,
    )

    t_dbt = PythonOperator(
        task_id="dbt_run_transforms",
        python_callable=task_dbt_run,
    )

    t_train = PythonOperator(
        task_id="train_rul_model",
        python_callable=task_train_model,
    )

    t_score = PythonOperator(
        task_id="score_latest_predictions",
        python_callable=task_score_latest,
    )

    t_report = PythonOperator(
        task_id="report_pipeline_metrics",
        python_callable=task_report_metrics,
    )

    # ── Task dependencies ──────────────────────────────────────────────────
    t_extract >> t_load_raw >> t_validate >> t_dbt >> t_train >> t_score >> t_report
