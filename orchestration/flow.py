"""BankFlow Sentinel - Prefect Orchestration Pipeline.

Orchestrates the end-to-end data pipeline for a transaction batch:
- Task 1 (ingest): Verifies files exist, registers batch in PostgreSQL.
- Task 2 (validate): Runs ETL loading and data-quality checks (with forced retry simulation).
- Task 3 (reconcile): Executes adjacent-stage reconciliation across pure SQL JOINs.
- Task 4 (report): Compiles executive Markdown report in docs/reports/<batch_id>.md.

Each task is configured with retries=3, retry_delay_seconds=2, and logs every hop
to pipeline_runs in PostgreSQL for visibility on the Streamlit monitoring dashboard.
"""

import argparse
import json
import os
import sys
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

# Ensure project root is in sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from prefect import flow, task
from prefect.logging import get_run_logger
from sqlalchemy import text

from data_gen.utils import normalize_batch
from db.connection import get_engine
from etl.load import STAGES, run_etl
from reconcile.engine import reconcile
from reconcile.quality import get_quality_metrics

REPORTS_DIR = ROOT_DIR / "docs" / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def record_pipeline_run(
    batch_id: str,
    task_name: str,
    status: str,
    started_at: datetime,
    finished_at: datetime,
    retries: int = 0,
    details: Optional[Dict[str, Any]] = None,
) -> str:
    """Record a task execution entry into PostgreSQL pipeline_runs table."""
    run_id = f"run_{task_name}_{batch_id}_{uuid.uuid4().hex[:8]}"
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO pipeline_runs (run_id, batch_id, task_name, status, started_at, finished_at, retries, details)
                VALUES (:rid, :b, :t, :s, :start, :fin, :ret, :det);
            """),
            {
                "rid": run_id,
                "b": batch_id,
                "t": task_name,
                "s": status,
                "start": started_at,
                "fin": finished_at,
                "ret": retries,
                "det": json.dumps(details or {}, default=str),
            },
        )
    return run_id


@task(name="ingest", retries=3, retry_delay_seconds=2)
def ingest_task(batch_id: str, data_dir: str = "data") -> Dict[str, Any]:
    """Prefect Task 1: Ingest.
    
    Verifies stage CSV files exist, calculates source row counts,
    and initializes the batch entry in PostgreSQL.
    """
    logger = get_run_logger()
    started_at = datetime.now(timezone.utc)
    b_id, dir_name = normalize_batch(batch_id)
    batch_path = Path(data_dir) / dir_name

    logger.info("Executing Ingest Task for batch %s at '%s'", b_id, batch_path)

    if not batch_path.exists():
        err_msg = f"Batch folder not found: {batch_path}"
        logger.error(err_msg)
        record_pipeline_run(b_id, "ingest", "failed", started_at, datetime.now(timezone.utc), details={"error": err_msg})
        raise FileNotFoundError(err_msg)

    # Confirm all three stage CSVs exist
    found_files: Dict[str, str] = {}
    for stage in STAGES:
        csv_file = batch_path / f"{dir_name}_{stage}.csv"
        if not csv_file.exists():
            err_msg = f"Missing expected stage CSV: {csv_file}"
            logger.error(err_msg)
            record_pipeline_run(b_id, "ingest", "failed", started_at, datetime.now(timezone.utc), details={"error": err_msg})
            raise FileNotFoundError(err_msg)
        found_files[stage] = str(csv_file)

    # Estimate source row count
    src_csv = batch_path / f"{dir_name}_source.csv"
    with open(src_csv, "r", encoding="utf-8") as f:
        row_count = max(0, sum(1 for _ in f) - 1)

    # Upsert batch row in PostgreSQL
    engine = get_engine()
    now_utc = datetime.now(timezone.utc)
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO batches (batch_id, file_path, row_count, status, created_at, updated_at)
                VALUES (:b, :fp, :rc, 'created', :now, :now)
                ON CONFLICT (batch_id) DO UPDATE SET
                    file_path = EXCLUDED.file_path,
                    row_count = EXCLUDED.row_count,
                    updated_at = EXCLUDED.updated_at;
            """),
            {"b": b_id, "fp": str(batch_path), "rc": row_count, "now": now_utc},
        )

    finished_at = datetime.now(timezone.utc)
    res = {
        "batch_id": b_id,
        "dir_name": dir_name,
        "batch_path": str(batch_path),
        "row_count": row_count,
        "files": found_files,
    }
    record_pipeline_run(b_id, "ingest", "success", started_at, finished_at, retries=0, details=res)
    logger.info("Ingest task complete for %s. Source row count: %d", b_id, row_count)
    return res


@task(name="validate", retries=3, retry_delay_seconds=2)
def validate_task(batch_id: str, fail_once: bool = False, data_dir: str = "data") -> Dict[str, Any]:
    """Prefect Task 2: Validate.
    
    Executes ETL loading and data quality checks.
    Supports deliberate failure simulation on Attempt 1 via fail_once flag or BANKFLOW_FAIL_ONCE env var.
    """
    logger = get_run_logger()
    started_at = datetime.now(timezone.utc)
    b_id, dir_name = normalize_batch(batch_id)

    # Check for forced failure simulation
    should_fail_once = fail_once or os.getenv("BANKFLOW_FAIL_ONCE", "false").lower() in ("true", "1", "yes")
    counter_file = Path(tempfile.gettempdir()) / f"bankflow_fail_once_{b_id}.counter"

    retries_count = 0
    if should_fail_once:
        attempts = 0
        if counter_file.exists():
            try:
                attempts = int(counter_file.read_text().strip())
            except Exception:
                attempts = 0

        if attempts == 0:
            counter_file.write_text("1")
            err_msg = f"Attempt 1 failed: [SIMULATED INGESTION FAULT] Deliberate validation failure for batch {b_id} demonstration. Retrying in 2 seconds..."
            logger.warning(err_msg)
            # Record failed run
            record_pipeline_run(
                b_id,
                "validate",
                "retry_wait",
                started_at,
                datetime.now(timezone.utc),
                retries=0,
                details={"attempt": 1, "error": err_msg, "simulated": True},
            )
            raise RuntimeError(err_msg)
        else:
            logger.info("Attempt 2 succeeded: Recovery triggered! Task 'validate' succeeded on retry attempt.")
            retries_count = 1
            try:
                counter_file.unlink(missing_ok=True)
            except Exception:
                pass

    logger.info("Running ETL for batch %s...", b_id)
    etl_result = run_etl(batch_id=b_id, data_dir=data_dir)
    logger.info("ETL succeeded: loaded %d total rows %s", etl_result.get("total_rows"), etl_result.get("stage_counts"))

    logger.info("Running Data Quality profiling for batch %s...", b_id)
    quality_result = get_quality_metrics(batch_id=b_id)
    logger.info(
        "Quality profiling complete: Completeness=%.2f%%, Validity=%.2f%%, Duplicates=%d",
        quality_result.get("completeness_rate", 1.0) * 100,
        quality_result.get("validity_rate", 1.0) * 100,
        quality_result.get("total_duplicates", 0),
    )

    finished_at = datetime.now(timezone.utc)
    res = {
        "etl": etl_result,
        "quality": quality_result,
        "retries": retries_count,
    }
    record_pipeline_run(b_id, "validate", "success", started_at, finished_at, retries=retries_count, details=res)
    return res


@task(name="reconcile", retries=3, retry_delay_seconds=2)
def reconcile_task(batch_id: str) -> Dict[str, Any]:
    """Prefect Task 3: Reconcile.
    
    Executes adjacent-stage cross-reconciliation across pure SQL JOINs,
    detecting all 6 discrepancy types and raising incident tickets.
    """
    logger = get_run_logger()
    started_at = datetime.now(timezone.utc)
    b_id, _ = normalize_batch(batch_id)

    logger.info("Executing Reconciliation Engine for batch %s...", b_id)
    reconcile_result = reconcile(batch_id=b_id)

    logger.info(
        "Reconciliation complete: %d incidents raised. Discrepancies by type: %s",
        reconcile_result.get("total_incidents", 0),
        reconcile_result.get("incident_counts_by_type", {}),
    )

    finished_at = datetime.now(timezone.utc)
    record_pipeline_run(b_id, "reconcile", "success", started_at, finished_at, retries=0, details=reconcile_result)
    return reconcile_result


@task(name="report", retries=3, retry_delay_seconds=2)
def report_task(
    batch_id: str,
    ingest_res: Dict[str, Any],
    validate_res: Dict[str, Any],
    reconcile_res: Dict[str, Any],
    flow_started_at: datetime,
) -> Dict[str, Any]:
    """Prefect Task 4: Report.
    
    Compiles detailed audit report at docs/reports/<batch_id>.md and
    records final summary pipeline_runs entry with total duration and retries.
    """
    logger = get_run_logger()
    started_at = datetime.now(timezone.utc)
    b_id, _ = normalize_batch(batch_id)
    now_utc = datetime.now(timezone.utc)

    total_duration = round((now_utc - flow_started_at).total_seconds(), 2)
    total_retries = validate_res.get("retries", 0)

    report_path = REPORTS_DIR / f"{b_id}.md"
    logger.info("Compiling pipeline report for batch %s to '%s'", b_id, report_path)

    stage_counts = reconcile_res.get("stage_counts", {})
    incidents_by_type = reconcile_res.get("incident_counts_by_type", {})
    incidents_list = reconcile_res.get("incidents", [])

    # Format Markdown Report
    lines = [
        f"# BankFlow Sentinel Pipeline Audit Report: {b_id}",
        "",
        f"> **Generated At (UTC):** {now_utc.isoformat()}  ",
        f"> **Batch ID:** `{b_id}`  ",
        f"> **Pipeline Status:** `{reconcile_res.get('status', 'reconciled').upper()}`  ",
        f"> **Total Execution Duration:** `{total_duration}s`  ",
        f"> **Total Pipeline Retries:** `{total_retries}`  ",
        "",
        "---",
        "",
        "## 1. Stage Reconciliation Summary",
        "",
        "| Stage | Source Count | Target Count | Missing | Duplicates | Amount Mismatch | Delayed | Invalid |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ]

    for stage_name, m in stage_counts.items():
        lines.append(
            f"| **{stage_name.capitalize()}** | {m.get('source_count', 0):,} | {m.get('target_count', 0):,} | "
            f"{m.get('missing_count', 0):,} | {m.get('duplicate_count', 0):,} | {m.get('mismatch_count', 0):,} | "
            f"{m.get('delayed_count', 0):,} | {m.get('invalid_count', 0):,} |"
        )

    lines.extend([
        "",
        "## 2. Discrepancy Breakdown by Type",
        "",
        "| Discrepancy Type | Affected Records | Contract Severity |",
        "| :--- | :--- | :--- |",
    ])

    from reconcile.engine import SEVERITY_MAP
    for disc_type, count in incidents_by_type.items():
        sev = SEVERITY_MAP.get(disc_type, "medium").upper()
        lines.append(f"| `{disc_type}` | {count:,} | **{sev}** |")

    lines.extend([
        "",
        "## 3. Incident Evidence Samples",
        "",
    ])

    for inc in incidents_list:
        ev = inc.get("evidence", [])
        sample_ev = ev[:10]
        remaining = max(0, len(ev) - 10)
        lines.append(
            f"### Incident: `{inc.get('type')}` at `{inc.get('stage')}` ({len(ev)} affected transactions)\n"
            f"- **Severity:** {inc.get('severity').upper()}\n"
            f"- **Evidence sample:** `{sample_ev}`{' + ' + str(remaining) + ' more' if remaining else ''}\n"
        )

    lines.extend([
        "---",
        "*Automated report generated by Prefect Flow `bankflow_pipeline`.*",
        "*Synthetic Data Disclaimer: Discrepancies indicate data pipeline issues, not lost funds.*",
    ])

    # Write report file
    report_path.write_text("\n".join(lines), encoding="utf-8")

    # Record report task run in DB
    record_pipeline_run(
        b_id,
        "report",
        "success",
        started_at,
        now_utc,
        retries=0,
        details={"report_file": str(report_path)},
    )

    # Record overall flow run in DB
    record_pipeline_run(
        b_id,
        "bankflow_pipeline",
        "success",
        flow_started_at,
        now_utc,
        retries=total_retries,
        details={
            "duration_seconds": total_duration,
            "stages": ["ingest", "validate", "reconcile", "report"],
            "total_incidents": reconcile_res.get("total_incidents", 0),
            "report_path": str(report_path),
        },
    )

    logger.info("Report task complete. File written to %s", report_path)
    return {
        "report_file": str(report_path),
        "total_duration_seconds": total_duration,
        "total_retries": total_retries,
    }


@flow(name="bankflow-pipeline", log_prints=True)
def bankflow_pipeline(
    batch_id: str,
    fail_once: bool = False,
    data_dir: str = "data",
) -> Dict[str, Any]:
    """BankFlow Sentinel Main Orchestration Flow."""
    flow_started_at = datetime.now(timezone.utc)
    logger = get_run_logger()
    b_id, _ = normalize_batch(batch_id)

    logger.info("================================================================")
    logger.info(" Starting Prefect Flow 'bankflow-pipeline' for Batch: %s", b_id)
    if fail_once:
        logger.info(" [DEMO MODE] --fail-once enabled: Attempt 1 will simulate failure.")
    logger.info("================================================================")

    # Task 1: Ingest
    ingest_res = ingest_task(batch_id=b_id, data_dir=data_dir)

    # Task 2: Validate (with automatic retry)
    validate_res = validate_task(batch_id=b_id, fail_once=fail_once, data_dir=data_dir)

    # Task 3: Reconcile
    reconcile_res = reconcile_task(batch_id=b_id)

    # Task 4: Report
    report_res = report_task(
        batch_id=b_id,
        ingest_res=ingest_res,
        validate_res=validate_res,
        reconcile_res=reconcile_res,
        flow_started_at=flow_started_at,
    )

    logger.info("================================================================")
    logger.info(" Prefect Flow 'bankflow-pipeline' Completed Successfully!")
    logger.info(" Batch: %s | Retries: %d | Duration: %.2fs", b_id, validate_res.get("retries", 0), report_res.get("total_duration_seconds", 0))
    logger.info(" Audit Report: %s", report_res.get("report_file"))
    logger.info("================================================================")

    return {
        "batch_id": b_id,
        "status": "success",
        "ingest": ingest_res,
        "validate": validate_res,
        "reconcile": reconcile_res,
        "report": report_res,
    }


def main():
    parser = argparse.ArgumentParser(description="BankFlow Sentinel - Prefect Pipeline Runner")
    parser.add_argument("batch", nargs="?", default="B001", help="Batch identifier (e.g. B001)")
    parser.add_argument("--batch-id", dest="batch_opt", default=None, help="Batch identifier (option flag)")
    parser.add_argument(
        "--fail-once",
        action="store_true",
        default=False,
        help="Simulate deliberate failure on Attempt 1 of validate task to demo retry recovery",
    )
    parser.add_argument("--data-dir", default="data", help="Data directory (default: data)")

    args = parser.parse_args()
    target_batch = args.batch_opt or args.batch

    summary = bankflow_pipeline(
        batch_id=target_batch,
        fail_once=args.fail_once,
        data_dir=args.data_dir,
    )

    print("\n" + "=" * 65)
    print(" PREFECT FLOW RUN SUMMARY")
    print("=" * 65)
    print(f"Batch ID       : {summary['batch_id']}")
    print(f"Status         : {summary['status'].upper()}")
    print(f"Retries        : {summary['validate'].get('retries', 0)}")
    print(f"Report Written : {summary['report'].get('report_file')}")
    print("=" * 65)


if __name__ == "__main__":
    main()
