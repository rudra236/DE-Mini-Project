"""BankFlow Sentinel - ETL Ingestion and Loading Pipeline.

Reads synthetic stage CSVs (source, payment, settlement), standardizes types,
validates schema and integrity, and performs idempotent bulk loading into PostgreSQL.
"""

import argparse
import json
import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Ensure project root is on sys.path for direct script execution
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import pandas as pd
import psycopg2.extras

from data_gen.utils import normalize_batch
from db.connection import get_db_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("etl.load")

REQUIRED_COLUMNS = ["txn_id", "account_id", "amount", "currency", "timestamp", "status"]
STAGES = ["source", "payment", "settlement"]


def process_stage_dataframe(
    csv_path: Path,
    stage: str,
    batch_id: str,
) -> Tuple[List[Tuple], Optional[Dict[str, Any]]]:
    """Read, standardize, and validate a single stage CSV.

    Returns:
        (tuples_to_insert, schema_issue_dict_or_none)
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing expected CSV file for stage '{stage}': {csv_path}")

    # Read CSV without type coercion for amount to accurately detect empty strings
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)

    schema_issue = None
    present_cols = set(df.columns)
    missing_cols = [c for c in REQUIRED_COLUMNS if c not in present_cols]
    unexpected_cols = [c for c in df.columns if c not in REQUIRED_COLUMNS]

    if missing_cols or unexpected_cols:
        schema_issue = {
            "stage": stage,
            "missing_columns": missing_cols,
            "unexpected_columns": unexpected_cols,
        }
        logger.warning("Schema discrepancy detected in stage '%s': %s", stage, schema_issue)

    # Ensure all required columns exist in DataFrame, filled with None if missing
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = None

    # 1. Standardize string columns
    for str_col in ["txn_id", "account_id", "currency", "status"]:
        df[str_col] = df[str_col].apply(lambda x: x.strip() if isinstance(x, str) else x)

    df["currency"] = df["currency"].apply(lambda x: x.upper() if isinstance(x, str) else x)

    # 2. Standardize timestamps to UTC
    # Replace empty strings with None
    df["timestamp"] = df["timestamp"].replace("", None)
    df["parsed_ts"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")

    # 3. Standardize and validate amounts
    # Detect empty strings or non-numeric values
    tuples: List[Tuple] = []
    for _, row in df.iterrows():
        txn_id = row["txn_id"] or f"UNKNOWN_{uuid.uuid4().hex[:6]}"
        account_id = row["account_id"]
        raw_amt = row["amount"]
        curr = row["currency"]
        ts_val = row["parsed_ts"]
        ts_py = ts_val.to_pydatetime() if pd.notnull(ts_val) else None
        st_val = row["status"]

        # Validity determination
        parsed_amount = None
        is_valid = True
        invalid_reason = None

        if raw_amt is None or raw_amt == "" or pd.isna(raw_amt):
            is_valid = False
            invalid_reason = "NULL_AMOUNT"
        else:
            try:
                amt_float = float(raw_amt)
                parsed_amount = round(amt_float, 2)
                if amt_float <= 0:
                    is_valid = False
                    invalid_reason = f"NEGATIVE_OR_ZERO_AMOUNT: {raw_amt}"
            except ValueError:
                is_valid = False
                invalid_reason = f"NON_NUMERIC_AMOUNT: {raw_amt}"

        tuples.append((
            batch_id,
            stage,
            txn_id,
            account_id,
            parsed_amount,
            curr,
            ts_py,
            st_val,
            is_valid,
            invalid_reason,
        ))

    return tuples, schema_issue


def run_etl(batch_id: str, data_dir: str = "data") -> Dict[str, Any]:
    """Execute idempotent ETL for a given batch across all three stages.

    Args:
        batch_id: Batch identifier (e.g. 'B001' or 'batch_001').
        data_dir: Path to directory containing batch subfolders.

    Returns:
        Summary dict containing row counts, statuses, and schema issues.
    """
    b_id, dir_name = normalize_batch(batch_id)
    batch_folder = Path(data_dir) / dir_name
    started_at = datetime.now(timezone.utc)
    run_id = f"run_etl_{b_id}_{uuid.uuid4().hex[:8]}"

    logger.info("Starting ETL load for batch '%s' from folder '%s' (Run ID: %s)", b_id, batch_folder, run_id)

    all_stage_tuples: List[Tuple] = []
    schema_issues: List[Dict[str, Any]] = []
    stage_counts: Dict[str, int] = {}

    # Process all three stages
    for stage in STAGES:
        csv_file = batch_folder / f"{dir_name}_{stage}.csv"
        records, issue = process_stage_dataframe(csv_path=csv_file, stage=stage, batch_id=b_id)
        all_stage_tuples.extend(records)
        stage_counts[stage] = len(records)
        if issue:
            schema_issues.append(issue)

    total_rows = len(all_stage_tuples)

    # Perform atomic, idempotent write using a single transaction
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # 1. Upsert batch record in 'batches'
            cur.execute(
                """
                INSERT INTO batches (batch_id, file_path, row_count, status, updated_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (batch_id) DO UPDATE SET
                    file_path = EXCLUDED.file_path,
                    row_count = EXCLUDED.row_count,
                    status = EXCLUDED.status,
                    updated_at = NOW();
                """,
                (b_id, str(batch_folder), total_rows, "ingested"),
            )

            # 2. Delete existing transactions for this batch to ensure idempotency
            cur.execute("DELETE FROM transactions WHERE batch_id = %s;", (b_id,))

            # 3. Bulk insert transactions
            insert_query = """
                INSERT INTO transactions (
                    batch_id, stage, txn_id, account_id, amount,
                    currency, ts, status, is_valid, invalid_reason
                ) VALUES %s;
            """
            psycopg2.extras.execute_values(
                cur,
                insert_query,
                all_stage_tuples,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                page_size=5000,
            )

            # 4. Insert pipeline_runs record
            finished_at = datetime.now(timezone.utc)
            details_payload = {
                "schema_issues": schema_issues,
                "stage_counts": stage_counts,
                "total_rows": total_rows,
            }
            cur.execute(
                """
                INSERT INTO pipeline_runs (
                    run_id, batch_id, task_name, status, started_at, finished_at, retries, details
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    run_id,
                    b_id,
                    "etl_load",
                    "completed",
                    started_at,
                    finished_at,
                    0,
                    json.dumps(details_payload),
                ),
            )

        conn.commit()

    logger.info(
        "ETL complete for batch %s. Rows loaded: source=%d, payment=%d, settlement=%d (Total: %d)",
        b_id,
        stage_counts.get("source", 0),
        stage_counts.get("payment", 0),
        stage_counts.get("settlement", 0),
        total_rows,
    )

    return {
        "run_id": run_id,
        "batch_id": b_id,
        "dir_name": dir_name,
        "total_rows": total_rows,
        "stage_counts": stage_counts,
        "schema_issues": schema_issues,
    }


def main():
    parser = argparse.ArgumentParser(description="BankFlow Sentinel ETL Ingestion")
    parser.add_argument("batch", type=str, help="Batch identifier (e.g. 'batch_001' or 'B001')")
    parser.add_argument("--data-dir", type=str, default="data", help="Directory containing batches")

    args = parser.parse_args()
    summary = run_etl(batch_id=args.batch, data_dir=args.data_dir)

    print("=" * 60)
    print(f" ETL LOAD COMPLETED: Batch {summary['batch_id']}")
    print("=" * 60)
    for stage, count in summary["stage_counts"].items():
        print(f"Stage {stage:12s}: {count:,} rows")
    print(f"Total Rows Loaded : {summary['total_rows']:,}")
    if summary["schema_issues"]:
        print(f"Schema Issues     : {len(summary['schema_issues'])} detected")
        print(json.dumps(summary["schema_issues"], indent=2))
    print("=" * 60)


if __name__ == "__main__":
    main()
