"""BankFlow Sentinel - Data Quality Profiler.

Calculates key data quality indicators per batch:
- Null counts and completeness rate
- Invalid amounts and validity rate
- Duplicate counts and duplicate rate
- Data freshness (now - max(ts))
- Schema discrepancies
"""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

# Ensure project root is on sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from sqlalchemy import text

from data_gen.utils import normalize_batch
from db.connection import get_engine

logger = logging.getLogger("reconcile.quality")


def get_quality_metrics(batch_id: str) -> Dict[str, Any]:
    """Retrieve comprehensive data-quality metrics for a batch.

    Args:
        batch_id: Batch identifier (e.g. 'B001' or 'batch_001').

    Returns:
        Dictionary containing quality metrics, rates, freshness, and stage breakdowns.
    """
    b_id, dir_name = normalize_batch(batch_id)
    engine = get_engine()
    now_utc = datetime.now(timezone.utc)

    with engine.connect() as conn:
        # 1. Total and per-stage row counts
        stage_counts_rows = conn.execute(
            text("SELECT stage, COUNT(*), COUNT(DISTINCT txn_id) FROM transactions WHERE batch_id = :b GROUP BY stage;"),
            {"b": b_id},
        ).fetchall()

        stage_breakdown: Dict[str, Dict[str, Any]] = {}
        total_rows = 0
        total_duplicates = 0

        for stage, total, distinct in stage_counts_rows:
            stage_dups = total - distinct
            stage_breakdown[stage] = {
                "total_rows": total,
                "distinct_txns": distinct,
                "duplicate_rows": stage_dups,
                "duplicate_rate": round(stage_dups / total, 4) if total > 0 else 0.0,
            }
            total_rows += total
            total_duplicates += stage_dups

        # 2. Null amount counts
        null_rows = conn.execute(
            text("SELECT stage, COUNT(*) FROM transactions WHERE batch_id = :b AND amount IS NULL GROUP BY stage;"),
            {"b": b_id},
        ).fetchall()

        total_null_amounts = 0
        for stage, null_cnt in null_rows:
            if stage in stage_breakdown:
                stage_breakdown[stage]["null_amounts"] = null_cnt
            total_null_amounts += null_cnt

        # 3. Invalid records count
        invalid_rows = conn.execute(
            text("SELECT stage, COUNT(*) FROM transactions WHERE batch_id = :b AND is_valid = false GROUP BY stage;"),
            {"b": b_id},
        ).fetchall()

        total_invalid_records = 0
        for stage, inv_cnt in invalid_rows:
            if stage in stage_breakdown:
                stage_breakdown[stage]["invalid_records"] = inv_cnt
            total_invalid_records += inv_cnt

        # 4. Freshness calculation
        max_ts = conn.execute(
            text("SELECT MAX(ts) FROM transactions WHERE batch_id = :b;"),
            {"b": b_id},
        ).scalar()

        freshness_seconds = None
        freshness_human = "N/A"
        if max_ts:
            if max_ts.tzinfo is None:
                max_ts = max_ts.replace(tzinfo=timezone.utc)
            delta = now_utc - max_ts
            freshness_seconds = int(delta.total_seconds())
            hours = freshness_seconds // 3600
            minutes = (freshness_seconds % 3600) // 60
            freshness_human = f"{hours}h {minutes}m ago" if hours > 0 else f"{minutes}m ago"

        # 5. Schema issues from pipeline_runs
        run_record = conn.execute(
            text("SELECT details FROM pipeline_runs WHERE batch_id = :b AND task_name = 'etl_load' ORDER BY finished_at DESC LIMIT 1;"),
            {"b": b_id},
        ).fetchone()

        schema_issues: List[Dict[str, Any]] = []
        if run_record and run_record[0]:
            details = run_record[0]
            if isinstance(details, str):
                details = json.loads(details)
            schema_issues = details.get("schema_issues", [])

    completeness_rate = round(1.0 - (total_null_amounts / total_rows), 4) if total_rows > 0 else 1.0
    validity_rate = round(1.0 - (total_invalid_records / total_rows), 4) if total_rows > 0 else 1.0
    duplicate_rate = round(total_duplicates / total_rows, 4) if total_rows > 0 else 0.0

    return {
        "batch_id": b_id,
        "total_transactions": total_rows,
        "null_amounts": total_null_amounts,
        "invalid_amounts": total_invalid_records,
        "duplicate_rows": total_duplicates,
        "duplicate_rate": duplicate_rate,
        "completeness_rate": completeness_rate,
        "validity_rate": validity_rate,
        "latest_timestamp": max_ts.isoformat() if max_ts else None,
        "freshness_seconds": freshness_seconds,
        "freshness_human": freshness_human,
        "schema_issues": schema_issues,
        "has_schema_issues": len(schema_issues) > 0,
        "stage_breakdown": stage_breakdown,
    }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="BankFlow Sentinel Data Quality Profiler")
    parser.add_argument("batch", type=str, help="Batch identifier (e.g. 'B001')")
    args = parser.parse_args()

    metrics = get_quality_metrics(args.batch)
    print("=" * 60)
    print(f" DATA QUALITY REPORT: Batch {metrics['batch_id']}")
    print("=" * 60)
    print(f"Total Transactions : {metrics['total_transactions']:,}")
    print(f"Completeness Rate  : {metrics['completeness_rate'] * 100:.2f}% (Null amounts: {metrics['null_amounts']:,})")
    print(f"Validity Rate      : {metrics['validity_rate'] * 100:.2f}% (Invalid amounts: {metrics['invalid_amounts']:,})")
    print(f"Duplicate Rate     : {metrics['duplicate_rate'] * 100:.2f}% (Duplicates: {metrics['duplicate_rows']:,})")
    print(f"Freshness          : {metrics['freshness_human']} ({metrics['freshness_seconds']}s)")
    print(f"Schema Issues      : {len(metrics['schema_issues'])} detected")
    print("=" * 60)


if __name__ == "__main__":
    main()
