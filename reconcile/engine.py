"""BankFlow Sentinel - Reconciliation Engine.

Performs adjacent-stage reconciliation (source->payment, payment->settlement)
detecting all six contract discrepancy types using pure parameterized SQL JOINs.
Generates actionable incident tickets and stores summary metrics in reconciliation_results.
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

# Ensure project root is on sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from sqlalchemy import text

from data_gen.utils import normalize_batch
from db.connection import get_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("reconcile.engine")

# Contract severity mapping (verbatim from docs/contracts.md)
SEVERITY_MAP = {
    "missing_record": "high",
    "amount_mismatch": "high",
    "schema_change": "high",
    "duplicate": "medium",
    "invalid_record": "medium",
    "delayed": "low",
}

STAGE_PAIRS = [
    ("source", "payment"),
    ("payment", "settlement"),
]


def reconcile_stage_pair(
    conn,
    batch_id: str,
    src_stage: str,
    dst_stage: str,
    schema_issues: List[Dict[str, Any]],
) -> Tuple[Dict[str, int], List[Dict[str, Any]]]:
    """Reconcile an adjacent stage pair and return (reconciliation_counts, incidents).

    Checks in order:
    1. Count check (source_count, target_count)
    2. ID check -> missing_record
    3. Amount check -> amount_mismatch
    4. Duplicate check -> duplicate
    5. Delay check -> delayed (> 30 min)
    6. Schema check -> schema_change
    7. Invalid check -> invalid_record
    """
    logger.info("Reconciling %s -> %s for batch %s", src_stage, dst_stage, batch_id)

    # 1. Counts
    src_count = conn.execute(
        text("SELECT COUNT(*) FROM transactions WHERE batch_id = :b AND stage = :s;"),
        {"b": batch_id, "s": src_stage},
    ).scalar() or 0

    dst_count = conn.execute(
        text("SELECT COUNT(*) FROM transactions WHERE batch_id = :b AND stage = :s;"),
        {"b": batch_id, "s": dst_stage},
    ).scalar() or 0

    detected_incidents: List[Dict[str, Any]] = []

    # 2. ID check -> missing_record (in upstream, absent downstream)
    missing_query = text("""
        SELECT DISTINCT s.txn_id
        FROM transactions s
        LEFT JOIN transactions p
          ON s.txn_id = p.txn_id AND p.stage = :dst_stage AND p.batch_id = :batch_id
        WHERE s.stage = :src_stage AND s.batch_id = :batch_id AND p.txn_id IS NULL
        ORDER BY s.txn_id;
    """)
    missing_ids = [r[0] for r in conn.execute(missing_query, {
        "src_stage": src_stage,
        "dst_stage": dst_stage,
        "batch_id": batch_id,
    }).fetchall()]

    if missing_ids:
        detected_incidents.append({
            "stage": dst_stage,
            "type": "missing_record",
            "severity": SEVERITY_MAP["missing_record"],
            "evidence": missing_ids,
        })

    # 3. Amount check -> amount_mismatch (amounts differ, both valid)
    mismatch_query = text("""
        SELECT DISTINCT s.txn_id
        FROM transactions s
        JOIN transactions p
          ON s.txn_id = p.txn_id AND s.batch_id = p.batch_id
        WHERE s.stage = :src_stage AND p.stage = :dst_stage AND s.batch_id = :batch_id
          AND s.amount <> p.amount
          AND s.is_valid = true AND p.is_valid = true
        ORDER BY s.txn_id;
    """)
    mismatch_ids = [r[0] for r in conn.execute(mismatch_query, {
        "src_stage": src_stage,
        "dst_stage": dst_stage,
        "batch_id": batch_id,
    }).fetchall()]

    if mismatch_ids:
        detected_incidents.append({
            "stage": dst_stage,
            "type": "amount_mismatch",
            "severity": SEVERITY_MAP["amount_mismatch"],
            "evidence": mismatch_ids,
        })

    # 4. Duplicate check -> duplicate (count > 1 downstream)
    duplicate_query = text("""
        SELECT txn_id
        FROM transactions
        WHERE stage = :dst_stage AND batch_id = :batch_id
        GROUP BY txn_id
        HAVING COUNT(*) > 1
        ORDER BY txn_id;
    """)
    duplicate_ids = [r[0] for r in conn.execute(duplicate_query, {
        "dst_stage": dst_stage,
        "batch_id": batch_id,
    }).fetchall()]

    if duplicate_ids:
        detected_incidents.append({
            "stage": dst_stage,
            "type": "duplicate",
            "severity": SEVERITY_MAP["duplicate"],
            "evidence": duplicate_ids,
        })

    # 5. Delay check -> delayed (downstream ts - upstream ts > 30 minutes)
    delay_query = text("""
        SELECT DISTINCT p.txn_id
        FROM transactions s
        JOIN transactions p
          ON s.txn_id = p.txn_id AND s.batch_id = p.batch_id
        WHERE s.stage = :src_stage AND p.stage = :dst_stage AND s.batch_id = :batch_id
          AND s.ts IS NOT NULL AND p.ts IS NOT NULL
          AND p.ts - s.ts > INTERVAL '30 minutes'
        ORDER BY p.txn_id;
    """)
    delayed_ids = [r[0] for r in conn.execute(delay_query, {
        "src_stage": src_stage,
        "dst_stage": dst_stage,
        "batch_id": batch_id,
    }).fetchall()]

    if delayed_ids:
        detected_incidents.append({
            "stage": dst_stage,
            "type": "delayed",
            "severity": SEVERITY_MAP["delayed"],
            "evidence": delayed_ids,
        })

    # 6. Schema check -> schema_change (from pipeline_runs.details)
    has_schema_fault = any(issue.get("stage") == dst_stage for issue in schema_issues)
    if has_schema_fault:
        detected_incidents.append({
            "stage": dst_stage,
            "type": "schema_change",
            "severity": SEVERITY_MAP["schema_change"],
            "evidence": ["*"],
        })

    # 7. Invalid check -> invalid_record (is_valid=false in downstream)
    invalid_query = text("""
        SELECT DISTINCT txn_id
        FROM transactions
        WHERE stage = :dst_stage AND batch_id = :batch_id
          AND is_valid = false
        ORDER BY txn_id;
    """)
    invalid_ids = [r[0] for r in conn.execute(invalid_query, {
        "dst_stage": dst_stage,
        "batch_id": batch_id,
    }).fetchall()]

    if invalid_ids:
        detected_incidents.append({
            "stage": dst_stage,
            "type": "invalid_record",
            "severity": SEVERITY_MAP["invalid_record"],
            "evidence": invalid_ids,
        })

    counts_dict = {
        "source_count": src_count,
        "target_count": dst_count,
        "missing_count": len(missing_ids),
        "duplicate_count": len(duplicate_ids),
        "mismatch_count": len(mismatch_ids),
        "delayed_count": len(delayed_ids),
        "invalid_count": len(invalid_ids),
    }

    return counts_dict, detected_incidents


def reconcile(batch_id: str) -> Dict[str, Any]:
    """Execute complete reconciliation for a batch across all stage pairs.

    Args:
        batch_id: Batch identifier (e.g. 'B001' or 'batch_001').

    Returns:
        Summary dictionary containing stage counts, incidents, and metrics.
    """
    b_id, dir_name = normalize_batch(batch_id)
    engine = get_engine()
    now_utc = datetime.now(timezone.utc)

    logger.info("Initiating reconciliation for batch '%s'", b_id)

    with engine.begin() as conn:
        # Retrieve schema issues logged during ETL from pipeline_runs
        runs_query = text("""
            SELECT details FROM pipeline_runs
            WHERE batch_id = :b AND task_name = 'etl_load'
            ORDER BY finished_at DESC LIMIT 1;
        """)
        run_record = conn.execute(runs_query, {"b": b_id}).fetchone()
        schema_issues: List[Dict[str, Any]] = []
        if run_record and run_record[0]:
            details = run_record[0]
            if isinstance(details, str):
                details = json.loads(details)
            schema_issues = details.get("schema_issues", [])

        # Idempotent cleanup: delete OPEN incidents and prior reconciliation_results for this batch
        conn.execute(
            text("DELETE FROM incidents WHERE batch_id = :b AND status = 'open';"),
            {"b": b_id},
        )
        conn.execute(
            text("DELETE FROM reconciliation_results WHERE batch_id = :b;"),
            {"b": b_id},
        )

        all_incidents: List[Dict[str, Any]] = []
        all_stage_counts: Dict[str, Dict[str, int]] = {}

        for src_stage, dst_stage in STAGE_PAIRS:
            counts, incidents = reconcile_stage_pair(
                conn=conn,
                batch_id=b_id,
                src_stage=src_stage,
                dst_stage=dst_stage,
                schema_issues=schema_issues,
            )
            all_stage_counts[dst_stage] = counts
            all_incidents.extend(incidents)

            # Insert reconciliation_results
            conn.execute(
                text("""
                    INSERT INTO reconciliation_results (
                        batch_id, stage, source_count, target_count,
                        missing_count, duplicate_count, mismatch_count,
                        delayed_count, invalid_count, created_at
                    ) VALUES (
                        :batch_id, :stage, :source_count, :target_count,
                        :missing_count, :duplicate_count, :mismatch_count,
                        :delayed_count, :invalid_count, :created_at
                    );
                """),
                {
                    "batch_id": b_id,
                    "stage": dst_stage,
                    "source_count": counts["source_count"],
                    "target_count": counts["target_count"],
                    "missing_count": counts["missing_count"],
                    "duplicate_count": counts["duplicate_count"],
                    "mismatch_count": counts["mismatch_count"],
                    "delayed_count": counts["delayed_count"],
                    "invalid_count": counts["invalid_count"],
                    "created_at": now_utc,
                },
            )

        # Bulk insert detected incidents (one incident per (batch, stage, type))
        for inc in all_incidents:
            inc_id = f"INC_{b_id}_{inc['stage']}_{inc['type']}"
            conn.execute(
                text("""
                    INSERT INTO incidents (
                        id, batch_id, stage, type, severity, evidence, detected_at, status
                    ) VALUES (
                        :id, :batch_id, :stage, :type, :severity, :evidence, :detected_at, 'open'
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        severity = EXCLUDED.severity,
                        evidence = EXCLUDED.evidence,
                        detected_at = EXCLUDED.detected_at,
                        status = CASE
                            WHEN incidents.status = 'resolved' THEN 'resolved'
                            WHEN incidents.status = 'investigating' THEN 'investigating'
                            ELSE 'open'
                        END;
                """),
                {
                    "id": inc_id,
                    "batch_id": b_id,
                    "stage": inc["stage"],
                    "type": inc["type"],
                    "severity": inc["severity"],
                    "evidence": json.dumps(inc["evidence"]),
                    "detected_at": now_utc,
                },
            )

        # Update batch status to 'reconciled'
        conn.execute(
            text("UPDATE batches SET status = 'reconciled', updated_at = NOW() WHERE batch_id = :b;"),
            {"b": b_id},
        )

    # Compute incident summary counts
    incident_counts_by_type: Dict[str, int] = {}
    for inc in all_incidents:
        disc_type = inc["type"]
        incident_counts_by_type[disc_type] = incident_counts_by_type.get(disc_type, 0) + len(inc["evidence"])

    summary = {
        "batch_id": b_id,
        "status": "reconciled",
        "stage_counts": all_stage_counts,
        "incident_counts_by_type": incident_counts_by_type,
        "total_incidents": len(all_incidents),
        "incidents": all_incidents,
    }

    logger.info(
        "Reconciliation finished for batch %s. Total incidents: %d, Discrepancies by type: %s",
        b_id,
        len(all_incidents),
        incident_counts_by_type,
    )

    return summary


def main():
    parser = argparse.ArgumentParser(description="BankFlow Sentinel Reconciliation Engine")
    parser.add_argument("batch", type=str, help="Batch identifier (e.g. 'B001' or 'batch_001')")

    args = parser.parse_args()
    summary = reconcile(args.batch)

    print("=" * 65)
    print(f" RECONCILIATION REPORT: Batch {summary['batch_id']}")
    print("=" * 65)
    print("Stage Reconciliation Metrics:")
    for stage, metrics in summary["stage_counts"].items():
        print(f"\n[Stage: {stage.upper()}]")
        print(f"  Source Count    : {metrics['source_count']:,}")
        print(f"  Target Count    : {metrics['target_count']:,}")
        print(f"  Missing Records : {metrics['missing_count']:,}")
        print(f"  Duplicates      : {metrics['duplicate_count']:,}")
        print(f"  Amount Mismatch : {metrics['mismatch_count']:,}")
        print(f"  Delayed Records : {metrics['delayed_count']:,}")
        print(f"  Invalid Records : {metrics['invalid_count']:,}")

    print("\n" + "-" * 65)
    print("Incident Tickets Raised:")
    for inc in summary["incidents"]:
        count = len(inc["evidence"])
        print(f"  [{inc['severity'].upper():6s}] {inc['stage']:10s} -> {inc['type']:16s}: {count:,} affected txns")

    print("=" * 65)
    print(f"Overall Status: {summary['status'].upper()} | Incidents Created: {summary['total_incidents']}")


if __name__ == "__main__":
    main()
