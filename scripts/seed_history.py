"""BankFlow Sentinel - Historical Batch Seeding Script.

Generates and processes 6 additional synthetic banking transaction batches (B004 to B009)
with varied row sizes, fault distributions, schema anomalies, and distinct timestamps.
Populates PostgreSQL with rich multi-stage data for window functions and OLAP queries.
"""

import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

# Ensure project root is in sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from sqlalchemy import text

from data_gen.generate import generate_batch
from db.connection import get_engine
from etl.load import run_etl
from reconcile.engine import reconcile

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("seed_history")

BATCH_CONFIGS = [
    {"batch_num": 4, "rows": 10000, "seed": 404, "schema_fault": False, "days_ago": 5},
    {"batch_num": 5, "rows": 8000,  "seed": 405, "schema_fault": False, "days_ago": 4},
    {"batch_num": 6, "rows": 5000,  "seed": 406, "schema_fault": True,  "days_ago": 3},  # Schema change
    {"batch_num": 7, "rows": 12000, "seed": 407, "schema_fault": False, "days_ago": 2},
    {"batch_num": 8, "rows": 7500,  "seed": 408, "schema_fault": False, "days_ago": 1},
    {"batch_num": 9, "rows": 10000, "seed": 409, "schema_fault": False, "days_ago": 0},
]


def seed_history():
    """Generate, load, reconcile, and backdate historical batches."""
    logger.info("Starting historical batch data generation (Batches B004..B009)...")
    engine = get_engine()

    results: List[Dict[str, Any]] = []

    for cfg in BATCH_CONFIGS:
        b_num = cfg["batch_num"]
        b_id = f"B{b_num:03d}"
        dir_name = f"batch_{b_num:03d}"
        days_ago = cfg["days_ago"]

        logger.info(
            "--- Processing %s: %d rows (seed=%d, schema_fault=%s, date=-%dd) ---",
            b_id,
            cfg["rows"],
            cfg["seed"],
            cfg["schema_fault"],
            days_ago,
        )

        # 1. Generate data
        gen_summary = generate_batch(
            batch=b_num,
            rows=cfg["rows"],
            seed=cfg["seed"],
            schema_fault=cfg["schema_fault"],
            output_dir="data",
        )

        # 2. Ingest via ETL
        etl_summary = run_etl(batch_id=b_id)

        # 3. Reconcile
        rec_summary = reconcile(batch_id=b_id)

        # 4. Backdate batch and incidents to establish distinct historical calendar dates
        past_date = datetime.now(timezone.utc) - timedelta(days=days_ago)
        with engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE batches 
                    SET created_at = :dt, updated_at = :dt 
                    WHERE batch_id = :b;
                    
                    UPDATE incidents 
                    SET detected_at = :dt 
                    WHERE batch_id = :b;
                    
                    UPDATE reconciliation_results 
                    SET created_at = :dt 
                    WHERE batch_id = :b;
                """),
                {"b": b_id, "dt": past_date},
            )

        results.append({
            "batch_id": b_id,
            "date": past_date.strftime("%Y-%m-%d"),
            "rows": gen_summary["source_count"],
            "payment_rows": gen_summary["payment_count"],
            "settlement_rows": gen_summary["settlement_count"],
            "incidents": rec_summary["total_incidents"],
            "schema_fault": cfg["schema_fault"],
        })

    print("\n" + "=" * 80)
    print(" HISTORICAL BATCH SEEDING COMPLETE")
    print("=" * 80)
    print(f"{'Batch ID':<10} | {'Date':<12} | {'Source Rows':<12} | {'Payment':<10} | {'Settlement':<10} | {'Incidents':<10} | {'Schema Fault'}")
    print("-" * 80)
    for r in results:
        print(f"{r['batch_id']:<10} | {r['date']:<12} | {r['rows']:<12,d} | {r['payment_rows']:<10,d} | {r['settlement_rows']:<10,d} | {r['incidents']:<10} | {str(r['schema_fault'])}")
    print("=" * 80)


if __name__ == "__main__":
    seed_history()
