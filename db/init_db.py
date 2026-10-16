"""
BankFlow Sentinel - Database and Schema Initialization Script
Executes PostgreSQL schema.sql, creates analytical views, and configures MongoDB indexes.
"""

import sys
from pathlib import Path

# Fix Windows console UTF-8 output
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

# Ensure project root is in sys.path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from sqlalchemy import text

from db.connection import get_engine
from db.mongo_setup import init_mongo


def init_all():
    project_root = Path(__file__).resolve().parent.parent
    schema_path = project_root / "db" / "schema.sql"
    queries_path = project_root / "db" / "queries_sql_features.sql"

    print("--- Initializing BankFlow Sentinel Databases ---")

    # 1. PostgreSQL Schema
    print("1. Initializing PostgreSQL tables & indexes...")
    engine = get_engine()
    with engine.begin() as conn:
        with open(schema_path, "r", encoding="utf-8") as f:
            schema_sql = f.read()
        conn.execute(text(schema_sql))

        # Also create the analytical view v_batch_summary
        view_sql = """
        CREATE OR REPLACE VIEW v_batch_summary AS
        SELECT 
            b.batch_id,
            b.created_at::date AS ingest_date,
            b.row_count AS declared_source_rows,
            b.status AS pipeline_status,
            COALESCE(r_pay.source_count, 0) AS payment_source_count,
            COALESCE(r_pay.target_count, 0) AS payment_target_count,
            COALESCE(r_settle.target_count, 0) AS settlement_target_count,
            COALESCE(r_pay.missing_count + r_settle.missing_count, 0) AS total_missing,
            COALESCE(r_pay.mismatch_count + r_settle.mismatch_count, 0) AS total_amount_mismatches,
            COALESCE(r_pay.duplicate_count + r_settle.duplicate_count, 0) AS total_duplicates,
            COALESCE(r_pay.delayed_count + r_settle.delayed_count, 0) AS total_delayed,
            COALESCE(r_pay.invalid_count + r_settle.invalid_count, 0) AS total_invalid,
            COUNT(DISTINCT i.id) AS total_incidents_raised,
            COUNT(DISTINCT CASE WHEN i.status = 'open' THEN i.id END) AS open_incidents,
            COUNT(DISTINCT CASE WHEN i.status = 'resolved' THEN i.id END) AS resolved_incidents
        FROM batches b
        LEFT JOIN reconciliation_results r_pay ON b.batch_id = r_pay.batch_id AND r_pay.stage = 'payment'
        LEFT JOIN reconciliation_results r_settle ON b.batch_id = r_settle.batch_id AND r_settle.stage = 'settlement'
        LEFT JOIN incidents i ON b.batch_id = i.batch_id
        GROUP BY 
            b.batch_id, b.created_at::date, b.row_count, b.status,
            r_pay.source_count, r_pay.target_count, r_settle.target_count,
            r_pay.missing_count, r_settle.missing_count,
            r_pay.mismatch_count, r_settle.mismatch_count,
            r_pay.duplicate_count, r_settle.duplicate_count,
            r_pay.delayed_count, r_settle.delayed_count,
            r_pay.invalid_count, r_settle.invalid_count;
        """
        conn.execute(text(view_sql))
    print("   ✓ PostgreSQL schema & v_batch_summary view ready.")

    # 2. MongoDB Collections & Indexes
    print("2. Initializing MongoDB collections & indexes...")
    try:
        mongo_info = init_mongo()
        print(f"   ✓ MongoDB collections & indexes ready: {list(mongo_info.keys())}")
    except Exception as e:
        print(f"   ⚠️ MongoDB setup warning: {e}")

    print("✓ All databases successfully initialized!\n")

if __name__ == "__main__":
    init_all()
