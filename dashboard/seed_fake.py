"""
BankFlow Sentinel - Fake Data Seeder (dashboard/seed_fake.py)
Optional standalone seeder for UI testing without running the full ingestion pipeline.
"""

import random
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from dashboard.db import get_engine


def seed_fake_data():
    engine = get_engine()
    now = datetime.now(timezone.utc)
    print("Seeding synthetic operational telemetry into PostgreSQL...")

    with engine.begin() as conn:
        # Seed 3 demo batches if not present
        demo_batches = [
            ("B090", "data/batch_090/batch_090_source.csv", 10000, "reconciled", now - timedelta(hours=3)),
            ("B091", "data/batch_091/batch_091_source.csv", 10000, "reconciled", now - timedelta(hours=2)),
            ("B092", "data/batch_092/batch_092_source.csv", 1000, "reconciled", now - timedelta(minutes=45))
        ]

        for b_id, path, rows, status, created_at in demo_batches:
            conn.execute(text("""
                INSERT INTO batches (batch_id, file_path, row_count, status, created_at, updated_at)
                VALUES (:b, :p, :r, :s, :c, :u)
                ON CONFLICT (batch_id) DO UPDATE SET status = :s
            """), {"b": b_id, "p": path, "r": rows, "s": status, "c": created_at, "u": now})

            # Seed reconciliation results
            conn.execute(text("""
                INSERT INTO reconciliation_results
                (batch_id, stage, source_count, target_count, missing_count, duplicate_count, mismatch_count, delayed_count, invalid_count, created_at)
                VALUES
                (:b, 'payment', :r, :r - 150, 150, 50, 25, 10, 5, :c),
                (:b, 'settlement', :r - 150, :r - 300, 150, 50, 25, 8, 3, :c)
                ON CONFLICT DO NOTHING
            """), {"b": b_id, "r": rows, "c": created_at})

            # Seed pipeline runs
            for task, dur in [("ingest", 0.8), ("validate", 1.4), ("reconcile", 2.1), ("report", 0.5)]:
                conn.execute(text("""
                    INSERT INTO pipeline_runs (run_id, batch_id, task_name, status, started_at, finished_at, retries, details)
                    VALUES (:run, :b, :t, 'completed', :s, :f, 0, '{}')
                    ON CONFLICT (run_id) DO NOTHING
                """), {
                    "run": f"run_{b_id}_{task}",
                    "b": b_id,
                    "t": task,
                    "s": created_at,
                    "f": created_at + timedelta(seconds=dur)
                })

            # Seed incidents
            fault_types = [
                ("missing_record", "payment", "high"),
                ("amount_mismatch", "payment", "high"),
                ("duplicate", "settlement", "medium"),
                ("delayed", "settlement", "low")
            ]
            for f_type, stage, sev in fault_types:
                inc_id = f"inc_{b_id}_{stage}_{f_type}"
                evidence = [f"T{random.randint(100000, 999999)}" for _ in range(12)]
                conn.execute(text("""
                    INSERT INTO incidents (id, batch_id, stage, type, severity, evidence, detected_at, status, notes)
                    VALUES (:id, :b, :s, :t, :sev, :ev::jsonb, :d, 'open', 'Automated anomaly alert')
                    ON CONFLICT (id) DO NOTHING
                """), {
                    "id": inc_id,
                    "b": b_id,
                    "s": stage,
                    "t": f_type,
                    "sev": sev,
                    "ev": str(evidence).replace("'", '"'),
                    "d": created_at
                })

    print("Fake data seeded successfully!")


if __name__ == "__main__":
    seed_fake_data()
