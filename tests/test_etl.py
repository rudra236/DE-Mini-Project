"""BankFlow Sentinel - ETL Unit and Integration Tests.

Validates:
1. ETL Idempotency: Re-running ETL produces identical row counts without duplication.
2. Schema fault tolerance: Missing or unexpected columns load without crashing and log schema issues.
3. Invalid record handling: Null and negative amounts are marked is_valid=False with reasons.
"""

from pathlib import Path

import pytest
from sqlalchemy import text

from data_gen.generate import generate_batch
from db.connection import get_engine
from etl.load import run_etl


@pytest.fixture(scope="module", autouse=True)
def setup_test_batches():
    """Ensure test batches are generated before running ETL tests."""
    # Ensure Batch 1 (10,000 rows) exists
    b1_dir = Path("data/batch_001")
    if not (b1_dir / "batch_001_source.csv").exists():
        generate_batch(batch=1, rows=10000, seed=42)

    # Ensure Batch 2 (100 rows with schema fault) exists
    b2_dir = Path("data/batch_002")
    if not (b2_dir / "batch_002_source.csv").exists():
        generate_batch(batch=2, rows=100, seed=7, schema_fault=True)


def test_etl_idempotency():
    """Verify executing ETL multiple times on the same batch is completely idempotent."""
    engine = get_engine()

    # First ETL execution
    summary1 = run_etl("batch_001")
    assert summary1["stage_counts"]["source"] == 10000
    assert summary1["stage_counts"]["payment"] == 9850
    assert summary1["stage_counts"]["settlement"] == 9700
    assert summary1["total_rows"] == 29550

    with engine.connect() as conn:
        result1 = conn.execute(
            text("SELECT stage, COUNT(*) FROM transactions WHERE batch_id = 'B001' GROUP BY stage ORDER BY stage;")
        ).fetchall()
        counts1 = dict(result1)

    assert counts1 == {"payment": 9850, "settlement": 9700, "source": 10000}

    # Second ETL execution (idempotency check)
    summary2 = run_etl("batch_001")
    assert summary2["total_rows"] == 29550

    with engine.connect() as conn:
        result2 = conn.execute(
            text("SELECT stage, COUNT(*) FROM transactions WHERE batch_id = 'B001' GROUP BY stage ORDER BY stage;")
        ).fetchall()
        counts2 = dict(result2)

        # Check batches table status
        batch_row = conn.execute(
            text("SELECT row_count, status FROM batches WHERE batch_id = 'B001';")
        ).fetchone()

    assert counts2 == counts1
    assert batch_row[0] == 29550
    assert batch_row[1] == "ingested"


def test_etl_schema_fault_tolerance():
    """Verify batch with schema alterations loads without exception and logs issues."""
    engine = get_engine()

    summary = run_etl("batch_002")
    assert summary["total_rows"] == 295
    assert len(summary["schema_issues"]) > 0

    with engine.connect() as conn:
        run_record = conn.execute(
            text("SELECT details FROM pipeline_runs WHERE batch_id = 'B002' ORDER BY started_at DESC LIMIT 1;")
        ).fetchone()

        assert run_record is not None
        details = run_record[0]
        assert "schema_issues" in details
        assert len(details["schema_issues"]) > 0

        # Verify rows were actually loaded into transactions
        tx_count = conn.execute(
            text("SELECT COUNT(*) FROM transactions WHERE batch_id = 'B002';")
        ).scalar()
        assert tx_count == 295


def test_etl_invalid_records_flagged():
    """Verify invalid transactions are flagged with is_valid=False and reasons recorded."""
    engine = get_engine()

    with engine.connect() as conn:
        invalid_rows = conn.execute(
            text("SELECT txn_id, amount, invalid_reason FROM transactions WHERE batch_id = 'B001' AND is_valid = false;")
        ).fetchall()

    assert len(invalid_rows) > 0
    reasons = {row[2] for row in invalid_rows}
    # Check that both null amounts and negative amounts were detected
    has_null = any("NULL_AMOUNT" in r for r in reasons)
    has_negative = any("NEGATIVE" in r for r in reasons)
    assert has_null, "Expected NULL_AMOUNT invalid reasons"
    assert has_negative, "Expected NEGATIVE_OR_ZERO_AMOUNT invalid reasons"
