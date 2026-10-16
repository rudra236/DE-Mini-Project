import os


def test_delta_demo_output_exists_and_valid():
    output_path = os.path.join(os.path.dirname(__file__), "..", "docs", "delta_demo_output.txt")
    assert os.path.exists(output_path), f"Output file missing at {output_path}"

    with open(output_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Verify all 6 required sections exist
    assert "SECTION 1: INGESTION & DELTA TABLE CREATION (VERSION 0)" in content
    assert "SECTION 2: DELTA IN-PLACE ROW UPDATE (VERSION 1)" in content
    assert "SECTION 3: DELTA TABLE HISTORY & AUDIT TRAIL" in content
    assert "SECTION 4: TIME TRAVEL QUERY (VERSION 0 vs LATEST VERSION)" in content
    assert "SECTION 5: SCHEMA ENFORCEMENT & REJECTION" in content
    assert "SECTION 6: SCHEMA EVOLUTION (mergeSchema = true)" in content

    # Verify time travel differentiation
    assert "Time travel confirms: Version 0 mismatch_count" in content
    assert "!= Latest mismatch_count" in content

    # Verify rejection
    assert "REJECTED:" in content
    assert "Delta Lake schema enforcement blocked the write" in content

    # Verify Delta table directories on disk
    table_path = os.path.join(os.path.dirname(__file__), "..", "lakehouse", "tables", "reconciliation_results")
    assert os.path.exists(table_path), f"Delta table missing at {table_path}"
    assert os.path.exists(os.path.join(table_path, "_delta_log")), "_delta_log directory missing"
