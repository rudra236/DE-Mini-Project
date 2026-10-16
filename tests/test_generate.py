"""BankFlow Sentinel - Data Generation Unit and Integration Tests.

Validates:
1. Determinism: Same seed produces byte-identical files.
2. Exact Row Counts: Source 10,000 / Payment 9,850 / Settlement 9,700 rows.
3. Fast 100-row mode behavior.
4. Mutually exclusive faults per stage (no fault overlap per txn).
5. Ground truth file matches actual file differences.
6. Schema fault injection and ground truth recording.
7. Batch ID and directory naming conversion contracts.
"""

import csv
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pytest

from data_gen.generate import generate_batch
from data_gen.utils import (
    batch_id_to_dir_name,
    batch_num_to_batch_id,
    dir_name_to_batch_id,
    normalize_batch,
)


def hash_file(file_path: Path) -> str:
    """Return SHA256 hash of a file's contents."""
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(8192):
            hasher.update(chunk)
    return hasher.hexdigest()


def read_csv_as_dicts(file_path: Path) -> List[Dict[str, str]]:
    """Read a CSV file into a list of row dictionaries."""
    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def test_batch_id_conversions():
    """Verify bidirectional conversion contracts between batch IDs and directory names."""
    assert batch_num_to_batch_id(1) == "B001"
    assert batch_num_to_batch_id(42) == "B042"

    assert batch_id_to_dir_name("B001") == "batch_001"
    assert batch_id_to_dir_name("B042") == "batch_042"

    assert dir_name_to_batch_id("batch_001") == "B001"
    assert dir_name_to_batch_id("batch_042") == "B042"

    assert normalize_batch(1) == ("B001", "batch_001")
    assert normalize_batch("B001") == ("B001", "batch_001")
    assert normalize_batch("batch_001") == ("B001", "batch_001")

    with pytest.raises(ValueError):
        batch_id_to_dir_name("invalid_id")

    with pytest.raises(ValueError):
        dir_name_to_batch_id("invalid_dir")


def test_determinism(tmp_path: Path):
    """Verify that identical seeds produce byte-identical files across all stages."""
    dir1 = tmp_path / "run1"
    dir2 = tmp_path / "run2"

    summary1 = generate_batch(batch=1, rows=200, seed=123, output_dir=dir1)
    summary2 = generate_batch(batch=1, rows=200, seed=123, output_dir=dir2)

    files_to_check = [
        "batch_001_source.csv",
        "batch_001_payment.csv",
        "batch_001_settlement.csv",
        "batch_001_ground_truth.csv",
    ]

    for fname in files_to_check:
        f1 = Path(summary1["source_file"]).parent / fname
        f2 = Path(summary2["source_file"]).parent / fname
        assert hash_file(f1) == hash_file(f2), f"File {fname} is not byte-identical across runs!"


def test_row_counts_10000(tmp_path: Path):
    """Verify default 10,000 batch hits exact target counts: 10,000 / 9,850 / 9,700."""
    summary = generate_batch(batch=1, rows=10000, seed=42, output_dir=tmp_path)

    assert summary["source_count"] == 10000
    assert summary["payment_count"] == 9850
    assert summary["settlement_count"] == 9700

    source_rows = read_csv_as_dicts(Path(summary["source_file"]))
    payment_rows = read_csv_as_dicts(Path(summary["payment_file"]))
    settlement_rows = read_csv_as_dicts(Path(summary["settlement_file"]))

    assert len(source_rows) == 10000
    assert len(payment_rows) == 9850
    assert len(settlement_rows) == 9700


def test_fast_mode_100_rows(tmp_path: Path):
    """Verify fast 100-row mode behavior."""
    summary = generate_batch(batch=2, rows=100, seed=7, output_dir=tmp_path)

    assert summary["source_count"] == 100
    assert summary["payment_count"] == 98
    assert summary["settlement_count"] == 97

    source_rows = read_csv_as_dicts(Path(summary["source_file"]))
    payment_rows = read_csv_as_dicts(Path(summary["payment_file"]))
    settlement_rows = read_csv_as_dicts(Path(summary["settlement_file"]))

    assert len(source_rows) == 100
    assert len(payment_rows) == 98
    assert len(settlement_rows) == 97


def test_no_fault_overlap_per_stage(tmp_path: Path):
    """Verify a transaction receives at most one fault per stage."""
    summary = generate_batch(batch=1, rows=1000, seed=99, output_dir=tmp_path)
    gt_rows = read_csv_as_dicts(Path(summary["ground_truth_file"]))

    for stage in ["payment", "settlement"]:
        stage_txns = [r["txn_id"] for r in gt_rows if r["stage"] == stage and r["txn_id"] != "*"]
        assert len(stage_txns) == len(set(stage_txns)), f"Found duplicate fault assignments in stage '{stage}'!"


def test_ground_truth_matches_file_differences(tmp_path: Path):
    """Verify ground truth matches actual file differences between adjacent stages."""
    summary = generate_batch(batch=1, rows=500, seed=42, output_dir=tmp_path)

    source_rows = read_csv_as_dicts(Path(summary["source_file"]))
    payment_rows = read_csv_as_dicts(Path(summary["payment_file"]))
    settlement_rows = read_csv_as_dicts(Path(summary["settlement_file"]))
    gt_rows = read_csv_as_dicts(Path(summary["ground_truth_file"]))

    # Index by txn_id
    src_map = {r["txn_id"]: r for r in source_rows}
    pay_map: Dict[str, List[Dict[str, str]]] = {}
    for r in payment_rows:
        pay_map.setdefault(r["txn_id"], []).append(r)

    settle_map: Dict[str, List[Dict[str, str]]] = {}
    for r in settlement_rows:
        settle_map.setdefault(r["txn_id"], []).append(r)

    for gt in gt_rows:
        stage = gt["stage"]
        fault_type = gt["type"]
        txn_id = gt["txn_id"]

        if fault_type == "schema_change":
            continue

        if stage == "payment":
            if fault_type == "missing_record":
                assert txn_id in src_map
                assert txn_id not in pay_map, f"Txn {txn_id} marked as missing in payment was found!"
            elif fault_type == "duplicate":
                assert len(pay_map[txn_id]) > 1, f"Txn {txn_id} marked duplicate had count <= 1 in payment!"
            elif fault_type == "amount_mismatch":
                src_amt = float(src_map[txn_id]["amount"])
                pay_amt = float(pay_map[txn_id][0]["amount"])
                assert src_amt != pay_amt, f"Txn {txn_id} amounts match ({src_amt}) despite mismatch fault!"
            elif fault_type == "delayed":
                src_ts = datetime.fromisoformat(src_map[txn_id]["timestamp"].replace("Z", "+00:00"))
                pay_ts = datetime.fromisoformat(pay_map[txn_id][0]["timestamp"].replace("Z", "+00:00"))
                delay_sec = (pay_ts - src_ts).total_seconds()
                assert delay_sec > 1800, f"Txn {txn_id} delay is {delay_sec}s, expected > 1800s (30m)!"
            elif fault_type == "invalid_record":
                raw_amt = pay_map[txn_id][0]["amount"]
                is_invalid = (raw_amt == "") or (float(raw_amt) < 0)
                assert is_invalid, f"Txn {txn_id} amount '{raw_amt}' was expected to be null or negative!"

        elif stage == "settlement":
            if fault_type == "missing_record":
                assert txn_id in pay_map
                assert txn_id not in settle_map, f"Txn {txn_id} marked missing in settlement was found!"
            elif fault_type == "duplicate":
                assert len(settle_map[txn_id]) > 1, f"Txn {txn_id} marked duplicate had count <= 1 in settlement!"
            elif fault_type == "amount_mismatch":
                pay_amt = float(pay_map[txn_id][0]["amount"])
                settle_amt = float(settle_map[txn_id][0]["amount"])
                assert pay_amt != settle_amt, f"Txn {txn_id} amounts match in settlement!"
            elif fault_type == "delayed":
                pay_ts = datetime.fromisoformat(pay_map[txn_id][0]["timestamp"].replace("Z", "+00:00"))
                settle_ts = datetime.fromisoformat(settle_map[txn_id][0]["timestamp"].replace("Z", "+00:00"))
                delay_sec = (settle_ts - pay_ts).total_seconds()
                assert delay_sec > 1800, f"Txn {txn_id} delay is {delay_sec}s in settlement, expected > 1800s!"
            elif fault_type == "invalid_record":
                raw_amt = settle_map[txn_id][0]["amount"]
                is_invalid = (raw_amt == "") or (float(raw_amt) < 0)
                assert is_invalid, f"Txn {txn_id} amount in settlement was expected to be null or negative!"


def test_schema_change_fault(tmp_path: Path):
    """Verify schema fault injects schema alteration and logs schema_change in ground truth."""
    summary = generate_batch(batch=3, rows=50, seed=11, schema_fault=True, output_dir=tmp_path)
    gt_rows = read_csv_as_dicts(Path(summary["ground_truth_file"]))

    schema_faults = [r for r in gt_rows if r["type"] == "schema_change"]
    assert len(schema_faults) == 1
    assert schema_faults[0]["stage"] == "payment"
    assert schema_faults[0]["txn_id"] == "*"

    payment_rows = read_csv_as_dicts(Path(summary["payment_file"]))
    assert "payment_status" in payment_rows[0]
    assert "status" not in payment_rows[0]
