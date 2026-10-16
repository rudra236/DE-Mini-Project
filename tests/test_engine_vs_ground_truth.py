"""BankFlow Sentinel - Fault Injection vs Engine Verification Test Suite.

MOST IMPORTANT TEST IN THE PROJECT:
Loads ground truth CSV (injected faults), executes reconciliation engine,
and compares sets of (stage, type, txn_id) detected vs injected.
Asserts 100% precision and 100% recall (injected == detected EXACTLY).
Generates docs/fault_injection_report.md for viva evaluation evidence.
"""

import csv
import json
from pathlib import Path
from typing import Any, Dict, Set, Tuple

from sqlalchemy import text

from data_gen.generate import generate_batch
from data_gen.utils import normalize_batch
from db.connection import get_engine
from etl.load import run_etl
from reconcile.engine import reconcile


def load_ground_truth_set(batch_id: str, data_dir: str = "data") -> Set[Tuple[str, str, str]]:
    """Load ground truth CSV and return set of (stage, type, txn_id)."""
    b_id, dir_name = normalize_batch(batch_id)
    gt_path = Path(data_dir) / dir_name / f"{dir_name}_ground_truth.csv"

    if not gt_path.exists():
        raise FileNotFoundError(f"Ground truth file not found: {gt_path}")

    gt_set = set()
    with open(gt_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            gt_set.add((row["stage"], row["type"], row["txn_id"]))

    return gt_set


def load_detected_set(batch_id: str) -> Set[Tuple[str, str, str]]:
    """Load detected incidents from PostgreSQL incidents table and return set of (stage, type, txn_id)."""
    b_id, _ = normalize_batch(batch_id)
    engine = get_engine()

    detected_set = set()
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT stage, type, evidence FROM incidents WHERE batch_id = :b;"),
            {"b": b_id},
        ).fetchall()

        for stage, disc_type, evidence_raw in rows:
            evidence = evidence_raw
            if isinstance(evidence, str):
                evidence = json.loads(evidence)
            for txn_id in evidence:
                detected_set.add((stage, disc_type, txn_id))

    return detected_set


def compare_and_report(batch_id: str, batch_title: str) -> Dict[str, Any]:
    """Compare ground truth vs engine detection, assert 100% match, and return breakdown."""
    gt_set = load_ground_truth_set(batch_id)
    detected_set = load_detected_set(batch_id)

    tp_set = gt_set & detected_set
    fp_set = detected_set - gt_set
    fn_set = gt_set - detected_set

    precision = len(tp_set) / len(detected_set) if detected_set else 1.0
    recall = len(tp_set) / len(gt_set) if gt_set else 1.0

    # Group counts by (stage, type)
    stage_type_keys = sorted(list({(s, t) for s, t, _ in (gt_set | detected_set)}))
    breakdown = []
    for s, t in stage_type_keys:
        injected = len([x for x in gt_set if x[0] == s and x[1] == t])
        detected = len([x for x in detected_set if x[0] == s and x[1] == t])
        breakdown.append({
            "stage": s,
            "type": t,
            "injected": injected,
            "detected": detected,
            "match": injected == detected,
        })

    # Print clean terminal report
    print("\n" + "=" * 70)
    print(f" GROUND TRUTH EVALUATION: {batch_title} ({batch_id})")
    print("=" * 70)
    print(f"Total Injected Discrepancies : {len(gt_set):,}")
    print(f"Total Detected Discrepancies : {len(detected_set):,}")
    print(f"True Positives (Exact Match) : {len(tp_set):,}")
    print(f"False Positives              : {len(fp_set):,}")
    print(f"False Negatives              : {len(fn_set):,}")
    print(f"Precision                    : {precision * 100:.2f}%")
    print(f"Recall                       : {recall * 100:.2f}%")
    print("-" * 70)
    for b in breakdown:
        status = "MATCH [100%]" if b["match"] else "MISMATCH"
        print(f"  {b['stage']:10s} | {b['type']:16s} | Injected: {b['injected']:4d} | Detected: {b['detected']:4d} | {status}")
    print("=" * 70)

    if fp_set or fn_set:
        print("\nMISMATCH DETAILS:")
        if fp_set:
            print("  False Positives (detected but not in ground truth):", list(fp_set)[:10])
        if fn_set:
            print("  False Negatives (in ground truth but not detected):", list(fn_set)[:10])

    return {
        "batch_id": batch_id,
        "batch_title": batch_title,
        "total_injected": len(gt_set),
        "total_detected": len(detected_set),
        "precision": precision,
        "recall": recall,
        "breakdown": breakdown,
        "fp": fp_set,
        "fn": fn_set,
    }


def write_markdown_report(report_b1: Dict[str, Any], report_b2: Dict[str, Any], output_path: Path):
    """Write comprehensive verification evidence report to docs/fault_injection_report.md."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("# BankFlow Sentinel - Fault Injection & Detection Verification Report\n\n")
        f.write("> **Evaluation Evidence**: Proves that the BankFlow Sentinel reconciliation engine detects 100% of injected data faults across all processing stages with **Zero False Positives** and **Zero False Negatives**.\n\n")

        f.write("## 1. Executive Summary\n\n")
        f.write("| Batch | Description | Injected Faults | Detected Faults | Precision | Recall | Result |\n")
        f.write("|---|---|---|---|---|---|---|\n")
        f.write(f"| **{report_b1['batch_id']}** | {report_b1['batch_title']} | {report_b1['total_injected']:,} | {report_b1['total_detected']:,} | **{report_b1['precision']*100:.2f}%** | **{report_b1['recall']*100:.2f}%** | 🟢 **100% MATCH** |\n")
        f.write(f"| **{report_b2['batch_id']}** | {report_b2['batch_title']} | {report_b2['total_injected']:,} | {report_b2['total_detected']:,} | **{report_b2['precision']*100:.2f}%** | **{report_b2['recall']*100:.2f}%** | 🟢 **100% MATCH** |\n\n")

        for report in [report_b1, report_b2]:
            f.write(f"## 2. Detailed Breakdown: {report['batch_title']} (`{report['batch_id']}`)\n\n")
            f.write("| Stage | Discrepancy Type | Injected | Detected | Accuracy | Severity | Status |\n")
            f.write("|---|---|---|---|---|---|---|\n")
            for b in report["breakdown"]:
                f.write(f"| `{b['stage']}` | `{b['type']}` | {b['injected']} | {b['detected']} | 100.0% | `{SEVERITY_MAP.get(b['type'], 'medium')}` | ✅ Verified |\n")
            f.write("\n")

        f.write("## 3. Verification Methodology\n\n")
        f.write("1. **Data Generation & Fault Injection** (`data_gen/`): Clean banking transactions are synthesized and faults are injected stage-by-stage with exact mathematical tracking.\n")
        f.write("2. **Ground Truth Audit Trail**: Every injected fault is logged to `batch_NNN_ground_truth.csv` as the gold standard.\n")
        f.write("3. **ETL Ingestion** (`etl/`): Normalized tables are populated into PostgreSQL 16.\n")
        f.write("4. **Adjacent-Stage SQL Reconciliation** (`reconcile/engine.py`): Pure parameterized relational SQL queries detect discrepancies between adjacent stages.\n")
        f.write("5. **Automated Assertion**: Ground truth tuples `(stage, type, txn_id)` are compared set-for-set with incident evidence in PostgreSQL.\n")


def test_reconciliation_against_ground_truth():
    """Execute end-to-end reconciliation on 10k batch and 100-row batch, asserting 100% exact match."""
    # 1. Setup & Ingest Batch 1 (10,000 rows)
    generate_batch(batch=1, rows=10000, seed=42)
    run_etl("batch_001")
    reconcile("batch_001")

    # 2. Setup & Ingest Batch 2 (100 rows with schema fault)
    generate_batch(batch=2, rows=100, seed=7, schema_fault=True)
    run_etl("batch_002")
    reconcile("batch_002")

    # 3. Compare Batch 1
    rep1 = compare_and_report("B001", "Production 10,000-Row Batch")
    assert rep1["precision"] == 1.0, f"Precision is {rep1['precision']}, expected 1.0"
    assert rep1["recall"] == 1.0, f"Recall is {rep1['recall']}, expected 1.0"
    assert len(rep1["fp"]) == 0, f"Unexpected false positives: {rep1['fp']}"
    assert len(rep1["fn"]) == 0, f"Uncaught false negatives: {rep1['fn']}"

    # 4. Compare Batch 2
    rep2 = compare_and_report("B002", "100-Row Batch with Schema Fault")
    assert rep2["precision"] == 1.0, f"Precision is {rep2['precision']}, expected 1.0"
    assert rep2["recall"] == 1.0, f"Recall is {rep2['recall']}, expected 1.0"
    assert len(rep2["fp"]) == 0, f"Unexpected false positives: {rep2['fp']}"
    assert len(rep2["fn"]) == 0, f"Uncaught false negatives: {rep2['fn']}"

    # 5. Save viva markdown report
    report_file = Path("docs/fault_injection_report.md")
    write_markdown_report(rep1, rep2, report_file)
    assert report_file.exists(), "docs/fault_injection_report.md was not written!"
    print(f"\nReport successfully saved to: {report_file}")


SEVERITY_MAP = {
    "missing_record": "high",
    "amount_mismatch": "high",
    "schema_change": "high",
    "duplicate": "medium",
    "invalid_record": "medium",
    "delayed": "low",
}
