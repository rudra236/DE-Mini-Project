"""BankFlow Sentinel - Synthetic Banking Data Generator.

Generates realistic synthetic banking transactions and downstream stage files
with injected faults according to docs/contracts.md:
  - Source:  Clean generated transactions.
  - Payment: Source with injected faults (missing, duplicates, mismatches, delays, invalid, schema).
  - Settlement: Payment with injected faults.
  - Ground Truth: Discrepancy log matching every injected fault.
"""

import argparse
import csv
import logging
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import numpy as np
from faker import Faker

from data_gen.inject_faults import StageFaultConfig, inject_stage_faults
from data_gen.utils import normalize_batch

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("data_gen.generate")

CSV_COLUMNS = ["txn_id", "account_id", "amount", "currency", "timestamp", "status"]
GROUND_TRUTH_COLUMNS = ["batch_id", "stage", "type", "txn_id"]


def generate_clean_source(
    rows: int,
    seed: int,
    start_date: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """Generate deterministic clean source transactions.

    Args:
        rows: Number of records to generate.
        seed: Random seed for determinism.
        start_date: Base UTC date (defaults to 2026-10-10 00:00:00 UTC).

    Returns:
        List of clean transaction dictionaries matching CSV_COLUMNS.
    """
    rng = random.Random(seed)
    np_rng = np.random.RandomState(seed)
    fake = Faker()
    fake.seed_instance(seed)

    # 1. Accounts pool: ~2000 distinct account IDs (A00001 .. A02000)
    account_pool = [f"A{i:05d}" for i in range(1, 2001)]

    # 2. Amounts: Lognormal distribution clipped to 1.00 - 50,000.00
    # Mean ~ 7.5, sigma ~ 1.2 generates values largely between 100 and 15,000 with a realistic tail
    raw_amounts = np_rng.lognormal(mean=7.5, sigma=1.2, size=rows)
    clipped_amounts = np.clip(raw_amounts, 1.00, 50000.00)

    # 3. Currency weights: INR 90%, USD 6%, EUR 4%
    currencies = ["INR", "USD", "EUR"]
    currency_weights = [0.90, 0.06, 0.04]

    # 4. Status weights: COMPLETED 95%, PENDING 5%
    statuses = ["COMPLETED", "PENDING"]
    status_weights = [0.95, 0.05]

    # 5. Timestamps: Uniformly distributed across 24 hours of start_date
    base_time = start_date or datetime(2026, 10, 10, 0, 0, 0, tzinfo=timezone.utc)

    records: List[Dict[str, Any]] = []
    for i in range(rows):
        txn_id = f"T{i + 1:06d}"
        account_id = rng.choice(account_pool)
        amount_val = round(float(clipped_amounts[i]), 2)
        currency_val = rng.choices(currencies, weights=currency_weights, k=1)[0]
        offset_seconds = rng.randint(0, 86399)
        ts_val = (base_time + timedelta(seconds=offset_seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")
        status_val = rng.choices(statuses, weights=status_weights, k=1)[0]

        records.append({
            "txn_id": txn_id,
            "account_id": account_id,
            "amount": f"{amount_val:.2f}",
            "currency": currency_val,
            "timestamp": ts_val,
            "status": status_val,
        })

    return records


def write_csv_file(file_path: Path, records: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    """Write records to a CSV file with deterministic row and column ordering."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)


def generate_batch(
    batch: Union[int, str] = 1,
    rows: int = 10000,
    seed: int = 42,
    schema_fault: bool = False,
    output_dir: Union[str, Path] = "data",
    payment_config: Optional[StageFaultConfig] = None,
    settlement_config: Optional[StageFaultConfig] = None,
) -> Dict[str, Any]:
    """Generate an entire batch dataset across 3 stages and write ground truth.

    Args:
        batch: Batch number (1) or batch id ('B001').
        rows: Number of source rows (default 10,000).
        seed: Random seed for deterministic generation.
        schema_fault: Flag to inject schema fault in payment stage.
        output_dir: Root output directory ('data').
        payment_config: Optional custom StageFaultConfig for payment stage.
        settlement_config: Optional custom StageFaultConfig for settlement stage.

    Returns:
        Summary dict containing counts and file paths.
    """
    batch_id, dir_name = normalize_batch(batch)
    batch_dir = Path(output_dir) / dir_name
    batch_dir.mkdir(parents=True, exist_ok=True)

    rng = random.Random(seed)

    # 1. Clean source generation
    source_records = generate_clean_source(rows=rows, seed=seed)

    # 2. Configure faults per stage
    pay_cfg = payment_config or StageFaultConfig.default_for_rows(
        row_count=rows, stage="payment", schema_change=schema_fault
    )
    settle_cfg = settlement_config or StageFaultConfig.default_for_rows(
        row_count=rows, stage="settlement", schema_change=False
    )

    # 3. Stage 1: payment from source
    payment_records, payment_gt = inject_stage_faults(
        upstream_records=source_records,
        stage_name="payment",
        batch_id=batch_id,
        config=pay_cfg,
        rng=rng,
    )

    import copy
    src_map = {r["txn_id"]: r for r in source_records}
    seen_txns = set()
    settlement_upstream = []
    for r in payment_records:
        tid = r["txn_id"]
        if tid not in seen_txns:
            seen_txns.add(tid)
            row_copy = copy.deepcopy(r)
            if row_copy["amount"] == "" or float(row_copy["amount"]) <= 0:
                row_copy["amount"] = src_map[tid]["amount"]
            if "payment_status" in row_copy and "status" not in row_copy:
                row_copy["status"] = row_copy.pop("payment_status")
            settlement_upstream.append(row_copy)

    # 4. Stage 2: settlement from payment
    settlement_records, settlement_gt = inject_stage_faults(
        upstream_records=settlement_upstream,
        stage_name="settlement",
        batch_id=batch_id,
        config=settle_cfg,
        rng=rng,
    )

    # 5. Combined ground truth
    all_ground_truth = payment_gt + settlement_gt

    # 6. File paths
    source_file = batch_dir / f"{dir_name}_source.csv"
    payment_file = batch_dir / f"{dir_name}_payment.csv"
    settlement_file = batch_dir / f"{dir_name}_settlement.csv"
    gt_file = batch_dir / f"{dir_name}_ground_truth.csv"

    # Determine headers for payment in case schema change renamed or dropped a column
    payment_fields = list(payment_records[0].keys()) if payment_records else CSV_COLUMNS
    settlement_fields = list(settlement_records[0].keys()) if settlement_records else CSV_COLUMNS

    write_csv_file(source_file, source_records, CSV_COLUMNS)
    write_csv_file(payment_file, payment_records, payment_fields)
    write_csv_file(settlement_file, settlement_records, settlement_fields)
    write_csv_file(gt_file, all_ground_truth, GROUND_TRUTH_COLUMNS)

    summary = {
        "batch_id": batch_id,
        "dir_name": dir_name,
        "source_count": len(source_records),
        "payment_count": len(payment_records),
        "settlement_count": len(settlement_records),
        "ground_truth_count": len(all_ground_truth),
        "source_file": str(source_file),
        "payment_file": str(payment_file),
        "settlement_file": str(settlement_file),
        "ground_truth_file": str(gt_file),
    }

    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="BankFlow Sentinel Data Generator")
    parser.add_argument("--batch", type=str, default="1", help="Batch number or ID (e.g. 1 or B001)")
    parser.add_argument("--rows", type=int, default=10000, help="Number of source rows (default: 10000)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for determinism (default: 42)")
    parser.add_argument(
        "--schema-fault",
        action="store_true",
        default=False,
        help="Inject schema fault in payment stage (default: False)",
    )
    parser.add_argument("--output-dir", type=str, default="data", help="Output directory path (default: data)")

    args = parser.parse_args()

    summary = generate_batch(
        batch=args.batch,
        rows=args.rows,
        seed=args.seed,
        schema_fault=args.schema_fault,
        output_dir=args.output_dir,
    )

    # Print required counts format clearly
    print(f"Batch {summary['batch_id']} generated at {summary['dir_name']}/")
    print(
        f"source {summary['source_count']:,} / "
        f"payment {summary['payment_count']:,} / "
        f"settlement {summary['settlement_count']:,} rows"
    )
    print(f"Counts: {summary['source_count']} / {summary['payment_count']} / {summary['settlement_count']}")
    print(f"Ground truth faults injected: {summary['ground_truth_count']}")


if __name__ == "__main__":
    main()
