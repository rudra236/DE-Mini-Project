"""BankFlow Sentinel - Fault Injection Engine.

Constructs downstream stages (payment from source, settlement from payment)
by injecting synthetic processing faults according to docs/contracts.md.

=============================================================================
FAULT ARITHMETIC DESIGN:
-----------------------------------------------------------------------------
Let N_src = len(source). In default 10k demo: N_src = 10,000 rows.
Target row counts:
  - source:     10,000 rows
  - payment:     9,850 rows (net change: -150)
  - settlement:  9,700 rows (net change relative to payment: -150)

For any stage (payment from source, settlement from payment):
  N_downstream = N_upstream - missing_count + duplicate_count

Net row reduction = missing_count - duplicate_count = 150.
Default 10,000-row configuration:
  - missing_count          = 200 (rows omitted from downstream stage)
  - duplicate_count        =  50 (rows duplicated: +1 duplicate row appended)
  Net row count change     = -200 + 50 = -150 rows.
  - amount_mismatch_count  =  50 (amount altered, row count unchanged)
  - delayed_count          =  50 (timestamp shifted +2..6h, row count unchanged)
  - invalid_record_count   =  50 (null or negative amount, row count unchanged)
  Total disjoint faults    = 400 unique transactions per stage.

A txn receives at most ONE fault per stage, ensuring unambiguous ground truth.

For 100-row test mode:
  - payment:    missing=3, duplicate=1, mismatch=1, delayed=1, invalid=1 (100 - 3 + 1 = 98)
  - settlement: missing=2, duplicate=1, mismatch=1, delayed=1, invalid=1 (98 - 2 + 1 = 97)
=============================================================================
"""

import copy
import logging
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class StageFaultConfig:
    """Configuration for faults injected in a single downstream stage."""

    missing_count: int = 200
    duplicate_count: int = 50
    amount_mismatch_count: int = 50
    delayed_count: int = 50
    invalid_record_count: int = 50
    schema_change: bool = False

    @classmethod
    def default_for_rows(cls, row_count: int, stage: str, schema_change: bool = False) -> "StageFaultConfig":
        """Compute fault counts to hit exact targets based on row count."""
        if row_count >= 10000:
            missing = 200 if stage == "payment" else 150
            return cls(
                missing_count=missing,
                duplicate_count=50,
                amount_mismatch_count=50,
                delayed_count=50,
                invalid_record_count=50,
                schema_change=schema_change,
            )
        elif row_count <= 100:
            if stage == "payment":
                return cls(
                    missing_count=3,
                    duplicate_count=1,
                    amount_mismatch_count=1,
                    delayed_count=1,
                    invalid_record_count=1,
                    schema_change=schema_change,
                )
            else:  # settlement
                return cls(
                    missing_count=1,
                    duplicate_count=1,
                    amount_mismatch_count=1,
                    delayed_count=1,
                    invalid_record_count=1,
                    schema_change=schema_change,
                )
        else:
            scale = row_count / 10000.0
            dup = max(1, int(round(50 * scale)))
            # Net drop should be approx 150 * scale
            net_drop = max(1, int(round(150 * scale)))
            missing = net_drop + dup
            return cls(
                missing_count=missing,
                duplicate_count=dup,
                amount_mismatch_count=max(1, int(round(50 * scale))),
                delayed_count=max(1, int(round(50 * scale))),
                invalid_record_count=max(1, int(round(50 * scale))),
                schema_change=schema_change,
            )


def shift_timestamp(ts_str: str, min_hours: float = 2.0, max_hours: float = 6.0, rng: Optional[random.Random] = None) -> str:
    """Shift an ISO-8601 timestamp forward by 2 to 6 hours (contract: > 30 minutes)."""
    r = rng or random
    # Support 'Z' or '+00:00'
    normalized = ts_str.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    hours_to_add = r.uniform(min_hours, max_hours)
    shifted = dt + timedelta(hours=hours_to_add)
    return shifted.strftime("%Y-%m-%dT%H:%M:%SZ")


def inject_stage_faults(
    upstream_records: List[Dict[str, Any]],
    stage_name: str,
    batch_id: str,
    config: StageFaultConfig,
    rng: random.Random,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, str]]]:
    """Inject faults into upstream records to generate downstream stage records.

    Args:
        upstream_records: List of transaction dictionaries from preceding stage.
        stage_name: Target downstream stage name ('payment' or 'settlement').
        batch_id: Normalized database batch identifier ('B001').
        config: StageFaultConfig specifying counts for each discrepancy type.
        rng: Deterministic random.Random instance.

    Returns:
        (downstream_records, ground_truth_entries)
    """
    total_available = len(upstream_records)
    total_faults_requested = (
        config.missing_count
        + config.duplicate_count
        + config.amount_mismatch_count
        + config.delayed_count
        + config.invalid_record_count
    )

    if total_faults_requested > total_available:
        raise ValueError(
            f"Requested {total_faults_requested} faults from {total_available} available records in stage '{stage_name}'."
        )

    # 1. Deterministically shuffle indices to assign mutually exclusive faults
    all_indices = list(range(total_available))
    rng.shuffle(all_indices)

    idx_cursor = 0

    def take_indices(n: int) -> List[int]:
        nonlocal idx_cursor
        selected = all_indices[idx_cursor : idx_cursor + n]
        idx_cursor += n
        return selected

    missing_idx = set(take_indices(config.missing_count))
    duplicate_idx = set(take_indices(config.duplicate_count))
    mismatch_idx = set(take_indices(config.amount_mismatch_count))
    delayed_idx = set(take_indices(config.delayed_count))
    invalid_idx = set(take_indices(config.invalid_record_count))

    downstream_records: List[Dict[str, Any]] = []
    ground_truth: List[Dict[str, str]] = []
    duplicate_rows_to_append: List[Dict[str, Any]] = []

    # 2. Process records in order
    for idx, orig_row in enumerate(upstream_records):
        txn_id = orig_row["txn_id"]

        # Case A: Missing record (dropped from downstream)
        if idx in missing_idx:
            ground_truth.append({
                "batch_id": batch_id,
                "stage": stage_name,
                "type": "missing_record",
                "txn_id": txn_id,
            })
            continue

        row_copy = copy.deepcopy(orig_row)

        # Case B: Duplicate record
        if idx in duplicate_idx:
            ground_truth.append({
                "batch_id": batch_id,
                "stage": stage_name,
                "type": "duplicate",
                "txn_id": txn_id,
            })
            # Create a duplicate copy to insert later
            duplicate_copy = copy.deepcopy(row_copy)
            duplicate_rows_to_append.append(duplicate_copy)

        # Case C: Amount mismatch
        elif idx in mismatch_idx:
            orig_amount = float(row_copy["amount"])
            # Adjust amount by multiplier + delta
            delta = rng.choice([-15.00, 25.50, 50.00, 100.25, 250.00])
            new_amount = round(max(1.00, orig_amount + delta), 2)
            if new_amount == orig_amount:
                new_amount = round(orig_amount + 12.34, 2)
            row_copy["amount"] = f"{new_amount:.2f}"
            ground_truth.append({
                "batch_id": batch_id,
                "stage": stage_name,
                "type": "amount_mismatch",
                "txn_id": txn_id,
            })

        # Case D: Delayed record
        elif idx in delayed_idx:
            orig_ts = row_copy["timestamp"]
            new_ts = shift_timestamp(orig_ts, min_hours=2.0, max_hours=6.0, rng=rng)
            row_copy["timestamp"] = new_ts
            ground_truth.append({
                "batch_id": batch_id,
                "stage": stage_name,
                "type": "delayed",
                "txn_id": txn_id,
            })

        # Case E: Invalid record (null or negative amount)
        elif idx in invalid_idx:
            # Alternating between null (empty string) and negative amount
            if rng.random() < 0.5:
                row_copy["amount"] = ""
            else:
                orig_amount = float(row_copy["amount"])
                row_copy["amount"] = f"{-abs(orig_amount):.2f}"
            ground_truth.append({
                "batch_id": batch_id,
                "stage": stage_name,
                "type": "invalid_record",
                "txn_id": txn_id,
            })

        # Ensure amount is formatted as two decimal places string if not empty
        if row_copy.get("amount") and not isinstance(row_copy["amount"], str):
            row_copy["amount"] = f"{float(row_copy['amount']):.2f}"

        downstream_records.append(row_copy)

    # 3. Append duplicates into the downstream stream
    downstream_records.extend(duplicate_rows_to_append)

    # 4. Handle schema change if enabled (remove or rename column)
    if config.schema_change:
        ground_truth.append({
            "batch_id": batch_id,
            "stage": stage_name,
            "type": "schema_change",
            "txn_id": "*",
        })
        # Rename 'status' to 'payment_status'
        for row in downstream_records:
            if "status" in row:
                row["payment_status"] = row.pop("status")

    logger.debug(
        "Stage '%s' faults injected: missing=%d, duplicates=%d, mismatch=%d, delayed=%d, invalid=%d, schema=%s. "
        "Upstream: %d -> Downstream: %d",
        stage_name,
        len(missing_idx),
        len(duplicate_idx),
        len(mismatch_idx),
        len(delayed_idx),
        len(invalid_idx),
        config.schema_change,
        total_available,
        len(downstream_records),
    )

    return downstream_records, ground_truth
