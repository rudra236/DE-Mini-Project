# Reconciliation Engine & Data Quality Module (`reconcile/`)

Adjacent-stage reconciliation engine and data quality profiler detecting data processing discrepancies with 100% precision and recall against synthetic ground truth.

## How to Run

### Reconciliation Engine
```bash
python -m reconcile.engine B001
```
Or programmatically:
```python
from reconcile.engine import reconcile
summary = reconcile("B001")
```

### Data Quality Profiler
```bash
python -m reconcile.quality B001
```

### Ground Truth Verification Test
```bash
python -m pytest tests/test_engine_vs_ground_truth.py -v -s
```

## Reconciliation Checks & Execution Order
Checks are performed on adjacent stage pairs (`source` -> `payment`, `payment` -> `settlement`) using pure parameterized SQL JOINs:
1. **Count Check**: Record upstream vs downstream row counts in `reconciliation_results`.
2. **ID Check (`missing_record`)**: Left join identifying upstream `txn_id`s absent downstream.
3. **Amount Check (`amount_mismatch`)**: Join identifying matching `txn_id`s where amounts differ between valid rows.
4. **Duplicate Check (`duplicate`)**: Grouping identifying `txn_id`s with `COUNT(*) > 1` downstream.
5. **Delay Check (`delayed`)**: Join identifying downstream timestamp shifts greater than 30 minutes (`> INTERVAL '30 minutes'`).
6. **Schema Check (`schema_change`)**: Verifies schema discrepancies logged during ETL in `pipeline_runs.details`.
7. **Invalid Check (`invalid_record`)**: Identifies downstream records flagged with `is_valid = false`.

## Incident Generation & Idempotency Rules
- Exactly one incident per `(batch_id, stage, discrepancy_type)` containing an array of offending `txn_id` evidence.
- Severity levels assigned strictly per [`docs/contracts.md`](../docs/contracts.md):
  - `missing_record`: High
  - `amount_mismatch`: High
  - `schema_change`: High
  - `duplicate`: Medium
  - `invalid_record`: Medium
  - `delayed`: Low
- **Idempotency**: Re-running deletes prior `open` incidents and prior `reconciliation_results` for the batch without modifying `resolved` incidents.
- Updates `batches.status = 'reconciled'`.
