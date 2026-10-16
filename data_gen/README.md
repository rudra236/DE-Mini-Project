# Data Generation Module (`data_gen/`)

Synthetic banking transaction generation and fault injection engine adhering strictly to [`docs/contracts.md`](../docs/contracts.md).

## How to Run
Standard 10,000-row demo batch:
```bash
python -m data_gen.generate --batch 1 --rows 10000 --seed 42
```

Fast 100-row test batch with schema fault:
```bash
python -m data_gen.generate --batch 2 --rows 100 --seed 7 --schema-fault
```

Run unit & integration test suite:
```bash
python -m pytest tests/test_generate.py -v
```

## Input
CLI flags:
- `--batch`: Batch number or identifier (`1` -> `B001`, `batch_001`). Default: `1`.
- `--rows`: Total source records to generate. Default: `10000`.
- `--seed`: Deterministic pseudo-random seed. Default: `42`.
- `--schema-fault`: Flag to inject column rename in payment stage. Default: `False`.
- `--output-dir`: Base directory for output batches. Default: `data`.

## Output
Files created under `data/batch_NNN/`:
- `batch_NNN_source.csv`: Clean synthetic source transactions (`txn_id, account_id, amount, currency, timestamp, status`).
- `batch_NNN_payment.csv`: Payment stage records with faults injected relative to source.
- `batch_NNN_settlement.csv`: Settlement stage records with faults injected relative to payment.
- `batch_NNN_ground_truth.csv`: Complete discrepancy audit trail (`batch_id, stage, type, txn_id`).

## Fault Arithmetic Design
For default 10,000 records:
- **Source**: 10,000 rows
- **Payment**: 9,850 rows (Source 10,000 - 200 missing + 50 duplicates)
- **Settlement**: 9,700 rows (Payment 9,850 - 200 missing + 50 duplicates)
- Fault types per stage: `missing_record` (200), `duplicate` (50), `amount_mismatch` (50), `delayed` (50), `invalid_record` (50).
- Max 1 fault per transaction per stage ensuring unambiguous ground truth.
