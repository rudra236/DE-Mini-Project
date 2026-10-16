# Database Module (`db/`)

Relational schema definitions for PostgreSQL 16 according to [`docs/contracts.md`](../docs/contracts.md).

## Schema Tables
1. **`batches`**: Tracks batch lifecycle (`created` -> `ingested` -> `validated` -> `reconciled` -> `failed`).
2. **`transactions`**: Stage-by-stage transaction records (`stage`, `txn_id`, `account_id`, `amount`, `currency`, `ts`, `status`, `is_valid`, `invalid_reason`). No UNIQUE constraint on `txn_id` so duplicates are preserved.
3. **`pipeline_runs`**: Orchestration and task run audit log (`run_id`, `task_name`, `status`, `retries`, `details` JSONB).
4. **`reconciliation_results`**: Stage-by-stage counts, discrepancies, and discrepancy counts.
5. **`incidents`**: Actionable tickets with transaction IDs as evidence and resolution statuses.

## Indexes
- `transactions(batch_id, stage, txn_id)`
- `transactions(txn_id)`
- `pipeline_runs(batch_id)`
- `reconciliation_results(batch_id)`
- `incidents(batch_id)`
- `incidents(status)`
- `incidents(detected_at)`

## How to Initialize / Inspect
```bash
docker compose exec -T postgres psql -U bankflow -d bankflow -c "\dt"
```
