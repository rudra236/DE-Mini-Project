# ETL Module (`etl/`)

Idempotent extraction, cleaning, schema validation, and bulk loading pipeline into PostgreSQL.

## How to Run
```bash
python etl/load.py batch_001
```
Or programmatically:
```python
from etl.load import run_etl
summary = run_etl("batch_001")
```

## Features
- **Cleaning**: Trims whitespace on string fields, converts currencies to uppercase, parses timestamps to UTC.
- **Fault-Tolerant Schema Validation**: Gracefully loads batches with renamed or missing columns by recording issues in `pipeline_runs.details` JSONB without crashing.
- **Invalid Record Detection**: Identifies null or negative/zero amounts, flags them as `is_valid = false` with `invalid_reason`, and loads them for audit.
- **Idempotency**: Executes in a single atomic transaction: deletes previous records for the batch, bulk inserts with `execute_values`, upserts batch status to `'ingested'`, and logs the pipeline run.

## Output
- Normalized transaction rows in `transactions` table.
- Ingestion summary in `batches` and audit trail in `pipeline_runs`.
