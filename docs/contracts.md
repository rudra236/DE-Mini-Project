# Contracts Specification (Single Source of Truth)

> **Important**: This document represents the single source of truth across all modules of BankFlow Sentinel. Never deviate from these definitions.

- CSV columns: txn_id, account_id, amount, currency, timestamp, status
- Files: data/batch_001/batch_001_source.csv, _payment.csv, _settlement.csv, _ground_truth.csv
- Batch IDs: "B001" (DB) / "batch_001" (files). Provide helper functions to convert.
- Stage order: source -> payment -> settlement. Reconciliation is ADJACENT-STAGE: payment vs source, settlement vs payment.
- Discrepancy types (exact strings): missing_record, duplicate, amount_mismatch, delayed, schema_change, invalid_record
- Severity: missing_record=high, amount_mismatch=high, schema_change=high, duplicate=medium, invalid_record=medium, delayed=low
- Delayed = downstream timestamp minus upstream timestamp > 30 minutes
- Incident JSON: {"batch_id":"B001","stage":"payment","type":"missing_record","severity":"high","evidence":["T1023","T1099"],"detected_at":"2026-10-10T10:00:00Z","status":"open"}  (status: open|investigating|resolved)
- Kafka: topic batch_created, value {"batch_id":"B001","file_path":"data/batch_001","row_count":10000}
- Ground truth CSV columns: batch_id, stage, type, txn_id
- API: POST /events, POST /sources, GET /reconciliation/{batch_id}, GET /incidents, plus GET /health and PATCH /incidents/{id} (status update)
- Env vars: POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, KAFKA_BOOTSTRAP, MONGO_URI, DATABASE_URL
