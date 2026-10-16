-- =============================================================================
-- BankFlow Sentinel - PostgreSQL 16 Schema Definition
-- Single Source of Truth for Relational Storage
-- =============================================================================

-- 1. Batches table: tracks each ingested batch life cycle
CREATE TABLE IF NOT EXISTS batches (
    batch_id VARCHAR(50) PRIMARY KEY,
    file_path TEXT NOT NULL,
    row_count INT NOT NULL DEFAULT 0,
    status VARCHAR(50) NOT NULL CHECK (status IN ('created', 'ingested', 'validated', 'reconciled', 'failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Transactions table: stage-by-stage transaction records
-- Note: No UNIQUE constraint on txn_id because duplicates are legitimate processing flaws
CREATE TABLE IF NOT EXISTS transactions (
    id BIGSERIAL PRIMARY KEY,
    batch_id VARCHAR(50) NOT NULL REFERENCES batches(batch_id) ON DELETE CASCADE,
    stage VARCHAR(20) NOT NULL CHECK (stage IN ('source', 'payment', 'settlement')),
    txn_id VARCHAR(50) NOT NULL,
    account_id VARCHAR(50),
    amount NUMERIC(14,2) NULL,
    currency VARCHAR(10),
    ts TIMESTAMPTZ,
    status VARCHAR(50),
    is_valid BOOLEAN NOT NULL DEFAULT true,
    invalid_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_transactions_batch_stage_txn ON transactions(batch_id, stage, txn_id);
CREATE INDEX IF NOT EXISTS idx_transactions_txn_id ON transactions(txn_id);

-- 3. Pipeline runs table: tracks execution hops, task statuses, retries and error/schema details
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR(100) PRIMARY KEY,
    batch_id VARCHAR(50) NOT NULL REFERENCES batches(batch_id) ON DELETE CASCADE,
    task_name VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    retries INT NOT NULL DEFAULT 0,
    details JSONB
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_batch_id ON pipeline_runs(batch_id);

-- 4. Reconciliation results table: summary metrics per stage comparison
CREATE TABLE IF NOT EXISTS reconciliation_results (
    id BIGSERIAL PRIMARY KEY,
    batch_id VARCHAR(50) NOT NULL REFERENCES batches(batch_id) ON DELETE CASCADE,
    stage VARCHAR(20) NOT NULL,
    source_count INT NOT NULL DEFAULT 0,
    target_count INT NOT NULL DEFAULT 0,
    missing_count INT NOT NULL DEFAULT 0,
    duplicate_count INT NOT NULL DEFAULT 0,
    mismatch_count INT NOT NULL DEFAULT 0,
    delayed_count INT NOT NULL DEFAULT 0,
    invalid_count INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_reconciliation_results_batch_id ON reconciliation_results(batch_id);

-- 5. Incidents table: actionable discrepancy tickets with transaction evidence
CREATE TABLE IF NOT EXISTS incidents (
    id VARCHAR(100) PRIMARY KEY,
    batch_id VARCHAR(50) NOT NULL REFERENCES batches(batch_id) ON DELETE CASCADE,
    stage VARCHAR(20) NOT NULL,
    type VARCHAR(50) NOT NULL CHECK (type IN (
        'missing_record',
        'duplicate',
        'amount_mismatch',
        'delayed',
        'schema_change',
        'invalid_record'
    )),
    severity VARCHAR(20) NOT NULL CHECK (severity IN ('high', 'medium', 'low')),
    evidence JSONB,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status VARCHAR(20) NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'investigating', 'resolved')),
    resolved_at TIMESTAMPTZ,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_incidents_batch_id ON incidents(batch_id);
CREATE INDEX IF NOT EXISTS idx_incidents_status ON incidents(status);
CREATE INDEX IF NOT EXISTS idx_incidents_detected_at ON incidents(detected_at);
