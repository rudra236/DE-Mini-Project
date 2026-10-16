-- =============================================================================
-- BankFlow Sentinel - Query Plan Optimization Demo
-- Experiment 3: EXPLAIN (ANALYZE, BUFFERS) Before and After Indexing
-- =============================================================================

-- Target Query: Transaction lookup by unique/selective identifier txn_id
-- Query: SELECT * FROM transactions WHERE txn_id = 'T000042';

-- -----------------------------------------------------------------------------
-- STEP 1: BEFORE INDEX (Simulate missing index on transaction lookup)
-- -----------------------------------------------------------------------------
DROP INDEX IF EXISTS idx_transactions_txn_id;

-- Execute query plan analysis: Sequential Scan over table blocks
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, COSTS, FORMAT TEXT)
SELECT id, batch_id, stage, txn_id, account_id, amount, currency, ts, status, is_valid
FROM transactions
WHERE txn_id = 'T000042';

-- -----------------------------------------------------------------------------
-- STEP 2: CREATE B-TREE INDEX & RE-ANALYZE
-- -----------------------------------------------------------------------------
CREATE INDEX idx_transactions_txn_id ON transactions (txn_id);
ANALYZE transactions;

-- -----------------------------------------------------------------------------
-- STEP 3: AFTER INDEX (Optimized B-tree lookup)
-- -----------------------------------------------------------------------------
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, COSTS, FORMAT TEXT)
SELECT id, batch_id, stage, txn_id, account_id, amount, currency, ts, status, is_valid
FROM transactions
WHERE txn_id = 'T000042';
