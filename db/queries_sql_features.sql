-- =============================================================================
-- BankFlow Sentinel - Advanced SQL Features & Analytical Window Functions
-- Experiment 3: Joins, Window Functions (DENSE_RANK, LAG, LEAD), and Views
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Multi-Table JOIN across batches, transactions, and incidents
-- Summarizes stage volume and high-severity discrepancy counts per batch
-- -----------------------------------------------------------------------------
SELECT 
    b.batch_id,
    b.created_at::date AS batch_date,
    b.status AS batch_status,
    t.stage,
    COUNT(DISTINCT t.txn_id) AS unique_transactions,
    ROUND(COALESCE(SUM(t.amount), 0), 2) AS total_volume_inr,
    COUNT(DISTINCT i.id) AS incidents_detected,
    COUNT(DISTINCT CASE WHEN i.severity = 'high' THEN i.id END) AS high_severity_incidents
FROM batches b
JOIN transactions t ON b.batch_id = t.batch_id
LEFT JOIN incidents i ON b.batch_id = i.batch_id AND t.stage = i.stage
GROUP BY b.batch_id, b.created_at::date, b.status, t.stage
ORDER BY b.batch_id, t.stage;


-- -----------------------------------------------------------------------------
-- 2. DENSE_RANK() Window Function
-- Ranks batches by volume of raised incidents to prioritize operational attention
-- -----------------------------------------------------------------------------
SELECT 
    b.batch_id,
    b.file_path,
    b.row_count,
    COUNT(i.id) AS total_incidents,
    DENSE_RANK() OVER (ORDER BY COUNT(i.id) DESC) AS incident_density_rank,
    RANK() OVER (ORDER BY COUNT(i.id) DESC) AS standard_rank
FROM batches b
LEFT JOIN incidents i ON b.batch_id = i.batch_id
GROUP BY b.batch_id, b.file_path, b.row_count
ORDER BY incident_density_rank, b.batch_id;


-- -----------------------------------------------------------------------------
-- 3. LAG() and LEAD() Window Functions
-- Analyzes adjacent-batch discrepancy trends by comparing mismatch counts
-- -----------------------------------------------------------------------------
SELECT 
    r.batch_id,
    r.stage,
    r.source_count,
    r.target_count,
    r.mismatch_count,
    LAG(r.mismatch_count, 1, 0) OVER (PARTITION BY r.stage ORDER BY r.batch_id) AS prev_batch_mismatches,
    LEAD(r.mismatch_count, 1, 0) OVER (PARTITION BY r.stage ORDER BY r.batch_id) AS next_batch_mismatches,
    r.mismatch_count - LAG(r.mismatch_count, 1, 0) OVER (PARTITION BY r.stage ORDER BY r.batch_id) AS delta_from_prev_batch
FROM reconciliation_results r
ORDER BY r.stage, r.batch_id;


-- -----------------------------------------------------------------------------
-- 4. Analytical View: v_batch_summary
-- Reusable executive view combining batch health, flow rates, and open incident tickets
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_batch_summary AS
SELECT 
    b.batch_id,
    b.created_at::date AS ingest_date,
    b.row_count AS declared_source_rows,
    b.status AS pipeline_status,
    COALESCE(r_pay.source_count, 0) AS payment_source_count,
    COALESCE(r_pay.target_count, 0) AS payment_target_count,
    COALESCE(r_settle.target_count, 0) AS settlement_target_count,
    COALESCE(r_pay.missing_count + r_settle.missing_count, 0) AS total_missing,
    COALESCE(r_pay.mismatch_count + r_settle.mismatch_count, 0) AS total_amount_mismatches,
    COALESCE(r_pay.duplicate_count + r_settle.duplicate_count, 0) AS total_duplicates,
    COALESCE(r_pay.delayed_count + r_settle.delayed_count, 0) AS total_delayed,
    COALESCE(r_pay.invalid_count + r_settle.invalid_count, 0) AS total_invalid,
    COUNT(DISTINCT i.id) AS total_incidents_raised,
    COUNT(DISTINCT CASE WHEN i.status = 'open' THEN i.id END) AS open_incidents,
    COUNT(DISTINCT CASE WHEN i.status = 'resolved' THEN i.id END) AS resolved_incidents
FROM batches b
LEFT JOIN reconciliation_results r_pay ON b.batch_id = r_pay.batch_id AND r_pay.stage = 'payment'
LEFT JOIN reconciliation_results r_settle ON b.batch_id = r_settle.batch_id AND r_settle.stage = 'settlement'
LEFT JOIN incidents i ON b.batch_id = i.batch_id
GROUP BY 
    b.batch_id, b.created_at::date, b.row_count, b.status,
    r_pay.source_count, r_pay.target_count, r_settle.target_count,
    r_pay.missing_count, r_settle.missing_count,
    r_pay.mismatch_count, r_settle.mismatch_count,
    r_pay.duplicate_count, r_settle.duplicate_count,
    r_pay.delayed_count, r_settle.delayed_count,
    r_pay.invalid_count, r_settle.invalid_count;
