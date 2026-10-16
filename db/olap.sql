-- =============================================================================
-- BankFlow Sentinel - OLAP Analytical Queries (Experiment 7)
-- Multi-dimensional analysis across incidents, batches, and processing stages
-- Features: CUBE, ROLLUP, GROUPING SETS, and GROUPING() subtotal labeling
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Query 1: CUBE Multi-Dimensional Aggregation
-- Explores all combinations of Date, Stage, and Severity (2^3 = 8 dimensional combinations)
-- Uses GROUPING() to cleanly label dimensional aggregations and overall totals
-- -----------------------------------------------------------------------------
SELECT 
    CASE 
        WHEN GROUPING(b.created_at::date) = 1 THEN 'ALL DATES (TOTAL)' 
        ELSE b.created_at::date::text 
    END AS batch_date,
    CASE 
        WHEN GROUPING(i.stage) = 1 THEN 'ALL STAGES (TOTAL)' 
        ELSE i.stage 
    END AS stage,
    CASE 
        WHEN GROUPING(i.severity) = 1 THEN 'ALL SEVERITIES (TOTAL)' 
        ELSE i.severity 
    END AS severity,
    GROUPING(b.created_at::date) AS grp_date,
    GROUPING(i.stage) AS grp_stage,
    GROUPING(i.severity) AS grp_severity,
    COUNT(i.id) AS incident_tickets,
    COUNT(DISTINCT b.batch_id) AS distinct_batches,
    SUM(jsonb_array_length(i.evidence)) AS affected_txns_count
FROM incidents i
JOIN batches b ON i.batch_id = b.batch_id
GROUP BY CUBE(b.created_at::date, i.stage, i.severity)
ORDER BY 
    GROUPING(b.created_at::date),
    GROUPING(i.stage),
    GROUPING(i.severity),
    batch_date,
    stage,
    severity;


-- -----------------------------------------------------------------------------
-- Query 2: ROLLUP Hierarchical Aggregation
-- Hierarchical drill-down: Date -> Stage -> Discrepancy Type -> Subtotals -> Grand Total
-- -----------------------------------------------------------------------------
SELECT 
    CASE 
        WHEN GROUPING(b.created_at::date) = 1 THEN 'GRAND TOTAL' 
        ELSE b.created_at::date::text 
    END AS report_date,
    CASE 
        WHEN GROUPING(i.stage) = 1 THEN 'STAGE SUBTOTAL' 
        ELSE i.stage 
    END AS pipeline_stage,
    CASE 
        WHEN GROUPING(i.type) = 1 THEN 'TYPE SUBTOTAL' 
        ELSE i.type 
    END AS discrepancy_type,
    GROUPING(b.created_at::date) + GROUPING(i.stage) + GROUPING(i.type) AS aggregation_level,
    COUNT(i.id) AS incident_tickets,
    SUM(jsonb_array_length(i.evidence)) AS total_affected_txns
FROM incidents i
JOIN batches b ON i.batch_id = b.batch_id
GROUP BY ROLLUP(b.created_at::date, i.stage, i.type)
ORDER BY 
    b.created_at::date NULLS LAST,
    i.stage NULLS LAST,
    i.type NULLS LAST;


-- -----------------------------------------------------------------------------
-- Query 3: GROUPING SETS Targeted Analytical Reporting
-- Custom reporting slices: (Stage, Severity), (Type), and Overall Grand Total ()
-- -----------------------------------------------------------------------------
SELECT 
    CASE 
        WHEN GROUPING(i.stage) = 1 AND GROUPING(i.type) = 1 THEN 'SYSTEM WIDE SUMMARY'
        WHEN GROUPING(i.type) = 0 THEN 'BY DISCREPANCY TYPE'
        ELSE 'BY STAGE & SEVERITY'
    END AS reporting_slice,
    COALESCE(i.stage, 'ALL') AS stage,
    COALESCE(i.severity, 'ALL') AS severity,
    COALESCE(i.type, 'ALL') AS discrepancy_type,
    COUNT(i.id) AS incident_count,
    SUM(jsonb_array_length(i.evidence)) AS total_affected_txns,
    ROUND(AVG(jsonb_array_length(i.evidence)), 1) AS avg_evidence_per_incident
FROM incidents i
JOIN batches b ON i.batch_id = b.batch_id
GROUP BY GROUPING SETS (
    (i.stage, i.severity),
    (i.type),
    ()
)
ORDER BY 
    GROUPING(i.stage),
    GROUPING(i.type),
    stage,
    severity,
    discrepancy_type;
