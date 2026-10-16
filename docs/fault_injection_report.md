# BankFlow Sentinel - Fault Injection & Detection Verification Report

> **Evaluation Evidence**: Proves that the BankFlow Sentinel reconciliation engine detects 100% of injected data faults across all processing stages with **Zero False Positives** and **Zero False Negatives**.

## 1. Executive Summary

| Batch | Description | Injected Faults | Detected Faults | Precision | Recall | Result |
|---|---|---|---|---|---|---|
| **B001** | Production 10,000-Row Batch | 750 | 750 | **100.00%** | **100.00%** | 🟢 **100% MATCH** |
| **B002** | 100-Row Batch with Schema Fault | 13 | 13 | **100.00%** | **100.00%** | 🟢 **100% MATCH** |

## 2. Detailed Breakdown: Production 10,000-Row Batch (`B001`)

| Stage | Discrepancy Type | Injected | Detected | Accuracy | Severity | Status |
|---|---|---|---|---|---|---|
| `payment` | `amount_mismatch` | 50 | 50 | 100.0% | `high` | ✅ Verified |
| `payment` | `delayed` | 50 | 50 | 100.0% | `low` | ✅ Verified |
| `payment` | `duplicate` | 50 | 50 | 100.0% | `medium` | ✅ Verified |
| `payment` | `invalid_record` | 50 | 50 | 100.0% | `medium` | ✅ Verified |
| `payment` | `missing_record` | 200 | 200 | 100.0% | `high` | ✅ Verified |
| `settlement` | `amount_mismatch` | 50 | 50 | 100.0% | `high` | ✅ Verified |
| `settlement` | `delayed` | 50 | 50 | 100.0% | `low` | ✅ Verified |
| `settlement` | `duplicate` | 50 | 50 | 100.0% | `medium` | ✅ Verified |
| `settlement` | `invalid_record` | 50 | 50 | 100.0% | `medium` | ✅ Verified |
| `settlement` | `missing_record` | 150 | 150 | 100.0% | `high` | ✅ Verified |

## 2. Detailed Breakdown: 100-Row Batch with Schema Fault (`B002`)

| Stage | Discrepancy Type | Injected | Detected | Accuracy | Severity | Status |
|---|---|---|---|---|---|---|
| `payment` | `amount_mismatch` | 1 | 1 | 100.0% | `high` | ✅ Verified |
| `payment` | `delayed` | 1 | 1 | 100.0% | `low` | ✅ Verified |
| `payment` | `duplicate` | 1 | 1 | 100.0% | `medium` | ✅ Verified |
| `payment` | `invalid_record` | 1 | 1 | 100.0% | `medium` | ✅ Verified |
| `payment` | `missing_record` | 3 | 3 | 100.0% | `high` | ✅ Verified |
| `payment` | `schema_change` | 1 | 1 | 100.0% | `high` | ✅ Verified |
| `settlement` | `amount_mismatch` | 1 | 1 | 100.0% | `high` | ✅ Verified |
| `settlement` | `delayed` | 1 | 1 | 100.0% | `low` | ✅ Verified |
| `settlement` | `duplicate` | 1 | 1 | 100.0% | `medium` | ✅ Verified |
| `settlement` | `invalid_record` | 1 | 1 | 100.0% | `medium` | ✅ Verified |
| `settlement` | `missing_record` | 1 | 1 | 100.0% | `high` | ✅ Verified |

## 3. Verification Methodology

1. **Data Generation & Fault Injection** (`data_gen/`): Clean banking transactions are synthesized and faults are injected stage-by-stage with exact mathematical tracking.
2. **Ground Truth Audit Trail**: Every injected fault is logged to `batch_NNN_ground_truth.csv` as the gold standard.
3. **ETL Ingestion** (`etl/`): Normalized tables are populated into PostgreSQL 16.
4. **Adjacent-Stage SQL Reconciliation** (`reconcile/engine.py`): Pure parameterized relational SQL queries detect discrepancies between adjacent stages.
5. **Automated Assertion**: Ground truth tuples `(stage, type, txn_id)` are compared set-for-set with incident evidence in PostgreSQL.
