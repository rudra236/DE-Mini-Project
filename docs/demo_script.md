# BankFlow Sentinel — Live Demonstration Script & Operations Walkthrough

This document outlines the exact, click-by-click walkthrough for evaluating the BankFlow Sentinel platform end-to-end.

---

## ⏱️ Walkthrough Timeline Summary (~4 Minutes Total)

| Step | Action | Expected Duration | Primary Component |
|---|---|---|---|
| **0. Pre-Flight** | Execute startup script & open browser | ~20 seconds | `scripts/run_all.ps1` |
| **1. Ingestion Simulation** | Click "Simulate Ingestion Batch" (10,000 rows) | ~3 seconds | Streamlit Sidebar |
| **2. Event Streaming** | Kafka event published & consumer receipt | ~4 seconds | FastAPI & Kafka |
| **3. Orchestration** | Prefect flow validation & stage checks | ~6 seconds | Prefect Flow Engine |
| **4. Executive Overview** | Inspect Funnel drop-offs (10,000 → 9,850 → 9,700) | ~30 seconds | `01_overview.py` |
| **5. Reconciliation Triage** | Verify missing transaction IDs and amount deltas | ~45 seconds | `02_reconciliation.py` |
| **6. Incident Resolution** | Triage incident, add root cause note, mark Resolved | ~30 seconds | `04_incident_management.py` |
| **7. Historical Trends & OLAP** | Review batch-over-batch shifts and multidimensional cube | ~45 seconds | `05_historical_analytics.py` |

---

## 🚀 Step-by-Step Click Walkthrough

### Step 0: Pre-Flight Stack Initialization
1. In a terminal, run:
   ```powershell
   .\scripts\run_all.ps1
   ```
2. Verify that the terminal confirms all three containers are healthy, schemas are initialized, and background microservices are running.
3. Open your browser and navigate to:
   ```
   http://localhost:8501
   ```
4. Confirm all 4 service dots in the sidebar are **ONLINE** (PostgreSQL 16, Kafka 3.7, MongoDB 7, FastAPI Gateway).

---

### Step 1: Trigger Ingestion Batch Simulation
1. On the left sidebar, navigate down to the **TRIGGER SIMULATION** panel.
2. Select **Batch Volume**: `10,000 Rows`.
3. Click the **🚀 Simulate Ingestion Batch** button.
4. **Live Visual Indicator (`st.status`)**:
   * A progress panel opens immediately showing:
     * `✓ Generated: Synthetic transactions & injected fault profiles created`

---

### Step 2: Kafka Event Publication & Consumer Receipt
1. The simulation client posts the ingestion payload to the FastAPI Gateway:
   ```http
   POST http://127.0.0.1:8000/events
   {"batch_id": "B011", "file_path": "data/batch_011/batch_011_source.csv", "row_count": 10000}
   ```
2. The FastAPI router stores the raw JSON event into MongoDB collection `raw_events` and publishes a `batch_created` message to Kafka topic `batch_created`.
3. The background Kafka consumer (`streaming.consumer`) detects and picks up the event off partition 0.
4. Status panel updates:
   * `✓ Kafka event published: POST /events (202 Accepted)`
   * `✓ Consumer picked up: Pipeline ingestion initiated`

---

### Step 3: Resilient Orchestration & Prefect Logging
1. Because `USE_PREFECT=true` is enabled in `.env`, the consumer delegates stage execution to Prefect:
   * **Task 1: `ingest`**: Confirms source and downstream CSVs, registers batch metadata in PostgreSQL.
   * **Task 2: `validate`**: Executes ETL stage transformation (`run_etl`) and data quality rules.
   * **Task 3: `reconcile`**: Executes relational SQL reconciliation engine (`reconcile`).
   * **Task 4: `report`**: Generates markdown audit summary in `docs/reports/` and records run metrics.
2. Status panel updates:
   * `✓ Reconciled: Stage counts and discrepancies reconciled in DB`
   * Progress panel turns green: `✓ Batch B011 Reconciled Successfully!`
3. The dashboard automatically invalidates cached data, sets `B011` as the active batch filter, and reloads.

---

### Step 4: Executive Overview & Flow Funnel
1. In the sidebar radio navigation, click **📊 Overview**.
2. **Top Metric Cards**:
   * Review Total Batches, Transactions Processed (29,550), Unmatched Items, and Reconciled Health %.
3. **Pipeline Stage Funnel**:
   * Observe the horizontal funnel chart demonstrating banking drop-offs:
     * **Source Stage**: `10,000` records (100.0%)
     * **Payment Stage**: `9,850` records (98.5% — 150 dropped)
     * **Settlement Stage**: `9,700` records (98.5% — 150 dropped)
4. **Fault Mix Proportion**:
   * Observe the interactive donut chart showing balanced distribution across missing records, amount mismatches, duplicate IDs, delay breaches, and invalid schema records.
5. **Recent Operational Incidents Table**:
   * View the raised tickets with severity badges (Red `HIGH`, Gold `MEDIUM`, Blue `LOW`) and Red `OPEN` status chips.

---

### Step 5: Granular Reconciliation Deep-Dive
1. In the sidebar, select **⚖️ Reconciliation**.
2. **Stage Comparison**:
   * Click between `Source -> Payment` and `Payment -> Settlement` tabs to view exact upstream vs downstream counts and net losses.
3. **Missing IDs Sub-Tab**:
   * Review the list of missing transaction IDs.
   * Type a prefix (e.g. `T000`) in the **Search Missing Transaction ID** field to filter dynamically.
   * Click the **📥 Download CSV** button to download the exportable audit manifest.
4. **Amount Deltas Sub-Tab**:
   * Inspect the table displaying upstream amounts, altered downstream amounts, and computed currency deltas.
5. **Duplicate IDs & Delayed Transactions Sub-Tabs**:
   * View duplicate transaction occurrences and timestamps exceeding the 30-minute SLA threshold.

---

### Step 6: Incident Triage & Operational Resolution
1. In the sidebar, click **🚨 Incident Management**.
2. Locate the incident ticket: `INC_B011_payment_missing_record`.
3. Click the expander to inspect the **JSONB Evidence Payload** containing the full list of 150 offending transaction IDs.
4. Under **Operational Triage & Actions**:
   * Enter a root cause explanation in the text input:
     * `"Simulated network packet drop during payment gateway handoff. Downstream retry scheduled."`
   * Click **🔍 Investigate**: Notice the status chip updates to `INVESTIGATING` in amber.
   * Click **✅ Resolve**: Notice the status chip transitions to `RESOLVED` in emerald, and the resolved timestamp is permanently recorded in PostgreSQL.

---

### Step 7: Historical Trends, Window Functions & OLAP Analytics
1. In the sidebar, select **📈 Historical Analytics**.
2. **Fault Frequency Timeline**:
   * View the multi-day trend line showing daily discrepancy volumes across batches.
3. **Cross-Batch Fault Density Heatmap**:
   * Inspect the full-width heatmap matrix showing fault types across all ingested batches.
4. **Batch-over-Batch Mismatch Shifts**:
   * Review the database view `v_batch_summary` showing analytical SQL window calculations (`LAG()`, `LEAD()`, and `DENSE_RANK()`).
5. **OLAP Multidimensional Cube**:
   * Examine the SQL `GROUP BY CUBE(stage, severity)` summary table with `GROUPING()` subtotals.

---

### Step 8: Data Quality & Governance Verification
1. In the sidebar, select **🛡️ Data Quality**.
2. View the **Composite Quality Score** semicircle gauge (calibrated via `reconcile/quality.py`).
3. Inspect the Null Amount Rate, Invalid Row Rate, Duplicate Collision Rate, and Freshness.
4. Review the verified **Banking Data Quality Rules Checklist** validating syntax, currency codes, and status flags.

---

## 🛑 Clean Teardown
When finished, stop all background services:
```powershell
.\scripts\stop_all.ps1
```
To destroy Docker container volumes completely:
```bash
docker compose down -v
```
