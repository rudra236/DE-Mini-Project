# Streamlit Dashboard Module (`dashboard/`)

## How to Run
```bash
streamlit run dashboard/app.py
```

## Input
- Real-time queries against PostgreSQL tables (`batches`, `transactions`, `reconciliation_results`, `incidents`).

## Output
- Interactive 6-view web UI:
  1. Overview
  2. Reconciliation
  3. Pipeline Monitoring
  4. Incident Management
  5. Historical Analytics
  6. Data Quality
- "Simulate batch" action button triggering Kafka pipeline run.

> **Disclaimer**: Synthetic data ONLY. A discrepancy is a data-processing issue to investigate, not proof of lost money.
