# BankFlow Sentinel

**Real-Time Banking Data Pipeline Monitoring & Transaction Reconciliation Platform**

BankFlow Sentinel is a data engineering platform that monitors synthetic banking transactions across Source, Payment, and Settlement stages. It detects missing records, duplicates, amount mismatches, delays, schema changes, and invalid data, generating incidents with evidence for investigation.

## Key Features

- Automated ETL pipeline using Python and Pandas.
- Real-time event streaming with Apache Kafka.
- Workflow orchestration using Apache Airflow.
- Transaction reconciliation and data-quality validation.
- Incident tracking and storage using PostgreSQL.
- Raw event storage using MongoDB.
- Versioned data storage using Apache Spark and Delta Lake.
- Interactive monitoring dashboard using Streamlit.
- Performance benchmarking with Pandas, DuckDB, and Polars.

## Tech Stack

`Python` · `Pandas` · `FastAPI` · `Apache Kafka` · `Apache Airflow` · `PostgreSQL` · `MongoDB` · `DuckDB` · `Polars` · `Apache Spark` · `Delta Lake` · `Streamlit` · `Docker`

## Architecture

```text
Synthetic CSV Data
       |
       v
  FastAPI + Kafka
       |
       v
 Kafka Consumer
       |
       v
  Apache Airflow
       |
       v
 ETL + Validation
       |
       v
 Reconciliation Engine
       |
       v
PostgreSQL + MongoDB
       |
       v
Streamlit Dashboard
```

## Getting Started

**1. Clone the repository**

```bash
git clone <your-repository-url>
cd bankflow-sentinel
```

**2. Create a virtual environment**

```bash
python -m venv .venv
```

Activate it using `.venv\Scripts\activate` on Windows or `source .venv/bin/activate` on Linux/macOS.

**3. Install dependencies**

```bash
pip install -r requirements.txt
```

**4. Start configured Docker services**

```bash
docker compose up -d
```

**5. Run the application**

Start the configured Airflow services, run the Kafka producer and consumer, and launch the dashboard:

```bash
streamlit run dashboard/app.py
```

*Note: Configure environment variables, database schemas, and Airflow DAGs before running the complete pipeline. Commands assume the corresponding files and services are implemented.*

## Evaluation

The system is evaluated using synthetic transaction data with intentionally injected faults. Detected discrepancies are compared against a ground-truth file to verify correctness. Performance is measured using execution time and memory consumption.

## Disclaimer

This project uses synthetic banking data for educational and demonstration purposes only. Detected discrepancies do not prove actual financial loss, and no real banking system is connected.

---

**BankFlow Sentinel — Making transaction data quality visible, traceable, and easier to investigate.**
