# BankFlow Sentinel

**Real-Time Banking Data Pipeline Monitoring, Failure Detection, and Multi-Stage Transaction Reconciliation Platform**

BankFlow Sentinel is a production-grade data engineering platform built to monitor, audit, reconcile, and triage financial transactions across multi-stage banking pipelines (`Source` $\rightarrow$ `Payment Gateway` $\rightarrow$ `Settlement Clearinghouse`). It detects 6 distinct classes of data processing anomalies (missing records, amount mismatches, duplicate IDs, SLA delays, schema drift, and invalid records) using relational SQL joins, logs incident tickets with JSONB transaction-level evidence, and provides an interactive dark-themed operations console.

---

> ### ⚠️ Mandatory Financial Disclaimer
> **Synthetic data ONLY.** All accounts, transactions, balances, and discrepancies generated within this repository represent simulated banking data processing scenarios for operational testing and pipeline reconciliation, not confirmed monetary loss or real financial transactions.

---

## 🏛️ System Architecture

```mermaid
flowchart TD
    subgraph DataGen ["Synthetic Data Layer"]
        DG["data_gen.generate (Faker)"] --> FI["data_gen.inject_faults"]
        FI --> SRC["Source Manifest (10k)"]
        FI --> PMT["Payment Manifest (9.85k)"]
        FI --> STL["Settlement Manifest (9.7k)"]
    end

    subgraph Ingestion ["Ingestion & Streaming Gateway"]
        SRC --> API["FastAPI Gateway (Port 8000)"]
        API -->|Raw JSON Audit Trail| MGO[("MongoDB (Port 27017)<br/>raw_events & metadata")]
        API -->|Publish batch_created| KP["Kafka Producer (Port 9092)"]
        KP --> KT["Topic: batch_created"]
        KT --> KC["Kafka Consumer Group:<br/>bankflow-consumers"]
    end

    subgraph Orchestration ["Orchestration & Transformation"]
        KC -->|USE_PREFECT=true| PF["Prefect Flow: bankflow-pipeline"]
        PF --> ETL["etl.load (PostgreSQL Ingestion)"]
        PF --> ENG["reconcile.engine (Adjacent SQL Joins)"]
        PF --> DQ["reconcile.quality (Quality Score)"]
    end

    subgraph Storage ["Relational Ledger & OLAP"]
        ETL --> PG[("PostgreSQL 16 (Port 5433)<br/>batches, transactions, incidents")]
        ENG --> PG
        PG --> VW["View: v_batch_summary<br/>LAG(), DENSE_RANK()"]
        PG --> OLAP["OLAP Aggregations<br/>CUBE, ROLLUP, GROUPING SETS"]
    end

    subgraph Lakehouse ["Lakehouse Layer"]
        PG --> DL["PySpark + Delta Lake 3.2.0<br/>lakehouse/delta_demo.py"]
        DL --> DT[("Delta Tables<br/>ACID, Time Travel, Schema")]
    end

    subgraph Console ["Fintech Operations Console"]
        PG --> ST["Streamlit Console (Port 8501)<br/>dashboard/app.py"]
        ST --> O1["1. Overview & Dropoff Funnel"]
        ST --> O2["2. Reconciliation & CSV Export"]
        ST --> O3["3. Pipeline Telemetry & Gantt"]
        ST --> O4["4. Incident Triage & Resolution"]
        ST --> O5["5. Historical Trends & OLAP"]
        ST --> O6["6. Data Quality & Contract Health"]
    end
```

---

## 🔬 11 Experiments: Code Mapping & Run Commands

| Exp # | Experiment Title | Key Code Files | Direct Run Command |
|---|---|---|---|
| **1** | **Exploratory Data Analysis** | `notebooks/01_eda_cleaning.ipynb` | `python scripts/build_eda_notebook.py` |
| **2** | **Synthetic Data & Fault Injection** | `data_gen/generate.py`, `data_gen/inject_faults.py` | `python -m data_gen.generate --batch 1 --rows 10000`<br/>`python -m data_gen.inject_faults --batch 1` |
| **3** | **Advanced SQL & Query Optimization** | `db/queries_sql_features.sql`, `scripts/explain_demo.py` | `python scripts/explain_demo.py` |
| **4** | **NoSQL Document Storage** | `db/mongo_setup.py`, `scripts/mongo_demo.py` | `python scripts/mongo_demo.py` |
| **5** | **REST API Ingestion Gateway** | `api/main.py`, `api/routes/` | `pytest tests/test_api.py` |
| **6** | **Real-Time Event Streaming** | `streaming/producer.py`, `streaming/consumer.py` | `pytest tests/test_streaming.py` |
| **7** | **OLAP Multidimensional Aggregations** | `db/olap.sql` | `python -m db.init_db` |
| **8** | **High-Scale Engine Benchmarks** | `benchmarks/run_benchmark.py` | `python benchmarks/run_benchmark.py --rows 1000000` |
| **9** | **Lakehouse ACID & Time Travel** | `lakehouse/delta_demo.py` | `python lakehouse/delta_demo.py` |
| **10** | **Workflow Orchestration & Retries** | `orchestration/flow.py` | `python -m orchestration.flow B001 --fail-once` |
| **11** | **Fintech Operations Console** | `dashboard/app.py`, `dashboard/pages/` | `streamlit run dashboard/app.py` |

---

## ⚡ Quickstart: One-Command Master Startup

### Windows (PowerShell)
```powershell
# 1. Start all infrastructure containers, initialize schemas, and launch microservices:
.\scripts\run_all.ps1

# 2. To cleanly stop all background services when finished:
.\scripts\stop_all.ps1
```

### Linux / macOS (Bash)
```bash
# 1. Start all infrastructure containers, initialize schemas, and launch microservices:
chmod +x scripts/*.sh
./scripts/run_all.sh

# 2. To cleanly stop all background services when finished:
./scripts/stop_all.sh
```

### Active Application Endpoints
* 🏦 **Operations Console**: [http://localhost:8501](http://localhost:8501)
* ⚡ **FastAPI REST Gateway**: [http://127.0.0.1:8000](http://127.0.0.1:8000)
* 📖 **API Swagger Docs**: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)
* 🔍 **API Health Check**: [http://127.0.0.1:8000/health](http://127.0.0.1:8000/health)

---

## 🛠️ Troubleshooting & Technical Gotchas

### 1. Port 5432 Already in Use (Host Postgres Conflict)
* **Problem**: The local machine already has a native PostgreSQL service bound to default port `5432`.
* **Solution**: In `docker-compose.yml` and `.env`, PostgreSQL is mapped to port **`5433`** (`5433:5432`). Connection URLs must always target `localhost:5433`:
  ```
  DATABASE_URL=postgresql://bankflow:change_me@localhost:5433/bankflow
  ```

### 2. Kafka Advertised Listeners
* **Problem**: External Python producers/consumers cannot reach Kafka or hang indefinitely on connection handshake.
* **Solution**: `docker-compose.yml` explicitly configures the dual advertised listeners:
  ```yaml
  KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://bankflow-kafka:29092
  ```
  Host clients must connect to `localhost:9092`.

### 3. PostgreSQL Docker Schema Initialization Only Runs on Empty Volumes
* **Problem**: Modifications to `db/schema.sql` do not apply when restarting `docker compose up -d` because the existing Docker named volume `postgres_data` already exists.
* **Solution**: Either destroy the volume to re-trigger Docker's init entrypoint:
  ```bash
  docker compose down -v
  ```
  Or run the idempotent Python database initializer without destroying container data:
  ```bash
  python db/init_db.py
  ```

### 4. Java 17 and PySpark / Delta Lake Version Pins
* **Problem**: Spark raises `UnsupportedClassVersionError` or `java.lang.NoClassDefFoundError` on newer Java versions (Java 21/22) or unpinned Delta JARs.
* **Solution**: Pin Java to **Java 17** (`JAVA_HOME`) and ensure exact dependency alignment:
  ```txt
  pyspark==3.5.1
  delta-spark==3.2.0
  ```
  If local Java is unavailable, build the pre-configured Spark container:
  ```bash
  docker build -f Dockerfile.spark -t bankflow-spark .
  ```

---

## 📚 Documentation & Deliverables Index

* 📋 **Live Demo Script**: [`docs/demo_script.md`](file:///c:/Users/Rudra/Desktop/de%20mini%20project/docs/demo_script.md)
* 📖 **Comprehensive Project Report**: [`docs/REPORT_SKELETON.md`](file:///c:/Users/Rudra/Desktop/de%20mini%20project/docs/REPORT_SKELETON.md)
* 💡 **25 Viva Voce Questions & Answers**: [`docs/VIVA_QA.md`](file:///c:/Users/Rudra/Desktop/de%20mini%20project/docs/VIVA_QA.md)
* 📊 **1M Row Engine Benchmark Report**: [`docs/benchmark.md`](file:///c:/Users/Rudra/Desktop/de%20mini%20project/docs/benchmark.md)
* ⚡ **EXPLAIN ANALYZE Optimization Proof**: [`docs/explain_before_after.md`](file:///c:/Users/Rudra/Desktop/de%20mini%20project/docs/explain_before_after.md)
* 📜 **Data & Contract Specifications**: [`docs/contracts.md`](file:///c:/Users/Rudra/Desktop/de%20mini%20project/docs/contracts.md)
* 📸 **Operations Console Screenshots**: [`screenshots/`](file:///c:/Users/Rudra/Desktop/de%20mini%20project/screenshots/)
#   D E - M i n i - P r o j e c t  
 