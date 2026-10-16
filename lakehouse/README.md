# Lakehouse Module (`lakehouse/`) — Experiment 9: Delta Lake Architecture

## Overview
This module demonstrates Lakehouse architecture using **Apache Spark 3.5.0** and **Delta Lake 3.2.0**. It extracts reconciliation results and operational incident data from PostgreSQL into ACID-compliant Delta tables, showcasing table versioning, in-place updates, historical commit logs, time travel querying, schema enforcement, and schema evolution.

---

## Architecture & Features Demonstrated
1. **Delta Lake Table Ingestion (Version 0)**:
   - Queries `reconciliation_results` and `incidents` from PostgreSQL via pandas.
   - Writes snapshot to Delta format at `lakehouse/tables/reconciliation_results`.
2. **ACID In-Place Row Updates (Version 1)**:
   - Uses `DeltaTable.forPath(spark, table_path)`.
   - Modifies specific rows in place (e.g. marking batch `B004` mismatch count corrected to `0`) without rewriting the entire dataset.
3. **Audit History & Commit Log (`DeltaTable.history`)**:
   - Inspects version numbers, timestamps, operations (`WRITE`, `UPDATE`), and predicate parameters.
4. **Time Travel Querying**:
   - Queries historical snapshot with `.option("versionAsOf", 0)`.
   - Proves difference between version 0 (`mismatch_count = 50`) and latest version (`mismatch_count = 0`).
5. **Schema Enforcement**:
   - Rejects append attempts containing undeclared extra columns (`unauthorized_audit_field`), throwing `AnalysisException`.
6. **Controlled Schema Evolution (`mergeSchema = true`)**:
   - Safely evolves table schema to add `reconciled_by_analyst` column when explicitly requested.

---

## How to Run

### Option 1: Via Docker Compose (Recommended)
This runs inside the preconfigured `spark` service (`bankflow-spark:latest`), which avoids host Windows `winutils.exe` or Java 25 compatibility issues:

```bash
docker compose run --rm spark python lakehouse/delta_demo.py
```

### Option 2: Directly on Host (Requires Java 17 + Hadoop Winutils on Windows)
1. Install Java 17:
   ```powershell
   winget install Microsoft.OpenJDK.17
   ```
2. Configure `JAVA_HOME` pointing to Java 17.
3. Download Hadoop 3.3.x `winutils.exe` and `hadoop.dll` into `%HADOOP_HOME%\bin`.
4. Run:
   ```powershell
   python lakehouse/delta_demo.py
   ```

---

## Output
Demo output is recorded at [`docs/delta_demo_output.txt`](../docs/delta_demo_output.txt).
Delta tables are persisted in [`lakehouse/tables/`](tables/).
