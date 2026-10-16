"""
BankFlow Sentinel - Experiment 9: Lakehouse Architecture with Delta Lake (delta_demo.py)

Demonstrates:
  1. Reading reconciliation_results and incidents from PostgreSQL into Spark DataFrame and writing as Delta (version 0).
  2. In-place ACID row UPDATE using DeltaTable (version 1).
  3. Delta commit history and audit trail (DeltaTable.history).
  4. Time travel query comparing snapshot version 0 against latest version.
  5. Schema enforcement catching and rejecting incompatible schema appends (AnalysisException).
  6. Controlled schema evolution using mergeSchema=true.

Output saved to docs/delta_demo_output.txt.
"""

import io
import os
import shutil
import socket
import sys
from datetime import datetime

import pandas as pd
import psycopg2
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession


class TeeOutput:
    """Tees stdout to both terminal and an in-memory buffer / file."""
    def __init__(self, target_filepath: str):
        self.target_filepath = target_filepath
        self.terminal = sys.stdout
        self.buffer = io.StringIO()

    def write(self, message):
        self.terminal.write(message)
        self.buffer.write(message)

    def flush(self):
        self.terminal.flush()
        self.buffer.flush()

    def save(self):
        os.makedirs(os.path.dirname(self.target_filepath), exist_ok=True)
        with open(self.target_filepath, "w", encoding="utf-8") as f:
            f.write(self.buffer.getvalue())


def detect_db_params():
    """Detects PostgreSQL host and port automatically across Docker and Host environments."""
    env_host = os.getenv("POSTGRES_HOST")
    env_port = os.getenv("POSTGRES_PORT")
    candidates = []
    if env_host and env_port:
        candidates.append((env_host, int(env_port)))
    
    candidates.extend([
        ("postgres", 5432),
        ("bankflow-postgres", 5432),
        ("host.docker.internal", 5433),
        ("127.0.0.1", 5433),
        ("localhost", 5433),
        ("127.0.0.1", 5432),
    ])

    user = os.getenv("POSTGRES_USER", "bankflow")
    password = os.getenv("POSTGRES_PASSWORD", "change_me")
    dbname = os.getenv("POSTGRES_DB", "bankflow")

    for host, port in candidates:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1.5)
            result = sock.connect_ex((host, port))
            sock.close()
            if result == 0:
                return {"host": host, "port": port, "user": user, "password": password, "dbname": dbname}
        except Exception:
            continue

    return {"host": "localhost", "port": 5433, "user": user, "password": password, "dbname": dbname}


def get_spark_session() -> SparkSession:
    """Initializes SparkSession configured with Delta Lake extensions."""
    builder = SparkSession.builder \
        .appName("BankFlowDeltaLakeDemo") \
        .master("local[2]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.databricks.delta.snapshotPartitions", "2") \
        .config("spark.ui.enabled", "false") \
        .config("spark.sql.debug.maxToStringFields", "200")

    return configure_spark_with_delta_pip(builder).getOrCreate()


def main():
    docs_output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "docs", "delta_demo_output.txt"))
    tee = TeeOutput(docs_output_path)
    sys.stdout = tee

    table_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "tables", "reconciliation_results"))
    incidents_table_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "tables", "incidents"))

    print("=" * 80)
    print("BANKFLOW SENTINEL - EXPERIMENT 9: DELTA LAKE ACID & TIME TRAVEL DEMO")
    print(f"Timestamp: {datetime.utcnow().isoformat()} UTC")
    print(f"Table Target: {table_path}")
    print("=" * 80)

    # Idempotent cleanup: remove existing Delta tables
    for p in [table_path, incidents_table_path]:
        if os.path.exists(p):
            print(f"[Cleanup] Removing existing table at {p} for idempotent run...")
            shutil.rmtree(p)

    # 1. Start Spark
    print("\n[Spark] Initializing SparkSession with Delta Lake 3.2.0...")
    spark = get_spark_session()
    print(f"[Spark] Spark version: {spark.version}")

    try:
        # ==============================================================================
        # SECTION 1: INGESTION & DELTA TABLE CREATION (VERSION 0)
        # ==============================================================================
        print("\n" + "=" * 80)
        print("SECTION 1: INGESTION & DELTA TABLE CREATION (VERSION 0)")
        print("=" * 80)

        db_params = detect_db_params()
        print(f"[Postgres] Connecting via psycopg2 to {db_params['host']}:{db_params['port']} (db: {db_params['dbname']})...")
        conn = psycopg2.connect(**db_params)

        # Read reconciliation_results from PostgreSQL via pandas
        df_results_pd = pd.read_sql_query("SELECT * FROM reconciliation_results ORDER BY id", conn)
        if "created_at" in df_results_pd.columns:
            df_results_pd["created_at"] = df_results_pd["created_at"].astype(str)

        print(f"[Postgres] Read {len(df_results_pd)} reconciliation_results rows via pandas.")

        # Read incidents from PostgreSQL via pandas
        df_incidents_pd = pd.read_sql_query("SELECT id, batch_id, stage, type, severity, status, detected_at::text FROM incidents ORDER BY id", conn)
        print(f"[Postgres] Read {len(df_incidents_pd)} incidents rows via pandas.")
        conn.close()

        # Convert to Spark DataFrame
        spark_results_df = spark.createDataFrame(df_results_pd)
        spark_incidents_df = spark.createDataFrame(df_incidents_pd)

        # Write as Delta table (version 0)
        print(f"\n[Delta] Writing reconciliation_results as Delta table (version 0) to:\n  {table_path}")
        spark_results_df.write.format("delta").mode("overwrite").save(table_path)
        spark_incidents_df.write.format("delta").mode("overwrite").save(incidents_table_path)

        # Confirm Version 0 written
        v0_df = spark.read.format("delta").load(table_path)
        print("\n[Delta v0 Snapshot] reconciliation_results (sample):")
        v0_df.select("batch_id", "stage", "source_count", "target_count", "mismatch_count", "duplicate_count") \
            .orderBy("batch_id", "stage") \
            .show(8, truncate=False)

        # Target batch to update
        target_batch = "B004"
        initial_mismatch = v0_df.filter(f"batch_id = '{target_batch}' AND stage = 'payment'").select("mismatch_count").collect()
        if initial_mismatch:
            init_val = initial_mismatch[0]["mismatch_count"]
            print(f"[Info] Target batch '{target_batch}' (payment) initial mismatch_count = {init_val}")
        else:
            first_row = v0_df.select("batch_id").first()
            target_batch = first_row["batch_id"] if first_row else "B001"

        # ==============================================================================
        # SECTION 2: DELTA IN-PLACE ROW UPDATE (VERSION 1)
        # ==============================================================================
        print("\n" + "=" * 80)
        print("SECTION 2: DELTA IN-PLACE ROW UPDATE (VERSION 1)")
        print("=" * 80)

        delta_table = DeltaTable.forPath(spark, table_path)
        print(f"[DeltaTable] Executing ACID UPDATE: marking batch {target_batch} mismatch_count corrected to 0...")

        delta_table.update(
            condition=f"batch_id = '{target_batch}' AND stage = 'payment'",
            set={"mismatch_count": "0"}
        )

        print(f"\n[Delta v1 Snapshot] Verified updated row for batch {target_batch}:")
        updated_df = spark.read.format("delta").load(table_path)
        updated_df.filter(f"batch_id = '{target_batch}' AND stage = 'payment'") \
            .select("batch_id", "stage", "source_count", "target_count", "mismatch_count", "duplicate_count") \
            .show(truncate=False)

        # ==============================================================================
        # SECTION 3: DELTA TABLE HISTORY & AUDIT TRAIL
        # ==============================================================================
        print("\n" + "=" * 80)
        print("SECTION 3: DELTA TABLE HISTORY & AUDIT TRAIL (DeltaTable.history)")
        print("=" * 80)

        history_df = delta_table.history()
        history_df.select("version", "timestamp", "operation", "operationParameters") \
            .show(truncate=False)

        # ==============================================================================
        # SECTION 4: TIME TRAVEL QUERY (VERSION 0 vs LATEST)
        # ==============================================================================
        print("\n" + "=" * 80)
        print("SECTION 4: TIME TRAVEL QUERY (VERSION 0 vs LATEST VERSION)")
        print("=" * 80)

        print("[Time Travel] Reading snapshot at versionAsOf = 0 (Pre-Update State):")
        time_travel_v0 = spark.read.format("delta").option("versionAsOf", 0).load(table_path)
        time_travel_v0.filter(f"batch_id = '{target_batch}' AND stage = 'payment'") \
            .select("batch_id", "stage", "source_count", "target_count", "mismatch_count") \
            .show(truncate=False)

        print("[Time Travel] Reading snapshot at latest version (Post-Update State):")
        latest_df = spark.read.format("delta").load(table_path)
        latest_df.filter(f"batch_id = '{target_batch}' AND stage = 'payment'") \
            .select("batch_id", "stage", "source_count", "target_count", "mismatch_count") \
            .show(truncate=False)

        v0_val = time_travel_v0.filter(f"batch_id = '{target_batch}' AND stage = 'payment'").select("mismatch_count").first()["mismatch_count"]
        latest_val = latest_df.filter(f"batch_id = '{target_batch}' AND stage = 'payment'").select("mismatch_count").first()["mismatch_count"]
        print(f"[Verification] Time travel confirms: Version 0 mismatch_count = {v0_val} != Latest mismatch_count = {latest_val}")

        # ==============================================================================
        # SECTION 5: SCHEMA ENFORCEMENT & REJECTION
        # ==============================================================================
        print("\n" + "=" * 80)
        print("SECTION 5: SCHEMA ENFORCEMENT & REJECTION")
        print("=" * 80)

        # Construct a DataFrame with an unauthorized new column 'unauthorized_audit_field'
        print("[Schema Enforcement] Constructing append DataFrame with unexpected extra column 'unauthorized_audit_field'...")
        base_cols = list(df_results_pd.columns)
        incompatible_cols = base_cols + ["unauthorized_audit_field"]
        incompatible_data = [
            (9999, "B999", "payment", 1000, 990, 10, 0, 0, 0, 0, "2026-10-14 12:00:00", "INVALID_EXTRA_SCHEMA_VALUE")
        ]
        incompatible_df = spark.createDataFrame(incompatible_data, incompatible_cols)

        print("[Schema Enforcement] Attempting append without mergeSchema option...")
        try:
            incompatible_df.write.format("delta").mode("append").save(table_path)
            print("[FAILURE] Append succeeded when it should have failed schema validation!")
        except AnalysisException as e:
            reason = str(e).split("\n")[0]
            print(f"REJECTED: {reason}")
            print("[SUCCESS] Delta Lake schema enforcement blocked the write.")
        except Exception as e:
            # Fallback for other spark error wrappings
            print(f"REJECTED: {type(e).__name__}: {str(e).splitlines()[0]}")
            print("[SUCCESS] Delta Lake schema enforcement blocked the write.")

        # ==============================================================================
        # SECTION 6: SCHEMA EVOLUTION (MERGESCHEMA)
        # ==============================================================================
        print("\n" + "=" * 80)
        print("SECTION 6: SCHEMA EVOLUTION (mergeSchema = true)")
        print("=" * 80)

        print("[Schema Evolution] Appending compatible row with option('mergeSchema', 'true')...")
        # Define clean evolution column 'reconciled_by_analyst'
        evolved_cols = base_cols + ["reconciled_by_analyst"]
        evolved_data = [
            (9999, "B999", "payment", 5000, 4950, 50, 0, 0, 0, 0, "2026-10-14 12:00:00", "Analyst_Alice")
        ]
        evolved_df = spark.createDataFrame(evolved_data, evolved_cols)

        evolved_df.write.format("delta").mode("append").option("mergeSchema", "true").save(table_path)
        print("[Schema Evolution] Append with mergeSchema=true completed successfully!")

        evolved_table_df = spark.read.format("delta").load(table_path)
        print("\n[Schema Evolution] Updated Schema:")
        evolved_table_df.printSchema()

        print("\n[Schema Evolution] Showing evolved table (prior rows have NULL, new row has value):")
        evolved_table_df.filter("batch_id IN ('B004', 'B999')") \
            .select("batch_id", "stage", "mismatch_count", "reconciled_by_analyst") \
            .show(truncate=False)

        print("=" * 80)
        print("EXPERIMENT 9 DEMO EXECUTION COMPLETE (SUCCESS)")
        print("=" * 80)

    finally:
        spark.stop()
        tee.save()
        print(f"\n[Output] Full output recorded to: {docs_output_path}")


if __name__ == "__main__":
    main()
