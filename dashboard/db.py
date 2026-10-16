"""
BankFlow Sentinel - Database & Metrics Data Layer (dashboard/db.py)
Provides cached SQLAlchemy queries (st.cache_data ttl=5) against PostgreSQL.
"""

import json
import os
import socket
from datetime import datetime, timezone

import pandas as pd
import requests
import streamlit as st
from sqlalchemy import create_engine, text


def get_db_url() -> str:
    """Detects PostgreSQL connection URL across host and container environments."""
    user = os.getenv("POSTGRES_USER", "bankflow")
    password = os.getenv("POSTGRES_PASSWORD", "change_me")
    dbname = os.getenv("POSTGRES_DB", "bankflow")

    # Candidate ports to check: 5433 (default mapped host port), 5432 (internal container port)
    for host, port in [
        (os.getenv("POSTGRES_HOST", "localhost"), int(os.getenv("POSTGRES_PORT", 5433))),
        ("127.0.0.1", 5433),
        ("localhost", 5433),
        ("127.0.0.1", 5432),
        ("postgres", 5432),
    ]:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(0.5)
            if sock.connect_ex((host, port)) == 0:
                sock.close()
                return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
            sock.close()
        except Exception:
            pass

    return f"postgresql://{user}:{password}@localhost:5433/{dbname}"


@st.cache_resource
def get_engine():
    """Returns a shared SQLAlchemy engine with connection pooling."""
    return create_engine(get_db_url(), pool_pre_ping=True, pool_size=5)


# ==============================================================================
# CACHED DATA FETCHERS (TTL = 5 seconds)
# ==============================================================================

@st.cache_data(ttl=5)
def get_all_batches() -> pd.DataFrame:
    """Fetches all batches ordered by created_at descending."""
    engine = get_engine()
    query = """
        SELECT batch_id, file_path, row_count, status, created_at, updated_at
        FROM batches
        ORDER BY created_at DESC
    """
    return pd.read_sql(query, engine)


@st.cache_data(ttl=5)
def get_overview_kpis(batch_id: str = None) -> dict:
    """Calculates top-level KPI metrics."""
    engine = get_engine()
    with engine.connect() as conn:
        # Total batches
        total_batches = conn.execute(text("SELECT COUNT(*) FROM batches")).scalar() or 0

        # Total transactions processed
        if batch_id and batch_id != "All Batches":
            txns_query = text("SELECT COALESCE(SUM(target_count), 0) FROM reconciliation_results WHERE batch_id = :b")
            total_txns = conn.execute(txns_query, {"b": batch_id}).scalar() or 0

            unmatched_query = text("""
                SELECT COALESCE(SUM(missing_count + mismatch_count + duplicate_count), 0)
                FROM reconciliation_results WHERE batch_id = :b
            """)
            unmatched = conn.execute(unmatched_query, {"b": batch_id}).scalar() or 0

            active_inc_query = text("SELECT COUNT(*) FROM incidents WHERE batch_id = :b AND status != 'resolved'")
            active_incidents = conn.execute(active_inc_query, {"b": batch_id}).scalar() or 0
        else:
            total_txns = conn.execute(text("SELECT COALESCE(SUM(target_count), 0) FROM reconciliation_results")).scalar() or 0
            unmatched = conn.execute(text("SELECT COALESCE(SUM(missing_count + mismatch_count + duplicate_count), 0) FROM reconciliation_results")).scalar() or 0
            active_incidents = conn.execute(text("SELECT COUNT(*) FROM incidents WHERE status != 'resolved'")).scalar() or 0

        # Health rate: percentage of processed records that are clean
        if total_txns > 0:
            health_pct = max(0.0, round(((total_txns - unmatched) / total_txns) * 100, 2))
        else:
            health_pct = 100.0

    return {
        "total_batches": total_batches,
        "total_transactions": total_txns,
        "unmatched_records": unmatched,
        "active_incidents": active_incidents,
        "health_pct": health_pct
    }


@st.cache_data(ttl=5)
def get_funnel_counts(batch_id: str) -> dict:
    """Fetches row counts across stages (source -> payment -> settlement) for funnel chart."""
    engine = get_engine()
    query = text("""
        SELECT stage, source_count, target_count
        FROM reconciliation_results
        WHERE batch_id = :b
    """)
    df = pd.read_sql(query, engine, params={"b": batch_id})
    if df.empty:
        # Fallback to transactions counts
        txn_query = text("""
            SELECT stage, COUNT(*) as cnt
            FROM transactions
            WHERE batch_id = :b
            GROUP BY stage
        """)
        tdf = pd.read_sql(txn_query, engine, params={"b": batch_id})
        tmap = dict(zip(tdf["stage"], tdf["cnt"]))
        return {
            "source": tmap.get("source", 10000),
            "payment": tmap.get("payment", 9850),
            "settlement": tmap.get("settlement", 9700)
        }

    # Extract source count from payment stage row, target_count from payment, target_count from settlement
    pay_row = df[df["stage"] == "payment"]
    set_row = df[df["stage"] == "settlement"]

    src_cnt = int(pay_row["source_count"].iloc[0]) if not pay_row.empty else 10000
    pay_cnt = int(pay_row["target_count"].iloc[0]) if not pay_row.empty else 9850
    set_cnt = int(set_row["target_count"].iloc[0]) if not set_row.empty else 9700

    return {
        "source": src_cnt,
        "payment": pay_cnt,
        "settlement": set_cnt
    }


@st.cache_data(ttl=5)
def get_reconciliation_results(batch_id: str = None) -> pd.DataFrame:
    """Fetches reconciliation stage comparison metrics."""
    engine = get_engine()
    if batch_id and batch_id != "All Batches":
        query = text("""
            SELECT id, batch_id, stage, source_count, target_count, missing_count,
                   duplicate_count, mismatch_count, delayed_count, invalid_count, created_at
            FROM reconciliation_results
            WHERE batch_id = :b
            ORDER BY id
        """)
        return pd.read_sql(query, engine, params={"b": batch_id})
    else:
        query = """
            SELECT id, batch_id, stage, source_count, target_count, missing_count,
                   duplicate_count, mismatch_count, delayed_count, invalid_count, created_at
            FROM reconciliation_results
            ORDER BY batch_id, id
        """
        return pd.read_sql(query, engine)


@st.cache_data(ttl=5)
def get_pipeline_runs(batch_id: str = None) -> pd.DataFrame:
    """Fetches pipeline run executions and duration metrics."""
    engine = get_engine()
    if batch_id and batch_id != "All Batches":
        query = text("""
            SELECT run_id, batch_id, task_name, status, started_at, finished_at,
                   retries, details,
                   ROUND(EXTRACT(EPOCH FROM (finished_at - started_at))::numeric, 2) AS duration_seconds
            FROM pipeline_runs
            WHERE batch_id = :b
            ORDER BY started_at ASC
        """)
        return pd.read_sql(query, engine, params={"b": batch_id})
    else:
        query = """
            SELECT run_id, batch_id, task_name, status, started_at, finished_at,
                   retries, details,
                   ROUND(EXTRACT(EPOCH FROM (finished_at - started_at))::numeric, 2) AS duration_seconds
            FROM pipeline_runs
            ORDER BY started_at DESC
            LIMIT 50
        """
        return pd.read_sql(query, engine)


@st.cache_data(ttl=5)
def get_incidents(batch_id: str = None, status_filter: str = None, severity_filter: str = None) -> pd.DataFrame:
    """Fetches incidents with evidence JSONB."""
    engine = get_engine()
    clauses = []
    params = {}

    if batch_id and batch_id != "All Batches":
        clauses.append("batch_id = :b")
        params["b"] = batch_id
    if status_filter and status_filter != "All":
        clauses.append("status = :st")
        params["st"] = status_filter
    if severity_filter and severity_filter != "All":
        clauses.append("severity = :sev")
        params["sev"] = severity_filter

    where_sql = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    query = text(f"""
        SELECT id, batch_id, stage, type, severity, evidence, detected_at, status, resolved_at, notes
        FROM incidents
        {where_sql}
        ORDER BY detected_at DESC
    """)
    return pd.read_sql(query, engine, params=params)


def update_incident(incident_id: str, new_status: str, notes: str) -> bool:
    """Updates incident status and notes directly in PostgreSQL."""
    engine = get_engine()
    with engine.begin() as conn:
        resolved_at = datetime.now(timezone.utc) if new_status == "resolved" else None
        update_query = text("""
            UPDATE incidents
            SET status = :s,
                notes = :n,
                resolved_at = :r
            WHERE id = :id
        """)
        conn.execute(update_query, {
            "s": new_status,
            "n": notes,
            "r": resolved_at,
            "id": incident_id
        })
    # Invalidate cached queries
    get_incidents.clear()
    get_overview_kpis.clear()
    return True


@st.cache_data(ttl=5)
def get_stage_discrepancy_details(batch_id: str, stage: str, discrepancy_type: str) -> pd.DataFrame:
    """Extracts granular transaction records matching a specific discrepancy."""
    engine = get_engine()
    # Find incident evidence for this batch, stage, and type
    query = text("""
        SELECT evidence
        FROM incidents
        WHERE batch_id = :b AND stage = :s AND type = :t
        LIMIT 1
    """)
    with engine.connect() as conn:
        res = conn.execute(query, {"b": batch_id, "s": stage, "t": discrepancy_type}).scalar()

    if not res:
        return pd.DataFrame()

    txn_ids = res if isinstance(res, list) else json.loads(res) if isinstance(res, str) else []
    if not txn_ids:
        return pd.DataFrame()

    # For missing records, the transaction exists in the UPSTREAM stage, not downstream!
    query_stage = ("source" if stage == "payment" else "payment") if discrepancy_type == "missing_record" else stage

    # Query transaction rows
    txn_sql = text("""
        SELECT txn_id, account_id, amount, currency, ts, status, is_valid, invalid_reason
        FROM transactions
        WHERE batch_id = :b AND stage = :s AND txn_id = ANY(:ids)
        ORDER BY txn_id
        LIMIT 200
    """)
    return pd.read_sql(txn_sql, engine, params={"b": batch_id, "s": query_stage, "ids": txn_ids})


@st.cache_data(ttl=5)
def get_amount_mismatches(batch_id: str, stage: str = "payment") -> pd.DataFrame:
    """Joins upstream and downstream transactions to show exact amount differences."""
    engine = get_engine()
    upstream_stage = "source" if stage == "payment" else "payment"
    query = text("""
        SELECT s.txn_id, s.account_id,
               s.amount AS upstream_amount,
               t.amount AS downstream_amount,
               ROUND(t.amount - s.amount, 2) AS delta,
               s.currency,
               s.ts AS upstream_ts,
               t.ts AS downstream_ts
        FROM transactions s
        JOIN transactions t ON s.txn_id = t.txn_id AND s.batch_id = t.batch_id
        WHERE s.batch_id = :b
          AND s.stage = :up
          AND t.stage = :down
          AND s.amount != t.amount
        ORDER BY s.txn_id
        LIMIT 100
    """)
    return pd.read_sql(query, engine, params={"b": batch_id, "up": upstream_stage, "down": stage})


@st.cache_data(ttl=5)
def get_historical_trends() -> dict:
    """Calculates historical multi-batch trend data."""
    engine = get_engine()
    daily_query = """
        SELECT DATE(created_at) AS day,
               SUM(missing_count) AS total_missing,
               SUM(duplicate_count) AS total_duplicates,
               SUM(mismatch_count) AS total_mismatches,
               SUM(delayed_count) AS total_delayed
        FROM reconciliation_results
        GROUP BY DATE(created_at)
        ORDER BY day ASC
    """
    df_daily = pd.read_sql(daily_query, engine)

    batch_type_query = """
        SELECT batch_id, type, COUNT(*) as count
        FROM incidents
        GROUP BY batch_id, type
        ORDER BY batch_id, type
    """
    df_batch_type = pd.read_sql(batch_type_query, engine)

    return {
        "daily": df_daily,
        "batch_type": df_batch_type
    }


@st.cache_data(ttl=5)
def get_batch_summary_view() -> pd.DataFrame:
    """Reads v_batch_summary containing LAG and window metrics."""
    engine = get_engine()
    try:
        query = "SELECT * FROM v_batch_summary ORDER BY batch_id"
        return pd.read_sql(query, engine)
    except Exception:
        # Fallback if view not present
        return pd.DataFrame()


@st.cache_data(ttl=5)
def get_olap_rollup() -> pd.DataFrame:
    """Executes the OLAP grouping sets and rollup query."""
    engine = get_engine()
    query = """
        SELECT
            COALESCE(stage, 'ALL STAGES') AS stage,
            COALESCE(severity, 'ALL SEVERITIES') AS severity,
            COUNT(*) AS total_incidents,
            CASE GROUPING(stage, severity)
                WHEN 3 THEN 'Grand Total'
                WHEN 1 THEN 'Stage Subtotal'
                WHEN 2 THEN 'Severity Subtotal'
                ELSE 'Detail Row'
            END AS grouping_level
        FROM incidents
        GROUP BY CUBE(stage, severity)
        ORDER BY GROUPING(stage, severity) DESC, total_incidents DESC
    """
    try:
        return pd.read_sql(query, engine)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=5)
def get_quality_metrics(batch_id: str) -> dict:
    """Calculates data quality scores and violation rates."""
    engine = get_engine()
    query = text("""
        SELECT
            COUNT(*) as total_rows,
            COUNT(*) FILTER (WHERE amount IS NULL) as null_amount_rows,
            COUNT(*) FILTER (WHERE is_valid = false) as invalid_rows
        FROM transactions
        WHERE batch_id = :b
    """)
    with engine.connect() as conn:
        row = conn.execute(query, {"b": batch_id}).mappings().first()

    total = row["total_rows"] if row and row["total_rows"] else 1
    nulls = row["null_amount_rows"] if row and row["null_amount_rows"] else 0
    invalids = row["invalid_rows"] if row and row["invalid_rows"] else 0

    # Duplicates from reconciliation_results
    dup_query = text("SELECT COALESCE(SUM(duplicate_count), 0) FROM reconciliation_results WHERE batch_id = :b")
    with engine.connect() as conn:
        dups = conn.execute(dup_query, {"b": batch_id}).scalar() or 0

    null_rate = round((nulls / total) * 100, 2)
    invalid_rate = round((invalids / total) * 100, 2)
    dup_rate = round((dups / total) * 100, 2)

    # Composite quality score (100 - penalties)
    score = max(0, min(100, round(100 - (null_rate * 2) - (invalid_rate * 1.5) - (dup_rate * 2), 1)))

    return {
        "score": score,
        "null_rate": null_rate,
        "invalid_rate": invalid_rate,
        "dup_rate": dup_rate,
        "total_rows": total
    }


def check_system_services() -> dict:
    """Checks the health of PostgreSQL, MongoDB, Kafka, and FastAPI."""
    status = {"postgres": False, "mongo": False, "kafka": False, "api": False}

    # 1. Postgres check
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        status["postgres"] = True
    except Exception:
        status["postgres"] = False

    # 2. FastAPI and dependent services check
    try:
        r = requests.get("http://127.0.0.1:8000/health", timeout=1.0)
        if r.status_code == 200:
            status["api"] = True
            data = r.json().get("services", {})
            status["mongo"] = data.get("mongo", {}).get("status") == "pass"
            status["kafka"] = data.get("kafka", {}).get("status") == "pass"
    except Exception:
        # Direct port probes as fallback
        for name, port in [("mongo", 27017), ("kafka", 9092)]:
            try:
                s = socket.socket()
                s.settimeout(0.5)
                if s.connect_ex(("127.0.0.1", port)) == 0:
                    status[name] = True
                s.close()
            except Exception:
                pass

    return status
