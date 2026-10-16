"""BankFlow Sentinel - Kafka Event Consumer.

Consumes 'batch_created' events from Kafka under consumer group 'bankflow-consumers',
triggers atomic ETL ingestion and reconciliation, logs every processing hop
to console and logs/streaming.log, and handles commits manually after successful processing.
"""

import argparse
import json
import logging
import os
import signal
import sys
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

# Ensure project root is in sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
from sqlalchemy import text

from data_gen.utils import normalize_batch
from db.connection import get_engine
from etl.load import run_etl
from reconcile.engine import reconcile

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_BATCH_CREATED = "batch_created"
CONSUMER_GROUP = "bankflow-consumers"

# Setup logs directory and file
LOGS_DIR = ROOT_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)
STREAMING_LOG_FILE = LOGS_DIR / "streaming.log"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("streaming.consumer")

# Control flag for graceful shutdown
running = True


def signal_handler(signum, frame):
    """Handle termination signals for clean consumer shutdown."""
    global running
    sig_name = "SIGINT" if signum == signal.SIGINT else f"SIGNAL {signum}"
    log_hop("SHUTDOWN", "SYSTEM", f"Received {sig_name}. Initiating graceful consumer shutdown...")
    running = False


def log_hop(hop: str, batch_id: str, message: str) -> None:
    """Log structured hop to both console and logs/streaming.log synchronously."""
    now_iso = datetime.now(timezone.utc).isoformat()
    log_line = f"[{now_iso}] [{hop:11s}] batch_id={batch_id:<8s} {message}"
    print(log_line, flush=True)
    try:
        with open(STREAMING_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(log_line + "\n")
            f.flush()
    except Exception as exc:
        print(f"Failed writing to streaming log file: {exc}", file=sys.stderr)


def process_batch_message(msg, engine) -> bool:
    """Process a single Kafka batch_created message through ETL and Reconciliation.

    Hops:
    1. Log "RECEIVED"
    2. Set batches.status='ingested' (create row if absent) and write pipeline_runs row
    3. Call run_etl(batch_id) -> log "ETL DONE"
    4. Call reconcile(batch_id) -> log "RECONCILED"
    5. Set status final ('reconciled'); on exception set status 'failed', log traceback, keep consuming.

    Returns:
        True if processing succeeded and can be committed, False otherwise.
    """
    batch_id: Optional[str] = None
    run_id: Optional[str] = None

    try:
        # Decode and parse payload
        raw_val = msg.value().decode("utf-8")
        payload: Dict[str, Any] = json.loads(raw_val)

        raw_batch_id = payload.get("batch_id")
        if not raw_batch_id:
            raw_key = msg.key().decode("utf-8") if msg.key() else "UNKNOWN"
            raw_batch_id = raw_key

        norm_id, dir_name = normalize_batch(raw_batch_id)
        batch_id = norm_id
        file_path = payload.get("file_path", f"data/{dir_name}")
        row_count = payload.get("row_count", 0)

        # 1. Log "RECEIVED"
        log_hop(
            "RECEIVED",
            batch_id,
            f"file_path={file_path} row_count={row_count} topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}",
        )

        now_utc = datetime.now(timezone.utc)
        run_id = f"run_stream_{batch_id}_{uuid.uuid4().hex[:8]}"

        # 2. Set batches.status='ingested' and write pipeline_runs row
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO batches (batch_id, file_path, row_count, status, created_at, updated_at)
                    VALUES (:b, :fp, :rc, 'ingested', :now, :now)
                    ON CONFLICT (batch_id) DO UPDATE SET
                        file_path = EXCLUDED.file_path,
                        row_count = EXCLUDED.row_count,
                        status = 'ingested',
                        updated_at = EXCLUDED.updated_at;
                """),
                {
                    "b": batch_id,
                    "fp": file_path,
                    "rc": row_count,
                    "now": now_utc,
                },
            )
            conn.execute(
                text("""
                    INSERT INTO pipeline_runs (run_id, batch_id, task_name, status, started_at, details)
                    VALUES (:rid, :b, 'streaming_consumer', 'running', :now, :details);
                """),
                {
                    "rid": run_id,
                    "b": batch_id,
                    "now": now_utc,
                    "details": json.dumps({
                        "source": "kafka_stream",
                        "topic": msg.topic(),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                    }),
                },
            )

        log_hop("STATUS", batch_id, f"status=ingested pipeline_run_id={run_id}")

        use_prefect = os.getenv("USE_PREFECT", "false").lower() in ("true", "1", "yes")
        if use_prefect:
            from orchestration.flow import bankflow_pipeline
            log_hop("PREFECT", batch_id, "USE_PREFECT=true detected. Executing batch processing via Prefect flow 'bankflow-pipeline'...")
            flow_summary = bankflow_pipeline(batch_id=batch_id)
            rec_result = flow_summary["reconcile"]
            etl_result = flow_summary["validate"]["etl"]
            log_hop("PREFECT DONE", batch_id, f"Flow status={flow_summary.get('status')} report={flow_summary['report'].get('report_file')}")
        else:
            # 3. Call run_etl(batch_id) -> log "ETL DONE"
            etl_result = run_etl(batch_id=batch_id)
            stage_counts = etl_result.get("stage_counts", {})
            counts_str = " ".join([f"{k}={v}" for k, v in stage_counts.items()])
            log_hop(
                "ETL DONE",
                batch_id,
                f"total_rows={etl_result.get('total_rows')} {counts_str} duration_s={etl_result.get('duration_seconds')}",
            )

            # 4. Call reconcile(batch_id) -> log "RECONCILED"
            rec_result = reconcile(batch_id=batch_id)
            inc_types = rec_result.get("incident_counts_by_type", {})
            inc_str = " ".join([f"{k}={v}" for k, v in inc_types.items()]) if inc_types else "none"
            log_hop(
                "RECONCILED",
                batch_id,
                f"total_incidents={rec_result.get('total_incidents')} discrepancies=[{inc_str}] status={rec_result.get('status')}",
            )

        # 5. Set status final ('reconciled') and mark pipeline_run as success
        with engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE batches SET status = 'reconciled', updated_at = NOW() WHERE batch_id = :b;
                    UPDATE pipeline_runs SET status = 'success', finished_at = NOW() WHERE run_id = :rid;
                """),
                {"b": batch_id, "rid": run_id},
            )

        log_hop("STATUS", batch_id, "status=reconciled")
        return True

    except Exception as exc:
        tb = traceback.format_exc()
        err_target = batch_id or "UNKNOWN"
        log_hop("ERROR", err_target, f"Processing failed: {exc}")
        for tb_line in tb.strip().splitlines()[-5:]:
            log_hop("TRACEBACK", err_target, tb_line)

        # Set status 'failed' in PostgreSQL
        try:
            if batch_id:
                with engine.begin() as conn:
                    conn.execute(
                        text("UPDATE batches SET status = 'failed', updated_at = NOW() WHERE batch_id = :b;"),
                        {"b": batch_id},
                    )
                    if run_id:
                        conn.execute(
                            text("""
                                UPDATE pipeline_runs SET status = 'failed', finished_at = NOW(),
                                details = jsonb_set(COALESCE(details, '{}'::jsonb), '{error}', to_jsonb(CAST(:err AS text)))
                                WHERE run_id = :rid;
                            """),
                            {"err": str(exc), "rid": run_id},
                        )
                log_hop("STATUS", batch_id, "status=failed")
        except Exception as db_exc:
            log_hop("ERROR", err_target, f"Failed updating error status in DB: {db_exc}")

        return False


def start_consumer(
    bootstrap_servers: Optional[str] = None,
    group_id: str = CONSUMER_GROUP,
    topic: str = TOPIC_BATCH_CREATED,
    max_messages: Optional[int] = None,
    timeout_seconds: Optional[float] = None,
) -> None:
    """Run the long-running BankFlow Kafka streaming consumer."""
    import time
    global running

    servers = bootstrap_servers or KAFKA_BOOTSTRAP
    engine = get_engine()

    conf = {
        "bootstrap.servers": servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # Manual commit after successful processing
        "session.timeout.ms": 15000,
        "max.poll.interval.ms": 300000,
    }

    consumer = Consumer(conf)
    consumer.subscribe([topic])

    log_hop(
        "STARTUP",
        "SYSTEM",
        f"BankFlow Kafka Consumer initialized (group='{group_id}', topic='{topic}', bootstrap='{servers}')",
    )

    processed_count = 0
    start_time = time.time()

    try:
        while running:
            if timeout_seconds and (time.time() - start_time) >= timeout_seconds:
                log_hop("SHUTDOWN", "SYSTEM", f"Reached timeout limit ({timeout_seconds}s). Stopping.")
                break

            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    log_hop("KAFKA_ERROR", "SYSTEM", f"Consumer error encountered: {msg.error()}")
                    continue

            # Process the message through our pipeline hops
            success = process_batch_message(msg=msg, engine=engine)

            if success:
                # Manual commit AFTER successful processing
                consumer.commit(message=msg, asynchronous=False)
                key_str = msg.key().decode("utf-8") if msg.key() else "N/A"
                log_hop("COMMITTED", key_str, f"partition={msg.partition()} offset={msg.offset()}")
            else:
                # On failure, commit to prevent poison-pill infinite reprocessing, while keeping consuming
                consumer.commit(message=msg, asynchronous=False)
                key_str = msg.key().decode("utf-8") if msg.key() else "N/A"
                log_hop("COMMITTED_ERR", key_str, f"partition={msg.partition()} offset={msg.offset()} (committed after failure recorded)")

            processed_count += 1
            if max_messages and processed_count >= max_messages:
                log_hop("SHUTDOWN", "SYSTEM", f"Reached max_messages limit ({max_messages}). Stopping.")
                break

    except KeyboardInterrupt:
        log_hop("SHUTDOWN", "SYSTEM", "KeyboardInterrupt detected. Terminating consumer...")
    finally:
        log_hop("SHUTDOWN", "SYSTEM", "Closing Kafka consumer...")
        consumer.close()
        log_hop("SHUTDOWN", "SYSTEM", "Consumer closed cleanly.")


def main():
    parser = argparse.ArgumentParser(description="BankFlow Sentinel - Streaming Kafka Consumer")
    parser.add_argument("--bootstrap", default=None, help="Kafka bootstrap servers")
    parser.add_argument("--group", default=CONSUMER_GROUP, help="Consumer group ID")
    parser.add_argument("--topic", default=TOPIC_BATCH_CREATED, help="Kafka topic to consume")
    parser.add_argument("--max-messages", type=int, default=None, help="Stop after consuming N messages")
    parser.add_argument("--timeout", type=float, default=None, help="Stop after N seconds of runtime")

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    args = parser.parse_args()
    start_consumer(
        bootstrap_servers=args.bootstrap,
        group_id=args.group,
        topic=args.topic,
        max_messages=args.max_messages,
        timeout_seconds=args.timeout,
    )


if __name__ == "__main__":
    main()
