"""BankFlow Sentinel - Kafka Event Producer.

Publishes batch lifecycle notifications to Kafka according to docs/contracts.md:
Topic: batch_created
Payload: {"batch_id": "B001", "file_path": "data/batch_001", "row_count": 10000}
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

# Ensure project root is in sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("streaming.producer")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_BATCH_CREATED = "batch_created"


def ensure_topic_exists(
    bootstrap_servers: str,
    topic: str = TOPIC_BATCH_CREATED,
    num_partitions: int = 1,
    replication_factor: int = 1,
) -> None:
    """Ensure the Kafka topic exists, creating it via AdminClient if necessary."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin_conf = {"bootstrap.servers": bootstrap_servers}
    admin = AdminClient(admin_conf)

    try:
        cluster_metadata = admin.list_topics(timeout=5.0)
        if topic not in cluster_metadata.topics:
            logger.info("Topic '%s' not found on Kafka broker. Creating topic...", topic)
            new_topic = NewTopic(
                topic=topic,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
            )
            fs = admin.create_topics([new_topic])
            for t, f in fs.items():
                try:
                    f.result(timeout=5.0)
                    logger.info("Topic '%s' created successfully.", t)
                except Exception as exc:
                    logger.warning("Topic '%s' creation warning/collision: %s", t, exc)
    except Exception as exc:
        logger.error("Failed to verify/create topic '%s' via AdminClient: %s", topic, exc)
        raise RuntimeError(f"AdminClient failed to verify topic '{topic}': {exc}") from exc


def publish_batch_created(
    batch_id: str,
    file_path: str,
    row_count: int,
    bootstrap_servers: Optional[str] = None,
) -> Dict[str, Any]:
    """Publish a batch_created event to Kafka with real delivery confirmation.

    Args:
        batch_id: Normalized batch ID ("B001").
        file_path: Relative or absolute path to batch files ("data/batch_001").
        row_count: Total records in source file.
        bootstrap_servers: Kafka bootstrap server address (default from env).

    Returns:
        The published event dictionary.

    Raises:
        RuntimeError: If Kafka connection or message delivery fails.
    """
    from confluent_kafka import Producer

    servers = bootstrap_servers or KAFKA_BOOTSTRAP
    ensure_topic_exists(servers, TOPIC_BATCH_CREATED)

    event_payload = {
        "batch_id": batch_id,
        "file_path": file_path,
        "row_count": row_count,
    }
    payload_bytes = json.dumps(event_payload).encode("utf-8")

    conf = {
        "bootstrap.servers": servers,
        "client.id": "bankflow-api-producer",
        "retries": 5,
        "socket.timeout.ms": 5000,
        "request.required.acks": "all",
    }
    producer = Producer(conf)

    delivery_error = None
    delivery_msg = None

    def delivery_callback(err, msg):
        nonlocal delivery_error, delivery_msg
        if err is not None:
            delivery_error = err
            logger.error(
                "[PRODUCER] [%s] Delivery failed for batch %s: %s",
                datetime.now(timezone.utc).isoformat(),
                batch_id,
                err,
            )
        else:
            delivery_msg = msg
            logger.info(
                "[PRODUCER] [%s] batch_id=%s delivered to topic=%s partition=%s offset=%s",
                datetime.now(timezone.utc).isoformat(),
                batch_id,
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

    producer.produce(
        topic=TOPIC_BATCH_CREATED,
        key=batch_id.encode("utf-8"),
        value=payload_bytes,
        callback=delivery_callback,
    )
    producer.flush(timeout=10.0)

    if delivery_error is not None:
        raise RuntimeError(f"Kafka message delivery failed for batch '{batch_id}': {delivery_error}")

    logger.info("Successfully produced batch_created event to Kafka: %s", event_payload)
    return event_payload


def main():
    parser = argparse.ArgumentParser(description="BankFlow Sentinel - Kafka Event Producer CLI")
    parser.add_argument("--batch", required=True, help="Batch ID (e.g. B001)")
    parser.add_argument("--path", required=True, help="File path to batch (e.g. data/batch_001)")
    parser.add_argument("--rows", type=int, required=True, help="Row count (e.g. 10000)")
    parser.add_argument("--bootstrap", default=None, help="Kafka bootstrap servers (optional)")

    args = parser.parse_args()
    event = publish_batch_created(
        batch_id=args.batch,
        file_path=args.path,
        row_count=args.rows,
        bootstrap_servers=args.bootstrap,
    )
    print("\n" + "=" * 60)
    print(" KAFKA EVENT PUBLISHED SUCCESSFULLY")
    print("=" * 60)
    print(json.dumps(event, indent=2))


if __name__ == "__main__":
    main()
