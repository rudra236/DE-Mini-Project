"""BankFlow Sentinel - Infrastructure Stack Verification Script.

Tests connectivity to PostgreSQL, MongoDB, and Kafka:
1. PostgreSQL: Executes 'SELECT 1;' query.
2. MongoDB: Inserts and reads back a verification document in 'raw_events_check'.
3. Kafka: Produces and consumes a verification message on 'stack_check_topic'.
"""

import os
import sys
import time
import uuid
from typing import Dict, Tuple

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER", "bankflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "change_me")
POSTGRES_DB = os.getenv("POSTGRES_DB", "bankflow")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}",
)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")


def check_postgres() -> Tuple[bool, str]:
    """Test PostgreSQL connection and basic query execution."""
    try:
        import psycopg2

        conn = psycopg2.connect(
            DATABASE_URL,
            connect_timeout=5,
        )
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        res = cur.fetchone()
        cur.close()
        conn.close()

        if res and res[0] == 1:
            return True, "Successfully connected and executed 'SELECT 1;'"
        return False, f"Unexpected query result: {res}"
    except Exception as exc:
        return False, str(exc)


def check_mongo() -> Tuple[bool, str]:
    """Test MongoDB connection by inserting, reading, and deleting a test document."""
    try:
        from pymongo import MongoClient

        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client["bankflow_check"]
        col = db["stack_verification"]

        test_token = f"token_{uuid.uuid4()}"
        doc = {"test_id": test_token, "timestamp": time.time(), "status": "ok"}

        # Insert document
        insert_res = col.insert_one(doc)
        if not insert_res.acknowledged:
            return False, "Failed to insert verification document."

        # Read back document
        found = col.find_one({"test_id": test_token})
        if not found or found.get("test_id") != test_token:
            return False, "Failed to read inserted document back from MongoDB."

        # Clean up document
        col.delete_one({"test_id": test_token})
        client.close()

        return True, "Successfully inserted, retrieved, and deleted test document."
    except Exception as exc:
        return False, str(exc)


def check_kafka() -> Tuple[bool, str]:
    """Test Kafka by producing and consuming a message on a verification topic."""
    test_topic = f"stack_check_{int(time.time())}"
    test_payload = f"ping_{uuid.uuid4()}".encode("utf-8")
    group_id = f"stack_check_group_{uuid.uuid4().hex[:8]}"

    try:
        from confluent_kafka import Consumer, Producer

        # Producer setup
        producer_conf = {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "client.id": "stack-check-producer",
            "retries": 3,
        }
        producer = Producer(producer_conf)

        delivery_report = {"success": False, "error": None}

        def on_delivery(err, msg):
            if err:
                delivery_report["error"] = err
            else:
                delivery_report["success"] = True

        producer.produce(test_topic, test_payload, callback=on_delivery)
        producer.flush(timeout=10)

        if not delivery_report["success"]:
            return False, f"Producer delivery failed: {delivery_report.get('error')}"

        # Consumer setup
        consumer_conf = {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        consumer = Consumer(consumer_conf)
        consumer.subscribe([test_topic])

        received = False
        start_time = time.time()
        timeout_seconds = 15

        while time.time() - start_time < timeout_seconds:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            if msg.value() == test_payload:
                received = True
                break

        consumer.close()

        if received:
            return True, f"Successfully produced and consumed message on topic '{test_topic}'."
        return False, f"Timed out after {timeout_seconds}s waiting for Kafka message."
    except Exception as exc:
        return False, str(exc)


def main() -> int:
    print("=" * 60)
    print(" BankFlow Sentinel - Infrastructure Stack Verification")
    print("=" * 60)

    results: Dict[str, Tuple[bool, str]] = {}

    print("Checking PostgreSQL...")
    results["PostgreSQL"] = check_postgres()

    print("Checking MongoDB...")
    results["MongoDB"] = check_mongo()

    print("Checking Kafka...")
    results["Kafka"] = check_kafka()

    print("\n" + "=" * 60)
    print(" VERIFICATION SUMMARY")
    print("=" * 60)

    all_passed = True
    for service, (passed, detail) in results.items():
        status = "PASS" if passed else "FAIL"
        print(f"[{status}] {service}: {detail}")
        if not passed:
            all_passed = False

    print("=" * 60)
    if all_passed:
        print("OVERALL STATUS: ALL SERVICES PASS")
        return 0
    else:
        print("OVERALL STATUS: AT LEAST ONE SERVICE FAILED")
        return 1


if __name__ == "__main__":
    sys.exit(main())
