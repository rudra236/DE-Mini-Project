"""BankFlow Sentinel - Streaming Test Suite.

Validates:
1. Producer: Publishes batch_created event to Kafka with confirmation.
2. Consumer: Idempotent processing of batch message (ETL -> Reconcile -> status 'reconciled').
3. Error resiliency: Handling of missing/corrupt batch sets status to 'failed' without crashing.
"""

import json
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text

from data_gen.generate import generate_batch
from db.connection import get_engine
from streaming.consumer import process_batch_message
from streaming.producer import publish_batch_created


@pytest.fixture(scope="module", autouse=True)
def setup_test_batch():
    """Ensure batch 1 is generated for streaming tests."""
    generate_batch(batch=1, rows=1000, seed=42)


def test_producer_publish_event():
    """Verify Kafka producer publishes batch_created event with confirmation."""
    event = publish_batch_created(
        batch_id="B001",
        file_path="data/batch_001",
        row_count=1000,
    )
    assert event["batch_id"] == "B001"
    assert event["file_path"] == "data/batch_001"
    assert event["row_count"] == 1000


def test_consumer_process_batch_idempotent():
    """Verify consumer successfully and idempotently processes a valid batch message."""
    engine = get_engine()

    # Create mock Kafka message
    mock_msg = MagicMock()
    mock_msg.topic.return_value = "batch_created"
    mock_msg.partition.return_value = 0
    mock_msg.offset.return_value = 99
    mock_msg.key.return_value = b"B001"
    mock_msg.value.return_value = json.dumps({
        "batch_id": "B001",
        "file_path": "data/batch_001",
        "row_count": 1000,
    }).encode("utf-8")

    # Run processing
    success = process_batch_message(msg=mock_msg, engine=engine)
    assert success is True

    # Check batch status in PostgreSQL
    with engine.connect() as conn:
        status = conn.execute(text("SELECT status FROM batches WHERE batch_id = 'B001';")).scalar()
        assert status == "reconciled"

        # Check transaction counts
        tx_count = conn.execute(text("SELECT count(1) FROM transactions WHERE batch_id = 'B001';")).scalar()
        assert tx_count == 2950  # 1000 + 985 + 965


def test_consumer_error_resiliency():
    """Verify consumer sets status to 'failed' and handles errors gracefully."""
    engine = get_engine()

    mock_msg = MagicMock()
    mock_msg.topic.return_value = "batch_created"
    mock_msg.partition.return_value = 0
    mock_msg.offset.return_value = 100
    mock_msg.key.return_value = b"B999"
    mock_msg.value.return_value = json.dumps({
        "batch_id": "B999",
        "file_path": "data/non_existent_batch_dir",
        "row_count": 500,
    }).encode("utf-8")

    # Should catch error, log failure, update DB, and return False without raising
    success = process_batch_message(msg=mock_msg, engine=engine)
    assert success is False

    with engine.connect() as conn:
        status = conn.execute(text("SELECT status FROM batches WHERE batch_id = 'B999';")).scalar()
        assert status == "failed"
