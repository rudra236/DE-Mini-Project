"""BankFlow Sentinel - Health Check Routes."""

import os
from datetime import datetime, timezone

from fastapi import APIRouter
from sqlalchemy import text

from api.schemas import HealthCheckResponse, ServiceHealth
from db.connection import get_engine
from db.mongo_setup import get_mongo_client

router = APIRouter(tags=["Health"])
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")


@router.get(
    "/health",
    response_model=HealthCheckResponse,
    summary="Infrastructure Health Status",
    description="Validates live connectivity across PostgreSQL, MongoDB, and Apache Kafka.",
)
def check_health() -> HealthCheckResponse:
    services = {}
    all_healthy = True

    # 1. PostgreSQL check
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1;"))
        services["postgres"] = ServiceHealth(status="pass", detail="PostgreSQL 16 connection responsive.")
    except Exception as exc:
        all_healthy = False
        services["postgres"] = ServiceHealth(status="fail", detail=str(exc))

    # 2. MongoDB check
    try:
        client = get_mongo_client()
        client.admin.command("ping")
        services["mongo"] = ServiceHealth(status="pass", detail="MongoDB 7 ping successful.")
    except Exception as exc:
        all_healthy = False
        services["mongo"] = ServiceHealth(status="fail", detail=str(exc))

    # 3. Kafka check
    try:
        from confluent_kafka import Producer

        p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "socket.timeout.ms": 3000})
        metadata = p.list_topics(timeout=4.0)
        broker_count = len(metadata.brokers)
        services["kafka"] = ServiceHealth(
            status="pass",
            detail=f"Kafka KRaft cluster responsive with {broker_count} active broker(s).",
        )
    except Exception as exc:
        all_healthy = False
        services["kafka"] = ServiceHealth(status="fail", detail=str(exc))

    overall_status = "healthy" if all_healthy else "unhealthy"

    return HealthCheckResponse(
        status=overall_status,
        timestamp=datetime.now(timezone.utc).isoformat(),
        services=services,
    )
