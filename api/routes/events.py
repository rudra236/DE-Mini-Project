"""BankFlow Sentinel - Events & Source Registration Routes."""

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, status
from pymongo.errors import DuplicateKeyError
from sqlalchemy import text

from api.schemas import (
    EventIngestRequest,
    EventIngestResponse,
    SourceMetadataCreate,
    SourceMetadataResponse,
)
from db.connection import get_engine
from db.mongo_setup import get_mongo_db
from streaming.producer import publish_batch_created

logger = logging.getLogger("api.routes.events")
router = APIRouter(tags=["Events & Sources"])


@router.post(
    "/events",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=EventIngestResponse,
    summary="Ingest a new batch event",
    description="Registers an incoming batch event in MongoDB, marks status as 'created' in PostgreSQL, and publishes to Kafka.",
)
def ingest_event(payload: EventIngestRequest) -> EventIngestResponse:
    now_utc = datetime.now(timezone.utc)
    raw_dict = payload.model_dump()
    raw_dict["received_at"] = now_utc.isoformat()

    # 1. Store raw payload in MongoDB raw_events
    try:
        mongo_db = get_mongo_db()
        mongo_db["raw_events"].insert_one(raw_dict.copy())
    except Exception as exc:
        logger.error("Failed to persist raw event in MongoDB: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"MongoDB event persistence failed: {exc}",
        )

    # 2. Upsert batch record in PostgreSQL with status 'created'
    engine = get_engine()
    try:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO batches (batch_id, file_path, row_count, status, created_at, updated_at)
                    VALUES (:b, :fp, :rc, 'created', :now, :now)
                    ON CONFLICT (batch_id) DO UPDATE SET
                        file_path = EXCLUDED.file_path,
                        row_count = EXCLUDED.row_count,
                        status = 'created',
                        updated_at = EXCLUDED.updated_at;
                """),
                {
                    "b": payload.batch_id,
                    "fp": payload.file_path,
                    "rc": payload.row_count,
                    "now": now_utc,
                },
            )
    except Exception as exc:
        logger.error("Failed to upsert batch in PostgreSQL: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"PostgreSQL batch upsert failed: {exc}",
        )

    # 3. Publish to Kafka
    try:
        kafka_event = publish_batch_created(
            batch_id=payload.batch_id,
            file_path=payload.file_path,
            row_count=payload.row_count,
        )
    except Exception as exc:
        logger.error("Failed to publish batch_created event to Kafka: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Kafka publication failed: {exc}",
        )

    return EventIngestResponse(
        status="accepted",
        message=f"Batch {payload.batch_id} registered and published to Kafka.",
        event=kafka_event,
    )


@router.post(
    "/sources",
    status_code=status.HTTP_201_CREATED,
    response_model=SourceMetadataResponse,
    summary="Register a data source",
    description="Registers new upstream transaction source metadata in MongoDB.",
)
def register_source(source: SourceMetadataCreate) -> SourceMetadataResponse:
    mongo_db = get_mongo_db()
    source_col = mongo_db["source_metadata"]
    now_utc = datetime.now(timezone.utc).isoformat()

    doc = source.model_dump()
    doc["created_at"] = now_utc

    try:
        source_col.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Source with source_id '{source.source_id}' is already registered.",
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to register source: {exc}",
        )

    return SourceMetadataResponse(
        source_id=source.source_id,
        name=source.name,
        type=source.type,
        owner=source.owner,
        schema_version=source.schema_version,
        created_at=now_utc,
    )
