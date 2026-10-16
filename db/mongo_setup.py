"""BankFlow Sentinel - MongoDB Setup & Client Management.

Manages connections and schema indexes for NoSQL event documents:
- raw_events: Ingested event JSON + received_at (Indexed on batch_id)
- source_metadata: Source registration metadata (Unique index on source_id)
"""

import logging
import os
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from pymongo import ASCENDING, MongoClient
from pymongo.database import Database

load_dotenv()

logger = logging.getLogger("db.mongo_setup")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB", "bankflow")

_client: Optional[MongoClient] = None


def get_mongo_client() -> MongoClient:
    """Return a singleton MongoClient instance."""
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    return _client


def get_mongo_db(db_name: Optional[str] = None) -> Database:
    """Return the BankFlow MongoDB database instance."""
    client = get_mongo_client()
    return client[db_name or MONGO_DB_NAME]


def init_mongo() -> Dict[str, Any]:
    """Create collections and indexes according to system contracts.

    Indexes:
    - raw_events: index on batch_id
    - source_metadata: unique index on source_id
    """
    db = get_mongo_db()

    # 1. raw_events indexes
    raw_col = db["raw_events"]
    raw_col.create_index([("batch_id", ASCENDING)], name="idx_raw_events_batch_id")

    # 2. source_metadata indexes
    source_col = db["source_metadata"]
    source_col.create_index([("source_id", ASCENDING)], unique=True, name="idx_source_metadata_source_id_unique")

    indexes_info = {
        "raw_events": raw_col.index_information(),
        "source_metadata": source_col.index_information(),
    }

    logger.info("MongoDB collections and indexes initialized successfully.")
    return indexes_info


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    info = init_mongo()
    print("MongoDB Initialization Complete:")
    print(info)
