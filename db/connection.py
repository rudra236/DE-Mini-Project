"""BankFlow Sentinel - Database Connection Management.

Provides shared SQLAlchemy engine and psycopg2 connection helpers.
"""

import os
from contextlib import contextmanager

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER", "bankflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "change_me")
POSTGRES_DB = os.getenv("POSTGRES_DB", "bankflow")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5433")

DEFAULT_DATABASE_URL = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)


def get_engine() -> Engine:
    """Return a SQLAlchemy engine connected to the configured PostgreSQL instance."""
    return create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
    )


@contextmanager
def get_db_connection():
    """Context manager for obtaining a raw DB-API connection for bulk operations."""
    engine = get_engine()
    raw_conn = engine.raw_connection()
    try:
        yield raw_conn
    finally:
        raw_conn.close()
