"""BankFlow Sentinel - FastAPI Ingestion & Monitoring Service.

Endpoints:
- POST /events: Ingest batch metadata, store raw document in Mongo, publish to Kafka
- POST /sources: Register upstream source metadata in Mongo
- GET /reconciliation/{batch_id}: Retrieve reconciliation summary and unmatched IDs
- GET /incidents: List discrepancy tickets with status/batch filters
- PATCH /incidents/{id}: Update incident status ('open', 'investigating', 'resolved')
- GET /health: Live connectivity verification across Postgres, Mongo, and Kafka
"""

import sys
from pathlib import Path

# Ensure project root is on sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.routes import events, health, incidents, reconciliation
from db.mongo_setup import init_mongo


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Ensure database collections and indexes are initialized on startup."""
    try:
        init_mongo()
    except Exception as exc:
        print(f"Notice: Initializing Mongo during startup caught: {exc}")
    yield


app = FastAPI(
    title="BankFlow Sentinel API",
    description="""
**BankFlow Sentinel**: Real-Time Banking Data Pipeline Monitoring, Failure Detection, and Transaction Reconciliation Platform.

> **Disclaimer**: Synthetic data ONLY. A discrepancy is a data-processing issue to investigate, not proof of lost money. No real bank accounts are connected.
""",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# Enable Cross-Origin Resource Sharing (CORS)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Return clean, human-readable 422 error details for rejected payloads."""
    errors_summary = []
    for err in exc.errors():
        field_path = " -> ".join(str(loc) for loc in err.get("loc", []))
        errors_summary.append({
            "field": field_path,
            "message": str(err.get("msg", "Invalid value")),
            "type": str(err.get("type", "value_error")),
        })

    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": "Validation Error: Request payload violates contract constraints.",
            "details": errors_summary,
            "raw_errors": jsonable_encoder(exc.errors()),
        },
    )


# Include Routers
app.include_router(events.router)
app.include_router(reconciliation.router)
app.include_router(incidents.router)
app.include_router(health.router)


@app.get("/", tags=["Root"])
def root():
    return {
        "service": "BankFlow Sentinel API",
        "version": "1.0.0",
        "status": "online",
        "documentation": "/docs",
        "disclaimer": "Synthetic data ONLY. A discrepancy is a data-processing issue to investigate, not proof of lost money.",
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)
