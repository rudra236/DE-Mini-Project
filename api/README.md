# FastAPI Ingestion & Monitoring Service (`api/`)

FastAPI REST service validating incoming events, persisting raw documents in MongoDB, notifying Kafka, and querying PostgreSQL reconciliation and incident states.

## How to Run
```bash
python -m uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```
Interactive Swagger UI: [http://localhost:8000/docs](http://localhost:8000/docs)  
ReDoc UI: [http://localhost:8000/redoc](http://localhost:8000/redoc)

## Endpoints

| Method | Path | Purpose | Success Code |
|---|---|---|---|
| `POST` | `/events` | Ingest batch metadata, store raw payload in Mongo, publish to Kafka | `202 Accepted` |
| `POST` | `/sources` | Register upstream source system in MongoDB | `201 Created` |
| `GET` | `/reconciliation/{batch_id}` | Query reconciliation summary and unmatched IDs | `200 OK` |
| `GET` | `/incidents` | Query incident tickets with status/batch/type filters | `200 OK` |
| `PATCH` | `/incidents/{id}` | Update incident resolution status and notes | `200 OK` |
| `GET` | `/health` | Check live connectivity to Postgres, Mongo, and Kafka | `200 OK` |

## Validation & Error Handling
All endpoints enforce Pydantic v2 schemas. Invalidation triggers a descriptive 422 Unprocessable Entity response:
```json
{
  "error": "Validation Error: Request payload violates contract constraints.",
  "details": [
    {
      "field": "body -> batch_id",
      "message": "String should match pattern '^B\\d{3,}$'",
      "type": "string_pattern_mismatch"
    }
  ]
}
```
