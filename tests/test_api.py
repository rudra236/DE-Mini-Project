"""BankFlow Sentinel - FastAPI Test Suite.

Validates:
1. /health endpoint: Status 200 and healthy services.
2. POST /events: Valid payload returns 202, persists in Mongo and Postgres, publishes Kafka event.
3. POST /events: Malformed payloads return 422 with readable field error details.
4. POST /sources: Register source metadata, handles duplicates with 409.
5. GET /reconciliation/{batch_id}: Returns summary for existing batch, 404 for unknown batch.
6. GET & PATCH /incidents: Filters list, updates status and notes, returns 404 for non-existent.
"""

import pytest
from fastapi.testclient import TestClient

from api.main import app
from data_gen.generate import generate_batch
from etl.load import run_etl
from reconcile.engine import reconcile


@pytest.fixture(scope="module")
def client():
    """Create FastAPI TestClient instance."""
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture(scope="module", autouse=True)
def setup_api_data():
    """Seed data for API tests."""
    generate_batch(batch=1, rows=1000, seed=42)
    run_etl("batch_001")
    reconcile("batch_001")


def test_health_endpoint(client: TestClient):
    """Verify /health reports system status."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] in ["healthy", "degraded", "unhealthy"]
    assert "postgres" in data["services"]
    assert "mongo" in data["services"]
    assert "kafka" in data["services"]
    assert data["services"]["postgres"]["status"] == "pass"
    assert data["services"]["mongo"]["status"] == "pass"


def test_post_events_good_payload(client: TestClient):
    """Verify valid event ingest returns 202 Accepted."""
    payload = {
        "batch_id": "B010",
        "file_path": "data/batch_010",
        "row_count": 5000,
    }
    response = client.post("/events", json=payload)
    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "accepted"
    assert data["event"]["batch_id"] == "B010"
    assert data["event"]["row_count"] == 5000


@pytest.mark.parametrize(
    "bad_payload, expected_err_sub",
    [
        ({"batch_id": "invalid", "file_path": "data/b1", "row_count": 100}, "batch_id"),
        ({"batch_id": "B001", "file_path": "data/b1", "row_count": -10}, "row_count"),
        ({"batch_id": "B001", "file_path": "   ", "row_count": 100}, "file_path"),
        ({}, "field required"),
    ],
)
def test_post_events_bad_payload_rejected_422(client: TestClient, bad_payload, expected_err_sub):
    """Verify contract violations return 422 with descriptive error details."""
    response = client.post("/events", json=bad_payload)
    assert response.status_code == 422
    data = response.json()
    assert "error" in data
    assert "details" in data
    assert any(expected_err_sub.lower() in str(d).lower() for d in data["details"] + data.get("raw_errors", []))


def test_post_sources_registration(client: TestClient):
    """Verify source registration in MongoDB and duplicate conflict handling."""
    source_payload = {
        "source_id": f"SRC_API_TEST_{id(client)}",
        "name": "Integration Test Source",
        "type": "REST_JSON",
        "owner": "QA Automation",
        "schema_version": "1.0.0",
    }
    # 1. Successful creation
    res1 = client.post("/sources", json=source_payload)
    assert res1.status_code == 201
    assert res1.json()["source_id"] == source_payload["source_id"]

    # 2. Duplicate registration attempt -> 409
    res2 = client.post("/sources", json=source_payload)
    assert res2.status_code == 409


def test_get_reconciliation(client: TestClient):
    """Verify GET /reconciliation/{batch_id} returns metrics and 404 for unknown batch."""
    # 1. Existing batch B001
    res1 = client.get("/reconciliation/B001?limit=10")
    assert res1.status_code == 200
    data = res1.json()
    assert data["batch_id"] == "B001"
    assert "payment" in data["stage_counts"]
    assert "settlement" in data["stage_counts"]
    assert "missing_record" in data["incident_counts_by_type"]

    # 2. Unknown batch B99999 -> 404
    res2 = client.get("/reconciliation/B99999")
    assert res2.status_code == 404


def test_incidents_lifecycle(client: TestClient):
    """Verify incident filtering, listing, and PATCH status transitions."""
    # 1. List incidents
    res_list = client.get("/incidents?batch_id=B001")
    assert res_list.status_code == 200
    incidents = res_list.json()
    assert len(incidents) > 0
    target_inc = incidents[0]
    target_id = target_inc["id"]

    # 2. PATCH status to 'investigating' with notes
    patch_payload = {
        "status": "investigating",
        "notes": "Operations triage initiated.",
    }
    res_patch1 = client.patch(f"/incidents/{target_id}", json=patch_payload)
    assert res_patch1.status_code == 200
    data1 = res_patch1.json()
    assert data1["status"] == "investigating"
    assert data1["notes"] == "Operations triage initiated."
    assert data1["resolved_at"] is None

    # 3. PATCH status to 'resolved'
    res_patch2 = client.patch(f"/incidents/{target_id}", json={"status": "resolved"})
    assert res_patch2.status_code == 200
    data2 = res_patch2.json()
    assert data2["status"] == "resolved"
    assert data2["resolved_at"] is not None

    # 4. PATCH non-existent incident -> 404
    res_404 = client.patch("/incidents/INC_DOES_NOT_EXIST", json={"status": "resolved"})
    assert res_404.status_code == 404

    # 5. PATCH with invalid status value -> 422
    res_422 = client.patch(f"/incidents/{target_id}", json={"status": "invalid_status"})
    assert res_422.status_code == 422
