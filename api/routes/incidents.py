"""BankFlow Sentinel - Incident Management Routes."""

import json
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query, status
from sqlalchemy import text

from api.schemas import IncidentPatchRequest, IncidentResponse
from db.connection import get_engine

router = APIRouter(tags=["Incidents"])


@router.get(
    "/incidents",
    response_model=List[IncidentResponse],
    summary="List discrepancy incidents",
    description="Retrieve incidents with optional filtering by status, batch_id, and discrepancy type.",
)
def list_incidents(
    status_filter: Optional[str] = Query(None, alias="status", description="Filter by status (open, investigating, resolved)"),
    batch_id: Optional[str] = Query(None, description="Filter by batch ID (e.g. B001)"),
    incident_type: Optional[str] = Query(None, alias="type", description="Filter by discrepancy type"),
    limit: int = Query(50, ge=1, le=500, description="Max incidents to return"),
) -> List[IncidentResponse]:
    engine = get_engine()

    query_parts = ["SELECT id, batch_id, stage, type, severity, evidence, detected_at, status, resolved_at, notes FROM incidents WHERE 1=1"]
    params = {"limit": limit}

    if status_filter:
        query_parts.append("AND status = :status")
        params["status"] = status_filter
    if batch_id:
        query_parts.append("AND batch_id = :batch_id")
        params["batch_id"] = batch_id
    if incident_type:
        query_parts.append("AND type = :type")
        params["type"] = incident_type

    query_parts.append("ORDER BY detected_at DESC LIMIT :limit")
    full_query = " ".join(query_parts)

    with engine.connect() as conn:
        rows = conn.execute(text(full_query), params).fetchall()

    incidents = []
    for r in rows:
        evidence = r[5]
        if isinstance(evidence, str):
            evidence = json.loads(evidence)

        incidents.append(
            IncidentResponse(
                id=r[0],
                batch_id=r[1],
                stage=r[2],
                type=r[3],
                severity=r[4],
                evidence=evidence,
                detected_at=r[6].isoformat() if r[6] else "",
                status=r[7],
                resolved_at=r[8].isoformat() if r[8] else None,
                notes=r[9],
            )
        )

    return incidents


@router.patch(
    "/incidents/{incident_id}",
    response_model=IncidentResponse,
    summary="Update incident ticket status",
    description="Updates the resolution status ('open', 'investigating', 'resolved') and optional operational notes.",
)
def update_incident(
    incident_id: str,
    payload: IncidentPatchRequest,
) -> IncidentResponse:
    engine = get_engine()
    now_utc = datetime.now(timezone.utc)
    resolved_at_val = now_utc if payload.status == "resolved" else None

    with engine.begin() as conn:
        # Check if incident exists
        existing = conn.execute(
            text("SELECT id, batch_id, stage, type, severity, evidence, detected_at, status, resolved_at, notes FROM incidents WHERE id = :id;"),
            {"id": incident_id},
        ).fetchone()

        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Incident '{incident_id}' not found.",
            )

        # Update incident
        if payload.notes is not None:
            conn.execute(
                text("""
                    UPDATE incidents
                    SET status = :st, resolved_at = :res_at, notes = :notes
                    WHERE id = :id;
                """),
                {
                    "st": payload.status,
                    "res_at": resolved_at_val,
                    "notes": payload.notes,
                    "id": incident_id,
                },
            )
        else:
            conn.execute(
                text("""
                    UPDATE incidents
                    SET status = :st, resolved_at = :res_at
                    WHERE id = :id;
                """),
                {
                    "st": payload.status,
                    "res_at": resolved_at_val,
                    "id": incident_id,
                },
            )

        updated = conn.execute(
            text("SELECT id, batch_id, stage, type, severity, evidence, detected_at, status, resolved_at, notes FROM incidents WHERE id = :id;"),
            {"id": incident_id},
        ).fetchone()

    evidence = updated[5]
    if isinstance(evidence, str):
        evidence = json.loads(evidence)

    return IncidentResponse(
        id=updated[0],
        batch_id=updated[1],
        stage=updated[2],
        type=updated[3],
        severity=updated[4],
        evidence=evidence,
        detected_at=updated[6].isoformat() if updated[6] else "",
        status=updated[7],
        resolved_at=updated[8].isoformat() if updated[8] else None,
        notes=updated[9],
    )
