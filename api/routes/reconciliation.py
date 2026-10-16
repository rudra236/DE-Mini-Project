"""BankFlow Sentinel - Reconciliation Query Routes."""

import json
from typing import Dict, List

from fastapi import APIRouter, HTTPException, Query, status
from sqlalchemy import text

from api.schemas import ReconciliationResponse
from data_gen.utils import normalize_batch
from db.connection import get_engine

router = APIRouter(tags=["Reconciliation"])


@router.get(
    "/reconciliation/{batch_id}",
    response_model=ReconciliationResponse,
    summary="Get reconciliation summary for a batch",
    description="Returns stage counts, discrepancy metrics, and unmatched transaction IDs.",
)
def get_batch_reconciliation(
    batch_id: str,
    limit: int = Query(50, ge=1, le=1000, description="Max unmatched txn_ids to return per stage"),
) -> ReconciliationResponse:
    try:
        b_id, _ = normalize_batch(batch_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Batch '{batch_id}' is not recognized.",
        )

    engine = get_engine()
    with engine.connect() as conn:
        # Check if batch exists
        batch_row = conn.execute(
            text("SELECT status FROM batches WHERE batch_id = :b;"),
            {"b": b_id},
        ).fetchone()

        if not batch_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Batch '{b_id}' not found in database.",
            )

        batch_status = batch_row[0]

        # Retrieve stage metrics from reconciliation_results
        results_rows = conn.execute(
            text("""
                SELECT stage, source_count, target_count, missing_count,
                       duplicate_count, mismatch_count, delayed_count, invalid_count
                FROM reconciliation_results
                WHERE batch_id = :b;
            """),
            {"b": b_id},
        ).fetchall()

        stage_counts: Dict[str, Dict[str, int]] = {}
        for r in results_rows:
            stage_counts[r[0]] = {
                "source_count": r[1],
                "target_count": r[2],
                "missing_count": r[3],
                "duplicate_count": r[4],
                "mismatch_count": r[5],
                "delayed_count": r[6],
                "invalid_count": r[7],
            }

        # Retrieve incidents and unmatched txn_ids
        incidents_rows = conn.execute(
            text("SELECT stage, type, evidence FROM incidents WHERE batch_id = :b;"),
            {"b": b_id},
        ).fetchall()

        incident_counts_by_type: Dict[str, int] = {}
        unmatched_txn_ids: Dict[str, List[str]] = {"payment": [], "settlement": []}

        for stage, disc_type, evidence_raw in incidents_rows:
            evidence = evidence_raw
            if isinstance(evidence, str):
                evidence = json.loads(evidence)
            count = len(evidence)
            incident_counts_by_type[disc_type] = incident_counts_by_type.get(disc_type, 0) + count

            # Missing records represent unmatched transaction IDs
            if disc_type == "missing_record" and stage in unmatched_txn_ids:
                unmatched_txn_ids[stage] = evidence[:limit]

    return ReconciliationResponse(
        batch_id=b_id,
        status=batch_status,
        stage_counts=stage_counts,
        incident_counts_by_type=incident_counts_by_type,
        unmatched_txn_ids=unmatched_txn_ids,
    )
