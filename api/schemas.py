"""BankFlow Sentinel - FastAPI Pydantic v2 Request & Response Schemas.

Defines validated data transfer objects for all API endpoints.
"""

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator


class EventIngestRequest(BaseModel):
    """Payload to trigger batch ingestion event."""

    batch_id: str = Field(
        ...,
        pattern=r"^B\d{3,}$",
        description="Batch identifier starting with 'B' followed by at least 3 digits (e.g. B001)",
        examples=["B001"],
    )
    file_path: str = Field(
        ...,
        min_length=1,
        description="Path to batch directory containing stage CSV files",
        examples=["data/batch_001"],
    )
    row_count: int = Field(
        ...,
        gt=0,
        description="Total source transactions count, must be positive",
        examples=[10000],
    )

    @field_validator("file_path")
    @classmethod
    def validate_file_path(cls, v: str) -> str:
        trimmed = v.strip()
        if not trimmed:
            raise ValueError("file_path cannot be blank or whitespace.")
        return trimmed


class EventIngestResponse(BaseModel):
    """Response returned upon accepting batch ingestion event."""

    status: str = "accepted"
    message: str
    event: Dict[str, Any]


class SourceMetadataCreate(BaseModel):
    """Schema for registering a transaction source in MongoDB."""

    source_id: str = Field(..., min_length=2, description="Unique source system identifier", examples=["SRC_CORE_BANKING"])
    name: str = Field(..., min_length=2, description="Human-readable system name", examples=["Core Banking Service"])
    type: str = Field(..., description="System architecture or storage type", examples=["OLTP_RDBMS"])
    owner: str = Field(..., description="Responsible engineering or ops team", examples=["Platform Payments Team"])
    schema_version: str = Field(default="1.0.0", description="SemVer schema specification", examples=["1.0.0"])


class SourceMetadataResponse(SourceMetadataCreate):
    """Response schema for registered source metadata."""

    created_at: str


class IncidentPatchRequest(BaseModel):
    """Schema for updating incident status and notes."""

    status: Literal["open", "investigating", "resolved"] = Field(
        ...,
        description="Target lifecycle status for the incident ticket",
        examples=["investigating"],
    )
    notes: Optional[str] = Field(
        default=None,
        description="Optional operational notes or root-cause findings",
        examples=["Investigating payment gateway timeout spike during batch window."],
    )


class IncidentResponse(BaseModel):
    """Schema representing an actionable incident ticket."""

    id: str
    batch_id: str
    stage: str
    type: str
    severity: str
    evidence: List[str]
    detected_at: str
    status: str
    resolved_at: Optional[str] = None
    notes: Optional[str] = None


class ReconciliationStageSummary(BaseModel):
    """Reconciliation metrics for a stage pair."""

    source_count: int
    target_count: int
    missing_count: int
    duplicate_count: int
    mismatch_count: int
    delayed_count: int
    invalid_count: int


class ReconciliationResponse(BaseModel):
    """Response returned for batch reconciliation status."""

    batch_id: str
    status: str
    stage_counts: Dict[str, Dict[str, int]]
    incident_counts_by_type: Dict[str, int]
    unmatched_txn_ids: Dict[str, List[str]]


class ServiceHealth(BaseModel):
    """Individual infrastructure service health status."""

    status: Literal["pass", "fail"]
    detail: str


class HealthCheckResponse(BaseModel):
    """Overall system health check report."""

    status: Literal["healthy", "degraded", "unhealthy"]
    timestamp: str
    services: Dict[str, ServiceHealth]
