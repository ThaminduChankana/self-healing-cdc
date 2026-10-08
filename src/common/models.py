"""Domain models shared by every pipeline component."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class DriftType(str, Enum):
    ADDED_COLUMN = "ADDED_COLUMN"
    REMOVED_COLUMN = "REMOVED_COLUMN"
    RENAMED_COLUMN = "RENAMED_COLUMN"
    TYPE_CHANGE = "TYPE_CHANGE"
    NULLABILITY_CHANGE = "NULLABILITY_CHANGE"
    MULTIPLE_CHANGES = "MULTIPLE_CHANGES"
    UNKNOWN = "UNKNOWN"
    NONE = "NONE"


class DLQReason(str, Enum):
    SCHEMA_DRIFT = "SCHEMA_DRIFT"
    MALFORMED_JSON = "MALFORMED_JSON"
    MALFORMED_ENVELOPE = "MALFORMED_ENVELOPE"
    UNKNOWN_SCHEMA = "UNKNOWN_SCHEMA"
    PROCESSING_ERROR = "PROCESSING_ERROR"


class RiskLevel(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"

    @property
    def rank(self) -> int:
        return ["LOW", "MEDIUM", "HIGH", "CRITICAL"].index(self.value)

    @staticmethod
    def max(*levels: "RiskLevel") -> "RiskLevel":
        return max(levels, key=lambda r: r.rank)


class RepairStatus(str, Enum):
    REPAIRED = "REPAIRED"
    RETRY_AI = "RETRY_AI"
    REJECTED = "REJECTED"
    MANUAL_REVIEW = "MANUAL_REVIEW"


class CDCEvent(BaseModel):
    """A parsed Debezium change event."""

    event_id: str
    correlation_id: str = ""
    source_name: str = ""  # Debezium source.name == connector topic prefix
    source_database: str = ""
    source_table: str = ""
    operation: str = ""
    ts_ms: int | None = None
    key: dict[str, Any] | None = None
    before: dict[str, Any] | None = None
    after: dict[str, Any] | None = None
    source_metadata: dict[str, Any] = Field(default_factory=dict)
    # Per-column schema from the Debezium envelope (when schemas are enabled):
    # {column: {"connect_type": "int32", "optional": False, "sql_type": "INT", "length": 11}}
    column_schema: dict[str, dict[str, Any]] = Field(default_factory=dict)

    @property
    def payload(self) -> dict[str, Any]:
        """Row image to validate: `before` for deletes, otherwise `after`."""
        if self.operation == "d":
            return self.before or {}
        return self.after or {}

    @property
    def is_row_event(self) -> bool:
        return self.operation in {"c", "u", "d", "r"}


class TypeChange(BaseModel):
    field: str
    expected: str
    actual: str
    level: str = "sql"  # sql | connect | json
    compatibility: str = "risky"


class RenameCandidate(BaseModel):
    missing_field: str
    unexpected_field: str
    confidence: float
    ambiguous: bool
    evidence: list[str] = Field(default_factory=list)


class DriftReport(BaseModel):
    event_id: str = ""
    source_table: str = ""
    operation: str = ""
    drift_detected: bool = False
    drift_type: DriftType = DriftType.NONE
    ambiguous: bool = False
    confidence: float = 1.0
    unexpected_fields: list[str] = Field(default_factory=list)
    missing_fields: list[str] = Field(default_factory=list)
    type_mismatches: list[TypeChange] = Field(default_factory=list)
    nullability_changes: list[dict[str, Any]] = Field(default_factory=list)
    possible_renames: list[RenameCandidate] = Field(default_factory=list)
    validation_errors: list[str] = Field(default_factory=list)
    fingerprint: str = ""
    contract_id: str = ""
    original_event: dict[str, Any] = Field(default_factory=dict)
    canonical_schema: dict[str, Any] = Field(default_factory=dict)
    timestamp: str = Field(default_factory=utc_now)

    def summary(self) -> dict[str, Any]:
        """Compact view used in prompts, logs and reports (no payload values)."""
        return {
            "drift_type": self.drift_type.value,
            "ambiguous": self.ambiguous,
            "confidence": self.confidence,
            "missing_fields": self.missing_fields,
            "unexpected_fields": self.unexpected_fields,
            "type_mismatches": [t.model_dump() for t in self.type_mismatches],
            "nullability_changes": self.nullability_changes,
            "possible_renames": [r.model_dump() for r in self.possible_renames],
        }


class ValidatedEvent(BaseModel):
    event_id: str
    correlation_id: str
    source_name: str = ""
    source_database: str
    source_table: str
    operation: str
    contract_id: str = ""
    key: dict[str, Any] | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    before: dict[str, Any] | None = None
    source_metadata: dict[str, Any] = Field(default_factory=dict)
    ts_ms: int | None = None
    schema_notice: dict[str, Any] | None = None
    validated_at: str = Field(default_factory=utc_now)


class DLQMessage(BaseModel):
    """Everything the AI worker (or a human) needs to decide on a repair."""

    event_id: str
    correlation_id: str = ""
    reason: DLQReason = DLQReason.SCHEMA_DRIFT
    error: str | None = None
    topic: str = ""
    partition: int = 0
    offset: int = 0
    source_name: str = ""
    source_database: str = ""
    source_table: str = ""
    operation: str = ""
    key: dict[str, Any] | None = None
    payload: dict[str, Any] = Field(default_factory=dict)  # row image that failed
    before: dict[str, Any] | None = None
    source_metadata: dict[str, Any] = Field(default_factory=dict)
    column_schema: dict[str, dict[str, Any]] = Field(default_factory=dict)
    raw_event: Any = None  # original message value (dict, or truncated text if undecodable)
    canonical_schema: dict[str, Any] = Field(default_factory=dict)
    drift_report: dict[str, Any] = Field(default_factory=dict)
    received_at: str = Field(default_factory=utc_now)
    attempt: int = 1


class TestCase(BaseModel):
    model_config = ConfigDict(extra="forbid")
    __test__ = False  # not a pytest test class

    input: dict[str, Any]
    expected_output: dict[str, Any]


class RepairResponse(BaseModel):
    """The ONLY shape accepted from the model. Anything else is rejected."""

    model_config = ConfigDict(extra="forbid")

    classification: DriftType
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning_summary: str = Field(min_length=1, max_length=600)
    migration_sql: str | None = Field(default=None, max_length=2000)
    transformation_code: str = Field(min_length=10, max_length=8000)
    test_cases: list[TestCase] = Field(default_factory=list, max_length=10)
    risk_level: RiskLevel

    @field_validator("classification")
    @classmethod
    def _no_none(cls, v: DriftType) -> DriftType:
        if v == DriftType.NONE:
            raise ValueError("classification NONE is not a valid repair classification")
        return v

    @field_validator("migration_sql")
    @classmethod
    def _blank_sql_is_none(cls, v: str | None) -> str | None:
        if v is None:
            return None
        v = v.strip().rstrip(";").strip()
        # Models sometimes write the *string* "null"; that means "no migration".
        if v.lower() in {"", "null", "none", "n/a", "na", "no migration", "not required"}:
            return None
        return v

    @field_validator("transformation_code")
    @classmethod
    def _has_transform(cls, v: str) -> str:
        if "def transform(" not in v:
            raise ValueError("transformation_code must define 'def transform(event)'")
        return v


class RepairRequest(BaseModel):
    """What the LLM is given. Payload values are untrusted data."""

    event_id: str
    source_table: str
    operation: str
    payload: dict[str, Any]
    contract: dict[str, Any]
    drift_report: dict[str, Any]
    debezium_metadata: dict[str, Any] = Field(default_factory=dict)
    migration_policy: dict[str, Any] = Field(default_factory=dict)
    feedback: list[str] = Field(default_factory=list)


class CheckResult(BaseModel):
    name: str
    passed: bool
    detail: str = ""


class RepairedEvent(BaseModel):
    event_id: str
    repair_id: str
    correlation_id: str = ""
    source_name: str = ""
    source_database: str = ""
    source_table: str = ""
    operation: str = ""
    key: dict[str, Any] | None = None
    source_event: dict[str, Any] = Field(default_factory=dict)
    repaired_payload: dict[str, Any] = Field(default_factory=dict)
    drift_report: dict[str, Any] = Field(default_factory=dict)
    repair_classification: str = ""
    repair_source: str = "llm"  # llm | cache
    model: str = ""
    confidence: float = 0.0
    sandbox_status: str = ""
    schema_validation: str = ""
    migration_sql: str | None = None
    migration_status: str = "NOT_REQUIRED"
    transformation_hash: str = ""
    timestamp: str = Field(default_factory=utc_now)


class AuditEvent(BaseModel):
    event_id: str
    repair_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    correlation_id: str = ""
    source_table: str = ""
    dlq_reason: str = ""
    drift_type: str = ""
    classification: str = ""
    confidence: float = 0.0
    risk_level: str = ""
    migration_sql: str | None = None
    migration_status: str = "NOT_REQUIRED"
    transformation_hash: str = ""
    model: str = ""
    repair_source: str = ""
    ai_attempts: int = 0
    sandbox_result: str = "NOT_RUN"
    validation_result: str = "NOT_RUN"
    repair_result: str = ""
    reasons: list[str] = Field(default_factory=list)
    checks: list[CheckResult] = Field(default_factory=list)
    reasoning_summary: str = ""
    report: str = ""
    timestamp: str = Field(default_factory=utc_now)
