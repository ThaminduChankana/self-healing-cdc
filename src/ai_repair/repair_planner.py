"""Repair decision engine.

    AI proposal -> response schema validation -> SQL policy -> risk evaluation
    -> Python sandbox -> output validation -> determinism -> SQL sandbox
    -> DECISION (this module)

The model's own confidence and risk level are inputs, never the decision.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from src.ai_repair.sql_policy import MigrationPolicy, SqlPolicyResult
from src.common.config import get_settings
from src.common.models import CheckResult, DriftType, RepairResponse, RepairStatus, RiskLevel

# Checks whose failure means "unsafe" — never retried, never auto-applied.
FATAL_CHECKS = {"sql_policy", "code_security", "sandbox_migration", "model_risk"}


@dataclass
class Decision:
    status: RepairStatus
    risk: RiskLevel
    reasons: list[str] = field(default_factory=list)


class RepairPlanner:
    def __init__(self, policy: MigrationPolicy | None = None, confidence_threshold: float | None = None):
        self.policy = policy or MigrationPolicy()
        settings = get_settings()
        self.threshold = confidence_threshold if confidence_threshold is not None else settings.confidence_threshold
        thresholds = self.policy.config.get("confidence_thresholds", {})
        self.manual_floor = float(thresholds.get("manual_review_min_confidence", 0.6))
        self.manual_types = {DriftType(t) for t in self.policy.config.get("manual_review_drift_types", [])}

    def decide(
        self,
        proposal: RepairResponse,
        checks: list[CheckResult],
        sql_result: SqlPolicyResult,
        drift_type: DriftType,
        drift_ambiguous: bool,
        attempt: int,
        max_attempts: int,
    ) -> Decision:
        failed = [c for c in checks if not c.passed]
        fatal = [c for c in failed if c.name in FATAL_CHECKS]
        risk = RiskLevel.max(proposal.risk_level, sql_result.risk)

        if fatal:
            return Decision(RepairStatus.REJECTED, RiskLevel.max(risk, RiskLevel.HIGH),
                            [f"{c.name}: {c.detail}" for c in fatal])

        reasons = [f"{c.name}: {c.detail}" for c in failed]
        if proposal.confidence < self.threshold:
            reasons.append(f"confidence {proposal.confidence:.2f} below threshold {self.threshold:.2f}")
        if reasons:
            if attempt < max_attempts:
                return Decision(RepairStatus.RETRY_AI, risk, reasons)
            only_confidence = not failed
            if only_confidence and proposal.confidence >= self.manual_floor:
                return Decision(RepairStatus.MANUAL_REVIEW, RiskLevel.max(risk, RiskLevel.MEDIUM), reasons)
            return Decision(RepairStatus.REJECTED if failed else RepairStatus.MANUAL_REVIEW,
                            RiskLevel.max(risk, RiskLevel.MEDIUM), reasons)

        # Every independent check passed. Remaining gates are about uncertainty, not safety.
        if drift_type in self.manual_types:
            return Decision(RepairStatus.MANUAL_REVIEW, RiskLevel.max(risk, RiskLevel.MEDIUM),
                            [f"drift type {drift_type.value} always requires human review"])
        if drift_ambiguous:
            return Decision(RepairStatus.MANUAL_REVIEW, RiskLevel.max(risk, RiskLevel.MEDIUM),
                            ["deterministic drift analysis is ambiguous; mapping cannot be verified"])
        if risk != RiskLevel.LOW:
            return Decision(RepairStatus.MANUAL_REVIEW, risk, [f"effective risk {risk.value} requires review"])
        notes = []
        if proposal.classification != drift_type and drift_type != DriftType.MULTIPLE_CHANGES:
            notes.append(f"note: model classified {proposal.classification.value}, detector {drift_type.value}")
        return Decision(RepairStatus.REPAIRED, risk, notes)
