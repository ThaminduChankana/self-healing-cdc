from src.ai_repair.repair_planner import RepairPlanner
from src.ai_repair.sql_policy import SqlPolicyResult
from src.common.models import CheckResult, DriftType, RepairResponse, RepairStatus, RiskLevel

CODE = "def transform(event):\n    return dict(event)"


def resp(**kw):
    base = dict(classification=DriftType.RENAMED_COLUMN, confidence=0.95, reasoning_summary="x",
                transformation_code=CODE, risk_level=RiskLevel.LOW)
    base.update(kw)
    return RepairResponse(**base)


OK = [CheckResult(name="schema_validation", passed=True)]


def decide(r, checks=OK, sql=None, drift=DriftType.RENAMED_COLUMN, ambiguous=False, attempt=1, max_attempts=3):
    return RepairPlanner(confidence_threshold=0.9).decide(r, checks, sql or SqlPolicyResult(True), drift,
                                                          ambiguous, attempt, max_attempts)


def test_all_checks_pass_low_risk_high_confidence_is_repaired():
    assert decide(resp()).status == RepairStatus.REPAIRED


def test_fatal_checks_reject_immediately():
    d = decide(resp(), checks=[CheckResult(name="sql_policy", passed=False, detail="DROP")])
    assert d.status == RepairStatus.REJECTED and d.risk in (RiskLevel.HIGH, RiskLevel.CRITICAL)


def test_model_cannot_self_approve_high_risk():
    d = decide(resp(risk_level=RiskLevel.HIGH), checks=[CheckResult(name="model_risk", passed=False)])
    assert d.status == RepairStatus.REJECTED


def test_retryable_failures_retry_then_reject():
    bad = [CheckResult(name="data_fidelity", passed=False, detail="changed")]
    assert decide(resp(), checks=bad, attempt=1).status == RepairStatus.RETRY_AI
    assert decide(resp(), checks=bad, attempt=3).status == RepairStatus.REJECTED


def test_low_confidence_retries_then_manual_review():
    assert decide(resp(confidence=0.7)).status == RepairStatus.RETRY_AI
    assert decide(resp(confidence=0.7), attempt=3).status == RepairStatus.MANUAL_REVIEW


def test_ambiguous_drift_requires_human_even_if_checks_pass():
    assert decide(resp(), ambiguous=True).status == RepairStatus.MANUAL_REVIEW


def test_removed_column_requires_human():
    assert decide(resp(classification=DriftType.REMOVED_COLUMN), drift=DriftType.REMOVED_COLUMN).status \
        == RepairStatus.MANUAL_REVIEW


def test_medium_risk_requires_human():
    assert decide(resp(risk_level=RiskLevel.MEDIUM)).status == RepairStatus.MANUAL_REVIEW
