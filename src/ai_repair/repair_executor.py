"""Orchestrates one repair: DLQ event -> proposal -> independent checks -> decision.

LLM unavailability (LLMUnavailableError) and sandbox-DB unavailability
(SandboxUnavailableError) propagate to the worker, which defers the event
without committing it. Everything else ends in a Decision plus an audit record.
"""

from __future__ import annotations

import hashlib
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from src.ai_repair.llm_client import LLMClient, LLMResponseError
from src.ai_repair.repair_planner import Decision, RepairPlanner
from src.ai_repair.report import render_report
from src.ai_repair.sql_policy import MigrationPolicy, SqlPolicyResult
from src.common.config import get_settings
from src.common.logging import ComponentLogger
from src.common.metrics import AI_FAILURES, AI_REQUESTS, EVENTS_REPAIRED, REPAIR_CACHE_HITS, REPAIR_FAILED, REPAIR_LATENCY
from src.common.models import (
    AuditEvent,
    CheckResult,
    DLQMessage,
    DLQReason,
    DriftType,
    RepairedEvent,
    RepairRequest,
    RepairResponse,
    RepairStatus,
    RiskLevel,
)
from src.common.sources import Contract, ContractRegistry
from src.common.state_store import StateStore
from src.sandbox.executor import SandboxExecutor
from src.sandbox.isolation import FORBIDDEN_MODULES, FORBIDDEN_NAMES
from src.sandbox.sql_sandbox import SqlSandbox
from src.sandbox.validator import OutputValidator

log = ComponentLogger("repair_executor")


@dataclass
class Evaluation:
    checks: list[CheckResult] = field(default_factory=list)
    sql: SqlPolicyResult = field(default_factory=lambda: SqlPolicyResult(True))
    output: dict[str, Any] = field(default_factory=dict)
    sandbox_status: str = "NOT_RUN"
    migration_status: str = "NOT_REQUIRED"


@dataclass
class RepairOutcome:
    status: RepairStatus
    audit: AuditEvent
    repaired: RepairedEvent | None = None


def transformation_hash(code: str) -> str:
    return hashlib.sha256(code.encode()).hexdigest()[:16]


class RepairExecutor:
    def __init__(
        self,
        llm: LLMClient,
        contracts: ContractRegistry | None = None,
        state: StateStore | None = None,
        sandbox: SandboxExecutor | None = None,
        sql_sandbox: SqlSandbox | None = None,
        policy: MigrationPolicy | None = None,
    ):
        self._settings = get_settings()
        self._llm = llm
        self._contracts = contracts or ContractRegistry()
        self._state = state
        self._sandbox = sandbox or SandboxExecutor()
        self._sql_sandbox = sql_sandbox or SqlSandbox()
        self._policy = policy or MigrationPolicy()
        self._planner = RepairPlanner(self._policy)
        self._outputs = OutputValidator()

    # ── public ──────────────────────────────────────────────────────────────
    def execute(self, dlq: DLQMessage) -> RepairOutcome:
        start = time.monotonic()
        repair_id = str(uuid.uuid4())
        drift = dlq.drift_report or {}
        table = f"{dlq.source_database}.{dlq.source_table}" if dlq.source_table else "unknown"
        audit = AuditEvent(event_id=dlq.event_id, repair_id=repair_id, correlation_id=dlq.correlation_id,
                           source_table=table, dlq_reason=dlq.reason.value,
                           drift_type=drift.get("drift_type", "NONE"), model=self._llm.model)

        contract = self._contracts.get(dlq.source_database, dlq.source_table, dlq.source_name or None) \
            if dlq.source_table else None
        if dlq.reason != DLQReason.SCHEMA_DRIFT or contract is None:
            why = (f"DLQ reason {dlq.reason.value} is not repairable automatically"
                   if dlq.reason != DLQReason.SCHEMA_DRIFT else "no canonical contract for this table")
            return self._finish(audit, dlq, Decision(RepairStatus.MANUAL_REVIEW, RiskLevel.MEDIUM, [why]),
                                None, Evaluation(), start, code="")

        drift_type = DriftType(drift.get("drift_type", "UNKNOWN"))
        ambiguous = bool(drift.get("ambiguous"))
        fingerprint = drift.get("fingerprint", "")
        log.info("Starting repair", event_id=dlq.event_id, repair_id=repair_id, correlation_id=dlq.correlation_id,
                 source_table=table, drift_type=drift_type.value, fingerprint=fingerprint)

        # 1) Previously approved repair for the same drift shape (no LLM call).
        cached = self._state.get_cached_repair(fingerprint) if (self._state and fingerprint) else None
        if cached:
            proposal = RepairResponse.model_validate(cached["proposal"])
            evaluation = self._evaluate(proposal, dlq, contract)
            decision = self._planner.decide(proposal, evaluation.checks, evaluation.sql, drift_type,
                                            ambiguous, attempt=1, max_attempts=1)
            if decision.status == RepairStatus.REPAIRED:
                REPAIR_CACHE_HITS.inc()
                audit.repair_source = "cache"
                audit.model = cached.get("model", self._llm.model)
                decision.reasons.append(f"reused approved repair {cached.get('repair_id')} for fingerprint {fingerprint}")
                return self._finish(audit, dlq, decision, proposal, evaluation, start, contract=contract)
            log.warning("Cached repair no longer valid for this event; asking the model",
                        event_id=dlq.event_id, reasons=decision.reasons)

        # 2) Ask the model, with bounded retries and validator feedback.
        audit.repair_source = "llm"
        feedback: list[str] = []
        last: tuple[Decision, RepairResponse | None, Evaluation] | None = None
        max_attempts = self._settings.max_ai_retries
        for attempt in range(1, max_attempts + 1):
            audit.ai_attempts = attempt
            request = RepairRequest(
                event_id=dlq.event_id,
                source_table=table,
                operation=dlq.operation,
                payload=dlq.payload,
                contract=contract.compact(),
                drift_report={k: drift.get(k) for k in (
                    "drift_type", "ambiguous", "confidence", "missing_fields", "unexpected_fields",
                    "type_mismatches", "nullability_changes", "possible_renames")},
                debezium_metadata={
                    "source": dlq.source_name, "table": table, "op": dlq.operation,
                    "connector": dlq.source_metadata.get("connector"),
                    "source_column_types": {k: v.get("sql_type") or v.get("connect_type")
                                            for k, v in dlq.column_schema.items()},
                },
                migration_policy={**self._policy.summary(),
                                  "expected_for_this_drift": self._migration_hint(drift, contract)},
                feedback=feedback,
            )
            AI_REQUESTS.labels(model=self._llm.model).inc()
            try:
                proposal = self._llm.generate_repair(request)  # LLMUnavailableError propagates
            except LLMResponseError as exc:
                AI_FAILURES.labels(model=self._llm.model, reason="invalid_response").inc()
                log.warning(f"AI attempt {attempt} returned an invalid response", event_id=dlq.event_id,
                            repair_id=repair_id, error=str(exc)[:300])
                feedback.append(f"invalid response: {exc}")
                last = (Decision(RepairStatus.REJECTED, RiskLevel.HIGH, [f"invalid model output: {exc}"]),
                        None, Evaluation())
                continue

            evaluation = self._evaluate(proposal, dlq, contract)
            decision = self._planner.decide(proposal, evaluation.checks, evaluation.sql, drift_type,
                                            ambiguous, attempt, max_attempts)
            log.info(f"AI attempt {attempt}: {decision.status.value}", event_id=dlq.event_id, repair_id=repair_id,
                     classification=proposal.classification.value, confidence=proposal.confidence,
                     reasons=decision.reasons[:5])
            last = (decision, proposal, evaluation)
            if decision.status == RepairStatus.RETRY_AI:
                feedback.extend(self._feedback(decision, evaluation.checks))
                continue
            if decision.status == RepairStatus.REPAIRED and self._state and fingerprint:
                self._state.put_cached_repair(fingerprint, {
                    "proposal": proposal.model_dump(mode="json"), "repair_id": repair_id,
                    "model": self._llm.model, "event_id": dlq.event_id,
                })
            return self._finish(audit, dlq, decision, proposal, evaluation, start, contract=contract)

        assert last is not None
        decision, proposal, evaluation = last
        if decision.status in (RepairStatus.RETRY_AI, RepairStatus.REJECTED) and proposal is None:
            decision = Decision(RepairStatus.REJECTED, RiskLevel.HIGH,
                                [f"AI retries exhausted ({max_attempts}) without a valid proposal"] + feedback[-3:])
        elif decision.status == RepairStatus.RETRY_AI:
            decision = Decision(RepairStatus.MANUAL_REVIEW, RiskLevel.max(decision.risk, RiskLevel.MEDIUM),
                                [f"AI retries exhausted ({max_attempts})"] + decision.reasons)
        return self._finish(audit, dlq, decision, proposal, evaluation, start, contract=contract)

    @staticmethod
    def _migration_hint(drift: dict[str, Any], contract: Contract) -> str:
        """Deterministic guidance derived from the drift report (the model may still deviate)."""
        widen = [t for t in drift.get("type_mismatches", []) if t.get("compatibility") == "compatible"
                 and t.get("level") == "sql"]
        renamed = {r["unexpected_field"] for r in drift.get("possible_renames", []) if not r.get("ambiguous")}
        extra = [f for f in drift.get("unexpected_fields", []) if f not in renamed]
        notes = []
        if extra:
            notes.append(f"the canonical contract is fixed: drop field(s) {extra} in the transformation "
                         f"(adding them downstream needs human approval)")
        if renamed:
            notes.append("map renamed field(s) in the transformation: " + ", ".join(
                f"{r['unexpected_field']} -> {r['missing_field']}" for r in drift.get("possible_renames", [])
                if not r.get("ambiguous")))
        if widen:
            parts = []
            for t in widen:
                nullable = contract.fields[t["field"]].nullable if t["field"] in contract.fields else True
                parts.append(f"ALTER TABLE {contract.table} MODIFY COLUMN {t['field']} {t['actual']} "
                             f"{'NULL' if nullable else 'NOT NULL'}")
            notes.append("migration_sql: widen the downstream column(s): " + "; ".join(parts))
        else:
            notes.append("migration_sql must be JSON null (not the string \"null\"): the downstream table "
                         "needs no change")
        return "; ".join(notes)

    _FEEDBACK = {
        "unexpected_fields": "Your transform returned fields that are NOT in the canonical contract ({d}). "
                             "Return only the contract fields.",
        "before_image_unexpected_fields": "Same problem for the before-image ({d}). Return only contract fields.",
        "required_fields": "Your transform output lacks required contract fields ({d}). Map them from the source row.",
        "schema_validation": "Output does not match the canonical JSON Schema: {d}",
        "data_fidelity": "Values must be copied unchanged from the source row: {d}",
        "sandbox_execution": "Your code failed in the sandbox: {d}. Use only plain Python as specified.",
        "deterministic_replay": "Your transform is not deterministic: {d}",
        "sql_conformance": "migration_sql was rejected: {d}",
        "sandbox_migration": "migration_sql failed in the sandbox database: {d}",
        "sandbox_db_load": "The repaired row could not be loaded into the downstream table: {d}",
    }

    def _feedback(self, decision: Decision, checks: list[CheckResult]) -> list[str]:
        out = [self._FEEDBACK.get(c.name, c.name + ": {d}").format(d=c.detail[:250])
               for c in checks if not c.passed]
        out.extend(r for r in decision.reasons if r.startswith("confidence"))
        return out or decision.reasons

    # ── evaluation pipeline ────────────────────────────────────────────────
    def _evaluate(self, proposal: RepairResponse, dlq: DLQMessage, contract: Contract) -> Evaluation:
        ev = Evaluation()
        ev.checks.append(CheckResult(name="response_schema", passed=True, detail="strict RepairResponse model"))
        ev.checks.append(CheckResult(
            name="model_risk", passed=proposal.risk_level not in (RiskLevel.HIGH, RiskLevel.CRITICAL),
            detail=f"model declared risk {proposal.risk_level.value}"))

        # SQL policy (parser-based allowlist)
        ev.sql = self._policy.validate(proposal.migration_sql, contract)
        if proposal.migration_sql:
            # Destructive SQL is fatal ("sql_policy"); a harmless but non-conforming
            # proposal ("sql_conformance") goes back to the model with feedback.
            name = "sql_policy" if (ev.sql.allowed or ev.sql.destructive) else "sql_conformance"
            ev.checks.append(CheckResult(
                name=name, passed=ev.sql.allowed,
                detail="; ".join(ev.sql.violations) or f"allowed: {' ; '.join(ev.sql.rendered)}"))
            if not ev.sql.allowed:
                ev.migration_status = "REJECTED_BY_POLICY"
                return ev  # never run anything for a proposal whose SQL failed the policy

        # Python sandbox (separate process, timeout, AST allowlist)
        inputs = [dlq.payload] + ([dlq.before] if dlq.before else [])
        tests = [t.model_dump() for t in proposal.test_cases]
        run1 = self._sandbox.execute(proposal.transformation_code, inputs, tests)
        ev.sandbox_status = run1.status
        if not run1.success:
            malicious = run1.status == "UNSAFE_CODE" and any(
                f"forbidden name: {n}" in run1.error or f"import {n}" in run1.error
                for n in FORBIDDEN_NAMES | FORBIDDEN_MODULES)
            ev.checks.append(CheckResult(name="code_security" if malicious else "sandbox_execution",
                                         passed=False, detail=f"{run1.status}: {run1.error[:400]}"))
            return ev
        ev.checks.append(CheckResult(name="code_security", passed=True, detail="AST allowlist passed"))
        ev.checks.append(CheckResult(name="sandbox_execution", passed=True,
                                     detail=f"isolated process, {run1.duration_ms:.0f} ms"))
        ev.output = run1.output
        if run1.test_results:
            passed = sum(1 for t in run1.test_results if t["passed"])
            # Model-authored tests are untrusted, so they are informational only.
            ev.checks.append(CheckResult(name="model_test_cases", passed=True,
                                         detail=f"{passed}/{len(run1.test_results)} model-provided cases passed (informational)"))

        # Output validation: schema, required, unexpected, data fidelity (row and before-image)
        for idx, out in enumerate(run1.outputs):
            for c in self._outputs.validate(out, inputs[idx], contract.schema, dlq.drift_report):
                if idx:
                    c.name = f"before_image_{c.name}"
                ev.checks.append(c)

        # Deterministic replay in a fresh process
        run2 = self._sandbox.execute(proposal.transformation_code, inputs)
        deterministic = run2.success and run2.outputs == run1.outputs
        ev.checks.append(CheckResult(name="deterministic_replay", passed=deterministic,
                                     detail="identical output on replay" if deterministic
                                     else f"replay differed or failed ({run2.status})"))
        if not all(c.passed for c in ev.checks):
            return ev

        # SQL sandbox: migration (if any) + loading the repaired row
        result = self._sql_sandbox.run(contract, ev.sql.rendered, [run1.output],
                                       require_db=bool(proposal.migration_sql))
        ev.checks.extend(result.checks)
        if proposal.migration_sql:
            ev.migration_status = ("VERIFIED_IN_SANDBOX" if result.status == "PASSED" else "FAILED_IN_SANDBOX")
        return ev

    # ── result assembly ────────────────────────────────────────────────────
    def _finish(self, audit: AuditEvent, dlq: DLQMessage, decision: Decision, proposal: RepairResponse | None,
                ev: Evaluation, start: float, code: str | None = None,
                contract: Contract | None = None) -> RepairOutcome:
        code = proposal.transformation_code if proposal else (code or "")
        audit.repair_result = decision.status.value
        audit.reasons = decision.reasons
        audit.risk_level = decision.risk.value
        audit.checks = ev.checks
        audit.sandbox_result = ev.sandbox_status
        failed = [c for c in ev.checks if not c.passed]
        audit.validation_result = ("PASSED" if ev.checks and not failed else
                                   "FAILED" if failed else "NOT_RUN")
        audit.migration_status = ev.migration_status
        if proposal:
            audit.classification = proposal.classification.value
            audit.confidence = proposal.confidence
            audit.migration_sql = proposal.migration_sql
            audit.reasoning_summary = proposal.reasoning_summary
            audit.transformation_hash = transformation_hash(code)
        if (decision.status == RepairStatus.REPAIRED and proposal and proposal.migration_sql
                and self._settings.auto_apply_safe_migrations):
            audit.migration_status = "APPROVED_FOR_APPLY"
        audit.report = render_report(audit, dlq.drift_report, code)

        elapsed = time.monotonic() - start
        REPAIR_LATENCY.observe(elapsed)
        repaired = None
        if decision.status == RepairStatus.REPAIRED and proposal:
            EVENTS_REPAIRED.labels(source_table=audit.source_table, drift_type=audit.drift_type,
                                   source=audit.repair_source).inc()
            repaired = RepairedEvent(
                event_id=dlq.event_id, repair_id=audit.repair_id, correlation_id=dlq.correlation_id,
                source_name=dlq.source_name, source_database=dlq.source_database, source_table=dlq.source_table,
                operation=dlq.operation, key=dlq.key,
                source_event={"payload": dlq.payload, "before": dlq.before,
                              "source_metadata": dlq.source_metadata, "dlq_coordinates":
                              f"{dlq.topic}/{dlq.partition}/{dlq.offset}"},
                repaired_payload=ev.output, drift_report=dlq.drift_report,
                repair_classification=proposal.classification.value, repair_source=audit.repair_source,
                model=audit.model, confidence=proposal.confidence, sandbox_status="PASSED",
                schema_validation="PASSED", migration_sql=proposal.migration_sql,
                migration_status=audit.migration_status, transformation_hash=audit.transformation_hash,
            )
        else:
            REPAIR_FAILED.labels(source_table=audit.source_table, decision=decision.status.value).inc()
        log.info(f"Repair decision: {decision.status.value} in {elapsed:.2f}s", event_id=dlq.event_id,
                 repair_id=audit.repair_id, correlation_id=dlq.correlation_id, source_table=audit.source_table,
                 reasons=decision.reasons[:5])
        return RepairOutcome(decision.status, audit, repaired)
