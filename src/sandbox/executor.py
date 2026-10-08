"""Runs AI-generated transformation code in an isolated child process.

Never uses exec() in the worker process. Each run:
  1. AST safety validation in the parent (fast reject)
  2. a fresh `python -I -S src/sandbox/runner.py` child with an empty
     environment, cwd = new temporary directory, start_new_session=True
  3. a hard wall-clock timeout (the whole process group is killed) plus a
     CPU rlimit inside the child  ->  status SANDBOX_TIMEOUT
  4. bounded output size
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from src.common.config import get_settings
from src.common.metrics import SANDBOX_FAILURES
from src.sandbox.isolation import validate_code

_SANDBOX_DIR = Path(__file__).resolve().parent
RUNNER_PATH = _SANDBOX_DIR / "runner.py"
ISOLATION_PATH = _SANDBOX_DIR / "isolation.py"
MAX_OUTPUT_BYTES = 1024 * 1024


@dataclass
class SandboxResult:
    success: bool
    status: str
    outputs: list[dict[str, Any]] = field(default_factory=list)
    test_results: list[dict[str, Any]] = field(default_factory=list)
    error: str = ""
    duration_ms: float = 0.0

    @property
    def output(self) -> dict[str, Any]:
        return self.outputs[0] if self.outputs else {}


class SandboxExecutor:
    def __init__(self, timeout: float | None = None):
        self._timeout = float(timeout or get_settings().sandbox_timeout_seconds)

    def execute(
        self,
        code: str,
        inputs: list[dict[str, Any]],
        tests: list[dict[str, Any]] | None = None,
    ) -> SandboxResult:
        start = time.monotonic()
        violations = validate_code(code)
        if violations:
            SANDBOX_FAILURES.labels(stage="python", reason="unsafe_code").inc()
            return SandboxResult(False, "UNSAFE_CODE", error="; ".join(violations))

        request = json.dumps({
            "code": code,
            "inputs": inputs,
            "tests": tests or [],
            "cpu_seconds": max(1, int(self._timeout)),
            "isolation_path": str(ISOLATION_PATH),
        }, default=str)

        with tempfile.TemporaryDirectory(prefix="cdc-sandbox-") as workdir:
            proc = subprocess.Popen(
                [sys.executable, "-I", "-S", str(RUNNER_PATH)],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                cwd=workdir, env={}, start_new_session=True,
            )
            try:
                stdout, stderr = proc.communicate(request.encode(), timeout=self._timeout)
            except subprocess.TimeoutExpired:
                self._kill(proc)
                SANDBOX_FAILURES.labels(stage="python", reason="timeout").inc()
                return SandboxResult(False, "SANDBOX_TIMEOUT",
                                     error=f"execution exceeded {self._timeout:.0f}s",
                                     duration_ms=(time.monotonic() - start) * 1000)

        elapsed = (time.monotonic() - start) * 1000
        if proc.returncode in (-signal.SIGXCPU, -signal.SIGKILL):
            SANDBOX_FAILURES.labels(stage="python", reason="timeout").inc()
            return SandboxResult(False, "SANDBOX_TIMEOUT", error="CPU limit exceeded", duration_ms=elapsed)
        if len(stdout) > MAX_OUTPUT_BYTES:
            SANDBOX_FAILURES.labels(stage="python", reason="output_too_large").inc()
            return SandboxResult(False, "INVALID_OUTPUT", error="output exceeds size limit", duration_ms=elapsed)
        try:
            result = json.loads(stdout.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            SANDBOX_FAILURES.labels(stage="python", reason="crash").inc()
            return SandboxResult(False, "SANDBOX_CRASH",
                                 error=f"exit={proc.returncode} {stderr.decode(errors='replace')[-500:]}",
                                 duration_ms=elapsed)
        if not result.get("ok"):
            SANDBOX_FAILURES.labels(stage="python", reason=str(result.get("status", "error")).lower()).inc()
            return SandboxResult(False, result.get("status", "ERROR"), error=result.get("error") or "",
                                 duration_ms=elapsed)
        return SandboxResult(True, "PASSED", outputs=result.get("outputs", []),
                             test_results=result.get("tests", []), duration_ms=elapsed)

    @staticmethod
    def _kill(proc: subprocess.Popen) -> None:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            proc.kill()
        proc.communicate()
