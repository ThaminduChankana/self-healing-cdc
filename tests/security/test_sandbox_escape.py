"""Runtime defence in depth: even if the AST check were bypassed, the child process is constrained."""

import json
import subprocess
import sys

from src.sandbox.executor import ISOLATION_PATH, RUNNER_PATH, SandboxExecutor


def _run_child_directly(code: str) -> dict:
    """Bypass the parent's AST check to exercise the runner's own protections."""
    req = json.dumps({"code": code, "inputs": [{}], "tests": [], "cpu_seconds": 2,
                      "isolation_path": str(ISOLATION_PATH)})
    out = subprocess.run([sys.executable, "-I", "-S", str(RUNNER_PATH)], input=req.encode(),
                         capture_output=True, env={}, timeout=10)
    return json.loads(out.stdout)


def test_runner_revalidates_code():
    r = _run_child_directly("import os\ndef transform(event):\n    return {'x': os.getcwd()}")
    assert r["ok"] is False and r["status"] == "UNSAFE_CODE"


def test_executor_rejects_file_access():
    r = SandboxExecutor().execute("def transform(event):\n    return {'x': open('/etc/passwd').read()}", [{}])
    assert not r.success and r.status == "UNSAFE_CODE"


def test_no_import_machinery_at_runtime():
    # Not detectable by name: the builtins table simply has no __import__.
    from src.sandbox.runner import _safe_builtins
    from src.sandbox.isolation import ALLOWED_BUILTINS
    table = _safe_builtins(ALLOWED_BUILTINS)
    for name in ("__import__", "open", "eval", "exec", "getattr", "compile", "globals"):
        assert name not in table


def test_child_runs_with_empty_environment(monkeypatch):
    monkeypatch.setenv("MYSQL_PASSWORD", "super-secret")
    code = "def transform(event):\n    return {'n': len(event)}"
    r = SandboxExecutor().execute(code, [{"a": 1}])
    assert r.success and r.output == {"n": 1}
    # The executor passes env={} — verify by inspecting the call contract.
    import inspect
    from src.sandbox import executor
    assert "env={}" in inspect.getsource(executor.SandboxExecutor.execute)


def test_cpu_bound_code_killed_with_timeout_status():
    r = SandboxExecutor(timeout=1).execute(
        "def transform(event):\n    n = 0\n    while True:\n        n += 1", [{}])
    assert r.status == "SANDBOX_TIMEOUT"
