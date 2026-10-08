"""Sandbox child process. STDLIB ONLY — executed as `python -I -S runner.py`.

Protocol: reads one JSON object from stdin
    {"code": str, "inputs": [dict, ...], "tests": [{"input": dict, "expected_output": dict}],
     "cpu_seconds": int, "isolation_path": str}
and writes one JSON object to stdout
    {"ok": bool, "outputs": [...], "tests": [...], "error": str|null, "status": str}

Defence in depth (the parent has already run the same AST checks):
  * runs in a separate interpreter: isolated mode, no site-packages, empty
    environment, working directory = fresh temporary directory
  * resource limits: CPU seconds, no file creation/growth, no new processes,
    a handful of file descriptors (no sockets), bounded address space (Linux)
  * the code is re-validated here, then executed with an allowlisted
    __builtins__ that contains no __import__, open, eval, exec, getattr...
"""

import importlib.util
import json
import resource
import sys


def _limit(name, value):
    res = getattr(resource, name, None)
    if res is None:
        return
    try:
        _, hard = resource.getrlimit(res)
        if hard != resource.RLIM_INFINITY and value > hard:
            value = hard
        resource.setrlimit(res, (value, hard))
    except (ValueError, OSError):
        pass  # not supported on this platform (e.g. RLIMIT_AS on macOS)


def _apply_limits(cpu_seconds):
    _limit("RLIMIT_CPU", cpu_seconds)
    _limit("RLIMIT_FSIZE", 0)
    _limit("RLIMIT_NPROC", 0)
    _limit("RLIMIT_CORE", 0)
    if sys.platform.startswith("linux"):
        _limit("RLIMIT_AS", 512 * 1024 * 1024)


def _safe_builtins(allowed):
    import builtins

    table = {}
    for name in allowed:
        if name in ("True", "False", "None"):
            continue
        table[name] = getattr(builtins, name)
    table["print"] = lambda *a, **k: None
    return table


def _emit(obj):
    sys.stdout.write(json.dumps(obj, default=repr))
    sys.stdout.flush()


def main():
    request = json.loads(sys.stdin.read())
    spec = importlib.util.spec_from_file_location("sandbox_isolation", request["isolation_path"])
    isolation = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(isolation)

    _apply_limits(int(request.get("cpu_seconds", 5)))
    _limit("RLIMIT_NOFILE", 8)

    violations = isolation.validate_code(request["code"])
    if violations:
        _emit({"ok": False, "status": "UNSAFE_CODE", "error": "; ".join(violations)})
        return

    namespace = {"__builtins__": _safe_builtins(isolation.ALLOWED_BUILTINS), "__name__": "sandbox"}
    try:
        exec(compile(request["code"], "<transformation>", "exec"), namespace)  # noqa: S102 - restricted
    except Exception as exc:  # noqa: BLE001
        _emit({"ok": False, "status": "EXEC_ERROR", "error": f"{type(exc).__name__}: {exc}"})
        return
    transform = namespace.get("transform")
    if not callable(transform):
        _emit({"ok": False, "status": "NO_TRANSFORM", "error": "transform is not callable"})
        return

    outputs = []
    for item in request.get("inputs", []):
        try:
            result = transform(json.loads(json.dumps(item)))
        except Exception as exc:  # noqa: BLE001
            _emit({"ok": False, "status": "TRANSFORM_ERROR", "error": f"{type(exc).__name__}: {exc}"})
            return
        if not isinstance(result, dict):
            _emit({"ok": False, "status": "INVALID_OUTPUT",
                   "error": f"transform must return dict, got {type(result).__name__}"})
            return
        try:
            outputs.append(json.loads(json.dumps(result)))
        except (TypeError, ValueError) as exc:
            _emit({"ok": False, "status": "INVALID_OUTPUT", "error": f"output not JSON-serialisable: {exc}"})
            return

    tests = []
    for case in request.get("tests", []):
        try:
            got = transform(json.loads(json.dumps(case.get("input", {}))))
            tests.append({"passed": got == case.get("expected_output"), "error": None})
        except Exception as exc:  # noqa: BLE001
            tests.append({"passed": False, "error": f"{type(exc).__name__}: {exc}"})

    _emit({"ok": True, "status": "PASSED", "outputs": outputs, "tests": tests, "error": None})


if __name__ == "__main__":
    main()
