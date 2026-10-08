"""AST-based safety validation for AI-generated transformation code.

STDLIB ONLY: this module is also loaded by the sandbox child process
(src/sandbox/runner.py), which runs with `python -I -S` and cannot import the
application package.

The policy is an allowlist: only the AST node types, attribute names and
built-ins that a pure dict->dict transformation needs are permitted.
Everything else (imports, dunder access, eval/exec/open, classes, globals,
context managers, async, str.format attribute traversal...) is rejected.
"""

from __future__ import annotations

import ast

MAX_CODE_CHARS = 8000
MAX_AST_NODES = 2500

# Explicitly named so violations are easy to read in the audit trail.
FORBIDDEN_MODULES = {
    "os", "subprocess", "socket", "requests", "shutil", "pathlib", "sys", "signal",
    "ctypes", "multiprocessing", "threading", "http", "urllib", "ftplib", "smtplib",
    "pickle", "shelve", "marshal", "importlib", "runpy", "code", "codeop", "pty",
    "fcntl", "resource", "tempfile", "glob", "io", "builtins", "inspect", "gc", "asyncio",
}

FORBIDDEN_NAMES = {
    "eval", "exec", "open", "compile", "__import__", "globals", "locals", "getattr",
    "setattr", "delattr", "vars", "dir", "breakpoint", "exit", "quit", "input",
    "memoryview", "help", "type", "object", "super", "classmethod", "staticmethod",
    "property", "__builtins__", "__loader__", "__spec__", "__file__", "bytearray",
}

ALLOWED_BUILTINS = {
    "dict", "list", "tuple", "set", "frozenset", "str", "int", "float", "bool", "len",
    "range", "enumerate", "zip", "sorted", "reversed", "min", "max", "sum", "abs", "round",
    "isinstance", "any", "all", "map", "filter", "repr",
    "ValueError", "TypeError", "KeyError", "IndexError", "Exception",
    "True", "False", "None",
}

ALLOWED_ATTRIBUTES = {
    # dict
    "get", "items", "keys", "values", "pop", "setdefault", "update", "copy",
    # str
    "lower", "upper", "strip", "lstrip", "rstrip", "split", "rsplit", "replace",
    "startswith", "endswith", "join", "isdigit", "isnumeric", "isalpha", "isalnum",
    "title", "capitalize", "zfill",
    # list
    "append", "extend", "insert", "index", "count", "sort", "remove",
}

_ALLOWED_NODES: tuple[type, ...] = (
    ast.Module, ast.FunctionDef, ast.arguments, ast.arg, ast.Return, ast.Assign, ast.AugAssign,
    ast.AnnAssign, ast.For, ast.While, ast.If, ast.Expr, ast.Pass, ast.Break, ast.Continue,
    ast.Delete, ast.Try, ast.ExceptHandler, ast.Raise, ast.Assert,
    ast.Name, ast.Constant, ast.Dict, ast.List, ast.Tuple, ast.Set, ast.Subscript, ast.Slice,
    ast.Attribute, ast.Call, ast.keyword, ast.BinOp, ast.UnaryOp, ast.BoolOp, ast.Compare,
    ast.IfExp, ast.ListComp, ast.DictComp, ast.SetComp, ast.GeneratorExp, ast.comprehension,
    ast.JoinedStr, ast.FormattedValue, ast.Starred, ast.Lambda,
    ast.expr_context, ast.operator, ast.unaryop, ast.cmpop, ast.boolop,
)


def validate_code(code: str) -> list[str]:
    """Return a list of violations; empty means the code passed every check."""
    if not isinstance(code, str) or not code.strip():
        return ["empty transformation code"]
    if len(code) > MAX_CODE_CHARS:
        return [f"code too long ({len(code)} > {MAX_CODE_CHARS} chars)"]
    try:
        tree = ast.parse(code, mode="exec")
    except SyntaxError as exc:
        return [f"syntax error: {exc.msg} (line {exc.lineno})"]

    violations: list[str] = []
    nodes = list(ast.walk(tree))
    if len(nodes) > MAX_AST_NODES:
        violations.append(f"code too complex ({len(nodes)} AST nodes)")

    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            continue
        if isinstance(node, ast.Assign) and all(isinstance(t, ast.Name) for t in node.targets):
            continue  # module-level constants such as a field mapping
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant):
            continue  # docstring
        violations.append(f"top-level statement not allowed: {type(node).__name__} (line {node.lineno})")

    for node in nodes:
        line = getattr(node, "lineno", "?")
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names = [a.name for a in node.names] if isinstance(node, ast.Import) else [node.module or ""]
            for n in names:
                root = n.split(".")[0]
                label = "forbidden module" if root in FORBIDDEN_MODULES else "imports are not allowed"
                violations.append(f"{label}: import {n} (line {line})")
            continue
        if not isinstance(node, _ALLOWED_NODES):
            violations.append(f"construct not allowed: {type(node).__name__} (line {line})")
            continue
        if isinstance(node, ast.Name):
            if node.id in FORBIDDEN_NAMES:
                violations.append(f"forbidden name: {node.id} (line {line})")
            elif node.id.startswith("__"):
                violations.append(f"dunder name not allowed: {node.id} (line {line})")
        elif isinstance(node, ast.Attribute):
            if node.attr.startswith("_"):
                violations.append(f"private/dunder attribute access: .{node.attr} (line {line})")
            elif node.attr not in ALLOWED_ATTRIBUTES:
                violations.append(f"attribute not allowed: .{node.attr} (line {line})")
        elif isinstance(node, ast.Constant) and isinstance(node.value, (bytes,)):
            violations.append(f"bytes literals not allowed (line {line})")

    transforms = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "transform"]
    if not transforms:
        violations.append("code must define a top-level 'transform(event)' function")
    else:
        fn = transforms[0]
        a = fn.args
        if len(a.args) != 1 or a.vararg or a.kwarg or a.kwonlyargs or a.posonlyargs:
            violations.append("transform() must take exactly one positional argument")
        if fn.decorator_list:
            violations.append("decorators are not allowed")

    # de-duplicate, keep order
    seen: set[str] = set()
    return [v for v in violations if not (v in seen or seen.add(v))]


class ASTSafetyValidator:
    """Object wrapper kept for API compatibility: validate(code) -> (is_safe, violations)."""

    def validate(self, code: str) -> tuple[bool, list[str]]:
        violations = validate_code(code)
        return not violations, violations
