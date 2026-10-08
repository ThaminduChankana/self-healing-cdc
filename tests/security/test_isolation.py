"""AST allowlist: every classic Python sandbox-escape trick must be rejected statically."""

import pytest

from src.sandbox.isolation import validate_code

SAFE = "def transform(event):\n    out = dict(event)\n    out['q'] = out.pop('stock_q', None)\n    return out"


def test_safe_code_passes():
    assert validate_code(SAFE) == []


def test_module_level_mapping_constant_allowed():
    code = "MAP = {'stock_quantity': 'quantity'}\ndef transform(event):\n    return {MAP.get(k, k): v for k, v in event.items()}"
    assert validate_code(code) == []


@pytest.mark.parametrize("module", ["os", "subprocess", "socket", "requests", "shutil", "pathlib", "sys"])
def test_forbidden_imports(module):
    v = validate_code(f"import {module}\ndef transform(event):\n    return event")
    assert any(f"import {module}" in x for x in v)
    v2 = validate_code(f"from {module} import *\ndef transform(event):\n    return event")
    assert v2


@pytest.mark.parametrize("name", ["eval", "exec", "open", "compile", "__import__", "globals", "locals",
                                  "getattr", "setattr", "delattr", "vars", "type", "breakpoint"])
def test_forbidden_builtins_called_or_aliased(name):
    assert validate_code(f"def transform(event):\n    {name}('x')\n    return event")
    assert validate_code(f"def transform(event):\n    f = {name}\n    return event")


@pytest.mark.parametrize("snippet", [
    "().__class__.__bases__[0].__subclasses__()",
    "event.__class__",
    "transform.__globals__",
    "'{0.__class__}'.format(event)",           # str.format attribute traversal
    "'{x}'.format_map(event)",
    "(lambda: 0).__code__",
    "__builtins__['open']",
    "[x for x in ()].gi_frame",
])
def test_introspection_escapes(snippet):
    assert validate_code(f"def transform(event):\n    x = {snippet}\n    return event")


@pytest.mark.parametrize("code", [
    "class X:\n    pass\ndef transform(event):\n    return event",
    "def transform(event):\n    global G\n    return event",
    "async def transform(event):\n    return event",
    "def transform(event):\n    with x as y:\n        pass\n    return event",
    "def transform(event):\n    yield event",
    "import os\n",
    "print('side effect at import')\ndef transform(event):\n    return event",
    "def transform(a, b):\n    return a",
    "def transform(*a):\n    return a",
    "def helper(e):\n    return e",
    "def transform(event):\n    return b'raw'",
    "@decorator\ndef transform(event):\n    return event",
])
def test_structural_violations(code):
    assert validate_code(code)


def test_oversized_code_rejected():
    assert validate_code("def transform(event):\n" + "    x = 1\n" * 3000 + "    return event")
