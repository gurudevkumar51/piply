"""A deliberately small conditional evaluator shared by ``run_if`` and variables.

This is **not** an expression language. It walks a parsed AST and supports only
literals, names, comparisons, membership tests, boolean operators, and the
ternary ``A if C else B``. Anything else raises, so a typo fails loudly instead
of silently producing the wrong value. There is no ``eval`` anywhere in the path.

Two surfaces use it:

* ``run_if`` on a task, deciding whether that task executes.
* Conditional values in a ``variables`` block, choosing between two values.
"""

from __future__ import annotations

import ast
import os
import re
from typing import Any

# `{name}` placeholders are substituted as quoted literals before parsing, so a
# value can never be interpreted as code.
_PLACEHOLDER = re.compile(r"\{([A-Za-z_][A-Za-z0-9_.]*)\}")

# A cheap pre-check before attempting a parse. Requires the ternary keywords as
# whole words so an ordinary sentence is not treated as an expression.
_TERNARY_SHAPE = re.compile(r"\bif\b.*\belse\b", re.DOTALL)

_TRUTHY_TEXT = {"true", "yes", "on", "1"}
_FALSEY_TEXT = {"false", "no", "off", "0", ""}

# YAML spells these in lower case; Python's parser sees bare names.
_YAML_LITERALS = {"true": True, "false": False, "null": None, "none": None}


class ConditionError(ValueError):
    """Raised when a conditional expression is malformed or unsupported."""


def resolve_placeholders(expression: str, values: dict[str, Any]) -> str:
    """Replace ``{name}`` with a quoted literal so values are never code.

    Dotted names walk nested mappings, so ``{params.tenant}`` works.
    """

    def _replace(match: re.Match[str]) -> str:
        current: Any = values
        for part in match.group(1).split("."):
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                current = None
                break
        if current is None:
            return "None"
        if isinstance(current, bool | int | float):
            return repr(current)
        return repr(str(current))

    return _PLACEHOLDER.sub(_replace, expression)


def _coerce(value: Any) -> Any:
    """Normalise a context value so comparisons behave the way YAML authors expect.

    Config values arrive as strings, so ``"true" == true`` must hold for a
    condition written against a boolean-looking variable.
    """
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in _TRUTHY_TEXT:
            return True
        if lowered in _FALSEY_TEXT and lowered != "":
            return False
    return value


def _compare_equal(left: Any, right: Any) -> bool:
    """Compare two values, tolerating the string/bool and string/number mix in YAML."""
    if left == right:
        return True
    if isinstance(left, bool) or isinstance(right, bool):
        return _coerce(left) is _coerce(right)
    if isinstance(left, int | float) and isinstance(right, str):
        return str(left) == right.strip()
    if isinstance(right, int | float) and isinstance(left, str):
        return str(right) == left.strip()
    return False


def evaluate(expression: str, context: dict[str, Any]) -> Any:
    """Evaluate one small expression against a context mapping.

    Returns the expression's value, which for a ternary is whichever branch was
    selected. Raises :class:`ConditionError` for anything unsupported.
    """
    resolved = resolve_placeholders(expression, context)
    try:
        tree = ast.parse(resolved, mode="eval")
    except SyntaxError as exc:
        raise ConditionError(f"Invalid expression '{expression}'.") from exc

    def visit(node: ast.AST) -> Any:
        if isinstance(node, ast.Expression):
            return visit(node.body)
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.Name):
            if node.id in context:
                return context[node.id]
            # YAML writes true/false/null in lower case, and Python's parser
            # reads those as plain names. Treat them as the literals an author
            # obviously meant, unless a variable of that name shadows them.
            if node.id in _YAML_LITERALS:
                return _YAML_LITERALS[node.id]
            # `env` falls back to PIPLY_ENV so the common deployment-stage check
            # works without declaring it first.
            if node.id == "env":
                return os.environ.get("PIPLY_ENV")
            return None
        if isinstance(node, ast.IfExp):
            return visit(node.body) if _truthy(visit(node.test)) else visit(node.orelse)
        if isinstance(node, ast.BoolOp):
            values = [_truthy(visit(item)) for item in node.values]
            return all(values) if isinstance(node.op, ast.And) else any(values)
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
            return not _truthy(visit(node.operand))
        if isinstance(node, ast.Compare):
            left = visit(node.left)
            for operator, comparator in zip(node.ops, node.comparators, strict=True):
                right = visit(comparator)
                passed = _apply_comparison(operator, left, right, expression)
                if not passed:
                    return False
                left = right
            return True
        if isinstance(node, ast.List):
            return [visit(item) for item in node.elts]
        if isinstance(node, ast.Tuple):
            return tuple(visit(item) for item in node.elts)
        raise ConditionError(f"Unsupported expression '{expression}'.")

    return visit(tree)


def _apply_comparison(operator: ast.cmpop, left: Any, right: Any, expression: str) -> bool:
    """Apply one comparison operator with YAML-friendly coercion."""
    if isinstance(operator, ast.Eq):
        return _compare_equal(left, right)
    if isinstance(operator, ast.NotEq):
        return not _compare_equal(left, right)
    if isinstance(operator, ast.In):
        return left in right if isinstance(right, list | tuple | set | str | dict) else False
    if isinstance(operator, ast.NotIn):
        return left not in right if isinstance(right, list | tuple | set | str | dict) else True
    if isinstance(operator, ast.Lt | ast.LtE | ast.Gt | ast.GtE):
        return _compare_ordered(operator, left, right, expression)
    raise ConditionError(f"Unsupported operator in '{expression}'.")


def _compare_ordered(operator: ast.cmpop, left: Any, right: Any, expression: str) -> bool:
    """Compare two values by order, converting numeric-looking strings first."""
    left_value, right_value = _as_number(left), _as_number(right)
    if left_value is None or right_value is None:
        raise ConditionError(f"Cannot order-compare non-numeric values in '{expression}'.")
    if isinstance(operator, ast.Lt):
        return left_value < right_value
    if isinstance(operator, ast.LtE):
        return left_value <= right_value
    if isinstance(operator, ast.Gt):
        return left_value > right_value
    return left_value >= right_value


def _as_number(value: Any) -> float | None:
    """Return a numeric form of a value, or None when it is not numeric."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _truthy(value: Any) -> bool:
    """Return the truth of a value, treating "false"/"no"/"0" as false."""
    coerced = _coerce(value)
    if isinstance(coerced, str):
        return bool(coerced.strip())
    return bool(coerced)


def evaluate_boolean(expression: str, context: dict[str, Any]) -> bool:
    """Evaluate an expression and reduce it to a boolean, as ``run_if`` needs."""
    return _truthy(evaluate(expression, context))


def looks_like_conditional(value: Any) -> bool:
    """Return whether a scalar looks like an inline ``A if C else B``.

    Deliberately conservative: it must contain both keywords as whole words and
    parse as a ternary. An ordinary sentence such as "run if you can" does not.
    """
    if not isinstance(value, str) or not _TERNARY_SHAPE.search(value):
        return False
    try:
        tree = ast.parse(resolve_placeholders(value, {}), mode="eval")
    except SyntaxError:
        return False
    return isinstance(tree.body, ast.IfExp)


def evaluate_value(raw_value: Any, context: dict[str, Any], label: str) -> Any:
    """Resolve a config value that may be conditional.

    Three forms are accepted:

    * an inline ternary, ``true if env == "dev" else false``
    * an explicit mapping, ``{if: ..., then: ..., else: ...}``
    * anything else, returned unchanged

    The mapping form is preferred for anything non-trivial because it cannot be
    confused with prose.
    """
    if isinstance(raw_value, dict) and "if" in raw_value:
        if "then" not in raw_value:
            raise ConditionError(f"{label} conditional needs a 'then' value")
        try:
            chosen = evaluate_boolean(str(raw_value["if"]), context)
        except ConditionError as exc:
            raise ConditionError(f"{label}: {exc}") from exc
        if chosen:
            return raw_value["then"]
        return raw_value.get("else")

    if looks_like_conditional(raw_value):
        try:
            return evaluate(str(raw_value), context)
        except ConditionError as exc:
            raise ConditionError(f"{label}: {exc}") from exc

    return raw_value
