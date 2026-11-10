"""Conjunctive predicates used for block pruning and record filtering.

Only a small, well understood predicate surface is supported:

* comparisons (``=``, ``!=``, ``<``, ``<=``, ``>``, ``>=``),
* inclusive ranges (:func:`between`),
* ``is null`` / ``is not null``,
* combined with logical AND (a :class:`Query` is a conjunction).

The same predicate objects are used in two places:

1. :meth:`Query.can_skip_block` decides whether a whole block can be proven
   not to contain any matching record using only min/max/null_count
   statistics.  This is deliberately conservative - whenever a proof is not
   possible the block is kept.
2. :meth:`Query.evaluate` re-checks every record that is actually decoded, so
   pruning can never change the result set.

Comparisons follow SQL style three-valued logic: a ``None`` value never
matches a comparison (only an explicit ``is null`` does).
"""

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional, Tuple

EQ = "eq"
NE = "ne"
LT = "lt"
LE = "le"
GT = "gt"
GE = "ge"
IS_NULL = "is_null"
NOT_NULL = "not_null"

#: Every operator understood by the index.
OPS = frozenset({EQ, NE, LT, LE, GT, GE, IS_NULL, NOT_NULL})

_COMPARISONS = frozenset({EQ, NE, LT, LE, GT, GE})


def _is_supported_literal(value: Any) -> bool:
    """Return True for int (but not bool) and str literals."""
    if isinstance(value, bool):
        return False
    return isinstance(value, (int, str))


@dataclass(frozen=True)
class Predicate:
    """A single predicate against one top-level field."""

    field: str
    op: str
    value: Optional[Any] = None

    def __post_init__(self) -> None:
        if self.op not in OPS:
            raise ValueError(f"unsupported operator: {self.op!r}")
        if self.op in _COMPARISONS and not _is_supported_literal(self.value):
            raise ValueError(
                "comparison literals must be int or str, got "
                f"{type(self.value).__name__} for field {self.field!r}"
            )


def eq(field: str, value: Any) -> Predicate:
    return Predicate(field, EQ, value)


def ne(field: str, value: Any) -> Predicate:
    return Predicate(field, NE, value)


def lt(field: str, value: Any) -> Predicate:
    return Predicate(field, LT, value)


def le(field: str, value: Any) -> Predicate:
    return Predicate(field, LE, value)


def gt(field: str, value: Any) -> Predicate:
    return Predicate(field, GT, value)


def ge(field: str, value: Any) -> Predicate:
    return Predicate(field, GE, value)


def is_null(field: str) -> Predicate:
    return Predicate(field, IS_NULL, None)


def isnot_null(field: str) -> Predicate:
    return Predicate(field, NOT_NULL, None)


def between(field: str, low: Any, high: Any) -> "Query":
    """Inclusive range predicate ``low <= field <= high``."""
    if not _is_supported_literal(low) or not _is_supported_literal(high):
        raise ValueError("range bounds must be int or str")
    if type(low) is not type(high) or low > high:
        raise ValueError("range bounds must be comparable and low <= high")
    return Query((Predicate(field, GE, low), Predicate(field, LE, high)))


class _Column:
    """Fluent builder so callers can write ``column("x").ge(0)``."""

    def __init__(self, field: str) -> None:
        self.field = field

    def eq(self, value: Any) -> Predicate:
        return eq(self.field, value)

    def ne(self, value: Any) -> Predicate:
        return ne(self.field, value)

    def lt(self, value: Any) -> Predicate:
        return lt(self.field, value)

    def le(self, value: Any) -> Predicate:
        return le(self.field, value)

    def gt(self, value: Any) -> Predicate:
        return gt(self.field, value)

    def ge(self, value: Any) -> Predicate:
        return ge(self.field, value)

    def is_null(self) -> Predicate:
        return is_null(self.field)

    def isnot_null(self) -> Predicate:
        return isnot_null(self.field)

    def between(self, low: Any, high: Any) -> "Query":
        return between(self.field, low, high)


def column(field: str) -> _Column:
    """Return a fluent predicate builder for a field."""
    return _Column(field)


@dataclass(frozen=True)
class Query:
    """A conjunction (logical AND) of :class:`Predicate` objects."""

    predicates: Tuple[Predicate, ...]

    def __init__(self, predicates: Iterable[Predicate] = ()) -> None:
        preds = tuple(predicates)
        for pred in preds:
            if not isinstance(pred, Predicate):
                raise TypeError("Query only accepts Predicate instances")
        object.__setattr__(self, "predicates", preds)

    @classmethod
    def all_of(cls, *predicates: Any) -> "Query":
        """Build a conjunction from predicates or nested queries."""
        flat = []
        for item in predicates:
            if isinstance(item, Query):
                flat.extend(item.predicates)
            elif isinstance(item, Predicate):
                flat.append(item)
            else:
                raise TypeError("all_of expects Predicate or Query arguments")
        return cls(flat)

    def __and__(self, other: "Query") -> "Query":
        return Query.all_of(self, other)

    @property
    def fields(self) -> Tuple[str, ...]:
        return tuple(dict.fromkeys(pred.field for pred in self.predicates))

    def evaluate(self, record: Mapping[str, Any]) -> bool:
        """Return True only if every predicate holds for the record."""
        return all(_eval_predicate(record, pred) for pred in self.predicates)

    def can_skip_block(
        self, block_stats: Mapping[str, Mapping[str, Any]]
    ) -> bool:
        """Return True if the block can be proven not to match.

        A conjunction is impossible as soon as one of its predicates is
        impossible for every record in the block.
        """
        return any(
            _predicate_impossible(pred, block_stats.get(pred.field))
            for pred in self.predicates
        )


def _eval_predicate(record: Mapping[str, Any], pred: Predicate) -> bool:
    value = record.get(pred.field)
    if pred.op == IS_NULL:
        return value is None
    if pred.op == NOT_NULL:
        return value is not None
    if value is None:
        # SQL semantics: NULL never satisfies a comparison.
        return False
    if not _is_supported_literal(value) or not isinstance(
        value, type(pred.value)
    ):
        # Defensive: never claim a match on an unexpected type.
        return False
    if pred.op == EQ:
        return value == pred.value
    if pred.op == NE:
        return value != pred.value
    if pred.op == LT:
        return value < pred.value
    if pred.op == LE:
        return value <= pred.value
    if pred.op == GT:
        return value > pred.value
    if pred.op == GE:
        return value >= pred.value
    raise AssertionError(f"unhandled operator {pred.op!r}")  # pragma: no cover


def _predicate_impossible(
    pred: Predicate, stats: Optional[Mapping[str, Any]]
) -> bool:
    """Conservative block-level test using min/max/null_count."""
    if stats is None:
        # No statistics for this field - cannot prove anything.
        return False

    null_count = stats["null_count"]
    block_count = stats["record_count"]
    minimum = stats["min"]
    maximum = stats["max"]

    if pred.op == IS_NULL:
        # No nulls in the block -> no record matches is_null.
        return null_count == 0
    if pred.op == NOT_NULL:
        # Every value is null -> no non-null record exists.
        return null_count == block_count

    # All remaining predicates need a non-null value.
    if minimum is None:
        # The block only contains nulls for this field.
        return True

    literal = pred.value
    if not _is_supported_literal(literal):
        return False
    if not isinstance(minimum, type(literal)) or not isinstance(
        maximum, type(literal)
    ):
        # Mixed/unknown physical type - do not prune.
        return False

    if pred.op == EQ:
        return literal < minimum or literal > maximum
    if pred.op == NE:
        # != can match any non-null value and most blocks, never prune.
        return False
    if pred.op == LT:
        # Nothing is strictly less than the literal if min >= literal.
        return minimum >= literal
    if pred.op == LE:
        return minimum > literal
    if pred.op == GT:
        # Nothing is strictly greater if max <= literal.
        return maximum <= literal
    if pred.op == GE:
        return maximum < literal
    raise AssertionError(f"unhandled operator {pred.op!r}")  # pragma: no cover
