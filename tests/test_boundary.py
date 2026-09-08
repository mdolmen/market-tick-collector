"""Nothing downstream of an adapter may learn which venue a record came from.

`CLAUDE.md` calls this the design and `NOTES.md` explains why; this is the part
that keeps it true a year from now. It is cheap to assert and expensive to
notice broken — a single `from collector.adapters import binance` in the book
would restore the coupling Phase 2 spent its whole budget removing, and every
test would still pass.

Static, over the module ASTs, rather than by importing anything. An import test
would pass on a module that reaches for a venue lazily inside a function, which
is exactly how this boundary would erode in practice.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

_COLLECTOR = Path(__file__).parent.parent / "collector"
_ADAPTERS = "collector.adapters"

# The whole pipeline downstream of the boundary. Each of these is handed a
# `VenueAdapter` or a normalized record and must work the same whichever venue
# produced it — so none of them may name one.
DOWNSTREAM = (
    "transform.py",
    "book.py",
    "model.py",
    "sinks.py",
    "capture.py",
    "replay.py",
    "source.py",
    "router.py",
    "shard.py",
)

# `collector.adapters` itself resolves a venue name to a venue: it is the one
# place allowed to know they exist, and `main` is the one place allowed to ask.
RESOLVERS = ("adapters/__init__.py", "main.py")


def _imported_modules(path: Path) -> set[str]:
    """Every module named by an import anywhere in the file, nested included."""
    tree = ast.parse(path.read_text())
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
            # `from collector.adapters import binance` names the venue in the
            # alias, not the module.
            modules.update(f"{node.module}.{a.name}" for a in node.names)
    return modules


def _venue_modules() -> set[str]:
    """Concrete adapters: everything under `adapters/` that is not the contract."""
    return {
        f"{_ADAPTERS}.{path.stem}"
        for path in (_COLLECTOR / "adapters").glob("*.py")
        if path.stem not in {"__init__", "base"}
    }


def test_there_is_more_than_one_venue_to_confuse() -> None:
    # Guards the guard: with a single adapter every assertion below passes
    # trivially, and the boundary would be untested exactly when it is easiest
    # to break.
    assert len(_venue_modules()) >= 2


@pytest.mark.parametrize("name", DOWNSTREAM)
def test_downstream_names_no_venue(name: str) -> None:
    imported = _imported_modules(_COLLECTOR / name)
    assert not imported & _venue_modules(), (
        f"{name} imports a concrete venue; it must take a VenueAdapter instead"
    )


@pytest.mark.parametrize("name", DOWNSTREAM)
def test_downstream_may_still_use_the_contract(name: str) -> None:
    # The complement, so the rule above cannot be satisfied by a module that
    # simply stopped depending on the boundary at all.
    imported = _imported_modules(_COLLECTOR / name)
    assert f"{_ADAPTERS}.base" in imported or not imported & {_ADAPTERS}, name


@pytest.mark.parametrize("name", RESOLVERS)
def test_the_resolver_is_where_venues_are_named(name: str) -> None:
    imported = _imported_modules(_COLLECTOR / name)
    assert imported & {_ADAPTERS, *_venue_modules()}, (
        f"{name} is meant to be a place a venue is resolved"
    )
