"""Book semantics: absolute sets, zero-deletes, and the top-of-book read."""

from __future__ import annotations

import pytest

from collector.book import Book


def test_apply_sets_an_absolute_size_rather_than_an_increment() -> None:
    book = Book()
    book.apply("bid", 100, 10)
    book.apply("bid", 100, 4)

    # 4, not 14. The increment reading leaves a book that drifts slowly and
    # stays plausible, which is the failure mode worth a dedicated test.
    assert book.bids == {100: 4}


def test_zero_size_deletes_the_level() -> None:
    book = Book()
    book.apply("ask", 200, 7)
    book.apply("ask", 200, 0)

    assert book.asks == {}


def test_zero_size_on_an_absent_level_is_a_no_op() -> None:
    book = Book()
    book.apply("ask", 200, 0)

    assert book.asks == {}


def test_sides_are_independent() -> None:
    book = Book()
    book.apply("bid", 100, 1)
    book.apply("ask", 100, 2)

    assert book.bids == {100: 1}
    assert book.asks == {100: 2}


def test_negative_size_is_a_protocol_violation() -> None:
    book = Book()

    with pytest.raises(ValueError, match="negative size"):
        book.apply("bid", 100, -1)


def test_best_bid_ask_is_the_max_bid_and_the_min_ask() -> None:
    book = Book()
    for ticks in (98, 99, 100):
        book.apply("bid", ticks, 1)
    for ticks in (101, 102, 103):
        book.apply("ask", ticks, 1)

    assert book.best_bid_ask() == (100, 101)


def test_best_bid_ask_is_none_on_an_empty_side() -> None:
    book = Book()
    book.apply("bid", 100, 1)

    assert book.best_bid_ask() == (100, None)


def test_best_bid_ask_follows_a_deletion_at_the_top() -> None:
    book = Book()
    book.apply("bid", 99, 1)
    book.apply("bid", 100, 1)
    book.apply("bid", 100, 0)

    assert book.best_bid_ask() == (99, None)


def test_clear_drops_both_sides() -> None:
    book = Book()
    book.apply("bid", 100, 1)
    book.apply("ask", 101, 1)
    book.clear()

    assert book.best_bid_ask() == (None, None)
