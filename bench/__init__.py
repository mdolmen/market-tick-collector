"""Committed, reproducible benchmarks.

A package rather than a directory of scripts because ``bench/clickhouse.py``
reads a capture through ``bench.volume.sessions``: without this marker mypy
resolves that file as both ``volume`` and ``bench.volume`` and refuses to check
anything.
"""
