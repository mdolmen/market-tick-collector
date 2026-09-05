"""Project sinks.

``ConsoleSink`` prints each record as one JSON line. Wire a run to it instead
of the storage sink to eyeball level rows before committing them to Parquet.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping

from data_pipeline_core import WriteResult


class ConsoleSink:
    """Print each record as one JSON line; for dev/visual confirmation."""

    def write(self, records: Iterable[Mapping[str, object]]) -> WriteResult:
        count = 0
        for record in records:
            print(json.dumps(record, ensure_ascii=False, default=str))
            count += 1
        print(f"[console-sink] {count} record(s)")
        return WriteResult(row_count=count)
