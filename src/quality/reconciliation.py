from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ReconciliationResult:
    source_count: int
    target_count: int
    difference: int


def reconcile_counts(source_count: int, target_count: int) -> ReconciliationResult:
    return ReconciliationResult(
        source_count=source_count,
        target_count=target_count,
        difference=source_count - target_count,
    )
