"""Monotonic, non-overlapping scan schedule calculations."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ScanScheduleDecision:
    """The next deadline and delay evidence for one completed scan."""

    next_deadline: float
    cycle_overrun_ms: float
    skipped_ticks: int


def next_scan_deadline(
    *,
    scheduled_deadline: float,
    finished_at: float,
    interval_sec: float,
) -> ScanScheduleDecision:
    """Advance an anchored deadline without replaying missed scan ticks."""

    if interval_sec <= 0:
        raise ValueError("interval_sec must be positive")

    next_deadline = scheduled_deadline + interval_sec
    skipped_ticks = 0
    while next_deadline < finished_at:
        next_deadline += interval_sec
        skipped_ticks += 1

    return ScanScheduleDecision(
        next_deadline=next_deadline,
        cycle_overrun_ms=max(0.0, finished_at - (scheduled_deadline + interval_sec))
        * 1000,
        skipped_ticks=skipped_ticks,
    )
