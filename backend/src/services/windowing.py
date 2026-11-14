"""Field-sampling to five-minute windows.

Pure functions only: no database, no clock.  Input observations must already
be sorted by (pipe_id, UTC instant).

Rules implemented here, and nowhere else:

* One window per UTC whole-minute start S, sliding one minute at a time.
* A start S must lie between the pipe's first and last sample: first <= S.
* A window covers [S, S + 5 minutes), left-closed and right-open.
* A start whose window holds no sample at all is skipped (empty window).
  A non-empty window that misses some of the five minutes is kept as-is and
  reported as partial coverage; its aggregation uses the samples it has.
* flow / acoustic_energy / temperature -> arithmetic mean
* pressure -> first sample's pressure minus last sample's pressure
* flow_variation -> population standard deviation of flow / mean flow
* pipe_age -> the latest sample's value
* label -> 1 if any sample in the window is labelled leak
* a window whose mean flow is zero is rejected, because flow_variation would
  be undefined

Sliding the start forward by one minute only moves the right edge forward by
one minute, so a single forward sweep over time-ordered samples is exact.  At
most one window of samples is buffered, which keeps a long pipe's history out
of process memory.

Duplicate observations for the same pipe and instant never reach this module:
services.rawdatasets collapses identical re-sends and rejects conflicting ones.
"""
from __future__ import annotations

import itertools
import math
from collections import deque
from datetime import datetime, timedelta, timezone

WINDOW_MINUTES = 5
SLOT_SECONDS = 60
SLOT_COUNT = WINDOW_MINUTES  # five whole-minute slots, left-closed right-open
SPAN = timedelta(minutes=WINDOW_MINUTES)
ONE_MINUTE = timedelta(minutes=1)

# Agreement tolerance when the same (pipe, instant) is re-sent.  Field sensors
# write floats; a retry of the identical record must compare equal even though
# it round-trips through decimal text, so values within this fraction of their
# magnitude (or this absolute floor) count as the same reading.
_DEDUP_ABS = 1e-9
_DEDUP_REL = 1e-12


class WindowError(ValueError):
    """Input that cannot yield a well-formed window set."""


def _same_reading(a: float, b: float) -> bool:
    return a == b or abs(a - b) <= max(_DEDUP_ABS, _DEDUP_REL * max(abs(a), abs(b)))


def same_sample(a: dict, b: dict) -> bool:
    """True when two uploads of one natural key carry the same reading."""
    return a["leak"] == b["leak"] and all(
        _same_reading(float(a[k]), float(b[k]))
        for k in ("flow", "pressure", "acoustic_energy", "temperature", "pipe_age")
    )


def check_instant(value: datetime) -> datetime:
    """Utc-normalise an aware timestamp; reject naive input (no timezone)."""
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise WindowError("采样时间必须带时区（UTC 或可换算偏移），无时区记录拒绝")
    return value.astimezone(timezone.utc)


def ceil_minute(value: datetime) -> datetime:
    minute = value.replace(second=0, microsecond=0)
    return minute if minute == value else minute + ONE_MINUTE


def floor_minute(value: datetime) -> datetime:
    return value.replace(second=0, microsecond=0)


def window_starts(first: datetime, last: datetime):
    """Whole-UTC-minute starts S with first <= S <= last."""
    first = check_instant(first)
    last = check_instant(last)
    if last < first:
        raise WindowError("采样时间范围倒置")
    start = ceil_minute(first)
    while start <= last:
        yield start
        start += ONE_MINUTE


def window_reference(pipe_id: str, start: datetime) -> str:
    return f"{pipe_id}@{start.strftime('%Y-%m-%dT%H:%MZ')}"


def minute_slots(samples: list[dict], start: datetime) -> int:
    return len({int((s["sampled_at"] - start).total_seconds()) // SLOT_SECONDS for s in samples})


def aggregate(samples: list[dict], start: datetime) -> dict:
    """Reduce one window's samples to six model features, label and coverage.

    Coverage (sample count, distinct minute slots) travels with the window but
    is not a model feature: training only reads the six features and the label.
    """
    ordered = sorted(samples, key=lambda s: s["sampled_at"])
    n = len(ordered)
    flow = [float(s["flow"]) for s in ordered]
    flow_mean = sum(flow) / n
    if not math.isfinite(flow_mean) or flow_mean == 0:
        raise WindowError(f"流量均值为零，无法计算流量变异（{ordered[0]['pipe_id']} {start.isoformat()}）")
    variance = sum((x - flow_mean) ** 2 for x in flow) / n
    flow_variation = math.sqrt(variance) / flow_mean
    if not math.isfinite(flow_variation):
        raise WindowError(f"流量变异非有限数值（{ordered[0]['pipe_id']} {start.isoformat()}）")
    latest = ordered[-1]
    features = {
        "night_flow": flow_mean,
        "pressure_drop": float(ordered[0]["pressure"]) - float(ordered[-1]["pressure"]),
        "acoustic_energy": sum(float(s["acoustic_energy"]) for s in ordered) / n,
        "flow_variation": flow_variation,
        "pipe_age": float(latest["pipe_age"]),
        "ambient_temp": sum(float(s["temperature"]) for s in ordered) / n,
    }
    if not all(math.isfinite(v) for v in features.values()):
        raise WindowError(f"窗口聚合出现非有限数值（{ordered[0]['pipe_id']} {start.isoformat()}）")
    return {
        "features": features,
        "label": 1 if any(int(s["leak"]) for s in ordered) else 0,
        "samples": n,
        "minute_slots": minute_slots(ordered, start),
    }


def _advance(buffer: deque, start: datetime) -> tuple[list[dict], int]:
    """Rows in [start, start+5min) and the distinct minute slots they hit.

    `buffer` is ordered and every row in it is >= start (callers trim first).
    A row exactly on a minute boundary belongs to that slot (left-closed).
    """
    end = start + SPAN
    inside = []
    for sample in buffer:
        if sample["sampled_at"] >= end:
            break
        inside.append(sample)
    return inside, minute_slots(inside, start)


def _next_possible_start(buffer: deque, start: datetime) -> datetime:
    """Earliest start after `start` that could still contain buffered rows.

    Used only when the current window is empty, to jump over long sparse
    stretches instead of iterating every minute of them.  A window is at most
    five minutes wide, so the next start that can reach the oldest buffered row
    is its own minute - 4 minutes.
    """
    if not buffer:
        return start + ONE_MINUTE
    # A window is at most five minutes wide, so the earliest start that can
    # still reach the oldest buffered row is its own minute minus four.  Floor,
    # not ceiling: a sample at :10:45 is already inside [22:06, 22:11).
    target = floor_minute(buffer[0]["sampled_at"]) - (SLOT_COUNT - 1) * ONE_MINUTE
    return target if target > start + ONE_MINUTE else start + ONE_MINUTE


def _pipe_windows(pipe_id: str, rows):
    """Yield (start, aggregate, skipped_before, partial) for one pipe.

    `skipped_before` counts empty candidate starts since the previous window;
    `partial` is True when the window covers fewer than five minute slots, so
    callers can report sparse coverage without dropping the window.
    """
    buffer: deque = deque()
    start: datetime | None = None
    last: datetime | None = None
    skipped = 0

    def step(at: datetime):
        """Evaluate one start; returns (window, skipped_before, partial) or None."""
        nonlocal skipped
        while buffer and buffer[0]["sampled_at"] < at:
            buffer.popleft()
        inside, slots = _advance(buffer, at)
        if not inside:
            skipped += 1
            return None
        window = aggregate(inside, at)
        result = (window, skipped, slots < SLOT_COUNT)
        skipped = 0
        return result

    for row in rows:
        t = row["sampled_at"]
        if start is None:
            start = ceil_minute(t)
        last = t
        buffer.append(row)
        while start + SPAN <= t:
            outcome = step(start)
            if outcome is None:
                following = _next_possible_start(buffer, start)
                # Every skipped minute is an empty candidate start, so it is
                # counted, not silently dropped.
                skipped += int((following - start) / ONE_MINUTE) - 1
                start = following
            else:
                yield start, outcome[0], outcome[1], outcome[2]
                start += ONE_MINUTE
    if start is not None:
        while start <= last:
            outcome = step(start)
            if outcome is None:
                following = _next_possible_start(buffer, start)
                skipped += int((following - start) / ONE_MINUTE) - 1
                start = following
            else:
                yield start, outcome[0], outcome[1], outcome[2]
                start += ONE_MINUTE


def iter_windows_stream(rows):
    """Yield (pipe_id, start, aggregate, skipped_before, partial) in one sweep.

    `rows` is any iterable sorted by (pipe_id, UTC instant).  Every emitted
    window is final: its right edge has already been passed, so no later row
    can enter it.
    """
    for pipe_id, group in itertools.groupby(rows, key=lambda r: r["pipe_id"]):
        for start, window, skipped, partial in _pipe_windows(pipe_id, group):
            yield pipe_id, start, window, skipped, partial


def iter_windows(samples: list[dict]):
    """List-in / list-out helper: yields (pipe_id, start, aggregate)."""
    ordered = sorted(samples, key=lambda s: (s["pipe_id"], s["sampled_at"]))
    for pipe_id, start, window, _, _ in iter_windows_stream(ordered):
        yield pipe_id, start, window
