"""Independent oracle for the window rules, compared against the service.

The reference implementation below is deliberately naive: it enumerates every
candidate start and scans every sample for each window.  The production code
sweeps forward once.  If the fast sweep ever disagrees, this test fails.
"""
import math
from datetime import datetime, timedelta, timezone

import pytest

from src.services import windowing as W

BASE = datetime(2026, 7, 11, 22, 0, tzinfo=timezone.utc)


def sample(pipe, minute, *, flow=12.0, pressure=500.0, acoustic=40.0, temperature=18.0, age=20.0, leak=0, second=0, micro=0):
    return {
        "pipe_id": pipe,
        "sampled_at": BASE + timedelta(minutes=minute, seconds=second, microseconds=micro),
        "flow": flow,
        "pressure": pressure,
        "acoustic_energy": acoustic,
        "temperature": temperature,
        "pipe_age": age,
        "leak": leak,
    }


def oracle(samples):
    """Every UTC whole-minute start between first and last sample, per pipe."""
    by_pipe = {}
    for s in samples:
        by_pipe.setdefault(s["pipe_id"], []).append(s)
    out = []
    for pipe, rows in sorted(by_pipe.items()):
        rows = sorted(rows, key=lambda r: r["sampled_at"])
        first, last = rows[0]["sampled_at"], rows[-1]["sampled_at"]
        start = first.replace(second=0, microsecond=0)
        if start < first:
            start += timedelta(minutes=1)
        while start <= last:
            inside = [r for r in rows if start <= r["sampled_at"] < start + timedelta(minutes=5)]
            if inside:
                out.append((pipe, start, W.aggregate(inside, start)))
            start += timedelta(minutes=1)
    return out


def fast(samples):
    return [
        (pipe, start, window)
        for pipe, start, window, _skipped, _partial in W.iter_windows_stream(sorted(samples, key=lambda s: (s["pipe_id"], s["sampled_at"])))
    ]


def assert_same(samples):
    expected, actual = oracle(samples), fast(samples)
    assert [(p, s) for p, s, _ in expected] == [(p, s) for p, s, _ in actual]
    for (_, _, a), (_, _, b) in zip(expected, actual):
        assert a["label"] == b["label"]
        assert a["samples"] == b["samples"]
        for key, value in a["features"].items():
            assert b["features"][key] == pytest.approx(value, rel=1e-12, abs=1e-12), key


def test_one_sample_per_minute_produces_expected_sliding_starts():
    samples = [sample("P1", m, flow=10 + m) for m in range(10)]
    result = fast(samples)
    starts = [s.minute for _, s, _ in result]
    # Every whole minute from the first sample to the last is a start.
    assert starts == list(range(10))
    # Window [0,5) holds minutes 0..4; flow mean is 12.
    first = result[0][2]
    assert first["features"]["night_flow"] == pytest.approx(12.0)
    assert first["features"]["pressure_drop"] == pytest.approx(0.0)
    assert first["samples"] == 5
    assert first["minute_slots"] == 5


def test_left_closed_right_open_boundaries():
    # Sample exactly on the right edge of the first window and exactly on the
    # second window's start must be counted once, in the later window.
    samples = [sample("P1", m, flow=float(m + 1)) for m in range(6)]
    windows = {start.minute: window for _, start, window in fast(samples)}
    assert windows[0]["features"]["night_flow"] == pytest.approx(3.0)  # minutes 0..4
    assert windows[1]["features"]["night_flow"] == pytest.approx(4.0)  # minutes 1..5


def test_first_and_last_sample_bound_the_starts():
    samples = [sample("P1", m, second=30) for m in (3, 4, 5, 6, 7, 8, 9)]
    starts = [_start.minute for _, _start, _, _, _ in W.iter_windows_stream(sorted(samples, key=lambda s: s["sampled_at"]))]
    # The first sample sits at :03:30, so :03 is not a legal start; the last is
    # at :09:30, so the final start is :09 (no start may follow the last sample).
    assert (min(starts), max(starts)) == (4, 9)


def test_non_empty_window_with_a_missing_minute_is_kept_and_flagged():
    samples = [sample("P1", m, flow=10.0) for m in [0, 1, 2, 3, 4, 5, 6, 8, 9, 10]]
    stream = list(W.iter_windows_stream(sorted(samples, key=lambda s: s["sampled_at"])))
    starts = [_start.minute for _, _start, _, _, _ in stream]
    # Only empty windows are skipped. Window 3 = [3,8) has minutes 3..6 and no
    # minute 7, but it is not empty, so it stays and is flagged partial.
    assert starts == list(range(11))
    partial = {_start.minute: flag for _, _start, _, _, flag in stream}
    assert partial[3] is True
    assert partial[0] is False  # minutes 0..4 all present


def test_empty_window_is_skipped_even_between_far_samples():
    samples = [sample("P1", 0), sample("P1", 40)]
    # Starts 1..35 cover no sample at all; start 0 holds the first sample and
    # 36..40 hold the last, so only those are emitted.
    stream = list(W.iter_windows_stream(sorted(samples, key=lambda s: s["sampled_at"])))
    starts = [_start.minute for _, _start, _, _, _ in stream]
    assert starts == [0, 36, 37, 38, 39, 40]
    assert sum(skip for _, _, _, skip, _ in stream) == 35


def test_aggregations_follow_the_contract():
    window = [
        sample("P1", 0, flow=10.0, pressure=100.0, acoustic=30.0, temperature=17.0, age=19.0, leak=0),
        sample("P1", 2, flow=12.0, pressure=98.0, acoustic=42.0, temperature=19.0, age=21.0, leak=1),
        sample("P1", 4, flow=14.0, pressure=96.0, acoustic=54.0, temperature=21.0, age=23.0, leak=0),
    ]
    agg = W.aggregate(window, BASE)
    f = agg["features"]
    assert f["night_flow"] == pytest.approx(12.0)  # mean
    assert f["pressure_drop"] == pytest.approx(4.0)  # first 100 - last 96
    assert f["acoustic_energy"] == pytest.approx(42.0)  # mean
    assert f["ambient_temp"] == pytest.approx(19.0)  # mean
    # population std of (10,12,14) = sqrt(8/3); divided by mean 12
    assert f["flow_variation"] == pytest.approx(math.sqrt(8.0 / 3.0) / 12.0)
    assert f["pipe_age"] == pytest.approx(23.0)  # latest sample
    assert agg["label"] == 1  # any leak


def test_pressure_drop_is_signed():
    rising = [sample("P1", 0, pressure=90.0), sample("P1", 1, pressure=95.0)]
    falling = [sample("P1", 0, pressure=95.0), sample("P1", 1, pressure=90.0)]
    assert W.aggregate(rising, BASE)["features"]["pressure_drop"] == pytest.approx(-5.0)
    assert W.aggregate(falling, BASE)["features"]["pressure_drop"] == pytest.approx(5.0)


def test_pipe_age_uses_latest_instant_not_upload_order():
    window = [sample("P1", 3, age=30.0), sample("P1", 1, age=10.0), sample("P1", 0, age=20.0)]
    assert W.aggregate(window, BASE)["features"]["pipe_age"] == pytest.approx(30.0)


def test_any_leak_marks_the_window():
    window = [sample("P1", 0, leak=0), sample("P1", 1, leak=0), sample("P1", 2, leak=1)]
    assert W.aggregate(window, BASE)["label"] == 1
    assert W.aggregate(window[:2], BASE)["label"] == 0


def test_zero_mean_flow_is_rejected():
    window = [sample("P1", 0, flow=1.0), sample("P1", 1, flow=-1.0)]
    with pytest.raises(W.WindowError, match="流量均值为零"):
        W.aggregate(window, BASE)


def test_naive_timestamp_is_rejected():
    naive = dict(sample("P1", 0))
    naive["sampled_at"] = datetime(2026, 7, 11, 22, 0)
    with pytest.raises(W.WindowError, match="时区"):
        W.check_instant(naive["sampled_at"])


def test_offset_timestamps_are_utc_normalised():
    stamped = dict(sample("P1", 0))
    stamped["sampled_at"] = datetime(2026, 7, 12, 6, 0, tzinfo=timezone(timedelta(hours=8)))
    assert W.check_instant(stamped["sampled_at"]) == datetime(2026, 7, 11, 22, 0, tzinfo=timezone.utc)


def test_duplicate_reading_comparison():
    a = sample("P1", 0)
    assert W.same_sample(a, dict(a))
    assert not W.same_sample(a, dict(a, flow=a["flow"] + 0.5))
    assert not W.same_sample(a, dict(a, leak=1))


@pytest.mark.parametrize("seed", range(6))
def test_matches_oracle_on_random_ragged_sampling(seed):
    import random
    rng = random.Random(seed)
    samples = []
    for pipe in ("P01", "P02", "P03"):
        minute = 0.0
        while minute < rng.uniform(30, 90):
            if rng.random() < 0.18:  # irregular gaps
                minute += rng.uniform(1.0, 6.0)
                continue
            samples.append(sample(pipe, int(minute), second=rng.choice([0, 0, 15, 45]), flow=rng.uniform(5, 20), leak=int(rng.random() < 0.15)))
            minute += rng.choice([1.0, 1.0, 1.0, 2.0])
    rng.shuffle(samples)
    assert_same(samples)


def test_matches_oracle_on_long_pipe_spanning_many_chunks():
    samples = [sample("P1", m, flow=5 + (m % 7)) for m in range(0, 400)]
    samples += [sample("P2", m, flow=8.0) for m in range(150, 260)]
    assert_same(samples)


def test_long_empty_stretch_is_skipped_quickly():
    # Two samples a day apart: the naive start enumeration would be 1440
    # candidates; the sweep must still return only the real windows.
    samples = [sample("P1", 0), sample("P1", 1440)]
    result = fast(samples)
    assert result[0][1] == BASE
    assert result[-1][1] == BASE + timedelta(minutes=1440)
    assert len(result) == 6  # start 0, then 1436..1440
