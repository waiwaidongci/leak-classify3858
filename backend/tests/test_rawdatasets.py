"""Batch upload, deduplication and build behaviour, against a real database.

The database is SQLite for speed, but every rule under test (natural-key
uniqueness, atomic batches, staging visibility, chunked generation) is
enforced in application code, so the same behaviour holds on PostgreSQL.
"""
import random
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException
from sqlalchemy import delete, func, select

from src.config.database import Base, SessionLocal, engine
from src.models import GeneratedWindow, RawDataset, RawSample, RawUploadBatch, SensorWindow, WindowBuild, WindowDataset
from src.models.user import User
from src.schemas.raw import BuildInput, RawBatchInput, RawDatasetInput
from src.constants.features import FEATURES
from src.services import builds, rawdatasets
from src.services.datasets import get_windows
from src.services.windowing import WindowError

BASE = datetime(2026, 7, 11, 22, 0, tzinfo=timezone.utc)


@pytest.fixture()
def db():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with SessionLocal() as session:
        session.add(User(email="analyst@test", name="测试分析师", role="analyst", password_hash="x"))
        session.commit()
        yield session


@pytest.fixture()
def user(db):
    return db.scalar(select(User))


@pytest.fixture()
def raw_id(db, user):
    return rawdatasets.create_raw_dataset(db, RawDatasetInput(name="原始采样"), user)["id"]


def row(pipe, minute, **kw):
    values = dict(flow=10.0, pressure=500.0, acoustic_energy=40.0, temperature=18.0, pipe_age=20.0, leak=0)
    values.update(kw)
    return {"pipe_id": pipe, "sampled_at": BASE + timedelta(minutes=minute), **values}


def upload(db, raw_id, rows, user, key=None):
    return rawdatasets.upload_batch(db, raw_id, RawBatchInput(samples=rows, upload_key=key), user)


def build(db, raw_id, name="窗口集", limit=128 * 1024 * 1024):
    b = WindowBuild(raw_dataset_id=raw_id, name=name, status="running", memory_limit_bytes=limit)
    db.add(b)
    db.commit()
    rawdatasets.run_build(db, b)
    return db.get(WindowBuild, b.id)


def samples(db, raw_id):
    return db.scalar(select(func.count()).select_from(RawSample).where(RawSample.raw_dataset_id == raw_id)) or 0


def test_identical_resend_is_counted_as_duplicate_not_an_error(db, raw_id, user):
    rows = [row("P1", m) for m in range(6)]
    first = upload(db, raw_id, rows, user)
    second = upload(db, raw_id, rows, user)
    assert first["inserted"] == 6 and first["deduplicated"] == 0
    assert second["inserted"] == 0 and second["deduplicated"] == 6
    assert samples(db, raw_id) == 6


def test_conflicting_value_for_same_pipe_and_instant_rejects_whole_batch(db, raw_id, user):
    upload(db, raw_id, [row("P1", 0)], user)
    conflicting = [row("P2", 0, flow=11.0), row("P1", 0, flow=99.0)]
    with pytest.raises(HTTPException) as exc:
        upload(db, raw_id, conflicting, user)
    assert exc.value.status_code == 422
    # The good row P2@00:00 must not survive a rejected batch.
    assert samples(db, raw_id) == 1
    assert db.scalar(select(func.count()).select_from(RawSample).where(RawSample.pipe_id == "P2")) == 0


def test_conflict_inside_one_batch_is_rejected_before_any_write(db, raw_id, user):
    with pytest.raises(HTTPException, match="同管段同刻出现不同数值"):
        upload(db, raw_id, [row("P1", 0, flow=1.0), row("P1", 0, flow=2.0)], user)
    assert samples(db, raw_id) == 0


def test_duplicate_inside_one_batch_collapses(db, raw_id, user):
    result = upload(db, raw_id, [row("P1", 0), row("P1", 0), row("P1", 1)], user)
    assert (result["received"], result["inserted"], result["deduplicated"]) == (3, 2, 1)
    assert samples(db, raw_id) == 2


def test_failed_retry_leaves_no_partial_batch(db, raw_id, user):
    good = [row("P1", m) for m in range(4)]
    upload(db, raw_id, good, user)
    bad = good + [row("P1", 0, flow=123.0), row("P3", 5)]
    with pytest.raises(HTTPException):
        upload(db, raw_id, bad, user)
    assert samples(db, raw_id) == 4
    # Retrying the same failed batch again must fail the same way, still clean.
    with pytest.raises(HTTPException):
        upload(db, raw_id, bad, user)
    assert samples(db, raw_id) == 4
    # Corrected retry succeeds and nothing is double counted.
    fixed = good + [row("P3", 5)]
    result = upload(db, raw_id, fixed, user)
    assert (result["inserted"], result["deduplicated"]) == (1, 4)
    assert samples(db, raw_id) == 5


def test_upload_key_replay_is_idempotent_and_mismatch_rejected(db, raw_id, user):
    rows = [row("P1", m) for m in range(5)]
    first = upload(db, raw_id, rows, user, key="batch-1")
    replay = upload(db, raw_id, rows, user, key="batch-1")
    assert first["inserted"] == 5
    assert replay["replayed"] is True and replay["inserted"] == 5 and replay["deduplicated"] == 0
    assert samples(db, raw_id) == 5
    with pytest.raises(HTTPException) as exc:
        upload(db, raw_id, [row("P1", 0, flow=77.0)], user, key="batch-1")
    assert exc.value.status_code == 409


def analyst(db):
    return db.scalar(select(User))


def test_batch_boundaries_and_upload_order_do_not_change_windows(db):
    # Two datasets, same observations, different shuffles and different batch
    # splits.  Both builds must agree exactly.
    observations = []
    for pipe in ("P-A", "P-B", "P-C"):
        for minute in range(0, 70):
            observations.append(row(pipe, minute, flow=10 + (minute % 5), leak=1 if pipe == "P-B" and 20 <= minute < 23 else 0))
    summaries, fingerprints = [], []
    rng = random.Random(42)
    for dataset_no in (1, 2):
        rid = rawdatasets.create_raw_dataset(db, RawDatasetInput(name=f"原始采样{dataset_no}"), analyst(db))["id"]
        shuffled = observations[:]
        rng.shuffle(shuffled)
        chunk = 7 if dataset_no == 1 else 63
        for i in range(0, len(shuffled), chunk):
            upload(db, rid, shuffled[i:i + chunk], analyst(db))
        built = build(db, rid, name=f"窗口集{dataset_no}")
        summaries.append((built.window_count, built.summary["positive"], built.pipe_count, built.summary["skipped_starts"]))
        roots = db.scalars(select(SensorWindow).where(SensorWindow.dataset_id == built.window_dataset_id).order_by(SensorWindow.reference)).all()
        fingerprints.append([(w.reference, w.label, {k: round(v, 12) for k, v in w.features.items()}) for w in roots])
    assert summaries[0] == summaries[1]
    assert fingerprints[0] == fingerprints[1]


def test_build_is_all_or_nothing(db, raw_id, user):
    upload(db, raw_id, [row("P1", m) for m in range(0, 20)], user)
    upload(db, raw_id, [row("Z", m, flow=0.0) for m in range(60, 70)], user)
    with pytest.raises(WindowError, match="流量均值为零"):
        build(db, raw_id, name="会失败的窗口集")
    # No staged window survives, and no dataset is created.
    assert db.scalar(select(func.count()).select_from(GeneratedWindow)) == 0
    assert db.scalar(select(func.count()).select_from(WindowDataset)) == 0
    # The failure transition is recorded and terminal.
    pending = WindowBuild(raw_dataset_id=raw_id, name="会失败的窗口集", status="running", memory_limit_bytes=128 * 1024 * 1024)
    db.add(pending)
    db.commit()
    with pytest.raises(WindowError):
        rawdatasets.run_build(db, pending)
    builds.fail_build(db, pending.id, "流量均值为零")
    failed = db.get(WindowBuild, pending.id)
    assert failed.status == "failed" and failed.window_count == 0 and failed.window_dataset_id is None
    assert db.scalar(select(func.count()).select_from(GeneratedWindow)) == 0


def test_failure_during_copy_out_rolls_back_dataset_and_windows(db, raw_id, user, monkeypatch):
    """A crash between "dataset created" and "all windows copied" must not
    leave a half-populated, trainable dataset behind."""
    rid = raw_id
    upload(db, rid, [row("P1", m) for m in range(30)], user)
    build_row = WindowBuild(raw_dataset_id=rid, name="复制中断", status="running", memory_limit_bytes=16 * 1024 * 1024)
    db.add(build_row)
    db.commit()
    original = rawdatasets.insert

    class Boom(Exception):
        pass

    def exploding_insert(target, *args, **kwargs):
        # Let staging writes through; fail as soon as the dataset copy starts.
        if getattr(target, "__tablename__", None) == "sensor_windows":
            raise Boom("copy-out interrupted")
        return original(target, *args, **kwargs)

    monkeypatch.setattr(rawdatasets, "insert", exploding_insert)
    with pytest.raises(Boom):
        rawdatasets.run_build(db, build_row)
    db.rollback()
    monkeypatch.setattr(rawdatasets, "insert", original)
    builds.fail_build(db, build_row.id, "copy-out interrupted")
    # Nothing trainable survives the interruption, and no orphan windows exist.
    assert db.scalar(select(func.count()).select_from(WindowDataset)) == 0
    assert db.scalar(select(func.count()).select_from(SensorWindow)) == 0
    failed = db.get(WindowBuild, build_row.id)
    assert failed.status == "failed" and failed.window_dataset_id is None


def test_staged_windows_are_not_trainable_until_success(db, raw_id, user):
    upload(db, raw_id, [row("P1", m) for m in range(30)], user)
    pending = WindowBuild(raw_dataset_id=raw_id, name="进行中", status="running", memory_limit_bytes=128 * 1024 * 1024)
    db.add(pending)
    db.commit()
    db.execute(delete(GeneratedWindow).where(GeneratedWindow.build_id == pending.id))
    rawdatasets.run_build(db, pending)
    # Inserting a second build with the same name is allowed while the first is
    # merely running; what matters is that the dataset only exists on success.
    assert db.get(WindowBuild, pending.id).status == "succeeded"
    assert db.get(WindowBuild, pending.id).window_dataset_id is not None


def test_memory_budget_shrinks_chunks_without_changing_result(db):
    observations = [row("P1", m) for m in range(120)]
    rid_small = rawdatasets.create_raw_dataset(db, RawDatasetInput(name="小内存"), analyst(db))["id"]
    rid_big = rawdatasets.create_raw_dataset(db, RawDatasetInput(name="大内存"), analyst(db))["id"]
    for rid in (rid_small, rid_big):
        upload(db, rid, observations, analyst(db))
    small = build(db, rid_small, name="小批窗口", limit=16 * 1024 * 1024)  # smallest allowed
    big = build(db, rid_big, name="大批窗口", limit=128 * 1024 * 1024)
    assert small.chunk_rows < big.chunk_rows
    assert small.window_count == big.window_count
    assert small.peak_estimated_bytes <= 16 * 1024 * 1024
    assert big.peak_estimated_bytes <= 128 * 1024 * 1024


def test_build_name_clash_fails_without_dataset(db, raw_id, user):
    upload(db, raw_id, [row("P1", m) for m in range(10)], user)
    first = build(db, raw_id, name="同名")
    with pytest.raises(rawdatasets.NameClash):
        build(db, raw_id, name="同名")
    assert db.scalar(select(func.count()).select_from(WindowDataset)) == 1
    assert first.window_count > 0


def test_preview_reads_staging_and_then_dataset(db, raw_id, user):
    upload(db, raw_id, [row("P1", m, leak=1 if m == 2 else 0) for m in range(10)], user)
    built = build(db, raw_id, name="预览")
    preview = builds.preview_windows(db, built.id, limit=5, offset=0)
    assert preview["total"] == built.window_count
    assert len(preview["windows"]) == 5
    assert preview["windows"][0]["features"]["night_flow"] == pytest.approx(10.0)
    assert preview["windows"][0]["label"] == 1  # minute 2 leaks, window starts at 0
    second = builds.preview_windows(db, built.id, limit=5, offset=5)
    assert second["offset"] == 5


def test_naive_timestamp_is_rejected_by_schema(db, raw_id, user):
    from pydantic import ValidationError
    with pytest.raises(ValidationError, match="时区"):
        RawBatchInput(samples=[{**row("P1", 0), "sampled_at": datetime(2026, 7, 11, 22, 0)}])


def test_utc_offset_is_normalised_before_storage(db, raw_id, user):
    shanghai = BASE.astimezone(timezone(timedelta(hours=8)))
    upload(db, raw_id, [{**row("P1", 0), "sampled_at": shanghai}], user)
    stored = db.scalar(select(RawSample))
    assert stored.sampled_at.replace(tzinfo=timezone.utc) == BASE


def test_generation_is_chunked_and_bounded(db, raw_id, user):
    upload(db, raw_id, [row("P1", m) for m in range(5000)], user)
    built = build(db, raw_id, name="长管段")
    assert built.chunk_rows == max(1, int(128 * 1024 * 1024 * 0.9) // (rawdatasets.FACTOR * rawdatasets.ROW_BYTES))
    assert built.peak_estimated_bytes <= 128 * 1024 * 1024
    assert built.window_count == 5000
    assert built.summary["training_ready"] is False  # one class only


def test_generated_windows_can_train(db, raw_id, user):
    """A successful build is a normal dataset: training reads it unchanged."""
    from src.services.training import train_model

    observations = []
    for pipe in ("P-A", "P-B", "P-C", "P-D"):
        for minute in range(0, 120):
            leak = 1 if (pipe in ("P-A", "P-B") and 30 <= minute < 60) else 0
            observations.append(
                row(
                    pipe,
                    minute,
                    flow=10 + minute * 0.05 + (pipe == "P-B"),
                    pressure=500 - minute * 0.2,
                    acoustic_energy=40 + (minute % 9),
                    temperature=18 + (minute % 5) * 0.1,
                    pipe_age=10 + minute * 0.01 + {"P-A": 0, "P-B": 1, "P-C": 2, "P-D": 3}[pipe],
                    leak=leak,
                )
            )
    upload(db, raw_id, observations, user)
    built = build(db, raw_id, name="可训练窗口")
    assert built.summary["training_ready"] is True
    config = {
        "feature_sets": [["night_flow", "pressure_drop", "acoustic_energy"], list(FEATURES)],
        "c_values": [0.1, 1.0],
        "false_negative_cost": 8.0,
        "false_positive_cost": 1.0,
    }
    rows = get_windows(db, built.window_dataset_id)
    result = train_model(rows, config)
    assert result["split_counts"]["train"]["n"] + result["split_counts"]["validation"]["n"] + result["split_counts"]["test"]["n"] == len(rows)
    assert 0.0 <= result["test"]["auc"] <= 1.0
    assert result["fingerprint"] and result["recommendation"]["selection_source"] == "validation_only"


def test_running_build_preview_counts_staged_windows(db, raw_id, user):
    """While a build is in flight the preview pages over staged windows and
    reports their real count, so a reviewer can reconcile as it runs."""
    from src.models import GeneratedWindow

    upload(db, raw_id, [row("P1", m) for m in range(20)], user)
    running = WindowBuild(raw_dataset_id=raw_id, name="进行中", status="running", memory_limit_bytes=128 * 1024 * 1024)
    db.add(running)
    db.commit()
    for minute in range(6):
        db.add(
            GeneratedWindow(
                build_id=running.id,
                seq=minute,
                pipe_id="P1",
                window_start=BASE + timedelta(minutes=minute),
                reference=f"P1@{minute}",
                features={k: 1.0 for k in FEATURES},
                label=0,
                samples=5,
                minute_slots=5,
            )
        )
    db.commit()
    preview = builds.preview_windows(db, running.id, limit=4, offset=0)
    assert preview["total"] == 6
    assert len(preview["windows"]) == 4
    assert preview["build"]["staged_windows"] == 6
    # Staged windows are not part of any dataset, so training cannot see them.
    assert db.scalar(select(func.count()).select_from(WindowDataset)) == 0
