"""API-level checks: permissions, upload/validation errors, build status.

Celery is not running in tests, so the build task is stubbed at the point the
route imports it; that also lets us assert the route's response shape and the
fact that a failing enqueue is surfaced rather than silently dropped.
"""
import os

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from src.config.database import Base, SessionLocal, engine
from src.models.user import User
from src.utils.security import hash_password


@pytest.fixture()
def client():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with SessionLocal() as db:
        db.add_all(
            [
                User(email="analyst@test", name="分析师", role="analyst", password_hash=hash_password("pw")),
                User(email="viewer@test", name="观察员", role="viewer", password_hash=hash_password("pw")),
            ]
        )
        db.commit()
    from src.main import app

    return TestClient(app)


def login(client, email):
    response = client.post("/auth/login", json={"email": email, "password": "pw"})
    assert response.status_code == 200, response.text
    return {"Authorization": "Bearer " + response.json()["token"]}


@pytest.fixture()
def api(client):
    return login(client, "analyst@test")


def make_dataset(client, api, name="采样集"):
    response = client.post("/raw-datasets", json={"name": name}, headers=api)
    assert response.status_code == 201, response.text
    return response.json()["id"]


def batch(pipe="P1", minutes=(0, 1, 2, 3, 4), **kw):
    from datetime import datetime, timedelta, timezone

    base = datetime(2026, 7, 11, 22, 0, tzinfo=timezone.utc)
    values = dict(flow=10.0, pressure=500.0, acoustic_energy=40.0, temperature=18.0, pipe_age=20.0, leak=0)
    values.update(kw)
    return [{**values, "pipe_id": pipe, "sampled_at": (base + timedelta(minutes=m)).isoformat()} for m in minutes]


def test_upload_roundtrip_and_preview(client, api):
    rid = make_dataset(client, api)
    response = client.post(f"/raw-datasets/{rid}/batches", json={"samples": batch()}, headers=api)
    assert response.status_code == 201, response.text
    assert response.json()["inserted"] == 5
    listed = client.get("/raw-datasets", headers=api).json()
    assert listed[0]["samples"] == 5 and listed[0]["pipes"] == 1
    preview = client.get(f"/raw-datasets/{rid}/samples?limit=3", headers=api).json()
    assert len(preview) == 3 and preview[0]["pipe_id"] == "P1"


def test_missing_timezone_is_rejected(client, api):
    rid = make_dataset(client, api)
    rows = batch()
    rows[0]["sampled_at"] = "2026-07-11T22:00:00"
    response = client.post(f"/raw-datasets/{rid}/batches", json={"samples": rows}, headers=api)
    assert response.status_code == 422
    assert "时区" in response.text


def test_conflicting_resend_rejected(client, api):
    rid = make_dataset(client, api)
    client.post(f"/raw-datasets/{rid}/batches", json={"samples": batch()}, headers=api)
    response = client.post(f"/raw-datasets/{rid}/batches", json={"samples": batch(flow=99.0)}, headers=api)
    assert response.status_code == 422
    assert client.get("/raw-datasets", headers=api).json()[0]["samples"] == 5


def test_viewer_cannot_write_but_can_read(client, api):
    rid = make_dataset(client, api)
    client.post(f"/raw-datasets/{rid}/batches", json={"samples": batch()}, headers=api)
    viewer = login(client, "viewer@test")
    assert client.get("/raw-datasets", headers=viewer).status_code == 200
    assert client.get(f"/raw-datasets/{rid}/samples", headers=viewer).status_code == 200
    assert client.post("/raw-datasets", json={"name": "x"}, headers=viewer).status_code == 403
    assert client.post(f"/raw-datasets/{rid}/batches", json={"samples": batch()}, headers=viewer).status_code == 403
    assert client.post(f"/raw-datasets/{rid}/builds", json={"name": "x"}, headers=viewer).status_code == 403


def test_build_rejects_memory_budget_above_128mb(client, api):
    rid = make_dataset(client, api)
    client.post(f"/raw-datasets/{rid}/batches", json={"samples": batch()}, headers=api)
    response = client.post(f"/raw-datasets/{rid}/builds", json={"name": "w", "memory_limit_bytes": 128 * 1024 * 1024 + 1}, headers=api)
    assert response.status_code == 422
    too_small = client.post(f"/raw-datasets/{rid}/builds", json={"name": "w", "memory_limit_bytes": 1024}, headers=api)
    assert too_small.status_code == 422


def test_duplicate_generated_dataset_name_is_rejected_before_queueing(client, api, monkeypatch):
    rid = make_dataset(client, api)
    client.post(f"/raw-datasets/{rid}/batches", json={"samples": batch()}, headers=api)
    from src import worker

    monkeypatch.setattr(worker.build_windows, "delay", lambda *a, **k: None)
    assert client.post(f"/raw-datasets/{rid}/builds", json={"name": "同名窗口"}, headers=api).status_code == 202
    worker.build_windows.run(client.get("/builds", headers=api).json()[0]["id"])
    # A second request with the same name is refused immediately, not queued
    # to fail after a full generation pass.
    again = client.post(f"/raw-datasets/{rid}/builds", json={"name": "同名窗口"}, headers=api)
    assert again.status_code == 409


def test_build_without_samples_is_rejected(client, api):
    rid = make_dataset(client, api)
    response = client.post(f"/raw-datasets/{rid}/builds", json={"name": "w"}, headers=api)
    assert response.status_code == 422


def test_build_queue_failure_is_reported(client, api, monkeypatch):
    rid = make_dataset(client, api)
    client.post(f"/raw-datasets/{rid}/batches", json={"samples": batch()}, headers=api)

    def unavailable(*args, **kwargs):
        raise ConnectionError("redis down")

    from src import worker

    monkeypatch.setattr(worker.build_windows, "delay", unavailable)
    response = client.post(f"/raw-datasets/{rid}/builds", json={"name": "w"}, headers=api)
    assert response.status_code == 503
    builds = client.get("/builds", headers=api).json()
    assert builds[0]["status"] == "failed"


def test_build_task_end_to_end_via_worker_function(client, api, monkeypatch):
    """Run the Celery task synchronously (eager) to cover the worker path."""
    rid = make_dataset(client, api)
    rows = batch(minutes=range(0, 40), leak=0)
    rows += batch(pipe="P2", minutes=range(0, 40))
    for i in range(0, len(rows), 13):
        assert client.post(f"/raw-datasets/{rid}/batches", json={"samples": rows[i:i + 13]}, headers=api).status_code == 201
    from src import worker

    monkeypatch.setattr(worker.build_windows, "delay", lambda *a, **k: None)
    queued = client.post(f"/raw-datasets/{rid}/builds", json={"name": "五分钟窗口"}, headers=api)
    assert queued.status_code == 202
    build_id = queued.json()["id"]
    worker.build_windows.run(build_id)
    detail = client.get(f"/builds/{build_id}", headers=api).json()
    assert detail["status"] == "succeeded", detail
    assert detail["window_count"] == 40 * 2
    assert detail["summary"]["samples"] == 80
    windows = client.get(f"/builds/{build_id}/windows?limit=5", headers=api).json()
    assert windows["total"] == 80 and len(windows["windows"]) == 5
    datasets = client.get("/datasets", headers=api).json()
    assert [d["name"] for d in datasets] == ["五分钟窗口"]
    assert datasets[0]["count"] == 80


def test_failed_build_task_records_failure_and_keeps_no_windows(client, api, monkeypatch):
    rid = make_dataset(client, api)
    rows = batch(minutes=range(0, 10), flow=0.0)
    client.post(f"/raw-datasets/{rid}/batches", json={"samples": rows}, headers=api)
    from src import worker

    monkeypatch.setattr(worker.build_windows, "delay", lambda *a, **k: None)
    build_id = client.post(f"/raw-datasets/{rid}/builds", json={"name": "零流量"}, headers=api).json()["id"]
    worker.build_windows.run(build_id)
    detail = client.get(f"/builds/{build_id}", headers=api).json()
    assert detail["status"] == "failed"
    assert "流量均值为零" in detail["error"]
    assert detail["window_count"] == 0 and detail["staged_windows"] == 0
    assert client.get("/datasets", headers=api).json() == []
