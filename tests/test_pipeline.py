import json
import math
import os
import uuid

import pytest
from starlette.testclient import TestClient

import app
import puller
import stores

PANEL = "https://zoo-control.s1.zoo.sorv.dev"
STORE_VARS = ["CLICKHOUSE_URL", "CLICKHOUSE_PASSWORD", "MEILI_URL", "MEILI_MASTER_KEY", "QDRANT_URL"]


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("ZOO_PANEL_ORIGIN", PANEL)
    return TestClient(app.app)


def test_vector_is_deterministic_unit_64():
    a, b = stores.vector("Otters and pandas"), stores.vector("otters and pandas")
    assert a == b and len(a) == stores.DIM
    assert math.isclose(sum(x * x for x in a), 1.0, rel_tol=1e-9)
    assert stores.vector("red pandas") != stores.vector("sea otters")


def test_vector_never_zero():
    for text in ("", "!!!", "a"):
        v = stores.vector(text)
        assert math.isclose(math.sqrt(sum(x * x for x in v)), 1.0)


def event(**kw):
    return {"id": 1, "trace": None, "kind": "zoo.seed", "text": "hello", "caller": "celery-hub",
            "created_at": "2026-10-09T03:00:00Z"} | kw


def test_parse_events_keeps_good_drops_bad():
    good_trace = str(uuid.uuid4())
    body = json.dumps({"events": [
        event(), event(id=2, trace=good_trace, text="x" * 5000),
        event(id=0), event(id="3"), event(id=4, kind="Bad Kind"), event(id=5, trace="nope"),
        event(id=6, created_at="yesterday"), {"id": 7}, "junk",
    ], "next": 7}).encode()
    rows = puller.parse_events(body)
    assert [r["id"] for r in rows] == [1, 2]
    assert rows[0]["trace"] == "" and rows[1]["trace"] == good_trace
    assert len(rows[1]["text"]) == puller.MAX_TEXT
    assert rows[0]["created_at"] == "2026-10-09T03:00:00+00:00"


def test_parse_events_rejects_wrong_shape():
    for body in (b"[]", b'{"events": 3}', b"not json"):
        with pytest.raises(ValueError):
            puller.parse_events(body)


def test_health_cors(client):
    r = client.get("/_zoo/health", headers={"Origin": PANEL})
    assert r.status_code == 200 and r.json()["name"] == "pipeline-py"
    assert r.headers["access-control-allow-origin"] == PANEL
    r = client.get("/_zoo/health", headers={"Origin": "https://evil.example"})
    assert "access-control-allow-origin" not in r.headers


def test_preflight(client):
    r = client.options("/_zoo/probe", headers={"Origin": PANEL, "Access-Control-Request-Method": "GET"})
    assert r.status_code == 204 and r.headers["access-control-allow-origin"] == PANEL


def test_no_cors_off_zoo_paths(client, monkeypatch):
    for name in STORE_VARS:
        monkeypatch.delenv(name, raising=False)
    r = client.get("/api/search?q=" + "x" * 201, headers={"Origin": PANEL})
    assert r.status_code == 400 and "access-control-allow-origin" not in r.headers


def test_bad_trace_id_is_400(client):
    for tid in ("abc", "9BDA61C3-7FC3-4AE3-AE58-EB661BFB7063", "x" * 36):
        assert client.get(f"/_zoo/trace/{tid}").status_code == 400


def test_search_needs_q(client):
    assert client.get("/api/search").status_code == 400


def test_store_down_is_503(client, monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_URL", "http://127.0.0.1:9")
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "secret-value-123")
    r = client.get("/api/stats")
    assert r.status_code == 503 and "secret-value-123" not in r.text


def test_probe_reports_missing_vars(client, monkeypatch):
    for name in STORE_VARS + ["EVENTS_URL", "EVENTS_KEY"]:
        monkeypatch.delenv(name, raising=False)
    body = client.get("/_zoo/probe", headers={"Origin": PANEL}).json()
    assert body["ok"] is False
    assert {c["id"] for c in body["checks"]} == {"clickhouse", "meili", "qdrant", "puller", "peer:event-bus"}
    assert all(c["error"].endswith("is not set") for c in body["checks"])
    roles = {v["name"]: v["role"] for v in body["vars"]}
    assert roles["EVENTS_KEY"] == "signs" and roles["CLICKHOUSE_PASSWORD"] == "service"


# ---- integration: real ClickHouse, Meilisearch and Qdrant -----------------

LIVE = all(os.environ.get(f"TEST_{n}") for n in STORE_VARS)


@pytest.mark.skipif(not LIVE, reason="set TEST_CLICKHOUSE_URL and the other TEST_* store variables")
def test_pull_store_trace_search(client, monkeypatch):
    for n in STORE_VARS:
        monkeypatch.setenv(n, os.environ[f"TEST_{n}"])
    stores.migrate()
    stores.migrate()  # idempotent
    ch = stores.ClickHouse()
    after = puller.cursor(ch)
    trace = str(uuid.uuid4())
    word = f"okapi{uuid.uuid4().hex[:8]}"
    feed = {"events": [event(id=after + 1, trace=trace, text=f"an {word} walked by"), event(id=after + 2)]}
    calls = []

    def fake_bus(method, url, key, caller, *a, **kw):
        calls.append(url)
        return 200, json.dumps(feed).encode()

    monkeypatch.setattr(puller.zoo, "signed_request", fake_bus)
    monkeypatch.setenv("EVENTS_URL", "http://bus.invalid")
    monkeypatch.setenv("EVENTS_KEY", "k" * 32)
    assert puller.pull_once(ch, stores.Qdrant(), stores.Meili()) == 2
    assert calls[0].endswith(f"/api/events?after={after}&limit=100")
    assert puller.cursor(ch) == after + 2

    hops = client.get(f"/_zoo/trace/{trace}").json()
    assert hops["found"] and [h["step"] for h in hops["hops"]] == ["clickhouse", "qdrant", "meili"]
    found = client.get(f"/api/search?q={word}").json()
    assert [h["id"] for h in found["hits"]] == [after + 1]
    assert found["similar"][0]["id"] == after + 1
    assert client.get("/api/stats").json()["cursor"] == after + 2
