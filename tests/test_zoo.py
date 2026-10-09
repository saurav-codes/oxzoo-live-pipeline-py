"""The zoo contract's shared tests: DESIGN.md signing vectors, verify
reasons, CORS, trace ids, vars, and the probe runner's limits."""

import threading
import time

import pytest

import zoo

KEY = "zoo-test-key-0123456789abcdef"
T = 1760000000
BODY = b'{"sku":"ZOO-1"}'


def test_signing_vectors():
    assert zoo.hashlib.sha256(BODY).hexdigest() == "cc2860a77ea231854ea58f9cb05f3217059f80a8e95d7b69a204293ae4f3a444"
    assert zoo.signature(KEY, T, "POST", "/api/items?x=1", BODY) == "50c22839fe6a06cb51a9fd25167d9e457eb0b5ee63ce696f4c5428a6b9271da1"
    assert zoo.signature(KEY, T, "get", "/_zoo/verify") == "9a404bebaa32497c5ed39ef8990e8466428f8023d6aa9f5acc94f16fb7670ecb"
    assert zoo.fp(KEY) == "915a"


def test_verify_accepts_and_names_caller():
    header = zoo.sign_header(KEY, "mesh-shop", "POST", "/api/items?x=1", BODY, t=T)
    assert zoo.verify(header, "POST", "/api/items?x=1", BODY, KEY, {"mesh-shop"}, now=T + 299) == "mesh-shop"


@pytest.mark.parametrize("header,now,callers,reason", [
    (None, T, {"mesh-shop"}, "missing signature"),
    ("", T, {"mesh-shop"}, "missing signature"),
    ("t=abc,caller=mesh-shop,sig=00", T, {"mesh-shop"}, "bad format"),
    ("sig=" + "0" * 64, T, {"mesh-shop"}, "bad format"),
    (f"t={T},caller=mesh-shop,sig=" + "0" * 64, T + 301, {"mesh-shop"}, "expired"),
    (f"t={T},caller=mesh-shop,sig=" + "0" * 64, T - 301, {"mesh-shop"}, "expired"),
    (f"t={T},caller=evil,sig=" + "0" * 64, T, {"mesh-shop"}, "unknown caller"),
    (f"t={T},caller=mesh-shop,sig=" + "0" * 64, T, {"mesh-shop"}, "bad signature"),
])
def test_verify_reasons(header, now, callers, reason):
    with pytest.raises(zoo.SigError) as e:
        zoo.verify(header, "GET", "/_zoo/verify", b"", KEY, callers, now=now)
    assert e.value.reason == reason


def test_verify_binds_method_path_body_and_key():
    header = zoo.sign_header(KEY, "mesh-shop", "POST", "/api/items?x=1", BODY, t=T)
    for method, path, body, key in [("GET", "/api/items?x=1", BODY, KEY), ("POST", "/api/items?x=2", BODY, KEY),
                                    ("POST", "/api/items?x=1", b"{}", KEY), ("POST", "/api/items?x=1", BODY, "other"),
                                    ("POST", "/api/items?x=1", BODY, "")]:
        with pytest.raises(zoo.SigError, match="bad signature"):
            zoo.verify(header, method, path, body, key, {"mesh-shop"}, now=T)


def test_request_target_keeps_query():
    assert zoo.request_target("https://x.s2.zoo.sorv.dev/api/items?x=1") == "/api/items?x=1"
    assert zoo.request_target("https://x.s2.zoo.sorv.dev") == "/"


def test_cors(monkeypatch):
    monkeypatch.setenv("ZOO_PANEL_ORIGIN", "https://zoo-control.s1.zoo.sorv.dev, http://localhost:5173")
    assert zoo.cors_headers("http://localhost:5173") == {"Access-Control-Allow-Origin": "http://localhost:5173", "Vary": "Origin"}
    assert zoo.cors_headers("https://evil.example") == {}
    assert zoo.cors_headers(None) == {}
    pre = zoo.preflight_headers("https://zoo-control.s1.zoo.sorv.dev")
    assert pre["Access-Control-Allow-Methods"] == "GET, POST, OPTIONS"
    assert pre["Access-Control-Allow-Headers"] == "Content-Type"
    assert pre["Access-Control-Max-Age"] == "600"
    assert "*" not in pre.values() and "Access-Control-Allow-Credentials" not in pre
    assert zoo.preflight_headers("https://evil.example") == {}
    monkeypatch.delenv("ZOO_PANEL_ORIGIN")
    assert zoo.cors_headers("http://localhost:5173") == {}


@pytest.mark.parametrize("value,ok", [
    ("3f9c2a10-1b2c-4d5e-8f90-0123456789ab", True),
    ("3F9C2A10-1B2C-4D5E-8F90-0123456789AB", False),
    ("3f9c2a10-1b2c-4d5e-8f90-0123456789ab\n", False),
    ("3f9c2a101b2c4d5e8f900123456789ab", False),
    ("../../etc/passwd", False),
    (None, False),
    (42, False),
])
def test_trace_ids(value, ok):
    assert zoo.valid_trace(value) is ok


def test_server_label_and_identity(monkeypatch):
    assert zoo.server_label("catalog-api.s2.zoo.sorv.dev") == "s2"
    assert zoo.server_label("example.com") == "local"
    monkeypatch.setenv("OX_RELEASE", "3f9c2a1b4d5e6f708192")
    monkeypatch.setenv("OX_ENV", "production")
    monkeypatch.setenv("PUBLIC_HOST", "celery-hub.s1.zoo.sorv.dev")
    ident = zoo.identity("celery-hub", "Django")
    assert ident == {"name": "celery-hub", "stack": "Django", "server": "s1", "release": "3f9c2a1b4d5e", "env": "production"}
    monkeypatch.delenv("OX_RELEASE")
    monkeypatch.delenv("OX_ENV")
    assert zoo.identity("x", "y")["release"] == "unknown"
    assert zoo.identity("x", "y")["env"] == "local"


def test_health_build_info(tmp_path):
    path = tmp_path / "build.json"
    zoo.write_build_info(str(path))
    h = zoo.health("x", "y", str(path))
    assert h["build"]["tag"] == "zoo-1" and h["build"]["built_at"].endswith("Z")
    assert h["build"]["runtime"].startswith("python 3.")
    assert "built_at" not in zoo.health("x", "y", str(tmp_path / "none.json"))["build"]


def test_vars(monkeypatch):
    monkeypatch.setenv("EVENTS_KEY", KEY)
    monkeypatch.setenv("EVENTS_URL", "https://event-bus.s3.zoo.sorv.dev")
    monkeypatch.setenv("REDIS_URL", "redis://:pw@127.0.0.1:6379/0")
    monkeypatch.setenv("CELERY_BROKER_URL", "redis://:pw@127.0.0.1:6379/0")
    monkeypatch.delenv("NOPE", raising=False)
    assert zoo.var("EVENTS_KEY", "signs") == {"name": "EVENTS_KEY", "role": "signs", "fp": "915a"}
    assert zoo.var("EVENTS_URL", "url", "event-bus")["value"] == "https://event-bus.s3.zoo.sorv.dev"
    ref = zoo.var("CELERY_BROKER_URL", "reference", "REDIS_URL")
    svc = zoo.var("REDIS_URL", "service")
    assert ref == {"name": "CELERY_BROKER_URL", "role": "reference", "peer": "REDIS_URL", "fp": svc["fp"]}
    assert svc == {"name": "REDIS_URL", "role": "service", "fp": zoo.fp("redis://:pw@127.0.0.1:6379/0")}
    assert zoo.var("NOPE", "verifies") == {"name": "NOPE", "role": "verifies", "missing": True}


def test_probe_reports_missing_env_failure_and_timeout(monkeypatch):
    monkeypatch.setattr(zoo, "PROBE_TIMEOUT_S", 2.0)
    monkeypatch.setenv("SECRET_THING", "hunter2-secret")
    monkeypatch.delenv("ABSENT_VAR", raising=False)

    def boom():
        raise RuntimeError("connect to hunter2-secret failed")

    checks = [
        zoo.Check("good", "Good", lambda: "fine"),
        zoo.Check("missing", "Missing", lambda: "never", env=["ABSENT_VAR"]),
        zoo.Check("bad", "Bad", boom, env=["SECRET_THING"]),
        zoo.Check("slow", "Slow", lambda: time.sleep(3), timeout=0.2),
    ]
    status, body = zoo.Prober("x", "y", lambda: checks, lambda: []).run()
    assert status == 200 and body["ok"] is False
    by = {c["id"]: c for c in body["checks"]}
    assert by["good"]["ok"] and by["good"]["detail"] == "fine"
    assert by["missing"]["error"] == "ABSENT_VAR is not set"
    assert "hunter2-secret" not in by["bad"]["error"] and "[redacted]" in by["bad"]["error"]
    assert by["slow"]["error"] == "timeout after 200 ms"
    assert body["ms"] < 1500


def test_probe_busy(monkeypatch):
    monkeypatch.setattr(zoo, "BUSY_WAIT_S", 0.1)
    gate = threading.Event()
    prober = zoo.Prober("x", "y", lambda: [zoo.Check("wait", "Wait", lambda: gate.wait(2) and None)], lambda: [])
    first = threading.Thread(target=prober.run)
    first.start()
    time.sleep(0.05)
    assert prober.run() == (429, {"error": "probe busy"})
    gate.set()
    first.join()
    assert prober.run()[0] == 200


def test_rate_limit():
    rl = zoo.RateLimit(10, 60)
    assert all(rl.allow(now=0) for _ in range(10))
    assert not rl.allow(now=1)
    assert rl.allow(now=61)
