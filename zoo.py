"""The oxzoo-live contract (DESIGN.md): health, probe runner, zoo-sig v1,
CORS, and fingerprints. Framework-free and stdlib-only, so the same file is
copied into every Python project of the zoo."""

import hashlib
import hmac
import json
import os
import platform
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

TRACE_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
SERVER_RE = re.compile(r"^s[0-9]+$")
SIG_RE = re.compile(r"^t=([0-9]{1,12}),caller=([a-z][a-z0-9-]{0,40}),sig=([0-9a-f]{64})$")
MAX_BODY = 64 * 1024
MAX_SKEW_S = 300
LOCAL_TIMEOUT_S = 5.0
PEER_TIMEOUT_S = 8.0
PROBE_TIMEOUT_S = 20.0
BUSY_WAIT_S = 5.0
BUILD_TAG = "zoo-1"
STARTED_AT = time.time()


def fp(value: str) -> str:
    """Last 4 hex characters of sha256(value): all a secret ever shows."""
    return hashlib.sha256(value.encode()).hexdigest()[-4:]


def iso(ts: float | None = None) -> str:
    when = datetime.fromtimestamp(time.time() if ts is None else ts, timezone.utc)
    return when.strftime("%Y-%m-%dT%H:%M:%SZ")


def server_label(host: str | None = None) -> str:
    host = os.environ.get("PUBLIC_HOST", "") if host is None else host
    return next((part for part in host.split(".") if SERVER_RE.fullmatch(part)), "local")


def identity(name: str, stack: str) -> dict:
    return {
        "name": name,
        "stack": stack,
        "server": server_label(),
        "release": os.environ.get("OX_RELEASE", "")[:12] or "unknown",
        "env": os.environ.get("OX_ENV") or "local",
    }


def write_build_info(path: str) -> None:
    """Run at build time; health reports built_at from it."""
    with open(path, "w") as f:
        json.dump({"built_at": iso()}, f)


def health(name: str, stack: str, build_file: str | None = None) -> dict:
    build = {"tag": BUILD_TAG, "runtime": f"python {platform.python_version()}"}
    if build_file and os.path.exists(build_file):
        with open(build_file) as f:
            build["built_at"] = json.load(f).get("built_at")
    return identity(name, stack) | {
        "uptime_s": int(time.time() - STARTED_AT),
        "started_at": iso(STARTED_AT),
        "build": build,
    }


# ---- zoo-sig v1 --------------------------------------------------------


class SigError(Exception):
    """reason is one of: missing signature, bad format, expired, unknown
    caller, bad signature."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def signature(key: str, t: int, method: str, path: str, body: bytes = b"") -> str:
    msg = f"{t}.{method.upper()}.{path}.{hashlib.sha256(body).hexdigest()}"
    return hmac.new(key.encode(), msg.encode(), hashlib.sha256).hexdigest()


def sign_header(key: str, caller: str, method: str, path: str, body: bytes = b"", t: int | None = None) -> str:
    t = int(time.time()) if t is None else t
    return f"t={t},caller={caller},sig={signature(key, t, method, path, body)}"


def verify(header: str | None, method: str, path: str, body: bytes, key: str, callers: set[str], now: float | None = None) -> str:
    """Checks an X-Zoo-Signature header and returns the caller. path is the
    request line's path with its query; body is at most MAX_BODY bytes."""
    if not header:
        raise SigError("missing signature")
    m = SIG_RE.fullmatch(header.strip())
    if not m:
        raise SigError("bad format")
    t, caller, sig = int(m.group(1)), m.group(2), m.group(3)
    if abs((time.time() if now is None else now) - t) > MAX_SKEW_S:
        raise SigError("expired")
    if caller not in callers:
        raise SigError("unknown caller")
    if not key or not hmac.compare_digest(signature(key, t, method, path, body), sig):
        raise SigError("bad signature")
    return caller


def request_target(url: str) -> str:
    """The path with query that goes on the request line, as signed."""
    parts = urllib.parse.urlsplit(url)
    return (parts.path or "/") + (f"?{parts.query}" if parts.query else "")


def signed_request(method: str, url: str, key: str, caller: str, body: bytes = b"", trace: str | None = None,
                   timeout: float = PEER_TIMEOUT_S, limit: int = 1 << 20) -> tuple[int, bytes]:
    """A signed HTTP call. Returns (status, body) for any HTTP answer;
    raises on network errors and timeouts."""
    headers = {
        "X-Zoo-Signature": sign_header(key, caller, method, request_target(url), body),
        "User-Agent": f"oxzoo-{caller}",
        "Accept": "application/json",
    }
    if body:
        headers["Content-Type"] = "application/json"
    if trace:
        headers["X-Zoo-Trace"] = trace
    req = urllib.request.Request(url, data=body or None, method=method.upper(), headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read(limit)
    except urllib.error.HTTPError as e:
        return e.code, e.read(limit)


# ---- CORS --------------------------------------------------------------


def allowed_origins(env: str = "ZOO_PANEL_ORIGIN") -> list[str]:
    return [o.strip() for o in os.environ.get(env, "").split(",") if o.strip()]


def cors_headers(origin: str | None, env: str = "ZOO_PANEL_ORIGIN") -> dict[str, str]:
    if origin and origin in allowed_origins(env):
        return {"Access-Control-Allow-Origin": origin, "Vary": "Origin"}
    return {}


def preflight_headers(origin: str | None, env: str = "ZOO_PANEL_ORIGIN") -> dict[str, str]:
    """Headers for a 204 answer to OPTIONS; empty when origin is not listed."""
    h = cors_headers(origin, env)
    if h:
        h |= {
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Max-Age": "600",
        }
    return h


# ---- vars --------------------------------------------------------------


def var(name: str, role: str, peer: str | None = None) -> dict:
    """A probe vars entry: value only for url and plain, else its fp. The
    key a reference points at is listed too, with role service."""
    value = os.environ.get(name, "")
    out: dict = {"name": name, "role": role}
    if peer:
        out["peer"] = peer
    if not value:
        out["missing"] = True
    elif role in ("url", "plain"):
        out["value"] = value
    else:
        out["fp"] = fp(value)
    return out


# ---- probe -------------------------------------------------------------


@dataclass
class Check:
    id: str
    label: str
    fn: Callable[[], str | None]  # raises on failure, returns a detail
    env: list[str] = field(default_factory=list)
    hops: list[str] | None = None
    timeout: float = LOCAL_TIMEOUT_S


def scrub(text: str, names: list[str]) -> str:
    for name in names:
        value = os.environ.get(name, "")
        if len(value) >= 4:
            text = text.replace(value, "[redacted]")
    return text[:300]


def peer_check(me: str, peer: str, url_var: str, key_var: str) -> Check:
    """Signed GET <peer>/_zoo/verify; passes on name, key fp and URL."""
    url = os.environ.get(url_var, "").rstrip("/")

    def run() -> str:
        key = os.environ[key_var]
        status, body = signed_request("GET", url + "/_zoo/verify", key, me)
        if status != 200:
            raise RuntimeError(f"verify answered {status}: {body[:120].decode(errors='replace')}")
        got = json.loads(body)
        if got.get("name") != peer:
            raise RuntimeError(f"expected name {peer}, got {got.get('name')!r}")
        if got.get("key_fp") != fp(key):
            raise RuntimeError(f"key fp {got.get('key_fp')!r} != ours {fp(key)}")
        if (got.get("public_url") or "").rstrip("/") != url:
            raise RuntimeError(f"peer public_url {got.get('public_url')!r} != {url_var}")
        return f"verified by {peer} as {got.get('caller')}, key fp {fp(key)}"

    peer_server = server_label(urllib.parse.urlsplit(url).hostname or "")
    return Check(f"peer:{peer}", f"Signed call to {peer}", run, [url_var, key_var],
                 [f"{me}@{server_label()}", f"{peer}@{peer_server}"], PEER_TIMEOUT_S)


class Prober:
    """One probe at a time; a second waits BUSY_WAIT_S then gets 429."""

    def __init__(self, name: str, stack: str, checks: Callable[[], list[Check]], vars: Callable[[], list[dict]]):
        self.name, self.stack, self.checks, self.vars = name, stack, checks, vars
        self.lock = threading.Lock()

    def run(self) -> tuple[int, dict]:
        if not self.lock.acquire(timeout=BUSY_WAIT_S):
            return 429, {"error": "probe busy"}
        try:
            return 200, self._run()
        finally:
            self.lock.release()

    def _run(self) -> dict:
        start = time.monotonic()
        deadline = start + PROBE_TIMEOUT_S
        checks = self.checks()
        me = f"{self.name}@{server_label()}"
        pool = ThreadPoolExecutor(max_workers=max(1, len(checks)), thread_name_prefix="probe")
        futures = []
        for c in checks:
            missing = [n for n in c.env if not os.environ.get(n)]
            futures.append(None if missing else pool.submit(timed, c.fn))
        results = []
        for c, fut in zip(checks, futures):
            r = {"id": c.id, "label": c.label, "ok": False, "ms": 0, "env": c.env, "hops": c.hops or [me]}
            if fut is None:
                r["error"] = f"{next(n for n in c.env if not os.environ.get(n))} is not set"
                results.append(r)
                continue
            wait = max(0.0, min(c.timeout - (time.monotonic() - start), deadline - time.monotonic()))
            try:
                ms, detail = fut.result(timeout=wait)
                r |= {"ok": True, "ms": ms}
                if detail:
                    r["detail"] = scrub(detail, c.env)
            except FutureTimeout:
                r |= {"ms": int(c.timeout * 1000), "error": f"timeout after {int(c.timeout * 1000)} ms"}
            except Exception as e:  # a failed check is reported, never raised
                r |= {"ms": getattr(e, "ms", 0), "error": scrub(f"{type(e).__name__}: {e}", c.env)}
            results.append(r)
        pool.shutdown(wait=False, cancel_futures=True)
        return identity(self.name, self.stack) | {
            "ok": all(r["ok"] for r in results),
            "ms": int((time.monotonic() - start) * 1000),
            "at": iso(),
            "checks": results,
            "vars": self.vars(),
        }


def timed(fn: Callable[[], str | None]) -> tuple[int, str | None]:
    t0 = time.monotonic()
    try:
        detail = fn()
    except Exception as e:
        try:
            e.ms = int((time.monotonic() - t0) * 1000)
        except AttributeError:
            pass
        raise
    return int((time.monotonic() - t0) * 1000), detail


# ---- chains ------------------------------------------------------------


class RateLimit:
    """At most `limit` events per `window` seconds in this process."""

    def __init__(self, limit: int = 10, window: float = 60.0):
        self.limit, self.window = limit, window
        self.times: deque[float] = deque()
        self.lock = threading.Lock()

    def allow(self, now: float | None = None) -> bool:
        now = time.monotonic() if now is None else now
        with self.lock:
            while self.times and now - self.times[0] >= self.window:
                self.times.popleft()
            if len(self.times) >= self.limit:
                return False
            self.times.append(now)
            return True


def valid_trace(value: object) -> bool:
    return isinstance(value, str) and bool(TRACE_RE.fullmatch(value))


def hop(at: float | datetime, step: str, detail: str = "") -> dict:
    ts = at.timestamp() if isinstance(at, datetime) else at
    return {"at": iso(ts), "step": step, "detail": detail}


def trace_body(trace: str, hops: list[dict]) -> dict:
    return {"trace": trace, "found": bool(hops), "hops": hops}
