"""pipeline-py: dashboard, search and the zoo endpoints over the stores the
puller fills."""

import logging
import random
import secrets

from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route
from starlette.templating import Jinja2Templates

import stores
import zoo

NAME = "pipeline-py"
STACK = "Starlette + uvicorn + ClickHouse + Qdrant + Meilisearch"
MAX_LAG_S = 30
MAX_QUERY = 200

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
templates = Jinja2Templates(directory="templates")


class ZooCors:
    """CORS for the panel on /_zoo/health, probe and trace."""

    PREFIXES = ("/_zoo/health", "/_zoo/probe", "/_zoo/trace/")

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or not scope["path"].startswith(self.PREFIXES):
            return await self.app(scope, receive, send)
        origin = dict(scope["headers"]).get(b"origin", b"").decode("latin-1") or None
        if scope["method"] == "OPTIONS":
            return await Response(status_code=204, headers=zoo.preflight_headers(origin))(scope, receive, send)

        async def send_cors(message):
            if message["type"] == "http.response.start":
                message["headers"] = list(message.get("headers", [])) + [
                    (k.lower().encode(), v.encode()) for k, v in zoo.cors_headers(origin).items()]
            await send(message)

        await self.app(scope, receive, send_cors)


def stats() -> dict:
    ch = stores.ClickHouse()
    events = ch.rows("SELECT count() AS n, max(id) AS last_id FROM zoo.events FINAL")[0]
    pull = ch.rows("SELECT dateDiff('second', max(at), now64(3)) AS age_s, max(max_id) AS cursor FROM zoo.pulls")[0]
    hourly = ch.rows("SELECT hour, kind, events FROM zoo.hourly FINAL ORDER BY hour DESC, kind LIMIT 24")
    recent = ch.rows("SELECT id, kind, text, created_at FROM zoo.events FINAL ORDER BY id DESC LIMIT 10")
    return {"events": int(events["n"]), "cursor": int(pull["cursor"]), "last_pull_age_s": int(pull["age_s"]),
            "hourly": hourly, "recent": recent}


def search(q: str) -> dict:
    hits = stores.Meili().call("POST", f"/indexes/{stores.EVENTS}/search", {"q": q, "limit": 10})["hits"]
    similar = stores.Qdrant().nearest(stores.EVENTS, stores.vector(q), 5)
    return {"query": q, "hits": [{"id": h["id"], "kind": h["kind"], "text": h["text"]} for h in hits],
            "similar": [{"id": p["id"], "score": round(p["score"], 3), "text": p["payload"].get("text", "")} for p in similar]}


def query_param(request: Request) -> str | JSONResponse:
    q = request.query_params.get("q", "").strip()
    if len(q) > MAX_QUERY:
        return JSONResponse({"error": f"q is at most {MAX_QUERY} characters"}, status_code=400)
    return q


async def index(request: Request):
    q = query_param(request)
    if isinstance(q, JSONResponse):
        return q
    ctx: dict = {"q": q, "name": NAME}
    for key, fn in (("stats", stats), ("results", (lambda: search(q)) if q else None)):
        if fn is None:
            continue
        try:
            ctx[key] = await run_in_threadpool(fn)
        except Exception as e:  # shown on the page
            ctx[key + "_error"] = zoo.scrub(f"{type(e).__name__}: {e}", ["CLICKHOUSE_PASSWORD", "MEILI_MASTER_KEY"])
    return templates.TemplateResponse(request, "index.html", ctx)


def store_error(e: Exception) -> JSONResponse:
    msg = zoo.scrub(f"{type(e).__name__}: {e}", ["CLICKHOUSE_PASSWORD", "MEILI_MASTER_KEY"])
    return JSONResponse({"error": msg}, status_code=503)


async def api_stats(request: Request):
    try:
        return JSONResponse(await run_in_threadpool(stats))
    except Exception as e:  # a store is down; say which
        return store_error(e)


async def api_search(request: Request):
    q = query_param(request)
    if isinstance(q, JSONResponse):
        return q
    if not q:
        return JSONResponse({"error": "q is required"}, status_code=400)
    try:
        return JSONResponse(await run_in_threadpool(search, q))
    except Exception as e:
        return store_error(e)


async def health(request: Request):
    return JSONResponse(zoo.health(NAME, STACK))


async def trace(request: Request):
    tid = request.path_params["trace"]
    if not zoo.valid_trace(tid):
        return JSONResponse({"error": "bad trace id"}, status_code=400)
    rows = await run_in_threadpool(stores.ClickHouse().rows,
                                   "SELECT toUnixTimestamp64Milli(at) AS ms, step, detail FROM zoo.traces FINAL "
                                   "WHERE trace = {t:String} ORDER BY at", {"t": tid})
    return JSONResponse(zoo.trace_body(tid, [zoo.hop(int(r["ms"]) / 1000, r["step"], r["detail"]) for r in rows]))


# ---- probe ---------------------------------------------------------------


def check_clickhouse() -> str:
    ch, token = stores.ClickHouse(), secrets.token_hex(8)
    ch.run("INSERT INTO zoo.probe (token) VALUES ({t:String})", {"t": token})
    got = ch.rows("SELECT token FROM zoo.probe WHERE token = {t:String}", {"t": token})
    ch.run("DELETE FROM zoo.probe WHERE token = {t:String}", {"t": token})
    left = ch.rows("SELECT count() AS n FROM zoo.probe WHERE token = {t:String}", {"t": token})
    if [r["token"] for r in got] != [token] or int(left[0]["n"]) != 0:
        raise RuntimeError(f"read back {got}, {left[0]['n']} rows left after delete")
    version = ch.rows("SELECT version() AS v")[0]["v"]
    return f"zoo.probe insert, select, delete, server {version}"


def check_meili() -> str:
    m, doc_id = stores.Meili(), secrets.token_hex(6)
    word = f"zebra{doc_id}"
    m.wait(m.call("POST", f"/indexes/{stores.PROBE}/documents", [{"id": doc_id, "text": f"probe {word}"}]), 4)
    try:
        hits = m.call("POST", f"/indexes/{stores.PROBE}/search", {"q": word})["hits"]
    finally:
        m.wait(m.call("DELETE", f"/indexes/{stores.PROBE}/documents/{doc_id}"), 4)
    if [h["id"] for h in hits] != [doc_id]:
        raise RuntimeError(f"search for the probe word found {len(hits)} hits")
    return f"zoo_probe add, search, delete, meilisearch {m.call('GET', '/version')['pkgVersion']}"


def check_qdrant() -> str:
    q, point = stores.Qdrant(), random.randint(1, 2**53)
    vec = stores.vector(f"probe {point}")
    q.upsert(stores.PROBE, [{"id": point, "vector": vec, "payload": {"probe": True}}])
    try:
        best = q.nearest(stores.PROBE, vec, 1)
    finally:
        q.call("POST", f"/collections/{stores.PROBE}/points/delete?wait=true", {"points": [point]})
    if not best or best[0]["id"] != point or best[0]["score"] < 0.999:
        raise RuntimeError(f"nearest point was {best[:1]}")
    return f"zoo_probe upsert, search (score {best[0]['score']:.3f}), delete"


def check_puller() -> str:
    pull = stores.ClickHouse().rows(
        "SELECT dateDiff('second', max(at), now64(3)) AS age_s, max(max_id) AS cursor, count() AS n FROM zoo.pulls")[0]
    if int(pull["n"]) == 0:
        raise RuntimeError("no pull recorded yet; is the puller worker running?")
    age = int(pull["age_s"])
    if age > MAX_LAG_S:
        raise RuntimeError(f"last successful pull {age}s ago (limit {MAX_LAG_S}s), cursor {pull['cursor']}")
    return f"last successful pull {age}s ago, cursor at event {pull['cursor']}"


prober = zoo.Prober(
    NAME, STACK,
    lambda: [
        zoo.Check("clickhouse", "ClickHouse insert, select, delete", check_clickhouse, ["CLICKHOUSE_URL", "CLICKHOUSE_PASSWORD"]),
        zoo.Check("meili", "Meilisearch add, search, delete", check_meili, ["MEILI_URL", "MEILI_MASTER_KEY"]),
        zoo.Check("qdrant", "Qdrant upsert, search, delete", check_qdrant, ["QDRANT_URL"]),
        zoo.Check("puller", f"Puller pulled in the last {MAX_LAG_S} s", check_puller, ["CLICKHOUSE_URL", "CLICKHOUSE_PASSWORD"]),
        zoo.peer_check(NAME, "event-bus", "EVENTS_URL", "EVENTS_KEY"),
    ],
    lambda: [
        zoo.var("EVENTS_KEY", "signs"),
        zoo.var("EVENTS_URL", "url", "event-bus"),
        zoo.var("CLICKHOUSE_URL", "service"),
        zoo.var("CLICKHOUSE_PASSWORD", "service"),
        zoo.var("MEILI_URL", "service"),
        zoo.var("MEILI_MASTER_KEY", "service"),
        zoo.var("QDRANT_URL", "service"),
        zoo.var("ZOO_PANEL_ORIGIN", "plain"),
    ],
)


async def probe(request: Request):
    status, body = await run_in_threadpool(prober.run)
    return JSONResponse(body, status_code=status)


app = Starlette(
    routes=[
        Route("/", index),
        Route("/api/stats", api_stats),
        Route("/api/search", api_search),
        Route("/_zoo/health", health),
        Route("/_zoo/probe", probe),
        Route("/_zoo/trace/{trace}", trace),
    ],
    middleware=[Middleware(ZooCors)],
)
