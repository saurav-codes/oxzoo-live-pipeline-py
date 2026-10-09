"""The three stores the pipeline writes, over HTTP with httpx, and the
deterministic event vector. `python stores.py` creates the schema (the
deploy's migrate step); every statement is idempotent."""

import hashlib
import json
import math
import os
import re
import time

import httpx

DIM = 64
TIMEOUT = httpx.Timeout(5.0)
EVENTS = "events"  # Qdrant collection and Meilisearch index
PROBE = "zoo_probe"
WORD_RE = re.compile(r"\w+")


def vector(text: str) -> list[float]:
    """A 64-dim unit vector from the text's words: each word adds +1 or -1
    at a slot picked by its sha256. Same text, same vector."""
    v = [0.0] * DIM
    for word in WORD_RE.findall(text.lower()) or [text]:
        h = hashlib.sha256(word.encode()).digest()
        v[int.from_bytes(h[:2], "big") % DIM] += 1.0 if h[2] & 1 else -1.0
    norm = math.sqrt(sum(x * x for x in v))
    if norm == 0:  # words that cancel out; fall back to the whole text's hash
        h = hashlib.sha256(text.encode()).digest()
        v[h[0] % DIM] = 1.0
        return v
    return [x / norm for x in v]


class StoreError(RuntimeError):
    pass


def check(r: httpx.Response, what: str) -> httpx.Response:
    if r.status_code >= 300:
        raise StoreError(f"{what} answered {r.status_code}: {r.text[:200]}")
    return r


class ClickHouse:
    """ClickHouse HTTP interface as user default; values go in as query
    parameters ({name:Type}), never formatted into SQL."""

    def __init__(self):
        self.url = os.environ["CLICKHOUSE_URL"].rstrip("/") + "/"
        self.headers = {"X-ClickHouse-User": "default", "X-ClickHouse-Key": os.environ["CLICKHOUSE_PASSWORD"]}

    def run(self, sql: str, params: dict | None = None, data: bytes | None = None, timeout=TIMEOUT) -> str:
        q = {"query": sql, "date_time_input_format": "best_effort"}
        q |= {f"param_{k}": str(v) for k, v in (params or {}).items()}
        r = httpx.post(self.url, params=q, content=data or b"", headers=self.headers, timeout=timeout)
        return check(r, "clickhouse").text

    def rows(self, sql: str, params: dict | None = None) -> list[dict]:
        out = self.run(sql + " FORMAT JSONEachRow", params)
        return [json.loads(line) for line in out.splitlines() if line]

    def insert(self, table: str, rows: list[dict]) -> None:
        if rows:
            body = "\n".join(json.dumps(r) for r in rows).encode()
            self.run(f"INSERT INTO {table} FORMAT JSONEachRow", data=body)


SCHEMA = [
    "CREATE DATABASE IF NOT EXISTS zoo",
    """CREATE TABLE IF NOT EXISTS zoo.events (
        id UInt64, trace String, kind LowCardinality(String), text String, caller LowCardinality(String),
        created_at DateTime64(3, 'UTC'), pulled_at DateTime64(3, 'UTC') DEFAULT now64(3)
    ) ENGINE = ReplacingMergeTree ORDER BY id""",
    # One row per pull; the cursor is the max id here, written after all
    # three stores took the batch.
    """CREATE TABLE IF NOT EXISTS zoo.pulls (at DateTime64(3, 'UTC'), max_id UInt64, events UInt32)
       ENGINE = MergeTree ORDER BY at TTL toDateTime(at) + INTERVAL 2 DAY""",
    """CREATE TABLE IF NOT EXISTS zoo.traces (trace String, at DateTime64(3, 'UTC'), step LowCardinality(String), detail String)
       ENGINE = ReplacingMergeTree ORDER BY (trace, step) TTL toDateTime(at) + INTERVAL 7 DAY""",
    """CREATE TABLE IF NOT EXISTS zoo.hourly (hour DateTime('UTC'), kind LowCardinality(String), events UInt64, updated_at DateTime('UTC'))
       ENGINE = ReplacingMergeTree(updated_at) ORDER BY (hour, kind)""",
    "CREATE TABLE IF NOT EXISTS zoo.probe (token String, at DateTime DEFAULT now()) ENGINE = MergeTree ORDER BY token",
]


class Meili:
    def __init__(self):
        self.url = os.environ["MEILI_URL"].rstrip("/")
        self.headers = {"Authorization": f"Bearer {os.environ['MEILI_MASTER_KEY']}"}

    def call(self, method: str, path: str, body=None) -> dict:
        r = httpx.request(method, self.url + path, json=body, headers=self.headers, timeout=TIMEOUT)
        return check(r, "meilisearch").json()

    def wait(self, task: dict, limit_s: float = 10.0) -> dict:
        deadline = time.monotonic() + limit_s
        while True:
            t = self.call("GET", f"/tasks/{task['taskUid']}")
            if t["status"] == "succeeded":
                return t
            if t["status"] in ("failed", "canceled"):
                raise StoreError(f"meilisearch task {t['uid']} {t['status']}: {(t.get('error') or {}).get('message')}")
            if time.monotonic() > deadline:
                raise StoreError(f"meilisearch task {t['uid']} still {t['status']} after {limit_s:.0f}s")
            time.sleep(0.1)

    def ensure_index(self, uid: str) -> None:
        r = httpx.get(f"{self.url}/indexes/{uid}", headers=self.headers, timeout=TIMEOUT)
        if r.status_code == 404:
            self.wait(self.call("POST", "/indexes", {"uid": uid, "primaryKey": "id"}))
        else:
            check(r, "meilisearch")


class Qdrant:
    def __init__(self):
        self.url = os.environ["QDRANT_URL"].rstrip("/")

    def call(self, method: str, path: str, body=None) -> dict:
        r = httpx.request(method, self.url + path, json=body, timeout=TIMEOUT)
        return check(r, "qdrant").json()

    def ensure_collection(self, name: str) -> None:
        r = httpx.get(f"{self.url}/collections/{name}", timeout=TIMEOUT)
        if r.status_code == 404:
            self.call("PUT", f"/collections/{name}", {"vectors": {"size": DIM, "distance": "Cosine"}})
        else:
            check(r, "qdrant")

    def upsert(self, name: str, points: list[dict]) -> None:
        self.call("PUT", f"/collections/{name}/points?wait=true", {"points": points})

    def nearest(self, name: str, vec: list[float], limit: int = 5) -> list[dict]:
        return self.call("POST", f"/collections/{name}/points/query",
                         {"query": vec, "limit": limit, "with_payload": True})["result"]["points"]


def migrate() -> None:
    ch = ClickHouse()
    for stmt in SCHEMA:
        ch.run(stmt)
    meili = Meili()
    for uid in (EVENTS, PROBE):
        meili.ensure_index(uid)
    qdrant = Qdrant()
    for name in (EVENTS, PROBE):
        qdrant.ensure_collection(name)


if __name__ == "__main__":
    migrate()
    print("clickhouse tables, meilisearch indexes and qdrant collections ready", flush=True)
