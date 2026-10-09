"""The puller worker: every 5 s, a signed GET of new events from event-bus,
then ClickHouse, Qdrant and Meilisearch in that order, then the cursor.

A crash mid-batch pulls the batch again: every write is keyed by event id,
so a second pass replaces rather than duplicates."""

import json
import logging
import os
import re
import signal
import time
from datetime import datetime, timezone

import stores
import zoo

NAME = "pipeline-py"
EVERY_S = 5
BATCH = 100
KINDS_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
MAX_TEXT = 2000

log = logging.getLogger("puller")


def parse_events(body: bytes) -> list[dict]:
    """event-bus's {"events": [...]} into clean rows; anything malformed
    is dropped with a log line, never stored."""
    data = json.loads(body)
    if not isinstance(data, dict) or not isinstance(data.get("events"), list):
        raise ValueError("expected {\"events\": [...]}")
    out = []
    for e in data["events"][:BATCH]:
        try:
            ev_id, trace, kind, text = e["id"], e.get("trace") or "", e["kind"], e["text"]
            if not (isinstance(ev_id, int) and ev_id > 0 and isinstance(text, str) and isinstance(kind, str)
                    and KINDS_RE.fullmatch(kind) and (trace == "" or zoo.valid_trace(trace))):
                raise ValueError("bad field")
            created = datetime.fromisoformat(str(e.get("created_at", "")).replace("Z", "+00:00"))
            out.append({"id": ev_id, "trace": trace, "kind": kind, "text": text[:MAX_TEXT],
                        "caller": str(e.get("caller", ""))[:40], "created_at": created.astimezone(timezone.utc).isoformat()})
        except (KeyError, TypeError, ValueError) as err:
            log.warning("dropping malformed event %r: %s", e.get("id") if isinstance(e, dict) else e, err)
    return out


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def cursor(ch: stores.ClickHouse) -> int:
    return int(ch.rows("SELECT max(max_id) AS c FROM zoo.pulls")[0]["c"])


def pull_once(ch: stores.ClickHouse, qdrant: stores.Qdrant, meili: stores.Meili) -> int:
    after = cursor(ch)
    url = os.environ["EVENTS_URL"].rstrip("/") + f"/api/events?after={after}&limit={BATCH}"
    status, body = zoo.signed_request("GET", url, os.environ["EVENTS_KEY"], NAME)
    if status != 200:
        raise RuntimeError(f"event-bus answered {status}: {body[:120].decode(errors='replace')}")
    events = parse_events(body)
    hops = []
    if events:
        traced = [e for e in events if e["trace"]]
        ch.insert("zoo.events", events)
        hops += [{"trace": e["trace"], "at": now_iso(), "step": "clickhouse", "detail": f"event {e['id']} in zoo.events"} for e in traced]
        qdrant.upsert(stores.EVENTS, [{"id": e["id"], "vector": stores.vector(e["text"]),
                                       "payload": {"kind": e["kind"], "text": e["text"], "trace": e["trace"]}} for e in events])
        hops += [{"trace": e["trace"], "at": now_iso(), "step": "qdrant", "detail": f"point {e['id']} in collection events"} for e in traced]
        task = meili.wait(meili.call("POST", f"/indexes/{stores.EVENTS}/documents", events))
        hops += [{"trace": e["trace"], "at": now_iso(), "step": "meili", "detail": f"document {e['id']} indexed, task {task['uid']}"}
                 for e in traced]
        ch.insert("zoo.traces", hops)
    ch.insert("zoo.pulls", [{"at": now_iso(), "max_id": max([after] + [e["id"] for e in events]), "events": len(events)}])
    return len(events)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    stop = []
    signal.signal(signal.SIGTERM, lambda *_: stop.append(1))
    ch, qdrant, meili = stores.ClickHouse(), stores.Qdrant(), stores.Meili()
    log.info("pulling %s every %ss", "/api/events", EVERY_S)
    while not stop:
        started = time.monotonic()
        try:
            if n := pull_once(ch, qdrant, meili):
                log.info("stored %d events", n)
        except Exception as e:  # keep the worker alive; the probe shows the lag
            log.error("pull failed: %s", zoo.scrub(f"{type(e).__name__}: {e}", ["EVENTS_KEY", "CLICKHOUSE_PASSWORD", "MEILI_MASTER_KEY"]))
        time.sleep(max(0.0, EVERY_S - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
