# pipeline-py

> **Role in the zoo:** project `pipeline-py` of [oxzoo-live](https://github.com/saurav-codes/oxzoo-live-control/blob/main/zoo/README.md#projects), deployed with ox on server s4 at https://pipeline-py.s4.zoo.sorv.dev. The contract it follows is [DESIGN.md](https://github.com/saurav-codes/oxzoo-live-control/blob/main/zoo/DESIGN.md).

Starlette + uvicorn, a puller worker and a cron job over three stores:
ClickHouse, Qdrant and Meilisearch. The worker pulls new events from
event-bus every 5 s and writes each one to all three; the page shows counts,
hourly rollups and search.

## What it proves

- An ox worker that runs next to the web process: `puller` does a signed
  `GET ${EVENTS_URL}/api/events?after=<cursor>&limit=100`, then writes
  ClickHouse (`zoo.events`), Qdrant (a 64-dim vector from the text, collection
  `events`) and Meilisearch (index `events`), in that order, and only then
  moves the cursor (`zoo.pulls`). Every write is keyed by event id, so a
  crash mid-batch replays it without duplicates.
- Malformed events (bad id, kind, trace or date) are dropped and logged,
  never stored; text is capped at 2000 characters.
- Traced events record one hop per store, served at `GET /_zoo/trace/<id>`:
  `clickhouse`, `qdrant`, `meili`.
- A cron job (`rollup`, every 5 minutes) recounts the last two hours into
  `zoo.hourly`.
- A custom ClickHouse service with a pinned, sha256-checked official build,
  HTTP only on 127.0.0.1, password auth, under 1 GB.

## ox features used

- Starlette detection (the uvicorn start, since ox 740c9efc), `[app] health`.
- `[build] migrate`: `python stores.py` creates tables, indexes and
  collections; every step is idempotent.
- `[workers] puller` (128M), `[cron] rollup`.
- `qdrant = {}` (built-in), `[services.search]` (Meilisearch via
  `github:meilisearch/meilisearch@1.54.2`, copied from search-svc) and
  `[services.clickhouse]` (`tool = "github:clickhouse/clickhouse@26.8.21.10-lts"`,
  a run line that writes its config, `backup = false`).

## Variables

| Variable | From | Role |
| --- | --- | --- |
| `CLICKHOUSE_URL`, `CLICKHOUSE_PASSWORD` | ox (clickhouse service) | service |
| `MEILI_URL`, `MEILI_MASTER_KEY` | ox (search service) | service |
| `QDRANT_URL` | ox (qdrant) | service |
| `PORT`, `PUBLIC_HOST`, `OX_ENV`, `OX_RELEASE` | ox | |
| `EVENTS_KEY` | yours, secret shared with event-bus | signs |
| `EVENTS_URL` | yours, `https://event-bus.s3.zoo.sorv.dev` | url, peer event-bus |
| `ZOO_PANEL_ORIGIN` | yours, `https://zoo-control.s1.zoo.sorv.dev` | plain |

## Zoo endpoints

- `GET /_zoo/health`, `GET /_zoo/probe`, `GET /_zoo/trace/<id>` (CORS for
  `ZOO_PANEL_ORIGIN`; a malformed trace id answers 400).
- Probe checks: ClickHouse insert, select and delete on `zoo.probe`;
  Meilisearch add, search and delete on `zoo_probe`; Qdrant upsert, search
  and delete on `zoo_probe`; the puller's last successful pull under 30 s
  ago; `peer:event-bus` via its `/_zoo/verify`.
- `GET /` page, `GET /api/stats`, `GET /api/search?q=` (q up to 200
  characters; Meilisearch hits and Qdrant nearest). A store that is down
  answers 503 with the error, keys scrubbed.

## Run locally

```sh
uv sync
export CLICKHOUSE_URL=http://127.0.0.1:8123 CLICKHOUSE_PASSWORD=...
export MEILI_URL=http://127.0.0.1:7700 MEILI_MASTER_KEY=... QDRANT_URL=http://127.0.0.1:6333
export EVENTS_URL=http://127.0.0.1:8080 EVENTS_KEY=...
uv run python stores.py
.venv/bin/python puller.py &
uv run python rollup.py
.venv/bin/uvicorn app:app --host 127.0.0.1 --port 8000
```

## Tests

```sh
uv run pytest      # unit tests
TEST_CLICKHOUSE_URL=... TEST_CLICKHOUSE_PASSWORD=... TEST_MEILI_URL=... \
TEST_MEILI_MASTER_KEY=... TEST_QDRANT_URL=... uv run pytest   # plus the stores
```

Recorded: unit only 37 passed, 1 skipped; with local ClickHouse 26.8.21.10
(macOS build), Meilisearch 1.54 and Qdrant 1.19, 38 passed (migrate twice, a
pull with a traced event, trace hops, search hits and nearest, stats).

Also run by hand against those stores and a local fake event-bus: migrate
twice, puller stored 30 seeded events and dropped one with a bad trace id,
a traced event showed three hops at `/_zoo/trace/<id>`, rollup wrote the hour,
probe all 5 checks ok, page and preflight fine, SIGTERM stops the puller.

## ox check

```
ox check . (manifest: ox.toml)

  app.start                  uv run uvicorn app:app --host 127.0.0.1 --port $PORT detected:app.py
  app.health                 /_zoo/health                                         declared
  build.install              uv sync --frozen --no-dev                            detected:uv.lock
  build.migrate              uv run python stores.py                              declared
  workers.puller             exec .venv/bin/python puller.py                      declared
  cron.rollup                */5 * * * *  uv run python rollup.py                 declared
  tools.github:clickhouse/clickhouse 26.8.21.10-lts                                       declared
  tools.github:meilisearch/meilisearch 1.54.2                                               declared
  tools.github:qdrant/qdrant 1.19.1                                               default
  tools.python               3.13                                                 detected:.python-version
  tools.uv                   0.11                                                 default
  services.clickhouse        custom (only for this project)                       declared
  services.qdrant            qdrant 1 (only for this project)                     declared
  services.search            custom (only for this project)                       declared

  Provided by ox: PORT, HOST, OX_ENV, OX_PROJECT, OX_RELEASE, OX_DATA_DIR, PUBLIC_URL, PUBLIC_HOST, CLICKHOUSE_PASSWORD, CLICKHOUSE_URL, QDRANT_URL, MEILI_MASTER_KEY, MEILI_URL
  Set on the dashboard before the first deploy: EVENTS_KEY, EVENTS_URL, ZOO_PANEL_ORIGIN

Ready to deploy.
```

## ClickHouse as a tool

`github:clickhouse/clickhouse@26.8.21.10-lts` installs the official
`clickhouse-common-static` build, which ox pins and checks against its
sha256 (ox 8b86520a, x86_64 only). Until then the run line downloaded and
checked the tarball itself, because mise picked the `clickhouse-client`
tarball, which has no server, and Ubuntu has no `clickhouse-server` package
after noble.

Not verified here: the Linux ClickHouse build running under ox's sandbox
(the run line was tested on macOS up to the exec, and the same config ran
with the macOS build); the first-start download finishing inside ox's
service health wait; the real event-bus (a local fake with the same API and
signing was used); the worker and cron under ox's real supervisor.
