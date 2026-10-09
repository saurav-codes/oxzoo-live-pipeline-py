"""Cron, every 5 minutes: recount the last two hours of events into
zoo.hourly (one row per hour and kind; a newer count replaces the old)."""

import stores

SQL = """INSERT INTO zoo.hourly (hour, kind, events, updated_at)
SELECT toStartOfHour(created_at) AS hour, kind, count() AS events, now()
FROM zoo.events FINAL
WHERE created_at >= toStartOfHour(now()) - INTERVAL 1 HOUR
GROUP BY hour, kind"""

if __name__ == "__main__":
    ch = stores.ClickHouse()
    ch.run(SQL)
    rows = ch.rows("SELECT count() AS n FROM zoo.hourly FINAL WHERE hour >= toStartOfHour(now()) - INTERVAL 1 HOUR")
    print(f"rollup: {rows[0]['n']} hour and kind rows for the last two hours", flush=True)
