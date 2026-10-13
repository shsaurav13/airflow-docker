"""News pipeline, Airflow side.

news_poll: hourly, one task per source (news_sources.py) sends links we have never seen to Kafka `news.raw`.
news_load: hourly, read new messages from Kafka `news.articles` and bulk-insert into Postgres.
The always-on fetcher (news_fetcher.py) sits between the two topics and downloads the pages.
"""
import hashlib
import json
import re
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from news_sources import SOURCES

DEFAULTS = {"retries": 2, "retry_delay": timedelta(minutes=1)}
MAX_ROWS_PER_LOAD = 5000  # bounds worker memory; after an outage the backlog drains over several runs


@dag(schedule="@hourly", start_date=datetime(2026, 1, 1), catchup=False, max_active_runs=1,
     default_args=DEFAULTS, tags=["news"])
def news_poll():
    @task
    def setup() -> None:
        import news_lib

        news_lib.setup()

    @task(map_index_template="{{ task.op_kwargs['name'] }}")
    def poll_source(name: str) -> None:
        """One mapped task per source: a broken site fails (and retries) on its own, the rest carry on."""
        from confluent_kafka import Producer

        import news_lib
        import news_sources

        links = news_sources.discover(news_sources.BY_NAME[name])
        urls = [i["url"] for i in links]
        with news_lib.db() as conn, conn.cursor() as cur:
            cur.execute("SELECT url FROM seen_urls WHERE url = ANY(%s)", (urls,))
            seen = {r[0] for r in cur.fetchall()}

        producer = Producer({"bootstrap.servers": news_lib.KAFKA})
        new = []
        for i in links:
            if i["url"] not in seen:
                producer.produce(news_lib.RAW, key=i["source"], value=json.dumps(i))
                seen.add(i["url"])
                new.append(i["url"])
        producer.flush()
        # Mark as seen only after Kafka has them: a crash in between re-sends (dupes are dropped at load), never loses.
        with news_lib.db() as conn, conn.cursor() as cur:
            cur.execute("INSERT INTO seen_urls (url) SELECT unnest(%s::text[]) ON CONFLICT DO NOTHING", (new,))
        print(f"{name}: found {len(links)} links, sent {len(new)} new to {news_lib.RAW}")

    setup() >> poll_source.expand(name=[s.name for s in SOURCES])


def title_key(title: str) -> str:
    return hashlib.sha1(re.sub(r"\W+", " ", title.lower()).strip().encode()).hexdigest()


@dag(schedule="@hourly", start_date=datetime(2026, 1, 1), catchup=False, max_active_runs=1,
     default_args=DEFAULTS, tags=["news"])
def news_load():
    @task
    def load_articles() -> None:
        from psycopg2.extras import execute_values

        import news_lib

        news_lib.setup()
        c = news_lib.consumer("news-loader")
        c.subscribe([news_lib.ARTICLES])
        rows = []
        try:
            for _ in range(100):  # hard stop so a run can never hang
                msgs = c.consume(num_messages=500, timeout=5)
                for m in (m for m in msgs if not m.error()):
                    try:
                        a = json.loads(m.value())
                        rows.append((a["url"], title_key(a["title"]), a["title"], a["source"],
                                     a["published_at"], a["body"]))
                    except (ValueError, KeyError, TypeError, AttributeError) as e:
                        # A bad message must not fail every run forever; skip it (its offset still gets committed).
                        print(f"skip bad message at partition {m.partition()} offset {m.offset()}: {e!r}")
                # A partial batch means we reached the live end of the topic. The fetcher keeps adding
                # articles, so "wait until it's quiet" would never end; the next run takes the rest.
                # (Empty polls before the group is assigned partitions don't count.)
                if len(rows) >= MAX_ROWS_PER_LOAD or (len(msgs) < 500 and c.assignment()):
                    break
            if rows:
                with news_lib.db() as conn, conn.cursor() as cur:
                    inserted = execute_values(cur, """
                        INSERT INTO articles (url, title_key, title, source, published_at, body)
                        VALUES %s ON CONFLICT DO NOTHING RETURNING 1""", rows, fetch=True)
                print(f"read {len(rows)} articles, inserted {len(inserted)}, skipped {len(rows) - len(inserted)} duplicates")
                # Commit the Kafka offset only after Postgres committed: a crash re-reads, never loses.
                c.commit(asynchronous=False)
            else:
                print("no new articles")
        finally:
            c.close()

    load_articles()


news_poll()
news_load()
