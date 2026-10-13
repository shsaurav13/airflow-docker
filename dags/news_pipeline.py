"""News pipeline, Airflow side.

news_poll: every 10 min, read RSS feeds and send links we have never seen to Kafka `news.raw`.
news_load: every 10 min, read new messages from Kafka `news.articles` and bulk-insert into Postgres.
The always-on fetcher (news_fetcher.py) sits between the two topics and downloads the pages.
"""
import hashlib
import json
import re
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

FEEDS = {
    "BBC World": "https://feeds.bbci.co.uk/news/world/rss.xml",
    "The Guardian": "https://www.theguardian.com/world/rss",
    "NPR": "https://feeds.npr.org/1001/rss.xml",
    "Al Jazeera": "https://www.aljazeera.com/xml/rss/all.xml",
    "The Hindu": "https://www.thehindu.com/news/national/feeder/default.rss",
    "Indian Express": "https://indianexpress.com/feed/",
    "TechCrunch": "https://techcrunch.com/feed/",
    "The Verge": "https://www.theverge.com/rss/index.xml",
    "Ars Technica": "https://feeds.arstechnica.com/arstechnica/index",
    "Hacker News": "https://hnrss.org/frontpage",
}
EVERY_10_MIN = timedelta(minutes=10)
DEFAULTS = {"retries": 2, "retry_delay": timedelta(minutes=1)}


@dag(schedule=EVERY_10_MIN, start_date=datetime(2026, 1, 1), catchup=False, max_active_runs=1,
     default_args=DEFAULTS, tags=["news"])
def news_poll():
    @task
    def poll_feeds() -> None:
        import feedparser
        import requests
        from confluent_kafka import Producer

        import news_lib

        news_lib.setup()
        items = []
        for source, url in FEEDS.items():
            try:
                resp = requests.get(url, timeout=20, headers={"User-Agent": "airflow-news-learning/0.1"})
                resp.raise_for_status()
            except requests.RequestException as e:
                print(f"skip {source}: {e}")  # one broken feed must not stop the others
                continue
            for e in feedparser.parse(resp.content).entries:
                if e.get("link") and e.get("title"):
                    published = e.get("published_parsed") or e.get("updated_parsed")
                    items.append({
                        "url": e.link, "title": e.title.strip(), "source": source,
                        "published_at": datetime(*published[:6], tzinfo=timezone.utc).isoformat() if published else None,
                    })

        urls = [i["url"] for i in items]
        with news_lib.db() as conn, conn.cursor() as cur:
            cur.execute("SELECT url FROM news.seen_urls WHERE url = ANY(%s)", (urls,))
            sent = {r[0] for r in cur.fetchall()}  # already seen = skip

        producer = Producer({"bootstrap.servers": news_lib.KAFKA})
        new = []
        for i in items:
            if i["url"] not in sent:
                producer.produce(news_lib.RAW, key=i["source"], value=json.dumps(i))
                sent.add(i["url"])
                new.append(i["url"])
        producer.flush()
        # Mark as seen only after Kafka has them: a crash in between re-sends (dupes are dropped at load), never loses.
        with news_lib.db() as conn, conn.cursor() as cur:
            cur.execute("INSERT INTO news.seen_urls (url) SELECT unnest(%s::text[]) ON CONFLICT DO NOTHING", (new,))
        sent = new
        print(f"read {len(items)} feed items from {len(FEEDS)} feeds, sent {len(sent)} new links to {news_lib.RAW}")

    poll_feeds()


def title_key(title: str) -> str:
    return hashlib.sha1(re.sub(r"\W+", " ", title.lower()).strip().encode()).hexdigest()


@dag(schedule=EVERY_10_MIN, start_date=datetime(2026, 1, 1), catchup=False, max_active_runs=1,
     default_args=DEFAULTS, tags=["news"])
def news_load():
    @task
    def load_articles() -> None:
        from psycopg2.extras import execute_values

        import news_lib

        news_lib.setup()
        c = news_lib.consumer("news-loader")
        c.subscribe([news_lib.ARTICLES])
        rows, empty_polls = [], 0
        try:
            # Read until the topic is drained. The first polls can be empty while the group joins.
            while empty_polls < 3:
                msgs = [m for m in c.consume(num_messages=500, timeout=5) if not m.error()]
                empty_polls = 0 if msgs else empty_polls + 1
                for m in msgs:
                    a = json.loads(m.value())
                    rows.append((a["url"], title_key(a["title"]), a["title"], a["source"],
                                 a["published_at"], a["body"]))
            if rows:
                with news_lib.db() as conn, conn.cursor() as cur:
                    inserted = execute_values(cur, """
                        INSERT INTO news.articles (url, title_key, title, source, published_at, body)
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
