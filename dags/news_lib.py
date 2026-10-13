"""Shared bits for the news pipeline: Kafka topics, Postgres tables, consumer loop."""
import os

import psycopg2
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic

KAFKA = os.environ["KAFKA_BOOTSTRAP"]
DB_URL = os.environ["NEWS_DB_URL"]

RAW, ARTICLES, DEAD = "news.raw", "news.articles", "news.dead"

# ponytail: news tables live in a `news` schema of the Airflow metadata DB; move to their own DB when real.
DDL = """
CREATE SCHEMA IF NOT EXISTS news;
CREATE TABLE IF NOT EXISTS news.seen_urls (
    url        text PRIMARY KEY,
    first_seen timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS news.articles (
    id           bigserial PRIMARY KEY,
    url          text UNIQUE NOT NULL,
    title_key    text UNIQUE NOT NULL,   -- same headline from two feeds = one story
    title        text NOT NULL,
    source       text NOT NULL,
    published_at timestamptz,
    body         text,
    loaded_at    timestamptz NOT NULL DEFAULT now(),
    search       tsvector GENERATED ALWAYS AS (
                   to_tsvector('english', title || ' ' || coalesce(body, ''))) STORED
);
CREATE INDEX IF NOT EXISTS articles_search_idx ON news.articles USING gin (search);
"""


def db():
    return psycopg2.connect(DB_URL)


def setup():
    """Create topics and tables if missing. Safe to call every run."""
    admin = AdminClient({"bootstrap.servers": KAFKA})
    existing = admin.list_topics(timeout=10).topics
    new = [NewTopic(t, num_partitions=3, replication_factor=1) for t in (RAW, ARTICLES, DEAD) if t not in existing]
    for f in admin.create_topics(new).values() if new else []:
        try:
            f.result()
        except Exception as e:  # another process created it first
            if "TOPIC_ALREADY_EXISTS" not in str(e):
                raise
    with db() as conn, conn.cursor() as cur:
        cur.execute(DDL)


def consumer(group):
    return Consumer({
        "bootstrap.servers": KAFKA,
        "group.id": group,
        "auto.offset.reset": "earliest",   # a new group starts from the oldest message
        "enable.auto.commit": False,       # we commit only after the work is done
    })
