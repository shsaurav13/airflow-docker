"""Always-on Kafka consumer: news.raw -> download + extract article text -> news.articles.

Polite by design: obeys robots.txt, waits between requests to the same site, gives up after 3 tries
and parks the link in news.dead with the reason.
"""
import json
import time
from urllib.parse import urlsplit
from urllib.robotparser import RobotFileParser

import requests
import trafilatura
from confluent_kafka import Producer

import news_lib

UA = "airflow-news-learning/0.1"
SAME_SITE_GAP_S = 3
TRIES = 3

robots: dict[str, RobotFileParser] = {}
last_hit: dict[str, float] = {}


def allowed(url: str) -> bool:
    site = "{0.scheme}://{0.netloc}".format(urlsplit(url))
    if site not in robots:
        rp = RobotFileParser()
        try:
            r = requests.get(site + "/robots.txt", timeout=10, headers={"User-Agent": UA})
            rp.parse(r.text.splitlines() if r.ok else [])  # no robots.txt = allowed
        except requests.RequestException:
            rp.parse([])
        robots[site] = rp
    return robots[site].can_fetch(UA, url)


def download(url: str) -> str:
    host = urlsplit(url).netloc
    wait = SAME_SITE_GAP_S - (time.monotonic() - last_hit.get(host, 0))
    if wait > 0:
        time.sleep(wait)
    last_hit[host] = time.monotonic()
    r = requests.get(url, timeout=20, headers={"User-Agent": UA})
    r.raise_for_status()
    return r.text


def process(item: dict) -> tuple[str, dict]:
    """Returns (topic, message) for one news.raw item."""
    if not allowed(item["url"]):
        return news_lib.DEAD, {**item, "reason": "blocked by robots.txt"}
    error = ""
    for attempt in range(1, TRIES + 1):
        try:
            doc = trafilatura.bare_extraction(download(item["url"]), url=item["url"], with_metadata=True)
            break
        except Exception as e:
            error = f"{type(e).__name__}: {e}"
            time.sleep(2 ** attempt)
    else:
        return news_lib.DEAD, {**item, "reason": error[:300]}
    # Links from RSS come with a title; links from a crawler may not, so take it from the page.
    title = item.get("title") or (doc and doc.title)
    if not title:
        return news_lib.DEAD, {**item, "reason": "no title found on page"}
    return news_lib.ARTICLES, {
        **item,
        "title": title,
        "published_at": item.get("published_at") or (doc and doc.date),
        "body": doc and doc.text,  # None = page had no extractable text
    }


def main():
    news_lib.setup()
    producer = Producer({"bootstrap.servers": news_lib.KAFKA})
    c = news_lib.consumer("news-fetcher")
    c.subscribe([news_lib.RAW])
    print("fetcher started, waiting for links on", news_lib.RAW, flush=True)
    while True:
        m = c.poll(5)
        if m is None or m.error():
            continue
        try:
            topic, msg = process(json.loads(m.value()))
        except Exception as e:
            # Poison message (bad JSON, missing url, ...): park it in news.dead. Crashing here would
            # restart, re-read the same message and crash again forever, stalling the whole pipeline.
            topic, msg = news_lib.DEAD, {"raw": m.value().decode(errors="replace")[:2000],
                                         "reason": f"bad message: {type(e).__name__}: {e}"}
        producer.produce(topic, key=m.key(), value=json.dumps(msg))
        producer.flush()
        c.commit(m, asynchronous=False)  # move the bookmark only after the result is safely in Kafka
        label = msg.get("title") or msg.get("url") or msg["reason"]
        print(f"{'OK  ' if topic == news_lib.ARTICLES else 'DEAD'} {msg.get('source', '?')}: {label[:80]}", flush=True)


if __name__ == "__main__":
    main()
