"""Where news links come from.

Every source has a kind, and every kind is one function: Source -> list of links.
  - add a site:  append a Source to SOURCES
  - add a kind (e.g. a crawler):  write a function `(src: Source) -> list[dict]`,
    register it in DISCOVER (e.g. "crawl"), then add Source(..., kind="crawl", url=<homepage>)
A link is {"url", "title", "source", "published_at"}. title and published_at may be None:
the fetcher fills them in from the page itself.
"""
from dataclasses import dataclass
from datetime import datetime, timezone

UA = "airflow-news-learning/0.1"


@dataclass(frozen=True)
class Source:
    name: str
    kind: str  # key in DISCOVER
    url: str
    max_links: int = 100


SOURCES = [
    Source("BBC World", "rss", "https://feeds.bbci.co.uk/news/world/rss.xml"),
    Source("The Guardian", "rss", "https://www.theguardian.com/world/rss"),
    Source("NPR", "rss", "https://feeds.npr.org/1001/rss.xml"),
    Source("Al Jazeera", "rss", "https://www.aljazeera.com/xml/rss/all.xml"),
    Source("The Hindu", "rss", "https://www.thehindu.com/news/national/feeder/default.rss"),
    Source("Indian Express", "rss", "https://indianexpress.com/feed/"),
    Source("TechCrunch", "rss", "https://techcrunch.com/feed/"),
    Source("The Verge", "rss", "https://www.theverge.com/rss/index.xml"),
    Source("Ars Technica", "rss", "https://feeds.arstechnica.com/arstechnica/index"),
    Source("Hacker News", "rss", "https://hnrss.org/frontpage"),
]
BY_NAME = {s.name: s for s in SOURCES}


def rss_links(src: Source) -> list[dict]:
    import feedparser
    import requests

    resp = requests.get(src.url, timeout=20, headers={"User-Agent": UA})
    resp.raise_for_status()
    links = []
    for e in feedparser.parse(resp.content).entries[: src.max_links]:
        if e.get("link") and e.get("title"):
            published = e.get("published_parsed") or e.get("updated_parsed")
            links.append({
                "url": e.link, "title": e.title.strip(), "source": src.name,
                "published_at": datetime(*published[:6], tzinfo=timezone.utc).isoformat() if published else None,
            })
    return links


DISCOVER = {"rss": rss_links}


def discover(src: Source) -> list[dict]:
    return DISCOVER[src.kind](src)
