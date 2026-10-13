from collections import Counter
from datetime import datetime
import requests
from airflow.sdk import dag, task

URL = "https://microsoftedge.github.io/Demos/json-dummy-data/64KB.json"

@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False, tags=["demo"])
def edge_extract_print():
    @task
    def extract():
        resp = requests.get(URL, timeout=30)
        resp.raise_for_status()
        return resp.json()

    @task
    def print_count(people):
        print(f"Got {len(people)} records")

    @task
    def print_people(people):
        for p in people:
            print(f"{p['id']} | {p['name']} | {p['language']} | v{p['version']}")

    @task
    def print_languages(people):
        for lang, n in Counter(p["language"] for p in people).most_common():
            print(f"{lang}: {n}")

    data = extract()
    print_count(data)
    print_people(data)
    print_languages(data)

edge_extract_print()
