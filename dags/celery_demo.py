import socket
import time
from collections import Counter
from datetime import datetime
from airflow.sdk import dag, task

@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False, tags=["demo"])
def celery_demo():
    @task
    def work(i: int) -> str:
        host = socket.gethostname()
        print(f"task {i} running on worker {host}")
        time.sleep(20)
        return host

    @task
    def summary(hosts) -> None:
        for host, n in Counter(list(hosts)).items():
            print(f"{host} ran {n} tasks")

    summary(work.expand(i=list(range(10))))

celery_demo()
