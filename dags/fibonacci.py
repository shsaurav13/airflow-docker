from datetime import datetime
from airflow.sdk import Param, dag, task

@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={"n": Param(15, type="integer", minimum=1, maximum=5000)},
    tags=["demo"],
)
def fibonacci():
    @task
    def generate(**context) -> list[int]:
        n = context["params"]["n"]
        seq = [0, 1][:n]
        while len(seq) < n:
            seq.append(seq[-1] + seq[-2])
        return seq

    @task
    def print_series(seq: list[int]) -> None:
        print(f"First {len(seq)} Fibonacci numbers:")
        print(", ".join(map(str, seq)))

    print_series(generate())

fibonacci()
