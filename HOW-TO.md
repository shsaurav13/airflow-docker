# How to set up Airflow 3.3.1 on Docker (MacBook Pro 14, multi-container)

This explains how to build the setup yourself, step by step. The other files in this folder
are a finished, tested version of each step you can compare against.

## 0. Version check
3.3.1 exists (`apache/airflow:3.3.1-python3.12`). 3.3.2 is also out as a patch release, so
consider pinning 3.3.2 instead. Every image used here ships native arm64, so Apple Silicon is fine.

## 1. Prepare Docker Desktop
Settings > Resources: Memory 8 GB (6 GB minimum), CPUs 4+, Swap 1 to 2 GB, disk 40 GB+.
Settings > General: file sharing = VirtioFS. Keep the project under your home folder.

## 2. Understand the containers (why it is "multi-container")
| Service | Role |
|---|---|
| postgres | Metadata DB (DAG runs, task state, users, connections) and Celery result backend |
| redis | Celery broker: the queue the scheduler pushes tasks into |
| airflow-init | One-shot job: `airflow db migrate` and create the admin user, then exit |
| airflow-apiserver | UI + REST API, and the Execution API that tasks report back to (new in Airflow 3) |
| airflow-scheduler | Decides what runs when and queues tasks |
| airflow-dag-processor | Parses DAG files (split out from the scheduler in Airflow 3) |
| airflow-triggerer | Runs deferrable (async) sensors/operators |
| airflow-worker | Celery workers that actually execute tasks; scale horizontally |
| flower (optional) | Celery monitoring UI |

## 3. Create the folder layout
```
airflow-docker/
  Dockerfile  requirements.txt  docker-compose.yaml  .env  .gitignore
  dags/  plugins/  config/  scripts/
```

## 4. Build a custom image (don't pip install at startup)
`Dockerfile`: `FROM apache/airflow:3.3.1-python3.12`, copy `requirements.txt`, and
`pip install "apache-airflow==3.3.1" -r requirements.txt --constraint <official constraints-3.3.1/constraints-3.12.txt URL>`.
The constraints file stops pip from breaking Airflow's own dependencies.
Avoid `_PIP_ADDITIONAL_REQUIREMENTS`; it reinstalls on every container start.

## 5. Put every secret in `.env` (never commit it)
Generate them, don't type them:
- `POSTGRES_PASSWORD`, `REDIS_PASSWORD`: `openssl rand -hex 24` (hex avoids URL-escaping issues)
- `AIRFLOW_FERNET_KEY` (encrypts connections): `openssl rand -base64 32 | tr '+/' '-_'`
- `AIRFLOW_API_SECRET_KEY`, `AIRFLOW_JWT_SECRET`: `openssl rand -hex 32`
- Admin username/password, `AIRFLOW_UID=50000` on macOS
Back up `.env`: losing the Fernet key makes stored connections unreadable.

## 6. Write `docker-compose.yaml`
Use a shared YAML anchor (`x-airflow-common`) for image, env and volumes so every Airflow
service is configured identically. Key settings:
- `AIRFLOW__CORE__EXECUTOR: CeleryExecutor`
- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://user:${POSTGRES_PASSWORD}@postgres:5432/airflow`
- `AIRFLOW__CELERY__BROKER_URL: redis://:${REDIS_PASSWORD}@redis:6379/0`
- `AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql+psycopg2://...`
- `AIRFLOW__CORE__EXECUTION_API_SERVER_URL: http://airflow-apiserver:8080/execution/` (required in 3.x)
- `AIRFLOW__CORE__FERNET_KEY`, `AIRFLOW__API__SECRET_KEY`, `AIRFLOW__API_AUTH__JWT_SECRET` from `.env`
- `AIRFLOW__CORE__AUTH_MANAGER`: FAB auth manager for real users/roles
- `AIRFLOW__CORE__LOAD_EXAMPLES: "false"`
- Use `${VAR:?message}` so Compose refuses to start when a secret is missing.

Per service: a healthcheck, `restart: unless-stopped`, `depends_on` with
`condition: service_healthy` (and `service_completed_successfully` on airflow-init),
and memory limits. Laptop sizing that fits an 8 GB VM: 1 worker, concurrency 4,
api-server 1.5 GB, scheduler 1 GB, dag-processor 1 GB, triggerer 768 MB, worker 1.5 GB,
postgres 512 MB, redis 256 MB.

## 7. Init and start
```bash
docker compose build
docker compose up airflow-init        # migrate DB + create admin, exits 0
docker compose up -d                  # start everything else
docker compose ps                     # wait for (healthy) everywhere
```
Open http://localhost:8080. Scale workers with `docker compose up -d --scale airflow-worker=2`.

## 8. Production-style hardening
- Don't publish Postgres or Redis ports; bind the UI to `127.0.0.1:8080`.
- Password-protect Redis (`--requirepass`) and turn on `--appendonly yes`.
- Rotate container logs (`json-file`, `max-size`, `max-file`).
- Give workers `stop_grace_period` so running tasks finish on shutdown.
- Back up Postgres with `pg_dump -Fc` on a schedule.
- Use remote logging (S3/GCS) and a secrets backend for connections when it matters.
- Upgrades: back up the DB, bump the version, rebuild; airflow-init runs `db migrate`.

## 9. Know the limit
One laptop means schedules pause whenever it sleeps. For 24/7 pipelines, run the same compose
files on an always-on Linux VM.
