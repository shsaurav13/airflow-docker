# Airflow 3.3.1 on Docker: production runbook

## What you get

| Service | Role | Exposed |
|---|---|---|
| postgres 16 | Metadata DB + Celery result backend | internal only |
| redis 7.2 | Celery broker (password + AOF persistence) | internal only |
| airflow-init | One-shot: `db migrate` + admin user, then exits | none |
| airflow-apiserver | UI + REST API + Execution API for tasks | 127.0.0.1:8080 |
| airflow-scheduler | Schedules task instances, health on :8974 | internal |
| airflow-dag-processor | Parses DAG files (separate process in Airflow 3) | internal |
| airflow-triggerer | Runs deferrable operators | internal |
| airflow-worker (x N) | Celery workers, 1 by default on a laptop | internal |
| flower (profile) | Celery monitoring, basic auth | 127.0.0.1:5555 |
| caddy (profile) | Reverse proxy with automatic HTTPS | 80 / 443 |

Every service has a healthcheck, `restart: unless-stopped`, rotated logs, and secrets
come only from `.env`. Compose refuses to start if a required secret is missing.

## Prerequisites (MacBook Pro 14 + Docker Desktop)
- Docker Desktop 4.30+ (Compose v2 is bundled). `openssl` ships with macOS.
- All images (apache/airflow, postgres, redis, caddy) are multi-arch, so they run natively
  on Apple Silicon (arm64) with no Rosetta emulation.
- Docker Desktop > Settings > Resources:
  - **Memory: 8 GB** (minimum 6 GB; with 16 GB+ total RAM you can give 10 to 12 GB)
  - **CPUs: 4** or more
  - **Swap: 1 to 2 GB**
  - **Virtual disk limit: 40 GB or more** (image is about 3 GB, plus volumes)
- Docker Desktop > Settings > General: file sharing implementation **VirtioFS** (fast bind mounts for `dags/`).
- Keep the project under your home folder (for example `~/airflow-docker`) so Docker Desktop can mount it.
- Leave `AIRFLOW_UID=50000` on macOS; gen-env.sh already does this.

## Sizing on the laptop
Measured with the smoke DAG running: the whole stack uses about 1.7 GB of RAM at rest.
The memory caps in `.env` add up to about 6.8 GB, so heavy tasks have room inside an 8 GB VM.
| Need | Change in `.env` |
|---|---|
| More tasks in parallel | `AIRFLOW_WORKER_CONCURRENCY=8` and `AIRFLOW_PARALLELISM=16` (give Docker 10 GB+) |
| A task gets OOM-killed (exit 137) | Raise `WORKER_MEM_LIMIT` (for example `3g`) |
| Prove multi-worker behaviour | `AIRFLOW_WORKER_REPLICAS=2` |
| Save battery when idle | `docker compose stop`, data stays in volumes |

## First deploy
```bash
cd airflow-docker
bash scripts/gen-env.sh          # creates .env with random secrets, prints admin password
# edit .env: AIRFLOW_BASE_URL, sizing, AIRFLOW_DOMAIN if using the proxy
# add providers to requirements.txt if needed
docker compose build
docker compose up -d
docker compose ps                # wait until everything says (healthy)
```
Open http://localhost:8080 and log in as `admin` with the printed password.
Unpause and trigger `smoke_test` to prove scheduler, workers and the Execution API work.

## Day-2 operations
| Task | Command |
|---|---|
| Scale workers | `docker compose up -d --scale airflow-worker=4` (or set `AIRFLOW_WORKER_REPLICAS`) |
| Airflow CLI | `docker compose run --rm airflow-cli dags list` |
| Tail logs | `docker compose logs -f airflow-scheduler` |
| Flower | `docker compose --profile flower up -d` then http://localhost:5555 |
| HTTPS (only when hosted on a server with a domain; skip on the laptop) | Set `AIRFLOW_DOMAIN` + `AIRFLOW_BASE_URL=https://<domain>`, then `docker compose --profile proxy up -d` |
| Backup metadata DB | `bash scripts/backup-db.sh` (cron it daily; keeps 14) |
| Restore | `docker compose exec -T postgres pg_restore -U airflow -d airflow --clean < backups/<file>.dump` |
| Add a provider | Add to `requirements.txt`, `docker compose build && docker compose up -d` |
| Stop (tasks finish) | `docker compose stop` (workers get 5 min warm shutdown) |

## Upgrading Airflow (e.g. 3.3.1 to 3.3.2)
1. `bash scripts/backup-db.sh`
2. Set `AIRFLOW_VERSION=3.3.2` in `.env`
3. `docker compose build && docker compose up -d` (airflow-init runs `db migrate` first)

## Hardening checklist before real production
- Never commit `.env`. Back it up in a secret manager: losing `AIRFLOW_FERNET_KEY` makes every stored connection unreadable.
- Use a managed Postgres (RDS / Cloud SQL) instead of the container for real durability; just change `SQL_ALCHEMY_CONN` and `RESULT_BACKEND`.
- Turn on remote logging (S3/GCS block in `docker-compose.yaml`) so logs survive container and host loss.
- Bake DAGs into the image (see Dockerfile comment) or sync them from git, instead of bind mounts.
- Store connections/variables in a secrets backend (AWS Secrets Manager, Vault) rather than the metadata DB.
- Keep the API bound to 127.0.0.1 and put TLS (Caddy profile or your LB) in front.
- Ship metrics: enable StatsD/OpenTelemetry (`AIRFLOW__METRICS__*`) to Prometheus/Grafana.

## Limits of this design
Everything runs on one machine, so a laptop that sleeps, reboots or loses Wi-Fi pauses all
schedules (missed runs are not backfilled since `catchup=False` is the default you should keep).
That is fine for development and personal pipelines. If DAGs must run 24/7, move the same
compose stack to an always-on Linux VM; nothing in it is Mac-specific.

## Troubleshooting
- `variable is not set` on `up`: a secret is missing from `.env`, rerun gen-env or fill it.
- Permission errors on `logs/` or `dags/` (Linux): `AIRFLOW_UID` must match `id -u`.
- Tasks stuck in queued: `docker compose logs airflow-worker` and check Redis password matches.
- Containers restarting with exit 137, or the Mac fans spinning: Docker Desktop memory is too low or a cap is too tight; check `docker stats`.
- Slow DAG parsing on Mac: confirm VirtioFS is enabled.
- Behind a corporate TLS proxy, `docker compose build` fails on pip SSL: add your CA to the
  image and set `PIP_CERT` before the `pip install` line.
