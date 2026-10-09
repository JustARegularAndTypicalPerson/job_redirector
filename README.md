# Job Redirector

A Python worker system that dispatches business-listing jobs through Redis and browser automation. The code includes integrations for Yandex Business and 2GIS workflows, with queue state, worker tracking, logging, and dead-letter handling.

## Project structure

- `worker.py` polls Redis queues and manages job state.
- `job_runner.py` routes a job to its provider-specific implementation.
- `yandex_redirector.py` and `gis_redirector.py` contain provider workflows.
- `scrapers/` contains browser-facing page parsing and interaction logic.
- `worker_config.py` validates required and optional environment settings.

## Setup

Use Python 3.10 or newer and a Redis instance you are authorized to use.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m playwright install chromium firefox
Copy-Item .env.example .env
```

Edit `.env` and set `REDIS_URL`. If Redis requires a password, set `REDIS_PASSWORD`. External log shipping is optional; set both `LOGTAIL_TOKEN` and `INGESTION_HOST`, or leave them both empty.

Start the worker with:

```powershell
python worker.py
```

The worker exits if `REDIS_URL` is missing or Redis cannot be reached. Do not point it at production queues while evaluating the project.

## Checks

Run the configuration tests with:

```powershell
python -m unittest discover -s tests -v
```

These tests cover configuration parsing only. They do not connect to Redis or verify external portals.

## Safety and limitations

- Browser automation can read and change business-account data, including review responses and uploaded content. Use only accounts and data for which you have explicit authorization, and follow the relevant platform rules.
- Browser sessions and organization data are local runtime state. Keep them out of version control; the repository ignores `.env` files and the known profile-data directories.
- Portal selectors and flows can break when provider pages change. Live Yandex Business and 2GIS workflows have not been verified by this portfolio cleanup.
- No production usage metrics or outcomes are claimed.
- This repository does not currently specify a software license.
