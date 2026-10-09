"""Load worker settings without embedding credentials in source code."""

from dataclasses import dataclass
import os
from typing import Mapping


@dataclass(frozen=True)
class WorkerConfig:
    redis_url: str
    redis_password: str | None
    logtail_token: str | None
    ingestion_host: str | None


def load_worker_config(environ: Mapping[str, str] | None = None) -> WorkerConfig:
    """Read the worker's required and optional settings from the environment."""
    source = os.environ if environ is None else environ

    redis_url = source.get("REDIS_URL", "").strip()
    if not redis_url:
        raise ValueError("REDIS_URL is required.")

    redis_password = source.get("REDIS_PASSWORD", "").strip() or None
    logtail_token = source.get("LOGTAIL_TOKEN", "").strip() or None
    ingestion_host = source.get("INGESTION_HOST", "").strip() or None

    if bool(logtail_token) != bool(ingestion_host):
        raise ValueError("Set both LOGTAIL_TOKEN and INGESTION_HOST, or leave both empty.")

    return WorkerConfig(
        redis_url=redis_url,
        redis_password=redis_password,
        logtail_token=logtail_token,
        ingestion_host=ingestion_host,
    )
