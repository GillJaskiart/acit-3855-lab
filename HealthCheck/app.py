import json
import logging
import logging.config
import os
from datetime import datetime, timezone

import connexion
from connexion import NoContent
import httpx
import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware


with open("/config/health_check_log_config.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("basicLogger")


with open("/config/health_check_config.yml", "r") as f:
    app_config = yaml.safe_load(f.read())


STATUS_FILE = app_config["datastore"]["filename"]
os.makedirs(os.path.dirname(STATUS_FILE), exist_ok=True)

SERVER_PORT = int(app_config["server"]["port"])
SCHED_INTERVAL_SECONDS = int(app_config["scheduler"]["interval"])
REQUEST_TIMEOUT_SECONDS = float(app_config["scheduler"]["timeout"])
API_ENDPOINTS = app_config["api"]
SERVICE_ENDPOINTS = app_config["services"]


def utc_now_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def default_statuses():
    statuses = {service_name: "Unknown" for service_name in SERVICE_ENDPOINTS}
    statuses["last_update"] = "2026-01-01T00:00:00Z"
    return statuses


def read_status_file():
    if not os.path.exists(STATUS_FILE):
        return None

    try:
        with open(STATUS_FILE, "r") as status_file:
            content = status_file.read().strip()
            if not content:
                return None
            return json.loads(content)
    except (json.JSONDecodeError, OSError) as err:
        logger.warning("Status file missing/empty/invalid. Reinitializing. Error: %s", err)
        return None


def write_status_file(statuses: dict):
    with open(STATUS_FILE, "w") as status_file:
        json.dump(statuses, status_file, indent=2)


def check_service(service_name: str, service_config: dict):
    url = service_config["url"]
    status = "Down"

    try:
        response = httpx.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        if response.status_code == 200:
            status = "Up"
        else:
            logger.warning(
                "Health check for %s returned status code %s",
                service_name,
                response.status_code
            )
    except httpx.RequestError as err:
        logger.warning("Health check request failed for %s: %s", service_name, err)

    return status


def refresh_service_statuses():
    logger.info("Periodic health check started")
    statuses = read_status_file()
    if statuses is None:
        statuses = default_statuses()

    checked_at = utc_now_z()

    for service_name, service_config in SERVICE_ENDPOINTS.items():
        status = check_service(service_name, service_config)
        statuses[service_name] = status
        logger.info(
            "Recorded %s service status as %s at %s",
            service_name,
            status,
            checked_at
        )

    statuses["last_update"] = checked_at
    write_status_file(statuses)

    logger.debug("Updated health statuses: %s", statuses)
    logger.info("Periodic health check ended")


def get_service_statuses():
    logger.info(
        "Service health status request received at %s",
        API_ENDPOINTS["status_path"]
    )

    statuses = read_status_file()
    if statuses is None:
        logger.error("Health statuses do not exist")
        return {"message": "Health statuses do not exist"}, 404

    logger.debug("Returning service health status: %s", statuses)
    logger.info("Service health status request completed")
    return statuses, 200


def health():
    """Returns the health status of the Health Check service."""
    logger.debug("Health endpoint accessed at %s", API_ENDPOINTS["health_path"])
    return NoContent, 200


def init_scheduler():
    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(
        refresh_service_statuses,
        "interval",
        seconds=SCHED_INTERVAL_SECONDS,
        max_instances=1
    )
    scheduler.start()
    return scheduler


app = connexion.FlaskApp(__name__, specification_dir="")

if os.environ.get("CORS_ALLOW_ALL") == "yes":
    app.add_middleware(
        CORSMiddleware,
        position=MiddlewarePosition.BEFORE_EXCEPTION,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

app.add_api(
    "openapi.yml",
    base_path="/health-check",
    strict_validation=True,
    validate_responses=True,
)


if __name__ == "__main__":
    refresh_service_statuses()
    init_scheduler()
    app.run(port=SERVER_PORT, host="0.0.0.0")
