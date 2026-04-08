import logging
import logging.config
import os
from datetime import datetime, timezone

import connexion
from connexion import NoContent
import httpx
import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from models import Base, ServiceStatus, dt_to_iso_z


with open("/config/health_check_log_config.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("basicLogger")


with open("/config/health_check_config.yml", "r") as f:
    app_config = yaml.safe_load(f.read())


DATASTORE_FILE = app_config["datastore"]["filename"]
os.makedirs(os.path.dirname(DATASTORE_FILE), exist_ok=True)

ENGINE = create_engine(
    f"sqlite:///{DATASTORE_FILE}",
    connect_args={"check_same_thread": False}
)
SESSION_MAKER = sessionmaker(bind=ENGINE, expire_on_commit=False)

SERVER_PORT = int(app_config["server"]["port"])
SCHED_INTERVAL_SECONDS = int(app_config["scheduler"]["interval"])
REQUEST_TIMEOUT_SECONDS = float(app_config["scheduler"]["timeout"])
API_ENDPOINTS = app_config["api"]
SERVICE_ENDPOINTS = app_config["services"]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def init_database():
    Base.metadata.create_all(ENGINE)


def make_session():
    return SESSION_MAKER()


def record_status(session, service_name: str, status: str, checked_at: datetime):
    service_status = session.get(ServiceStatus, service_name)

    if service_status is None:
        service_status = ServiceStatus(
            service_name=service_name,
            status=status,
            last_checked=checked_at
        )
        session.add(service_status)
    else:
        service_status.status = status
        service_status.last_checked = checked_at

    session.commit()

    logger.info(
        "Recorded %s service status as %s at %s",
        service_name,
        status,
        dt_to_iso_z(checked_at)
    )


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

    checked_at = utc_now()
    return status, checked_at


def refresh_service_statuses():
    logger.info("Starting service health poll")
    session = make_session()

    try:
        for service_name, service_config in SERVICE_ENDPOINTS.items():
            status, checked_at = check_service(service_name, service_config)
            record_status(session, service_name, status, checked_at)
    finally:
        session.close()

    logger.info("Completed service health poll")


def get_service_statuses():
    logger.info(
        "Service health status request received at %s",
        API_ENDPOINTS["status_path"]
    )

    session = make_session()
    try:
        statement = select(ServiceStatus)
        rows = {
            row.service_name: row
            for row in session.execute(statement).scalars().all()
        }

        response = {}
        latest_update = None

        for service_name in SERVICE_ENDPOINTS:
            row = rows.get(service_name)
            response[service_name] = row.status if row else "Unknown"

            if row and (latest_update is None or row.last_checked > latest_update):
                latest_update = row.last_checked

        response["last_update"] = dt_to_iso_z(latest_update) or dt_to_iso_z(utc_now())

        logger.debug("Returning service health status: %s", response)
        logger.info("Service health status request completed")
        return response, 200
    finally:
        session.close()


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
app.add_api("openapi.yml", strict_validation=True, validate_responses=True)


if __name__ == "__main__":
    init_database()
    refresh_service_statuses()
    init_scheduler()
    app.run(port=SERVER_PORT, host="0.0.0.0")
