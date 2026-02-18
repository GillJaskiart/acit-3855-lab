import functools
from datetime import datetime
import connexion
from connexion import NoContent

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import logging
import logging.config
import yaml
from sqlalchemy import select


from models import SpeedingViolation, CongestionCount

with open("log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("basicLogger")



with open("app_conf.yml", "r") as f:
    app_config = yaml.safe_load(f.read())

ds = app_config["datastore"]
DB_URL = f'mysql+pymysql://{ds["user"]}:{ds["password"]}@{ds["hostname"]}:{ds["port"]}/{ds["db"]}'
ENGINE = create_engine(DB_URL)



def make_session():
    return sessionmaker(bind=ENGINE)()


def parse_dt(value: str) -> datetime:
    """
    Parse ISO-8601 datetime strings from OpenAPI payloads.
    Handles trailing 'Z' (UTC).
    """
    if value is None:
        raise ValueError("Timestamp is required")
    # "2026-01-09T08:00:00Z" -> "+00:00"
    if isinstance(value, str) and value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def use_db_session(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        session = make_session()
        try:
            return func(session, *args, **kwargs)
        finally:
            session.close()
    return wrapper



@use_db_session
def receive_speeding_batch(session, body):
    """Receives a speeding reading batch event"""


    event = SpeedingViolation(
        sender_id=body["sender_id"],
        location_id=body["location_id"],
        trace_id=body["trace_id"],
        batch_timestamp=parse_dt(body["batch_timestamp"]),
        reading_timestamp=parse_dt(body["reading_timestamp"]),
        speed_kmh=float(body["speed_kmh"]),
        speed_limit_kmh=float(body["speed_limit_kmh"]),
        direction=body.get("direction"),
    )

    session.add(event)
    session.commit()

    logger.debug(
        f"Stored event speeding with a trace id of {body['trace_id']}"
    )
    return NoContent, 201


@use_db_session
def receive_congestion_batch(session, body):
    """
    Receives ONE congestion count event and stores it in DB.
    """
    event = CongestionCount(
        sender_id=body["sender_id"],
        location_id=body["location_id"],
        trace_id=body["trace_id"],
        batch_timestamp=parse_dt(body["batch_timestamp"]),
        reading_timestamp=parse_dt(body["reading_timestamp"]),
        vehicles_passing=int(body["vehicles_passing"]),
        interval_seconds=int(body["interval_seconds"]),
        direction=body["direction"],
    )

    session.add(event)
    session.commit()

    logger.debug(
        f"Stored event congestion with a trace id of {body['trace_id']}"
    )

    return NoContent, 201


@use_db_session
def get_speeding_events(session, start_timestamp, end_timestamp):
    try:
        start = parse_dt(start_timestamp)
        end = parse_dt(end_timestamp)
    except Exception:
        return NoContent, 400
    
    logger.debug("Query speeding: %s to %s", start, end)

    statement = (
        select(SpeedingViolation)
        .where(SpeedingViolation.date_created >= start)
        .where(SpeedingViolation.date_created < end)
        .order_by(SpeedingViolation.date_created)
    )

    results = [row.to_dict() for row in session.execute(statement).scalars().all()]
    logger.debug("Found %d speeding events", len(results))
    return results, 200


@use_db_session
def get_congestion_events(session, start_timestamp, end_timestamp):
    try:
        start = parse_dt(start_timestamp)
        end = parse_dt(end_timestamp)
    except Exception:
        return NoContent, 400
    
    logger.debug("Query congestion: %s to %s", start, end)

    statement = (
        select(CongestionCount)
        .where(CongestionCount.date_created >= start)
        .where(CongestionCount.date_created < end)
        .order_by(CongestionCount.date_created)
    )

    results = [row.to_dict() for row in session.execute(statement).scalars().all()]
    logger.debug("Found %d congestion events", len(results))
    return results, 200



app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yml",
    strict_validation=True,
    validate_responses=True)

if __name__ == "__main__":
    app.run(port=8090)
