import functools
from datetime import datetime
import connexion
from connexion import NoContent

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
import logging
import logging.config
import yaml
import json
from threading import Thread

from kafka import KafkaConsumer


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

# Kafka config (add this section to app_conf.yml)
kcfg = app_config["events"]
KAFKA_HOST = f"{kcfg['hostname']}:{kcfg['port']}"
TOPIC = kcfg["topic"]
CONSUMER_GROUP = bcfg_group = kcfg.get("consumer_group", "event_group")  # default group



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
def store_speeding_event(session, body: dict):
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
    logger.debug("Stored speeding event trace_id=%s", body["trace_id"])


@use_db_session
def store_congestion_event(session, body: dict):
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
    logger.debug("Stored congestion event trace_id=%s", body["trace_id"])


def process_messages():
    """
    Kafka consumer loop:
    - connects to Kafka
    - blocks waiting for messages
    - stores payload to DB based on msg["type"]
    - commits offsets only AFTER successful DB commit
    """
    logger.info("Storage Kafka consumer starting. Broker=%s Topic=%s Group=%s",
                KAFKA_HOST, TOPIC, CONSUMER_GROUP)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_HOST],
        group_id=CONSUMER_GROUP,
        enable_auto_commit=False,      # we commit manually after DB write
        auto_offset_reset="latest",    # match lab intent (do not replay old)
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    for message in consumer:
        try:
            msg = message.value  # already deserialized dict
            logger.info("Message received from Kafka: %s", msg)

            payload = msg["payload"]
            mtype = msg["type"]

            if mtype == "speeding":
                store_speeding_event(payload)
            elif mtype == "congestion":
                store_congestion_event(payload)
            else:
                logger.warning("Unknown message type '%s' - skipping", mtype)
                # If you skip unknown types, you probably still want to commit
                # so you don't get stuck re-reading them forever.

            consumer.commit()
            logger.info("Committed Kafka offset (group=%s)", CONSUMER_GROUP)

        except Exception as e:
            # Don't commit on failure — you WANT to retry this message
            logger.exception("Error processing Kafka message: %s", e)


def setup_kafka_thread():
    t1 = Thread(target=process_messages)
    t1.daemon = True
    t1.start()
    logger.info("Kafka consumer thread started")



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
