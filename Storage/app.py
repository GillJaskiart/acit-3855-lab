import functools
import json
import logging
import logging.config
import os
import random
import time
from datetime import datetime
from threading import Thread

import connexion
import yaml
from connexion import NoContent
from connexion.middleware import MiddlewarePosition
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from starlette.middleware.cors import CORSMiddleware

from models import CongestionCount, SpeedingViolation

with open("/config/storage_log_config.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("basicLogger")


with open("/config/storage_config.yml", "r") as f:
    app_config = yaml.safe_load(f.read())

ds = app_config["datastore"]
DB_URL = f'mysql+pymysql://{ds["user"]}:{ds["password"]}@{ds["hostname"]}:{ds["port"]}/{ds["db"]}'
ENGINE = create_engine(DB_URL)

kcfg = app_config["events"]
KAFKA_HOST = f"{kcfg['hostname']}:{kcfg['port']}"
TOPIC = kcfg["topic"]
CONSUMER_GROUP = kcfg.get("consumer_group", "event_group")


def make_session():
    return sessionmaker(bind=ENGINE)()


def parse_dt(value: str) -> datetime:
    """
    Parse ISO-8601 datetime strings from OpenAPI payloads.
    Handles trailing 'Z' (UTC).
    """
    if value is None:
        raise ValueError("Timestamp is required")
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
    logger.info(
        "Storage Kafka consumer starting. Broker=%s Topic=%s Group=%s",
        KAFKA_HOST,
        TOPIC,
        CONSUMER_GROUP,
    )

    while True:
        consumer = None

        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=[KAFKA_HOST],
                group_id=CONSUMER_GROUP,
                enable_auto_commit=False,
                auto_offset_reset="latest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )

            partitions = consumer.partitions_for_topic(TOPIC)
            if partitions is None:
                raise NoBrokersAvailable()

            logger.info("Storage consumer connected to %s for topic %s", KAFKA_HOST, TOPIC)

            for message in consumer:
                try:
                    msg = message.value
                    logger.info("Message received from Kafka: %s", msg)

                    payload = msg["payload"]
                    mtype = msg["type"]

                    if mtype == "speeding":
                        store_speeding_event(payload)
                    elif mtype == "congestion":
                        store_congestion_event(payload)
                    else:
                        logger.warning("Unknown message type '%s' - skipping", mtype)

                    consumer.commit()
                    logger.info("Committed Kafka offset (group=%s)", CONSUMER_GROUP)

                except Exception as e:
                    # Leave the offset uncommitted so failed messages can be retried.
                    logger.exception("Error processing Kafka message: %s", e)

        except (KafkaError, NoBrokersAvailable, OSError, ValueError) as e:
            logger.warning("Storage Kafka consumer connection failed: %s", e)
        except Exception as e:
            logger.exception("Unexpected storage consumer failure: %s", e)
        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception as err:
                    logger.warning("Error while closing storage consumer: %s", err)

            time.sleep(random.randint(500, 1500) / 1000)


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


def health():
    """Returns the health status of the Storage service."""
    return NoContent, 200


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
    base_path="/storage",
    strict_validation=True,
    validate_responses=True,
)

if __name__ == "__main__":
    setup_kafka_thread()
    app.run(port=8090, host="0.0.0.0")
