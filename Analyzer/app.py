import json
import logging
import logging.config
import yaml
import connexion
from connexion import NoContent
from kafka import KafkaConsumer

from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware

with open("/config/analyzer_log_config.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("basicLogger")


with open("/config/analyzer_config.yml", "r") as f:
    app_config = yaml.safe_load(f.read())


# Kafka config
kcfg = app_config["events"]
KAFKA_HOST = f"{kcfg['hostname']}:{kcfg['port']}"
TOPIC = kcfg["topic"]
CONSUMER_GROUP = bcfg_group = kcfg.get("analyzer_group", "analyzer_group")


def _validate_index(index):
    try:
        idx = int(index)
        if idx < 0:
            raise ValueError()
        return idx, None
    except Exception:
        return None, ({"message": "index must be an integer >= 0"}, 400)

def _new_consumer_from_beginning():
    """
    scans from the beginning.
    Kafka is treated like an indexable queue; implement by scanning.
    """
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_HOST],
        group_id=CONSUMER_GROUP,
        enable_auto_commit=False,          # do not commit; Analyzer is read-only
        auto_offset_reset="earliest",      # start at beginning to support index lookups
        consumer_timeout_ms=1000,          # stop iteration if no more messages
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    return consumer


def _find_nth_event_payload(event_type: str, target_index: int):
    """
    Scan the mixed topic and return the payload for the Nth event of given type.
    Index is per-type (speeding index counts only speeding messages).
    """
    consumer = _new_consumer_from_beginning()
    count_for_type = 0

    logger.debug("Searching for type=%s index=%d on topic=%s", event_type, target_index, TOPIC)

    try:
        for message in consumer:
            msg = message.value  # dict: {"type": "...", "payload": {...}}
            mtype = msg.get("type")
            if mtype != event_type:
                continue

            if count_for_type == target_index:
                payload = msg.get("payload")
                logger.info("Found %s event at index=%d", event_type, target_index)
                return payload, 200

            count_for_type += 1

        # If we get here, we hit consumer_timeout_ms with no more messages
        logger.info("No %s event at index=%d (found %d total)", event_type, target_index, count_for_type)
        return {"message": f"No {event_type} event at index {target_index}!"}, 404

    except Exception as e:
        logger.exception("Error scanning Kafka for %s index=%d: %s", event_type, target_index, e)
        return {"message": "Error reading from Kafka"}, 500

    finally:
        consumer.close()

def _count_events():
    """
    Scan topic and count events by type.
    """
    consumer = _new_consumer_from_beginning()
    num_speeding = 0
    num_congestion = 0

    try:
        for message in consumer:
            msg = message.value
            mtype = msg.get("type")
            if mtype == "speeding":
                num_speeding += 1
            elif mtype == "congestion":
                num_congestion += 1

        logger.info("Stats scan finished: speeding=%d congestion=%d", num_speeding, num_congestion)
        return {"num_speeding_events": num_speeding, "num_congestion_events": num_congestion}, 200

    except Exception as e:
        logger.exception("Error counting Kafka events: %s", e)
        return {"message": "Error reading from Kafka"}, 500

    finally:
        consumer.close()


# API Handlers

def get_speeding_event(index):
    idx, err = _validate_index(index)
    if err:
        return err
    return _find_nth_event_payload("speeding", idx)


def get_congestion_event(index):
    idx, err = _validate_index(index)
    if err:
        return err
    return _find_nth_event_payload("congestion", idx)


def get_stats():
    return _count_events()


def health():
    """Returns the health status of the Analyzer service."""
    return NoContent, 200


app = connexion.FlaskApp(__name__, specification_dir='')

app.add_middleware(
    CORSMiddleware,
    position=MiddlewarePosition.BEFORE_EXCEPTION,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_api("openapi.yml",
    strict_validation=True,
    validate_responses=True)

if __name__ == "__main__":
    app.run(port=8110, host="0.0.0.0")
