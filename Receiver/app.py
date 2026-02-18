import connexion
from connexion import NoContent
# import httpx
import uuid
import logging
import logging.config
import yaml
from kafka import KafkaProducer
import json


with open("log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("basicLogger")

with open("app_conf.yml", "r") as f:
    app_config = yaml.safe_load(f.read())


KAFKA_HOST = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
TOPIC = app_config["events"]["topic"]

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_HOST],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def _publish_event(event_type: str, event: dict, trace_id: str) -> int:
    """
    Publishes one event to Kafka topic 'events'
    Returns HTTP-like status code (201 success, 500 failure)
    """
    payload = {
        "type": event_type,
        "payload": event
    }

    try:
        # kafka-python uses send(), not produce()
        future = producer.send(TOPIC, payload)

        # Block until Kafka acknowledges (so you can log success/failure reliably)
        future.get(timeout=10)
        producer.flush()

        logger.info(f"Published event {event_type} to Kafka (trace_id: {trace_id})")
        return 201

    except Exception as e:
        logger.exception(
            f"Failed to publish event {event_type} to Kafka (trace_id: {trace_id}): {e}"
        )
        return 500

def receive_speeding_batch(body):
    """Receives a speeding reading batch event"""

    sender_id = body["sender_id"]
    location_id = body["location_id"]
    trace_id = str(uuid.uuid4())
    batch_timestamp = body["sent_timestamp"] 

    # status_code = 201  # default if nothing goes wrong

    for v in body.get("violations", []):
        storage_event = {
            "trace_id": trace_id,
            "sender_id": sender_id,
            "location_id": location_id,
            "batch_timestamp": batch_timestamp,
            "reading_timestamp": v["recorded_timestamp"],
            "speed_kmh": v["speed_kmh"],
            "speed_limit_kmh": v["speed_limit_kmh"],
            "direction": v.get("direction"),
        }

        logger.info(
            f"Received event speeding with a trace id of {trace_id}"
        )

        # r = httpx.post(SPEEDING_URL, json=storage_event, timeout=5.0)
        # status_code = r.status_code
      
        status_code = _publish_event("speeding", storage_event, trace_id)

        logger.info(
            f"Kafka publish result for speeding (trace_id: {trace_id}) status {status_code}"
        )


        # If Storage fails, stop immediately and return that failure code
        if status_code >= 400:
            return NoContent, status_code

    return NoContent, 201

def receive_congestion_batch(body):
    """Receives a congestion reading batch event"""

    sender_id = body["sender_id"]
    location_id = body["location_id"]
    trace_id = str(uuid.uuid4())
    batch_timestamp = body["sent_timestamp"]  # rename for Storage

    # status_code = 201

    for c in body.get("counts", []):
        storage_event = {
            "sender_id": sender_id,
            "location_id": location_id,
            "trace_id": trace_id,
            "batch_timestamp": batch_timestamp,
            "reading_timestamp": c["recorded_timestamp"],
            "vehicles_passing": c["vehicles_passing"],
            "interval_seconds": c["interval_seconds"],
            "direction": c["direction"],
        }

        logger.info(
            f"Received event congestion with a trace id of {trace_id}"
        )

        # r = httpx.post(CONGESTION_URL, json=storage_event, timeout=5.0)
        # status_code = r.status_code
        
        status_code = _publish_event("congestion", storage_event, trace_id)

        logger.info(
            f"Response for event congestion (trace_id: {trace_id}) has status {status_code}"
        )

        if status_code >= 400:
            return NoContent, status_code

    return NoContent, 201

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yml",
    strict_validation=True,
    validate_responses=True)

if __name__ == "__main__":
    app.run(port=8080, host="0.0.0.0")
