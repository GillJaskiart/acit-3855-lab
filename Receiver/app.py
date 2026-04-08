import connexion
from connexion import NoContent
import json
import logging
import logging.config
import random
import threading
import time
import uuid

import yaml
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable


with open("/config/receiver_log_config.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("basicLogger")

with open("/config/receiver_config.yml", "r") as f:
    app_config = yaml.safe_load(f.read())


KAFKA_HOST = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
TOPIC = app_config["events"]["topic"]


class KafkaProducerWrapper:
    """Keeps retrying until Kafka is available and reuses one producer."""

    def __init__(self, hostname: str, topic: str):
        self.hostname = hostname
        self.topic = topic
        self.producer = None
        self._lock = threading.RLock()
        self.connect()

    @staticmethod
    def _sleep_before_retry():
        time.sleep(random.randint(500, 1500) / 1000)

    def _reset_producer_locked(self):
        if self.producer is not None:
            try:
                self.producer.close()
            except Exception as err:
                logger.warning("Error while closing Kafka producer: %s", err)
            finally:
                self.producer = None

    def make_producer(self) -> bool:
        """
        Tries once to create a producer and confirm Kafka is reachable.
        Returns True on success, False on failure.
        """
        with self._lock:
            if self.producer is not None:
                return True

            temp_producer = None

            try:
                temp_producer = KafkaProducer(
                    bootstrap_servers=[self.hostname],
                    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                )

                partitions = temp_producer.partitions_for(self.topic)
                if partitions is None:
                    raise NoBrokersAvailable()

                self.producer = temp_producer
                logger.info("Kafka producer connected to %s for topic %s", self.hostname, self.topic)
                return True

            except (KafkaError, NoBrokersAvailable, OSError, ValueError) as err:
                logger.warning("Kafka producer connection failed: %s", err)
                if temp_producer is not None:
                    try:
                        temp_producer.close()
                    except Exception:
                        pass
                self.producer = None
                return False

    def connect(self):
        """Infinite loop that keeps retrying until Kafka becomes available."""
        while True:
            logger.debug("Trying to connect Receiver producer to Kafka...")
            if self.make_producer():
                return
            self._sleep_before_retry()

    def send(self, payload: dict):
        """
        Sends using the shared producer and reconnects if Kafka goes away.
        """
        while True:
            if self.producer is None:
                self.connect()

            with self._lock:
                try:
                    future = self.producer.send(self.topic, payload)
                    future.get(timeout=10)
                    self.producer.flush()
                    return
                except (KafkaError, OSError, AttributeError, ValueError) as err:
                    logger.warning("Kafka send failed, reconnecting producer: %s", err)
                    self._reset_producer_locked()

            self._sleep_before_retry()


producer_wrapper = KafkaProducerWrapper(KAFKA_HOST, TOPIC)


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
        producer_wrapper.send(payload)
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


def health():
    """Returns the health status of the Receiver service."""
    return NoContent, 200

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yml",
    strict_validation=True,
    validate_responses=True)

if __name__ == "__main__":
    app.run(port=8080, host="0.0.0.0")
