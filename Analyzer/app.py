import json
import logging
import logging.config
import os
import random
import threading
import time

import yaml
import connexion
from connexion import NoContent
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

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

class KafkaAnalyzerWrapper:
    """Consumes Kafka once in the background and serves Analyzer reads from memory."""

    def __init__(self, hostname: str, topic: str, group_id: str):
        self.hostname = hostname
        self.topic = topic
        self.group_id = group_id
        self.consumer = None
        self._consumer_lock = threading.RLock()
        self._data_lock = threading.RLock()
        self._speeding_events = []
        self._congestion_events = []
        self._last_offsets = {}
        self.connect()
        self._consumer_thread = threading.Thread(
            target=self._consume_messages,
            name="analyzer-kafka-consumer",
            daemon=True,
        )
        self._consumer_thread.start()

    @staticmethod
    def _sleep_before_retry():
        time.sleep(random.randint(500, 1500) / 1000)

    def _reset_consumer_locked(self):
        if self.consumer is not None:
            try:
                self.consumer.close()
            except Exception as err:
                logger.warning("Error while closing Analyzer consumer: %s", err)
            finally:
                self.consumer = None

    def make_consumer(self) -> bool:
        """Tries once to create a reusable consumer and validate topic access."""
        with self._consumer_lock:
            if self.consumer is not None:
                return True

            temp_consumer = None

            try:
                temp_consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=[self.hostname],
                    group_id=self.group_id,
                    enable_auto_commit=False,
                    auto_offset_reset="earliest",
                    consumer_timeout_ms=1000,
                    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
                )

                partitions = temp_consumer.partitions_for_topic(self.topic)
                if partitions is None:
                    raise NoBrokersAvailable()

                self.consumer = temp_consumer
                logger.info(
                    "Analyzer consumer connected to %s for topic %s",
                    self.hostname,
                    self.topic,
                )
                return True

            except (KafkaError, NoBrokersAvailable, OSError, ValueError) as err:
                logger.warning("Analyzer Kafka consumer connection failed: %s", err)
                if temp_consumer is not None:
                    try:
                        temp_consumer.close()
                    except Exception:
                        pass
                self.consumer = None
                return False

    def connect(self):
        """Infinite loop that keeps retrying until Kafka becomes available."""
        while True:
            logger.debug("Trying to connect Analyzer consumer to Kafka...")
            if self.make_consumer():
                return
            self._sleep_before_retry()

    def _record_message(self, message):
        msg = message.value
        message_key = (message.topic, message.partition)

        with self._data_lock:
            last_offset = self._last_offsets.get(message_key, -1)
            if message.offset <= last_offset:
                return

            self._last_offsets[message_key] = message.offset

            payload = msg.get("payload")
            message_type = msg.get("type")

            if message_type == "speeding":
                self._speeding_events.append(payload)
            elif message_type == "congestion":
                self._congestion_events.append(payload)
            else:
                logger.warning("Analyzer received unknown Kafka message type '%s'", message_type)

    def _consume_messages(self):
        """Continuously consumes Kafka once and keeps the in-memory cache warm."""
        while True:
            if self.consumer is None:
                self.connect()

            try:
                for message in self.consumer:
                    self._record_message(message)
            except (KafkaError, OSError, AttributeError, ValueError) as err:
                logger.warning("Analyzer consumer loop failed, reconnecting: %s", err)
                with self._consumer_lock:
                    self._reset_consumer_locked()
                self._sleep_before_retry()
            except Exception as err:
                logger.exception("Unexpected Analyzer consumer failure: %s", err)
                with self._consumer_lock:
                    self._reset_consumer_locked()
                self._sleep_before_retry()

    def get_stats(self):
        with self._data_lock:
            return {
                "num_speeding_events": len(self._speeding_events),
                "num_congestion_events": len(self._congestion_events),
            }

    def get_event_payload(self, event_type: str, target_index: int):
        with self._data_lock:
            events = self._speeding_events if event_type == "speeding" else self._congestion_events

            if target_index >= len(events):
                logger.info("No %s event at index=%d (found %d total)", event_type, target_index, len(events))
                return None

            payload = events[target_index]
            return dict(payload) if isinstance(payload, dict) else payload


analyzer_wrapper = KafkaAnalyzerWrapper(KAFKA_HOST, TOPIC, CONSUMER_GROUP)


# API Handlers

def get_speeding_event(index):
    idx, err = _validate_index(index)
    if err:
        return err

    payload = analyzer_wrapper.get_event_payload("speeding", idx)
    if payload is None:
        return {"message": f"No speeding event at index {idx}!"}, 404

    logger.info("Found speeding event at index=%d", idx)
    return payload, 200


def get_congestion_event(index):
    idx, err = _validate_index(index)
    if err:
        return err

    payload = analyzer_wrapper.get_event_payload("congestion", idx)
    if payload is None:
        return {"message": f"No congestion event at index {idx}!"}, 404

    logger.info("Found congestion event at index=%d", idx)
    return payload, 200


def get_stats():
    stats = analyzer_wrapper.get_stats()
    logger.info(
        "Analyzer stats served from cache: speeding=%d congestion=%d",
        stats["num_speeding_events"],
        stats["num_congestion_events"],
    )
    return stats, 200


def health():
    """Returns the health status of the Analyzer service."""
    return NoContent, 200


app = connexion.FlaskApp(__name__, specification_dir='')

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
    base_path="/analyzer",
    strict_validation=True,
    validate_responses=True,
)

if __name__ == "__main__":
    app.run(port=8110, host="0.0.0.0")
