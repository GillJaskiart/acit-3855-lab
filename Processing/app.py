import os
import json
import logging
import logging.config
from datetime import datetime, timezone

import connexion
from connexion import NoContent
import httpx
import yaml
from apscheduler.schedulers.background import BackgroundScheduler


# Logging
with open("log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("basicLogger")


with open("app_conf.yml", "r") as f:
    app_config = yaml.safe_load(f.read())

# BASE_DIR = os.path.dirname(os.path.abspath(__file__)) 
# STATS_FILE = os.path.join(BASE_DIR, app_config["datastore"]["filename"])
# if this app.py is run from a different working directory, stats.json might get created somewhere random. This prevents it.

STATS_FILE = app_config["datastore"]["filename"]
SCHED_INTERVAL_SECONDS = int(app_config["scheduler"]["interval"])
SPEEDING_GET_URL = app_config["eventstores"]["speeding"]["url"]
CONGESTION_GET_URL = app_config["eventstores"]["congestion"]["url"]



def utc_now_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def read_stats_file():
    if not os.path.exists(STATS_FILE):
        return None
    with open(STATS_FILE, "r") as f:
        return json.load(f)


def write_stats_file(stats: dict):
    with open(STATS_FILE, "w") as f:
        json.dump(stats, f, indent=2)


def default_stats():
    # last_updated = epoch so first run grabs "everything"
    return {
        "num_speeding_events": 0,
        "min_speed_kmh": None,
        "max_speed_kmh": None,
        "num_congestion_events": 0,
        "max_vehicles_passing": None,
        "last_updated": "2026-01-01T00:00:00Z"
    }


def populate_stats():
    logger.info("Periodic processing started")

    stats = read_stats_file()
    if stats is None:
        stats = default_stats()

    start_ts = stats["last_updated"]
    end_ts = utc_now_z()

    params = {"start_timestamp": start_ts, "end_timestamp": end_ts}

    speeding_events = []
    congestion_events = []

    # --- Fetch speeding ---
    r1 = httpx.get(SPEEDING_GET_URL, params=params, timeout=5.0)
    if r1.status_code != 200:
        logger.error("Speeding GET failed with status %s", r1.status_code)
    else:
        speeding_events = r1.json()
        logger.info("Received %d speeding events", len(speeding_events))

    # --- Fetch congestion ---
    r2 = httpx.get(CONGESTION_GET_URL, params=params, timeout=5.0)
    if r2.status_code != 200:
        logger.error("Congestion GET failed with status %s", r2.status_code)
    else:
        congestion_events = r2.json()
        logger.info("Received %d congestion events", len(congestion_events))

    # --- Update stats ---
    if speeding_events:
        speeds = [float(e["speed_kmh"]) for e in speeding_events]
        stats["num_speeding_events"] += len(speeds)

        current_min = stats["min_speed_kmh"]
        current_max = stats["max_speed_kmh"]

        stats["min_speed_kmh"] = min(speeds) if current_min is None else min(current_min, min(speeds))
        stats["max_speed_kmh"] = max(speeds) if current_max is None else max(current_max, max(speeds))

    if congestion_events:
        counts = [int(e["vehicles_passing"]) for e in congestion_events]
        stats["num_congestion_events"] += len(counts)

        current_max_v = stats["max_vehicles_passing"]
        stats["max_vehicles_passing"] = max(counts) if current_max_v is None else max(current_max_v, max(counts))

    # Update last_updated to end_ts (most recent window end)
    stats["last_updated"] = end_ts

    write_stats_file(stats)

    logger.debug("Updated stats: %s", stats)
    logger.info("Periodic processing ended")


def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats, "interval", seconds=SCHED_INTERVAL_SECONDS)
    sched.start()


def get_stats():
    logger.info("Stats request received")

    stats = read_stats_file()
    if stats is None:
        logger.error("Statistics do not exist")
        return {"message": "Statistics do not exist"}, 404

    logger.debug("Stats content: %s", stats)
    logger.info("Stats request completed")
    return stats, 200


app = connexion.FlaskApp(__name__, specification_dir="")
app.add_api("openapi.yml", strict_validation=True, validate_responses=True)


if __name__ == "__main__":
    init_scheduler()
    app.run(port=8100)
