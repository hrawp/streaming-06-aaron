"""
earthquake_producer_aaron.py

Stream live earthquake data from USGS API to a file and Kafka topic.

Example JSON message:
{
    "id": "us7000l63p",
    "place": "74 km SW of Paredon, Mexico",
    "mag": 4.8,
    "time": "2025-09-30 21:10:12",
    "url": "https://earthquake.usgs.gov/earthquakes/eventpage/us7000l63p",
    "coordinates": [-94.4822, 15.3731, 35.0]
}
"""

#####################################
# Import Modules
#####################################

import json
import os
import time
import pathlib
import requests
from datetime import datetime
from dotenv import load_dotenv

# Import Kafka only if available
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

# Import logging utility
from utils.utils_logger import logger  # Make sure this exists

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Constants
#####################################

USGS_FEED_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"

#####################################
# Environment Variable Getters
#####################################

def get_message_interval() -> int:
    return int(os.getenv("PROJECT_INTERVAL_SECONDS", 30))

def get_kafka_topic() -> str:
    return os.getenv("PROJECT_TOPIC", "earthquake-events")

def get_kafka_server() -> str:
    return os.getenv("KAFKA_SERVER", "localhost:9092")

#####################################
# File Paths
#####################################

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DATA_FOLDER = PROJECT_ROOT.joinpath("data")
DATA_FOLDER.mkdir(parents=True, exist_ok=True)
DATA_FILE = DATA_FOLDER.joinpath("earthquake_live.json")

#####################################
# Earthquake Event Fetcher
#####################################

def fetch_earthquake_events():
    """
    Poll the USGS earthquake feed and yield new earthquake events.
    """
    seen_ids = set()

    while True:
        try:
            response = requests.get(USGS_FEED_URL)
            data = response.json()
            events = data.get("features", [])

            for event in events:
                quake_id = event.get("id")
                if quake_id not in seen_ids:
                    seen_ids.add(quake_id)

                    props = event.get("properties", {})
                    coords = event.get("geometry", {}).get("coordinates", [])

                    json_event = {
                        "id": quake_id,
                        "place": props.get("place"),
                        "mag": props.get("mag"),
                        "time": datetime.utcfromtimestamp(props.get("time") / 1000).strftime("%Y-%m-%d %H:%M:%S"),
                        "url": props.get("url"),
                        "coordinates": coords
                    }

                    yield json_event
        except Exception as e:
            logger.error(f"Error fetching earthquake data: {e}")

        time.sleep(get_message_interval())  # Polling delay

#####################################
# Main Function
#####################################

def main():
    logger.info("START earthquake producer...")
    interval_secs = get_message_interval()
    topic = get_kafka_topic()
    kafka_server = get_kafka_server()

    # Attempt to create Kafka producer
    producer = None
    if KAFKA_AVAILABLE:
        try:
            producer = KafkaProducer(
                bootstrap_servers=kafka_server,
                value_serializer=lambda x: json.dumps(x).encode("utf-8")
            )
            logger.info(f"Kafka producer connected to {kafka_server}")
        except Exception as e:
            logger.error(f"Kafka connection failed: {e}")
            producer = None

    try:
        for message in fetch_earthquake_events():
            logger.info(message)

            # Write to file
            with DATA_FILE.open("a") as f:
                f.write(json.dumps(message) + "\n")

            # Send to Kafka if available
            if producer:
                producer.send(topic, value=message)
                logger.info(f"Sent message to Kafka topic '{topic}': {message}")

    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        if producer:
            producer.close()
            logger.info("Kafka producer closed.")
        logger.info("Producer shutting down.")

#####################################
# Run if Main
#####################################

if __name__ == "__main__":
    main()
