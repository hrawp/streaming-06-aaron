#####################################
# Earthquake Kafka Consumer
#####################################

import os
import json
from dotenv import load_dotenv

# Local utility functions
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

load_dotenv()

def get_kafka_topic() -> str:
    topic = os.getenv("PROJECT_TOPIC", "earthquake-events")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_consumer_group_id() -> str:
    group_id = os.getenv("EARTHQUAKE_CONSUMER_GROUP_ID", "earthquake-group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id

def process_message(message: str) -> None:
    """
    Process a single earthquake message from Kafka.
    """
    try:
        message_dict = json.loads(message)
        if isinstance(message_dict, dict):
            quake_id = message_dict.get("id", "unknown")
            place = message_dict.get("place", "unknown")
            mag = message_dict.get("mag", "N/A")
            time = message_dict.get("time", "unknown")
            logger.info(f"Earthquake ID: {quake_id}")
            logger.info(f"  Location: {place}")
            logger.info(f"  Magnitude: {mag}")
            logger.info(f"  Time: {time}")
        else:
            logger.error(f"Expected a dict, got: {type(message_dict)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def main() -> None:
    logger.info("START earthquake consumer")

    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    consumer = create_kafka_consumer(topic, group_id)

    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Kafka message at offset {message.offset}: {message_str}")
            process_message(message_str)

    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info("END earthquake consumer")

if __name__ == "__main__":
    main()
