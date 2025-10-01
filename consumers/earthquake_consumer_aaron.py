"""
Earthquake Kafka Consumer with Rolling Window and Cluster Visualization

- Consumes JSON messages from Kafka
- Each message includes location, magnitude, and time of earthquake
- Live plots a US map showing:
  - Red dots for isolated earthquakes
  - Colored clusters for nearby quakes (3+ within 100km)
  - Semi-transparent circles around clusters
  - Rolling window of last N minutes (default: 30)
"""

import os
import json
import numpy as np
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from matplotlib.patches import Circle
from sklearn.cluster import DBSCAN
from matplotlib.colors import to_hex
from collections import defaultdict
from datetime import datetime, timedelta
from dotenv import load_dotenv
from utils.utils_logger import logger
from utils.utils_consumer import create_kafka_consumer
import matplotlib.cm as cm

# Load .env settings
load_dotenv()

# Initialize plot
plt.ion()

# Global quake list
quake_data = []

# Get environment variables
def get_kafka_topic() -> str:
    return os.getenv("PROJECT_TOPIC", "earthquake-topic")

def get_kafka_consumer_group_id() -> str:
    return os.getenv("BUZZ_CONSUMER_GROUP_ID", "eq_group")

# Rolling window duration (minutes)
ROLLING_MINUTES = 30

# Cluster proximity (km)
CLUSTER_RADIUS_KM = 100


def plot_earthquakes_with_clusters(quakes, cluster_radius_km=100, minutes_back=30):
    # Filter to recent quakes
    time_cutoff = datetime.utcnow() - timedelta(minutes=minutes_back)
    recent_quakes = [q for q in quakes if q["timestamp"] >= time_cutoff]
    if not recent_quakes:
        return

    # Prepare map
    fig, ax = plt.subplots(figsize=(10, 7), subplot_kw={'projection': ccrs.PlateCarree()})
    ax.set_extent([-130, -65, 24, 50], crs=ccrs.PlateCarree())
    ax.add_feature(cfeature.COASTLINE)
    ax.add_feature(cfeature.BORDERS, linestyle=':')
    ax.add_feature(cfeature.STATES, linestyle='-', edgecolor='gray')
    ax.set_title(f"US Earthquakes (Last {minutes_back} Minutes)", fontsize=14)

    # Coordinates
    coords = np.array([[q["lat"], q["lon"]] for q in recent_quakes])
    kms_per_radian = 6371.0088
    epsilon = cluster_radius_km / kms_per_radian
    coords_rad = np.radians(coords)

    db = DBSCAN(eps=epsilon, min_samples=3, algorithm='ball_tree', metric='haversine')
    labels = db.fit_predict(coords_rad)

    # Color each cluster
    unique_labels = sorted(set(labels) - {-1})
    cmap = cm.get_cmap('tab10', len(unique_labels))
    cluster_colors = {label: to_hex(cmap(i)) for i, label in enumerate(unique_labels)}

    for i, quake in enumerate(recent_quakes):
        lon = quake["lon"]
        lat = quake["lat"]
        mag = quake["mag"]
        label = labels[i]

        color = 'red' if label == -1 else cluster_colors[label]

        ax.plot(lon, lat, marker='o', color=color, markersize=mag * 2, transform=ccrs.PlateCarree())
        ax.text(lon + 0.3, lat + 0.2, f"M {mag}", fontsize=8, transform=ccrs.PlateCarree())

        quake["cluster"] = label

    for label in unique_labels:
        members = [q for q in recent_quakes if q["cluster"] == label]
        if len(members) >= 3:
            avg_lat = np.mean([q["lat"] for q in members])
            avg_lon = np.mean([q["lon"] for q in members])
            circle = Circle(
                (avg_lon, avg_lat),
                radius=cluster_radius_km * 1000,
                facecolor=cluster_colors[label],
                edgecolor=cluster_colors[label],
                alpha=0.2,
                transform=ccrs.PlateCarree()
            )
            ax.add_patch(circle)

    plt.tight_layout()
    plt.draw()
    plt.pause(0.01)
    plt.clf()


def process_message(message: str) -> None:
    try:
        message_dict = json.loads(message)
        coords = message_dict.get("coordinates", None)
        mag = message_dict.get("mag", None)

        if coords and mag:
            quake = {
                "lon": coords[0],
                "lat": coords[1],
                "mag": mag,
                "place": message_dict.get("place", ""),
                "timestamp": datetime.utcnow()
            }
            quake_data.append(quake)
            logger.info(f"New quake: {quake}")
            plot_earthquakes_with_clusters(quake_data,
                                           cluster_radius_km=CLUSTER_RADIUS_KM,
                                           minutes_back=ROLLING_MINUTES)
        else:
            logger.warning("Missing coordinates or magnitude in message.")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


def main():
    logger.info("START earthquake consumer")
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()

    consumer = create_kafka_consumer(topic, group_id)
    logger.info(f"Consuming from topic '{topic}' in group '{group_id}'...")

    try:
        for msg in consumer:
            message_str = msg.value
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted.")
    except Exception as e:
        logger.error(f"Error while consuming: {e}")
    finally:
        consumer.close()
        logger.info("Consumer closed.")


if __name__ == "__main__":
    main()
    plt.ioff()
    plt.show()
