"""
Earthquake Kafka Consumer with Rolling Window and Cluster Visualization

- Consumes JSON messages from Kafka
- Each message includes location, magnitude, and time of earthquake
- Live plots a truncated US map showing:
  - Red dots for isolated earthquakes
  - Colored clusters for nearby quakes (3+ within 100km)
  - Semi-transparent circles around clusters
  - Rolling window of last N minutes (default: 30)
  - Inset map for Alaska
"""

import os
import json
import numpy as np
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
from cartopy import geodesic
import cartopy.feature as cfeature
from matplotlib.patches import Circle
from sklearn.cluster import DBSCAN
from matplotlib.colors import to_hex
from datetime import datetime, timedelta
from dotenv import load_dotenv
from utils.utils_logger import logger
from utils.utils_consumer import create_kafka_consumer
import matplotlib
from shapely.geometry import Polygon
from haversine import haversine, Unit

# Load .env settings
load_dotenv()

# Interactive plotting
plt.ion()

# Global quake list
quake_data = []

def get_kafka_topic() -> str:
    return os.getenv("PROJECT_TOPIC", "earthquake-topic")

def get_kafka_consumer_group_id() -> str:
    return os.getenv("BUZZ_CONSUMER_GROUP_ID", "eq_group")

# Rolling window duration (minutes)
ROLLING_MINUTES = 90

# Cluster proximity (km)
CLUSTER_RADIUS_KM = 200

# --- Initialize figure with main map + Alaska inset ---
fig = plt.figure(figsize=(12, 8))

# Main map: western/central US (cut off east of Iowa ~ -93°)
ax_main = fig.add_axes([0.05, 0.05, 0.9, 0.9], projection=ccrs.PlateCarree())
ax_main.set_extent([-130, -93, 24, 50], crs=ccrs.PlateCarree())

# Alaska inset floating in Pacific west of California
ax_ak = fig.add_axes([0.02, 0.02, 0.35, 0.35], projection=ccrs.PlateCarree())
ax_ak.set_extent([-170, -130, 52, 72], crs=ccrs.PlateCarree())

# Status text (on main map)
status_text = ax_main.text(
    0.01, 0.99,
    "",
    transform=ax_main.transAxes,
    verticalalignment='top',
    horizontalalignment='left',
    fontsize=10,
    bbox=dict(facecolor='white', alpha=0.7, edgecolor='none')
)

def draw_basemap(ax, title=None):
    ax.add_feature(cfeature.COASTLINE)
    ax.add_feature(cfeature.BORDERS, linestyle=':')
    ax.add_feature(cfeature.STATES, linestyle='-', edgecolor='gray')
    if title:
        ax.set_title(title, fontsize=12)

def plot_quakes_on_axis(ax, quakes, cluster_radius_km=100):
    if not quakes:
        return

    # --- DBSCAN clustering on haversine distance ---
    coords = np.array([[q["lat"], q["lon"]] for q in quakes])
    kms_per_radian = 6371.0088
    epsilon = cluster_radius_km / kms_per_radian
    coords_rad = np.radians(coords)

    db = DBSCAN(eps=epsilon, min_samples=2, algorithm='ball_tree', metric='haversine')
    labels = db.fit_predict(coords_rad)

    unique_labels = sorted(set(labels) - {-1})
    cmap = matplotlib.colormaps.get_cmap('tab10')
    cluster_colors = {
        label: to_hex(cmap(i / max(1, len(unique_labels) - 1)))
        for i, label in enumerate(unique_labels)
    }

    # --- Plot each quake ---
    for i, quake in enumerate(quakes):
        lon = quake["lon"]
        lat = quake["lat"]
        mag = quake["mag"]
        label = labels[i]
        color = 'red' if label == -1 else cluster_colors[label]

        ax.plot(
            lon, lat,
            marker='o',
            color=color,
            markersize=mag * 2,
            transform=ccrs.PlateCarree()
        )
        quake["cluster"] = label

    # --- Cluster circles (geodesic polygons) ---
    for label in unique_labels:
        members = [q for q in quakes if q["cluster"] == label]
        if len(members) >= 2:
            avg_lat = np.mean([q["lat"] for q in members])
            avg_lon = np.mean([q["lon"] for q in members])

            # Compute max radius needed to cover all points in cluster
            max_dist_km = max(
                haversine((avg_lat, avg_lon), (q["lat"], q["lon"]), unit=Unit.KILOMETERS)
                for q in members
            )
            max_dist_km *= 1.2  # add some buffer

            # Create geodesic circle polygon
            g = geodesic.Geodesic()
            circle_coords = g.circle(lon=avg_lon, lat=avg_lat, radius=max_dist_km * 1000)
            circle_geom = Polygon(circle_coords)  # ✅ shapely geometry

            ax.add_geometries(
                [circle_geom],
                crs=ccrs.PlateCarree(),
                facecolor=cluster_colors[label],
                edgecolor=cluster_colors[label],
                alpha=0.3
            )

def plot_earthquakes_with_clusters(quakes, cluster_radius_km=100, minutes_back=30):
    time_cutoff = datetime.utcnow() - timedelta(minutes=minutes_back)
    recent_quakes = [q for q in quakes if q["timestamp"] >= time_cutoff]
    if not recent_quakes:
        return

    # --- Clear axes ---
    ax_main.clear()
    ax_ak.clear()

    # --- Redraw basemaps ---
    ax_main.set_extent([-130, -93, 24, 50], crs=ccrs.PlateCarree())
    draw_basemap(ax_main, f"US West/Central Earthquakes (Last {minutes_back} Minutes)")

    ax_ak.set_extent([-170, -130, 52, 72], crs=ccrs.PlateCarree())
    draw_basemap(ax_ak, "Alaska")

    # --- Split quakes by region ---
    ak_quakes = [q for q in recent_quakes if q["lon"] < -130 and q["lat"] > 50]
    main_quakes = [q for q in recent_quakes if q not in ak_quakes]

    # --- Plot ---
    plot_quakes_on_axis(ax_main, main_quakes, cluster_radius_km)
    plot_quakes_on_axis(ax_ak, ak_quakes, cluster_radius_km)

    # --- Update status text ---
    last = recent_quakes[-1]
    stamp = last["timestamp"].strftime("%Y-%m-%d %H:%M:%S UTC")
    status = (f"Last quake: M {last['mag']:.1f} @ "
              f"({last['lat']:.2f}, {last['lon']:.2f})\n{stamp}")
    status_text.set_text(status)

    plt.draw()
    plt.pause(0.01)

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
