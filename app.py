import os
import time
import random
import logging

from faker import Faker
from logtail import LogtailHandler

#
# 1. LOGTAIL (BETTER STACK) SETUP
#
source_token = os.environ.get("LOGTAIL_SOURCE_TOKEN")
if not source_token:
    raise ValueError("LOGTAIL_SOURCE_TOKEN env var not found! "
                     "Set it via `heroku config:set LOGTAIL_SOURCE_TOKEN=xxxx`")

logtail_handler = LogtailHandler(source_token=source_token)
logger = logging.getLogger("UltraDetailedDataStorage")
logger.setLevel(logging.INFO)

# Remove any default console handlers so we only log to Better Stack
logger.handlers = []
logger.addHandler(logtail_handler)

#
# 2. SCENARIO DATA
#
fake = Faker()

# Example "data centers" or "regions"
regions = ["us-east-1", "us-west-2", "eu-central-1", "ap-northeast-1"]

# Some nodes in various clusters
nodes = [
    {"id": "node1", "cluster": "clusterA", "ip": "10.0.0.1", "region": "us-east-1"},
    {"id": "node2", "cluster": "clusterA", "ip": "10.0.0.2", "region": "us-east-1"},
    {"id": "node3", "cluster": "clusterB", "ip": "10.1.0.1", "region": "us-west-2"},
    {"id": "node4", "cluster": "clusterB", "ip": "10.1.0.2", "region": "us-west-2"},
    {"id": "node5", "cluster": "clusterC", "ip": "10.2.0.1", "region": "eu-central-1"},
]

# Volumes with capacity, cluster ownership, etc.
volumes = [
    {"id": "vol-100", "cluster": "clusterA", "capacity_gb": 500},
    {"id": "vol-101", "cluster": "clusterA", "capacity_gb": 1000},
    {"id": "vol-200", "cluster": "clusterB", "capacity_gb": 250},
    {"id": "vol-201", "cluster": "clusterB", "capacity_gb": 750},
    {"id": "vol-300", "cluster": "clusterC", "capacity_gb": 2000},
]

# Weighted event definitions: (EVENT_NAME, LOG_LEVEL, WEIGHT)
EVENTS = [
    ("NODE_JOINED", logging.INFO, 0.05),
    ("NODE_LEFT", logging.WARNING, 0.02),
    ("NODE_OFFLINE", logging.ERROR, 0.03),
    ("VOLUME_CREATED", logging.INFO, 0.07),
    ("VOLUME_EXPANDED", logging.INFO, 0.05),
    ("VOLUME_DELETED", logging.WARNING, 0.03),
    ("SNAPSHOT_CREATED", logging.INFO, 0.08),
    ("SNAPSHOT_RESTORED", logging.INFO, 0.06),
    ("BACKUP_COMPLETED", logging.INFO, 0.08),
    ("BACKUP_FAILED", logging.ERROR, 0.03),
    ("DATA_READ", logging.INFO, 0.15),
    ("DATA_WRITE", logging.INFO, 0.18),
    ("DATA_REPLICATION", logging.INFO, 0.05),
    ("DATA_REPLICATION_FAILED", logging.ERROR, 0.02),
    ("ALERT_CAPACITY", logging.ERROR, 0.02),
    ("ALERT_PERFORMANCE", logging.WARNING, 0.02),
    ("MAINTENANCE_MODE_ENABLED", logging.WARNING, 0.01),
    ("MAINTENANCE_MODE_DISABLED", logging.INFO, 0.01),
    # Add more if you like
]

def weighted_choice(events):
    """
    Choose an event from EVENTS based on the 'weight' field.
    """
    total_weight = sum(e[2] for e in events)
    r = random.random() * total_weight
    cumulative = 0.0
    for (name, lvl, weight) in events:
        cumulative += weight
        if r < cumulative:
            return name, lvl
    # fallback (should never happen)
    return ("DATA_WRITE", logging.INFO)

def generate_event():
    """
    Generates a single log event with a scenario-based message + extra data.
    Returns (log_level, message, extra).
    """
    event_name, level = weighted_choice(EVENTS)

    # Pick a random node & volume
    node = random.choice(nodes)
    volume = random.choice(volumes)

    # Potential user or system
    user = fake.user_name()
    # Some random process ID to add flavor
    process_id = random.randint(100, 9999)

    # We'll build an extra dict with lots of detail
    extra = {
        "event": event_name,
        "cluster": node["cluster"],
        "node_id": node["id"],
        "node_ip": node["ip"],
        "node_region": node["region"],
        "volume_id": volume["id"],
        "volume_cluster": volume["cluster"],
        "volume_capacity_gb": volume["capacity_gb"],
        "process_id": process_id,
    }

    if event_name == "NODE_JOINED":
        message = f"Node {node['id']} joined cluster {node['cluster']} in region {node['region']}"
    elif event_name == "NODE_LEFT":
        message = f"Node {node['id']} LEFT cluster {node['cluster']} (possible maintenance)"
    elif event_name == "NODE_OFFLINE":
        message = f"Node {node['id']} is OFFLINE, IP={node['ip']}, cluster={node['cluster']}!"
        extra["reason"] = "Connectivity Lost"
    elif event_name == "VOLUME_CREATED":
        new_size = random.randint(100, 3000)
        message = f"Volume {volume['id']} CREATED with {new_size}GB"
        extra["new_size_gb"] = new_size
        extra["created_by"] = user
    elif event_name == "VOLUME_EXPANDED":
        old_size = volume["capacity_gb"]
        add_size = random.randint(100, 500)
        new_size = old_size + add_size
        volume["capacity_gb"] = new_size  # update the volume capacity
        message = f"Volume {volume['id']} expanded from {old_size}GB to {new_size}GB"
        extra["old_size_gb"] = old_size
        extra["new_size_gb"] = new_size
        extra["expanded_by"] = user
    elif event_name == "VOLUME_DELETED":
        message = f"Volume {volume['id']} was DELETED from cluster {node['cluster']}"
        extra["deleted_by"] = user
    elif event_name == "SNAPSHOT_CREATED":
        snap_id = f"snap-{random.randint(1000,9999)}"
        message = f"Snapshot {snap_id} created for volume {volume['id']}"
        extra["snapshot_id"] = snap_id
        extra["created_by"] = user
    elif event_name == "SNAPSHOT_RESTORED":
        snap_id = f"snap-{random.randint(1000,9999)}"
        message = f"Snapshot {snap_id} restored onto volume {volume['id']}"
        extra["snapshot_id"] = snap_id
        extra["restored_by"] = user
    elif event_name == "BACKUP_COMPLETED":
        message = f"Backup completed for volume {volume['id']}"
        extra["backup_by"] = user
    elif event_name == "BACKUP_FAILED":
        message = f"Backup FAILED for volume {volume['id']}"
        extra["error"] = "Simulated backup error"
        extra["backup_user"] = user
    elif event_name == "DATA_READ":
        size_mb = random.randint(1, 500)
        message = f"Data read {size_mb}MB from volume {volume['id']} by user {user}"
        extra["read_mb"] = size_mb
        extra["user"] = user
    elif event_name == "DATA_WRITE":
        size_mb = random.randint(1, 2000)
        message = f"Data write {size_mb}MB to volume {volume['id']} by user {user}"
        extra["written_mb"] = size_mb
        extra["user"] = user
    elif event_name == "DATA_REPLICATION":
        message = (f"Data replication started on node {node['id']} "
                   f"for volume {volume['id']}")
        extra["replication_initiator"] = user
    elif event_name == "DATA_REPLICATION_FAILED":
        message = (f"Data replication FAILED on node {node['id']} "
                   f"for volume {volume['id']}")
        extra["error"] = "Simulated replication failure"
    elif event_name == "ALERT_CAPACITY":
        usage_gb = volume["capacity_gb"] + random.randint(100, 300)
        message = (f"Capacity ALERT: volume {volume['id']} usage={usage_gb}GB / "
                   f"capacity={volume['capacity_gb']}GB!")
        extra["usage_gb"] = usage_gb
    elif event_name == "ALERT_PERFORMANCE":
        iops = random.randint(10000, 50000)
        message = f"Performance ALERT: node {node['id']} iops={iops}!"
        extra["iops"] = iops
    elif event_name == "MAINTENANCE_MODE_ENABLED":
        message = f"MAINTENANCE MODE ENABLED on cluster {node['cluster']}"
        extra["enabled_by"] = user
    elif event_name == "MAINTENANCE_MODE_DISABLED":
        message = f"MAINTENANCE MODE DISABLED on cluster {node['cluster']}"
        extra["disabled_by"] = user
    else:
        message = f"UNKNOWN EVENT {event_name}"
        extra["unknown_event"] = True

    # Add some random "timestamp" override if you want to simulate different times
    # Or add more fields like "deviceType", "operatingSystem", etc.
    # But we'll keep it consistent for now.

    return level, message, extra

def main():
    """
    Infinite loop generating highly detailed, scenario-based logs,
    all sent to Better Stack's Logtail (no console output).
    """
    logger.info("Starting Ultra-Detailed Data Storage Simulation...")

    while True:
        level, message, extra = generate_event()
        logger.log(level, message, extra=extra)
        time.sleep(1)

if __name__ == "__main__":
    main()