import os
import time
import random
import logging
import uuid
import threading
import psutil

from faker import Faker
from logtail import LogtailHandler
from prometheus_client import start_http_server, Counter, Histogram, Gauge
from kubernetes import client, config

# -------------------------------------------------
# 1. LOGTAIL SETUP
# -------------------------------------------------
old_host = os.environ.get("LOGTAIL_OLD_HOST")
new_host = os.environ.get("LOGTAIL_NEW_HOST")

old_source_token = os.environ.get("LOGTAIL_SOURCE_TOKEN")
if not old_source_token:
    raise ValueError("LOGTAIL_SOURCE_TOKEN env var not found!")
new_source_token = os.environ.get("SECOND_LOGTAIL_SOURCE_TOKEN")
if not new_source_token:
    raise ValueError("SECOND_LOGTAIL_SOURCE_TOKEN env var not found!")

old_handler = LogtailHandler(source_token=old_source_token, host=old_host)
new_handler = LogtailHandler(source_token=new_source_token, host=new_host)

logger = logging.getLogger("DataStoragePlus")
logger.setLevel(logging.INFO)
logger.handlers = []
logger.addHandler(old_handler)
logger.addHandler(new_handler)

# -------------------------------------------------
# 2. PROMETHEUS METRICS SETUP
# -------------------------------------------------
# Simulated event metrics
EVENT_COUNTER = Counter('generated_events_total', 'Total number of generated events', ['event_name'])
EVENT_PROCESSING_TIME = Histogram('event_processing_seconds', 'Time spent processing events')

# System metrics (Node Exporter–like)
# CPU times per mode
NODE_CPU_SECONDS_TOTAL = Gauge('node_cpu_seconds_total', 'Time spent by CPU in various modes', ['cpu', 'mode'])
# Memory
NODE_MEMORY_MemTotal_bytes = Gauge('node_memory_MemTotal_bytes', 'Total physical memory in bytes')
NODE_MEMORY_MemAvailable_bytes = Gauge('node_memory_MemAvailable_bytes', 'Available memory in bytes')
NODE_MEMORY_MemFree_bytes = Gauge('node_memory_MemFree_bytes', 'Free memory in bytes')
NODE_MEMORY_Cached_bytes = Gauge('node_memory_Cached_bytes', 'Cached memory in bytes')
# Load averages
NODE_LOAD1 = Gauge('node_load1', '1m load average')
NODE_LOAD5 = Gauge('node_load5', '5m load average')
NODE_LOAD15 = Gauge('node_load15', '15m load average')
# Processes
NODE_PROCESSES_THREADS_TOTAL = Gauge('node_processes_threads_total', 'Total number of threads in the system')
NODE_PROCS_BLOCKED = Gauge('node_procs_blocked', 'Number of blocked processes')  # simulated, often 0
NODE_PROCESSES_PIDS = Gauge('node_processes_pids', 'Total number of processes')
# Processes state (vector with state label)
NODE_PROCESSES_STATE = Gauge('node_processes_state', 'Number of processes per state', ['state'])
# Disk IO (per device)
NODE_DISK_READ_BYTES_TOTAL = Gauge('node_disk_read_bytes_total', 'Total disk read bytes', ['device'])
NODE_DISK_WRITTEN_BYTES_TOTAL = Gauge('node_disk_written_bytes_total', 'Total disk written bytes', ['device'])
# Network (per interface)
NODE_NETWORK_RECEIVE_BYTES_TOTAL = Gauge('node_network_receive_bytes_total', 'Total bytes received by network interfaces', ['interface'])
NODE_NETWORK_TRANSMIT_BYTES_TOTAL = Gauge('node_network_transmit_bytes_total', 'Total bytes transmitted by network interfaces', ['interface'])
# Filesystem
NODE_FILESYSTEM_FREE_BYTES = Gauge('node_filesystem_free_bytes', 'Free bytes on filesystem', ['mountpoint', 'fstype'])
NODE_FILESYSTEM_SIZE_BYTES = Gauge('node_filesystem_size_bytes', 'Total bytes on filesystem', ['mountpoint', 'fstype'])
# Uptime/boot time
NODE_BOOT_TIME_SECONDS = Gauge('node_boot_time_seconds', 'System boot time in seconds since epoch')

# Kubernetes metrics (if available)
POD_COUNT_GAUGE = Gauge('k8s_pod_count', 'Number of pods running in the cluster')
NODE_COUNT_GAUGE = Gauge('k8s_node_count', 'Number of nodes in the cluster')

# Start the Prometheus HTTP server using the Heroku-assigned port.
port = int(os.environ.get("PORT", 8000))
start_http_server(port)
logging.info(f"Prometheus metrics HTTP server started on port {port}")

# -------------------------------------------------
# 3. KUBERNETES METRICS UPDATER
# -------------------------------------------------
def update_k8s_metrics():
    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()
    v1 = client.CoreV1Api()
    pods = v1.list_pod_for_all_namespaces(watch=False)
    nodes = v1.list_node(watch=False)
    POD_COUNT_GAUGE.set(len(pods.items))
    NODE_COUNT_GAUGE.set(len(nodes.items))
    logging.info("Updated Kubernetes metrics: %d pods, %d nodes", len(pods.items), len(nodes.items))

def k8s_metrics_updater():
    while True:
        update_k8s_metrics()
        time.sleep(60)

threading.Thread(target=k8s_metrics_updater, daemon=True).start()

# -------------------------------------------------
# 4. SYSTEM METRICS UPDATER (Node Exporter–like)
# -------------------------------------------------
def update_system_metrics():
    # CPU metrics
    cpu_times = psutil.cpu_times(percpu=True)
    for idx, ct in enumerate(cpu_times):
        cpu_label = f"cpu{idx}"
        NODE_CPU_SECONDS_TOTAL.labels(cpu=cpu_label, mode="user").set(ct.user)
        NODE_CPU_SECONDS_TOTAL.labels(cpu=cpu_label, mode="system").set(ct.system)
        NODE_CPU_SECONDS_TOTAL.labels(cpu=cpu_label, mode="idle").set(ct.idle)
        if hasattr(ct, 'iowait'):
            NODE_CPU_SECONDS_TOTAL.labels(cpu=cpu_label, mode="iowait").set(ct.iowait)
    
    # Memory metrics
    vm = psutil.virtual_memory()
    NODE_MEMORY_MemTotal_bytes.set(vm.total)
    NODE_MEMORY_MemAvailable_bytes.set(vm.available)
    NODE_MEMORY_MemFree_bytes.set(vm.free)
    # Cached memory might not be available on all systems
    NODE_MEMORY_Cached_bytes.set(getattr(vm, 'cached', 0))
    
    # Load averages (if available)
    try:
        load1, load5, load15 = os.getloadavg()
        NODE_LOAD1.set(load1)
        NODE_LOAD5.set(load5)
        NODE_LOAD15.set(load15)
    except (AttributeError, OSError):
        NODE_LOAD1.set(0)
        NODE_LOAD5.set(0)
        NODE_LOAD15.set(0)
    
    # Processes metrics
    total_threads = 0
    processes_by_state = {}
    pids = psutil.pids()
    for pid in pids:
        try:
            proc = psutil.Process(pid)
            total_threads += proc.num_threads()
            state = proc.status()
            processes_by_state[state] = processes_by_state.get(state, 0) + 1
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    NODE_PROCESSES_THREADS_TOTAL.set(total_threads)
    NODE_PROCESSES_PIDS.set(len(pids))
    # Blocked processes: typically not provided; we simulate as 0.
    NODE_PROCS_BLOCKED.set(0)
    # Update processes state gauge vector:
    for state, count in processes_by_state.items():
        NODE_PROCESSES_STATE.labels(state=state).set(count)
    
    # Disk IO metrics per device
    disk_io = psutil.disk_io_counters(perdisk=True)
    for device, io in disk_io.items():
        NODE_DISK_READ_BYTES_TOTAL.labels(device=device).set(io.read_bytes)
        NODE_DISK_WRITTEN_BYTES_TOTAL.labels(device=device).set(io.write_bytes)
    
    # Network metrics per interface
    net_io = psutil.net_io_counters(pernic=True)
    for iface, io in net_io.items():
        NODE_NETWORK_RECEIVE_BYTES_TOTAL.labels(interface=iface).set(io.bytes_recv)
        NODE_NETWORK_TRANSMIT_BYTES_TOTAL.labels(interface=iface).set(io.bytes_sent)
    
    # Filesystem metrics: update for each mounted partition
    for part in psutil.disk_partitions():
        try:
            usage = psutil.disk_usage(part.mountpoint)
            NODE_FILESYSTEM_FREE_BYTES.labels(mountpoint=part.mountpoint, fstype=part.fstype).set(usage.free)
            NODE_FILESYSTEM_SIZE_BYTES.labels(mountpoint=part.mountpoint, fstype=part.fstype).set(usage.total)
        except Exception:
            continue
    
    # Uptime metric: system boot time
    NODE_BOOT_TIME_SECONDS.set(psutil.boot_time())

def system_metrics_updater():
    while True:
        update_system_metrics()
        time.sleep(15)  # update system metrics every 15 seconds

threading.Thread(target=system_metrics_updater, daemon=True).start()

# -------------------------------------------------
# 5. FAKE EVENT SIMULATION (existing code)
# -------------------------------------------------
fake = Faker()
regions = ["us-east-1", "us-west-2", "eu-central-1", "ap-northeast-1"]

nodes = [
    {"id": "node1", "cluster": "clusterA", "ip": "10.0.0.1", "region": "us-east-1"},
    {"id": "node2", "cluster": "clusterA", "ip": "10.0.0.2", "region": "us-east-1"},
    {"id": "node3", "cluster": "clusterB", "ip": "10.1.0.1", "region": "us-west-2"},
    {"id": "node4", "cluster": "clusterB", "ip": "10.1.0.2", "region": "us-west-2"},
    {"id": "node5", "cluster": "clusterC", "ip": "10.2.0.1", "region": "eu-central-1"},
]

volumes = [
    {"id": "vol-100", "cluster": "clusterA", "capacity_gb": 500},
    {"id": "vol-101", "cluster": "clusterA", "capacity_gb": 1000},
    {"id": "vol-200", "cluster": "clusterB", "capacity_gb": 250},
    {"id": "vol-201", "cluster": "clusterB", "capacity_gb": 750},
    {"id": "vol-300", "cluster": "clusterC", "capacity_gb": 2000},
]

EVENTS = [
    ("NODE_JOINED", logging.INFO, 0.05),
    ("NODE_LEFT", logging.WARNING, 0.02),
    ("NODE_OFFLINE", logging.ERROR, 0.03),
    ("NODE_REBOOT", logging.WARNING, 0.01),
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
    ("IO_TIMEOUT", logging.ERROR, 0.01),
    ("CACHE_MISS", logging.INFO, 0.05),
    ("DISK_FAILURE", logging.CRITICAL, 0.005),
    ("FILE_CORRUPTION", logging.CRITICAL, 0.005),
    ("MAINTENANCE_MODE_ENABLED", logging.WARNING, 0.01),
    ("MAINTENANCE_MODE_DISABLED", logging.INFO, 0.01),
]

DEVICE_TYPES = ["desktop", "mobile", "tablet", "server"]
OPERATING_SYSTEMS = [
    "Windows 10", "Windows Server 2022", "macOS 13.0", "Ubuntu 22.04",
    "CentOS 7", "Android 13", "iOS 16", "Red Hat Enterprise Linux 8",
]
BOT_USER_AGENTS = [
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Mozilla/5.0 (compatible; Bingbot/2.0; +http://www.bing.com/bingbot.htm)",
    "Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)"
]
REAL_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.5615.137 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_4 like Mac OS X) AppleWebKit/604.1.34 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.5481.153 Safari/537.36",
    "Mozilla/5.0 (iPad; CPU OS 16_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; rv:112.0) Gecko/20100101 Firefox/112.0",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:55.0) Gecko/20100101 Firefox/55.0",
]

BOT_CHANCE = 0.03

def pick_user_agent():
    return random.choice(BOT_USER_AGENTS) if random.random() < BOT_CHANCE else random.choice(REAL_USER_AGENTS)

def pick_device_type():
    return random.choice(DEVICE_TYPES)

def pick_os_version():
    return random.choice(OPERATING_SYSTEMS)

def weighted_choice(events):
    total_weight = sum(e[2] for e in events)
    r = random.random() * total_weight
    cumulative = 0.0
    for (name, lvl, weight) in events:
        cumulative += weight
        if r < cumulative:
            return name, lvl
    return ("DATA_WRITE", logging.INFO)

PRIORITIES = ["P0", "P1", "P2", "P3"]
ALL_TAGS = ["production", "infra", "beta", "urgent", "devops", "high-traffic", "critical"]

def generate_event():
    event_name, level = weighted_choice(EVENTS)
    node = random.choice(nodes)
    volume = random.choice(volumes)
    user = fake.user_name()
    process_id = random.randint(100, 9999)
    device_type = pick_device_type()
    os_version = pick_os_version()
    user_agent = pick_user_agent()
    random_user_ip = fake.ipv4_private()
    correlation_id = str(uuid.uuid4())
    cpu_usage = random.randint(1, 99)
    memory_free_mb = random.randint(100, 32000)
    priority = random.choice(PRIORITIES)
    num_tags = random.randint(0, 2)
    chosen_tags = random.sample(ALL_TAGS, k=num_tags)
    phase = random.choice(["init", "processing", "finalizing", "cleanup", "verifying"])

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
        "device_type": device_type,
        "os_version": os_version,
        "user_agent": user_agent,
        "caller_ip": random_user_ip,
        "correlation_id": correlation_id,
        "cpu_usage_percent": cpu_usage,
        "memory_free_mb": memory_free_mb,
        "priority": priority,
        "tags": chosen_tags,
        "phase": phase,
    }

    org_id = random.randint(1, 20)
    dept = random.choice(["sales", "engineering", "it", "devops", "finance"])
    extra["org_id"] = org_id
    extra["department"] = dept

    if event_name in ["DISK_FAILURE", "FILE_CORRUPTION"]:
        extra["disk_temperature_c"] = random.randint(30, 90)
        extra["io_queue_length"] = random.randint(0, 100)

    if event_name == "NODE_JOINED":
        message = f"Node {node['id']} joined cluster {node['cluster']} in region {node['region']}"
    elif event_name == "NODE_LEFT":
        message = f"Node {node['id']} LEFT cluster {node['cluster']} (possible maintenance)"
    elif event_name == "NODE_OFFLINE":
        message = f"Node {node['id']} is OFFLINE, IP={node['ip']}, cluster={node['cluster']}!"
        extra["reason"] = "Connectivity Lost"
    elif event_name == "NODE_REBOOT":
        message = f"Node {node['id']} is rebooting for scheduled maintenance"
        extra["reboot_reason"] = random.choice(["hardware upgrade", "software update", "unexpected error"])
    elif event_name == "VOLUME_CREATED":
        new_size = random.randint(100, 3000)
        message = f"Volume {volume['id']} CREATED with {new_size}GB"
        extra["new_size_gb"] = new_size
        extra["created_by"] = user
    elif event_name == "VOLUME_EXPANDED":
        old_size = volume["capacity_gb"]
        add_size = random.randint(100, 500)
        new_size = old_size + add_size
        volume["capacity_gb"] = new_size
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
        read_latency = random.randint(1, 400)
        message = f"Data read {size_mb}MB from volume {volume['id']} by user {user}"
        extra["read_mb"] = size_mb
        extra["latency_ms"] = read_latency
    elif event_name == "DATA_WRITE":
        size_mb = random.randint(1, 2000)
        write_latency = random.randint(10, 2000)
        message = f"Data write {size_mb}MB to volume {volume['id']} by user {user}"
        extra["written_mb"] = size_mb
        extra["latency_ms"] = write_latency
    elif event_name == "DATA_REPLICATION":
        throughput = random.uniform(0.1, 10.0)
        message = f"Data replication started on node {node['id']} for volume {volume['id']}"
        extra["replication_initiator"] = user
        extra["throughput_mb_s"] = round(throughput, 2)
    elif event_name == "DATA_REPLICATION_FAILED":
        message = f"Data replication FAILED on node {node['id']} for volume {volume['id']}"
        extra["error"] = "Simulated replication failure"
    elif event_name == "ALERT_CAPACITY":
        usage_gb = volume["capacity_gb"] + random.randint(100, 300)
        message = f"Capacity ALERT: volume {volume['id']} usage={usage_gb}GB / capacity={volume['capacity_gb']}GB!"
        extra["usage_gb"] = usage_gb
    elif event_name == "ALERT_PERFORMANCE":
        iops = random.randint(10000, 50000)
        message = f"Performance ALERT: node {node['id']} iops={iops}!"
        extra["iops"] = iops
    elif event_name == "IO_TIMEOUT":
        timeout_duration = random.randint(100, 2000)
        message = f"I/O timeout on node {node['id']} for volume {volume['id']} (timeout: {timeout_duration}ms)"
        extra["timeout_duration_ms"] = timeout_duration
    elif event_name == "CACHE_MISS":
        message = f"Cache miss for volume {volume['id']}, falling back to disk read"
        extra["cache_hit_ratio"] = round(random.uniform(0.7, 0.99), 2)
    elif event_name == "DISK_FAILURE":
        disk_id = f"disk-{random.randint(1,8)}"
        message = f"Critical: Disk failure detected on node {node['id']} (disk {disk_id})"
        extra["disk_id"] = disk_id
        extra["error_code"] = random.choice(["E101", "E202", "E303"])
    elif event_name == "FILE_CORRUPTION":
        message = f"Critical: File corruption detected in volume {volume['id']} on node {node['id']}"
        extra["corruption_level"] = random.choice(["minor", "severe"])
        extra["error_checksum"] = hex(random.randint(0, 0xFFFFFF))[2:]
    elif event_name == "MAINTENANCE_MODE_ENABLED":
        message = f"MAINTENANCE MODE ENABLED on cluster {node['cluster']}"
        extra["enabled_by"] = user
    elif event_name == "MAINTENANCE_MODE_DISABLED":
        message = f"MAINTENANCE MODE DISABLED on cluster {node['cluster']}"
        extra["disabled_by"] = user
    else:
        message = f"UNKNOWN EVENT {event_name}"
        extra["unknown_event"] = True

    return level, message, extra

def main():
    logger.info("Data Storage Simulation with Extra Metadata...")
    while True:
        with EVENT_PROCESSING_TIME.time():
            level, message, extra = generate_event()
        EVENT_COUNTER.labels(event_name=extra.get("event", "unknown")).inc()
        logger.log(level, message, extra=extra)
        sleep_ms = random.randint(10, 65)
        time.sleep(sleep_ms / 1000.0)

if __name__ == "__main__":
    main()