
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

import psutil
from datetime import datetime
from scripts.utils.bd_oracle_connection import get_oracle_connection
from scripts.utils.logger import logger
from scripts.utils.log_checker import check_and_alert_log
import socket


def collect_metrics():
    cpu_percent = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory()
    memory_percent = memory.percent
    disk = psutil.disk_usage('/')
    disk_usage = disk.percent
    net = psutil.net_io_counters()
    network_in = net.bytes_recv
    network_out = net.bytes_sent
    # Load average (Unix only)
    try:
        load_avg = psutil.getloadavg()[0]
    except (AttributeError, OSError):
        load_avg = 0.0
    return {
        'cpu_percent': cpu_percent,
        'memory_percent': memory_percent,
        'disk_usage': disk_usage,
        'network_in': network_in,
        'network_out': network_out,
        'load_average': load_avg
    }

def insert_system_metrics(host_id, metrics, measured_at=None):
    conn = get_oracle_connection()
    if not conn:
        logger.error("❌ Connexion Oracle impossible, insertion annulée.")
        return
    cur = conn.cursor()
    if measured_at is None:
        measured_at = datetime.now()
    try:
        cur.execute("""
            INSERT INTO system_metrics (
                measured_at, host_id, cpu_percent, memory_percent, disk_usage, network_in, network_out, load_average
            ) VALUES (
                :measured_at, :host_id, :cpu_percent, :memory_percent, :disk_usage, :network_in, :network_out, :load_average
            )
        """, {
            "measured_at": measured_at,
            "host_id": host_id,
            "cpu_percent": metrics['cpu_percent'],
            "memory_percent": metrics['memory_percent'],
            "disk_usage": metrics['disk_usage'],
            "network_in": metrics['network_in'],
            "network_out": metrics['network_out'],
            "load_average": metrics['load_average']
        })
        conn.commit()
        logger.info(f"✅ Metrics insérées pour {host_id} à {measured_at}")
    except Exception as e:
        logger.error(f"❌ Erreur insertion metrics : {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    host_id = socket.gethostname()
    metrics = collect_metrics()
    insert_system_metrics(host_id, metrics)
    print(f"Metrics insérées pour {host_id} : {metrics}")


if __name__ == "__main__":
    check_and_alert_log("insert_system_metrics", "insert_system_metrics")