"""
==============================================================
  Distributed AQI Monitoring System — SERVER
  DC Principles: Distributed Aggregation + Byzantine Agreement
==============================================================
  Run FIRST before starting any clients.
  Usage: python server.py
"""

import socket
import threading
import json
import time
import statistics
import math
from datetime import datetime

HOST = "127.0.0.1"
PORT = 9999
EXPECTED_CLIENTS = 4          # Wait for all 4 nodes before consensus
BYZANTINE_THRESHOLD = 1.5     # IQR multiplier for outlier detection
ROUNDS = 5                    # Number of sensing rounds

# ── Shared state ──────────────────────────────────────────────────────────────
lock = threading.Lock()
round_data: dict[int, dict] = {}   # round_id -> {client_id: reading_dict}
round_events: dict[int, threading.Event] = {}
connected_clients: dict[int, socket.socket] = {}
client_id_counter = 0


# ══════════════════════════════════════════════════════════════════════════════
#  METRIC HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def compute_aqi_from_pm25(pm25: float) -> float:
    """Simple linear AQI estimation from PM2.5 (µg/m³)."""
    breakpoints = [
        (0.0,   12.0,   0,   50),
        (12.1,  35.4,  51,  100),
        (35.5,  55.4, 101,  150),
        (55.5, 150.4, 151,  200),
        (150.5, 250.4, 201, 300),
        (250.5, 350.4, 301, 400),
        (350.5, 500.4, 401, 500),
    ]
    for lo_c, hi_c, lo_i, hi_i in breakpoints:
        if lo_c <= pm25 <= hi_c:
            return round((hi_i - lo_i) / (hi_c - lo_c) * (pm25 - lo_c) + lo_i, 2)
    return 500.0


def aqi_category(aqi: float) -> str:
    if aqi <= 50:   return "Good"
    if aqi <= 100:  return "Moderate"
    if aqi <= 150:  return "Unhealthy for Sensitive Groups"
    if aqi <= 200:  return "Unhealthy"
    if aqi <= 300:  return "Very Unhealthy"
    return "Hazardous"


# ══════════════════════════════════════════════════════════════════════════════
#  BYZANTINE AGREEMENT MODULE
#  Uses IQR-based outlier rejection — a practical BFT approach for sensor nets.
#  Any node whose reading lies beyond Q1 - k*IQR or Q3 + k*IQR is flagged.
# ══════════════════════════════════════════════════════════════════════════════

def byzantine_filter(readings: dict) -> tuple[dict, list]:
    """
    Returns (trusted_readings, byzantine_node_ids).
    Works on PM2.5 values. Needs ≥ 4 nodes for meaningful BFT (3f+1 rule).
    """
    values = {cid: r["pm25"] for cid, r in readings.items()}
    sorted_vals = sorted(values.values())

    if len(sorted_vals) < 3:
        return readings, []          # not enough nodes to judge

    q1 = statistics.quantiles(sorted_vals, n=4)[0]
    q3 = statistics.quantiles(sorted_vals, n=4)[2]
    iqr = q3 - q1

    lower = q1 - BYZANTINE_THRESHOLD * iqr
    upper = q3 + BYZANTINE_THRESHOLD * iqr

    trusted, byzantine = {}, []
    for cid, r in readings.items():
        if lower <= r["pm25"] <= upper:
            trusted[cid] = r
        else:
            byzantine.append(cid)

    # Safety: always keep at least 1 node
    if not trusted:
        trusted = readings
        byzantine = []

    return trusted, byzantine


# ══════════════════════════════════════════════════════════════════════════════
#  DISTRIBUTED AGGREGATION MODULE
#  Weighted average across surviving (trusted) nodes.
#  Weight = 1 / (1 + node_reported_noise_level)  so quieter sensors dominate.
# ══════════════════════════════════════════════════════════════════════════════

def distributed_aggregate(trusted_readings: dict) -> dict:
    """
    Returns aggregated sensor values and per-metric statistics.
    """
    if not trusted_readings:
        return {}

    metrics = ["pm25", "co", "no2", "o3"]
    aggregated = {}
    weights = []

    for r in trusted_readings.values():
        noise = r.get("noise_level", 1.0)
        weights.append(1.0 / (1.0 + noise))

    total_w = sum(weights)
    norm_weights = [w / total_w for w in weights]

    readings_list = list(trusted_readings.values())

    for m in metrics:
        vals = [r[m] for r in readings_list]
        weighted_avg = sum(v * w for v, w in zip(vals, norm_weights))
        aggregated[m] = {
            "weighted_avg": round(weighted_avg, 4),
            "mean":         round(statistics.mean(vals), 4),
            "stdev":        round(statistics.stdev(vals), 4) if len(vals) > 1 else 0.0,
            "min":          round(min(vals), 4),
            "max":          round(max(vals), 4),
        }

    # Derive consensus AQI from aggregated PM2.5
    consensus_pm25 = aggregated["pm25"]["weighted_avg"]
    consensus_aqi  = compute_aqi_from_pm25(consensus_pm25)
    aggregated["consensus_aqi"]      = consensus_aqi
    aggregated["aqi_category"]       = aqi_category(consensus_aqi)
    aggregated["participating_nodes"] = len(trusted_readings)

    return aggregated


# ══════════════════════════════════════════════════════════════════════════════
#  CONSENSUS ROUND RUNNER
# ══════════════════════════════════════════════════════════════════════════════

def run_consensus_round(round_id: int):
    t_start = time.time()
    readings = round_data[round_id]

    raw_pm25_values = [r["pm25"] for r in readings.values()]
    naive_avg_pm25  = statistics.mean(raw_pm25_values)
    naive_aqi       = compute_aqi_from_pm25(naive_avg_pm25)

    # ── Step 1: Byzantine Agreement ──────────────────────────────────────────
    trusted, byzantine_ids = byzantine_filter(readings)

    # ── Step 2: Distributed Aggregation ──────────────────────────────────────
    aggregated = distributed_aggregate(trusted)

    latency_ms = round((time.time() - t_start) * 1000, 2)

    # ── Step 3: Per-node deviation stats ─────────────────────────────────────
    consensus_pm25 = aggregated["pm25"]["weighted_avg"]
    node_deviations = {}
    for cid, r in readings.items():
        dev = round(abs(r["pm25"] - consensus_pm25), 4)
        node_deviations[cid] = {
            "pm25":        r["pm25"],
            "deviation":   dev,
            "is_byzantine": cid in byzantine_ids,
            "location":    r.get("location", "unknown"),
        }

    # ── Step 4: Print server-side report ─────────────────────────────────────
    sep = "═" * 60
    print(f"\n{sep}")
    print(f"  ROUND {round_id} CONSENSUS REPORT  [{datetime.now().strftime('%H:%M:%S')}]")
    print(sep)
    print(f"  Nodes received    : {len(readings)}")
    print(f"  Byzantine nodes   : {len(byzantine_ids)}  {byzantine_ids if byzantine_ids else ''}")
    print(f"  Trusted nodes     : {len(trusted)}")
    print(f"  Naive AQI (no BFT): {naive_aqi}  ({aqi_category(naive_aqi)})")
    print(f"  Consensus AQI     : {aggregated['consensus_aqi']}  ({aggregated['aqi_category']})")
    print(f"  Round latency     : {latency_ms} ms")
    print()
    print(f"  {'Node':<8} {'Location':<18} {'PM2.5':>7} {'Deviation':>10} {'Status'}")
    print(f"  {'-'*8} {'-'*18} {'-'*7} {'-'*10} {'-'*12}")
    for cid, info in node_deviations.items():
        status = "⚠ BYZANTINE" if info["is_byzantine"] else "✓ trusted"
        print(f"  Node-{cid:<3} {info['location']:<18} {info['pm25']:>7.2f} {info['deviation']:>10.4f}  {status}")
    print()
    print(f"  Aggregated PM2.5 : {aggregated['pm25']['weighted_avg']} µg/m³  "
          f"(σ={aggregated['pm25']['stdev']})")
    print(f"  Aggregated CO    : {aggregated['co']['weighted_avg']} ppm")
    print(f"  Aggregated NO2   : {aggregated['no2']['weighted_avg']} ppb")
    print(f"  Aggregated O3    : {aggregated['o3']['weighted_avg']} ppb")
    print(sep)

    # ── Step 5: Build response and broadcast ─────────────────────────────────
    response = {
        "round_id":          round_id,
        "consensus_aqi":     aggregated["consensus_aqi"],
        "aqi_category":      aggregated["aqi_category"],
        "aggregated":        aggregated,
        "byzantine_nodes":   byzantine_ids,
        "naive_aqi":         naive_aqi,
        "latency_ms":        latency_ms,
        "node_deviations":   {str(k): v for k, v in node_deviations.items()},
    }

    payload = json.dumps(response) + "\n"
    with lock:
        dead = []
        for cid, sock in connected_clients.items():
            try:
                sock.sendall(payload.encode())
            except Exception:
                dead.append(cid)
        for cid in dead:
            del connected_clients[cid]


# ══════════════════════════════════════════════════════════════════════════════
#  CLIENT HANDLER THREAD
# ══════════════════════════════════════════════════════════════════════════════

def handle_client(conn: socket.socket, client_id: int):
    print(f"[SERVER] Node-{client_id} connected.")
    buffer = ""
    try:
        while True:
            chunk = conn.recv(4096).decode()
            if not chunk:
                break
            buffer += chunk
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                if not line.strip():
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    continue

                round_id = msg.get("round_id")
                if round_id is None:
                    continue

                with lock:
                    if round_id not in round_data:
                        round_data[round_id] = {}
                        round_events[round_id] = threading.Event()
                    round_data[round_id][client_id] = msg

                    count = len(round_data[round_id])
                    print(f"[SERVER] Round {round_id}: received from Node-{client_id} "
                          f"(PM2.5={msg['pm25']:.2f})  [{count}/{EXPECTED_CLIENTS}]")

                    if count == EXPECTED_CLIENTS:
                        round_events[round_id].set()

                # Wait until all clients have reported for this round
                round_events[round_id].wait(timeout=15)

                # Only the last-arriving thread triggers consensus
                with lock:
                    already_done = round_data[round_id].get("_done", False)
                    if not already_done and len(round_data[round_id]) >= EXPECTED_CLIENTS:
                        round_data[round_id]["_done"] = True
                        trigger = True
                    else:
                        trigger = False

                if trigger:
                    run_consensus_round(round_id)

    except Exception as e:
        print(f"[SERVER] Node-{client_id} error: {e}")
    finally:
        print(f"[SERVER] Node-{client_id} disconnected.")
        with lock:
            connected_clients.pop(client_id, None)
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    global client_id_counter
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(10)
    print(f"[SERVER] AQI Monitoring Server listening on {HOST}:{PORT}")
    print(f"[SERVER] Waiting for {EXPECTED_CLIENTS} sensor nodes...\n")

    try:
        while True:
            conn, addr = server.accept()
            with lock:
                client_id_counter += 1
                cid = client_id_counter
                connected_clients[cid] = conn
            t = threading.Thread(target=handle_client, args=(conn, cid), daemon=True)
            t.start()
    except KeyboardInterrupt:
        print("\n[SERVER] Shutting down.")
    finally:
        server.close()


if __name__ == "__main__":
    main()