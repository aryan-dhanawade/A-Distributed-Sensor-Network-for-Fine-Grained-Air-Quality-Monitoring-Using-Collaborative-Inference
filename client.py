"""
==============================================================
  Distributed AQI Monitoring System — CLIENT (Sensor Node)
  Run this file 4 times in separate terminals.
==============================================================
  Usage:
    Terminal 1: python client.py          (normal node)
    Terminal 2: python client.py          (normal node)
    Terminal 3: python client.py          (normal node)
    Terminal 4: python client.py          (will auto-become Byzantine if 4th)

  One of the 4 instances is randomly elected as a Byzantine node
  that injects faulty/exaggerated PM2.5 readings.
"""

import socket
import json
import time
import random
import os
import sys
from datetime import datetime

HOST = "127.0.0.1"
PORT = 9999
ROUNDS = 5
ROUND_INTERVAL = 4   # seconds between rounds

# ── Dummy sensor locations (assigned round-robin by node index) ───────────────
LOCATIONS = [
    "Andheri-West",
    "Bandra-Kurla",
    "Powai-Lake",
    "Dharavi-Rd",
]

# ── Baseline AQI conditions for Mumbai (realistic ranges) ────────────────────
#    PM2.5 µg/m³  |  CO ppm  |  NO2 ppb  |  O3 ppb
BASELINE = {
    "pm25": (45.0, 10.0),    # (mean, std)
    "co":   (1.2,  0.3),
    "no2":  (38.0, 6.0),
    "o3":   (22.0, 4.0),
}


def generate_normal_reading(node_id: int, round_id: int) -> dict:
    """Simulate a realistic (honest) sensor reading with Gaussian noise."""
    random.seed(node_id * 100 + round_id)   # reproducible per node/round
    reading = {}
    for metric, (mean, std) in BASELINE.items():
        val = random.gauss(mean, std)
        reading[metric] = round(max(0.0, val), 4)
    reading["noise_level"] = round(random.uniform(0.1, 0.5), 3)
    return reading


def generate_byzantine_reading(node_id: int, round_id: int) -> dict:
    """
    Simulate a faulty / malicious sensor that reports wildly inflated PM2.5
    — classic Byzantine behaviour in sensor networks.
    """
    random.seed(node_id * 100 + round_id)
    reading = generate_normal_reading(node_id, round_id)
    # Inject fault: PM2.5 blown up 4-6x to simulate a hardware fault or attack
    fault_factor = random.uniform(4.0, 6.0)
    reading["pm25"] = round(reading["pm25"] * fault_factor, 4)
    reading["noise_level"] = round(random.uniform(0.8, 1.5), 3)   # high noise
    return reading


def print_reading(round_id, reading, is_byzantine, location):
    tag = "⚠ [BYZANTINE NODE]" if is_byzantine else "  [NORMAL NODE]   "
    print(f"  {tag}  Round {round_id} | Location: {location}")
    print(f"    PM2.5 = {reading['pm25']:>7.2f} µg/m³   "
          f"CO = {reading['co']:>5.2f} ppm   "
          f"NO2 = {reading['no2']:>6.2f} ppb   "
          f"O3 = {reading['o3']:>6.2f} ppb   "
          f"noise = {reading['noise_level']:.3f}")


def print_consensus(response: dict, node_id: int):
    sep = "─" * 56
    print(f"\n  {sep}")
    print(f"  ◈ CONSENSUS RESULT — Round {response['round_id']}")
    print(f"  {sep}")
    print(f"  Consensus AQI  : {response['consensus_aqi']}  "
          f"({response['aqi_category']})")
    print(f"  Naive AQI      : {response['naive_aqi']}  "
          f"(without Byzantine filter)")
    print(f"  Byzantine nodes: {response['byzantine_nodes']}")
    print(f"  Latency        : {response['latency_ms']} ms")

    my_dev = response["node_deviations"].get(str(node_id), {})
    if my_dev:
        tag = "⚠ YOUR NODE WAS FLAGGED BYZANTINE" if my_dev["is_byzantine"] else "✓ Your node trusted"
        print(f"  Your deviation : {my_dev.get('deviation', 'N/A')}  — {tag}")
    print(f"  {sep}\n")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    # Determine node identity via a small handshake file trick:
    # each client increments a shared counter file to self-assign an index.
    counter_file = "aqi_node_counter.txt"
    try:
        with open(counter_file, "r") as f:
            node_index = int(f.read().strip()) + 1
    except (FileNotFoundError, ValueError):
        node_index = 1
    with open(counter_file, "w") as f:
        f.write(str(node_index))

    location = LOCATIONS[(node_index - 1) % len(LOCATIONS)]

    # Node 4 is the Byzantine fault node
    is_byzantine = (node_index == 4)

    sep = "═" * 56
    print(f"\n{sep}")
    print(f"  AQI SENSOR NODE — Index {node_index}")
    print(f"  Location : {location}")
    print(f"  Role     : {'⚠ BYZANTINE (faulty sensor)' if is_byzantine else 'Normal (honest sensor)'}")
    print(f"{sep}\n")

    # Connect to server
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((HOST, PORT))
    except ConnectionRefusedError:
        print("[CLIENT] Could not connect to server. Start server.py first.")
        sys.exit(1)

    print(f"[Node-{node_index}] Connected to server at {HOST}:{PORT}\n")

    buffer = ""

    for round_id in range(1, ROUNDS + 1):
        # ── Generate reading ──────────────────────────────────────────────────
        if is_byzantine:
            reading = generate_byzantine_reading(node_index, round_id)
        else:
            reading = generate_normal_reading(node_index, round_id)

        reading["round_id"]  = round_id
        reading["location"]  = location
        reading["timestamp"] = datetime.utcnow().isoformat()

        print(f"[Node-{node_index}] Sending Round {round_id}:")
        print_reading(round_id, reading, is_byzantine, location)

        # ── Send to server ────────────────────────────────────────────────────
        payload = json.dumps(reading) + "\n"
        try:
            sock.sendall(payload.encode())
        except BrokenPipeError:
            print("[CLIENT] Server disconnected.")
            break

        # ── Receive consensus response ────────────────────────────────────────
        # (server sends one JSON line per round)
        sock.settimeout(20)
        try:
            while True:
                chunk = sock.recv(4096).decode()
                if not chunk:
                    break
                buffer += chunk
                if "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    if line.strip():
                        response = json.loads(line)
                        if response.get("round_id") == round_id:
                            print_consensus(response, node_index)
                            break
        except socket.timeout:
            print(f"[Node-{node_index}] Timeout waiting for Round {round_id} consensus.")

        if round_id < ROUNDS:
            print(f"[Node-{node_index}] Waiting {ROUND_INTERVAL}s for next round...\n")
            time.sleep(ROUND_INTERVAL)

    print(f"\n[Node-{node_index}] All {ROUNDS} rounds complete. Disconnecting.")
    sock.close()

    # Clean up counter on last node (optional)
    if node_index == 4:
        try:
            os.remove(counter_file)
        except Exception:
            pass


if __name__ == "__main__":
    main()