"""
==============================================================
  Distributed AQI Monitoring System — CLIENT (Sensor Node)
  Run this file 6 times in separate terminals.
==============================================================
  Node assignments (by launch order):
    Node 1 — Zone-A (Andheri)  normal
    Node 2 — Zone-A (Andheri)  normal
    Node 3 — Zone-B (Bandra)   normal
    Node 4 — Zone-B (Bandra)   ⚠ BYZANTINE  ← sends inflated PM2.5
    Node 5 — Zone-C (Powai)    normal
    Node 6 — Zone-C (Powai)    normal

  Node 4 being Byzantine causes Zone-B to appear DEGRADED at Tier 1.
  If you want to test full zone elimination at Tier 2, set BOTH_BYZANTINE = True
  below — that makes nodes 3 AND 4 send bad data so the entire zone gets dropped.

  Usage (run each in its own terminal, in order):
    python client.py   ×6
"""

import socket
import json
import time
import random
import os
import sys
from datetime import datetime

HOST   = "127.0.0.1"
PORT   = 9999
ROUNDS = 5
ROUND_INTERVAL = 4   # seconds between rounds

# Set True to make BOTH Zone-B nodes Byzantine → triggers inter-zone drop
BOTH_BYZANTINE = False

# ── Zone-aware baselines (Mumbai realistic µg/m³ / ppm / ppb) ────────────────
# Each zone has a slightly different pollution profile
ZONE_BASELINES = {
    "Zone-A (Andheri)": {"pm25": (48.0, 6.0),  "co": (1.3, 0.2), "no2": (40.0, 5.0), "o3": (20.0, 3.0)},
    "Zone-B (Bandra)":  {"pm25": (42.0, 5.0),  "co": (1.1, 0.2), "no2": (36.0, 4.0), "o3": (24.0, 3.5)},
    "Zone-C (Powai)":   {"pm25": (35.0, 4.0),  "co": (0.9, 0.15),"no2": (30.0, 3.5), "o3": (28.0, 4.0)},
}

NODE_ZONE = {
    1: "Zone-A (Andheri)",
    2: "Zone-A (Andheri)",
    3: "Zone-B (Bandra)",
    4: "Zone-B (Bandra)",
    5: "Zone-C (Powai)",
    6: "Zone-C (Powai)",
}

NODE_LOCATION = {
    1: "Andheri-West",
    2: "Andheri-East",
    3: "Bandra-West",
    4: "Bandra-Kurla",
    5: "Powai-Lake",
    6: "Hiranandani",
}


def get_node_index() -> int:
    counter_file = "aqi_node_counter.txt"
    try:
        with open(counter_file, "r") as f:
            idx = int(f.read().strip()) + 1
    except (FileNotFoundError, ValueError):
        idx = 1
    with open(counter_file, "w") as f:
        f.write(str(idx))
    return idx


def generate_normal_reading(node_idx: int, round_id: int, zone: str) -> dict:
    random.seed(node_idx * 100 + round_id)
    baseline = ZONE_BASELINES[zone]
    reading  = {}
    for metric, (mean, std) in baseline.items():
        reading[metric] = round(max(0.0, random.gauss(mean, std)), 4)
    reading["noise_level"] = round(random.uniform(0.1, 0.4), 3)
    return reading


def generate_byzantine_reading(node_idx: int, round_id: int, zone: str) -> dict:
    """Inject a massive PM2.5 spike — simulates hardware fault or spoofed data."""
    random.seed(node_idx * 100 + round_id)
    reading = generate_normal_reading(node_idx, round_id, zone)
    spike   = random.uniform(4.5, 6.0)
    reading["pm25"]        = round(reading["pm25"] * spike, 4)
    reading["noise_level"] = round(random.uniform(1.0, 2.0), 3)
    return reading


def is_byzantine(node_idx: int) -> bool:
    if BOTH_BYZANTINE:
        return node_idx in (3, 4)   # entire Zone-B goes bad
    return node_idx == 4            # only node 4 is bad


def print_reading(round_id, reading, byzantine, location):
    tag = "⚠ [BYZANTINE]" if byzantine else "  [NORMAL]   "
    print(f"  {tag} Round {round_id} | {location}")
    print(f"    PM2.5={reading['pm25']:>7.2f} µg/m³  "
          f"CO={reading['co']:>5.2f} ppm  "
          f"NO2={reading['no2']:>6.2f} ppb  "
          f"O3={reading['o3']:>6.2f} ppb  "
          f"noise={reading['noise_level']:.3f}")


def print_consensus(resp: dict, node_idx: int, zone: str):
    sep = "─" * 58
    print(f"\n  {sep}")
    print(f"  ◈ CONSENSUS — Round {resp['round_id']}")
    print(f"  {sep}")
    print(f"  Naive AQI (no BFT) : {resp['naive_aqi']}")
    print(f"  Consensus AQI      : {resp['consensus_aqi']}  ({resp['aqi_category']})")
    print(f"  Byzantine zones    : {resp['byzantine_zones'] or 'none'}")
    print(f"  Round latency      : {resp['latency_ms']} ms")

    # Show this node's zone result
    for zr in resp.get("zone_results", []):
        if zr["zone"] == zone:
            print(f"  Your zone status   : {zr['status'].upper()}  "
                  f"(conf={zr['confidence']})  "
                  f"Intra-Δ={zr.get('intra_pm25_diff', 'N/A')}")
            break
    print(f"  {sep}\n")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    node_idx  = get_node_index()
    zone      = NODE_ZONE.get(node_idx, "Zone-A (Andheri)")
    location  = NODE_LOCATION.get(node_idx, "Unknown")
    byzantine = is_byzantine(node_idx)

    sep = "═" * 58
    print(f"\n{sep}")
    print(f"  AQI SENSOR NODE {node_idx}")
    print(f"  Zone     : {zone}")
    print(f"  Location : {location}")
    print(f"  Role     : {'⚠ BYZANTINE (faulty sensor)' if byzantine else '✓ Normal (honest sensor)'}")
    print(f"{sep}\n")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((HOST, PORT))
    except ConnectionRefusedError:
        print("[CLIENT] Cannot connect — start server.py first.")
        sys.exit(1)

    print(f"[Node-{node_idx}] Connected to {HOST}:{PORT}\n")

    buffer = ""

    for round_id in range(1, ROUNDS + 1):
        reading = (generate_byzantine_reading(node_idx, round_id, zone)
                   if byzantine
                   else generate_normal_reading(node_idx, round_id, zone))

        reading["round_id"]  = round_id
        reading["location"]  = location
        reading["zone"]      = zone
        reading["timestamp"] = datetime.utcnow().isoformat()

        print(f"[Node-{node_idx}] Round {round_id}:")
        print_reading(round_id, reading, byzantine, location)

        try:
            sock.sendall((json.dumps(reading) + "\n").encode())
        except BrokenPipeError:
            print("[CLIENT] Server disconnected.")
            break

        # Wait for consensus response
        sock.settimeout(25)
        try:
            while True:
                chunk = sock.recv(4096).decode()
                if not chunk:
                    break
                buffer += chunk
                if "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    if line.strip():
                        resp = json.loads(line)
                        if resp.get("round_id") == round_id:
                            print_consensus(resp, node_idx, zone)
                            break
        except socket.timeout:
            print(f"[Node-{node_idx}] Timeout on round {round_id}.")

        if round_id < ROUNDS:
            print(f"[Node-{node_idx}] Next round in {ROUND_INTERVAL}s...\n")
            time.sleep(ROUND_INTERVAL)

    print(f"\n[Node-{node_idx}] All {ROUNDS} rounds complete.")
    sock.close()

    if node_idx >= 6:
        try:
            os.remove("aqi_node_counter.txt")
        except Exception:
            pass


if __name__ == "__main__":
    main()