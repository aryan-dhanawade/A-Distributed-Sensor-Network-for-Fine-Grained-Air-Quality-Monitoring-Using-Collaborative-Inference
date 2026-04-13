"""
==============================================================
  Distributed AQI Monitoring System — SERVER
  DC Principles: Distributed Aggregation + Byzantine Agreement
  Architecture : 6 nodes | 3 zones | 2 nodes per zone
  Two-tier BFT : Intra-zone → Inter-zone
==============================================================
  Usage: python server.py
"""

import socket
import threading
import json
import time
import statistics
from datetime import datetime

HOST = "127.0.0.1"
PORT = 9999
EXPECTED_CLIENTS = 6

# Zone assignment: node_index (1-6) -> zone name
ZONE_MAP = {
    1: "Zone-A (Andheri)",
    2: "Zone-A (Andheri)",
    3: "Zone-B (Bandra)",
    4: "Zone-B (Bandra)",
    5: "Zone-C (Powai)",
    6: "Zone-C (Powai)",
}
ZONES = {
    "Zone-A (Andheri)": [1, 2],
    "Zone-B (Bandra)":  [3, 4],
    "Zone-C (Powai)":   [5, 6],
}

# Intra-zone: max allowed PM2.5 difference between the 2 nodes in a zone
INTRA_ZONE_DISAGREEMENT_THRESHOLD = 30.0  # µg/m³

# Inter-zone: z-score threshold to flag an entire zone as Byzantine
INTER_ZONE_ZSCORE_THRESHOLD = 1.8

ROUNDS = 5

# ── Shared state ──────────────────────────────────────────────────────────────
lock = threading.Lock()
round_data: dict[int, dict] = {}
round_events: dict[int, threading.Event] = {}
connected_clients: dict[int, socket.socket] = {}
client_id_counter = 0


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def compute_aqi_from_pm25(pm25: float) -> float:
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


def weighted_avg_metric(readings_list: list[dict], metric: str) -> float:
    vals    = [r[metric] for r in readings_list]
    noises  = [r.get("noise_level", 1.0) for r in readings_list]
    weights = [1.0 / (1.0 + n) for n in noises]
    total_w = sum(weights)
    return round(sum(v * w for v, w in zip(vals, weights)) / total_w, 4)


# ══════════════════════════════════════════════════════════════════════════════
#  TIER 1 — INTRA-ZONE BYZANTINE AGREEMENT
#
#  With exactly 2 nodes per zone we cannot mathematically determine which node
#  is lying (3f+1 requires ≥3 nodes to tolerate 1 fault).
#  Strategy:
#    • If nodes AGREE (|Δ PM2.5| ≤ threshold) → weighted aggregate, confidence=1.0
#    • If nodes DISAGREE                       → plain average, confidence=0.5 (degraded)
#    • If only 1 node reported                 → use it, confidence=0.5
#    • If 0 nodes reported                     → confidence=0.0, status=no_data
# ══════════════════════════════════════════════════════════════════════════════

def intra_zone_bft(zone_name: str, node_ids: list[int], readings: dict) -> dict:
    present = {nid: readings[nid] for nid in node_ids if nid in readings}
    metrics = ["pm25", "co", "no2", "o3"]

    base = {"zone": zone_name, "node_ids": node_ids,
            "zone_pm25": None, "zone_co": None, "zone_no2": None, "zone_o3": None}

    if len(present) == 0:
        return {**base, "confidence": 0.0, "status": "no_data",
                "byzantine_nodes": [], "intra_pm25_diff": None}

    rlist = list(present.values())

    if len(present) == 1:
        r = rlist[0]
        return {**base,
                "zone_pm25": r["pm25"], "zone_co": r["co"],
                "zone_no2":  r["no2"],  "zone_o3": r["o3"],
                "confidence": 0.5, "status": "degraded",
                "byzantine_nodes": [], "intra_pm25_diff": None}

    # Both nodes present
    diff = abs(rlist[0]["pm25"] - rlist[1]["pm25"])

    if diff <= INTRA_ZONE_DISAGREEMENT_THRESHOLD:
        # Agreement — weighted average
        agg = {m: weighted_avg_metric(rlist, m) for m in metrics}
        return {**base,
                "zone_pm25": agg["pm25"], "zone_co": agg["co"],
                "zone_no2":  agg["no2"],  "zone_o3": agg["o3"],
                "confidence": 1.0, "status": "ok",
                "byzantine_nodes": [], "intra_pm25_diff": round(diff, 4)}
    else:
        # Disagreement — plain average, mark degraded (can't tell who lied)
        agg = {m: round(statistics.mean([r[m] for r in rlist]), 4) for m in metrics}
        return {**base,
                "zone_pm25": agg["pm25"], "zone_co": agg["co"],
                "zone_no2":  agg["no2"],  "zone_o3": agg["o3"],
                "confidence": 0.5, "status": "degraded",
                "byzantine_nodes": [], "intra_pm25_diff": round(diff, 4)}


# ══════════════════════════════════════════════════════════════════════════════
#  TIER 2 — INTER-ZONE BYZANTINE AGREEMENT
#
#  Compare the 3 zone-level PM2.5 aggregates.
#  A zone whose PM2.5 z-score exceeds the threshold means BOTH nodes in that
#  zone are reporting bad data → drop the entire zone from city-wide aggregation.
# ══════════════════════════════════════════════════════════════════════════════

def inter_zone_bft(zone_results: list[dict]) -> tuple[list[dict], list[str]]:
    active = [z for z in zone_results if z["zone_pm25"] is not None]

    if len(active) < 2:
        return active, []

    pm25_vals = [z["zone_pm25"] for z in active]

    # With only 2 active zones use relative difference instead of z-score
    if len(pm25_vals) == 2:
        mean_v = statistics.mean(pm25_vals)
        diff_pct = abs(pm25_vals[0] - pm25_vals[1]) / (mean_v + 1e-9)
        if diff_pct > 0.6:
            suspect = max(active, key=lambda z: z["zone_pm25"])
            byzantine_zones = [suspect["zone"]]
            trusted = [z for z in active if z["zone"] not in byzantine_zones]
            return trusted, byzantine_zones
        return active, []

    # 3 active zones — z-score method
    mean_v  = statistics.mean(pm25_vals)
    stdev_v = statistics.stdev(pm25_vals)

    trusted, byzantine_zones = [], []
    for z in active:
        z_score = abs(z["zone_pm25"] - mean_v) / (stdev_v + 1e-9)
        if z_score > INTER_ZONE_ZSCORE_THRESHOLD:
            byzantine_zones.append(z["zone"])
        else:
            trusted.append(z)

    if not trusted:   # safety — never drop everything
        return active, []

    return trusted, byzantine_zones


# ══════════════════════════════════════════════════════════════════════════════
#  DISTRIBUTED AGGREGATION — CITY-WIDE
#  Each trusted zone contributes proportionally to its confidence score.
#  This is the final stage of the two-level distributed aggregation protocol.
# ══════════════════════════════════════════════════════════════════════════════

def city_wide_aggregate(trusted_zones: list[dict]) -> dict:
    if not trusted_zones:
        return {}

    metrics    = ["pm25", "co", "no2", "o3"]
    confs      = [z["confidence"] for z in trusted_zones]
    total_c    = sum(confs)
    norm_c     = [c / total_c for c in confs]

    agg = {}
    for m in metrics:
        vals   = [z[f"zone_{m}"] for z in trusted_zones]
        agg[m] = round(sum(v * w for v, w in zip(vals, norm_c)), 4)

    agg["consensus_aqi"] = compute_aqi_from_pm25(agg["pm25"])
    agg["aqi_category"]  = aqi_category(agg["consensus_aqi"])
    return agg


# ══════════════════════════════════════════════════════════════════════════════
#  CONSENSUS ROUND RUNNER
# ══════════════════════════════════════════════════════════════════════════════

def run_consensus_round(round_id: int):
    t_start  = time.time()
    readings = {k: v for k, v in round_data[round_id].items()
                if k != "_done" and isinstance(k, int)}

    # Naive AQI — raw average, no filtering
    raw_pm25  = [r["pm25"] for r in readings.values()]
    naive_aqi = compute_aqi_from_pm25(statistics.mean(raw_pm25))

    # ── TIER 1: Intra-zone BFT + aggregation ─────────────────────────────────
    zone_results = [intra_zone_bft(zname, nids, readings)
                    for zname, nids in ZONES.items()]

    # ── TIER 2: Inter-zone BFT ────────────────────────────────────────────────
    trusted_zones, byzantine_zone_names = inter_zone_bft(zone_results)

    # ── City-wide distributed aggregation ────────────────────────────────────
    city = city_wide_aggregate(trusted_zones)

    latency_ms = round((time.time() - t_start) * 1000, 2)

    # ── PRINT REPORT ─────────────────────────────────────────────────────────
    sep = "═" * 68
    print(f"\n{sep}")
    print(f"  ROUND {round_id} — TWO-TIER CONSENSUS REPORT  [{datetime.now().strftime('%H:%M:%S')}]")
    print(sep)
    print(f"  Nodes received  : {len(readings)} / {EXPECTED_CLIENTS}")
    print(f"  Naive AQI       : {naive_aqi}  ({aqi_category(naive_aqi)})  ← no filtering")
    print(f"  Consensus AQI   : {city.get('consensus_aqi', 'N/A')}  ({city.get('aqi_category', 'N/A')})")
    print(f"  Round latency   : {latency_ms} ms")

    # Tier 1 table
    print(f"\n  {'─'*66}")
    print(f"  TIER 1 — INTRA-ZONE AGGREGATION & BFT")
    print(f"  {'─'*66}")
    print(f"  {'Zone':<22} {'Node1 PM2.5':>11} {'Node2 PM2.5':>11} {'|Δ|':>7} "
          f"{'Zone PM2.5':>11} {'Conf':>5}  Status")
    print(f"  {'-'*22} {'-'*11} {'-'*11} {'-'*7} {'-'*11} {'-'*5}  {'-'*10}")

    for zr in zone_results:
        nids = zr["node_ids"]
        v    = [readings[n]["pm25"] if n in readings else None for n in nids]
        va   = f"{v[0]:.2f}" if v[0] is not None else "  N/A"
        vb   = f"{v[1]:.2f}" if v[1] is not None else "  N/A"
        diff = f"{zr['intra_pm25_diff']:.2f}" if zr["intra_pm25_diff"] is not None else "  N/A"
        zpm  = f"{zr['zone_pm25']:.2f}" if zr["zone_pm25"] is not None else "  N/A"
        conf = f"{zr['confidence']:.1f}"
        stat_map = {"ok": "✓ OK", "degraded": "⚠ DEGRADED", "no_data": "✗ NO DATA"}
        stat = stat_map[zr["status"]]
        print(f"  {zr['zone']:<22} {va:>11} {vb:>11} {diff:>7} {zpm:>11} {conf:>5}  {stat}")

    # Tier 2 table
    print(f"\n  {'─'*66}")
    print(f"  TIER 2 — INTER-ZONE BFT")
    print(f"  {'─'*66}")
    if byzantine_zone_names:
        print(f"  ⚠  Byzantine zones dropped : {byzantine_zone_names}")
    else:
        print(f"  ✓  All zones passed inter-zone BFT — no zones dropped")
    trusted_zone_names = [z["zone"] for z in trusted_zones]
    print(f"  Trusted zones              : {trusted_zone_names}")

    # City-wide result
    print(f"\n  {'─'*66}")
    print(f"  CITY-WIDE DISTRIBUTED AGGREGATION")
    print(f"  {'─'*66}")
    if city:
        print(f"  PM2.5  : {city['pm25']} µg/m³")
        print(f"  CO     : {city['co']} ppm")
        print(f"  NO2    : {city['no2']} ppb")
        print(f"  O3     : {city['o3']} ppb")
        print(f"  AQI    : {city['consensus_aqi']}  → {city['aqi_category']}")
    else:
        print(f"  [!] No trusted zones — cannot produce city-wide AQI")
    print(sep)

    # ── Broadcast to all clients ──────────────────────────────────────────────
    response = {
        "round_id":        round_id,
        "naive_aqi":       naive_aqi,
        "consensus_aqi":   city.get("consensus_aqi"),
        "aqi_category":    city.get("aqi_category"),
        "city_aggregated": city,
        "zone_results":    [{k: v for k, v in zr.items()} for zr in zone_results],
        "byzantine_zones": byzantine_zone_names,
        "latency_ms":      latency_ms,
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
    print(f"[SERVER] Node-{client_id} connected  ({ZONE_MAP.get(client_id, 'unknown')})")
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
                    count = len([k for k in round_data[round_id] if k != "_done"])
                    print(f"[SERVER] Round {round_id}: Node-{client_id} "
                          f"PM2.5={msg['pm25']:.2f}  [{count}/{EXPECTED_CLIENTS}]")
                    if count == EXPECTED_CLIENTS:
                        round_events[round_id].set()

                round_events[round_id].wait(timeout=20)

                with lock:
                    already_done = round_data[round_id].get("_done", False)
                    count_now = len([k for k in round_data[round_id] if k != "_done"])
                    if not already_done and count_now >= EXPECTED_CLIENTS:
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
    print(f"[SERVER] AQI Monitoring Server  {HOST}:{PORT}")
    print(f"[SERVER] Zones  : {list(ZONES.keys())}")
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