#!/usr/bin/env python3
""" N3 GTP-U Management Function

Author: Heven Tafese
"""
import time, json, psutil, os
from collections import deque

INTERVAL  = 1.0
OUTPUT    = "/home/heven/data/mnf_flooding_n3_v3.jsonl"
INTERFACE = "enp0s8"
BASE      = f"/sys/class/net/{INTERFACE}/statistics"

N3_MBPS_CEILING = 138.0
N3_PPS_CEILING  = 18500

ONSET_RHO      = 0.85
CONGESTION_RHO = 1.0
FAILING_DURATION_S = 75.0
ROLLING_WINDOW = 3

RESET_SIGNAL_FILE = "/home/heven/data/n3_mitigation_reset.signal"

os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
print(f"[N3 MnF] Output: {OUTPUT}")
print(f"[N3 MnF] Ceiling: {N3_MBPS_CEILING} Mbps / {N3_PPS_CEILING} PPS")
print(f"[N3 MnF] Bands: normal < 0.85 <= onset < 1.0 <= CONGESTION")
print(f"[N3 MnF] FAILING: CONGESTION sustained for {FAILING_DURATION_S:.0f}s, "
      f"one way latch")
print(f"[N3 MnF] Reading sysfs rx counters on {INTERFACE} "
      f"(validated proxy for gtp5g-forwarded traffic)")
print(f"[N3 MnF] To clear a latched failure, touch: {RESET_SIGNAL_FILE}")
print()


def read_stat(name):
    try:
        with open(f"{BASE}/{name}") as f:
            return int(f.read().strip())
    except Exception:
        return 0


def reset_failed_latch_if_signalled():
    if os.path.exists(RESET_SIGNAL_FILE):
        try:
            os.remove(RESET_SIGNAL_FILE)
        except Exception:
            pass
        return True
    return False


prev_bytes   = 0
prev_packets = 0

tput_history = deque(maxlen=ROLLING_WINDOW)
pps_history  = deque(maxlen=ROLLING_WINDOW)

congestion_since = None
failed_latch      = False

try:
    with open(OUTPUT, 'a') as f:
        while True:
            time.sleep(INTERVAL)
            now = time.time()

            rx_bytes   = read_stat("rx_bytes")
            rx_packets = read_stat("rx_packets")

            tput_bps = (rx_bytes   - prev_bytes)   * 8 if prev_bytes   > 0 else 0
            pkt_rate = (rx_packets - prev_packets)      if prev_packets > 0 else 0

            prev_bytes   = rx_bytes
            prev_packets = rx_packets

            upf_cpu = psutil.cpu_percent(interval=None)
            mem     = psutil.virtual_memory()
            load1   = psutil.getloadavg()[0]

            tput_history.append(tput_bps)
            pps_history.append(pkt_rate)
            tput_rolling = sum(tput_history) / len(tput_history)
            pps_rolling  = sum(pps_history)  / len(pps_history)

            rho_tput = tput_rolling / (N3_MBPS_CEILING * 1_000_000)
            rho_pps  = pps_rolling  / N3_PPS_CEILING
            rho      = max(rho_tput, rho_pps)

            if reset_failed_latch_if_signalled():
                failed_latch      = False
                congestion_since  = None
                print("[N3 MnF] External reset signal consumed, "
                      "FAILING latch cleared")

            if not failed_latch:
                if rho >= CONGESTION_RHO:
                    if congestion_since is None:
                        congestion_since = now
                    if (now - congestion_since) >= FAILING_DURATION_S:
                        failed_latch = True
                else:
                    congestion_since = None

            if failed_latch:
                flag = "FAILING"
            elif rho >= CONGESTION_RHO:
                flag = "CONGESTION"
            elif rho >= ONSET_RHO:
                flag = "onset"
            else:
                flag = "normal"

            congestion_detected = rho >= ONSET_RHO

            sustained_congestion_s = (
                round(now - congestion_since, 1)
                if congestion_since is not None else 0.0
            )

            rec = {
                "timestamp":             round(now, 3),
                "layer":                 "upf",
                "plane":                 "user",
                "interface":             "N3",
                "source":                f"sysfs:{INTERFACE}",
                "scenario":              "flooding_n3",
                "n3_rx_throughput_bps":         round(float(tput_bps), 2),
                "n3_packet_rate_pps":           round(float(pkt_rate), 2),
                "n3_rx_throughput_rolling_bps": round(float(tput_rolling), 2),
                "n3_packet_rate_rolling_pps":   round(float(pps_rolling), 2),
                "n3_rx_total_bytes":     rx_bytes,
                "n3_rx_total_packets":   rx_packets,
                "rho":                   round(rho, 4),
                "rho_tput":              round(rho_tput, 4),
                "rho_pps":               round(rho_pps, 4),
                "band":                  flag,
                "sustained_congestion_s": sustained_congestion_s,
                "host_cpu_percent":       round(upf_cpu, 2),
                "load_avg_1m":           round(load1, 4),
                "memory_percent":        round(mem.percent, 2),
                "congestion_detected":   congestion_detected,
                "system_failing":        failed_latch,
                "calibrated_metrics": [
                    {"field": "n3_packet_rate_rolling_pps", "ceiling": N3_PPS_CEILING, "rho": round(rho_pps, 4)},
                    {"field": "n3_rx_throughput_rolling_bps", "ceiling": N3_MBPS_CEILING * 1_000_000, "rho": round(rho_tput, 4)},
                    {"field": "host_cpu_percent", "ceiling": 43, "rho": round(upf_cpu / 43, 4) if upf_cpu is not None else None},
                    {"field": "load_avg_1m", "ceiling": 2.4, "rho": round(load1 / 2.4, 4) if load1 is not None else None},
                ],
            }

            f.write(json.dumps(rec) + '\n')
            f.flush()

            print(
                f"[{flag}] "
                f"tput={round(tput_bps/1e6,1)}Mbps(roll={round(tput_rolling/1e6,1)}) "
                f"pps={int(pkt_rate)}(roll={int(pps_rolling)}) "
                f"rho={rho:.3f} "
                f"held={sustained_congestion_s:.0f}s "
                f"cpu={upf_cpu:.1f}% load={load1:.2f}"
            )

except KeyboardInterrupt:
    print(f"\n[N3 MnF] Stopped. Data saved to {OUTPUT}")
