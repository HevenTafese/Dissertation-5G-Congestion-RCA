#!/usr/bin/env python3
"""
User Plane end to end management function.
Author: Heven Tafese


"""
import json, time, os, argparse
import psutil
from collections import deque
import statistics as _stat

PROC_NET_DEV = "/proc/net/dev"
N3_IFACE = "enp0s8"
UPFGTP_IFACE = "upfgtp"
N6_IFACE = "enp0s3"
OUTPUT_DIR = "/home/heven/data"

CEILING_BPS = 24_000_000
ONSET_RHO = 0.85
CONGESTION_RHO = 1.0
FAILING_SUSTAIN_SEC = 75
SMOOTH_WINDOW = 5


def read_all_interface_stats(interfaces):
    result = {}
    try:
        with open(PROC_NET_DEV, 'r') as f:
            for line in f:
                if ':' not in line:
                    continue
                name = line.split(':')[0].strip()
                if name in interfaces:
                    d = line.split(':')[1].split()
                    result[name] = {
                        "rx_bytes": int(d[0]), "rx_packets": int(d[1]),
                        "rx_errors": int(d[2]), "rx_drop": int(d[3]),
                        "tx_bytes": int(d[8]), "tx_packets": int(d[9]),
                        "tx_errors": int(d[10]), "tx_drop": int(d[11]),
                    }
    except FileNotFoundError:
        pass
    return result


def find_upf_proc():
    for p in psutil.process_iter(['pid', 'cmdline']):
        try:
            if 'bin/upf' in ' '.join(p.info['cmdline'] or []):
                return p
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return None


def safe_upf_metrics(proc):
    if proc is None:
        return None, None
    try:
        return proc.cpu_percent(interval=None), proc.memory_info().rss / (1024 * 1024)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return None, None


def classify_band(rho, st):
    now = time.time()
    if st.get("latched_failing"):
        return "FAILING"
    if rho >= CONGESTION_RHO:
        if st["congestion_start"] is None:
            st["congestion_start"] = now
        if now - st["congestion_start"] >= FAILING_SUSTAIN_SEC:
            st["latched_failing"] = True
            return "FAILING"
        return "congestion"
    st["congestion_start"] = None
    return "onset" if rho >= ONSET_RHO else "normal"


def rate(curr, prev, key, elapsed):
    if curr is None or prev is None:
        return 0.0
    return (curr.get(key, 0) - prev.get(key, 0)) / elapsed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--scenario', required=True)
    ap.add_argument('--interval', type=int, default=1)
    args = ap.parse_args()

    ifaces = [N3_IFACE, UPFGTP_IFACE, N6_IFACE]
    out = os.path.join(OUTPUT_DIR, f"mnf_{args.scenario}.jsonl")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"[N6-MLF] Multi layer UPF MnF starting")
    print(f"[N6-MLF] Layers: N3={N3_IFACE}  upfgtp={UPFGTP_IFACE}  N6={N6_IFACE}  + UPF process")
    print(f"[N6-MLF] Primary metric: upfgtp throughput.  Ceiling (PROVISIONAL): {CEILING_BPS/1e6:.1f} Mbps")
    print(f"[N6-MLF] Smoothing: {SMOOTH_WINDOW}-sample median.  Output: {out}\n")

    upf_proc = find_upf_proc()
    if upf_proc is not None:
        upf_proc.cpu_percent(interval=None)

    prev = read_all_interface_stats(ifaces)
    prev_t = time.time()
    st = {"congestion_start": None, "latched_failing": False}
    rho_buf = deque(maxlen=SMOOTH_WINDOW)

    with open(out, 'a') as f:
        while True:
            time.sleep(args.interval)
            now = time.time()
            curr = read_all_interface_stats(ifaces)
            elapsed = now - prev_t

            if upf_proc is None or not upf_proc.is_running():
                upf_proc = find_upf_proc()
                if upf_proc is not None:
                    upf_proc.cpu_percent(interval=None)

            n3_in_bps = rate(curr.get(N3_IFACE), prev.get(N3_IFACE), "rx_bytes", elapsed) * 8
            n3_in_pps = rate(curr.get(N3_IFACE), prev.get(N3_IFACE), "rx_packets", elapsed)
            u_rx_bps = rate(curr.get(UPFGTP_IFACE), prev.get(UPFGTP_IFACE), "rx_bytes", elapsed) * 8
            u_tx_bps = rate(curr.get(UPFGTP_IFACE), prev.get(UPFGTP_IFACE), "tx_bytes", elapsed) * 8
            upfgtp_bps = u_rx_bps + u_tx_bps
            upfgtp_pps = (rate(curr.get(UPFGTP_IFACE), prev.get(UPFGTP_IFACE), "rx_packets", elapsed)
                          + rate(curr.get(UPFGTP_IFACE), prev.get(UPFGTP_IFACE), "tx_packets", elapsed))
            n6_bps = (rate(curr.get(N6_IFACE), prev.get(N6_IFACE), "rx_bytes", elapsed)
                      + rate(curr.get(N6_IFACE), prev.get(N6_IFACE), "tx_bytes", elapsed)) * 8

            upf_cpu, upf_mem_mb = safe_upf_metrics(upf_proc)
            through_ratio = (upfgtp_bps / n3_in_bps) if n3_in_bps > 1 else None

            rho_buf.append(upfgtp_bps / CEILING_BPS)
            rho = _stat.median(rho_buf)
            band = classify_band(rho, st)

            record = {
                "timestamp": now, "scenario": args.scenario,
                "layer": "upf", "plane": "user", "interface": "N6", "node": "UPF",
                "source": "proc_net_dev_multi+psutil",
                "n3_ingress_bps": n3_in_bps, "n3_ingress_pps": n3_in_pps,
                "upfgtp_throughput_bps": upfgtp_bps, "upfgtp_pps": upfgtp_pps,
                "n6_egress_observed_bps": n6_bps,
                "through_ratio_upfgtp_over_n3": through_ratio,
                "upf_process_cpu_percent": upf_cpu, "upf_process_mem_mb": upf_mem_mb,
                "system_cpu_percent": psutil.cpu_percent(interval=None),
                "system_mem_percent": psutil.virtual_memory().percent,
                "system_load_avg_1m": os.getloadavg()[0],
                "rho": round(rho, 4), "band": band,
                "calibrated_metrics": [
                    {"field": "upfgtp_throughput_bps", "ceiling": CEILING_BPS, "rho": round(rho, 4)},
                ],
            }
            f.write(json.dumps(record) + '\n')
            f.flush()
            tr = f"{through_ratio:.2f}" if through_ratio is not None else "n/a"
            cpu = f"{upf_cpu:.0f}%" if upf_cpu is not None else "n/a"
            print(f"[N6-MLF] N3in={n3_in_bps/1e6:5.1f}M  upfgtp={upfgtp_bps/1e6:5.1f}M "
                  f"rho={rho:.2f} band={band:10s} through={tr} upfCPU={cpu} "
                  f"sysCPU={record['system_cpu_percent']:.0f}%")
            prev = curr
            prev_t = now


if __name__ == "__main__":
    main()
