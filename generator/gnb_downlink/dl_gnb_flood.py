#!/usr/bin/env python3
""" gNB Downlink Flood Generator

Author: Heven Tafese

This generator sends a continuous rate stepped packet stream across multiple UEs. 
"""
import socket, time, argparse

TICK = 0.002

def build_targets(a):
    if a.ips: return [ip.strip() for ip in a.ips.split(",") if ip.strip()]
    base = a.base.rsplit(".", 1); start = int(base[1]); prefix = base[0]
    return [f"{prefix}.{start+i}" for i in range(a.count)]

def parse_phases(s, default_pps, default_dur):
   
    if not s:
        return [(default_pps, default_dur)]
    out = []
    for ph in s.split(","):
        pps_s, dur_s = ph.split(":")
        out.append((int(pps_s), int(dur_s)))
    return out

def run(sock, targets, payload, port, phases):
    """One continuous loop across all phases. pps changes at boundaries and  the
    packet clock is never reset, so there is no gap between phases."""
    n = len(targets)
    now = time.time()
    start = now
    
    bounds = []
    t = start
    for pps, dur in phases:
        t += dur
        bounds.append((t, pps))
    total_end = bounds[-1][0]

    def pps_at(ts):
        for b_end, b_pps in bounds:
            if ts < b_end:
                return b_pps
        return 0

    next_t = start
    idx = 0
    sent = 0; dropped = 0
    last_report = start; last_sent = 0; last_drop = 0
    cur_pps = pps_at(start)
    interval = 1.0 / cur_pps if cur_pps > 0 else 0.0
    cap = max(20, int(cur_pps * 0.03))

    while True:
        now = time.time()
        if now >= total_end:
            break
       
        p = pps_at(now)
        if p != cur_pps:
            cur_pps = p
            interval = 1.0 / cur_pps if cur_pps > 0 else 0.0
            cap = max(20, int(cur_pps * 0.03))
           
            if next_t < now:
                next_t = now

        budget = 0
        while cur_pps > 0 and next_t <= now and budget < cap:
            try:
                sock.sendto(payload, (targets[idx % n], port)); sent += 1
            except (BlockingIOError, OSError):
                dropped += 1
            idx += 1; next_t += interval; budget += 1

        if cur_pps == 0:
            time.sleep(TICK)
        elif next_t > now:
            time.sleep(min(TICK, next_t - now))
        elif budget >= cap:
            next_t = now + interval

        if now - last_report >= 1.0:
            d = sent - last_sent; dr = dropped - last_drop
            extra = f"  drops={dr}/s" if dr else ""
            print(f"[dl-gnb] {d}/s aggregate  ~{d*len(payload)*8/1e6:.1f} Mbit/s  "
                  f"(across {n} UEs, ~{d//n if n else 0}/s each)  target={cur_pps}/s{extra}")
            last_report = now; last_sent = sent; last_drop = dropped

    return sent

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="10.60.0.1"); ap.add_argument("--count", type=int, default=50)
    ap.add_argument("--ips", default=None); ap.add_argument("--port", type=int, default=9999)
    ap.add_argument("--size", type=int, default=400); ap.add_argument("--pps", type=int, default=3000)
    ap.add_argument("--duration", type=int, default=30); ap.add_argument("--phases", default="")
    ap.add_argument("--sndbuf", type=int, default=4194304, help="SO_SNDBUF bytes (default 4MB)")
    a = ap.parse_args()

    targets = build_targets(a)
    if not targets:
        print("[dl-gnb] no targets (empty --ips?)"); return
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, a.sndbuf)
    s.setblocking(False)
    actual = s.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)
    payload = b"\x00" * a.size
    phases = parse_phases(a.phases, a.pps, a.duration)

    print(f"[dl-gnb] continuous  {len(targets)} UEs {targets[0]}..{targets[-1]}  "
          f"SO_SNDBUF={actual} (asked {a.sndbuf})  tick={TICK*1000:.0f}ms")
    print(f"[dl-gnb] phases: " + ", ".join(f"{p}/s x{d}s" for p, d in phases) +
          "  (continuous, no gaps)")
    total = run(s, targets, payload, a.port, phases)
    print(f"[dl-gnb] done: {total} packets total")

if __name__ == "__main__":
    main()
