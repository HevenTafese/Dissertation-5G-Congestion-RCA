#!/usr/bin/env python3
"""N2 tshark Management Function

Author: Heven Tafese

This management function captures NGAP procedure rates on SCTP port 38412 for the AMF control plane
and applies a rho based, collapse aware congestion model tuned to the
AMF's measured behaviour. 
"""
import subprocess, json, time, psutil, argparse, threading, os, statistics
from collections import defaultdict, deque

OUTPUT_DIR = "/home/heven/data"
CEILING_MSG_RATE       = 100.0

ONSET_RHO              = 0.85
CONGESTION_RHO         = 1.0

LOADED_RHO             = 0.95

COLLAPSE_LOW_RHO       = 0.70

LOAD_RECOVERY_THRESHOLD = 1.5

FAILING_HOLD_S         = 75.0
CLEAR_HOLD_S           = 5.0

MEDIAN_WINDOW          = 5

CORROBORATION_GRACE_S  = 20.0

# NGAP procedureCode to name (3GPP TS 38.413)
NGAP_PROCEDURES = {
    "0":  "amf_configuration_update",
    "1":  "amf_status_indication",
    "2":  "cell_traffic_trace",
    "4":  "downlink_nas_transport",
    "9":  "error_indication",
    "13": "handover_resource_allocation",
    "14": "initial_context_setup",
    "15": "initial_ue_message",
    "20": "ng_reset",
    "21": "ng_setup",
    "22": "overload_start",
    "23": "overload_stop",
    "24": "paging",
    "25": "path_switch_request",
    "28": "pdu_session_resource_release",
    "29": "pdu_session_resource_setup",
    "40": "ue_context_modification",
    "41": "ue_context_release",
    "45": "ue_tnla_binding_release",
    "46": "uplink_nas_transport",
}

# Registration attempt (initial_context_setup, code 14) is provisional, it read ~ 0 in every run so far, so it needs one clean verification that it appears per
# completed registration on this testbed.

# code 15
ATTEMPT_PROC    = "initial_ue_message"
# code 14
COMPLETION_PROC = "initial_context_setup"

counters = defaultdict(int)
counter_lock = threading.Lock()


def run_tshark(interface):
    cmd = ["tshark", "-i", interface, "-f", "port 38412",
           "-T", "fields", "-e", "ngap.procedureCode", "-l"]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.DEVNULL, text=True)
    for line in proc.stdout:
        line = line.strip()
        if not line:
            continue
        with counter_lock:
            for code in line.split(","):
                code = code.strip()
                if not code:
                    continue
                counters[NGAP_PROCEDURES.get(code, f"ngap_{code}")] += 1
                counters["total"] += 1



class N2CongestionModel:
    """
    Collapse aware congestion state machine for the AMF control plane.
  
    """
    def __init__(self):
        self.window = deque(maxlen=MEDIAN_WINDOW)
        self.loaded = False              
        self.congestion_since = None     
        self.last_congested = None      
        self.last_genuinely_congested = None  
        self.failing = False            

    def update(self, now, total_rate, load_avg):
        self.window.append(float(total_rate))
        smoothed = statistics.median(self.window)
        smoothed_rho = smoothed / CEILING_MSG_RATE
        inst_rho = float(total_rate) / CEILING_MSG_RATE

        if smoothed_rho >= LOADED_RHO:
            self.loaded = True

      
        genuinely_congested_now = smoothed_rho >= CONGESTION_RHO
        if genuinely_congested_now:
            self.last_genuinely_congested = now

        in_grace = (self.last_genuinely_congested is not None and
                    now - self.last_genuinely_congested <= CORROBORATION_GRACE_S)

        rate_collapsed = self.loaded and smoothed_rho < COLLAPSE_LOW_RHO and in_grace
        load_sustained = self.loaded and load_avg >= LOAD_RECOVERY_THRESHOLD and in_grace
        congested = genuinely_congested_now or rate_collapsed or load_sustained

        if congested:
            self.last_congested = now
            if self.congestion_since is None:
                self.congestion_since = now
            if now - self.congestion_since >= FAILING_HOLD_S:
                self.failing = True
        else:
            if (self.last_congested is None or
                    now - self.last_congested >= CLEAR_HOLD_S):
                self.congestion_since = None

        if self.failing or congested:
            congestion_rho = max(smoothed_rho, 1.0)
        else:
            congestion_rho = smoothed_rho

        if self.failing:
            band = "FAILING"
        elif congested:
            band = "CONGESTION"
        elif smoothed_rho >= ONSET_RHO:
            band = "ONSET"
        else:
            band = "normal"

        return {
            "band": band,
            "rho": round(inst_rho, 3),
            "rho_smoothed": round(smoothed_rho, 3),
            "congestion_rho": round(congestion_rho, 3),
            "loaded": self.loaded,
            "rate_collapsed": rate_collapsed,
            "load_sustained": load_sustained,
            "failing": self.failing,
        }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--interface", default="enp0s8")
    ap.add_argument("--interval", type=float, default=1.0)
    ap.add_argument("--scenario", required=True)
    args = ap.parse_args()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out = os.path.join(OUTPUT_DIR, f"mnf_{args.scenario}.jsonl")
    print(f"[N2 MnF] Output: {out}")
    print(f"[N2 MnF] Monitoring NGAP on port 38412 (ceiling {CEILING_MSG_RATE:.0f} msg/s)")
    print(f"[N2 MnF] MUST run with sudo (tshark capture).")

    threading.Thread(target=run_tshark, args=(args.interface,), daemon=True).start()

    prev = defaultdict(int)
    model = N2CongestionModel()

    try:
        while True:
            time.sleep(args.interval)
            now = time.time()
            with counter_lock:
                snap = dict(counters)

            rates = {k: snap.get(k, 0) - prev.get(k, 0) for k in snap}
            for k in snap:
                prev[k] = snap[k]

            total_rate  = rates.get("total", 0)
            attempts    = rates.get(ATTEMPT_PROC, 0)
            completions = rates.get(COMPLETION_PROC, 0)
            dl_nas      = rates.get("downlink_nas_transport", 0)
            ul_nas      = rates.get("uplink_nas_transport", 0)
            releases    = rates.get("ue_context_release", 0)
            pdu         = rates.get("pdu_session_resource_setup", 0)

            success_rate = round(completions / attempts, 3) if attempts > 0 else None

            cpu   = psutil.cpu_percent(interval=None)
            mem   = psutil.virtual_memory().percent
            load1 = psutil.getloadavg()[0]

            st = model.update(now, total_rate, load1)

            rec = {
                "timestamp": round(now, 3),
                "layer": "amf", "plane": "control", "interface": "N2",
                "source": "tshark:ngap:38412", "scenario": args.scenario,
                # primary load signal
                "ngap_total_message_rate": total_rate,
                "rho": st["rho"],
                "rho_smoothed": st["rho_smoothed"],
                "congestion_rho": st["congestion_rho"],
                # registration success (control plane congestion indicator)
                "ngap_registration_attempt_rate": attempts,
                "ngap_registration_complete_rate": completions,
                "registration_success_rate": success_rate,
                # per procedure breakdown 
                "ngap_downlink_nas_rate": dl_nas,
                "ngap_uplink_nas_rate": ul_nas,
                "ngap_ue_context_release_rate": releases,
                "ngap_pdu_session_setup_rate": pdu,
                "ngap_total_messages": snap.get("total", 0),
                # collapse aware state
                "band": st["band"],
                "loaded": st["loaded"],
                "rate_collapsed": st["rate_collapsed"],
                "load_sustained": st["load_sustained"],
                "congestion_detected": st["band"] in ("CONGESTION", "FAILING"),
                "system_failing": st["failing"],
                # system health 
                "cpu_percent": round(cpu, 2),
                "memory_percent": round(mem, 2),
                "load_avg_1m": round(load1, 4),
                # self describing calibration so a generic agent reads N2 correctly
                "calibrated_metrics": [
                    {"field": "ngap_total_message_rate",
                     "ceiling": CEILING_MSG_RATE,
                     "rho": st["congestion_rho"]}
                ],
            }

            with open(out, "a") as f:
                f.write(json.dumps(rec) + "\n")

            sr = f"{success_rate:.2f}" if success_rate is not None else "  - "
            tag = "L" if st["load_sustained"] else ("C" if st["rate_collapsed"] else " ")
            print(f"[{st['band']:10s}{tag}] total={total_rate:4d}/s "
                  f"rho={st['rho']:.2f}(sm {st['rho_smoothed']:.2f}) "
                  f"attempt={attempts:3d} complete={completions:3d} succ={sr} "
                  f"cpu={cpu:5.1f}% load={load1:.2f}")

    except KeyboardInterrupt:
        print("\n[N2 MnF] Stopped.")


if __name__ == "__main__":
    main()
