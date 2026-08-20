#!/usr/bin/env python3
""" 

Author: Heven Tafese
Mitigation Agent

Receives an RCA agents conclusion and picks one action per cycle by minimising a
receding horizon objective over six actions including (none, admission_control,
rate_limit, vertical_scale, horizontal_scale, traffic_shaping).

Immediate post action rho comes from the same rho = TF/R model the RCA
uses; each candidate is projected HORIZON_STEPS cycles forward and scored
on horizon summed rho error plus four normalised cost terms (loss, delay,
resource, disruption).

Citations for the topic discussed.
  Kleinrock 1975 Sec 2.4 (utilisation and delay), target_rho by plane.
  Rawlings, Mayne & Diehl 2017 Sec 1.3, receding horizon quadratic objective.
  Marler & Arora 2004, normalising heterogeneous terms before summing.
  3GPP TS 23.501 Sec 4.2, interface to NF map.
  3GPP TS 23.501 Table 5.7.4-1 5QI 9, PDB = 300 ms, used as L_MAX.
  3GPP TS 29.244, QER/MBR primitive that traffic_shaping represents.
  Le Boudec & Thiran 2001, token bucket delay bound b/r.
  ETSI NFV-IFA, scaling operation names. 
"""
import json
import math
import time
from pathlib import Path
from typing import TypedDict, Optional

import httpx
from langgraph.graph import StateGraph, END


ROOT              = Path(__file__).resolve().parent.parent
HISTORY_FILE      = str(ROOT / "data" / "mitigation_history.jsonl")
FULL_HISTORY_FILE = str(ROOT / "data" / "mitigation_full_history.jsonl")
INSTANCE_STATE    = str(ROOT / "data" / "deployed_instances.json")
OLLAMA_BASE       = "http://192.168.56.1:11434"
LLM_MODEL         = "rca-qwen"

TARGET_RHO_BY_PLANE = {
    "control":    0.65,
    "user":       0.80,
    "signalling": 0.65,
}
DEFAULT_TARGET_RHO  = 0.70


W_RHO         = 1.00
W_LOSS        = 1.00
W_DELAY       = 1.00
W_RESOURCE    = 1.00
W_DISRUPTION  = 1.00


F_MAX               = 0.95
T_MAX               = 0.95
PCT_MAX              = 200
K_MAX                = 5

COOLDOWN_S          = 60

DEADBAND_HALF_WIDTH = 0.05

HORIZON_STEPS       = 3

# measured from agentic_conclusions.jsonl

CYCLE_DURATION_S    = 15.0

# safety cap on linear extrapolation over the horizon. 
HORIZON_RHO_CAP     = 5.0

# how many cycles until an action's effect is fully in place. 
ACTION_LANDING_CYCLES = {
    "none":               0,
    "admission_control":  1,
    "rate_limit":         1,
    "traffic_shaping":    1,
    "vertical_scale":     2,
    "horizontal_scale":   3,
}

# gtp5g on this testbed uses a policer (TrafficPolicer, policePacket,

SHAPING_MODE = "rate_cap"   # "rate_cap" or "queue_aware"

# metrics whose name ends this way are bitrate metrics we can shape.
# CPU percent and message rate ceilings should not be shaped.
BITRATE_METRIC_SUFFIXES = ("_bps",)

# fraction of ceiling for burst tolerance. 0.1 is a common token-bucket sizing

SHAPE_BURST_FRACTION_OF_CEILING = 0.1

SHAPE_BUFFER_SECONDS = 1.0

# 3GPP TS 23.501 Table 5.7.4-1, 5QI 9, PDB = 300 ms. free5GC's SMF uses

L_MAX = 0.300


def _rho_from_conclusion(c, iface):
    """Return (rho, source). Tries severity[iface]['rho'] first, then any
    raw ratio the RCA passed through under 'rho', 'rho_tput', 'rho_pps'.
    Returns (None, 'unavailable') when neither exists. No band-floor
    fallback: an LLM's classification should not set a mitigation magnitude."""
    sev = (c.get("severity") or {})
    entry = sev.get(iface) or {}

    rho = entry.get("rho")
    if rho is not None:
        return float(rho), "compute_severity"

    metrics = entry.get("metrics") or {}
    for name in ("rho", "rho_tput", "rho_pps"):
        if metrics.get(name) is not None:
            return float(metrics[name]), f"telemetry.{name}"

    return None, "unavailable"


def _ceiling_metric_from_conclusion(c, iface):
    sev = (c.get("severity") or {})
    entry = sev.get(iface) or {}
    ceiling = entry.get("ceiling")
    metric = entry.get("metric")
    if ceiling is not None:
        try:
            return float(ceiling), metric
        except (TypeError, ValueError):
            return None, metric
    return None, metric


def _trajectory_from_conclusion(c, iface):
 
    sev = (c.get("severity") or {})
    entry = sev.get(iface) or {}
    traj = entry.get("trajectory") or {}
    rate = traj.get("rho_rate_per_s")
    eta = entry.get("eta_to_congestion_s")
    return (float(rate) if rate is not None else None,
            float(eta) if eta is not None else None)


def _is_bitrate_metric(metric_name):
    if not metric_name:
        return False
    return metric_name.endswith(BITRATE_METRIC_SUFFIXES)


AGGRESSIVE_ACTIONS   = {"horizontal_scale"}

RESIZABLE_RESOURCES  = {"cpu_saturation", "memory_saturation", "single_process_saturation"}
LEGITIMACY_RESOURCES = {"registration_storm", "signalling_overload"}


METRIC_TO_RESOURCE_CATEGORY = {
    "host_cpu_percent":             "cpu_saturation",
    "load_avg_1m":                  "system_load_saturation",
    "n3_packet_rate_rolling_pps":   "throughput_saturation",
    "n3_rx_throughput_rolling_bps": "throughput_saturation",
    "upfgtp_throughput_bps":        "throughput_saturation",
    "pfcp_heartbeat_req_rate":      "single_process_saturation",
}

# 3GPP TS 23.501 
INTERFACE_NF_MAP = {
    "N2":      "AMF",
    "N3":      "UPF",
    "N4":      "UPF",
    "N6":      "UPF",
    "gNB-DL":  "gNB",
    "N11":     "SMF",
    "e2e":     "SMF",
}


class MitigationState(TypedDict):
    conclusion: dict
    interface: str
    monitored_interfaces: list
    plane: str
    target_rho: float
    rho: Optional[float]
    rho_source: str
    ceiling: Optional[float]
    metric_name: Optional[str]
    rho_rate_per_s: Optional[float]
    eta_to_congestion_s: Optional[float]
    resource_category: Optional[str]
    feedback_loop: bool
    status: str
    worst_band: str
    calibrated: bool
    target_nf: str
    current_instances: int
    target_provenance: str
    candidates: list
    feasible: list
    scored: list
    decision: dict
    narrative: Optional[str]
    output: dict
    exec_log: list
    cooldown_active: bool
    last_action_ts: Optional[float]
    deadband_suppressed: bool



def _load_instance_state():
    p = Path(INSTANCE_STATE)
    if not p.exists():
        return {}
    try:
        with p.open() as f:
            return json.load(f)
    except (IOError, json.JSONDecodeError) as e:
        print(f"    [state] instance state read error (starting fresh): {e}")
        return {}


def _save_instance_state(state):
    p = Path(INSTANCE_STATE)
    p.parent.mkdir(parents=True, exist_ok=True)
    try:
        with p.open("w") as f:
            json.dump(state, f, indent=2)
    except (IOError, OSError) as e:
        print(f"    [state] instance state write error (non-fatal): {e}")


def _get_current_instances(nf):
   
    st = _load_instance_state()
    return int(st.get(nf, {}).get("instances", 1))


def _record_scale(nf, new_count, action):
    st = _load_instance_state()
    st[nf] = {
        "instances": int(new_count),
        "last_action": action,
        "last_action_ts": time.time(),
    }
    _save_instance_state(st)


def _pick_worst_iface(severity):
    worst_iface, worst_rho = None, None
    for iface, s in (severity or {}).items():
        r = s.get("rho")
        if r is None:
            continue
        if worst_rho is None or r > worst_rho:
            worst_iface, worst_rho = iface, r
    return worst_iface


def _plane_of(iface, conclusion):
    sev = (conclusion.get("severity") or {}).get(iface, {})
    plane = sev.get("plane")
    if plane in ("control", "user", "signalling"):
        return plane
    # fallback by interface name
    if iface in ("N2", "N4", "N11", "e2e"):
        return "control"
    if iface in ("N3", "N6", "gNB-DL"):
        return "user"
    return ""


def _load_recent_history(interface):
    p = Path(HISTORY_FILE)
    if not p.exists():
        return []
    now = time.time()
    recent = []
    try:
        with p.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if rec.get("interface") != interface:
                    continue
                if now - rec.get("timestamp", 0) < COOLDOWN_S:
                    recent.append(rec)
    except (IOError, OSError) as e:
        print(f"    [History] read error (non-fatal): {e}")
    return recent


def _append_history(record):
    p = Path(HISTORY_FILE)
    p.parent.mkdir(parents=True, exist_ok=True)
    try:
        with p.open("a") as f:
            f.write(json.dumps(record) + "\n")
    except (IOError, OSError) as e:
        print(f"    [History] write error (non-fatal): {e}")


def _append_history_full(record):
  
    p = Path(FULL_HISTORY_FILE)
    p.parent.mkdir(parents=True, exist_ok=True)
    try:
        with p.open("a") as f:
            f.write(json.dumps(record) + chr(10))
    except (IOError, OSError) as e:
        print(f"    [History] full-log write error (non-fatal): {e}")


def _ollama_generate(prompt, timeout=300.0):
    try:
        t = httpx.Timeout(connect=3.0, read=timeout, write=5.0, pool=5.0)
        r = httpx.post(f"{OLLAMA_BASE}/api/generate",
                       json={"model": LLM_MODEL, "prompt": prompt,
                             "stream": False,
                             "options": {"temperature": 0.2, "num_predict": 500, "num_ctx": 8192}},
                       timeout=t)
        r.raise_for_status()
        return (r.json().get("response") or "").strip()
    except (httpx.HTTPError, httpx.TimeoutException) as e:
        print(f"    [Ollama] narrative unavailable (falling back to skeleton): {e}")
        return None


def _record_step(state, tool, phase, inputs, output, provenance, claim=None):
    state.setdefault('exec_log', [])
    state['exec_log'].append({
        "step": len(state['exec_log']) + 1,
        "tool": tool,
        "phase": phase,
        "input": inputs,
        "output": output,
        "provenance": provenance,
        "claim": claim or tool,
        "timestamp": time.time(),
    })


def _project_natural_trajectory(rho0, rho_rate_per_s, steps, cap, cycle_duration_s):
    """Project rho forward `steps` cycles with no mitigation, linear on the
    RCA's own measured slope. Falls back to zero slope when none is available."""
    if rho0 is None:
        return [None] * steps
    slope = rho_rate_per_s if rho_rate_per_s is not None else 0.0
    out = []
    for i in range(1, steps + 1):
        projected = rho0 + slope * cycle_duration_s * i
        out.append(_clamp(projected, 0.0, cap))
    return out


def _build_rho_trajectory(action, immediate_predicted_rho, natural,
                          landing_cycles, horizon_steps):
    """Per-cycle rho trajectory a candidate implies over the horizon.
    'none' follows the natural projection all the way through. Everything
    else follows natural until it lands, then holds at the derived post
    action rho for the rest of the horizon (steady-state assumption)."""
    if action == "none":
        return list(natural)
    traj = []
    for step in range(1, horizon_steps + 1):
        if step < landing_cycles:
            traj.append(natural[step - 1])
        else:
            traj.append(immediate_predicted_rho)
    return traj


def receive_conclusion(state: MitigationState) -> MitigationState:
    c = state.get("conclusion") or {}
    state["status"]        = c.get("status", "normal")
    state["worst_band"]    = c.get("worst_band", "normal")
    state["feedback_loop"] = bool(c.get("feedback_loop", False))
    state["monitored_interfaces"] = c.get("monitored_interfaces") or []

   
    sev = c.get("severity") or {}
    worst_iface = _pick_worst_iface(sev)
    affected = c.get("affected_interfaces") or []
    monitored = c.get("monitored_interfaces") or []
    real_monitored = next((m for m in monitored if m in INTERFACE_NF_MAP), None)
    state["interface"] = (
        worst_iface
        or (affected[0] if affected else None)
        or real_monitored
        or "unknown"
    )

    state["plane"] = _plane_of(state["interface"], c) if state["interface"] != "unknown" else ""
    state["target_rho"] = TARGET_RHO_BY_PLANE.get(state["plane"], DEFAULT_TARGET_RHO)

   
    sev_entry = (c.get("severity") or {}).get(state["interface"]) or {}
    winning_metric = sev_entry.get("metric")
    state["resource_category"] = METRIC_TO_RESOURCE_CATEGORY.get(winning_metric)

    rho_val, rho_source = _rho_from_conclusion(c, state["interface"])
    state["rho"] = rho_val
    state["rho_source"] = rho_source
    state["calibrated"] = rho_val is not None

    ceiling_val, metric_name = _ceiling_metric_from_conclusion(c, state["interface"])
    state["ceiling"] = ceiling_val
    state["metric_name"] = metric_name

    rate_val, eta_val = _trajectory_from_conclusion(c, state["interface"])
    state["rho_rate_per_s"] = rate_val
    state["eta_to_congestion_s"] = eta_val

    _record_step(
        state, "receive_conclusion", "ingest",
        {"conclusion_keys": list(c.keys())},
        {"interface": state["interface"], "plane": state["plane"],
         "target_rho": state["target_rho"], "rho": state["rho"],
         "ceiling": state["ceiling"], "metric_name": state["metric_name"],
         "rho_rate_per_s": state["rho_rate_per_s"],
         "status": state["status"], "worst_band": state["worst_band"],
         "calibrated": state["calibrated"]},
        {"source": "RCA conclusion", "db": "in-memory",
         "rule": "worst-rho interface selection + plane-keyed target_rho "
                 "(Kleinrock 1975 Sec 2.4)",
         "refs": ["RCA conclusion.severity", "RCA conclusion.status"]},
        claim=f"ingested: interface={state['interface']} plane={state['plane']} "
              f"target_rho={state['target_rho']} current_rho={state['rho']} "
              f"rho_rate_per_s={state['rho_rate_per_s']}")

    _band = (state["worst_band"] or "").lower()
    if _band in ("baseline", "normal"):
        _mon = state["monitored_interfaces"] or ["(none)"]
        print(f"[Receive] {_band or 'baseline'} -- monitoring {_mon}. "
              f"No action needed.")
    else:
        parts = [f"status={state['status']}", f"worst_band={state['worst_band']}"]
        if state["interface"] != "unknown":
            parts.append(f"on {state['interface']} ({state['plane']} plane)")
        if state["monitored_interfaces"]:
            parts.append(f"monitoring={state['monitored_interfaces']}")
        if state["rho"] is not None:
            parts.append(f"rho={state['rho']:.3f} (from {state.get('rho_source','?')}) "
                         f"vs target={state['target_rho']:.2f}")
        if state["rho_rate_per_s"] is not None:
            parts.append(f"rho_rate_per_s={state['rho_rate_per_s']:.4f}")
        if state["resource_category"]:
            parts.append(f"category={state['resource_category']}")
        if state["feedback_loop"]:
            parts.append("feedback_loop=True")
        if not state["calibrated"]:
            parts.append("UNCALIBRATED")
        print(f"[Receive] " + ", ".join(parts))
    return state


def resolve_target(state: MitigationState) -> MitigationState:
    c = state.get("conclusion") or {}
    root = c.get("root_cause") or ""
    target_nf = None
    provenance = ""

    if root and any(nf in root for nf in ("AMF", "SMF", "UPF", "gNB",
                                           "NRF", "PCF", "AUSF", "UDM")):
        for nf in ("AMF", "SMF", "UPF", "gNB", "NRF", "PCF", "AUSF", "UDM"):
            if nf in root:
                target_nf = nf
                provenance = f"RCA root_cause named {nf}"
                break

    if not target_nf and state["interface"] != "unknown":
        target_nf = INTERFACE_NF_MAP.get(state["interface"])
        if target_nf:
            provenance = (f"derived from interface {state['interface']} "
                          f"via 3GPP TS 23.501 Sec 4.2")

    if not target_nf:
        target_nf = "any_nf"
        provenance = "safe default (no attribution and no interface)"

    state["target_nf"] = target_nf
    state["target_provenance"] = provenance
    state["current_instances"] = _get_current_instances(target_nf)

    _record_step(
        state, "resolve_target", "attribution",
        {"root_cause": root, "interface": state["interface"]},
        {"target_nf": target_nf, "current_instances": state["current_instances"]},
        {"source": "3GPP TS 23.501 Sec 4.2 (INTERFACE_NF_MAP)"
                   if "TS 23.501" in provenance else provenance,
         "db": "deployed_instances.json + RCA conclusion",
         "rule": "prefer root_cause NF; else INTERFACE_NF_MAP; else any_nf",
         "refs": ["3GPP TS 23.501 Sec 4.2"]},
        claim=f"target NF resolved to {target_nf} ({provenance}); "
              f"current_instances={state['current_instances']}")
    print(f"[Target] {target_nf} ({provenance}); N={state['current_instances']}")
    return state


def _clamp(x, lo, hi):
    return max(lo, min(x, hi))


def generate_candidates(state: MitigationState) -> MitigationState:
    rho     = state["rho"]
    t       = state["target_rho"]
    N       = state["current_instances"]
    ceiling = state.get("ceiling")
    metric  = state.get("metric_name")
    rho_rate = state.get("rho_rate_per_s")

    natural = _project_natural_trajectory(
        rho, rho_rate, HORIZON_STEPS, HORIZON_RHO_CAP, CYCLE_DURATION_S)

    def _traj(action, immediate):
        return _build_rho_trajectory(
            action, immediate, natural,
            ACTION_LANDING_CYCLES[action], HORIZON_STEPS)

    none_immediate = rho if rho is not None else None
    candidates = [{
        "action": "none",
        "target_nf": state["target_nf"],
        "params": {},
        "predicted_rho": none_immediate,
        "predicted_rho_horizon": _traj("none", none_immediate),
        "landing_cycles": ACTION_LANDING_CYCLES["none"],
        "loss": 0.0, "loss_raw": 0.0,
        "delay_norm": 0.0, "delay_s": 0.0,
        "resource": 0.0, "resource_raw": 0.0,
        "disruption": 0.0,
        "magnitude_derivation": "action=none, no rho change",
    }]

    deadband_active = (rho is not None) and (rho <= t + DEADBAND_HALF_WIDTH)
    state["deadband_suppressed"] = bool(deadband_active)

    if deadband_active:
        state["candidates"] = candidates
        _record_step(
            state, "generate_candidates", "planning",
            {"rho": rho, "target_rho": t, "deadband_half_width": DEADBAND_HALF_WIDTH},
            {"n_candidates": 1, "actions": ["none"], "deadband_suppressed": True},
            {"source": "deadband gate", "db": "in-memory",
             "rule": f"rho <= target_rho + {DEADBAND_HALF_WIDTH} -> none only",
             "refs": ["DEADBAND_HALF_WIDTH (modelling choice)"]},
            claim=f"within deadband (rho={rho:.3f} <= target+{DEADBAND_HALF_WIDTH}); "
                  f"none only")
        print(f"[Candidates] within deadband (rho={rho:.3f} <= "
              f"target={t:.2f}+{DEADBAND_HALF_WIDTH}); none only")
        return state

    if rho is not None and rho > t:
        # admission_control: f = 1 - target/rho
        f_raw = 1.0 - t/rho
        f = _clamp(f_raw, 0.0, F_MAX)
        immediate = rho * (1.0 - f)
        candidates.append({
            "action": "admission_control",
            "target_nf": state["target_nf"],
            "params": {"reject_fraction": round(f, 4),
                       "policy": "conservative"},
            "predicted_rho": immediate,
            "predicted_rho_horizon": _traj("admission_control", immediate),
            "landing_cycles": ACTION_LANDING_CYCLES["admission_control"],
            "loss": round(f / F_MAX, 4), "loss_raw": round(f, 4),
            "delay_norm": 0.0, "delay_s": 0.0,
            "resource": 0.0, "resource_raw": 0.0,
            "disruption": 0.0,
            "magnitude_derivation":
                f"f_raw = 1 - {t}/{rho:.3f} = {f_raw:.4f}, "
                f"clamped to [0, {F_MAX}] = {f:.4f}",
        })

        # rate_limit: only when the resource category implies a signalling / legitimacy problem
        if state["resource_category"] in LEGITIMACY_RESOURCES:
            tr_raw = 1.0 - t/rho
            tr = _clamp(tr_raw, 0.0, T_MAX)
            immediate = rho * (1.0 - tr)
            candidates.append({
                "action": "rate_limit",
                "target_nf": "gnb_ingress",
                "params": {"scope": "signalling",
                           "throttle_fraction": round(tr, 4),
                           "policy": "throttle_new_requests"},
                "predicted_rho": immediate,
                "predicted_rho_horizon": _traj("rate_limit", immediate),
                "landing_cycles": ACTION_LANDING_CYCLES["rate_limit"],
                "loss": round(tr / T_MAX, 4), "loss_raw": round(tr, 4),
                "delay_norm": 0.0, "delay_s": 0.0,
                "resource": 0.0, "resource_raw": 0.0,
                "disruption": 0.0,
                "magnitude_derivation":
                    f"throttle_raw = 1 - {t}/{rho:.3f} = {tr_raw:.4f}, "
                    f"clamped to [0, {T_MAX}] = {tr:.4f}",
            })

        # vertical_scale: pct = 100 * (rho/target - 1)
        pct_raw = 100.0 * (rho/t - 1.0)
        pct = _clamp(pct_raw, 0.0, PCT_MAX)
        immediate = rho / (1.0 + pct/100.0)
        candidates.append({
            "action": "vertical_scale",
            "target_nf": state["target_nf"],
            "params": {"capacity_increase_pct": round(pct, 2)},
            "predicted_rho": immediate,
            "predicted_rho_horizon": _traj("vertical_scale", immediate),
            "landing_cycles": ACTION_LANDING_CYCLES["vertical_scale"],
            "loss": 0.0, "loss_raw": 0.0,
            "delay_norm": 0.0, "delay_s": 0.0,
            "resource": round(pct / PCT_MAX, 4), "resource_raw": round(pct, 2),
            "disruption": 0.0,
            "magnitude_derivation":
                f"pct_raw = 100*({rho:.3f}/{t} - 1) = {pct_raw:.2f}%, "
                f"clamped to [0, {PCT_MAX}] = {pct:.2f}%",
        })

        # horizontal_scale: k = ceil(N * (rho/target - 1))
        k_raw = math.ceil(N * (rho/t - 1.0))
        k = int(_clamp(k_raw, 0, K_MAX))
        immediate = rho * N / (N + k) if (N + k) > 0 else rho
        candidates.append({
            "action": "horizontal_scale",
            "target_nf": state["target_nf"],
            "params": {"current_instances": N,
                       "desired_instances": N + k,
                       "delta": k},
            "predicted_rho": immediate,
            "predicted_rho_horizon": _traj("horizontal_scale", immediate),
            "landing_cycles": ACTION_LANDING_CYCLES["horizontal_scale"],
            "loss": 0.0, "loss_raw": 0.0,
            "delay_norm": 0.0, "delay_s": 0.0,
            "resource": round(k / K_MAX, 4), "resource_raw": k,
            "disruption": 0.0,
            "magnitude_derivation":
                f"k_raw = ceil({N}*({rho:.3f}/{t} - 1)) = {k_raw}, "
                f"clamped to [0, {K_MAX}] = {k}",
        })

        # traffic_shaping user plane only, real bitrate ceiling.
   
        worst_band_lower = (state["worst_band"] or "").lower()
        if (state["plane"] == "user"
                and _is_bitrate_metric(metric)
                and ceiling is not None
                and worst_band_lower != "failure"):
            drain_bps = t * ceiling
            burst_bits = SHAPE_BURST_FRACTION_OF_CEILING * ceiling
            peak_queue_bits = None
            buffer_cap_bits = None
            overflow = False

            if SHAPING_MODE == "queue_aware":
                arrival_bps = [n * ceiling for n in natural]
                q = 0.0
                queue_series = []
                for a_bps in arrival_bps:
                    q = max(0.0, q + (a_bps - drain_bps) * CYCLE_DURATION_S)
                    queue_series.append(q)
                peak_queue_bits = max(queue_series) if queue_series else 0.0
                delay_s = (peak_queue_bits / drain_bps) if drain_bps > 0 else 0.0
                buffer_cap_bits = SHAPE_BUFFER_SECONDS * ceiling
                overflow = peak_queue_bits > buffer_cap_bits
            else:
                delay_s = (burst_bits / drain_bps) if drain_bps > 0 else 0.0

            immediate = t  
            candidates.append({
                "action": "traffic_shaping",
                "target_nf": state["target_nf"],
                "params": {"policy": "shape", "shaping_mode": SHAPING_MODE,
                           "mbr_bps": round(drain_bps, 1),
                           "burst_bits": round(burst_bits, 1)},
                "predicted_rho": immediate,
                "predicted_rho_horizon": _traj("traffic_shaping", immediate),
                "landing_cycles": ACTION_LANDING_CYCLES["traffic_shaping"],
                "loss": 0.0, "loss_raw": 0.0,
                "delay_norm": round(_clamp(delay_s / L_MAX, 0.0, 1.0), 4),
                "delay_s": round(delay_s, 4),
                "resource": 0.0, "resource_raw": 0.0,
                "disruption": 0.0,
                "_queue_overflow": overflow,
                "_peak_queue_bits": peak_queue_bits,
                "_buffer_cap_bits": buffer_cap_bits,
                "magnitude_derivation":
                    f"shaping_mode={SHAPING_MODE}, drain={drain_bps:.0f} bps "
                    f"(target*ceiling), burst={burst_bits:.0f} bits, "
                    f"delay={delay_s*1000:.1f} ms" +
                    (f", peak_queue={peak_queue_bits:.0f} bits vs "
                     f"buffer_cap={buffer_cap_bits:.0f} bits"
                     if SHAPING_MODE == "queue_aware" else ""),
            })

    state["candidates"] = candidates
    _record_step(
        state, "generate_candidates", "planning",
        {"rho": rho, "target_rho": t, "N": N, "ceiling": ceiling,
         "metric_name": metric, "resource_category": state["resource_category"],
         "horizon_steps": HORIZON_STEPS},
        {"n_candidates": len(candidates),
         "actions": [c["action"] for c in candidates]},
        {"source": "algebra of rho = TF/R + horizon projection",
         "db": "in-memory",
         "rule": "magnitude to reach target: f=1-t/rho, pct=100(rho/t-1), "
                 "k=ceil(N(rho/t-1)), shaping mbr=t*ceiling; clamped to "
                 "[F_MAX, T_MAX, PCT_MAX, K_MAX]; each candidate projected "
                 f"over {HORIZON_STEPS} cycles via ACTION_LANDING_CYCLES",
         "refs": ["rho=TF/R model (rca_core)",
                  "3GPP TS 29.244 (QER/MBR, shaping only)"]},
        claim=f"generated {len(candidates)} candidate(s) with derived "
              f"magnitudes and horizon trajectories")
    print(f"[Candidates] {len(candidates)}: "
          f"{[c['action'] for c in candidates]}")
    return state


def filter_constraints(state: MitigationState) -> MitigationState:
    recent = _load_recent_history(state["interface"])
    aggressive_recent = [r for r in recent
                         if r.get("action") in AGGRESSIVE_ACTIONS]
    state["cooldown_active"] = bool(aggressive_recent)
    state["last_action_ts"] = (max(r["timestamp"] for r in aggressive_recent)
                               if aggressive_recent else None)

    feasible = []
    reasons_dropped = []
    for c in state["candidates"]:
        a = c["action"]
        keep = True
        why = None
        if state["status"] == "ambiguous" and a not in ("none", "admission_control"):
            keep = False
            why = "RCA status=ambiguous permits only none/admission_control"
        elif state["cooldown_active"] and a in AGGRESSIVE_ACTIONS:
            keep = False
            why = f"{a} is aggressive; cooldown active on {state['interface']}"
        elif state["resource_category"] in LEGITIMACY_RESOURCES \
                and a not in ("none", "rate_limit"):
            keep = False
            why = (f"resource_category={state['resource_category']} indicates "
                   f"legitimacy problem; only none/rate_limit are effective")
        elif state["feedback_loop"] and a == "vertical_scale":
            keep = False
            why = "feedback_loop=True: resize would be outgrown before it lands"
        elif a == "vertical_scale" \
                and state["resource_category"] not in RESIZABLE_RESOURCES:
            keep = False
            why = (f"resource_category={state['resource_category']} not "
                   f"resizable-per-instance; vertical_scale infeasible")
        elif a == "traffic_shaping" and SHAPING_MODE == "queue_aware" \
                and c.get("_queue_overflow"):
            keep = False
            why = (f"queue_aware shaping: projected peak buffer occupancy "
                   f"{c.get('_peak_queue_bits', 0):.0f} bits exceeds "
                   f"SHAPE_BUFFER_SECONDS*ceiling="
                   f"{c.get('_buffer_cap_bits', 0):.0f} bits within the "
                   f"{HORIZON_STEPS}-cycle horizon; shaping would overflow "
                   f"into loss, which this action's model does not cover")

        if keep:
            feasible.append(c)
        else:
            reasons_dropped.append({"action": a, "why": why})

    state["feasible"] = feasible
    _record_step(
        state, "filter_constraints", "planning",
        {"n_in": len(state["candidates"]),
         "status": state["status"], "cooldown_active": state["cooldown_active"],
         "resource_category": state["resource_category"],
         "feedback_loop": state["feedback_loop"],
         "calibrated": state["calibrated"], "shaping_mode": SHAPING_MODE},
        {"n_out": len(feasible),
         "kept": [c["action"] for c in feasible],
         "dropped": reasons_dropped},
        {"source": "RCA conclusion + local cooldown history + horizon "
                   "buffer projection",
         "db": "in-memory + mitigation_history.jsonl",
         "rule": "5 hard feasibility constraints (ambiguity, cooldown, "
                 "legitimacy, feedback_loop, resizability) plus a "
                 "queue_aware-only shaping-overflow check",
         "refs": ["AGGRESSIVE_ACTIONS, LEGITIMACY_RESOURCES, RESIZABLE_RESOURCES",
                  "SHAPE_BUFFER_SECONDS (modelling choice, queue_aware only)"]},
        claim=f"filtered {len(state['candidates'])} -> {len(feasible)} feasible")
    if reasons_dropped:
        print(f"[Feasibility] dropped: "
              + ", ".join(f"{d['action']} ({d['why']})" for d in reasons_dropped))
    print(f"[Feasibility] kept: {[c['action'] for c in feasible]}")
    return state


def evaluate_objective(state: MitigationState) -> MitigationState:
    t = state["target_rho"]
    scored = []
    for c in state["feasible"]:
        traj = c.get("predicted_rho_horizon") or []
        n_steps = len(traj) or 1
        e_rho_raw = 0.0
        # normalise against target_rho squared. Using HORIZON_RHO_CAP as the
        # denominator crushed everyday errors down to nothing and 'none' won on
        # real congestion. target_rho itself is the right reference: an error
        # equal to target size (rho at 2x target) lands at 1.0.
        worst_case = max(t ** 2, 1e-9)
        for v in traj:
            pr = v if v is not None else t  # missing signal counts as zero error
            e_rho_raw += (pr - t) ** 2
        # average over horizon, not sum. Summing hides horizon length inside the score.
        e_rho = _clamp(e_rho_raw / n_steps / worst_case, 0.0, 1.0)

        loss = c.get("loss", 0.0)
        delay_norm = c.get("delay_norm", 0.0)
        resource = c.get("resource", 0.0)
        disruption = c.get("disruption", 0.0)

        score = (W_RHO * e_rho + W_LOSS * loss + W_DELAY * delay_norm
                 + W_RESOURCE * resource + W_DISRUPTION * disruption)

        scored.append({**c, "score": score,
                       "score_terms": {
                           "e_rho_horizon_sum": e_rho_raw,
                           "e_rho_normalised": e_rho,
                           "weighted_e_rho": W_RHO * e_rho,
                           "weighted_loss": W_LOSS * loss,
                           "weighted_delay": W_DELAY * delay_norm,
                           "weighted_resource": W_RESOURCE * resource,
                           "weighted_disruption": W_DISRUPTION * disruption}})
    scored.sort(key=lambda x: x["score"])
    state["scored"] = scored

    _record_step(
        state, "evaluate_objective", "planning",
        {"n_feasible": len(state["feasible"]), "target_rho": t,
         "horizon_steps": HORIZON_STEPS,
         "weights": {"rho": W_RHO, "loss": W_LOSS, "delay": W_DELAY,
                     "resource": W_RESOURCE, "disruption": W_DISRUPTION}},
        {"scored_actions": [{"action": s["action"],
                             "predicted_rho_horizon": s["predicted_rho_horizon"],
                             "score": s["score"], "score_terms": s["score_terms"]}
                            for s in scored]},
        {"source": "receding-horizon MPC-style objective "
                   "(Rawlings, Mayne & Diehl 2017 Sec 1.3); normalisation "
                   "per Marler & Arora (2004)",
         "db": "in-memory",
         "rule": "W_RHO*sum(rho_err^2 over horizon) + W_LOSS*loss + "
                 "W_DELAY*delay + W_RESOURCE*resource + W_DISRUPTION*disruption",
         "refs": ["Rawlings, Mayne & Diehl (2017), MPC 2e Sec 1.3",
                  "Marler & Arora (2004), Structural and Multidisciplinary "
                  "Optimization 26:369-395"]},
        claim=f"scored {len(scored)} candidate(s); "
              f"best={scored[0]['action']} with score={scored[0]['score']:.4f}"
              if scored else "no feasible candidates to score")
    if scored:
        print(f"[Objective] scored (best first):")
        for s in scored[:6]:
            st_ = s["score_terms"]
            print(f"  {s['action']:>18}: e_rho_sum={st_['e_rho_horizon_sum']:.4f} "
                  f"loss_w={st_['weighted_loss']:.4f} delay_w={st_['weighted_delay']:.4f} "
                  f"res_w={st_['weighted_resource']:.4f} score={s['score']:.4f}")
    return state


def select_best(state: MitigationState) -> MitigationState:
    if not state["scored"]:
        state["decision"] = {
            "action": "none", "target_nf": state["target_nf"],
            "target_params": {},
            "predicted_rho": state["rho"],
            "predicted_rho_horizon": None,
            "score": None,
            "reasoning_trail": [
                "No feasible candidate satisfied all constraints. Fell back "
                "to 'none' as the only safe action. This typically means the "
                "RCA reported ambiguous with a resource category that also "
                "forbids admission_control, an unusual combination worth "
                "investigating in the RCA layer, not here.",
            ],
            "source_refs": ["mitigation constraint filter (feasibility empty)"],
        }
    elif state.get("deadband_suppressed"):
        best = state["scored"][0]
        state["decision"] = {
            "action": "none", "target_nf": best["target_nf"],
            "target_params": {},
            "predicted_rho": best["predicted_rho"],
            "predicted_rho_horizon": best.get("predicted_rho_horizon"),
            "score": best["score"],
            "reasoning_trail": [
                f"RCA reports {state['status']} on {state['interface']} "
                f"({state['plane']} plane) with rho={state['rho']} "
                f"(target={state['target_rho']}, "
                f"deadband half-width={DEADBAND_HALF_WIDTH}).",
                f"rho is within the deadband (target, target+"
                f"{DEADBAND_HALF_WIDTH}]; no action taken to avoid reacting "
                f"to a marginal excursion. This differs from a 'no feasible "
                f"candidate' outcome: action candidates were never "
                f"generated for this cycle.",
            ],
            "source_refs": ["DEADBAND_HALF_WIDTH (modelling choice)"],
        }
    else:
        best = state["scored"][0]
        trail = [
            f"RCA reports {state['status']} on {state['interface']} "
            f"({state['plane']} plane) with rho={state['rho']} "
            f"(target={state['target_rho']}).",
            f"Enumerated {len(state['candidates'])} candidate action(s) over a "
            f"{HORIZON_STEPS}-cycle receding horizon; {len(state['feasible'])} "
            f"survived feasibility constraints.",
            f"Objective: {W_RHO}*sum(rho_err^2 over horizon) + {W_LOSS}*loss + "
            f"{W_DELAY}*delay + {W_RESOURCE}*resource + {W_DISRUPTION}*disruption. "
            f"Winner: {best['action']} with score {best['score']:.4f} "
            f"(horizon rho-error sum={best['score_terms']['e_rho_horizon_sum']:.4f}, "
            f"weighted loss={best['score_terms']['weighted_loss']:.4f}, "
            f"weighted delay={best['score_terms']['weighted_delay']:.4f}, "
            f"weighted resource={best['score_terms']['weighted_resource']:.4f}, "
            f"weighted disruption={best['score_terms']['weighted_disruption']:.4f}).",
            f"Magnitude derivation: {best.get('magnitude_derivation', 'n/a')}.",
            f"Predicted rho trajectory over horizon: "
            f"{best.get('predicted_rho_horizon')}.",
        ]
        state["decision"] = {
            "action": best["action"],
            "target_nf": best["target_nf"],
            "target_params": best["params"],
            "predicted_rho": best["predicted_rho"],
            "predicted_rho_horizon": best.get("predicted_rho_horizon"),
            "score": best["score"],
            "score_terms": best.get("score_terms"),
            "reasoning_trail": trail,
            "source_refs": [
                "supervisor rho = (L/a)/R model (rca_core)",
                "Rawlings, Mayne & Diehl (2017), MPC 2e Sec 1.3",
                "Kleinrock (1975), Queueing Systems Vol 1 Sec 2.4",
                "Marler & Arora (2004), Structural and Multidisciplinary "
                "Optimization 26:369-395",
                "3GPP TS 23.501 Sec 4.2 (interface -> NF)",
                "3GPP TS 29.244 (QER / MBR, traffic_shaping only)",
                "Le Boudec & Thiran (2001), Network Calculus (token-bucket "
                "delay bound b/r, traffic_shaping 'rate_cap' mode only)",
                "3GPP TS 23.501 Table 5.7.4-1, 5QI 9 (L_MAX)",
                "ETSI NFV-IFA (scaling operation names, exact document number "
                "to re-verify before submission)",
            ],
        }

    _record_step(
        state, "select_best", "decide",
        {"n_scored": len(state["scored"]), "deadband_suppressed":
            state.get("deadband_suppressed", False)},
        {"action": state["decision"]["action"],
         "target_nf": state["decision"]["target_nf"],
         "params": state["decision"].get("target_params", {}),
         "predicted_rho": state["decision"].get("predicted_rho"),
         "predicted_rho_horizon": state["decision"].get("predicted_rho_horizon"),
         "score": state["decision"].get("score")},
        {"source": "argmin over feasible candidates",
         "db": "in-memory",
         "rule": "minimum-score feasible candidate wins",
         "refs": ["MPC-style selection (Rawlings, Mayne & Diehl 2017 Sec 1.3)"]},
        claim=f"selected: {state['decision']['action']} on "
              f"{state['decision']['target_nf']} "
              f"(predicted_rho={state['decision'].get('predicted_rho')})")
    _pr = state['decision'].get('predicted_rho')
    _pr_str = f"predicted_rho={_pr:.4f}" if isinstance(_pr, (int, float)) else "predicted_rho=n/a"
    print(f"[Decide] {state['decision']['action']} on "
          f"{state['decision']['target_nf']} ({_pr_str})")
    return state


def justify(state: MitigationState) -> MitigationState:
  
    d = state["decision"]
    if d.get("action") == "none":
        if state.get("deadband_suppressed"):
            state["narrative"] = (
                "Utilisation is within the deadband around target. No "
                "action issued to avoid reacting to a marginal excursion; "
                "continue monitoring.")
        else:
            state["narrative"] = (
                "No congestion of a form this agent can act on. Continue "
                "monitoring; no action issued.")
        return state
    trail_txt = "\n".join(f"- {t}" for t in d.get("reasoning_trail", []))
    refs_txt  = "\n".join(f"- {r}" for r in d.get("source_refs", []))
    prompt = (
        "You are the justification module of an autonomous 5G congestion "
        "mitigation system. A constrained-optimization engine has ALREADY "
        "chosen the action and computed every number below, including a "
        "multi-cycle predicted utilisation trajectory. Your ONLY job is "
        "to write a clear 3-5 sentence engineer-facing justification of the "
        "chosen decision. STRICT RULES: state only facts present below; do "
        "not invent numbers, causes, or targets not listed; keep it precise, "
        "no marketing language.\n\n"
        f"CHOSEN ACTION: {d['action']}\n"
        f"TARGET NF: {d.get('target_nf')}\n"
        f"TARGET PARAMETERS: {json.dumps(d.get('target_params', {}))}\n"
        f"PREDICTED POST-ACTION RHO (immediate): {d.get('predicted_rho')}\n"
        f"PREDICTED RHO TRAJECTORY (over horizon): {d.get('predicted_rho_horizon')}\n\n"
        f"REASONING TRAIL:\n{trail_txt}\n\n"
        f"SOURCES:\n{refs_txt}\n\n"
        "JUSTIFICATION:")
    narrative = _ollama_generate(prompt)
    if narrative and len(narrative) > 40:
        state["narrative"] = narrative
    else:
        state["narrative"] = " ".join(d.get("reasoning_trail", []))
    _record_step(
        state, "justify", "narration",
        {"prompt_len": len(prompt)},
        {"narrative_chars": len(state["narrative"] or ""),
         "llm_used": narrative is not None and len(narrative) > 40},
        {"source": "rca-qwen narration" if (narrative and len(narrative) > 40)
                   else "skeleton (LLM unavailable)",
         "db": "none",
         "rule": "MODEL JUDGMENT (narration only; decision fixed before this)",
         "refs": []},
        claim=f"justification generated ({len(state['narrative'] or '')} chars)")
    print(f"[Justify] narrative ({len(state['narrative'])} chars)")
    return state


def format_output(state: MitigationState) -> MitigationState:
    d = state["decision"]
    now = time.time()
    state["output"] = {
        "agent":        "mitigation",
        "timestamp":    now,
        "input_source": "rca_engine (direct in-process handoff or A2A "
                        "JSON-RPC via MitigationExecutor)",
        "interface":    state["interface"],
        "target_nf":    d.get("target_nf"),
        "action":       d["action"],
        "target_params": d.get("target_params", {}),
        "predicted_rho": d.get("predicted_rho"),
        "predicted_rho_horizon": d.get("predicted_rho_horizon"),
        "score":        d.get("score"),
        "score_terms":  d.get("score_terms"),
        "reasoning_trail": d.get("reasoning_trail", []),
        "source_refs":  d.get("source_refs", []),
        "narrative":    state.get("narrative"),
        "exec_log":     state.get("exec_log", []),
        "confidence_gate_fired": state["status"] == "ambiguous",
        "deadband_suppressed": state.get("deadband_suppressed", False),
        "cooldown_active_at_decision": state["cooldown_active"],
        "target_provenance": state.get("target_provenance", ""),
        "current_instances_at_decision": state["current_instances"],
        "shaping_mode": SHAPING_MODE,
        "inputs_from_rca": {
            "status":         state["status"],
            "worst_band":     state["worst_band"],
            "rho":            state["rho"],
            "rho_source":     state.get("rho_source"),
            "target_rho":     state["target_rho"],
            "ceiling":        state.get("ceiling"),
            "metric_name":    state.get("metric_name"),
            "rho_rate_per_s": state.get("rho_rate_per_s"),
            "eta_to_congestion_s": state.get("eta_to_congestion_s"),
            "plane":          state["plane"],
            "resource_category": state["resource_category"],
            "feedback_loop":  state["feedback_loop"],
            "calibrated":     state["calibrated"],
        },
    }

    if d["action"] == "horizontal_scale":
        new_count = d.get("target_params", {}).get("desired_instances",
                                                    state["current_instances"])
        _record_scale(state["target_nf"], new_count, "horizontal_scale")

    if d["action"] not in ("none",):
        _append_history({"timestamp": now,
                         "interface": state["interface"],
                         "target_nf": d.get("target_nf"),
                         "action": d["action"]})

    _append_history_full({"timestamp": now,
                          "interface": state["interface"],
                          "target_nf": d.get("target_nf"),
                          "action": d["action"],
                          "worst_band": state.get("worst_band"),
                          "status": state.get("status"),
                          "deadband_suppressed": state.get("deadband_suppressed", False)})

    _record_step(
        state, "format_output", "commit",
        {"action": d["action"], "target_nf": d.get("target_nf")},
        {"output_keys": sorted(state["output"].keys())},
        {"source": "mitigation output assembly",
         "db": "deployed_instances.json + mitigation_history.jsonl",
         "rule": "persist state on scale actions; log history on any action",
         "refs": []},
        claim=f"final output assembled; action={d['action']} target={d.get('target_nf')}")

    print(f"[Format] decision ready -- action={d['action']} "
          f"target={d.get('target_nf')} params={d.get('target_params')}")
    return state


def build_mitigation_agent():
    g = StateGraph(MitigationState)
    g.add_node("receive_conclusion",  receive_conclusion)
    g.add_node("resolve_target",      resolve_target)
    g.add_node("generate_candidates", generate_candidates)
    g.add_node("filter_constraints",  filter_constraints)
    g.add_node("evaluate_objective",  evaluate_objective)
    g.add_node("select_best",         select_best)
    g.add_node("justify",             justify)
    g.add_node("format",              format_output)
    g.set_entry_point("receive_conclusion")
    g.add_edge("receive_conclusion",  "resolve_target")
    g.add_edge("resolve_target",      "generate_candidates")
    g.add_edge("generate_candidates", "filter_constraints")
    g.add_edge("filter_constraints",  "evaluate_objective")
    g.add_edge("evaluate_objective",  "select_best")
    g.add_edge("select_best",         "justify")
    g.add_edge("justify",             "format")
    g.add_edge("format",              END)
    return g.compile()


def mitigate(conclusion):
    agent = build_mitigation_agent()
    state: MitigationState = {
        "conclusion": conclusion,
        "interface": "", "monitored_interfaces": [], "plane": "",
        "target_rho": DEFAULT_TARGET_RHO,
        "rho": None, "rho_source": "unavailable",
        "ceiling": None, "metric_name": None,
        "rho_rate_per_s": None, "eta_to_congestion_s": None,
        "resource_category": None, "feedback_loop": False,
        "status": "", "worst_band": "", "calibrated": False,
        "target_nf": "", "current_instances": 1, "target_provenance": "",
        "candidates": [], "feasible": [], "scored": [],
        "decision": {}, "narrative": None, "output": {},
        "exec_log": [],
        "cooldown_active": False, "last_action_ts": None,
        "deadband_suppressed": False,
    }
    result = agent.invoke(state)
    return result["output"]



# A2A server.
import os
import asyncio

from starlette.applications import Starlette
from a2a.types import (AgentCard, AgentSkill, AgentCapabilities, AgentInterface)
from a2a.utils import TransportProtocol
from a2a.server.agent_execution import AgentExecutor
from a2a.server.tasks import InMemoryTaskStore, TaskUpdater
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.routes import create_jsonrpc_routes, create_agent_card_routes
from a2a.helpers.proto_helpers import new_task_from_user_message, new_text_part

PORT     = int(os.environ.get("MITIGATION_A2A_PORT", "9101"))
HOST     = os.environ.get("MITIGATION_A2A_HOST", "0.0.0.0")
ADV_HOST = os.environ.get("A2A_ADVERTISE_HOST", "localhost")
CARD_URL = f"http://{ADV_HOST}:{PORT}/"


def build_card() -> AgentCard:
    skill = AgentSkill(
        id="mitigate",
        name="Congestion Mitigation Decision",
        description=("Given the RCA engine's congestion conclusion, computes "
                     "one mitigation action by receding-horizon constrained "
                     "optimization over a six-action catalog (none, "
                     "admission_control, rate_limit, vertical_scale, "
                     "horizontal_scale, traffic_shaping). Every action's "
                     "immediate magnitude is derived from the rho = TF/R "
                     "model and projected over a multi-cycle horizon; the "
                     "objective is a horizon-summed quadratic deviation "
                     "from a plane-keyed target utilization plus four "
                     "normalised cost terms (loss, delay, resource, "
                     "disruption)."),
        tags=["5g", "congestion", "mitigation", "scaling", "shaping", "mpc"],
        input_modes=["application/json", "text"],
        output_modes=["application/json", "text"])
    return AgentCard(
        name="Mitigation Agent",
        description=("Autonomous 5G congestion mitigation. Consumes an RCA "
                     "conclusion over A2A and returns a mitigation decision. "
                     "Decision is deterministic (delete the LLM, decision "
                     "unchanged); the LLM writes narration only."),
        version="2.0.0",
        capabilities=AgentCapabilities(streaming=True),
        default_input_modes=["application/json", "text"],
        default_output_modes=["application/json", "text"],
        skills=[skill],
        supported_interfaces=[AgentInterface(
            url=CARD_URL, protocol_binding=TransportProtocol.JSONRPC)])


class MitigationExecutor(AgentExecutor):
    async def execute(self, context, event_queue) -> None:
        raw = context.get_user_input() or ""
        try:
            payload = json.loads(raw)
            conclusion = payload.get("conclusion", payload)
        except (json.JSONDecodeError, AttributeError):
            conclusion = {}
        task = context.current_task
        if task is None:
            task = new_task_from_user_message(context.message)
            await event_queue.enqueue_event(task)
        updater = TaskUpdater(event_queue, task.id, task.context_id)
        await updater.start_work()
        try:
            decision = await asyncio.to_thread(mitigate, conclusion)
        except Exception as e:
            decision = {"agent": "mitigation", "action": "error", "error": str(e)}
        await updater.add_artifact(
            [new_text_part(json.dumps(decision), media_type="application/json")],
            name="mitigation_decision")
        await updater.complete()

    async def cancel(self, context, event_queue) -> None:
        raise NotImplementedError("Mitigation tasks are short and not cancellable")


def build_app() -> Starlette:
    card = build_card()
    handler = DefaultRequestHandler(
        agent_executor=MitigationExecutor(),
        task_store=InMemoryTaskStore(),
        agent_card=card)
    routes = create_jsonrpc_routes(handler, "/") + create_agent_card_routes(card)
    return Starlette(routes=routes)


CARD = build_card()
app = build_app()

if __name__ == "__main__":
    import uvicorn
    print(f"[Mitigation A2A] serving on {HOST}:{PORT} "
          f"(Agent Card advertises {CARD_URL})")
    uvicorn.run(app, host=HOST, port=PORT)
