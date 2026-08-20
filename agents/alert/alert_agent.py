#!/usr/bin/env python3
"""Alert Agent

Author: Heven Tafese

This agent detects rate of change (velocity) congestion for any 5G core MnF.

It fits a short quadratic over recent samples per interface, classifies severity by z-score against a locally calibrated baseline, and confirms ambiguous readings with SPRT. It publishes one observation per interface per tick to MCP.

This agent calibrates baseline only from samples strictly before traffic starts, and freezes it the instant traffic does start. Zero-variance metrics (a counter reading exact zero when idle) switch to a rolling window against their own recent active traffic behaviour once enough post-transition history exists.
"""
import json, time, sys, os, math, glob, asyncio, logging
from typing import TypedDict, Optional, Any
from langgraph.graph import StateGraph, END
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from fastmcp import Client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("alert")

MCP_URL = os.environ.get("MCP_URL", "http://localhost:9000/mcp")
MCP_TIMEOUT_S = 5.0
MNF_DATA_DIR = os.environ.get("MNF_DATA_DIR", os.path.expanduser("~/data"))
DISCOVERY_INTERVAL = 5.0
FILE_INACTIVE_THRESHOLD_S = 60.0

MAX_HISTORY          = 60
REGRESSION_WINDOW    = 8
CALIBRATION_MINIMUM_SAMPLES = 3
LOAD_START_EPSILON = 1.0
NO_LOAD_SIGNAL_FALLBACK_TICKS = 20
ZERO_VARIANCE_EPSILON = 1.0
ROLLING_WINDOW_SIZE = 20
ROLLING_MINIMUM_SAMPLES = 8
# roughly mirrors onset to congestion ratio 1.0/0.85 ~= 1.18
INITIAL_LEVEL_ELEVATION_FACTOR = 1.15
SPRT_ALPHA           = 0.05
SPRT_BETA            = 0.10
SPRT_DELTA_K         = 2.0

Z_LOW    = 1.0
Z_MEDIUM = 2.0
Z_HIGH   = 3.0

ETA_MAX_HORIZON      = 300.0
SATURATION_RATIO     = 0.90

SPRT_UPPER = math.log((1 - SPRT_BETA) / SPRT_ALPHA)
SPRT_LOWER = math.log(SPRT_BETA / (1 - SPRT_ALPHA))

SEVERITY_ORDER = ["normal", "low", "medium", "high"]
SUSTAINED_TICKS_THRESHOLD = 10


def _group(key):
    if "cpu_percent" in key:                                        return "cpu"
    if "throughput_bps" in key:                                     return "throughput"
    if "packet_rate_pps" in key:                                    return "packet_rate"
    if "message_rate" in key:                                       return "message_rate"
    if "registration_rate" in key or "registration_request_rate" in key: return "registration_rate"
    if "conn_established" in key:                                   return "connections"
    if "open_fds" in key:                                           return "file_descriptors"
    if "load_avg" in key:                                           return "load"
    if "memory_percent" in key and "used" not in key:              return "memory"
    if "drop" in key.lower():                                       return "drops"
    if "error" in key.lower():                                      return "errors"
    return None


GROUP_CATEGORY = {
    "throughput": "load", "packet_rate": "load", "message_rate": "load",
    "registration_rate": "load", "connections": "load",
    "cpu": "resource", "memory": "resource", "load": "resource", "file_descriptors": "resource",
    "drops": "distress", "errors": "distress",
}

IGNORED = {
    "timestamp","layer","plane","interface","source","scenario",
    "elapsed_s","sample_id","amf_pid","smf_pid","upf_pid",
    "amf_threads","smf_threads","upf_threads","gnb_threads","gnb_connected_ues",
    "amf_cpu_rolling","smf_cpu_rolling","upf_cpu_rolling",
    "amf_conn_time_wait","smf_conn_time_wait","upf_conn_time_wait",
    "amf_conn_total","smf_conn_total","upf_conn_total",
    "amf_fd_delta","smf_fd_delta","upf_fd_delta",
    "amf_overhead_fraction","smf_overhead_fraction","upf_overhead_fraction",
    "amf_congestion","smf_congestion","upf_congestion",
    "congestion_detected","system_failing",
    "amf_open_fds","smf_open_fds","upf_open_fds",
    "amf_memory_mb","smf_memory_mb","upf_memory_mb",
    "amf_memory_percent","smf_memory_percent","upf_memory_percent",
    "sys_memory_percent","memory_used_mb",
}


def _current_levels(current_raw, groups):
    levels = {}
    for grp in groups:
        for key, v_val in current_raw.items():
            if _group(key) == grp and isinstance(v_val, (int, float)):
                levels[grp] = v_val
                break
    return levels


class _MnfFileReader:
    def __init__(self, path: str):
        self.path = path
        self._offset = 0
        self._interface_buffer: dict = {}

    def read_new_lines(self) -> list:
        try:
            size = os.path.getsize(self.path)
        except FileNotFoundError:
            return []
        if size < self._offset:
            logger.warning("%s shrank (rotated/truncated); resetting offset", self.path)
            self._offset = 0
        if size == self._offset:
            return []
        with open(self.path, "rb") as f:
            f.seek(self._offset)
            new_bytes = f.read()
        if new_bytes.endswith(b"\n"):
            complete_bytes = new_bytes
        else:
            last_nl = new_bytes.rfind(b"\n")
            if last_nl == -1:
                return []
            complete_bytes = new_bytes[: last_nl + 1]
        self._offset += len(complete_bytes)
        records = []
        for raw in complete_bytes.split(b"\n"):
            if not raw.strip():
                continue
            try:
                rec = json.loads(raw.decode("utf-8", errors="replace"))
            except json.JSONDecodeError:
                logger.warning("Malformed record in %s, skipped: %.200r", self.path, raw)
                continue
            records.append(rec)
        return records

    def _drain_or_read(self, interface: str) -> list:
        buffered = self._interface_buffer.pop(interface, None)
        if buffered:
            return buffered
        fresh = self.read_new_lines()
        if not fresh:
            return []
        by_iface: dict = {}
        for rec in fresh:
            by_iface.setdefault(rec.get("interface", "unknown"), []).append(rec)
        mine = by_iface.pop(interface, [])
        for other_iface, other_recs in by_iface.items():
            self._interface_buffer.setdefault(other_iface, []).extend(other_recs)
        return mine

    def drain_all_buffered(self) -> dict:
        drained = self._interface_buffer
        self._interface_buffer = {}
        return drained


class AlertState(TypedDict):
    mnf_path:          str
    layer:             str
    plane:             str
    interface:         str
    source:            str
    is_live:           bool

    # must be declared, LangGraph drops undeclared keys from initial state silently
    pending_new_records: Optional[list]

    current_metrics:   dict
    previous_metrics:  dict
    metric_history:    list

    baseline:          dict
    velocity:          dict
    acceleration:      dict
    shape:             dict
    zscore:            dict
    severity:          dict

    sprt_llr:          dict
    sprt_direction:    dict
    resolved:          dict
    sustained_ticks:   dict

    firing_directions: dict
    pattern:           str
    pattern_confidence: str
    eta:               dict

    observation:       dict

    mcp_client:        Optional[Any]


def collect(state: AlertState) -> AlertState:
    pending = state.pop("pending_new_records", None)
    if not pending:
        return state
    new_current = pending[-1]
    state["previous_metrics"] = state.get("current_metrics") or new_current
    state["current_metrics"] = new_current
    hist = state.get("metric_history", [])
    hist.extend(pending)
    if len(hist) > MAX_HISTORY:
        hist = hist[-MAX_HISTORY:]
    state["metric_history"] = hist
    return state


def _quadratic_fit(t, x):
    n = len(t)
    if n < 3:
        return None, None, None
    t0 = t[-1]
    ts = [ti - t0 for ti in t]
    s0, s1, s2, s3, s4 = n, sum(ts), sum(ti**2 for ti in ts), sum(ti**3 for ti in ts), sum(ti**4 for ti in ts)
    sx0 = sum(x)
    sx1 = sum(xi * ti for xi, ti in zip(x, ts))
    sx2 = sum(xi * ti * ti for xi, ti in zip(x, ts))

    def det3(m):
        return (m[0][0]*(m[1][1]*m[2][2] - m[1][2]*m[2][1])
               - m[0][1]*(m[1][0]*m[2][2] - m[1][2]*m[2][0])
               + m[0][2]*(m[1][0]*m[2][1] - m[1][1]*m[2][0]))

    M = [[s0, s1, s2], [s1, s2, s3], [s2, s3, s4]]
    D = det3(M)
    if abs(D) < 1e-12:
        return None, None, None
    Ma = [[sx0, s1, s2], [sx1, s2, s3], [sx2, s3, s4]]
    Mb = [[s0, sx0, s2], [s1, sx1, s3], [s2, sx2, s4]]
    Mc = [[s0, s1, sx0], [s1, s2, sx1], [s2, s3, sx2]]
    a = det3(Ma) / D
    b = det3(Mb) / D
    c = det3(Mc) / D
    velocity_at_end = b
    acceleration = 2 * c
    fitted = [a + b*ti + c*ti*ti for ti in ts]
    residuals = [xi - fi for xi, fi in zip(x, fitted)]
    mean_res = sum(residuals) / n
    variance = sum((r - mean_res)**2 for r in residuals) / max(n - 3, 1)
    residual_std = math.sqrt(variance)
    return velocity_at_end, acceleration, residual_std


def _traffic_has_started(current_raw):
    for key, v_val in current_raw.items():
        grp = _group(key)
        if grp is not None and GROUP_CATEGORY.get(grp) == "load" and isinstance(v_val, (int, float)):
            if abs(v_val) > LOAD_START_EPSILON:
                return True
    return False


_warned_missing_timestamp = False

def compute_velocity(state: AlertState) -> AlertState:
    global _warned_missing_timestamp
    hist = state.get("metric_history", [])
    window = hist[-REGRESSION_WINDOW:]
    velocity = {}
    acceleration = {}

    if window and not _warned_missing_timestamp:
        missing = sum(1 for m in window if "timestamp" not in m)
        if missing:
            _warned_missing_timestamp = True
            print(f"[Alert] WARNING: {missing}/{len(window)} recent samples have no 'timestamp' "
                  f"field. Falling back to list position as a fake time value.")

    baseline = state.get("baseline", {})
    if len(window) >= 3:
        t = [m.get("timestamp", i) for i, m in enumerate(window)]
        meta = baseline.setdefault("_meta", {"traffic_started": False, "ticks_seen": 0, "load_field_ever_seen": False})
        meta["ticks_seen"] += 1
        if not meta["load_field_ever_seen"]:
            if any(_group(k) is not None and GROUP_CATEGORY.get(_group(k)) == "load" for k in window[-1]):
                meta["load_field_ever_seen"] = True
        just_started_now = (not meta["traffic_started"]) and _traffic_has_started(window[-1])
        no_load_signal_fallback = ((not meta["load_field_ever_seen"])
                                    and meta["ticks_seen"] >= NO_LOAD_SIGNAL_FALLBACK_TICKS
                                    and not meta["traffic_started"])
        seen_groups = set()
        for key in window[-1]:
            if key in IGNORED:
                continue
            grp = _group(key)
            if grp is None or grp in seen_groups:
                continue
            vals = [m.get(key) for m in window]
            if any(v is None or not isinstance(v, (int, float)) for v in vals):
                continue
            seen_groups.add(grp)

            v, a, sigma = _quadratic_fit(t, vals)
            if v is None:
                continue
            velocity[grp] = round(v, 6)
            acceleration[grp] = round(a, 6)

            gb = baseline.setdefault(grp, {"velocities": [], "mean": None, "std": None, "calibrated": False,
                                           "levels": [], "level_mean": None, "level_std": None, "level_calibrated": False,
                                           "zero_variance": False, "level_zero_variance": False,
                                           "rolling_velocities": [], "rolling_levels": []})
            gb["peak"] = max(gb.get("peak", vals[-1]), max(vals))

            if gb["calibrated"] and gb.get("zero_variance"):
                gb["rolling_velocities"].append(v)
                if len(gb["rolling_velocities"]) > ROLLING_WINDOW_SIZE:
                    gb["rolling_velocities"] = gb["rolling_velocities"][-ROLLING_WINDOW_SIZE:]
            if gb.get("level_calibrated") and gb.get("level_zero_variance"):
                gb["rolling_levels"].append(vals[-1])
                if len(gb["rolling_levels"]) > ROLLING_WINDOW_SIZE:
                    gb["rolling_levels"] = gb["rolling_levels"][-ROLLING_WINDOW_SIZE:]
                if gb.get("initial_active_level") is None and len(gb["rolling_levels"]) >= ROLLING_MINIMUM_SAMPLES:
                    first_n = gb["rolling_levels"][:ROLLING_MINIMUM_SAMPLES]
                    gb["initial_active_level"] = sum(first_n) / len(first_n)
                    print(f"[Alert] {grp}: initial active operating level captured at "
                          f"{gb['initial_active_level']:.4f}")

            if not gb["calibrated"]:
                if not meta["traffic_started"] and not just_started_now:
                    gb["velocities"].append(v)
                    gb["levels"].append(vals[-1])
                    if len(gb["velocities"]) > MAX_HISTORY:
                        gb["velocities"] = gb["velocities"][-MAX_HISTORY:]
                        gb["levels"] = gb["levels"][-MAX_HISTORY:]

                if just_started_now or meta["traffic_started"] or no_load_signal_fallback:
                    n = len(gb["velocities"])
                    reason = "before traffic started" if just_started_now or meta["traffic_started"] else "no traffic-volume field ever seen on this stream"
                    if n >= CALIBRATION_MINIMUM_SAMPLES:
                        mean = sum(gb["velocities"]) / n
                        var = sum((x - mean)**2 for x in gb["velocities"]) / max(n - 1, 1)
                        gb["mean"] = mean
                        gb["std"] = max(math.sqrt(var), 1e-6)
                        gb["zero_variance"] = (max(gb["velocities"]) - min(gb["velocities"])) < 1e-6
                        gb["calibrated"] = True
                        zv_note = " [zero-variance: direct rule, not z-score]" if gb["zero_variance"] else ""
                        print(f"[Alert] Baseline calibrated for {grp}: mean={mean:.4f} std={gb['std']:.4f} "
                              f"(from {n} samples, {reason}){zv_note}")

                        nl = len(gb["levels"])
                        lmean = sum(gb["levels"]) / nl
                        lvar = sum((x - lmean)**2 for x in gb["levels"]) / max(nl - 1, 1)
                        gb["level_mean"] = lmean
                        gb["level_std"] = max(math.sqrt(lvar), 1e-6)
                        gb["level_zero_variance"] = (max(gb["levels"]) - min(gb["levels"])) < 1e-6
                        gb["level_calibrated"] = True
                        lzv_note = " [zero-variance: direct rule, not z-score]" if gb["level_zero_variance"] else ""
                        print(f"[Alert] Level baseline calibrated for {grp}: mean={lmean:.4f} std={gb['level_std']:.4f} "
                              f"(from {nl} samples, {reason}){lzv_note}")
                    else:
                        print(f"[Alert] {grp}: traffic started with only {n} idle samples "
                              f"(need {CALIBRATION_MINIMUM_SAMPLES}), staying uncalibrated.")

        if just_started_now:
            meta["traffic_started"] = True

    state["velocity"] = velocity
    state["acceleration"] = acceleration
    state["baseline"] = baseline
    return state


def compute_acceleration(state: AlertState) -> AlertState:
    acceleration = state.get("acceleration", {})
    velocity = state.get("velocity", {})
    shape = {}
    for grp, acc in acceleration.items():
        vel = velocity.get(grp, 0.0)
        baseline = state.get("baseline", {}).get(grp, {})
        if baseline.get("calibrated") and not baseline.get("zero_variance"):
            sigma = baseline.get("std")
        elif baseline.get("calibrated") and baseline.get("zero_variance") and len(baseline.get("rolling_velocities", [])) >= ROLLING_MINIMUM_SAMPLES:
            rolling = baseline["rolling_velocities"]
            rn = len(rolling)
            rmean = sum(rolling) / rn
            rvar = sum((x - rmean) ** 2 for x in rolling) / max(rn - 1, 1)
            sigma = max(math.sqrt(rvar), 1e-6)
        else:
            sigma = None
        acc_floor = (0.5 * sigma) if sigma else max(abs(vel) * 0.1, 1e-6)
        if abs(acc) < acc_floor:
            shape[grp] = "stable"
        elif acc > 0 and vel > 0:
            shape[grp] = "accelerating"
        elif acc < 0 and vel > 0:
            shape[grp] = "decelerating"
        elif acc < 0 and vel < 0:
            shape[grp] = "accelerating"
        else:
            shape[grp] = "decelerating"
    state["shape"] = shape
    return state


def _bump(tier):
    idx = SEVERITY_ORDER.index(tier)
    return SEVERITY_ORDER[min(idx + 1, len(SEVERITY_ORDER) - 1)]


def detect_anomaly(state: AlertState) -> AlertState:
    velocity = state.get("velocity", {})
    baseline = state.get("baseline", {})
    shape = state.get("shape", {})
    current_raw = state.get("current_metrics", {})
    zscore = {}
    severity = {}
    sprt_llr = state.get("sprt_llr", {})
    sprt_direction = state.get("sprt_direction", {})
    resolved = state.get("resolved", {})
    sustained_ticks = state.get("sustained_ticks", {})

    current_levels = _current_levels(current_raw, velocity.keys())

    for grp, vel in velocity.items():
        gb = baseline.get(grp, {})
        if not gb.get("calibrated"):
            zscore[grp] = None
            severity[grp] = "uncalibrated"
            continue

        if gb.get("zero_variance"):
            rolling = gb.get("rolling_velocities", [])
            if len(rolling) >= ROLLING_MINIMUM_SAMPLES:
                rn = len(rolling)
                rmean = sum(rolling) / rn
                rvar = sum((x - rmean) ** 2 for x in rolling) / max(rn - 1, 1)
                rstd = max(math.sqrt(rvar), 1e-6)
                z = (vel - rmean) / rstd
                zscore[grp] = round(z, 4)
                az = abs(z)
                if az >= Z_HIGH:
                    tier = "high"
                elif az >= Z_MEDIUM:
                    tier = "medium"
                elif az >= Z_LOW:
                    tier = "low"
                else:
                    tier = "normal"
                if shape.get(grp) == "accelerating":
                    tier = _bump(tier)
            else:
                # only correct for the one instant traffic starts
                zscore[grp] = None
                tier = "high" if abs(vel - gb["mean"]) > 1e-3 else "normal"
        else:
            z = (vel - gb["mean"]) / gb["std"]
            zscore[grp] = round(z, 4)
            az = abs(z)
            if az >= Z_HIGH:
                tier = "high"
            elif az >= Z_MEDIUM:
                tier = "medium"
            elif az >= Z_LOW:
                tier = "low"
            else:
                tier = "normal"

            if shape.get(grp) == "accelerating":
                tier = _bump(tier)

        # level check catches a slowly changing but still elevated raw value
        lvl = current_levels.get(grp)
        if lvl is not None and gb.get("level_calibrated") and not gb.get("level_zero_variance"):
            level_z = (lvl - gb["level_mean"]) / gb["level_std"]
            is_elevated = abs(level_z) >= Z_LOW

            if is_elevated:
                sustained_ticks[grp] = sustained_ticks.get(grp, 0) + 1
            else:
                sustained_ticks[grp] = 0

            if sustained_ticks.get(grp, 0) >= SUSTAINED_TICKS_THRESHOLD:
                if level_z is None:
                    level_tier = "high"
                else:
                    alz = abs(level_z)
                    if alz >= Z_HIGH:
                        level_tier = "high"
                    elif alz >= Z_MEDIUM:
                        level_tier = "medium"
                    else:
                        level_tier = "low"
                if SEVERITY_ORDER.index(level_tier) > SEVERITY_ORDER.index(tier):
                    tier = level_tier
        elif lvl is not None and gb.get("level_zero_variance") and gb.get("initial_active_level") is not None:
            is_elevated = lvl >= INITIAL_LEVEL_ELEVATION_FACTOR * gb["initial_active_level"]

            if is_elevated:
                sustained_ticks[grp] = sustained_ticks.get(grp, 0) + 1
            else:
                sustained_ticks[grp] = 0

            if sustained_ticks.get(grp, 0) >= SUSTAINED_TICKS_THRESHOLD:
                if SEVERITY_ORDER.index("high") > SEVERITY_ORDER.index(tier):
                    tier = "high"
        else:
            sustained_ticks[grp] = 0

        severity[grp] = tier

        # keep sprt_confirmed state alive across ongoing high ticks
        if tier == "normal":
            sprt_llr.pop(grp, None)
            sprt_direction.pop(grp, None)
            resolved.pop(grp, None)
        elif tier == "high" and resolved.get(grp) != "confirmed":
            sprt_llr.pop(grp, None)
            sprt_direction.pop(grp, None)
            resolved.pop(grp, None)

    state["zscore"] = zscore
    state["severity"] = severity
    state["sprt_llr"] = sprt_llr
    state["sprt_direction"] = sprt_direction
    state["resolved"] = resolved
    state["sustained_ticks"] = sustained_ticks

    state["pattern"] = "normal"
    state["pattern_confidence"] = "stable"
    state["firing_directions"] = {}
    state["eta"] = {}

    return state


def route_after_detect(state: AlertState) -> str:
    severity = state.get("severity", {})
    if any(s != "normal" and s != "uncalibrated" for s in severity.values()):
        return "deliberate"
    return "format"


def deliberate(state: AlertState) -> AlertState:
    velocity = state.get("velocity", {})
    baseline = state.get("baseline", {})
    severity = state.get("severity", {})
    sprt_llr = state.get("sprt_llr", {})
    sprt_direction = state.get("sprt_direction", {})
    resolved = state.get("resolved", {})

    for grp, tier in list(severity.items()):
        if tier not in ("low", "medium"):
            continue
        gb = baseline.get(grp, {})
        mu0, sigma = gb["mean"], gb["std"]
        x = velocity[grp]

        # locked once per episode so the test stays coherent
        if grp not in sprt_direction:
            sprt_direction[grp] = 1 if x >= mu0 else -1
        direction = sprt_direction[grp]
        delta = SPRT_DELTA_K * sigma * direction

        llr = sprt_llr.get(grp, 0.0)
        llr += (delta / sigma**2) * (x - mu0 - delta / 2)
        sprt_llr[grp] = llr

        if llr >= SPRT_UPPER:
            resolved[grp] = "confirmed"
            severity[grp] = "high"
            sprt_llr.pop(grp, None)
            sprt_direction.pop(grp, None)
        elif llr <= SPRT_LOWER:
            resolved[grp] = "normal"
            severity[grp] = "normal"
            sprt_llr.pop(grp, None)
            sprt_direction.pop(grp, None)
        else:
            resolved[grp] = "pending"

    state["sprt_llr"] = sprt_llr
    state["sprt_direction"] = sprt_direction
    state["severity"] = severity
    state["resolved"] = resolved
    return state


def correlate_pattern(state: AlertState) -> AlertState:
    severity = state.get("severity", {})
    velocity = state.get("velocity", {})
    resolved = state.get("resolved", {})

    firing_directions = {
        grp: ("up" if velocity.get(grp, 0) > 0 else "down")
        for grp, s in severity.items() if s not in ("normal", "uncalibrated")
    }

    cats_up = {GROUP_CATEGORY.get(g) for g, d in firing_directions.items() if d == "up"}
    load_up = "load" in cats_up
    resource_up = "resource" in cats_up
    distress_up = "distress" in cats_up

    if not firing_directions:
        pattern = "normal"
    elif distress_up and load_up and resource_up:
        pattern = "degrading_under_demand"
    elif distress_up:
        pattern = "internal_distress"
    elif load_up and resource_up:
        pattern = "healthy_scaling"
    elif resource_up and not load_up:
        pattern = "internal_resource_pressure"
    elif load_up and not resource_up:
        pattern = "nf_failing_to_keep_up"
    else:
        pattern = "single_signal"

    if pattern == "normal":
        pattern_confidence = "stable"
    else:
        origins = []
        for g in firing_directions:
            if severity[g] == "high" and resolved.get(g) is None:
                origins.append("direct")
            elif resolved.get(g) == "confirmed":
                origins.append("sprt_confirmed")
            else:
                origins.append("pending")
        if all(o == "pending" for o in origins):
            pattern_confidence = "pending"
        elif all(o != "pending" for o in origins):
            pattern_confidence = "sprt_confirmed" if "sprt_confirmed" in origins else "direct"
        else:
            pattern_confidence = "mixed"

    state["firing_directions"] = firing_directions
    state["pattern"] = pattern
    state["pattern_confidence"] = pattern_confidence
    return state


def _eta_to_threshold(current, velocity, acceleration, threshold, max_horizon=ETA_MAX_HORIZON):
    if current >= threshold:
        return 0.0, "already_past"
    if abs(acceleration) < 1e-6:
        if velocity <= 0:
            return None, "no_breach"
        t = (threshold - current) / velocity
        return (t, "beyond_horizon") if t > max_horizon else (t, "linear_fallback")
    a, b, c = 0.5 * acceleration, velocity, current - threshold
    disc = b*b - 4*a*c
    if disc < 0:
        return None, "no_breach"
    sqrt_d = math.sqrt(disc)
    roots = [r for r in ((-b + sqrt_d) / (2*a), (-b - sqrt_d) / (2*a)) if r > 0]
    if not roots:
        return None, "no_breach"
    t = min(roots)
    return (t, "beyond_horizon") if t > max_horizon else (t, "quadratic")


def compute_eta(state: AlertState) -> AlertState:
    velocity = state.get("velocity", {})
    acceleration = state.get("acceleration", {})
    baseline = state.get("baseline", {})
    severity = state.get("severity", {})
    current_raw = state.get("current_metrics", {})
    eta = {}

    for grp, tier in severity.items():
        if tier in ("normal", "uncalibrated"):
            continue
        gb = baseline.get(grp, {})
        if not gb.get("calibrated"):
            eta[grp] = {"seconds": None, "note": "uncalibrated"}
            continue
        if gb.get("zero_variance"):
            eta[grp] = {"seconds": None, "note": "no_baseline_noise"}
            continue
        mu0, sigma = gb["mean"], gb["std"]
        threshold_velocity = mu0 + Z_HIGH * sigma
        seconds, note = _eta_to_threshold(
            current=velocity[grp], velocity=acceleration.get(grp, 0.0),
            acceleration=0.0, threshold=threshold_velocity,
        )
        if note == "no_breach":
            raw_now = None
            for key, v_val in current_raw.items():
                if _group(key) == grp and isinstance(v_val, (int, float)):
                    raw_now = v_val
                    break
            peak = gb.get("peak")
            if (raw_now is not None and peak is not None and peak > 0
                and (raw_now / peak) >= SATURATION_RATIO
                and velocity[grp] <= 0):
                note = "saturated"
            else:
                note = "stable_low"
        eta[grp] = {"seconds": seconds, "note": note}

    state["eta"] = eta
    return state


def format_observation(state: AlertState) -> AlertState:
    raw = state.get("current_metrics", {})
    velocity = state.get("velocity", {})
    current_levels = _current_levels(raw, velocity.keys())

    state["observation"] = {
        "agent":     "alert",
        "layer":     state["layer"],
        "plane":     state["plane"],
        "interface": state["interface"],
        "source":    state["source"],
        "timestamp": raw.get("timestamp", time.time()),
        "telemetry": raw,
        "assessment": {
            "current_levels": current_levels,
            "velocity":     state.get("velocity", {}),
            "acceleration": state.get("acceleration", {}),
            "shape":        state.get("shape", {}),
            "zscore":       state.get("zscore", {}),
            "severity":     state.get("severity", {}),
            "resolved":     state.get("resolved", {}),
            "firing_directions": state.get("firing_directions", {}),
            "pattern":      state.get("pattern", "normal"),
            "pattern_confidence": state.get("pattern_confidence", "stable"),
            "eta":          state.get("eta", {}),
        },
    }
    print(f"[Alert] pattern={state.get('pattern','normal')} "
          f"confidence={state.get('pattern_confidence','stable')} "
          f"severity={state.get('severity', {})} "
          f"levels={current_levels} "
          f"velocity={state.get('velocity', {})} "
          f"zscore={state.get('zscore', {})} "
          f"eta={state.get('eta', {})}")
    return state


async def publish(state: AlertState) -> AlertState:
    obs = state.get("observation", {})
    client = state.get("mcp_client")
    if not obs or client is None:
        return state
    payload = {k: v for k, v in obs.items() if k not in ("agent", "layer", "interface", "source")}
    try:
        await client.call_tool("publish_observation", {
            "agent":     obs.get("agent", "alert"),
            "layer":     obs.get("layer", "unknown"),
            "interface": obs.get("interface", "unknown"),
            "source":    obs.get("source", "unknown"),
            "payload":   payload,
        })
        print(f"[Alert] MCP publish ok: {obs.get('agent')}:{obs.get('layer')}:{obs.get('interface')}")
    except Exception as e:
        print(f"[Alert] MCP unavailable: {e}")
    return state


def build_alert_agent():
    g = StateGraph(AlertState)
    g.add_node("collect",              collect)
    g.add_node("compute_velocity",     compute_velocity)
    g.add_node("compute_acceleration", compute_acceleration)
    g.add_node("detect",               detect_anomaly)
    g.add_node("deliberate",           deliberate)
    g.add_node("correlate",            correlate_pattern)
    g.add_node("compute_eta",          compute_eta)
    g.add_node("format",               format_observation)
    g.add_node("publish",              publish)

    g.set_entry_point("collect")
    g.add_edge("collect",              "compute_velocity")
    g.add_edge("compute_velocity",     "compute_acceleration")
    g.add_edge("compute_acceleration", "detect")
    g.add_conditional_edges("detect", route_after_detect, {
        "deliberate": "deliberate",
        "format":     "format",
    })
    g.add_edge("deliberate", "correlate")
    g.add_edge("correlate",    "compute_eta")
    g.add_edge("compute_eta",  "format")
    g.add_edge("format",      "publish")
    g.add_edge("publish",     END)
    return g.compile()



def discover_active_mnfs():
    results = []
    for path in sorted(glob.glob(os.path.join(MNF_DATA_DIR, "mnf_*.jsonl"))):
        try:
            with open(path) as f:
                lines = [l.strip() for l in f if l.strip()]
            if len(lines) < 1:
                continue
            latest = json.loads(lines[-1])
            age = time.time() - os.path.getmtime(path)
            results.append({
                "mnf_path": path,
                "latest_reading": latest,
                "is_live": age < FILE_INACTIVE_THRESHOLD_S,
            })
        except Exception as e:
            logger.warning(f"discover: skipping {path}: {e}")
    return results


def initial_state(mnf_path: str, layer: str, plane: str, interface: str,
                  source: str, is_live: bool, mcp_client) -> dict:
    return {
        "mnf_path":          mnf_path,
        "layer":             layer,
        "plane":             plane,
        "interface":         interface,
        "source":            source,
        "is_live":           is_live,
        "current_metrics":   {},
        "previous_metrics":  {},
        "metric_history":    [],
        "baseline":          {},
        "velocity":          {},
        "acceleration":      {},
        "shape":             {},
        "zscore":            {},
        "severity":          {},
        "sprt_llr":          {},
        "sprt_direction":    {},
        "resolved":          {},
        "sustained_ticks":   {},
        "firing_directions": {},
        "pattern":           "",
        "pattern_confidence": "",
        "eta":               {},
        "observation":       {},
        "mcp_client":        mcp_client,
    }


class MnfChangeHandler(FileSystemEventHandler):
    def __init__(self, loop: asyncio.AbstractEventLoop, wake_events: dict):
        self.loop = loop
        self.wake_events = wake_events

    def _wake(self, path):
        event = self.wake_events.get(path)
        if event is not None:
            self.loop.call_soon_threadsafe(event.set)

    def on_modified(self, event):
        if not event.is_directory:
            self._wake(event.src_path)

    def on_created(self, event):
        if not event.is_directory:
            self._wake(event.src_path)


async def track_mnf(mnf_path: str, mnf_info: dict, agent, wake_event: asyncio.Event, registered: set):
    reader = _MnfFileReader(mnf_path)
    interface_states: dict = {}

    _mcp_cm = Client(MCP_URL, timeout=MCP_TIMEOUT_S)
    mcp_client = await _mcp_cm.__aenter__()

    async def _state_for(interface: str, layer: str, plane: str, source: str):
        if interface not in interface_states:
            key = f"alert:{layer}:{interface}"
            if key not in registered:
                try:
                    await mcp_client.call_tool("register_agent", {
                        "agent": "alert",
                        "description": f"Alert Agent tracking {mnf_path} ({interface})",
                    })
                    logger.info(f"Registered with MCP: {key}")
                    registered.add(key)
                except Exception as e:
                    logger.warning(f"MCP registration failed for {key}: {e}")
            interface_states[interface] = initial_state(
                mnf_path, layer, plane, interface, source,
                mnf_info.get("is_live", False), mcp_client)
        return interface_states[interface]

    logger.info(f"Tracking new MnF file: {mnf_path}")

    try:
        while True:
            await wake_event.wait()
            wake_event.clear()
            if not os.path.exists(mnf_path):
                logger.info(f"MnF file gone, stopping tracker: {mnf_path}")
                return

            by_interface: dict = reader.drain_all_buffered()
            for rec in reader.read_new_lines():
                by_interface.setdefault(rec.get("interface", "unknown"), []).append(rec)

            for interface, records in by_interface.items():
                first = records[0]
                state = await _state_for(interface, first.get("layer", "unknown"),
                                         first.get("plane", "unknown"),
                                         first.get("source", "unknown"))
                state["pending_new_records"] = records
                try:
                    state = await agent.ainvoke(state)
                except Exception as e:
                    logger.error(f"Agent invocation error for {interface}: {e}")
                interface_states[interface] = state
    finally:
        try:
            await _mcp_cm.__aexit__(None, None, None)
        except Exception:
            pass


async def discovery_loop(agent, observer_handler: MnfChangeHandler, tasks: dict, registered: set):
    while True:
        try:
            active = discover_active_mnfs()
            for mnf_info in active:
                path = mnf_info["mnf_path"]
                if path in tasks:
                    continue
                if not mnf_info.get("is_live", False):
                    continue
                wake_event = asyncio.Event()
                observer_handler.wake_events[path] = wake_event
                tasks[path] = asyncio.create_task(
                    track_mnf(path, mnf_info, agent, wake_event, registered)
                )
                wake_event.set()

            gone = [p for p in tasks if not os.path.exists(p)]
            for p in gone:
                logger.info(f"Cleaning up tracker for removed MnF: {p}")
                tasks[p].cancel()
                del tasks[p]
                observer_handler.wake_events.pop(p, None)

        except Exception as e:
            logger.error(f"Discovery loop error: {e}")

        await asyncio.sleep(DISCOVERY_INTERVAL)


async def main():
    logger.info("=== Alert Agent (self-contained, FastMCP) ===")
    agent = build_alert_agent()
    loop = asyncio.get_event_loop()

    tasks: dict = {}
    registered: set = set()
    handler = MnfChangeHandler(loop, wake_events={})

    observer = Observer()
    observer.schedule(handler, MNF_DATA_DIR, recursive=False)
    observer.start()
    logger.info(f"Watching {MNF_DATA_DIR} for MnF file changes")

    try:
        await discovery_loop(agent, handler, tasks, registered)
    except KeyboardInterrupt:
        logger.info("Stopped.")
    finally:
        observer.stop()
        observer.join()


if __name__ == "__main__":
    asyncio.run(main())
