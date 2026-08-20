#!/usr/bin/env python3
""" Shared RCA Core

Author: Heven Tafese

The deterministic severity model, causal root finding, Dempster-Shafer
evidence fusion, trajectory assessment, cross agent corroboration, and
transition detection are all shared by the RCA and explanation agent.

"""

import sqlite3
import time
import json
from pathlib import Path

FAILURE_FIELDS = {
    "N3": ["n3_rx_drop", "n3_tx_drop", "n3_rx_errors", "n3_tx_errors"],
    "N2": ["n2_rx_drop", "n2_tx_drop", "n2_rx_errors", "n2_tx_errors",
           "ngap_reject_count"],
    "N4": ["n4_rx_drop", "n4_tx_drop", "pfcp_error_count"],
    "N6": ["n6_rx_drop", "n6_tx_drop", "n6_rx_errors", "n6_tx_errors"],
}

INTERFACE_TO_NF = {
    "N2": "AMF", "N3": "UPF", "N4": "UPF", "N6": "UPF",
    "N9": "UPF", "N11": "SMF", "gNB-DL": "gNB",
}

CAUSAL_EDGE_TYPES = ["OVERLOADS", "CAUSES", "AMPLIFIES", "CROSS_PLANE"]

INTERFACE_SPEC = {
    "N2": "TS 38.413", "N3": "TS 29.281", "N4": "TS 29.244",
    "N6": "TS 23.501", "N9": "TS 29.281",
}

BAND_ORDER = ["baseline", "onset", "congestion", "failure"]

TREND_NOISE_FLOOR_FRACTION = 0.005


def _ds_mass_from_rho(rho):
    """Dempster-Shafer basic probability assignment from one rho, over
    {congested (C), not_congested (N)}. Uncertainty theta is always the
    remainder. rho>=1.0 gives strong belief in C, 0.85-1.0 gradual belief
    in C, below 0.85 belief in N, stronger the lower rho is."""
    if rho is None:
        return {"C": 0.0, "N": 0.0, "theta": 1.0}
    if rho >= 1.0:
        mc = min(0.5 + (rho - 1.0) * 0.3, 0.95)
        return {"C": round(mc, 4), "N": 0.0, "theta": round(1.0 - mc, 4)}
    if rho >= 0.85:
        mc = (rho - 0.85) / 0.15 * 0.4
        return {"C": round(mc, 4), "N": 0.0, "theta": round(1.0 - mc, 4)}
    mn = min(0.3 + (0.85 - rho) * 0.5, 0.85)
    return {"C": 0.0, "N": round(mn, 4), "theta": round(1.0 - mn, 4)}


def _ds_combine(m1, m2):
    """Dempster's combination rule for two BPAs over {C, N, theta}. K is
    the conflict, the mass on empty intersections. Returns the combined
    BPA plus K."""
    if m1 is None:
        return m2, 0.0
    if m2 is None:
        return m1, 0.0

    k = (m1["C"] * m2["N"]) + (m1["N"] * m2["C"])
    if k >= 1.0:
        return {"C": 0.0, "N": 0.0, "theta": 1.0}, 1.0

    norm = 1.0 / (1.0 - k)
    mc = (m1["C"] * m2["C"]
          + m1["C"] * m2["theta"]
          + m1["theta"] * m2["C"]) * norm
    mn = (m1["N"] * m2["N"]
          + m1["N"] * m2["theta"]
          + m1["theta"] * m2["N"]) * norm
    mt = (m1["theta"] * m2["theta"]) * norm

    return {"C": round(mc, 4), "N": round(mn, 4),
            "theta": round(mt, 4)}, round(k, 4)


def _ds_fuse(masses):
    """Iterated pairwise Dempster combination across N mass functions.
    Returns (fused_bpa, total_conflict)."""
    if not masses:
        return {"C": 0.0, "N": 0.0, "theta": 1.0}, 0.0
    fused = masses[0]
    total_k = 0.0
    for m in masses[1:]:
        fused, k = _ds_combine(fused, m)
        total_k = max(total_k, k)
    return fused, total_k


def _classify_band(rho):
    if rho >= 1.0:
        return "congestion"
    if rho >= 0.85:
        return "onset"
    return "baseline"


def _queue_length(rho):
    if rho is None:
        return None
    if rho >= 1.0:
        return "unbounded"
    return round(rho / (1.0 - rho), 3)


def _trend_and_eta(value, ceiling, rho, velocity_value):
    if velocity_value is None or ceiling is None or ceiling == 0:
        return "unknown", None, None
    noise_floor = TREND_NOISE_FLOOR_FRACTION * ceiling
    if velocity_value > noise_floor:
        trend = "climbing"
    elif velocity_value < -noise_floor:
        trend = "falling"
    else:
        trend = "flat"
    eta_onset, eta_congestion = None, None
    if trend == "climbing":
        onset_value = 0.85 * ceiling
        congestion_value = 1.0 * ceiling
        if value < onset_value:
            eta_onset = round((onset_value - value) / velocity_value, 1)
        if value < congestion_value:
            eta_congestion = round(
                (congestion_value - value) / velocity_value, 1)
    return trend, eta_onset, eta_congestion


def _trajectory_assessment(rho, velocity_rho):
  
    if velocity_rho is None:
        return {"direction": "unknown", "urgency": "none",
                "rho_rate_per_s": None}
    if velocity_rho > 0.005:
        direction = "worsening"
        if rho is not None and rho >= 0.85:
            urgency = "critical"
        elif rho is not None and rho >= 0.7:
            urgency = "elevated"
        else:
            urgency = "moderate"
    elif velocity_rho < -0.005:
        direction = "improving"
        urgency = "none"
    else:
        direction = "stable"
        urgency = "none"
    return {"direction": direction, "urgency": urgency,
            "rho_rate_per_s": round(velocity_rho, 6)}


def _failure_check(iface, metrics, band):
    if band not in ("onset", "congestion"):
        return False, None, []
    fields_checked = FAILURE_FIELDS.get(iface, [])
    found = []
    for f in fields_checked:
        v = metrics.get(f)
        if v is not None:
            found.append((f, v))
    if not found:
        return False, "no drop/error fields present in this observation", []
    nonzero = [(f, v) for f, v in found if v and v > 0]
    if nonzero:
        reason = ", ".join(f"{f}={v}" for f, v in nonzero)
        return True, (f"drop/error signal present under sustained "
                      f"load: {reason}"), found
    return False, None, found


def _empty_severity(issue, note):
    return {
        "rho": None, "band": None, "calibrated": False,
        "calibration_issue": issue,
        "metric": None, "value": None, "ceiling": None,
        "source_ref": note,
        "trend": "unknown", "eta_to_onset_s": None,
        "eta_to_congestion_s": None, "expected_queue_length": None,
        "failure_signature": False, "failure_reason": None,
        "failure_fields_seen": [], "metric_disagreement": [],
        "trajectory": {"direction": "unknown", "urgency": "none",
                        "rho_rate_per_s": None},
        "ds_belief_congested": None, "ds_conflict": None,
        "sqlite_cross_check": None}


def compute_severity(active_observations, sqlite_path):
    """Reads calibrated_metrics from each observation, computes rho
    against SQLite's ceiling, fuses multiple metrics via Dempster-Shafer,
    and adds trajectory. Processes any observation with calibrated_metrics
    regardless of the anomaly flag, the RCA forms its own judgment."""
    severity = {}
    conn = None
    try:
        conn = sqlite3.connect(sqlite_path)
        conn.row_factory = sqlite3.Row
    except Exception as e:
        print(f"[rca_core] SQLite unavailable (advisory cross-check "
              f"disabled, severity still computed): {e}")

    for a in active_observations:
        iface = a.get("interface", "")
        if not iface or iface in severity:
            continue

        metrics = a.get("metrics", {}) or {}
        velocity = a.get("velocity", {}) or {}
        declared = metrics.get("calibrated_metrics", []) or []

        # only anomalous observations get severity scored, the RCA forms its own judgment from the anomalous subset
        if not a.get("anomaly", False):
            continue
        if not declared:
            severity[iface] = _empty_severity(
                "agent_declared_no_calibrated_metric",
                "observation flagged anomalous but calibrated_metrics "
                "was empty, the RCA cannot independently compute rho "
                "without a declared ceiling")
            continue

        candidates = []
        ds_masses = []
        no_baseline_row = True
        absent_fields = []
        for cm in declared:
            obs_field = cm.get("field")
            if not obs_field:
                continue
            val = metrics.get(obs_field)
            if val is None:
                absent_fields.append(obs_field)
                continue

            # ceiling comes from SQLite 
            if conn is None:
                continue
            row = conn.execute(
                "SELECT link_rate_r, unit, source_ref "
                "FROM interface_baselines "
                "WHERE interface=? AND metric=?",
                (iface, obs_field)).fetchone()
            if row is None:
                continue
            no_baseline_row = False
            if row["link_rate_r"] is None:
                candidates.append({
                    "rho": None, "band": None, "calibrated": False,
                    "calibration_issue": "ceiling_not_set",
                    "metric": obs_field, "value": round(val, 4),
                    "ceiling": None, "source_ref": row["source_ref"],
                    "trend": "unknown", "eta_to_onset_s": None,
                    "eta_to_congestion_s": None,
                    "trajectory": {"direction": "unknown",
                                   "urgency": "none",
                                   "rho_rate_per_s": None},
                    "sqlite_cross_check": None})
                continue

            ceiling = row["link_rate_r"]
            rho = val / ceiling
            vel_raw = velocity.get(obs_field)
            vel_rho = vel_raw / ceiling if vel_raw is not None else None
            trend, eta_onset, eta_cong = _trend_and_eta(
                val, ceiling, rho, vel_raw)
            trajectory = _trajectory_assessment(rho, vel_rho)
            ds_masses.append(_ds_mass_from_rho(rho))

            candidates.append({
                "rho": round(rho, 4),
                "band": _classify_band(rho),
                "calibrated": True, "calibration_issue": None,
                "metric": obs_field, "value": round(val, 4),
                "ceiling": ceiling, "source_ref": row["source_ref"],
                "trend": trend, "eta_to_onset_s": eta_onset,
                "eta_to_congestion_s": eta_cong,
                "trajectory": trajectory,
                "sqlite_cross_check": None})

        if not candidates:
            if absent_fields and no_baseline_row:
                severity[iface] = _empty_severity(
                    "declared_field_absent_from_observation",
                    f"agent declared calibrated_metrics field(s) "
                    f"{absent_fields} but this observation's own metrics "
                    f"dict did not contain them")
            elif no_baseline_row:
                severity[iface] = _empty_severity(
                    "no_baseline_row",
                    "declared field(s) present in the observation, but no "
                    "matching row in interface_baselines for any of them")
            else:
                severity[iface] = _empty_severity(
                    "ceiling_not_set",
                    "matching baseline row(s) found but ceiling not yet set")
            continue

        best = max(candidates, key=lambda c: c["rho"])

        bands_seen = {c["metric"]: c["band"] for c in candidates}
        disagreement = []
        if len(set(bands_seen.values())) > 1:
            disagreement = [{"metric": m, "band": b}
                            for m, b in bands_seen.items()]

        fused, conflict = _ds_fuse(ds_masses)

        is_fail, fail_reason, fail_fields = _failure_check(
            iface, metrics, best["band"])
        if is_fail:
            best["band"] = "failure"

        best["failure_signature"] = is_fail
        best["failure_reason"] = fail_reason
        best["failure_fields_seen"] = fail_fields

        best["expected_queue_length"] = _queue_length(best["rho"])
        best["metric_disagreement"] = disagreement
        best["ds_belief_congested"] = fused.get("C", 0.0)
        best["ds_conflict"] = conflict
        severity[iface] = best

    if conn is not None:
        conn.close()
    return severity


def worst_band(severity_dict):
    """Most severe band across all calibrated interfaces."""
    worst = "baseline"
    for s in severity_dict.values():
        if not s.get("calibrated") or s.get("band") is None:
            continue
        if BAND_ORDER.index(s["band"]) > BAND_ORDER.index(worst):
            worst = s["band"]
    return worst


def compute_corroboration(active_observations):
    """Cross agent corroboration weight per interface (ETSI GS
    NFV-IFA 042 multi-source correlation). Returns {interface: {agents_anomaly,
    agents_normal, level, weight}}."""
    by_iface = {}
    for a in active_observations:
        iface = a.get("interface")
        if not iface:
            continue
        by_iface.setdefault(iface, {"anomaly": set(), "normal": set()})
        agent = a.get("agent", "unknown")
        if a.get("anomaly"):
            by_iface[iface]["anomaly"].add(agent)
        else:
            by_iface[iface]["normal"].add(agent)

    corroboration = {}
    for iface, sets in by_iface.items():
        anomaly_agents = sorted(sets["anomaly"])
        normal_agents = sorted(sets["normal"])
        if len(anomaly_agents) >= 2:
            level = "multi-agent"
            weight = 1.0
        elif len(anomaly_agents) == 1 and not normal_agents:
            level = "single-agent"
            weight = 0.7
        elif len(anomaly_agents) == 1 and normal_agents:
            level = "conflicting"
            weight = 0.4
        else:
            level = "none-anomalous"
            weight = 0.1
        corroboration[iface] = {
            "agents_anomaly": anomaly_agents,
            "agents_normal": normal_agents,
            "level": level,
            "weight": weight}
    return corroboration


def find_root(active_nodes, edges):
    result = {"root": None, "chain": [], "feedback_loop": False,
              "gaps": [], "ambiguous_roots": []}
    if not active_nodes:
        return result
    inbound_from_active = {n: [] for n in active_nodes}
    for src, etype, dst, ref, xp in edges:
        if (dst in active_nodes and src in active_nodes
                and etype != "AMPLIFIES"):
            inbound_from_active[dst].append((src, etype, ref))
        if etype == "AMPLIFIES" and (
                src in active_nodes or dst in active_nodes):
            result["feedback_loop"] = True
    candidates = [n for n, inb in inbound_from_active.items() if not inb]
    if not candidates:
        result["gaps"].append(
            "no root candidate: causal cycle among active symptoms")
        return result
    candidates.sort(key=lambda n: active_nodes[n])
    result["root"] = candidates[0]
    if len(candidates) > 1:
        result["ambiguous_roots"] = candidates
        first_ts = active_nodes[candidates[0]]
        concurrent = [c for c in candidates
                      if abs(active_nodes[c] - first_ts) < 2.0]
        if len(concurrent) > 1:
            result["gaps"].append(
                f"multiple concurrent root candidates: {concurrent}")
    frontier, seen = [result["root"]], {result["root"]}
    while frontier:
        nxt = []
        for node in frontier:
            for src, etype, dst, ref, xp in edges:
                if (src == node and etype != "AMPLIFIES"
                        and dst not in seen):
                    result["chain"].append({
                        "from": src, "rel": etype, "to": dst,
                        "source_ref": ref, "cross_plane": bool(xp),
                        "observed": dst in active_nodes})
                    seen.add(dst)
                    nxt.append(dst)
        frontier = nxt
    return result


def fetch_causal_edges(neo4j_uri, neo4j_auth):
    edges = []
    try:
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(neo4j_uri, auth=neo4j_auth)
        with driver.session() as s:
            recs = s.run(
                "MATCH (a)-[r]->(b) WHERE type(r) IN $types "
                "RETURN a.name AS src, type(r) AS t, b.name AS dst, "
                "r.source_ref AS ref, r.cross_plane AS xp",
                types=CAUSAL_EDGE_TYPES)
            for rec in recs:
                edges.append((rec["src"], rec["t"], rec["dst"],
                              rec["ref"] or "", bool(rec["xp"])))
        driver.close()
    except Exception as e:
        print(f"[rca_core] Neo4j unavailable: {e}")
    return edges


def run_causal_trace(active_observations, neo4j_uri, neo4j_auth,
                     cross_plane_suspected=False, severity=None):
    """Seeds active nodes from severity confirmed interfaces via
    INTERFACE_TO_NF, then finds the causal root. """
                         
    causal = {"root": None, "chain": [], "feedback_loop": False,
              "gaps": [], "ambiguous_roots": [], "root_source_refs": [],
              "severity_seeded_nodes": [], "confidence": "low"}

    active_nodes = {}

    if severity:
        for iface, sev in severity.items():
            if sev.get("band") in ("onset", "congestion", "failure"):
                nf = INTERFACE_TO_NF.get(iface)
                if nf:
                    obs_ts = next(
                        (a.get("ts") or time.time()
                         for a in active_observations
                         if a.get("interface") == iface),
                        time.time())
                    if nf not in active_nodes:
                        active_nodes[nf] = obs_ts
                        causal["severity_seeded_nodes"].append(nf)
                    else:
                        active_nodes[nf] = min(active_nodes[nf], obs_ts)
                else:
                    causal["gaps"].append(
                        f"interface '{iface}' has no NF mapping in "
                        f"INTERFACE_TO_NF, cannot seed causal node")

    edges = fetch_causal_edges(neo4j_uri, neo4j_auth)

    if edges and active_nodes:
        found = find_root(active_nodes, edges)
        causal.update({k: v for k, v in found.items()
                       if k != "severity_seeded_nodes"})
        if (cross_plane_suspected and len(active_nodes) > 1
                and not found["chain"]):
            causal["gaps"].append(
                "symptoms on multiple planes but no connecting "
                "causal edge")
        causal["root_source_refs"] = sorted({
            c["source_ref"] for c in causal["chain"]
            if c["from"] == causal["root"] and c["source_ref"]})

    corroboration = compute_corroboration(active_observations)

    root_iface = None
    if causal["root"]:
        for iface, nf in INTERFACE_TO_NF.items():
            if nf == causal["root"] and iface in (
                    severity or {}):
                root_iface = iface
                break

    root_corrob = corroboration.get(
        root_iface, {}) if root_iface else {}
    corrob_weight = root_corrob.get("weight", 0.5)
    corrob_level = root_corrob.get("level", "unknown")

    fully_observed = bool(causal["chain"]) and all(
        c["observed"] for c in causal["chain"])
    concurrent_multi = len(
        causal.get("ambiguous_roots", [])) > 1

    if (causal["root"] and corrob_level == "multi-agent"
            and fully_observed and not concurrent_multi):
        causal["confidence"] = "high"
    elif causal["root"] and corrob_weight >= 0.7 \
            and not concurrent_multi:
        causal["confidence"] = "moderate"
    elif causal["root"]:
        causal["confidence"] = "low"
    else:
        causal["confidence"] = "low"

    causal["corroboration"] = corroboration
    return causal, active_nodes, edges


def detect_transitions(severity, history_path, failure_duration_s=75.0):
    path = Path(history_path)
    try:
        history = json.loads(path.read_text()) if path.exists() else {}
    except Exception as e:
        print(f"[rca_core] transition history unreadable: {e}")
        history = {}
    now = time.time()
    result = {}
    for iface, sev in severity.items():
        current_band = sev.get("band")
        record = history.get(
            iface, {"recent": [], "congestion_since": None})
        recent = record.get("recent", [])
        previous_band = recent[-1]["band"] if recent else None
        transitioned = (previous_band is not None
                        and current_band is not None
                        and previous_band != current_band)
        recent.append({"band": current_band, "ts": now})
        recent = recent[-4:]
        direction_changes = 0
        for i in range(1, len(recent)):
            if recent[i]["band"] != recent[i - 1]["band"]:
                direction_changes += 1
        oscillating = direction_changes >= 2 and len(recent) >= 3
        congestion_since = record.get("congestion_since")
        if current_band in ("congestion", "failure"):
            if congestion_since is None:
                congestion_since = now
        else:
            congestion_since = None
        sustained_s = (round(now - congestion_since, 1)
                       if congestion_since else 0.0)
        duration_failure = (congestion_since is not None
                            and sustained_s >= failure_duration_s)
        if ((oscillating or duration_failure)
                and current_band in ("onset", "congestion")):
            current_band = "failure"
            sev["band"] = "failure"
            sev["failure_signature"] = True
            prior_reason = sev.get("failure_reason")
            reasons = []
            if oscillating:
                reasons.append(
                    f"oscillating across last {len(recent)} cycles: "
                    f"{[r['band'] for r in recent]}")
            if duration_failure:
                reasons.append(
                    f"sustained congestion for {sustained_s}s "
                    f"(threshold {failure_duration_s}s)")
            new_reason = "; ".join(reasons)
            sev["failure_reason"] = (
                f"{prior_reason}; {new_reason}"
                if prior_reason else new_reason)
        record["recent"] = recent
        record["congestion_since"] = congestion_since
        history[iface] = record
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(history))
    except Exception as e:
        print(f"[rca_core] could not persist transition history: {e}")
    return result


def compute_velocity(active_observations, history_path):
    path = Path(history_path)
    try:
        history = json.loads(path.read_text()) if path.exists() else {}
    except Exception as e:
        print(f"[rca_core] velocity history unreadable: {e}")
        history = {}
    for a in active_observations:
        iface = a.get("interface")
        if not iface:
            continue
        metrics = a.get("metrics", {}) or {}
        ts = a.get("ts") or time.time()
        prev = history.get(iface, {})
        prev_metrics = prev.get("metrics", {})
        prev_ts = prev.get("ts")
        velocity = {}
        if prev_ts is not None and ts > prev_ts:
            dt = ts - prev_ts
            for k, v in metrics.items():
                if (isinstance(v, (int, float))
                        and k in prev_metrics
                        and isinstance(prev_metrics[k], (int, float))):
                    velocity[k] = (v - prev_metrics[k]) / dt
        a["velocity"] = velocity
        history[iface] = {"metrics": metrics, "ts": ts}
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(history))
    except Exception as e:
        print(f"[rca_core] could not persist velocity history: {e}")
    return active_observations
