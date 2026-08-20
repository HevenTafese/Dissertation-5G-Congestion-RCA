#!/usr/bin/env python3
"""  Configuration Agent

Author: Heven Tafese

This agent reads free5GC's own AMF/SMF/UPF YAML configuration and derives an
expected behaviour model from what is declared, and validates live
procedure logs and live MnF telemetry against that model, plus a small set
of NAS procedure order invariants. 

Every record is written to a local JSONL log and printed to stdout before
the MCP publish attempt. LEGAL_GMM_TRANSITIONS is read from free5GC's own
pinned source and cross checked against free5gc/util's fsm.go.
"""

import asyncio
import glob
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, TypedDict

import yaml
from langgraph.graph import END, StateGraph
from fastmcp import Client

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))

MCP_URL = os.environ.get("MCP_URL", "http://localhost:9000/mcp")
MCP_TIMEOUT_S = 3.0
AMF_CFG_PATH = os.path.expanduser("~/free5gc/config/amfcfg.yaml")
SMF_CFG_PATH = os.path.expanduser("~/free5gc/config/smfcfg.yaml")
UPF_CFG_PATH = os.path.expanduser("~/free5gc/config/upfcfg.yaml")
LOG_ROOT = os.path.expanduser("~/free5gc/log")
MNF_DATA_DIR = os.path.expanduser("~/data")
BASELINE_STORE = os.path.expanduser(
    "~/congestion-rca-5g/agents/configuration/baseline_snapshot.json"
)
LOCAL_RECORD_LOG = os.path.expanduser(
    "~/congestion-rca-5g/agents/configuration/published_records.jsonl"
)

CYCLE_SECONDS = 10
CORRELATION_WINDOW_S = 60
LOG_TAIL_LINES = 400

AGENT_NAME = "configuration"
LAYER = "layer2"

RELEVANT_KEY_PATTERNS = [
    "snssaiList", "snssaiInfos", "sNssai", "dnnInfos", "dnnUpfInfoList",
    "pools", "staticPools", "cidr", "supportDnnList", "dnnList",
    "t3502Value", "t3512Value", "non3gppDeregTimerValue",
    "t3513", "t3522", "t3550", "t3555", "t3560", "t3565", "t3570",
    "t3591", "t3592", "urrPeriod", "urrThreshold", "requestedUnit",
    "pfcp", "nodeID", "retransTimeout", "maxRetrans",
    "servedGuamiList", "supportTaiList", "plmnSupportList", "plmnList",
    "natifname",
]

# GMM state table read from free5gc/amf's real internal/gmm/init.go at the exact commit free5GC v3.4.1 pins. This is grouped by what each transition represents.
LEGAL_GMM_TRANSITIONS = {
    # GmmMessageEvent, same state self loop for an intermediate NAS message, legal from every state including the two terminal looking ones.
    ("Deregistered", "Deregistered"),
    ("Authentication", "Authentication"),
    ("SecurityMode", "SecurityMode"),
    ("ContextSetup", "ContextSetup"),
    ("Registered", "Registered"),
    # StartAuthEvent, normal registration start, and periodic reauth from an already registered UE.
    ("Deregistered", "Authentication"),
    ("Registered", "Authentication"),
    # AuthSuccessEvent / SecurityModeSuccessEvent / ContextSetupSuccessEvent. ContextSetup -> Registered is the actual registration accepted moment
    ("Authentication", "SecurityMode"),
    ("SecurityMode", "ContextSetup"),
    ("ContextSetup", "Registered"),
    # AuthFailEvent / AuthErrorEvent / SecurityModeFailEvent / ContextSetupFailEvent, legitimate failure exits. 
    ("Authentication", "Deregistered"),
    ("SecurityMode", "Deregistered"),
    ("ContextSetup", "Deregistered"),
    # InitDeregistrationEvent / DeregistrationAcceptEvent, normal deregistration flow.
    ("Registered", "DeregistrationInitiated"),
    ("DeregistrationInitiated", "Deregistered"),
}

_INTERFACE_BY_CHECK = {
    "smf_reported_state_mismatch": "N4_N3_N6",
    "cross_file_slice_mismatch": "N2_N4",
    "cross_file_address_pool_mismatch": "N4_N3_N6",
    "urr_cadence": "N4",
    "invariant_1_auth_before_registration": "N2",
    "invariant_2_registration_before_pdu_session": "N2",
    "invariant_3_session_before_data": "N3_N6",
}

LOG_LINE_RE = re.compile(
    r'time="(?P<time>[^"]+)"\s+level="(?P<level>[^"]+)"\s+msg="(?P<msg>(?:[^"\\]|\\.)*)"(?P<rest>.*)'
)
FIELD_RE = re.compile(r'(\w+)="([^"]*)"')
TRANSITION_RE = re.compile(r"transition from \[(\w+)\] to \[(\w+)\]")


class ConfigurationState(TypedDict, total=False):
    config_current: Dict[str, Any]
    config_baseline: Dict[str, Any]
    config_diffs: List[Dict[str, Any]]
    telemetry: Dict[str, Any]
    log_path: Optional[str]
    log_events: List[Dict[str, Any]]
    expected_model: Dict[str, Any]
    behaviour_findings: List[Dict[str, Any]]
    invariant_findings: List[Dict[str, Any]]
    correlated: List[Dict[str, Any]]
    mcp_records: List[Dict[str, Any]]
    publish_results: List[Dict[str, Any]]
    cycle_start: float
    mcp_client: Optional[Any]


def _flatten(d: Any, prefix: str = "") -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if isinstance(d, dict):
        for k, v in d.items():
            out.update(_flatten(v, f"{prefix}.{k}" if prefix else str(k)))
    elif isinstance(d, list):
        for i, v in enumerate(d):
            out.update(_flatten(v, f"{prefix}[{i}]"))
    else:
        out[prefix] = d
    return out


def _is_relevant(key: str) -> bool:
    return any(pattern in key for pattern in RELEVANT_KEY_PATTERNS)


def _retrans_envelope(cfg_block: Optional[Dict[str, Any]], label: str) -> Optional[Dict[str, Any]]:
    if not cfg_block or not cfg_block.get("enable", False):
        return None
    expire = str(cfg_block.get("expireTime", "0s")).rstrip("sS") or "0"
    try:
        seconds = float(expire)
    except ValueError:
        seconds = 0.0
    retries = cfg_block.get("maxRetryTimes", 0)
    return {
        "label": label, "expire_s": seconds, "max_retries": retries,
        "worst_case_s": seconds * (retries + 1),
    }


def parse_log_line(line: str) -> Optional[Dict[str, Any]]:
    m = LOG_LINE_RE.match(line)
    if not m:
        return None
    rec = {"time": m.group("time"), "level": m.group("level"), "msg": m.group("msg")}
    for k, v in FIELD_RE.findall(m.group("rest")):
        rec[k] = v
    return rec


def _parse_log_time(ts: str) -> Optional[float]:
    try:
        core = ts[:-1] if ts.endswith("Z") else ts
        if "." in core:
            main, frac = core.split(".", 1)
            core = f"{main}.{(frac + '000000')[:6]}"
        return datetime.fromisoformat(core + "+00:00").timestamp()
    except (ValueError, IndexError, AttributeError):
        return None


def classify_event(rec: Dict[str, Any]) -> Optional[str]:
    msg = rec.get("msg", "")
    if msg == "Ue Context in GMM-Registered":
        return "registration_complete"
    if msg == "Release Ue Context in GMM-Registered":
        return "registration_released"
    if msg.startswith("Handle event[Start Authentication]"):
        return "auth_start"
    if msg.startswith("Handle event[Authentication Success]"):
        return "auth_success"
    if msg.startswith("Handle event[SecurityMode Success]"):
        return "security_mode_success"
    if msg == "Sending PFCP Session Establishment Request":
        return "pdu_session_establish_request"
    if msg == "Received PFCP Session Establishment Accepted Response":
        return "pdu_session_establish_accept"
    if msg == "Receive Update SM Context Request":
        return "sm_context_update"
    if msg.startswith("Unexpected state, expect:"):
        return "smf_unexpected_state"
    return None


def _latest_log_path() -> Optional[str]:
    dirs = sorted(glob.glob(os.path.join(LOG_ROOT, "*/")), reverse=True)
    for d in dirs:
        candidate = os.path.join(d, "free5gc.log")
        if os.path.exists(candidate):
            return candidate
    return None


def _latest_mnf_path() -> Optional[str]:
    candidates = glob.glob(os.path.join(MNF_DATA_DIR, "mnf_*.jsonl"))
    if not candidates:
        return None
    return max(candidates, key=os.path.getmtime)


def _latest_mnf_record(path: str) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r") as f:
            lines = [l.strip() for l in f.readlines() if l.strip()]
    except OSError:
        return None
    for candidate in ([lines[-2]] if len(lines) >= 2 else []) + (lines[-1:] if lines else []):
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            continue
    return None


def _infer_interface(item: Dict[str, Any]) -> str:
    if item.get("mnf_interface"):
        return item["mnf_interface"]
    return _INTERFACE_BY_CHECK.get(item.get("check", ""), "N2_N3_N4_N6")


def collect_config(state: ConfigurationState) -> ConfigurationState:
    current = {}
    for name, path in (("amf", AMF_CFG_PATH), ("smf", SMF_CFG_PATH), ("upf", UPF_CFG_PATH)):
        try:
            with open(path, "r") as f:
                raw = f.read()
            current[name] = {
                "parsed": yaml.safe_load(raw),
                "sha256": hashlib.sha256(raw.encode()).hexdigest(),
                "path": path,
            }
        except FileNotFoundError:
            current[name] = {"parsed": None, "sha256": None, "path": path, "error": "not_found"}
        except yaml.YAMLError as e:
            current[name] = {"parsed": None, "sha256": None, "path": path, "error": f"parse_error: {e}"}

    baseline: Dict[str, Any] = {}
    if os.path.exists(BASELINE_STORE):
        try:
            with open(BASELINE_STORE, "r") as f:
                baseline = json.load(f)
        except (json.JSONDecodeError, OSError):
            baseline = {}

    if not baseline:
        baseline = {
            name: {"parsed": current[name]["parsed"], "sha256": current[name]["sha256"]}
            for name in current
        }
        os.makedirs(os.path.dirname(BASELINE_STORE), exist_ok=True)
        with open(BASELINE_STORE, "w") as f:
            json.dump(baseline, f, indent=2)
        print(f"[configuration_agent] golden baseline created at {BASELINE_STORE}", file=sys.stderr)

    return {
        "config_current": current,
        "config_baseline": baseline,
        "cycle_start": time.time(),
    }


def collect_telemetry(state: ConfigurationState) -> ConfigurationState:
    mnf_path = _latest_mnf_path()
    record = _latest_mnf_record(mnf_path) if mnf_path else None
    return {"telemetry": {"mnf_path": mnf_path, "latest_record": record}}


def collect_events(state: ConfigurationState) -> ConfigurationState:
    log_path = _latest_log_path()
    events: List[Dict[str, Any]] = []
    if log_path:
        try:
            with open(log_path, "r", errors="replace") as f:
                lines = f.readlines()[-LOG_TAIL_LINES:]
            for line in lines:
                rec = parse_log_line(line)
                if not rec:
                    continue
                etype = classify_event(rec)
                if etype:
                    rec["event_type"] = etype
                    rec["_epoch"] = _parse_log_time(rec.get("time", ""))
                    events.append(rec)
        except OSError:
            pass
    return {"log_path": log_path, "log_events": events}


def build_expected_model(state: ConfigurationState) -> ConfigurationState:
    current = state["config_current"]
    baseline = state["config_baseline"]

    diffs = []
    for name in ("amf", "smf", "upf"):
        cur_parsed = (current.get(name) or {}).get("parsed") or {}
        base_parsed = (baseline.get(name) or {}).get("parsed") or {}
        cur_flat = _flatten(cur_parsed)
        base_flat = _flatten(base_parsed)
        for key in set(cur_flat) | set(base_flat):
            if not _is_relevant(key):
                continue
            cur_val, base_val = cur_flat.get(key), base_flat.get(key)
            if cur_val != base_val:
                diffs.append({
                    "file": name, "field": key,
                    "old_value": base_val, "new_value": cur_val,
                    "detected_at": time.time(),
                })

    amf = (current.get("amf") or {}).get("parsed") or {}
    smf = (current.get("smf") or {}).get("parsed") or {}
    upf = (current.get("upf") or {}).get("parsed") or {}
    amf_cfg = amf.get("configuration", {}) or {}
    smf_cfg = smf.get("configuration", {}) or {}

    model: Dict[str, Any] = {}

    model["amf_procedure_envelopes"] = {
        k: v for k, v in {
            "t3513_paging": _retrans_envelope(amf_cfg.get("t3513"), "paging"),
            "t3522_dereg": _retrans_envelope(amf_cfg.get("t3522"), "deregistration"),
            "t3550_reg_accept": _retrans_envelope(amf_cfg.get("t3550"), "registration_accept"),
            "t3555_cfg_update": _retrans_envelope(amf_cfg.get("t3555"), "config_update"),
            "t3560_auth_secmode": _retrans_envelope(amf_cfg.get("t3560"), "auth_secmode"),
            "t3565_notify": _retrans_envelope(amf_cfg.get("t3565"), "notification"),
            "t3570_identity": _retrans_envelope(amf_cfg.get("t3570"), "identity_request"),
        }.items() if v is not None
    }
    model["smf_procedure_envelopes"] = {
        k: v for k, v in {
            "t3591_pdu_modify": _retrans_envelope(smf_cfg.get("t3591"), "pdu_session_modify"),
            "t3592_pdu_release": _retrans_envelope(smf_cfg.get("t3592"), "pdu_session_release"),
        }.items() if v is not None
    }

    pfcp = upf.get("pfcp", {}) if isinstance(upf, dict) else {}
    retrans_timeout = str(pfcp.get("retransTimeout", "1s")).rstrip("sS") or "1"
    try:
        rt_seconds = float(retrans_timeout)
    except ValueError:
        rt_seconds = 1.0
    max_retrans = pfcp.get("maxRetrans", 3)
    model["pfcp_envelope"] = {"worst_case_s": rt_seconds * (max_retrans + 1)}
    model["expected_urr_period_s"] = smf_cfg.get("urrPeriod")

    model["declared_slices"] = {
        "amf": [
            {"sst": s.get("sst"), "sd": s.get("sd")}
            for plmn in amf_cfg.get("plmnSupportList", [])
            for s in plmn.get("snssaiList", [])
        ],
        "smf": [
            {"sst": info.get("sNssai", {}).get("sst"), "sd": info.get("sNssai", {}).get("sd")}
            for info in smf_cfg.get("snssaiInfos", [])
        ],
    }

    declared_pools: Dict[str, List[Dict[str, Any]]] = {}
    up_nodes = smf_cfg.get("userplaneInformation", {}).get("upNodes", {}) or {}
    for node_name, node in up_nodes.items():
        if node.get("type") != "UPF":
            continue
        for info in node.get("sNssaiUpfInfos", []):
            for dnn in info.get("dnnUpfInfoList", []):
                declared_pools.setdefault(dnn.get("dnn"), []).append({
                    "source": f"smf.userplaneInformation.{node_name}",
                    "pools": [p.get("cidr") for p in dnn.get("pools", [])],
                    "staticPools": [p.get("cidr") for p in dnn.get("staticPools", [])],
                })
    for dnn in (upf.get("dnnList", []) if isinstance(upf, dict) else []):
        declared_pools.setdefault(dnn.get("dnn"), []).append({
            "source": "upf.dnnList", "pools": [dnn.get("cidr")], "staticPools": [],
        })
    model["declared_address_pools"] = declared_pools

    return {"config_diffs": diffs, "expected_model": model}


def validate_behaviour(state: ConfigurationState) -> ConfigurationState:
    findings = []
    model = state.get("expected_model", {})
    events = state.get("log_events", [])

    for ev in events:
        if ev.get("event_type") == "smf_unexpected_state":
            findings.append({
                "check": "smf_reported_state_mismatch", "severity": "violation",
                "detail": ev.get("msg"), "supi": ev.get("supi"),
                "pdu_session_id": ev.get("pdu_session_id"), "time": ev.get("time"),
            })

    amf_slices = {(s["sst"], s["sd"]) for s in model.get("declared_slices", {}).get("amf", [])}
    smf_slices = {(s["sst"], s["sd"]) for s in model.get("declared_slices", {}).get("smf", [])}
    if amf_slices != smf_slices:
        findings.append({
            "check": "cross_file_slice_mismatch", "severity": "violation",
            "detail": f"AMF declares {amf_slices}, SMF declares {smf_slices}",
        })

    for dnn, sources in model.get("declared_address_pools", {}).items():
        variants = {tuple(sorted(s.get("pools", []) + s.get("staticPools", []))) for s in sources}
        if len(sources) > 1 and len(variants) > 1:
            findings.append({
                "check": "cross_file_address_pool_mismatch", "severity": "violation",
                "dnn": dnn, "detail": sources,
            })

    if model.get("expected_urr_period_s"):
        by_session: Dict[str, List[Dict[str, Any]]] = {}
        for ev in events:
            if ev.get("event_type") == "sm_context_update" and ev.get("pdu_session_id"):
                by_session.setdefault(ev["pdu_session_id"], []).append(ev)
        for sid, evs in by_session.items():
            if len(evs) < 2:
                findings.append({
                    "check": "urr_cadence", "severity": "insufficient_data",
                    "pdu_session_id": sid,
                })

    return {"behaviour_findings": findings}


def check_invariants(state: ConfigurationState) -> ConfigurationState:
    findings = []
    events = state.get("log_events", [])

    for ev in events:
        m = TRANSITION_RE.search(ev.get("msg", ""))
        if not m:
            continue
        frm, to = m.group(1), m.group(2)
        if (frm, to) not in LEGAL_GMM_TRANSITIONS:
            findings.append({
                "check": "invariant_1_auth_before_registration", "severity": "violation",
                "detail": f"illegal transition {frm} -> {to}",
                "amf_ue_ngap_id": ev.get("amf_ue_ngap_id"), "time": ev.get("time"),
            })

    registered_ids = {
        ev.get("amf_ue_ngap_id") for ev in events
        if ev.get("event_type") == "registration_complete" and ev.get("amf_ue_ngap_id")
    }
    for ev in events:
        if ev.get("event_type") == "pdu_session_establish_request":
            ue_id = ev.get("amf_ue_ngap_id")
            if ue_id and ue_id not in registered_ids:
                findings.append({
                    "check": "invariant_2_registration_before_pdu_session", "severity": "violation",
                    "detail": "PDU session establishment with no prior registration in this window",
                    "amf_ue_ngap_id": ue_id, "time": ev.get("time"),
                })

    telemetry = state.get("telemetry", {})
    mnf_record = telemetry.get("latest_record")
    mnf_path = telemetry.get("mnf_path")
    if mnf_record and mnf_record.get("congestion_detected"):
        mnf_time = mnf_record.get("timestamp")
        session_activity_nearby = any(
            ev.get("event_type") in ("registration_complete", "pdu_session_establish_accept")
            and ev.get("_epoch") is not None and mnf_time is not None
            and abs(mnf_time - ev["_epoch"]) <= CORRELATION_WINDOW_S
            for ev in events
        )
        if not session_activity_nearby:
            findings.append({
                "check": "invariant_3_session_before_data", "severity": "violation",
                "detail": (
                    f"MnF reports congestion_detected on interface "
                    f"{mnf_record.get('interface')} (rho={mnf_record.get('rho')}, "
                    f"band={mnf_record.get('band')}) with no registration or session "
                    f"establishment activity in the log within {CORRELATION_WINDOW_S}s, "
                    f"consistent with traffic not matching a provisioned session"
                ),
                "mnf_source": mnf_path,
                "mnf_interface": mnf_record.get("interface"),
                "rho": mnf_record.get("rho"),
                "time": mnf_time,
            })

    return {"invariant_findings": findings}


def correlate(state: ConfigurationState) -> ConfigurationState:
    correlated = []
    now = time.time()
    config_diffs = state.get("config_diffs", [])
    all_findings = state.get("behaviour_findings", []) + state.get("invariant_findings", [])

    recent_change = next(
        (d for d in config_diffs if now - d.get("detected_at", 0) <= CORRELATION_WINDOW_S), None
    )

    for finding in all_findings:
        record = dict(finding)
        if recent_change:
            record["root_cause_candidate"] = {
                "type": "config_change", "file": recent_change.get("file"),
                "field": recent_change.get("field"),
                "old_value": recent_change.get("old_value"),
                "new_value": recent_change.get("new_value"),
                "confidence": "candidate",
            }
        else:
            record["root_cause_candidate"] = None
            record["note"] = "deviation with no recent config change in this window, origin unexplained"
        correlated.append(record)

    return {"correlated": correlated}


def format_finding(state: ConfigurationState) -> ConfigurationState:
    records = []
    correlated = state.get("correlated", [])
    config_diffs = state.get("config_diffs", [])

    if not correlated and not config_diffs:
        records.append({
            "agent": AGENT_NAME, "layer": LAYER, "interface": "N2_N3_N4_N6",
            "source": "configuration_agent", "anomaly": False,
            "detail": "no configuration deviations or invariant violations this cycle",
        })
    else:
        flagged_fields = {
            c["root_cause_candidate"]["field"] for c in correlated if c.get("root_cause_candidate")
        }
        for item in correlated:
            records.append({
                "agent": AGENT_NAME, "layer": LAYER, "interface": _infer_interface(item),
                "source": "configuration_agent", "anomaly": True,
                "check": item.get("check"), "severity": item.get("severity"),
                "detail": item.get("detail"), "root_cause_candidate": item.get("root_cause_candidate"),
                "note": item.get("note"),
                "supi": item.get("supi"), "pdu_session_id": item.get("pdu_session_id"),
                "amf_ue_ngap_id": item.get("amf_ue_ngap_id"), "time": item.get("time"),
                "rho": item.get("rho"), "mnf_source": item.get("mnf_source"),
            })
        for diff in config_diffs:
            if diff["field"] not in flagged_fields:
                records.append({
                    "agent": AGENT_NAME, "layer": LAYER, "interface": "config",
                    "source": "configuration_agent", "anomaly": True,
                    "check": "config_change_detected", "severity": "info",
                    "detail": f"{diff['file']}.{diff['field']} changed",
                    "old_value": diff["old_value"], "new_value": diff["new_value"],
                })

    return {"mcp_records": records}


async def publish(state: ConfigurationState) -> ConfigurationState:
    records = state.get("mcp_records", [])
    client = state.get("mcp_client")
    results = []
    for record in records:
        entry = dict(record)
        entry["_written_at"] = time.time()

        try:
            with open(LOCAL_RECORD_LOG, "a") as f:
                f.write(json.dumps(entry) + "\n")
        except OSError as e:
            print(f"[configuration_agent] local record log write failed: {e}", file=sys.stderr)

        print(json.dumps(entry, indent=2))

        #  everything else in the record goes in payload except identity tools, same as Performance and AlertAgents.
        if client is None:
            results.append({"record": record, "mcp_published": False, "error": "no mcp client"})
            continue
        payload = {k: v for k, v in record.items() if k not in ("agent", "layer", "interface", "source")}
        try:
            await client.call_tool("publish_observation", {
                "agent": record.get("agent", AGENT_NAME),
                "layer": record.get("layer", LAYER),
                "interface": record.get("interface", "unknown"),
                "source": record.get("source", "configuration_agent"),
                "payload": payload,
            })
            results.append({"record": record, "mcp_published": True, "error": None})
        except Exception as e:
            results.append({"record": record, "mcp_published": False, "error": str(e)})

    return {"publish_results": results}


def build_graph():
    graph = StateGraph(ConfigurationState)
    graph.add_node("collect_config", collect_config)
    graph.add_node("collect_telemetry", collect_telemetry)
    graph.add_node("collect_events", collect_events)
    graph.add_node("build_expected_model", build_expected_model)
    graph.add_node("validate_behaviour", validate_behaviour)
    graph.add_node("check_invariants", check_invariants)
    graph.add_node("correlate", correlate)
    graph.add_node("format_finding", format_finding)
    graph.add_node("publish", publish)

    graph.set_entry_point("collect_config")
    graph.add_edge("collect_config", "collect_telemetry")
    graph.add_edge("collect_telemetry", "collect_events")
    graph.add_edge("collect_events", "build_expected_model")
    graph.add_edge("build_expected_model", "validate_behaviour")
    graph.add_edge("build_expected_model", "check_invariants")
    graph.add_edge("validate_behaviour", "correlate")
    graph.add_edge("check_invariants", "correlate")
    graph.add_edge("correlate", "format_finding")
    graph.add_edge("format_finding", "publish")
    graph.add_edge("publish", END)
    return graph.compile()


_graph_app = None


async def run_cycle(mcp_client) -> ConfigurationState:
    global _graph_app
    if _graph_app is None:
        _graph_app = build_graph()
    return await _graph_app.ainvoke({"mcp_client": mcp_client})


async def main():
    print(f"[configuration_agent] starting, cycle={CYCLE_SECONDS}s", file=sys.stderr)
    print(f"[configuration_agent] every record is written to {LOCAL_RECORD_LOG} "
          f"and printed below, regardless of MCP status", file=sys.stderr)

    _mcp_cm = Client(MCP_URL, timeout=MCP_TIMEOUT_S)
    mcp_client = await _mcp_cm.__aenter__()
    try:
        await mcp_client.call_tool("register_agent", {
            "agent": AGENT_NAME,
            "description": "Configuration Agent: config-aware behaviour validation",
        })
        print(f"[configuration_agent] registered with MCP as {AGENT_NAME}", file=sys.stderr)
    except Exception as e:
        print(f"[configuration_agent] MCP registration failed: {e}", file=sys.stderr)

    try:
        while True:
            try:
                result = await run_cycle(mcp_client)
                pub = result.get("publish_results", [])
                ok = sum(1 for p in pub if p["mcp_published"])
                print(f"[configuration_agent] cycle complete, {ok}/{len(pub)} record(s) reached MCP "
                      f"({len(pub)} total, all logged locally)", file=sys.stderr)
            except Exception as e:
                print(f"[configuration_agent] cycle error: {e}", file=sys.stderr)
            await asyncio.sleep(CYCLE_SECONDS)
    finally:
        try:
            await _mcp_cm.__aexit__(None, None, None)
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())
