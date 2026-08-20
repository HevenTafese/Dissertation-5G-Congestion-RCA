#!/usr/bin/env python3
"""

Author: Heven Tafese
Agentic RCA engine for 5G congestion.

This agent ends at commit and  writes local logs and returns the conclusion
dictionary. The runner picks it up and calls Explanantion / mitigation.

Two LLM turns per cycle:
  1. investigate_batch: model emits all four tool calls in one JSON
  2. commit_llm: model reads the tool results and emits the final commit

Between the two turns, run_tools_from_plan runs the four tools in the
order the model asked for. Formulas including (rho, causal traversal, transition
history) come from shared/rca_core.


"""

import json
import re
import time
import hashlib
import asyncio
from pathlib import Path
from typing import TypedDict, Optional

import httpx
from fastmcp import Client
from langgraph.graph import StateGraph, END

import sys as _sys
_sys.path.insert(
    0, str(Path(__file__).resolve().parent.parent / "rca_pipeline" / "agents"))
from shared.rca_core import (                                   # noqa: E402
    compute_severity as _tool_severity,
    compute_velocity as _tool_velocity,
    run_causal_trace as _tool_causal,
    detect_transitions as _tool_transitions,
    worst_band,
    INTERFACE_SPEC,
)

# optional layer 2 fusion (Dempster-Shafer). 
_sys.path.insert(0, str(Path(__file__).resolve().parent))
try:
    from congestion_fusion import fuse as _fuse                 # noqa: E402
except Exception:
    _fuse = None

ROOT            = Path(__file__).resolve().parent.parent
MCP_BASE        = "http://localhost:9000"
MCP_URL         = MCP_BASE + "/mcp"
MCP_TIMEOUT_S   = 5.0
OLLAMA_BASE     = "http://192.168.56.1:11434"
EMBED_MODEL     = "nomic-embed-cpu"
LLM_MODEL       = "rca-qwen"
CHROMA_DIR      = str(ROOT / "data" / "chroma_db")
SQLITE_PATH     = str(ROOT / "data" / "kb.sqlite")
NEO4J_URI       = "bolt://localhost:7687"
NEO4J_AUTH      = ("neo4j", "heven123")

_AGENTIC_DATA   = Path(__file__).resolve().parent / "data"
VELOCITY_HISTORY   = str(_AGENTIC_DATA / "agentic_velocity_history.json")
TRANSITION_HISTORY = str(_AGENTIC_DATA / "agentic_band_history.json")
CONCLUSIONS_LOG    = str(_AGENTIC_DATA / "agentic_conclusions.jsonl")
TRACE_LOG          = str(_AGENTIC_DATA / "agentic_reasoning_trace.jsonl")

STALE_OBSERVATION_S = 120
TOP_K_SPEC          = 3
MAX_LLM_STEPS       = 6  # leftover from old controller loop, unused now

# FAILING = congestion sustained >= 75s. Not a higher band, a duration latch.
FAILING_DURATION_S = 75.0


INVESTIGATE_SCHEMA = {
    "type": "object",
    "properties": {
        "reasoning": {"type": "string"},
        "tool_calls": {
            "type": "array",
            "minItems": 4,
            "maxItems": 4,
            "items": {
                "type": "object",
                "properties": {
                    "tool": {"type": "string", "enum": [
                        "compute_severity", "run_causal_trace",
                        "detect_transition", "retrieve_spec"]},
                    "queries": {"type": "array",
                                "items": {"type": "string"}},
                },
                "required": ["tool"],
            },
        },
    },
    "required": ["reasoning", "tool_calls"],
}

COMMIT_SCHEMA = {
    "type": "object",
    "properties": {
        "reasoning":     {"type": "string"},
        "root_cause":    {"type": ["string", "null"]},
        "status":        {"type": "string",
                          "enum": ["baseline", "attributed",
                                   "confirmed_by_severity", "ambiguous"]},
        "is_congestion": {"type": "boolean"},
        "worst_band":    {"type": "string",
                          "enum": ["baseline", "onset", "congestion",
                                   "failure"]},
    },
    "required": ["reasoning", "root_cause", "status",
                 "is_congestion", "worst_band"],
}

MANDATORY_TOOLS = ("compute_severity", "run_causal_trace",
                   "detect_transition", "retrieve_spec")
REQUIRED_RETRIEVE_QUERIES = 4

# fires only when the LLM fails audit twice
DEFAULT_RETRIEVE_QUERIES = [
    "5G network congestion protocol specification clause",
    "network congestion mitigation strategy rate limiting",
    "5G network function topology causal graph relation",
    "5G testbed empirical measurement scenario capture",
]

SYSTEM_PROMPT = (
    "You are the root cause reasoning core of an explainable 5G congestion "
    "management system. You decide, on live telemetry, whether there is "
    "congestion, where it is, and what caused it. You do this over TWO "
    "structured JSON turns per cycle. This is the ONLY protocol; there is "
    "no free-form output at any point.\n\n"
    "TURN 1: INVESTIGATE BATCH. You emit ONE JSON object matching the "
    "investigate schema: a `reasoning` line and a `tool_calls` array of "
    "EXACTLY four entries, one per mandatory tool. The four tools are:\n"
    "  - compute_severity   (rho, band, trend per interface, SQLite)\n"
    "  - run_causal_trace   (causal root from the 3GPP graph, Neo4j)\n"
    "  - detect_transition  (oscillation or sustained failure signature)\n"
    "  - retrieve_spec      (grounding evidence, ChromaDB across all four "
    "collections: normative, remedial, external_graphs, empirical)\n\n"
    "For retrieve_spec you MUST provide at least four queries in its "
    "`queries` array, each broad enough to hit a different evidence "
    "dimension: (1) a 3GPP normative-clause-oriented query naming the "
    "interface, (2) a remedial or mitigation-oriented query naming the "
    "mechanism (rate limit, admission, back-pressure), (3) a topology or "
    "causal-graph-oriented query naming the affected network function, "
    "(4) an empirical or testbed-oriented query naming the scenario shape. "
    "The framework runs every query against every knowledge collection. "
    "Your queries determine breadth. The framework guarantees the "
    "collection coverage.\n\n"
    "TURN 2: COMMIT. After the framework runs your four tool calls and "
    "gives you the results, you emit ONE JSON object matching the commit "
    "schema. It MUST include:\n"
    "  root_cause: the NF or event (e.g. AMF, UPF), or null\n"
    "  status: baseline | attributed | confirmed_by_severity | ambiguous\n"
    "  is_congestion: true/false\n"
    "  worst_band: baseline | onset | congestion | failure\n\n"
    "Band definitions: onset = rho >= 0.85; congestion = rho >= 1.0; "
    "failure = sustained congestion, collapse, or degrading output.\n"
    "COLLAPSE: if the throughput rate is LOW but system resources (CPU, "
    "load) are HIGH and agents flag anomaly, the system has collapsed "
    "under load. The rate is low because it CANNOT process, not because "
    "load decreased. That is congestion or failure, not baseline.\n\n"
    "UNIFORM TREATMENT: baseline cycles receive identical evidence to "
    "congestion cycles. You call all four tools every cycle, with at least "
    "four retrieve_spec queries every cycle, regardless of what the "
    "telemetry looks like. A baseline commit is defensible only when it "
    "is grounded in all four evidence sources.\n\n"
    "Keep every `reasoning` line to ONE short sentence. Never write "
    "paragraphs."
)


class RCAState(TypedDict):
    raw_observations: dict
    active: list
    anomalous_interfaces: list
    cross_plane_suspected: bool
    messages: list
    trace: list
    step: int
    action: str
    action_query: str
    severity: dict
    causal: dict
    transition: dict
    retrieved: list
    exec_log: list
    llm_root_cause: Optional[str]
    llm_status: Optional[str]
    llm_is_congestion: Optional[bool]
    cycle_id: float
    conclusion: dict
    fusion: dict
    config_findings: list
    tools_called: list
    llm_worst_band: Optional[str]
    queue_fed: bool
    investigate_plan: list
    retrieve_queries: list


def _ollama_chat(messages, schema=None, timeout=300.0):
    try:
        body = {"model": LLM_MODEL, "messages": messages, "stream": False,
                "keep_alive": "30m",
                "options": {"temperature": 0.1, "num_ctx": 8192,
                            "num_predict": 4096}}
        if schema is not None:
            body["format"] = schema
        r = httpx.post(f"{OLLAMA_BASE}/api/chat", json=body, timeout=timeout)
        r.raise_for_status()
        content = r.json().get("message", {}).get("content", "")
        return _parse_decision(content)
    except (httpx.HTTPError, httpx.TimeoutException) as e:
        print(f"    [ollama] decision unavailable: {e}")
        return None
    except Exception as e:
        print(f"    [ollama] unexpected error: {e}")
        return None


def _parse_decision(content):
    # tolerates <think> blocks and markdown fences in case the model ever changes
    if not content:
        return None
    content = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL)
    content = re.sub(r"```(?:json)?", "", content)
    start = content.find("{")
    end = content.rfind("}")
    if start != -1 and end != -1 and end > start:
        content = content[start:end + 1]
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        print(f"    [ollama] unparseable decision after strip: {content[:120]!r}")
        return None


def _ollama_embed(text):
    try:
        r = httpx.post(f"{OLLAMA_BASE}/api/embeddings",
                       json={"model": EMBED_MODEL, "prompt": text}, timeout=60.0)
        r.raise_for_status()
        return r.json()["embedding"]
    except Exception as e:
        print(f"    [ollama] embed unavailable: {e}")
        return None


def collect(state: RCAState) -> RCAState:
    injected = state.get('raw_observations')
    if injected:
        obs = injected
        state['queue_fed'] = True
        print(f"[Collect] {len(obs)} key(s) from queued snapshot")
    else:
        async def _fetch():
            async with Client(MCP_URL, timeout=MCP_TIMEOUT_S) as c:
                res = await c.call_tool("get_current_observations", {})
                return (res.data or {}).get("observations", {})
        try:
            obs = asyncio.run(_fetch())
        except Exception as e:
            print(f"[Collect] MCP unreachable: {e}")
            obs = {}
        print(f"[Collect] {len(obs)} key(s) on MCP (live fetch)")

    # lift plane and timestamp out of payload so triage sees them at the top level
    now = time.time()
    for _o in obs.values():
        _pl = _o.get("payload") or {}
        if not _o.get("timestamp"):
            _o["timestamp"] = _pl.get("timestamp") or _o.get("_received_at") or now
        if not _o.get("plane"):
            _o["plane"] = _pl.get("plane", "")

    state['raw_observations'] = obs
    return state


def triage(state: RCAState) -> RCAState:
    now = time.time()
    active = []
    for key, obs in state['raw_observations'].items():
        if obs.get("agent") in ("rag", "rag_enrich"):
            continue
        ts = obs.get("timestamp") or 0
        # staleness only applies on the live path. Queued backlog is meant to be old.
        if (not state.get('queue_fed')) and ts and (now - ts) > STALE_OBSERVATION_S:
            print(f"[Triage] {key} stale ({now - ts:.0f}s) -- skipped")
            continue

        payload = obs.get("payload")
        if payload:
            assessment = payload.get("assessment", {}) or {}
            severity_lbl = assessment.get("severity", "normal")
            entry = {
                "key": key, "agent": obs.get("agent", key.split(":")[0]),
                "layer": obs.get("layer", ""), "interface": obs.get("interface", ""),
                "plane": obs.get("plane", ""), "source": obs.get("source", ""),
                "pattern": None,
                "anomaly": severity_lbl in ("warning", "critical"),
                "confidence": payload.get("agent_confidence", ""),
                "reasons": [f"{m} crossed threshold"
                            for m in assessment.get("crossed_metrics", [])],
                "ts": ts, "metrics": payload.get("telemetry", {}) or {},
                "velocity": {}, "severity": severity_lbl,
                "corroboration": assessment.get("corroboration", "normal"),
            }
        else:
            entry = {
                "key": key, "agent": obs.get("agent", key.split(":")[0]),
                "layer": obs.get("layer", ""), "interface": obs.get("interface", ""),
                "plane": obs.get("plane", ""), "source": obs.get("source", ""),
                "pattern": obs.get("pattern_match", "normal"),
                "anomaly": bool(obs.get("anomaly", False)),
                "confidence": obs.get("confidence", ""),
                "reasons": obs.get("anomaly_reasons", []) or [],
                "ts": ts, "metrics": obs.get("metrics", {}) or {},
                "velocity": obs.get("velocity", {}) or {},
                "severity": None, "corroboration": None,
            }
        active.append(entry)

    active = _tool_velocity(active, VELOCITY_HISTORY)

    # override the per agent anomaly label the rho band rpoduced here if it exits
    try:
        _own_sev = _tool_severity(active, SQLITE_PATH)
    except Exception:
        _own_sev = {}
    fusion = {}
    for _a in active:
        _iface = _a.get("interface", "")
        _own_band = (_own_sev.get(_iface) or {}).get("band")
        if _own_band is not None:
            _a["anomaly"] = _own_band in ("onset", "congestion", "failure")
            _a["own_band"] = _own_band
            _a["own_rho"] = (_own_sev.get(_iface) or {}).get("rho")
        _tel = _a.get("metrics", {}) or {}
        _caps = {}
        if _tel.get("rho_pps") is not None:
            _caps["rho_pps"] = float(_tel["rho_pps"])
        if _tel.get("rho_tput") is not None:
            _caps["rho_tput"] = float(_tel["rho_tput"])
        if _caps and _fuse is not None:
            try:
                _fr = _fuse(_caps, {"rho_pps": 0.9, "rho_tput": 0.9})
                fusion[_iface] = {
                    "belief_congestion": _fr["belief_congestion"],
                    "disagreement": _fr["disagreement"],
                    "own_band": _own_band}
            except Exception:
                pass
    state['fusion'] = fusion
    state['active'] = active
    anomalous = [a for a in active if a["anomaly"]]
    state['anomalous_interfaces'] = sorted({a["interface"] for a in anomalous})
    planes = {a["plane"] for a in anomalous if a["plane"]}
    state['cross_plane_suspected'] = (
        len(state['anomalous_interfaces']) > 1 or len(planes) > 1)

    
    config_findings = []
    for _ckey, _cobs in state['raw_observations'].items():
        if _cobs.get("agent") != "configuration":
            continue
        _cpl = _cobs.get("payload") or {}
        if _cpl.get("anomaly") and _cpl.get("check"):
            config_findings.append({
                "interface": _cobs.get("interface", ""),
                "check": _cpl.get("check"),
                "severity": _cpl.get("severity"),
                "detail": _cpl.get("detail"),
            })
    state['config_findings'] = config_findings

    if active:
        print(f"[Prepare] {len(active)} fresh obs:")
        for _a in active:
            _agent = _a.get('agent', '?')
            _iface = _a.get('interface', '?') or '?'
            _plane = _a.get('plane', '') or '?'
            _layer = _a.get('layer', '') or '?'
            _band  = _a.get('own_band') or 'uncalibrated'
            _tel   = _a.get('metrics', {}) or {}
            # our own rho first. Agent-published rho can mean something else (e.g. e2e smoothing).
            _rho = _a.get('own_rho')
            if _rho is None:
                for _k in ('rho', 'rho_pps', 'rho_tput'):
                    if _tel.get(_k) is not None:
                        _rho = _tel[_k]
                        break
            _flag = ' [ANOMALY]' if _a.get('anomaly') else ''
            print(f"  - {_agent} on {_iface} ({_layer}/{_plane}): "
                  f"rho={_rho}, band={_band}{_flag}")
        print(f"[Prepare] anomalous_interfaces="
              f"{state['anomalous_interfaces'] or 'none'}, "
              f"cross_plane={state['cross_plane_suspected']}"
              f"{f', {len(config_findings)} config finding(s) for context' if config_findings else ''}")
    else:
        print(f"[Prepare] no fresh observations this cycle "
              f"(all filtered out by staleness or upstream agents idle)")
    return state


def _audit_investigate(plan):
    reasons = []
    if not isinstance(plan, dict):
        return False, ["not a dict"]
    tool_calls = plan.get("tool_calls", [])
    if not isinstance(tool_calls, list) or len(tool_calls) != 4:
        reasons.append(
            f"tool_calls must be an array of exactly 4 items "
            f"(got {len(tool_calls) if isinstance(tool_calls, list) else 'non-list'})")
    tools_seen = [tc.get("tool") for tc in tool_calls if isinstance(tc, dict)]
    for t in MANDATORY_TOOLS:
        if t not in tools_seen:
            reasons.append(f"missing mandatory tool: {t}")
    if len(set(tools_seen)) != len(tools_seen):
        reasons.append(f"duplicate tools in plan: {tools_seen}")
    for tc in tool_calls:
        if isinstance(tc, dict) and tc.get("tool") == "retrieve_spec":
            qs = tc.get("queries") or []
            if len(qs) < REQUIRED_RETRIEVE_QUERIES:
                reasons.append(
                    f"retrieve_spec must have >= {REQUIRED_RETRIEVE_QUERIES} "
                    f"queries (got {len(qs)})")
    return (not reasons), reasons


def _fallback_investigate_plan(anomalous_interfaces):
    queries = list(DEFAULT_RETRIEVE_QUERIES)
    if anomalous_interfaces:
        queries[0] = (" ".join(anomalous_interfaces) +
                      " protocol specification clause 5G")
    return {
        "reasoning": "Deterministic fallback: LLM did not produce a "
                     "well-formed investigate plan after one retry.",
        "tool_calls": [
            {"tool": "compute_severity"},
            {"tool": "run_causal_trace"},
            {"tool": "detect_transition"},
            {"tool": "retrieve_spec", "queries": queries},
        ],
    }


def investigate_batch(state: RCAState) -> RCAState:
    # seed messages on first entry
    if not state.get('messages'):
        lines = []
        for a in state['active']:
            mets = ", ".join(f"{k}={v}" for k, v in list(a["metrics"].items())[:8])
            lines.append(f"- {a['agent']} agent on {a['interface']} "
                         f"({a['layer']}, {a['plane']} plane): {mets}")
        telem = "\n".join(lines) if lines else "- (none)"
        cfg_section = ""
        if state.get('config_findings'):
            cfg_lines = "\n".join(
                f"- {c.get('check')} on {c.get('interface')} "
                f"({c.get('severity')}): {c.get('detail')}"
                for c in state['config_findings'])
            cfg_section = ("\nConfiguration-validation findings this cycle "
                           "(from the configuration agent, for context, these "
                           "are not themselves the congestion signal):\n"
                           f"{cfg_lines}\n")
        state['messages'] = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content":
                f"Live telemetry this cycle (all agents):\n{telem}\n"
                f"{cfg_section}\n"
                f"Interfaces flagged: {', '.join(state['anomalous_interfaces'])}. "
                f"Cross-plane suspected: {state['cross_plane_suspected']}.\n"
                "Emit the investigate batch now (all four tool_calls, at "
                f"least {REQUIRED_RETRIEVE_QUERIES} queries for retrieve_spec)."}]
        state['trace'] = []
        state['step'] = 0

    plan = None
    audit_reasons = []
    for attempt in range(2):
        state['step'] += 1
        plan = _ollama_chat(state['messages'], schema=INVESTIGATE_SCHEMA)
        if plan is None:
            state['action'] = "abstain"
            _record_step(
                state, "llm_unreachable", "abstain", {}, {},
                {"source": "agentic_rca investigate_batch", "db": "none",
                 "rule": "no genuine LLM verdict this cycle, abstaining "
                         "rather than fabricating a commit",
                 "refs": []},
                claim="LLM unreachable on investigate, abstaining",
                source="agentic_rca investigate_batch")
            return state
        state['messages'].append(
            {"role": "assistant", "content": json.dumps(plan)})
        ok, audit_reasons = _audit_investigate(plan)
        if ok:
            break
        if attempt == 0:
            state['messages'].append({"role": "user", "content":
                f"Your last plan failed audit: {'; '.join(audit_reasons)}. "
                f"Reissue exactly four tool_calls covering "
                f"{list(MANDATORY_TOOLS)}, with at least "
                f"{REQUIRED_RETRIEVE_QUERIES} queries for retrieve_spec."})
            print(f"[Investigate] retry, audit failed: {audit_reasons}")

    if not ok:
        print(f"[Investigate] fallback, audit still failing: {audit_reasons}")
        plan = _fallback_investigate_plan(state['anomalous_interfaces'])
        state['messages'].append(
            {"role": "assistant", "content": json.dumps(plan)})

    state['investigate_plan'] = plan.get("tool_calls", [])
    state['retrieve_queries'] = []
    for tc in state['investigate_plan']:
        if tc.get("tool") == "retrieve_spec":
            state['retrieve_queries'] = list(tc.get("queries") or [])
            break

    state['action'] = "tools"
    _record_step(
        state, "llm_investigate", "investigate",
        {"step": state['step'], "attempts": attempt + 1},
        {"tool_calls": state['investigate_plan'],
         "retrieve_queries": state['retrieve_queries'],
         "audit_ok": ok},
        {"source": "agentic_rca:rca-qwen", "db": "none",
         "rule": "batched tool-call authoring (Inference 1)",
         "refs": []},
        claim=f"investigate batch: {[tc.get('tool') for tc in state['investigate_plan']]}, "
              f"{len(state['retrieve_queries'])} retrieve queries",
        source="agentic_rca:rca-qwen (investigate_batch)")
    print(f"[Investigate] plan={[tc.get('tool') for tc in state['investigate_plan']]} "
          f"queries={len(state['retrieve_queries'])}")
    return state


def run_tools_from_plan(state: RCAState) -> RCAState:
    fn_map = {
        "compute_severity":  tool_severity,
        "run_causal_trace":  tool_causal,
        "detect_transition": tool_transition,
        "retrieve_spec":     tool_retrieve,
    }
    for tc in state.get('investigate_plan', []):
        name = tc.get("tool")
        fn = fn_map.get(name)
        if fn is None:
            print(f"[Tools] skip unknown tool: {name}")
            continue
        fn(state)
    return state


def commit_llm(state: RCAState) -> RCAState:
    state['messages'].append({"role": "user", "content":
        "All four tool results are above. Emit the commit JSON now "
        "(reasoning, root_cause, status, is_congestion, worst_band)."})
    state['step'] += 1
    decision = _ollama_chat(state['messages'], schema=COMMIT_SCHEMA)

    if decision is None:
        state['action'] = "abstain"
        _record_step(
            state, "llm_unreachable", "abstain", {}, {},
            {"source": "agentic_rca commit_llm", "db": "none",
             "rule": "no genuine LLM verdict this cycle, abstaining "
                     "rather than fabricating a commit",
             "refs": []},
            claim="LLM unreachable on commit, abstaining",
            source="agentic_rca commit_llm")
        return state

    state['messages'].append({"role": "assistant", "content": json.dumps(decision)})
    state['llm_root_cause']    = decision.get("root_cause")
    state['llm_status']        = decision.get("status")
    state['llm_is_congestion'] = decision.get("is_congestion")
    state['llm_worst_band']    = decision.get("worst_band")
    state['action'] = "commit"

    _record_step(
        state, "llm_decision", "commit_llm",
        {"step": state['step']},
        {"root_cause": state['llm_root_cause'],
         "status": state['llm_status'],
         "is_congestion": state['llm_is_congestion'],
         "worst_band": state['llm_worst_band'],
         "reasoning": (decision.get("reasoning") or "").strip()},
        {"source": "agentic_rca:rca-qwen", "db": "none",
         "rule": "final commit judgment (Inference 2)",
         "refs": []},
        claim=f"commit: root={state['llm_root_cause']}, "
              f"status={state['llm_status']}, "
              f"band={state['llm_worst_band']}",
        source="agentic_rca:rca-qwen (commit_llm)")
    print(f"[CommitLLM] root={state['llm_root_cause']} "
          f"status={state['llm_status']} "
          f"band={state['llm_worst_band']}")
    return state


def route_after_investigate(state: RCAState) -> str:
    return "abstain" if state.get('action') == "abstain" else "tools"


def route_after_commit_llm(state: RCAState) -> str:
    return "abstain" if state.get('action') == "abstain" else "commit"


def _feed(state, summary):
    state['messages'].append({"role": "user", "content": f"tool result: {summary}"})


def _record_step(state, tool, phase, inputs, output,
                 provenance, claim=None, source=None):
    state.setdefault('exec_log', [])
    state['exec_log'].append({
        "step": len(state['exec_log']) + 1,
        "tool": tool,
        "phase": phase,
        "input": inputs,
        "output": output,
        "provenance": provenance,
        "timestamp": time.time(),
    })
    state.setdefault('trace', [])
    state['trace'].append({"claim": claim or tool,
                           "source": source or provenance.get("source", "")})


def tool_severity(state: RCAState) -> RCAState:
    state['severity'] = _tool_severity(state['active'], SQLITE_PATH)
    parts = []
    for iface, s in state['severity'].items():
        if s.get("calibrated"):
            parts.append(f"{iface}: rho={s['rho']} band={s['band']} "
                         f"trend={s.get('trend')}")
        else:
            parts.append(f"{iface}: uncalibrated ({s.get('calibration_issue')})")
    summary = "; ".join(parts) or "no calibrated anomalous interface"
    _feed(state, f"severity -> {summary}")
    _record_step(
        state, "compute_severity", "investigate",
        {"observations": [{"agent": a.get("agent"), "interface": a.get("interface"),
                           "plane": a.get("plane"), "anomaly": a.get("anomaly"),
                           "metrics": a.get("metrics", {})}
                          for a in state['active']]},
        state['severity'],
        {"source": "rca_core.compute_severity", "db": "SQLite interface_baselines",
         "rule": "rho = (L/a)/R ; bands: onset >= 0.85, congestion >= 1.0",
         "refs": [f"{iface}:baseline_R" for iface in state['severity'].keys()]},
        claim=f"severity: {summary}",
        source="rca_core.compute_severity; rho=(L/a)/R")
    print(f"[Tool] severity -> {summary}")
    state.setdefault('tools_called', []).append('compute_severity')
    return state


def tool_causal(state: RCAState) -> RCAState:
    if not state.get('severity'):
        state['severity'] = _tool_severity(state['active'], SQLITE_PATH)
    causal, _, _ = _tool_causal(state['active'], NEO4J_URI, NEO4J_AUTH,
                                state['cross_plane_suspected'],
                                severity=state['severity'])
    state['causal'] = causal
    summary = (f"root={causal.get('root')} "
               f"seeded={causal.get('severity_seeded_nodes') or 'no'} "
               f"chain_len={len(causal.get('chain', []))} "
               f"confidence={causal.get('confidence')} "
               f"feedback_loop={causal.get('feedback_loop')}")
    _feed(state, f"causal -> {summary}")
    _record_step(
        state, "run_causal_trace", "investigate",
        {"anomalous_interfaces": state['anomalous_interfaces'],
         "cross_plane_suspected": state['cross_plane_suspected'],
         "severity": state['severity']},
        causal,
        {"source": "rca_core.run_causal_trace", "db": "Neo4j 3GPP graph",
         "rule": "traverse CAUSAL_EDGE_TYPES from anomalous / severity-seeded "
                 "NFs to a single root candidate",
         "refs": (causal.get("root_source_refs") or [])
                 + [str(c) for c in causal.get("chain", [])]},
        claim=f"causal: {summary}",
        source="rca_core.run_causal_trace; 3GPP graph")
    print(f"[Tool] causal -> {summary}")
    state.setdefault('tools_called', []).append('run_causal_trace')
    return state


def tool_transition(state: RCAState) -> RCAState:
    if not state.get('severity'):
        state['severity'] = _tool_severity(state['active'], SQLITE_PATH)
    state['transition'] = _tool_transitions(state['severity'], TRANSITION_HISTORY)
    parts = []
    for iface, t in state['transition'].items():
        flags = []
        if t.get("transitioned"):
            flags.append(f"{t['previous_band']}->{t['current_band']}")
        if t.get("oscillating"):
            flags.append("oscillating")
        if t.get("sustained_congestion_s", 0) >= 75.0:
            flags.append(f"sustained {t['sustained_congestion_s']}s")
        if flags:
            parts.append(f"{iface}: {', '.join(flags)}")
    summary = "; ".join(parts) or "no transition of note"
    _feed(state, f"transition -> {summary}")
    _record_step(
        state, "detect_transition", "investigate",
        {"severity": state['severity']},
        state['transition'],
        {"source": "rca_core.detect_transitions", "db": "local band_history",
         "rule": "escalate on oscillation OR sustained_congestion_s >= 75.0",
         "refs": list(state['transition'].keys())},
        claim=f"transition: {summary}",
        source="rca_core.detect_transitions")
    print(f"[Tool] transition -> {summary}")
    state.setdefault('tools_called', []).append('detect_transition')
    return state


def tool_retrieve(state: RCAState) -> RCAState:
    queries = list(state.get('retrieve_queries') or [])
    if not queries:
        queries = list(DEFAULT_RETRIEVE_QUERIES)
        if state['anomalous_interfaces']:
            queries[0] = (" ".join(state['anomalous_interfaces']) +
                          " protocol specification clause 5G")

    hits = []
    seen = set()
    per_collection = {"normative": 0, "remedial": 0,
                      "external_graphs": 0, "empirical": 0}

    def _add(collection, source, clause, text):
        key = (collection, source, clause, (text or "")[:60])
        if key in seen:
            return
        seen.add(key)
        hits.append({"collection": collection, "source": source,
                     "clause": clause, "text": (text or "")[:200]})
        per_collection[collection] += 1

    try:
        import chromadb
        client = chromadb.PersistentClient(path=CHROMA_DIR)
    except Exception as e:
        print(f"    [retrieve] ChromaDB unavailable: {e}")
        client = None

    for q in queries:
        qvec = _ollama_embed(q)
        if qvec is None or client is None:
            continue

        specs = {INTERFACE_SPEC.get(i) for i in state['anomalous_interfaces']}
        filtered_specs = list(filter(None, specs))
        norm_queries = [(spec, {"source": spec}) for spec in filtered_specs]
        norm_queries.append((None, None))  # unfiltered always runs
        try:
            col = client.get_collection("normative")
            for spec, where in norm_queries:
                try:
                    kwargs = {"query_embeddings": [qvec], "n_results": TOP_K_SPEC}
                    if where:
                        kwargs["where"] = where
                    res = col.query(**kwargs)
                    for i in range(len(res["ids"][0])):
                        md = res["metadatas"][0][i] or {}
                        _add("normative",
                             md.get("source", spec or "unfiltered"),
                             md.get("clause", ""),
                             res["documents"][0][i])
                except Exception as e:
                    print(f"    [retrieve] normative "
                          f"{spec or 'unfiltered'} failed: {e}")
        except Exception as e:
            print(f"    [retrieve] normative collection unavailable: {e}")

        for cname in ("remedial", "external_graphs", "empirical"):
            try:
                col = client.get_collection(cname)
                res = col.query(query_embeddings=[qvec], n_results=TOP_K_SPEC)
                ids = res.get("ids", [[]])
                if not ids or not ids[0]:
                    continue
                for i in range(len(ids[0])):
                    md = (res["metadatas"][0][i] or {}) if res.get("metadatas") else {}
                    doc = res["documents"][0][i] if res.get("documents") else ""
                    _add(cname, md.get("source", cname),
                         md.get("clause", ""), doc)
            except Exception as e:
                print(f"    [retrieve] {cname} unavailable/empty: {e}")

    state.setdefault('retrieved', [])
    state['retrieved'].extend(hits)

    if hits:
        summary = "; ".join(
            f"[{h['collection']}] {h['source']} {h.get('clause','')}: {h['text'][:60]}"
            for h in hits[:4])
    else:
        summary = "no grounded evidence found in any collection"
    counts = ", ".join(f"{k}={v}" for k, v in per_collection.items())
    _feed(state, f"retrieve({len(queries)} queries) across all KB collections "
                 f"({counts}) -> {summary}")
    _record_step(
        state, "retrieve_spec", "investigate",
        {"queries": queries,
         "collections_queried": ["normative", "remedial",
                                 "external_graphs", "empirical"],
         "specs": sorted(filter(None, {INTERFACE_SPEC.get(i)
                                       for i in state['anomalous_interfaces']}))},
        hits,
        {"source": "ChromaDB (all collections)",
         "db": "ChromaDB: normative + remedial + external_graphs + empirical",
         "rule": "vector top-k similarity (nomic-embed-text) fanned across "
                 "every query x every collection; normative additionally "
                 "runs the spec-filtered branch per query; dedup on "
                 "(collection, source, clause, text[:60])",
         "per_collection_counts": per_collection,
         "queries_run": len(queries),
         "refs": [f"[{h['collection']}] {h['source']} {h.get('clause', '')}".strip()
                  for h in hits]},
        claim=f"retrieved evidence from {len(queries)} queries "
              f"across 4 collections ({counts})",
        source="ChromaDB all collections")
    print(f"[Tool] retrieve -> {len(hits)} unique chunk(s) across collections "
          f"({counts}) from {len(queries)} queries")
    state.setdefault('tools_called', []).append('retrieve_spec')
    return state


def commit(state: RCAState) -> RCAState:
    sev = state['severity']

    worst_by_rule = worst_band(sev)
    worst = state.get('llm_worst_band') or worst_by_rule
    is_congestion = (state['llm_is_congestion']
                     if state['llm_is_congestion'] is not None
                     else worst in ("onset", "congestion", "failure"))
    status = state['llm_status'] or (
        "confirmed_by_severity" if is_congestion else "baseline")
    root = state['llm_root_cause'] or state.get('causal', {}).get('root')

    orig_status = status
    orig_worst = worst
    severity_supported = any(s.get("calibrated") for s in sev.values())
    transition = state.get('transition', {}) or {}
    failure_supported = any(
        t.get("sustained_congestion_s", 0) >= FAILING_DURATION_S
        or t.get("oscillating", False)
        for t in transition.values())
    floor_fired = []
    if status == "confirmed_by_severity" and not severity_supported:
        status = "ambiguous"
        floor_fired.append(
            "status: confirmed_by_severity -> ambiguous "
            "(severity calibrated nothing this cycle)")
    if worst == "failure" and not failure_supported:
        worst = "congestion"
        floor_fired.append(
            "worst_band: failure -> congestion "
            f"(transition found no signature >= {FAILING_DURATION_S}s)")

    _record_step(
        state, "commit_decision", "commit",
        {"severity": sev, "causal": state['causal'],
         "transition": state.get('transition', {}),
         "worst_band_by_rule": worst_by_rule, "worst_band_llm": state.get('llm_worst_band')},
        {"root_cause": root, "status": status,
         "is_congestion": bool(is_congestion), "worst_band": worst,
         "llm_status_original": orig_status,
         "llm_worst_band_original": orig_worst,
         "evidence_floor_fired": floor_fired},
        {"source": "agentic_rca:rca-qwen (final commit)", "db": "none",
         "rule": "MODEL JUDGMENT. No deterministic rule governs is_congestion "
                 "or root_cause. Status and worst_band pass through an "
                 "evidence-consistency floor that corrects only factually "
                 "unsupported labels, logged above when it fires.",
         "refs": []},
        claim=f"decision: root_cause={root}, status={status}, "
              f"is_congestion={is_congestion}"
              + (f" [evidence floor: {'; '.join(floor_fired)}]"
                 if floor_fired else ""),
        source="agentic_rca:rca-qwen (final commit)")

    chain = list(state.get('trace', []))

    supporting = [{"source": h["source"], "clause": h.get("clause", ""),
                   "collection": "normative"}
                  for h in state.get('retrieved', [])[:8]]

    # display only field for mitigation_agent's log line
    monitored_interfaces = sorted({a.get("interface", "") for a in state.get('active', [])
                                   if a.get("interface")})

    state['cycle_id'] = time.time()
    state['conclusion'] = {
        "is_congestion": bool(is_congestion),
        "status": status,
        "root_cause": root,
        "affected_interfaces": state['anomalous_interfaces'],
        "monitored_interfaces": monitored_interfaces,
        "severity": {k: v for k, v in sev.items()},
        "worst_band": worst,
        "triage_fusion": state.get('fusion', {}),
        "confidence": state['causal'].get("confidence", "low"),
        "feedback_loop": state['causal'].get("feedback_loop", False),
        "explanation_chain": chain,
        "supporting_chunks": supporting,
        "narrative": None,
    }

    c = state['conclusion']
    try:
        _AGENTIC_DATA.mkdir(parents=True, exist_ok=True)
        with open(CONCLUSIONS_LOG, "a") as f:
            f.write(json.dumps({"t": time.time(), "cycle_id": state['cycle_id'],
                                "conclusion": c}, default=str) + "\n")
        with open(TRACE_LOG, "a") as f:
            f.write(json.dumps({"t": time.time(), "cycle_id": state['cycle_id'],
                                "steps": state.get('step', 0),
                                "trace": state.get('trace', []),
                                "exec_log": state.get('exec_log', [])},
                               default=str) + "\n")
    except (IOError, OSError) as e:
        print(f"[Commit] local log write failed: {e}")

    floor_msg = f" [floor: {len(floor_fired)} correction(s)]" if floor_fired else ""
    print(f"[Commit] status={status} root={root} worst_band={worst} "
          f"congestion={is_congestion} steps={state.get('step', 0)}{floor_msg}")
    return state


def route_after_collect(state: RCAState) -> str:
    return "triage" if state['raw_observations'] else "end"


def build_agentic_rca():
    g = StateGraph(RCAState)
    g.add_node("collect", collect)
    g.add_node("triage", triage)
    g.add_node("investigate_batch", investigate_batch)
    g.add_node("run_tools_from_plan", run_tools_from_plan)
    g.add_node("commit_llm", commit_llm)
    g.add_node("commit", commit)

    g.set_entry_point("collect")
    g.add_conditional_edges("collect", route_after_collect,
                            {"triage": "triage", "end": END})
    g.add_edge("triage", "investigate_batch")
    g.add_conditional_edges("investigate_batch", route_after_investigate,
                            {"tools": "run_tools_from_plan", "abstain": END})
    g.add_edge("run_tools_from_plan", "commit_llm")
    g.add_conditional_edges("commit_llm", route_after_commit_llm,
                            {"commit": "commit", "abstain": END})
    g.add_edge("commit", END)
    return g.compile()


def fresh_state():
    return RCAState(
        raw_observations={}, active=[], anomalous_interfaces=[],
        cross_plane_suspected=False, messages=[], trace=[], step=0,
        action="", action_query="", severity={}, causal={}, transition={},
        retrieved=[], exec_log=[], llm_root_cause=None, llm_status=None,
        llm_is_congestion=None, cycle_id=0.0, conclusion={}, fusion={},
        config_findings=[], tools_called=[], llm_worst_band=None,
        queue_fed=False,
        investigate_plan=[], retrieve_queries=[])


if __name__ == "__main__":
    print("=== Agentic RCA: single-cycle structural check ===\n")
    agent = build_agentic_rca()
    result = agent.invoke(fresh_state(), config={"recursion_limit": 60})
    if result.get("conclusion"):
        print("\n=== conclusion ===")
        print(json.dumps(result["conclusion"], indent=2)[:3000])
