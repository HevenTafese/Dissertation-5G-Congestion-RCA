#!/usr/bin/env python3
"""Explanation Agent

Author: Heven Tafese

This agent verifies and narrates the RCA engine actions, step by step, using the exec_log .
Each step is rendered by _describe_step from the fields _record_step() and writes claim, provenance, and input.
"""
import re
import json
import time
from pathlib import Path
from typing import TypedDict, Optional

import httpx
from langgraph.graph import StateGraph, END

import sys as _sys
_sys.path.insert(
    0, str(Path(__file__).resolve().parent.parent / "rca_pipeline" / "agents"))
from shared.rca_core import worst_band

_AGENTIC_DATA = Path(__file__).resolve().parent / "data"
TRACE_LOG       = str(_AGENTIC_DATA / "agentic_reasoning_trace.jsonl")
CONCLUSIONS_LOG = str(_AGENTIC_DATA / "agentic_conclusions.jsonl")
PACKET_LOG      = str(_AGENTIC_DATA / "explanation_packets.jsonl")
OLLAMA_BASE = "http://192.168.56.1:11434"
LLM_MODEL   = "rca-qwen"

CONGESTED_BANDS = ("onset", "congestion", "failure")

MANDATORY_TOOLS = ("compute_severity", "run_causal_trace",
                   "detect_transition", "retrieve_spec")


class ExpState(TypedDict):
    exec_log: list
    conclusion: dict
    cycle_id: float
    provenance: dict
    necessity: dict
    divergence: dict
    render: dict
    prose_check: dict
    packet: dict
    completeness: dict
    evidence_report: dict


def _load_latest(path):
    try:
        last = None
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line:
                    last = line
        return json.loads(last) if last else None
    except (IOError, json.JSONDecodeError):
        return None


def _find(exec_log, tool):
    return next((s for s in exec_log if s.get("tool") == tool), None)


def _find_last(exec_log, tool):
    return next((s for s in reversed(exec_log) if s.get("tool") == tool), None)


def load_record(state: ExpState) -> ExpState:
    if state.get('exec_log'):
        return state
    trace_rec = _load_latest(TRACE_LOG)
    conc_rec = _load_latest(CONCLUSIONS_LOG)
    state['exec_log'] = (trace_rec or {}).get('exec_log', []) or []
    state['conclusion'] = (conc_rec or {}).get('conclusion', {}) or {}
    state['cycle_id'] = (trace_rec or {}).get('cycle_id', 0.0)
    print(f"[load_record] {len(state['exec_log'])} step(s), "
          f"cycle_id={state['cycle_id']}")
    return state


def route_after_load(state: ExpState) -> str:
    return "explain" if state['exec_log'] else "trivial"


def baseline_note(state: ExpState) -> ExpState:
    prose = ("No execution steps were recorded for this cycle, so there is "
             "nothing to trace. This typically means the engine could not run "
             "(for example the reasoning model was unreachable and no fallback "
             "step was logged).")
    state['provenance'] = {"nodes": [], "edges": []}
    state['necessity'] = {}
    state['divergence'] = {"agrees": True, "divergences": []}
    state['completeness'] = {"tools_all_present": None, "missing_tools": [],
                             "provenance_gaps": [], "root_cause_traceable": None,
                             "note": "empty exec_log, no steps to check"}
    state['render'] = {"prose": prose, "mode": "template", "template": prose}
    state['prose_check'] = {"sentences": 2, "unbacked": [], "all_backed": True}
    state['evidence_report'] = {"entries": [], "step_count": 0}
    print("[baseline_note] empty exec_log, nothing to trace")
    return state


def build_provenance(state: ExpState) -> ExpState:
    steps = state['exec_log']
    node_ids = set()
    nodes, edges = [], []

    def add_node(nid, kind, phase):
        if nid not in node_ids:
            node_ids.add(nid)
            nodes.append({"id": nid, "kind": kind, "phase": phase})

    def sid_of(s):
        return f"step{s['step']}:{s['tool']}"

    for s in steps:
        sid = sid_of(s)
        add_node(sid, s['tool'], s.get('phase', ''))
        for ref in (s.get('provenance', {}).get('refs') or []):
            rid = f"ref:{ref}"
            add_node(rid, "evidence", "-")
            edges.append({"src": rid, "rel": "Support", "dst": sid})

    seen = []
    for s in steps:
        this_sid = sid_of(s)
        inp = s.get('input', {}) or {}
        input_keys = set()
        if isinstance(inp, dict):
            input_keys = {k.lower() for k in inp.keys() if isinstance(k, str)}
        for earlier in seen:
            e_tool = (earlier.get('tool') or '').lower()
            for key in input_keys:
                if e_tool and (key in e_tool or e_tool in key):
                    edges.append({"src": this_sid, "rel": "Depend-on",
                                  "dst": sid_of(earlier)})
                    break
        seen.append(s)

    for i, s in enumerate(steps):
        out = s.get('output')
        if isinstance(out, dict) and out:
            try:
                if worst_band(out) in CONGESTED_BANDS and i + 1 < len(steps):
                    edges.append({"src": sid_of(s), "rel": "Trigger",
                                  "dst": sid_of(steps[i + 1])})
            except Exception:
                pass

    state['provenance'] = {"nodes": nodes, "edges": edges}
    print(f"[build_provenance] {len(nodes)} nodes, {len(edges)} typed edges")
    return state


def attribute_necessity(state: ExpState) -> ExpState:
    com = _find_last(state['exec_log'], 'commit_decision')
    out = {"worst_band": {}, "is_congestion": {}, "escalation": {}, "root": {}}
    if not com:
        state['necessity'] = out
        print("[attribute_necessity] no commit step, nothing to attribute")
        return state
    inp = com.get('input', {}) or {}
    sev = inp.get('severity', {}) or {}
    tra = inp.get('transition', {}) or {}
    cau = inp.get('causal', {}) or {}
    full_worst = worst_band(sev)
    for iface in sev:
        reduced = {k: v for k, v in sev.items() if k != iface}
        w = worst_band(reduced)
        out["worst_band"][iface] = {"necessary": (w != full_worst),
                                    "full": full_worst, "without_it": w}
    out["is_congestion"] = {"rule_value": full_worst in CONGESTED_BANDS,
                            "from_worst_band": full_worst}
    for iface, t in tra.items():
        osc = bool(t.get("oscillating"))
        dur = float(t.get("sustained_congestion_s", 0) or 0) >= 75.0
        escalated = osc or dur
        out["escalation"][iface] = {
            "escalated": escalated,
            "oscillation_alone_caused_it": escalated and osc and not dur,
            "duration_alone_caused_it": escalated and dur and not osc,
            "either_sufficient": osc and dur}
    out["root"] = {
        "root": cau.get("root"),
        "seeded_by": cau.get("severity_seeded_nodes", []),
        "supported_by": cau.get("root_source_refs", []),
        "method": "recorded provenance (seed + 3GPP refs)",
        "note": "full-traversal counterfactual needs the graph, not claimed offline"}
    state['necessity'] = out
    nec_ifaces = [i for i, v in out["worst_band"].items() if v["necessary"]]
    print(f"[attribute_necessity] worst_band necessary interfaces: "
          f"{nec_ifaces or 'none single-handedly'}")
    return state


def flag_divergence(state: ExpState) -> ExpState:
    com = _find_last(state['exec_log'], 'commit_decision')
    div = {"agrees": True, "divergences": []}
    if not com:
        state['divergence'] = div
        return state
    model = com.get('output', {}) or {}
    inp = com.get('input', {}) or {}
    sev = inp.get('severity', {}) or {}
    cau = inp.get('causal', {}) or {}
    rule_worst = worst_band(sev)
    rule_is_cong = rule_worst in CONGESTED_BANDS
    if bool(model.get("is_congestion")) != rule_is_cong:
        div["divergences"].append({
            "field": "is_congestion",
            "model": model.get("is_congestion"), "rule": rule_is_cong,
            "note": f"model committed is_congestion={model.get('is_congestion')}, "
                    f"rules (worst_band={rule_worst}) give {rule_is_cong}"})
    model_root, rule_root = model.get("root_cause"), cau.get("root")
    if bool(model_root) != bool(rule_root):
        div["divergences"].append({
            "field": "root_cause",
            "model": model_root, "rule": rule_root,
            "note": "model named a root the causal trace did not, or omitted one it found"})
    div["agrees"] = len(div["divergences"]) == 0
    state['divergence'] = div
    print(f"[flag_divergence] agrees={div['agrees']}, "
          f"{len(div['divergences'])} divergence(s)")
    return state


def _extract_hit_fragments(output, max_hits=6):
    if not isinstance(output, list) or not output:
        return []
    fragments = []
    for h in output[:max_hits]:
        if not isinstance(h, dict):
            continue
        text = (h.get('text') or '').strip()
        if len(text) > 80:
            text = text[:80].rsplit(' ', 1)[0] + '...'
        collection = h.get('collection')
        source = h.get('source')
        clause = h.get('clause')
        label_parts = []
        if collection:
            label_parts.append(f"[{collection}]")
        if source:
            label_parts.append(str(source))
        if clause:
            label_parts.append(str(clause))
        prefix = " ".join(label_parts)
        if text and prefix:
            fragments.append(f'{prefix}: "{text}"')
        elif text:
            fragments.append(f'"{text}"')
        elif prefix:
            fragments.append(prefix)
    return fragments


def _extract_observing_agents(step):
    inp = step.get('input', {}) or {}
    if not isinstance(inp, dict):
        return []
    agents = set()
    obs = inp.get('observations')
    if isinstance(obs, list):
        for o in obs:
            if isinstance(o, dict) and o.get('agent'):
                agents.add(str(o['agent']))
    return sorted(agents)


def _describe_step(step):
    tool = step.get('tool', '?')
    step_num = step.get('step', '?')

    if tool == 'llm_unreachable':
        return {"headline": (f"[Step {step_num}] The LLM was unreachable this "
                             f"cycle; the engine abstained and did not commit "
                             f"a diagnosis."),
                "fragments": []}

    claim = (step.get('claim') or '').strip()
    prov = step.get('provenance', {}) or {}
    source = prov.get('source', '')
    db = prov.get('db', '')
    rule = prov.get('rule', '')
    refs = prov.get('refs', []) or []
    counts = prov.get('per_collection_counts')

    if claim and claim.lower() != tool.lower():
        primary = claim
    else:
        primary = f"{tool} ran"

    tail_bits = []

    observing_agents = _extract_observing_agents(step)
    if observing_agents:
        agents_str = ", ".join(observing_agents)
        plural = "s" if len(observing_agents) > 1 else ""
        tail_bits.append(f"inputs from the {agents_str} agent{plural}")

    db_meaningful = db and str(db).lower() != 'none'
    if source and db_meaningful:
        tail_bits.append(f"per {source} reading {db}")
    elif source:
        tail_bits.append(f"per {source}")
    elif db_meaningful:
        tail_bits.append(f"reading {db}")

    if rule:
        tail_bits.append(f"applying rule '{rule}'")

    if counts:
        cov_str = ", ".join(f"{k}={v}" for k, v in counts.items())
        tail_bits.append(f"coverage {cov_str}")

    if refs:
        sample = "; ".join(str(r) for r in refs[:3])
        more = f" (+{len(refs) - 3} more)" if len(refs) > 3 else ""
        tail_bits.append(f"citing {sample}{more}")

    if tail_bits:
        headline = f"[Step {step_num}] {primary}, " + ", ".join(tail_bits) + "."
    else:
        headline = f"[Step {step_num}] {primary}."

    fragments = _extract_hit_fragments(step.get('output'))
    return {"headline": headline, "fragments": fragments}


def _template_render(state):
    parts = []
    for step in state['exec_log']:
        d = _describe_step(step)
        parts.append(d["headline"])
        if d["fragments"]:
            frag_snippet = "; ".join(d["fragments"][:3])
            parts.append(f"(evidence includes: {frag_snippet})")
    return " ".join(parts) if parts else "No recorded steps to explain."


def _ollama_render(template, state):
    sys_p = ("You rewrite a factual step-by-step record of what a diagnostic "
             "agent did into clear operator prose. Use ONLY the facts given. "
             "Do not add, infer, guess, or explain anything beyond them. Keep "
             "every number, interface name, and agent name exactly. Preserve "
             "the step ordering and mention which agent produced which "
             "observations when the record says so. Output only the "
             "rewritten narrative.")
    try:
        r = httpx.post(
            f"{OLLAMA_BASE}/api/chat",
            json={"model": LLM_MODEL, "stream": False, "keep_alive": "30m",
                  "options": {"temperature": 0.0, "num_ctx": 8192,
                              "num_predict": 2048},
                  "messages": [{"role": "system", "content": sys_p},
                               {"role": "user", "content": template}]},
            timeout=90.0)
        r.raise_for_status()
        txt = r.json().get("message", {}).get("content", "").strip()
        return txt or None
    except (httpx.HTTPError, httpx.TimeoutException) as e:
        print(f"    [render] LLM unavailable, keeping template: {e}")
        return None


def render(state: ExpState) -> ExpState:
    template = _template_render(state)
    llm = _ollama_render(template, state)
    state['render'] = {"prose": llm or template,
                       "mode": "llm" if llm else "template",
                       "template": template}
    print(f"[render] mode={state['render']['mode']}")
    return state


def check_completeness(state: ExpState) -> ExpState:
    log = state['exec_log']
    conclusion = state.get('conclusion', {})

    tools_run = {s['tool'] for s in log}
    reached_commit = 'commit_decision' in tools_run
    missing_tools = (sorted(set(MANDATORY_TOOLS) - tools_run)
                     if reached_commit else [])

    provenance_gaps = []
    for s in log:
        prov = s.get('provenance', {}) or {}
        tool = s.get('tool', '?')
        if not prov.get('source'):
            provenance_gaps.append(f"{tool}: no source recorded")
        if 'refs' in prov and not prov.get('refs') \
                and s.get('phase') == 'investigate':
            provenance_gaps.append(f"{tool}: refs slot present but empty")

    committed_root = conclusion.get('root_cause')
    root_traceable = None
    if committed_root:
        chain_nodes = set()
        for s in log:
            if s.get('tool') == 'run_causal_trace':
                out = s.get('output', {}) or {}
                if out.get('root'):
                    chain_nodes.add(out['root'])
                for c in out.get('chain', []) or []:
                    if isinstance(c, dict):
                        chain_nodes.add(c.get('from'))
                        chain_nodes.add(c.get('to'))
                for n in out.get('severity_seeded_nodes', []) or []:
                    chain_nodes.add(n)
        div = state.get('divergence', {}) or {}
        divergence_recorded = not div.get('agrees', True)
        root_traceable = (committed_root in chain_nodes) or divergence_recorded

    completeness = {
        "tools_run": sorted(tools_run),
        "mandatory_tools": list(MANDATORY_TOOLS),
        "tools_all_present": (not missing_tools) if reached_commit else None,
        "missing_tools": missing_tools,
        "provenance_gaps": provenance_gaps,
        "root_cause_traceable": root_traceable,
        "reached_commit": reached_commit,
    }
    state['completeness'] = completeness

    flags = []
    if missing_tools:
        flags.append(f"MISSING TOOLS: {missing_tools}")
    if provenance_gaps:
        flags.append(f"{len(provenance_gaps)} provenance gap(s)")
    if root_traceable is False:
        flags.append(f"root_cause '{committed_root}' NOT in causal chain "
                     f"and no divergence recorded")
    print(f"[check_completeness] {'; '.join(flags) if flags else 'complete, no gaps'}")
    return state


def build_evidence_report(state: ExpState) -> ExpState:
    entries = []
    for step in state['exec_log']:
        d = _describe_step(step)
        entry = {"kind": step.get('tool', 'step'), "line": d["headline"]}
        if d["fragments"]:
            entry["sub_lines"] = d["fragments"]
        entries.append(entry)

    state['evidence_report'] = {"entries": entries, "step_count": len(entries)}

    print("[evidence_report] step-by-step trace:")
    for e in entries:
        print(f"  {e['line']}")
        for sub in e.get('sub_lines', []) or []:
            print(f"      {sub}")
    return state


def verify_prose(state: ExpState) -> ExpState:
    facts = set()
    for s in state['exec_log']:
        facts.add(str(s.get('tool', '')))
        prov = s.get('provenance', {}) or {}
        for k, v in prov.items():
            if isinstance(v, (str, int, float)):
                facts.add(str(v))
            elif isinstance(v, list):
                facts.update(str(x) for x in v)
            elif isinstance(v, dict):
                for kk, vv in v.items():
                    facts.add(str(kk)); facts.add(str(vv))
        out = s.get('output')
        if isinstance(out, dict):
            for v in out.values():
                if isinstance(v, dict):
                    for vv in v.values():
                        facts.add(str(vv))
                else:
                    facts.add(str(v))
        elif isinstance(out, list):
            for item in out[:8]:
                if isinstance(item, dict):
                    for vv in item.values():
                        facts.add(str(vv))
        claim = s.get('claim')
        if claim:
            facts.update(claim.split())
        for a in _extract_observing_agents(s):
            facts.add(a)
    facts = {f.lower() for f in facts if f and len(str(f)) > 1}
    prose = state['render']['prose']
    sents = [x.strip() for x in re.split(r'(?<=[.!?])\s+', prose) if x.strip()]
    unbacked = [s for s in sents
                if not any(f in s.lower() for f in facts)]
    state['prose_check'] = {"sentences": len(sents), "unbacked": unbacked,
                            "all_backed": len(unbacked) == 0}
    print(f"[verify_prose] {len(sents)} sentence(s), "
          f"{len(unbacked)} unbacked")
    return state


def emit_packet(state: ExpState) -> ExpState:
    packet = {
        "cycle_id": state.get('cycle_id', 0.0),
        "generated_at": time.time(),
        "artifact": state['render']['prose'],
        "render_mode": state['render']['mode'],
        "linked_evidence": {
            "exec_log": state['exec_log'],
            "provenance_graph": state.get('provenance', {"nodes": [], "edges": []})},
        "verification_signals": {
            "necessity": state.get('necessity', {}),
            "divergence": state.get('divergence', {"agrees": True, "divergences": []}),
            "completeness": state.get('completeness', {}),
            "evidence_report": state.get('evidence_report', {"entries": [], "step_count": 0}),
            "prose_check": state.get('prose_check', {})},
        "decision": state.get('conclusion', {})}
    state['packet'] = packet
    try:
        _AGENTIC_DATA.mkdir(parents=True, exist_ok=True)
        with open(PACKET_LOG, "a") as f:
            f.write(json.dumps(packet, default=str) + "\n")
    except (IOError, OSError) as e:
        print(f"[emit_packet] packet write failed: {e}")
    d = packet["verification_signals"]["divergence"]
    print(f"[emit_packet] packet ready, render={packet['render_mode']}, "
          f"agrees={d['agrees']}")
    print(f"[explanation] {packet['artifact']}")
    return state


def build_explanation_agent():
    g = StateGraph(ExpState)
    g.add_node("load_record", load_record)
    g.add_node("baseline_note", baseline_note)
    g.add_node("build_provenance", build_provenance)
    g.add_node("attribute_necessity", attribute_necessity)
    g.add_node("flag_divergence", flag_divergence)
    g.add_node("check_completeness", check_completeness)
    g.add_node("build_evidence_report", build_evidence_report)
    g.add_node("render", render)
    g.add_node("verify_prose", verify_prose)
    g.add_node("emit_packet", emit_packet)
    g.set_entry_point("load_record")
    g.add_conditional_edges("load_record", route_after_load,
                            {"explain": "build_provenance",
                             "trivial": "baseline_note"})
    g.add_edge("baseline_note", "emit_packet")
    g.add_edge("build_provenance", "attribute_necessity")
    g.add_edge("attribute_necessity", "flag_divergence")
    g.add_edge("flag_divergence", "check_completeness")
    g.add_edge("check_completeness", "build_evidence_report")
    g.add_edge("build_evidence_report", "render")
    g.add_edge("render", "verify_prose")
    g.add_edge("verify_prose", "emit_packet")
    g.add_edge("emit_packet", END)
    return g.compile()


def fresh_exp_state(exec_log=None, conclusion=None, cycle_id=0.0):
    return ExpState(
        exec_log=exec_log or [], conclusion=conclusion or {}, cycle_id=cycle_id,
        provenance={}, necessity={}, divergence={}, render={},
        prose_check={}, packet={}, completeness={}, evidence_report={})


def explain_cycle(exec_log, conclusion, cycle_id=0.0):
    agent = build_explanation_agent()
    result = agent.invoke(
        fresh_exp_state(exec_log, conclusion, cycle_id),
        config={"recursion_limit": 30})
    return result.get("packet", {})



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

PORT     = int(os.environ.get("EXPLANATION_A2A_PORT", "9102"))
HOST     = os.environ.get("EXPLANATION_A2A_HOST", "0.0.0.0")
ADV_HOST = os.environ.get("A2A_ADVERTISE_HOST", "localhost")
CARD_URL = f"http://{ADV_HOST}:{PORT}/"


def build_card() -> AgentCard:
    skill = AgentSkill(
        id="explain",
        name="Explain RCA Decision",
        description=("Given the RCA engine's execution trace and conclusion, "
                     "produces an operator-facing explanation packet with "
                     "LLM tracing, provenance graph, completeness audit, and "
                     "verification signals."),
        tags=["5g", "congestion", "explainability", "verification"],
        input_modes=["application/json", "text"],
        output_modes=["application/json", "text"])
    return AgentCard(
        name="Explanation Agent",
        description=("Trajectory-level, provenance-grounded explanation of RCA "
                     "decisions, with completeness and verification signals."),
        version="1.0.0",
        capabilities=AgentCapabilities(streaming=True),
        default_input_modes=["application/json", "text"],
        default_output_modes=["application/json", "text"],
        skills=[skill],
        supported_interfaces=[AgentInterface(
            url=CARD_URL, protocol_binding=TransportProtocol.JSONRPC)])


class ExplanationExecutor(AgentExecutor):
    async def execute(self, context, event_queue) -> None:
        raw = context.get_user_input() or ""
        try:
            payload = json.loads(raw)
        except (json.JSONDecodeError, AttributeError):
            payload = {}
        exec_log   = payload.get("exec_log", []) or []
        conclusion = payload.get("conclusion", {}) or {}
        cycle_id   = float(payload.get("cycle_id", 0.0) or 0.0)
        task = context.current_task
        if task is None:
            task = new_task_from_user_message(context.message)
            await event_queue.enqueue_event(task)
        updater = TaskUpdater(event_queue, task.id, task.context_id)
        await updater.start_work()
        try:
            packet = await asyncio.to_thread(
                explain_cycle, exec_log, conclusion, cycle_id)
        except Exception as e:
            packet = {"artifact": f"explanation error: {e}", "cycle_id": cycle_id}
        await updater.add_artifact(
            [new_text_part(json.dumps(packet, default=str),
                           media_type="application/json")],
            name="explanation_packet")
        await updater.complete()

    async def cancel(self, context, event_queue) -> None:
        raise NotImplementedError("Explanation tasks are short and not cancellable")


def build_app() -> Starlette:
    card = build_card()
    handler = DefaultRequestHandler(
        agent_executor=ExplanationExecutor(),
        task_store=InMemoryTaskStore(),
        agent_card=card)
    routes = create_jsonrpc_routes(handler, "/") + create_agent_card_routes(card)
    return Starlette(routes=routes)


CARD = build_card()
app = build_app()

if __name__ == "__main__":
    import uvicorn
    print(f"[Explanation A2A] serving on {HOST}:{PORT} "
          f"(Agent Card advertises {CARD_URL})")
    uvicorn.run(app, host=HOST, port=PORT)
