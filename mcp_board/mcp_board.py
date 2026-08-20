# Author: Heven Tafese

import os, json, time, threading
from pathlib import Path
from collections import deque
from fastmcp import FastMCP

HISTORY_PATH = os.environ.get("MCP_HISTORY", str(Path(__file__).resolve().parents[1] / "data" / "mcp_history.jsonl"))
RECENT_MAX   = 20
FRESH_SECS   = 30.0

mcp = FastMCP("congestion-rca-board")
_lock    = threading.Lock()
_current = {}
_recent  = {}
_agents  = {}
_seen    = {}

def _key(a, l, i): return f"{a}:{l}:{i}"

@mcp.tool
def publish_observation(agent: str, layer: str, interface: str, source: str, payload: dict) -> dict:
    """Store one observation by agent:layer:interface. Payload is opaque and never interpreted."""
    key = _key(agent, layer, interface)
    rec = {"agent": agent, "layer": layer, "interface": interface, "source": source,
           "payload": payload, "_received_at": time.time()}
    with _lock:
        _current[key] = rec
        _recent.setdefault(key, deque(maxlen=RECENT_MAX)).append(rec)
        _seen[key] = rec["_received_at"]
        try:
            with open(HISTORY_PATH, "a") as f:
                f.write(json.dumps({"_key": key, **rec}) + "\n")
        except Exception:
            pass
    return {"status": "ok", "key": key}

@mcp.tool
def get_current_observations() -> dict:
    """All current observations, one per key. RAG/RCA reads this to see the whole network state."""
    with _lock:
        return {"count": len(_current), "observations": dict(_current)}

@mcp.tool
def get_observation(key: str) -> dict:
    """Current observation for a single agent:layer:interface key."""
    with _lock:
        return _current.get(key, {})

@mcp.tool
def get_agent_history(key: str, n: int = RECENT_MAX) -> dict:
    """Last n observations for one key, for temporal reasoning."""
    with _lock:
        return {"key": key, "history": list(_recent.get(key, []))[-n:]}

def _context_summary() -> dict:
    now = time.time()
    with _lock:
        keys = list(_current.keys())
        fresh = [k for k in keys if now - _seen.get(k, 0) <= FRESH_SECS]
        return {"network_state": "reporting" if keys else "empty",
                "keys": keys, "count": len(keys),
                "fresh": fresh, "stale": [k for k in keys if k not in fresh],
                "agents": list(_agents.keys()), "generated_at": now}

@mcp.tool
def get_context() -> dict:
    """Liveness and coverage summary."""
    return _context_summary()

@mcp.tool
def register_agent(agent: str, description: str = "") -> dict:
    """Self-registration """
    with _lock:
        _agents[agent] = {"description": description, "registered_at": time.time()}
    return {"status": "registered", "agent": agent}

@mcp.tool
def list_agents() -> dict:
    with _lock:
        return {"agents": dict(_agents)}

@mcp.resource("obs://observations/{agent}/{layer}/{interface}")
def observation_resource(agent: str, layer: str, interface: str) -> dict:
    with _lock:
        return _current.get(_key(agent, layer, interface), {})

@mcp.resource("obs://context")
def context_resource() -> dict:
    return _context_summary()

if __name__ == "__main__":
    host = os.environ.get("MCP_HOST", "0.0.0.0"); port = int(os.environ.get("MCP_PORT", "9000"))
    mcp.run(transport="http", host=host, port=port)
