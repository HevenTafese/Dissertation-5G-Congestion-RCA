#!/usr/bin/env python3
"""
RCA runner as an A2A client -- WITH A DURABLE TELEMETRY QUEUE.

WHY THIS EXISTS (the problem it fixes):
    The MCP board keeps only the LATEST reading per interface in its current
    slot -- every new publish overwrites the previous one. While the LLM
    reasons for minutes on one cycle, telemetry keeps arriving every second and
    overwrites that slot, so by the time the old loop called
    get_current_observations again, everything in between was already gone.
    On a 3-minute scenario with a multi-minute LLM, that meant almost all
    telemetry was DROPPED unread, and the one cycle that did run analysed a
    stale single snapshot. The current state was never actually analysed --
    it was overwritten before the RCA looked at it.

WHAT THIS RUNNER DOES INSTEAD:
    A background DRAINER polls the board about once a second and copies every
    genuinely-new reading -- read from the board's per-key recent buffer via
    get_agent_history, NOT the overwrite-in-place current slot -- into an
    in-memory FIFO QUEUE, in arrival order, unchanged. Nothing is ever dropped.
    The LLM then consumes that queue OLDEST-FIRST, one moment at a time, at its
    own pace. While traffic runs the queue grows (the LLM is slower than
    arrival, by design on this hardware); when traffic stops the LLM keeps
    working the backlog until it is empty -- however long that takes -- then
    idles. The queue is IN-MEMORY, so restarting the runner starts empty: each
    run analyses only its own scenario's telemetry, with no stale bleed-in from
    a previous run.

    STARTUP FLOOR (this is the fix for the "queue not empty on fresh run"
    problem observed against N2 from a previous session): the drainer's
    freshness threshold for every not-yet-seen key starts at RUNNER STARTUP
    TIME, not at zero. That means records that were already sitting in the
    board's recent buffer -- carried over from whatever session last published
    to that key -- are ignored on the first poll. Only telemetry that arrives
    AFTER the runner starts is queued. This preserves the design promise
    ("each run analyses only its own scenario's telemetry") without depending
    on the board being cleared between runs.

WHAT IS UNCHANGED:
    The board is NOT modified -- it stays a pure message bus. The downstream
    hand-off is NOT modified -- once the LLM commits, the conclusion still goes
    to Mitigation and Explanation over A2A exactly as before (dispatch_both and
    everything it calls are byte-for-byte the previous file). Only the SOURCE of
    each cycle's observations changed: a popped queue moment instead of a live
    board fetch. The RCA engine change that pairs with this is in
    agentic_rca.collect(), which now accepts an injected snapshot and, when
    queue-fed, skips wall-clock staleness so deliberately-old backlog is still
    analysed.

  python run_rca_a2a.py
  MITIGATION_A2A_URL      (default http://localhost:9101)
  EXPLANATION_A2A_URL     (default http://localhost:9102)
  RCA_DRAIN_INTERVAL_S    (default 1.0)   how often the drainer polls the board
  RCA_BUCKET_WINDOW_S     (default 1.5)   readings within this window = one moment
"""
import os
import sys
import json
import time
import asyncio
from collections import deque

import httpx
from fastmcp import Client
from a2a.types import Role, SendMessageRequest
from a2a.utils import TransportProtocol
from a2a.helpers.proto_helpers import new_text_message, get_artifact_text
from a2a.client import create_client, ClientConfig

from agentic_rca import build_agentic_rca, fresh_state

MITIGATION_URL   = os.environ.get("MITIGATION_A2A_URL",  "http://localhost:9101")
EXPLANATION_URL  = os.environ.get("EXPLANATION_A2A_URL", "http://localhost:9102")
SEND_TIMEOUT_S   = float(os.environ.get("A2A_SEND_TIMEOUT_S", "150"))

# --- queue / drainer config ---
MCP_URL          = os.environ.get("MCP_URL", "http://localhost:9000/mcp")
MCP_TIMEOUT_S    = float(os.environ.get("MCP_TIMEOUT_S", "5.0"))
DRAIN_INTERVAL_S = float(os.environ.get("RCA_DRAIN_INTERVAL_S", "1.0"))
BUCKET_WINDOW_S  = float(os.environ.get("RCA_BUCKET_WINDOW_S", "1.5"))
IDLE_SLEEP_S     = float(os.environ.get("RCA_IDLE_SLEEP_S", "0.2"))


# ═══════════════════════════════════════════ A2A dispatch (verbatim) ══════
# Everything in this block is byte-for-byte the previous runner: the downstream
# hand-off is deliberately unchanged. Only the loop that FEEDS the RCA changed.

async def send_and_collect(client, payload: dict) -> dict:
    """Send a JSON payload as an A2A message and return the JSON artifact the
    remote agent produces."""
    msg = new_text_message(json.dumps(payload, default=str), role=Role.ROLE_USER)
    req = SendMessageRequest(message=msg)
    result_text = None
    async for resp in client.send_message(req):
        if resp.WhichOneof("payload") == "artifact_update":
            result_text = get_artifact_text(resp.artifact_update.artifact)
    return json.loads(result_text) if result_text else {}


async def dispatch_one(server_url: str, payload: dict) -> dict:
    """Open a real-HTTP A2A client to server_url, send the payload, return the
    reply. One client per call keeps the runner robust to a downstream server
    restarting."""
    httpx_client = httpx.AsyncClient(timeout=SEND_TIMEOUT_S)
    try:
        cfg = ClientConfig(httpx_client=httpx_client, streaming=True,
                           supported_protocol_bindings=[TransportProtocol.JSONRPC])
        client = await create_client(server_url, cfg)
        try:
            return await send_and_collect(client, payload)
        finally:
            await client.close()
    finally:
        await httpx_client.aclose()


async def dispatch_both(conclusion: dict, exec_log: list, cycle_id: float) -> tuple:
    """Send to Mitigation and Explanation concurrently. A failure of one does
    not sink the other."""
    mit_payload = {"conclusion": conclusion}
    exp_payload = {"exec_log": exec_log, "conclusion": conclusion,
                   "cycle_id": cycle_id}
    results = await asyncio.gather(
        dispatch_one(MITIGATION_URL, mit_payload),
        dispatch_one(EXPLANATION_URL, exp_payload),
        return_exceptions=True)
    return results[0], results[1]


def _summarise(mit, exp) -> str:
    if isinstance(mit, Exception):
        m = f"mitigation ERROR: {mit}"
    else:
        m = f"mitigation action={mit.get('action')} target={mit.get('target_nf')}"
    if isinstance(exp, Exception):
        e = f"explanation ERROR: {exp}"
    else:
        sig = (exp.get("verification_signals") or {}).get("divergence", {})
        e = (f"explanation render={exp.get('render_mode')} "
             f"agrees={sig.get('agrees')}")
    return m + " | " + e


# ═══════════════════════════════════════════ telemetry drainer + queue ════

async def drain_forever(queue: deque, last_seen: dict, stop: asyncio.Event):
    """Poll the board ~1/s and append every genuinely-new reading to the FIFO.

    Reads each interface's RECENT buffer (get_agent_history), not the
    overwrite-in-place current slot, so a burst that overwrote the current slot
    several times between polls is still fully captured -- the board keeps the
    last RECENT_MAX per key, and a ~1s poll never falls that far behind.
    De-duplicates per key by _received_at (strictly newer only), so the same
    reading is never queued twice.

    STARTUP FLOOR: on the very first time each key is seen, its last_seen is
    initialised to the runner's startup timestamp, NOT zero. Records with
    _received_at <= startup_ts are therefore leftover from a previous session
    (still sitting in the board's recent buffer) and are correctly NOT queued.
    Records that arrive after startup are queued normally, because their
    _received_at is strictly greater than startup_ts. Without this floor, the
    old 0.0 threshold let leftover records from previous runs flow into the
    queue as if they were fresh -- producing surprising cycles on a supposedly
    empty fresh startup (the "why did it just analyse N2 when I never ran an
    N2 test this session?" symptom).

    Board failures are caught and logged; the in-memory queue is untouched by
    them. That is what makes the runner tolerant of the board or the MnF being
    stopped mid-run: the drainer simply finds nothing new (or fails its poll and
    retries), while the consumer keeps draining the backlog already queued."""
    startup_ts = time.time()
    print(f"[drainer] up at t={startup_ts:.1f}; records already in the board's "
          f"recent buffer at this moment are ignored (fresh-run isolation).")
    while not stop.is_set():
        new = []
        try:
            async with Client(MCP_URL, timeout=MCP_TIMEOUT_S) as c:
                ctx = await c.call_tool("get_context", {})
                keys = (ctx.data or {}).get("keys", []) or []
                for key in keys:
                    # STARTUP FLOOR (the fix): any key we have not seen before
                    # is anchored to the runner's startup time. Records with
                    # _received_at <= startup_ts are therefore leftover from a
                    # previous run and correctly not queued. Once we accept a
                    # first post-startup record for a key, last_seen advances
                    # normally to that record's _received_at, and this floor
                    # becomes moot for that key -- the guard only fires on
                    # first-time encounter.
                    if key not in last_seen:
                        last_seen[key] = startup_ts
                    res = await c.call_tool("get_agent_history", {"key": key})
                    hist = (res.data or {}).get("history", []) or []
                    for rec in hist:
                        ra = rec.get("_received_at") or 0.0
                        if ra > last_seen[key]:
                            new.append((ra, key, rec))
                            last_seen[key] = ra
        except Exception as e:
            msg = str(e)
            if msg != getattr(drain_forever, '_last_err', None):
                print(f"[drainer] board poll failed (queue intact, suppressing repeats): {msg}")
                drain_forever._last_err = msg
        if new:
            if getattr(drain_forever, '_last_err', None):
                print(f"[drainer] board reconnected")
                drain_forever._last_err = None
            new.sort(key=lambda t: t[0])   # global arrival order within this tick
            queue.extend(new)
        await asyncio.sleep(DRAIN_INTERVAL_S)


def pop_next_bucket(queue: deque):
    """Pop the OLDEST moment from the FIFO and return it as {key: record}.

    A 'moment' is every queued reading within BUCKET_WINDOW_S of the oldest
    un-popped reading. Grouping co-occurring interfaces into one snapshot is
    what preserves cross-plane detection -- triage needs several interfaces
    together in a cycle, not one reading at a time. Within the moment, the
    NEWEST reading for each key wins (a moment's state per interface). Every
    reading in the window leaves the queue, so nothing is re-processed and
    nothing is dropped: the queue is the complete ordered record and this walks
    it one moment at a time, oldest first. Returns None when the queue is
    empty."""
    if not queue:
        return None
    base_ts = queue[0][0]
    snapshot = {}
    while queue and (queue[0][0] - base_ts) <= BUCKET_WINDOW_S:
        _ra, key, rec = queue.popleft()
        snapshot[key] = rec     # newer reading for a key overwrites -> moment state
    return snapshot


# ═══════════════════════════════════════════ consumer ═════════════════════

async def one_cycle(graph, cycle_no: int, snapshot: dict, queue=None) -> None:
    """Run ONE RCA cycle on a queued moment. fresh_state() per cycle keeps runs
    independent; the moment is injected as raw_observations, so collect() uses
    it (queue-fed) instead of fetching the board. Downstream dispatch is
    unchanged."""
    st = fresh_state()
    st['raw_observations'] = snapshot
    result = await asyncio.to_thread(
        graph.invoke, st, {"recursion_limit": 60})
    conclusion = result.get("conclusion", {}) or {}
    exec_log   = result.get("exec_log", []) or []
    cycle_id   = result.get("cycle_id", time.time())
    mit, exp = await dispatch_both(conclusion, exec_log, cycle_id)
    backlog = len(queue) if queue is not None else "?"
    print(f"\n====== CYCLE {cycle_no} COMPLETE (backlog: {backlog}) ======")
    print(f"  is_congestion={conclusion.get('is_congestion')} "
          f"status={conclusion.get('status')} root={conclusion.get('root_cause')}")
    print(f"  downstream: {_summarise(mit, exp)}")
    print(f"======{'=' * (len(str(cycle_no)) + 30)}======\n")


async def main():
    graph = build_agentic_rca()
    queue: deque = deque()
    last_seen: dict = {}
    stop = asyncio.Event()

    drainer = asyncio.create_task(drain_forever(queue, last_seen, stop))
    print(f"[run] queue-fed RCA up. drain every {DRAIN_INTERVAL_S}s, "
          f"moment window {BUCKET_WINDOW_S}s. "
          f"Mitigation={MITIGATION_URL} Explanation={EXPLANATION_URL}")
    print("[run] LLM consumes the FIFO oldest-first; backlog grows while "
          "traffic runs and drains to empty after it stops. Ctrl-C to stop.")

    n = 0
    try:
        while True:
            snapshot = pop_next_bucket(queue)
            if snapshot is None:
                # backlog empty -- wait for the drainer to add the next moment.
                # (After traffic stops and the queue drains, the runner simply
                # idles here until stopped -- it never exits on its own.)
                await asyncio.sleep(IDLE_SLEEP_S)
                continue
            n += 1
            try:
                await one_cycle(graph, n, snapshot, queue)
            except Exception as e:
                print(f"[cycle {n}] errored (continuing): {e}")
    except KeyboardInterrupt:
        print(f"\n[run] stopping -- {len(queue)} moment-reading(s) still queued "
              f"were not yet analysed (in-memory, discarded on stop by design).")
    finally:
        stop.set()
        drainer.cancel()
        try:
            await drainer
        except (asyncio.CancelledError, Exception):
            pass


if __name__ == "__main__":
    asyncio.run(main())
