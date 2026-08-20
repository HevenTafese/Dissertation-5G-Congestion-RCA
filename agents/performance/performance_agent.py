#!/usr/bin/env python3
"""   Performance Agent

Author: Heven Tafese

This agent is a performance detector for any 5G core management function and it is built on
the utilisation formula TF=L/a, rho=TF/R, onset >= 0.85, congestion >= 1.0.

The agent reasons over the management functions telemetry arriving through the LangGraph state
machine, and publishes raw telemetry alongside its own assessment
to MCP agent.

Every management function record is one JSON line with timestamp, layer, plane, interface,
source, and an optional calibrated_metrics list naming its own field and
ceiling. 
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Optional, TypedDict

from fastmcp import Client
from pydantic import BaseModel, Field, field_validator
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from langgraph.graph import StateGraph, END

MNF_DATA_DIR = os.environ.get("MNF_DATA_DIR", "/home/heven/data")
MNF_FILE_PATTERN = "mnf_*.jsonl"
POLL_FALLBACK_S = 30.0          
FILE_INACTIVE_THRESHOLD_S = 60.0  
                                   
DIRECTORY_RESCAN_INTERVAL_S = 5.0   
                                     

MAX_HISTORY = 5                
STALE_THRESHOLD_S = 10.0        
MAX_VERIFY_WAIT_S = 30.0        

HOST_CPU_THRESHOLD = 80.0
MEMORY_THRESHOLD = 85.0
LOAD_AVG_PER_CPU = 0.8
NF_PROCESS_CPU_THRESHOLD = 20.0   
NF_PROCESS_PREFIXES = ("amf_", "smf_", "upf_", "gnb_")

MCP_URL = os.environ.get("MCP_URL", "http://localhost:9000/mcp")
MCP_TIMEOUT_S = 5.0

LOG_LEVEL = logging.DEBUG

logging.basicConfig(level=LOG_LEVEL, stream=sys.stdout,
                     format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("performance_agent")


class CalibratedMetric(BaseModel):
    field: str
    ceiling: float
    rho: Optional[float] = None

    @field_validator("ceiling")
    @classmethod
    def ceiling_must_be_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("ceiling must be > 0")
        return v


class MnFRecord(BaseModel):
    timestamp: float
    layer: str
    plane: str = "unknown"
    interface: str
    source: str
    calibrated_metrics: list[CalibratedMetric] = Field(default_factory=list)

    model_config = {"extra": "allow"}

    @field_validator("plane")
    @classmethod
    def plane_known_or_explicit_unknown(cls, v: str) -> str:
        if v not in ("control", "user", "unknown"):
            raise ValueError(
                f"plane must be 'control', 'user', or 'unknown', got {v!r}")
        return v

    def all_fields(self) -> dict[str, Any]:
        return self.model_dump()


def validate_line(raw_line: str) -> Optional[MnFRecord]:
    try:
        data = json.loads(raw_line)
    except json.JSONDecodeError:
        return None
    try:
        return MnFRecord(**data)
    except Exception:
        return None


class _EventBridge(FileSystemEventHandler):
    def __init__(self, loop: asyncio.AbstractEventLoop, prefix: str, suffix: str):
        self._loop = loop
        self._file_events: dict[str, asyncio.Event] = {}
        self._new_file_queue: "asyncio.Queue[str]" = asyncio.Queue()
        self._prefix = prefix
        self._suffix = suffix

    def event_for(self, path: str) -> asyncio.Event:
        if path not in self._file_events:
            self._file_events[path] = asyncio.Event()
        return self._file_events[path]

    def on_modified(self, event):
        if event.is_directory:
            return
        ev = self._file_events.get(event.src_path)
        if ev is not None:
            self._loop.call_soon_threadsafe(ev.set)

    def on_created(self, event):
        if event.is_directory:
            return
        name = Path(event.src_path).name
        if name.startswith(self._prefix) and name.endswith(self._suffix):
            self._loop.call_soon_threadsafe(self._new_file_queue.put_nowait, event.src_path)

    async def next_new_file(self) -> str:
        return await self._new_file_queue.get()


class MnfFileReader:
    def __init__(self, path: str, bridge: _EventBridge):
        self.path = path
        self._bridge = bridge
        self._offset = 0
        self._lines_seen = 0
        self._valid_seen = 0
        self._interface_buffer: dict[str, list[MnFRecord]] = {}

    def seems_schema_incompatible(self, min_lines: int = 20) -> bool:
        """True once enough lines have been read that a real schema match
        should have appeared by now, rather than the file just being new."""
        return self._lines_seen >= min_lines and self._valid_seen == 0

    def read_new_lines(self) -> list[MnFRecord]:
        try:
            size = Path(self.path).stat().st_size
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

        records: list[MnFRecord] = []
        for raw in complete_bytes.split(b"\n"):
            if not raw.strip():
                continue
            self._lines_seen += 1
            rec = validate_line(raw.decode("utf-8", errors="replace"))
            if rec is None:
                logger.warning("Malformed/invalid record in %s, skipped: %.200r", self.path, raw)
                continue
            self._valid_seen += 1
            records.append(rec)
        return records

    async def wait_for_new_lines(self, timeout: Optional[float] = None) -> list[MnFRecord]:
        ev = self._bridge.event_for(str(Path(self.path).resolve()))
        ev.clear()
        records = self.read_new_lines()
        if records:
            return records
        try:
            await asyncio.wait_for(ev.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return []
        ev.clear()
        return self.read_new_lines()

    def _drain_or_read(self, interface: str) -> list[MnFRecord]:
        """Serves this interface's own buffered leftovers first, otherwise
        reads fresh and stashes other interfaces' records for their turn."""
        buffered = self._interface_buffer.pop(interface, None)
        if buffered:
            return buffered
        fresh = self.read_new_lines()
        if not fresh:
            return []
        by_iface: dict[str, list[MnFRecord]] = {}
        for rec in fresh:
            by_iface.setdefault(rec.interface, []).append(rec)
        mine = by_iface.pop(interface, [])
        for other_iface, other_recs in by_iface.items():
            self._interface_buffer.setdefault(other_iface, []).extend(other_recs)
        return mine

    def drain_all_buffered(self) -> dict[str, list[MnFRecord]]:
        """Called once per cycle in run_mnf_worker so nothing stashed here
        is left unprocessed."""
        drained = self._interface_buffer
        self._interface_buffer = {}
        return drained

    async def wait_for_new_lines_for(self, interface: str,
                                     timeout: Optional[float] = None) -> list[MnFRecord]:
        """Same blocking wait as wait_for_new_lines, scoped to one
        interface so another interface's line arriving during the wait is
        never misattributed as this one's confirmation sample."""
        mine = self._drain_or_read(interface)
        if mine:
            return mine
        ev = self._bridge.event_for(str(Path(self.path).resolve()))
        ev.clear()
        try:
            await asyncio.wait_for(ev.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return []
        ev.clear()
        return self._drain_or_read(interface)


class MnfDirectoryWatcher:
    def __init__(self, data_dir: str, file_pattern: str = MNF_FILE_PATTERN):
        self.data_dir = data_dir
        prefix, _, suffix = file_pattern.partition("*")
        self._bridge = _EventBridge(asyncio.get_event_loop(), prefix, suffix)
        self._observer = Observer()
        self._observer.schedule(self._bridge, data_dir, recursive=False)
        self._readers: dict[str, MnfFileReader] = {}

    def start(self):
        self._observer.start()

    def stop(self):
        self._observer.stop()
        self._observer.join(timeout=5)

    def existing_files(self) -> list[str]:
        return [str(p) for p in Path(self.data_dir).glob(
            self._bridge._prefix + "*" + self._bridge._suffix)]

    def reader_for(self, path: str) -> MnfFileReader:
        resolved = str(Path(path).resolve())
        if resolved not in self._readers:
            self._readers[resolved] = MnfFileReader(path, self._bridge)
        return self._readers[resolved]

    async def next_new_file(self) -> str:
        return await self._bridge.next_new_file()


class PerformanceState(TypedDict, total=False):
    reader: MnfFileReader
    interface: str
    mcp_client: Optional[Any]             
    pending_new_records: Optional[list]   
                                           
                                           
    current_record: Optional[MnFRecord]
    metric_history: list[MnFRecord]
    is_stale: bool
    capacity: dict[str, dict]
    capacity_uncalibrated: bool
    system: dict[str, dict]
    corroboration: str
    verify_pending_fields: list[str]
    awaiting_verify: bool
    ever_verified: bool
    observation: dict


async def collect(state: PerformanceState) -> PerformanceState:
    reader = state["reader"]
    if state.get("awaiting_verify"):
   
        new_records = await reader.wait_for_new_lines_for(state["interface"], timeout=MAX_VERIFY_WAIT_S)
    else:
        pending = state.pop("pending_new_records", None)
        new_records = pending if pending is not None else reader.read_new_lines()

    if new_records:
        state["current_record"] = new_records[-1]
        hist = list(state.get("metric_history", []))
        hist.extend(new_records)
        state["metric_history"] = hist[-MAX_HISTORY:]
    return state


def check_freshness(state: PerformanceState) -> PerformanceState:
    record = state.get("current_record")
    if record is None:
        state["is_stale"] = True
        return state
    state["is_stale"] = (time.time() - record.timestamp) > STALE_THRESHOLD_S
    return state


def route_after_freshness(state: PerformanceState) -> str:
    return "mark_stale" if state["is_stale"] else "evaluate_capacity"


def mark_stale(state: PerformanceState) -> PerformanceState:
    state["capacity"] = {}
    state["system"] = {}
    state["corroboration"] = "stale"
    return state


def evaluate_capacity(state: PerformanceState) -> PerformanceState:
    record = state["current_record"]
    all_fields = record.all_fields()
    capacity: dict[str, dict] = {}

    rho_fields = {}
    for key in ("rho", "rho_pps", "rho_tput"):
        val = all_fields.get(key)
        if val is not None and isinstance(val, (int, float)):
            rho_fields[key] = val

    if rho_fields:
        primary_rho = rho_fields.get("rho")
        if primary_rho is None:
            primary_rho = max(rho_fields.values())

        capacity["rho"] = {
            "value": round(primary_rho, 4),
            "ceiling": 1.0,
            "rho": round(primary_rho, 4),
            "crossed": primary_rho >= 1.0,
            "onset": primary_rho >= 0.85,
            "headroom_pct": round((1 - primary_rho) * 100, 1),
        }
        for key, val in rho_fields.items():
            if key != "rho":
                capacity[key] = {
                    "value": round(val, 4), "ceiling": 1.0,
                    "rho": round(val, 4),
                    "crossed": val >= 1.0, "onset": val >= 0.85,
                    "headroom_pct": round((1 - val) * 100, 1),
                }

    for entry in record.calibrated_metrics:
        value = all_fields.get(entry.field)
        if value is None or not isinstance(value, (int, float)):
            logger.warning("MnF %s declared calibrated_metrics for %r but the "
                            "record has no such numeric field", record.source, entry.field)
            continue
        computed_rho = value / entry.ceiling
        capacity[entry.field] = {
            "value": value, "ceiling": entry.ceiling,
            "rho": round(computed_rho, 4),
            "crossed": computed_rho >= 1.0, "onset": computed_rho >= 0.85,
            "headroom_pct": round((1 - computed_rho) * 100, 1),
        }

    band = all_fields.get("band")
    if isinstance(band, str) and band in ("onset", "CONGESTION", "FAILING"):
        capacity["band"] = {
            "value": band, "ceiling": None,
            "rho": rho_fields.get("rho"),
            "crossed": band in ("CONGESTION", "FAILING"),
            "onset": band in ("onset", "CONGESTION", "FAILING"),
            "headroom_pct": None,
        }

    if capacity:
        state["capacity"] = capacity
        state["capacity_uncalibrated"] = False
    else:
        state["capacity"] = {}
        state["capacity_uncalibrated"] = True
        if record.plane == "unknown":
            logger.warning("MnF %s: plane 'unknown', no rho, and no "
                            "calibrated_metrics, cannot evaluate capacity",
                            record.source)
    return state


def verify_decide(state: PerformanceState) -> PerformanceState:
    capacity = state.get("capacity", {})
    crossed_now = {k for k, v in capacity.items() if v.get("crossed")}
    pending = set(state.get("verify_pending_fields", []))

    if state.get("awaiting_verify"):
        still_crossed = pending & crossed_now
        if still_crossed:
            state["ever_verified"] = True
        state["verify_pending_fields"] = []
        state["awaiting_verify"] = False
        return state

    if crossed_now:
        state["verify_pending_fields"] = list(crossed_now)
        state["awaiting_verify"] = True
        return state

    state["verify_pending_fields"] = []
    state["awaiting_verify"] = False
    return state


def route_after_verify(state: PerformanceState) -> str:
    return "collect" if state.get("awaiting_verify") else "evaluate_system"


def evaluate_system(state: PerformanceState) -> PerformanceState:
    record = state["current_record"]
    fields = record.all_fields()
    already = set(state.get("capacity", {}).keys())
    system: dict[str, dict] = {}

    for key, value in fields.items():
        if key in already or not isinstance(value, (int, float)):
            continue
        if key == "host_cpu_percent":
            thr = HOST_CPU_THRESHOLD
        elif key.endswith("_cpu_percent") and any(key.startswith(p) for p in NF_PROCESS_PREFIXES):
            thr = NF_PROCESS_CPU_THRESHOLD
        elif key == "memory_percent":
            thr = MEMORY_THRESHOLD
        elif key == "load_avg_1m":
            thr = LOAD_AVG_PER_CPU * (fields.get("cpu_count", 1) or 1)
        elif key.endswith("_congestion") or key in ("congestion_detected", "system_failing"):
            continue
        else:
            continue
        system[key] = {
            "value": value, "threshold": thr, "crossed": value > thr,
            "headroom_pct": round((1 - value / thr) * 100, 1) if thr else None,
        }
    state["system"] = system
    return state


def corroborate(state: PerformanceState) -> PerformanceState:
    cap, sys_ = state.get("capacity", {}), state.get("system", {})
    cap_crossed = any(v.get("crossed") for v in cap.values())
    sys_crossed = any(v.get("crossed") for v in sys_.values())
    if state.get("capacity_uncalibrated"):
        state["corroboration"] = "uncalibrated"
    elif cap_crossed and sys_crossed:
        state["corroboration"] = "capacity_and_system_stressed"
    elif cap_crossed:
        state["corroboration"] = "capacity_stressed_system_healthy"
    elif sys_crossed:
        state["corroboration"] = "system_stressed_capacity_normal"
    else:
        state["corroboration"] = "normal"
    return state


def assess_trend(state: PerformanceState) -> PerformanceState:
    history = state.get("metric_history", [])
    if len(history) < 2:
        for section in ("capacity", "system"):
            for k in state.get(section, {}):
                state[section][k]["trend"] = "stable"
        return state
    oldest, newest = history[0].all_fields(), history[-1].all_fields()
    for section in ("capacity", "system"):
        for k, v in state.get(section, {}).items():
            old_val, new_val = oldest.get(k), newest.get(k)
            if old_val in (None, 0) or new_val is None or not isinstance(old_val, (int, float)) or not isinstance(new_val, (int, float)):
                v["trend"] = "stable"
                continue
            pct = ((new_val - old_val) / old_val) * 100
            v["trend"] = "degrading" if pct > 10 else ("improving" if pct < -10 else "stable")
    return state


def format_observation(state: PerformanceState) -> PerformanceState:
    record = state["current_record"]
    cap, sys_ = state.get("capacity", {}), state.get("system", {})
    crossed = [k for k, v in {**cap, **sys_}.items() if v.get("crossed")]
    all_m = {**cap, **sys_}
    min_headroom = min((v["headroom_pct"] for v in all_m.values()
                        if v.get("headroom_pct") is not None), default=100.0)

    cap_crossed = any(v.get("crossed") for v in cap.values())
    any_onset = any(v.get("onset") for v in cap.values() if isinstance(v.get("onset"), bool))
    sys_failing = any(v.get("value") is True for k, v in sys_.items() if k == "system_failing")

    if state.get("capacity_uncalibrated"):
        severity = "uncalibrated"
    elif cap_crossed and (min_headroom <= -20 or sys_failing):
        severity = "critical"
    elif cap_crossed:
        severity = "warning"
    elif any_onset:
        severity = "onset"
    else:
        severity = "normal"

    confidence = ("high" if (len(state.get("metric_history", [])) >= 3 and state.get("ever_verified"))
                  else "medium" if len(state.get("metric_history", [])) >= 2 else "low")

    state["observation"] = {
        "agent": "performance",
        "layer": record.layer, "plane": record.plane,
        "interface": record.interface, "source": record.source,
        "timestamp": record.timestamp,
        "payload": {
            "telemetry": record.all_fields(),
            "assessment": {
                "capacity": cap, "system": sys_,
                "corroboration": state.get("corroboration", "normal"),
                "severity": severity, "crossed_metrics": crossed,
            },
            "agent_confidence": confidence,
        },
    }
    rho_summary = ", ".join(f"{k}=rho:{v['rho']}" for k, v in cap.items()) or "no calibrated metrics"
    logger.debug("Full observation: %s", json.dumps(state["observation"], indent=2, default=str))
    logger.info("[%s] severity=%s corroboration=%s confidence=%s | %s",
                record.source, severity, state.get("corroboration", "normal"), confidence, rho_summary)
    return state


async def publish(state: PerformanceState) -> PerformanceState:
    obs = state.get("observation", {})
    client = state.get("mcp_client")
    if not obs or client is None:
        return state
    payload = dict(obs.get("payload", {}))
    payload["plane"] = obs.get("plane")
    payload["timestamp"] = obs.get("timestamp")
    try:
        await client.call_tool("publish_observation", {
            "agent": obs.get("agent", "performance"),
            "layer": obs.get("layer", "unknown"),
            "interface": obs.get("interface", "unknown"),
            "source": obs.get("source", "unknown"),
            "payload": payload,
        })
        logger.info("MCP publish ok: %s:%s:%s",
                    obs.get("agent"), obs.get("layer"), obs.get("interface"))
    except Exception as e:
        logger.warning("MCP publish failed: %s", e)
    return state


def build_performance_agent():
    g = StateGraph(PerformanceState)
    g.add_node("collect", collect)
    g.add_node("check_freshness", check_freshness)
    g.add_node("mark_stale", mark_stale)
    g.add_node("evaluate_capacity", evaluate_capacity)
    g.add_node("verify_decide", verify_decide)
    g.add_node("evaluate_system", evaluate_system)
    g.add_node("corroborate", corroborate)
    g.add_node("assess_trend", assess_trend)
    g.add_node("format", format_observation)
    g.add_node("publish", publish)

    g.set_entry_point("collect")
    g.add_edge("collect", "check_freshness")
    g.add_conditional_edges("check_freshness", route_after_freshness,
                             {"mark_stale": "mark_stale", "evaluate_capacity": "evaluate_capacity"})
    g.add_edge("mark_stale", "format")
    g.add_edge("evaluate_capacity", "verify_decide")
    g.add_conditional_edges("verify_decide", route_after_verify,
                             {"collect": "collect", "evaluate_system": "evaluate_system"})
    g.add_edge("evaluate_system", "corroborate")
    g.add_edge("corroborate", "assess_trend")
    g.add_edge("assess_trend", "format")
    g.add_edge("format", "publish")
    g.add_edge("publish", END)
    return g.compile()


async def run_mnf_worker(path: str, watcher: MnfDirectoryWatcher):
    reader = watcher.reader_for(path)
    agent = build_performance_agent()
    logger.info("Worker started for %s", path)
   
    _mcp_cm = Client(MCP_URL, timeout=MCP_TIMEOUT_S)
    mcp_client = await _mcp_cm.__aenter__()

   
    interface_states: dict[str, PerformanceState] = {}

    def _state_for(interface: str) -> PerformanceState:
        if interface not in interface_states:
            interface_states[interface] = {
                "reader": reader, "interface": interface,
                "metric_history": [], "mcp_client": mcp_client,
            }
        return interface_states[interface]

    try:
        while True:
            pending = await reader.wait_for_new_lines(timeout=POLL_FALLBACK_S)
            if reader.seems_schema_incompatible():
                logger.warning(
                    "Giving up on %s, %d lines read, none matched this "
                    "agent's schema (needs timestamp/layer/plane/interface/"
                    "source). This looks like a different MnF format, not "
                    "being retried further.", path, reader._lines_seen)
                return

            by_interface: dict[str, list[MnFRecord]] = reader.drain_all_buffered()
            for rec in pending:
                by_interface.setdefault(rec.interface, []).append(rec)

            if not by_interface:
                try:
                    age = time.time() - Path(path).stat().st_mtime
                except FileNotFoundError:
                    age = 999
                if age > STALE_THRESHOLD_S:
                    logger.info("MnF stopped: %s (no new data for %.0fs)", path, age)
                    return
                continue

            for interface, records in by_interface.items():
                state = _state_for(interface)
                state["pending_new_records"] = records
                state = await agent.ainvoke(state, config={"recursion_limit": 25})
                interface_states[interface] = state
    except asyncio.CancelledError:
        logger.info("Worker stopped for %s", path)
        raise
    finally:
        await _mcp_cm.__aexit__(None, None, None)


class Orchestrator:
    def __init__(self, data_dir: str = MNF_DATA_DIR, file_pattern: str = MNF_FILE_PATTERN):
        self.watcher = MnfDirectoryWatcher(data_dir, file_pattern)
        self._tasks: dict[str, asyncio.Task] = {}
        self._skip_logged: set[str] = set()

    def _spawn(self, path: str):
        if path in self._tasks:
            if not self._tasks[path].done():
                return
            del self._tasks[path]
        try:
            age = time.time() - Path(path).stat().st_mtime
        except FileNotFoundError:
            return
        if age > FILE_INACTIVE_THRESHOLD_S:
            self._skip_logged.add(path)
            return
        self._skip_logged.discard(path)
        logger.info("Spawning worker for new MnF: %s", path)
        self._tasks[path] = asyncio.create_task(run_mnf_worker(path, self.watcher))

    async def run(self):
        self.watcher.start()
        try:
            for path in self.watcher.existing_files():
                self._spawn(path)
            if self._skip_logged:
                logger.info("Skipped %d stale file(s) (not modified in the last %.0fs). "
                            "They'll be picked up automatically if they become active.",
                            len(self._skip_logged), FILE_INACTIVE_THRESHOLD_S)
            while True:
                try:
                    new_path = await asyncio.wait_for(
                        self.watcher.next_new_file(), timeout=DIRECTORY_RESCAN_INTERVAL_S)
                    self._spawn(new_path)
                except asyncio.TimeoutError:
                    for path in self.watcher.existing_files():
                        self._spawn(path)
        finally:
            for t in self._tasks.values():
                t.cancel()
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
            self.watcher.stop()

    def active_worker_count(self) -> int:
        return sum(1 for t in self._tasks.values() if not t.done())


async def main():
    logger.info("Starting performance agent. Watching %s for %s", MNF_DATA_DIR, MNF_FILE_PATTERN)
    orchestrator = Orchestrator()
    try:
        await orchestrator.run()
    except KeyboardInterrupt:
        logger.info("Stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
