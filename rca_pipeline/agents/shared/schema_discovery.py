# Author: Heven Tafese
import glob
import os
import json
import time

MNF_DATA_DIR = "/home/heven/data"
# This is in seconds if the file hasn't been modified in this time, it's stale
STALE_THRESHOLD = 10  


def discover_active_mnfs():
    """
    Finds all currently active MnF files.
    If none are active, returns the most recently modified file
    marked as is_live=False (replay mode).
    """
    pattern = os.path.join(MNF_DATA_DIR, "mnf_*.jsonl")
    files = glob.glob(pattern)

    if not files:
        return []

    now = time.time()
    active = []

    for f in files:
        mtime = os.path.getmtime(f)
        age = now - mtime

        try:
            with open(f, 'r') as fh:
                lines = [l.strip() for l in fh if l.strip()]

            if not lines:
                continue

            latest = json.loads(lines[-1])

            active.append({
                "mnf_path": f,
                "latest_reading": latest,
                "is_live": age <= STALE_THRESHOLD,
                "total_lines": len(lines),
                "mtime": mtime
            })
        except Exception:
            continue

    # If nothing is live, return the most recently modified file in replay mode
    live = [a for a in active if a["is_live"]]

    if live:
        return live

    if active:
        # Sort by mtime, return most recent as replay
        active.sort(key=lambda x: x["mtime"], reverse=True)
        return [active[0]]

    return []


def get_threshold_context(latest_reading):
    """
    Returns what is needed to determine thresholds.
    Extracts physical interface name from the MnF source field.
    """
    source = latest_reading.get("source", "")
    layer = latest_reading.get("layer", "")
    plane = latest_reading.get("plane", "")

    if "proc_net_dev:" in source:
        iface = source.split(":")[1]
    else:
        iface = None

    return {
        "interface": latest_reading.get("interface"),
        "layer": layer,
        "plane": plane,
        "physical_iface": iface,
        "source": source,
        "is_control_plane": plane == "control"
    }
