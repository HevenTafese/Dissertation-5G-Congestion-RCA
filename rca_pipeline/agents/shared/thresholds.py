# Author: Heven Tafese
import os
import psutil


def get_interface_speed(physical_iface):
    """
    Read interface speed from /sys/class/net/<iface>/speed.
    Returns speed in bits per second, or None if unavailable.
    """
    if not physical_iface:
        return None

    speed_path = f"/sys/class/net/{physical_iface}/speed"
    try:
        with open(speed_path, 'r') as f:
            speed_mbps = int(f.read().strip())
        return speed_mbps * 1_000_000
    except (FileNotFoundError, ValueError):
        return None


def get_max_pps(interface_speed_bps):
    """
    Theoretical max packets per second.
    84 bytes minimum on wire (64 byte frame + 20 byte overhead).
    """
    if interface_speed_bps is None:
        return None
    return interface_speed_bps / (84 * 8)


def build_thresholds(physical_iface, is_control_plane=False, cpu_count=None):
    """
    Build all thresholds for the Performance Agent.
    User plane: derived from interface speed.
    Control plane: empirically derived from testbed saturation points.
    """
    if cpu_count is None:
        cpu_count = psutil.cpu_count()

    thresholds = {
        "cpu_percent": 80.0,
        "memory_percent": 85.0,
        "load_avg_1m": cpu_count * 0.8,
    }

    if is_control_plane:
        # Control plane thresholds empirically derived by N2 signalling storm saturation observed at ~106 msg/sec, ~95 reg/sec
        thresholds["message_rate"] = 75.0
        thresholds["registration_rate"] = 65.0
    else:
        # User plane thresholds derived from interface speed
        speed_bps = get_interface_speed(physical_iface)
        if speed_bps:
            thresholds["throughput_bps"] = speed_bps * 0.7
            max_pps = get_max_pps(speed_bps)
            thresholds["packet_rate_pps"] = max_pps * 0.7
        else:
            # Fallback for interfaces without /sys/class/net speed
            thresholds["throughput_bps"] = 1_000_000_000 * 0.7
            thresholds["packet_rate_pps"] = (1_000_000_000 / 672) * 0.7

    return thresholds
