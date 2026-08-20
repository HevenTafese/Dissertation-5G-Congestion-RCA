#!/bin/bash
# N6 Dynamic Single UE Driver
# Author: Heven Tafese
DST=192.0.2.1; PORT=5201; LEN=1200
BASELINE_RATE="${1:-20M}"
BASELINE_SEC="${2:-70}"
# re-detects the tunnel IP each call so -B never goes stale after a UE restart
detect() { ip -4 -o addr show uesimtun0 2>/dev/null | awk '{print $4}' | cut -d/ -f1; }
SRC=$(detect); [ -z "$SRC" ] && { echo "no uesimtun0 tunnel, is the UE up?"; exit 1; }
echo "tunnel IP: $SRC   baseline: $BASELINE_RATE for ${BASELINE_SEC}s"
echo "[baseline] UDP $BASELINE_RATE (target ~0.85)"
iperf3 -c "$DST" -B "$SRC" -p "$PORT" -u -b "$BASELINE_RATE" -l "$LEN" -t "$BASELINE_SEC"
echo "[congestion] TCP, looping forever -> latches ~76s and holds. Ctrl+C to stop."
while true; do
  SRC=$(detect); [ -z "$SRC" ] && { echo "tunnel gone, retrying..."; sleep 3; continue; }
  iperf3 -c "$DST" -B "$SRC" -p "$PORT" -t 300
done
