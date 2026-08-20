#!/usr/bin/env bash
# gNB Downlink Capture
# Author: Heven Tafese
# Discovers live UE IPs on VM2, drives baseline->onset->congestion->FAILING.
set -e
HERE="$(cd "$(dirname "$0")" && pwd)"
VM2=heven@192.168.56.20
PHASES="${1:-670:40,760:40,1000:150}"
echo "[run] pulling live UE IPs from VM2..."
IPS=$(ssh $VM2 "ip -brief addr show | grep uesimtun | grep -oE '10\.60\.[0-9]+\.[0-9]+' | paste -sd,")
if [ -z "$IPS" ]; then echo "[run] no UE tunnels on VM2, are UEs registered?"; exit 1; fi
N=$(echo "$IPS" | tr ',' '\n' | wc -l)
echo "[run] found $N live UEs; phases: $PHASES"
python3 "$HERE/dl_gnb_flood.py" --ips "$IPS" --phases "$PHASES"
