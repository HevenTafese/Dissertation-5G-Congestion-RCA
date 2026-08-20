#!/bin/bash
# N3 GTP-U Traffic Generator
# Author: Heven Tafese
IFACE=enp0s8
sudo sysctl -w net.core.wmem_default=16777216 net.core.wmem_max=16777216 net.core.rmem_default=16777216 net.core.rmem_max=16777216 >/dev/null
sudo ip link set dev "$IFACE" txqueuelen 100000
sudo ethtool -K "$IFACE" gro off gso off tso off sg off 2>/dev/null || true
python3 /home/heven/5g-traffic-generator/shape_burst_trace.py
exec sudo /home/heven/5g-traffic-generator/build/gtpu_traffic_generator \
  /home/heven/5g-traffic-generator/config/profiles_congestion.json \
  /home/heven/5g-traffic-generator/config/teid_map_baseline_1.json \
  enp0s8 192.168.56.20 192.168.56.10
