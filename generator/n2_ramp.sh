#!/bin/bash
# N2 Three Phase Signalling Ramp
# Author: Heven Tafese

PR=/home/heven/PacketRusher/packetrusher
CFG=/home/heven/PacketRusher/config


BASELINE_UES=55

OVERLAY_UES=8

FLOOD_UES=15

BASELINE_HOLD=70

OVERLAY_GAP=8

TOTAL=180

LOOP="--td 1000 --tbrr 200 --loop --loopCount 0"
killall packetrusher 2>/dev/null; sleep 2
START=$(date +%s); el(){ echo $(( $(date +%s) - START )); }
cleanup(){ killall packetrusher 2>/dev/null; echo "[N2] stopped."; exit 0; }
trap cleanup INT TERM
echo "[N2] Phase 1 baseline: $BASELINE_UES UEs (~85 msg/s onset), runs entire window"
$PR --config $CFG/n2_baseline.yml multi-ue -n $BASELINE_UES $LOOP &
sleep $BASELINE_HOLD

echo "[$(el)s] Phase 2 ramp: overlays of $OVERLAY_UES UEs every ${OVERLAY_GAP}s"
$PR --config $CFG/n2_overlay1.yml multi-ue -n $OVERLAY_UES $LOOP & sleep $OVERLAY_GAP
$PR --config $CFG/n2_overlay2.yml multi-ue -n $OVERLAY_UES $LOOP & sleep $OVERLAY_GAP
$PR --config $CFG/n2_overlay3.yml multi-ue -n $OVERLAY_UES $LOOP & sleep $OVERLAY_GAP
echo "[$(el)s] Phase 3 overload: +$FLOOD_UES UEs (same cycling) -> sustained overload"
$PR --config $CFG/n2_flood.yml multi-ue -n $FLOOD_UES $LOOP &
echo "[$(el)s] holding to ${TOTAL}s, nothing killed"
while [ $(el) -lt $TOTAL ]; do sleep 2; done
echo "[$(el)s] window complete. Traffic still running; Ctrl+C to stop."
wait
