#!/bin/bash
# Control plane end to end Congestion scenario
# Author: Heven Tafese
set -u
BASE="$HOME/PacketRusher"; DIR="$HOME/e2e_ramp_phases/npdu"
MSIN="0000000220"
N=400
TR=500
NPDU=6
HOLD=230
START=$(date +%s); el(){ echo $(( $(date +%s) - START )); }
STOPPED=0
PR_PID=""
stop_clean(){
  [ "$STOPPED" = 1 ] && return
  STOPPED=1
  echo; echo "[$(el)s] stopping: SIGINT to the exact PID for graceful UE deregistration"
  if [ -n "$PR_PID" ] && kill -0 "$PR_PID" 2>/dev/null; then
    sudo kill -INT "$PR_PID" 2>/dev/null || true
    for i in $(seq 1 20); do
      kill -0 "$PR_PID" 2>/dev/null || break
      sleep 1
    done
    if kill -0 "$PR_PID" 2>/dev/null; then
      echo "[$(el)s] still running after 20s grace, forcing stop on that PID only"
      sudo kill -9 "$PR_PID" 2>/dev/null || true
    else
      echo "[$(el)s] exited gracefully"
    fi
  fi
  stty sane 2>/dev/null || true
}
trap stop_clean INT TERM
echo "[$(el)s] clearing any stale LOCAL packetrusher from an earlier, unrelated run"
sudo pkill -INT -f 'packetrusher multi-ue' 2>/dev/null || true; sleep 3
sudo pkill -9 -f 'packetrusher multi-ue' 2>/dev/null || true; sleep 1
mkdir -p "$(dirname "$DIR")"; rm -rf "$DIR"; cp -r "$BASE" "$DIR"
sed -i "s/^\([[:space:]]*msin:[[:space:]]*\)\"[0-9]*\"/\1\"$MSIN\"/" "$DIR/config/config.yml"
echo "[$(el)s] msin set: $(grep -E '^[[:space:]]*msin:' "$DIR/config/config.yml" | tr -d ' ')"
cd "$DIR" || { echo "cd failed"; exit 1; }
echo "[$(el)s] launching: n=$N tr=${TR}ms (2/s) nPdu=$NPDU td=0, hold ${HOLD}s"
sudo ./packetrusher multi-ue -n "$N" --tr "$TR" --td 0 --numPduSessions "$NPDU" > "$DIR/npdu.log" 2>&1 &
PR_PID=$!
disown
sleep 2
echo "[$(el)s] onboarding. On VM1 the SMF should now SIT in congestion. Watch for FAILING ~75s after it crosses."
sleep "$HOLD"
stop_clean
echo; echo "================ TALLY (this run) ================"
LOG="$DIR/npdu.log"
echo "Registrations completed:  $(grep -c 'Receive Registration Accept' "$LOG" 2>/dev/null || echo 0)  (want most of $N)"
echo "PDU sessions established:  $(grep -c 'Receiving PDU Session Establishment Accept' "$LOG" 2>/dev/null || echo 0)"
echo "Registration rejects:     $(grep -c 'Receive Registration Reject' "$LOG" 2>/dev/null || echo 0)"
stty sane 2>/dev/null || true
