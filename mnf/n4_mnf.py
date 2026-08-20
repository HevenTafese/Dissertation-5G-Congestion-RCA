#!/usr/bin/env python3
"""
N4 Management function sending PFCP heartbeat-flood congestion on the UPF (SMF<->UPF interface). 
Author: Heven Tafese
"""
import json, os, argparse, subprocess, threading, time, signal, sys
import psutil
from collections import deque

OUTPUT_DIR = "/home/heven/data"

MSG_HEARTBEAT_REQ  = 1
MSG_HEARTBEAT_RESP = 2

ONSET_RHO   = 0.85
CONG_RHO    = 1.0
ARM_RHO     = 0.95   
LOAD_SUSTAIN= 1.5    
FAIL_SEC    = 75
GRACE_SEC   = 15
WINDOW      = 5

class Layer:
    """ Collapse aware, grace tolerant, one way FAILING latch with honest pertick band.
   """
    def __init__(self, ceiling, window=WINDOW):
        self.ceiling=ceiling; self.buf=deque(maxlen=window)
        self.cong_start=None; self.last_high=None; self.latched=False; self.loaded=False
    def update(self, req_rate, load_avg):
        self.buf.append(req_rate/self.ceiling if self.ceiling>0 else 0.0)
        rho=sum(self.buf)/len(self.buf)
        now=time.time()
        if rho>=ARM_RHO:
            self.loaded=True
        congested = (rho>=CONG_RHO) or (self.loaded and load_avg>=LOAD_SUSTAIN)
        if not self.latched:
            if congested:
                if self.cong_start is None: self.cong_start=now
                self.last_high=now
            elif self.cong_start is not None and self.last_high is not None and (now-self.last_high)>GRACE_SEC:
                self.cong_start=None
            if self.cong_start is not None and (now-self.cong_start)>=FAIL_SEC:
                self.latched=True
        if self.latched:
            return rho, "FAILING"
        if congested:
            return rho, "congestion"
        if rho>=ONSET_RHO:
            return rho, "onset"
        return rho, "normal"

def get_upf_proc():
    for p in psutil.process_iter(['pid','name']):
        try:
            if p.info['name']=='upf': return psutil.Process(p.info['pid'])
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return None

def upf_cpu_mem(proc):
    if proc is None: return 0.0, 0.0
    try: return proc.cpu_percent(interval=None), proc.memory_percent()
    except (psutil.NoSuchProcess, psutil.AccessDenied): return 0.0, 0.0

class Counter:
    def __init__(self):
        self.lock=threading.Lock(); self.req=0; self.resp=0
    def record(self, mt):
        with self.lock:
            if mt==MSG_HEARTBEAT_REQ:  self.req+=1
            elif mt==MSG_HEARTBEAT_RESP: self.resp+=1
    def snap(self):
        with self.lock:
            r,rs=self.req,self.resp; self.req=0; self.resp=0
            return r,rs

def tshark_reader(counter, iface):
    cmd=['tshark','-i',iface,'-f','udp port 8805','-l','-T','fields','-e','pfcp.msg_type']
    proc=subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, bufsize=1)
    for line in proc.stdout:
        line=line.strip()
        if not line: continue
        for tok in line.split(','):
            try: counter.record(int(tok))
            except ValueError: pass
    proc.wait()

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--scenario', required=True)
    ap.add_argument('--interval', type=int, default=1)
    ap.add_argument('--interface', default='enp0s8')
    ap.add_argument('--ceiling', type=float, default=None,
                    help='Calibrated heartbeat service ceiling (msg/s). Omit for calibration mode.')
    ap.add_argument('--window', type=int, default=WINDOW)
    a=ap.parse_args()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out=os.path.join(OUTPUT_DIR, f"mnf_{a.scenario}.jsonl")
    mode = "SCENARIO (rho/bands/collapse-aware latch)" if a.ceiling else "CALIBRATION (no rho - finding the ceiling)"
    print(f"[N4 MnF] node=UPF interface=N4 plane=control  metric=PFCP heartbeat flood (collapse signature)")
    print(f"[N4 MnF] mode: {mode}")
    if a.ceiling:
        print(f"[N4 MnF] rho = heartbeat_req_rate / {a.ceiling}   arm@rho>={ARM_RHO}  load-sustain>={LOAD_SUSTAIN}  FAILING@{FAIL_SEC}s")
    print(f"[N4 MnF] interface={a.interface}  output={out}\n")

    upf=get_upf_proc(); upf_cpu_mem(upf)
    print(f"[N4 MnF] UPF process: {'found pid '+str(upf.pid) if upf else 'NOT FOUND'}")

    counter=Counter()
    threading.Thread(target=tshark_reader, args=(counter, a.interface), daemon=True).start()
    layer=Layer(a.ceiling, a.window) if a.ceiling else None

    def stop(*_): print("\n[N4 MnF] Stopped. Dataset: "+out); sys.exit(0)
    signal.signal(signal.SIGINT, stop)

    with open(out,'a') as f:
        while True:
            time.sleep(a.interval)
            req,resp = counter.snap()
            req_rate=req/a.interval
            resp_rate=resp/a.interval
            ucpu,umem = upf_cpu_mem(upf)
            sysmem = psutil.virtual_memory().percent
            load1 = os.getloadavg()[0]
            ts=time.time()

            rec={"timestamp":ts,"layer":"upf","plane":"control","interface":"N4",
                 "source":"tshark:pfcp:8805","scenario":a.scenario,
                 "pfcp_heartbeat_req_rate":round(req_rate,2),
                 "pfcp_heartbeat_resp_rate":round(resp_rate,2),
                 "upf_cpu_percent":round(ucpu,2),
                 "upf_memory_percent":round(umem,2),
                 "memory_percent":round(sysmem,2),
                 "load_avg_1m":round(load1,4)}

            if layer is not None:
                rho,band = layer.update(req_rate, load1)
                rec.update({"rho":round(rho,3),"rho_smoothed":round(rho,3),
                            "congestion_rho":round(rho,3),"band":band,
                            "calibrated_metrics":[{"field":"pfcp_heartbeat_req_rate","ceiling":a.ceiling,"rho":round(rho,3)}]})
                tag=f"rho={rho:4.2f} {band:10s} {'L' if layer.loaded else ''}"
            else:
                tag="[calibration]"

            f.write(json.dumps(rec)+"\n"); f.flush()
            print(f"[{time.strftime('%H:%M:%S')}] hb_req={req_rate:7.0f}/s hb_resp={resp_rate:7.0f}/s "
                  f"UPFcpu={ucpu:5.1f}% UPFmem={umem:4.1f}% load={load1:4.2f} {tag}")

if __name__=='__main__':
    main()
