#!/usr/bin/env python3
""" gNB Downlink Management Function

Author: Heven Tafese
"""
import json, os, glob, argparse, subprocess, threading, time, signal, sys
import psutil
from collections import deque

OUTPUT_DIR = "/home/heven/data"
ONSET_RHO=0.85; CONG_RHO=1.0; ARM_RHO=0.95; CPU_SUSTAIN=50.0
FAIL_SEC=75; GRACE_SEC=15; WINDOW=5

class Layer:
    def __init__(self, ceiling, window=WINDOW, cpu_sustain=CPU_SUSTAIN):
        self.ceiling=ceiling; self.buf=deque(maxlen=window); self.cpu_sustain=cpu_sustain
        self.cong_start=None; self.last_high=None; self.latched=False; self.loaded=False
    def update(self, pps, gnb_cpu):
        self.buf.append(pps/self.ceiling if self.ceiling>0 else 0.0)
        rho=sum(self.buf)/len(self.buf); now=time.time()
        if rho>=ARM_RHO: self.loaded=True
        congested = (rho>=CONG_RHO) or (self.loaded and gnb_cpu>=self.cpu_sustain)
        if not self.latched:
            if congested:
                if self.cong_start is None: self.cong_start=now
                self.last_high=now
            elif self.cong_start is not None and self.last_high is not None and (now-self.last_high)>GRACE_SEC:
                self.cong_start=None
            if self.cong_start is not None and (now-self.cong_start)>=FAIL_SEC:
                self.latched=True
        if self.latched: return rho, "FAILING"
        if congested: return rho, "congestion"
        if rho>=ONSET_RHO: return rho, "onset"
        return rho, "normal"

def get_gnb_proc():
    for p in psutil.process_iter(['pid','name']):
        try:
            if p.info['name']=='nr-gnb': return psutil.Process(p.info['pid'])
        except (psutil.NoSuchProcess, psutil.AccessDenied): continue
    return None

def gnb_cpu_mem(proc):
    if proc is None: return 0.0, 0.0
    try: return proc.cpu_percent(interval=None), proc.memory_percent()
    except (psutil.NoSuchProcess, psutil.AccessDenied): return 0.0, 0.0

def read_serviced_total(sysfs_glob="/sys/class/net/uesimtun*/statistics/rx_packets"):
    """Cumulative RX packets across all UE tunnels = packets the gNB decapsulated and delivered."""
    total=0
    for path in glob.glob(sysfs_glob):
        try:
            with open(path) as fh: total+=int(fh.read().strip())
        except (OSError, ValueError): pass
    return total

def read_udp_drops_total(snmp_path="/proc/net/snmp"):
    """Cumulative UDP receive buffer overflow drops (RcvbufErrors) on the host."""
    try:
        with open(snmp_path) as fh: lines=fh.read().splitlines()
    except OSError: return 0
    hdr=val=None
    for i,l in enumerate(lines):
        if l.startswith("Udp:") and "RcvbufErrors" in l:
            hdr=l.split(); val=lines[i+1].split(); break
    if hdr and val:
        try: return int(val[hdr.index("RcvbufErrors")])
        except (ValueError, IndexError): return 0
    return 0

class Counter:
    def __init__(self): self.lock=threading.Lock(); self.pkts=0; self.bytes=0
    def record(self, length):
        with self.lock: self.pkts+=1; self.bytes+=length
    def snap(self):
        with self.lock:
            p,b=self.pkts,self.bytes; self.pkts=0; self.bytes=0
            return p,b

def tshark_reader(counter, iface, gnb_ip):
    cmd=['tshark','-i',iface,'-f',f'udp port 2152 and dst {gnb_ip}','-l','-T','fields','-e','frame.len']
    proc=subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, bufsize=1)
    for line in proc.stdout:
        line=line.strip()
        if not line: continue
        try: counter.record(int(line))
        except ValueError: pass
    proc.wait()

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--scenario', required=True)
    ap.add_argument('--interval', type=int, default=1)
    ap.add_argument('--interface', default='enp0s8')
    ap.add_argument('--gnb-ip', default='192.168.56.20')
    ap.add_argument('--ceiling', type=float, default=None)
    ap.add_argument('--cpu-sustain', type=float, default=CPU_SUSTAIN)
    ap.add_argument('--window', type=int, default=WINDOW)
    a=ap.parse_args()
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out=os.path.join(OUTPUT_DIR, f"mnf_{a.scenario}.jsonl")
    mode = "SCENARIO (rho/bands/collapse aware latch)" if a.ceiling else "CALIBRATION (no rho, finding the ceiling)"
    print(f"[gNB MnF] node=gNB interface=N3-downlink plane=user  metric=downlink GTP-U (offered vs serviced + drops + CPU)")
    print(f"[gNB MnF] mode: {mode}")
    if a.ceiling:
        print(f"[gNB MnF] rho = offered_pps / {a.ceiling}   arm@rho>={ARM_RHO}  gNB-CPU-sustain>={a.cpu_sustain}  FAILING@{FAIL_SEC}s")
    print(f"[gNB MnF] interface={a.interface}  gnb_ip={a.gnb_ip}  output={out}\n")
    gnb=get_gnb_proc(); gnb_cpu_mem(gnb)
    print(f"[gNB MnF] nr-gnb process: {'found pid '+str(gnb.pid) if gnb else 'not found'}")
    ntun=len(glob.glob('/sys/class/net/uesimtun*'))
    print(f"[gNB MnF] UE tunnels found for serviced rate: {ntun}")

    counter=Counter()
    threading.Thread(target=tshark_reader, args=(counter, a.interface, a.gnb_ip), daemon=True).start()
    layer=Layer(a.ceiling, a.window, a.cpu_sustain) if a.ceiling else None
    prev_serviced=read_serviced_total(); prev_drops=read_udp_drops_total()

    def stop(*_): print("\n[gNB MnF] Stopped. Dataset: "+out); sys.exit(0)
    signal.signal(signal.SIGINT, stop)

    with open(out,'a') as f:
        while True:
            time.sleep(a.interval)
            pkts,byts = counter.snap()
            offered_pps=pkts/a.interval; offered_mbps=byts*8/a.interval/1e6
            cur_serviced=read_serviced_total(); serviced_pps=max(0,(cur_serviced-prev_serviced))/a.interval; prev_serviced=cur_serviced
            cur_drops=read_udp_drops_total(); drops_ps=max(0,(cur_drops-prev_drops))/a.interval; prev_drops=cur_drops
            gcpu,gmem = gnb_cpu_mem(gnb)
            sysmem = psutil.virtual_memory().percent
            sysload = os.getloadavg()[0]; ts=time.time()

            rec={"timestamp":ts,"layer":"gnb","plane":"user","interface":"N3",
                 "source":"tshark:gtpu:2152","scenario":a.scenario,
                 "offered_downlink_pps":round(offered_pps,2),
                 "offered_downlink_mbps":round(offered_mbps,3),
                 "serviced_pps":round(serviced_pps,2),
                 "udp_rx_drops_per_s":round(drops_ps,2),
                 "gnb_cpu_percent":round(gcpu,2),
                 "gnb_memory_percent":round(gmem,2),
                 "memory_percent":round(sysmem,2),
                 "load_avg_1m":round(sysload,4)}

            if layer is not None:
                rho,band = layer.update(offered_pps, gcpu)
                rec.update({"rho":round(rho,3),"rho_smoothed":round(rho,3),
                            "congestion_rho":round(rho,3),"band":band,
                            "calibrated_metrics":[{"field":"offered_downlink_pps","ceiling":a.ceiling,"rho":round(rho,3)}]})
                tag=f"rho={rho:4.2f} {band:9s} {'L' if layer.loaded else ''}"
            else:
                tag="[calibration]"

            f.write(json.dumps(rec)+"\n"); f.flush()
            print(f"[{time.strftime('%H:%M:%S')}] offered={offered_pps:7.0f}/s serviced={serviced_pps:7.0f}/s drops={drops_ps:6.0f}/s "
                  f"gNBcpu={gcpu:5.1f}% load={sysload:4.2f} {tag}")

if __name__=='__main__':
    main()
