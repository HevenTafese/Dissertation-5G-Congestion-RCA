#!/usr/bin/env python3
"""end to end  Management Function

Author: Heven Tafese

End to end control plane management function (AMF -> SMF -> UPF). 

"""
import json, time, os, argparse, subprocess, signal, sys
import psutil
from collections import deque
import statistics as st

# ngap msg/s
AMF_NGAP_CEILING = 100.0
SMF_N11_CEILING  = 110.0
# pfcp msg/s
UPF_PFCP_CEILING = 25.0
# info only smf cpu column
SMF_CPU_CEILING  = 20.0

ONSET_RHO = 0.85
CONG_RHO  = 1.0
FAIL_SEC  = 75
# tolerate dips up to 15s so onboarding gaps do not reset the 75s latch
GRACE_SEC = 15
# moving average window in samples, same as seconds at interval 1
WINDOW    = 5

N2_IF  = "enp0s8"
N4_IF  = "lo"
N11_IF = "lo"

AMF_SBI = "127.0.0.18"
SMF_SBI = "127.0.0.2"
SBI_PORT = 8000

def find_proc(substr):
    for p in psutil.process_iter(['pid','cmdline']):
        try:
            if substr in ' '.join(p.info['cmdline'] or []):
                return p
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return None

def cpu_of(proc):
    if proc is None: return 0.0
    try: return proc.cpu_percent(interval=None)
    except (psutil.NoSuchProcess, psutil.AccessDenied): return 0.0

class Layer:
    def __init__(self, name, ceiling, window=WINDOW):
        self.name=name; self.ceiling=ceiling
        self.buf=deque(maxlen=window); self.cong_start=None; self.last_high=None; self.latched=False
    def update(self, load):
        self.buf.append(load/self.ceiling if self.ceiling>0 else 0.0)
        rho=sum(self.buf)/len(self.buf)
        now=time.time()
        if self.latched:
            return rho, "FAILING"
        if rho>=CONG_RHO:
            if self.cong_start is None: self.cong_start=now
            self.last_high=now
            if now-self.cong_start>=FAIL_SEC:
                self.latched=True; return rho, "FAILING"
            return rho, "congestion"
        # rho below congestion: tolerate a brief dip so the 75s timer is not reset by onboarding gaps
        if self.cong_start is not None and self.last_high is not None and (now-self.last_high)<=GRACE_SEC:
            if now-self.cong_start>=FAIL_SEC:
                self.latched=True; return rho, "FAILING"
            return rho, ("onset" if rho>=ONSET_RHO else "normal")
        self.cong_start=None
        return rho, ("onset" if rho>=ONSET_RHO else "normal")

def start_tshark(iface, bpf):
    return subprocess.Popen(
        ["tshark","-i",iface,"-f",bpf,"-l","-q","-T","fields","-e","frame.time_epoch"],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, bufsize=1)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--scenario", required=True)
    ap.add_argument("--interval", type=int, default=1)
    ap.add_argument("--window", type=int, default=WINDOW)
    ap.add_argument("--n2-if", default=N2_IF)
    ap.add_argument("--n4-if", default=N4_IF)
    ap.add_argument("--n11-if", default=N11_IF)
    a=ap.parse_args()

    out=os.path.join("/home/heven/data", f"mnf_{a.scenario}.jsonl")
    os.makedirs("/home/heven/data", exist_ok=True)

    amf_p=find_proc("bin/amf"); smf_p=find_proc("bin/smf"); upf_p=find_proc("bin/upf")
    for p in (amf_p,smf_p,upf_p):
        if p: cpu_of(p)

    n11_bpf="tcp port 8000 and host 127.0.0.2 and not host 127.0.0.10"
    print("[e2e MnF] interfaces: AMF=ngap/s(N2)  SMF=n11 pkt/s(SBI)  UPF=pfcp/s(N4)  + per-NF cpu")
    print(f"[e2e MnF] N11 filter on {a.n11_if}: {n11_bpf}")
    print(f"[e2e MnF] NF found: amf={'y' if amf_p else 'n'} smf={'y' if smf_p else 'n'} upf={'y' if upf_p else 'n'}")
    print(f"[e2e MnF] ceilings: AMF ngap={AMF_NGAP_CEILING}/s  SMF n11={SMF_N11_CEILING}pkt/s  UPF pfcp={UPF_PFCP_CEILING}/s")
    print(f"[e2e MnF] smoothing: {a.window}-sample moving average   Output: {out}\n")

    ng =start_tshark(a.n2_if,  "sctp port 38412")
    pf =start_tshark(a.n4_if,  "udp port 8805")
    n11=start_tshark(a.n11_if, n11_bpf)
    import threading
    ng_count=[0]; pf_count=[0]; n11_count=[0]; lock=threading.Lock()
    def reader(proc, counter):
        for _ in proc.stdout:
            with lock: counter[0]+=1
    threading.Thread(target=reader,args=(ng,ng_count),daemon=True).start()
    threading.Thread(target=reader,args=(pf,pf_count),daemon=True).start()
    threading.Thread(target=reader,args=(n11,n11_count),daemon=True).start()

    L_amf=Layer("amf",AMF_NGAP_CEILING,a.window)
 
    L_smf=Layer("smf",SMF_N11_CEILING,a.window)
    L_upf=Layer("upf",UPF_PFCP_CEILING,a.window)

    def stop(*_):
        for p in (ng,pf,n11):
            try: p.terminate()
            except Exception: pass
        print("\n[e2e MnF] Stopped."); sys.exit(0)
    signal.signal(signal.SIGINT, stop)

    with open(out,"a") as f:
        while True:
            time.sleep(a.interval)
            with lock:
                ngs=ng_count[0]; ng_count[0]=0
                pfs=pf_count[0]; pf_count[0]=0
                n11s=n11_count[0]; n11_count[0]=0
            ngs/=a.interval; pfs/=a.interval; n11s/=a.interval
            amf_cpu=cpu_of(amf_p); smf_cpu=cpu_of(smf_p); upf_cpu=cpu_of(upf_p)

            amf_rho,amf_band=L_amf.update(ngs)
            smf_rho,smf_band=L_smf.update(n11s)
            upf_rho,upf_band=L_upf.update(pfs)

            bott=max([("amf",amf_rho),("smf",smf_rho),("upf",upf_rho)],key=lambda x:x[1])[0]

            ts=time.time()
            mem_pct=psutil.virtual_memory().percent
            try: load1=os.getloadavg()[0]
            except Exception: load1=0.0
            e2e_blk={"amf":{"ngap_rate":round(ngs,1),"rho":round(amf_rho,3),"band":amf_band,"cpu":amf_cpu},
                     "smf":{"n11_rate":round(n11s,1),"rho":round(smf_rho,3),"band":smf_band,"cpu":smf_cpu},
                     "upf":{"pfcp_rate":round(pfs,1),"rho":round(upf_rho,3),"band":upf_band,"cpu":upf_cpu},
                     "bottleneck":bott}
            _layers=[("amf","control","N2","tshark:ngap:38412","ngap_total_message_rate",ngs,amf_rho,amf_band,amf_cpu,AMF_NGAP_CEILING),
                     ("smf","control","N11","tshark:sbi:8000","n11_packet_rate",n11s,smf_rho,smf_band,smf_cpu,SMF_N11_CEILING),
                     ("upf","user","N4","tshark:pfcp:8805","pfcp_message_rate",pfs,upf_rho,upf_band,upf_cpu,UPF_PFCP_CEILING)]
            for (_l,_pl,_if,_src,_fld,_rate,_rho,_bnd,_cpu,_ceil) in _layers:
                rec={"timestamp":ts,"layer":_l,"plane":_pl,"interface":_if,"source":_src,"scenario":a.scenario,
                     _fld:round(_rate,1),
                     "rho":round(_rho,3),"rho_smoothed":round(_rho,3),"congestion_rho":round(_rho,3),
                     "band":_bnd,
                     "cpu_percent":_cpu,"memory_percent":mem_pct,"load_avg_1m":load1,
                     "calibrated_metrics":[{"field":_fld,"ceiling":_ceil,"rho":round(_rho,3)}],
                     "e2e":e2e_blk}
                f.write(json.dumps(rec)+"\n")
            f.flush()
            print(f"[bott={bott:3s}] "
                  f"AMF ngap={ngs:5.0f}/s rho={amf_rho:4.2f} {amf_band:10s} | "
                  f"SMF n11={n11s:5.0f}/s cpu={smf_cpu:4.1f} rho={smf_rho:4.2f} {smf_band:10s} | "
                  f"UPF pfcp={pfs:4.0f}/s rho={upf_rho:4.2f} {upf_band:10s}")

if __name__=="__main__":
    main()
