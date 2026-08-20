#!/usr/bin/env python3
""" Ingest Empirical

Author: Heven Tafese

This is the empirical testbed capture ingester.



 
"""
import os, sys, json, sqlite3, statistics, shutil, hashlib
from pathlib import Path
from datetime import datetime, timezone

ROOT        = Path(__file__).resolve().parent.parent.parent
DATA_DIR    = Path(os.environ.get("EMP_DATA_DIR",    "/home/heven/data"))
DB_PATH     = Path(os.environ.get("EMP_DB_PATH",     ROOT / "data" / "kb.sqlite"))
CHROMA_DIR  = Path(os.environ.get("EMP_CHROMA_DIR",  ROOT / "data" / "chroma_db"))
ARCHIVE_DIR = Path(os.environ.get("EMP_ARCHIVE_DIR", Path.home() / "data" / "empirical_raw_archive"))
OLLAMA_URL  = "http://192.168.56.1:11434/api/embeddings"
EMBED_MODEL = "nomic-embed-text"
COLLECTION  = "empirical"
MOCK_EMBED  = os.environ.get("EMP_MOCK_EMBED") == "1"
RUN_GAP_S   = 30

SCENARIOS = [
    dict(key="flooding_n3", file="mnf_flooding_n3_v3.jsonl",
         name="N3 GTP-U uplink flood", interface="N3", nf="UPF", plane="user", unit="pps",
         ceiling_value=18500, ceiling_status="validated",
         ceiling_note="18,500 pps / 138 Mbps; rho = max(throughput, packet-rate)",
         mechanism="UPF forwarding pins at its ceiling under sustained GTP-U flooding",
         caveat="Wire-level counts; generator-to-UPF TEID alignment unresolved, so not proven UPF-processed."),
    dict(key="n2_3phase", file="mnf_n2_3phase.jsonl",
         name="N2 NGAP signalling storm", interface="N2", nf="AMF", plane="control", unit="msg/s",
         ceiling_from_file=True, ceiling_status="validated",
         mechanism="AMF collapses under overload: observed message rate falls while system load stays high",
         caveat="Peak-rho spikes during the collapse are measurement transients, excluded from the sustained figure."),
    dict(key="n4_heartbeat", file="mnf_n4_heartbeat.jsonl",
         name="N4 PFCP heartbeat flood", interface="N4", nf="UPF", plane="control", unit="msg/s",
         ceiling_from_file=True, ceiling_status="provisional",
         mechanism="UPF PFCP processing thread saturates under heartbeat flood, driving a sustained deep overload",
         caveat="Ceiling provisional."),
    dict(key="n6_congestion_v3", file="mnf_n6_congestion_v3.jsonl",
         name="N6 user-plane egress congestion", interface="N6", nf="UPF", plane="user", unit="bps",
         ceiling_from_file=True, filter_ceiling=24000000, ceiling_status="provisional",
         mechanism="UPF forwarding toward N6 exceeds sustainable rate",
         caveat="Short FAILING tail (~21 s); UE-limited path (UERANSIM UE is the bottleneck); ceiling provisional."),
    dict(key="e2e_ramp", file="mnf_e2e_ramp.jsonl",
         name="End-to-end control-plane congestion", interface="e2e (N2/N4)", nf="AMF/SMF/UPF",
         plane="control", unit="msg/s", multilayer=True, ceiling_status="provisional",
         mechanism="Under session-establishment load the SMF saturates first; AMF and UPF trail",
         caveat=""),
    dict(key="gnb_dl_congestion", file="mnf_gnb_dl_congestion_clean.jsonl",
         name="gNB downlink congestion", interface="N3 downlink", nf="gNB", plane="user", unit="pps",
         ceiling_from_file=True, ceiling_status="provisional",
         mechanism="gNB decapsulation load rises as offered downlink exceeds serviced rate",
         caveat="Short FAILING tail (~9 s); ceiling provisional; extracted from a mixed-calibration capture."),
]

def _num(rec, keys):
    for k in keys:
        if k in rec and isinstance(rec[k], (int, float)): return float(rec[k])
    return None
def get_rho(r):   return _num(r, ["rho"])
def get_band(r):  return str(r.get("band", "")).strip()
def get_layer(r): return str(r.get("layer", "single")).strip()
def get_cpu(r):   return _num(r, ["host_cpu_percent","upf_process_cpu_percent","gnb_cpu_percent","system_cpu_percent","cpu_percent"])
def get_pps(r):   return _num(r, ["offered_downlink_pps","pps","rx_pps","n3_ingress_pps","upfgtp_pps"])
def row_ceiling(r):
    cm=r.get("calibrated_metrics")
    if isinstance(cm,list) and cm: cm=cm[0]
    if isinstance(cm,dict):
        v=cm.get("ceiling_bps", cm.get("ceiling", cm.get("ceiling_pps")))
        return float(v) if isinstance(v,(int,float)) else None
    return None

def load_rows(path):
    out=[]
    for ln in open(path):
        ln=ln.strip()
        if not ln: continue
        try: out.append(json.loads(ln))
        except json.JSONDecodeError: continue
    return out

def split_runs(rows):
    if not rows: return []
    runs=[[rows[0]]]
    for i in range(1,len(rows)):
        if (rows[i].get("timestamp",0)-rows[i-1].get("timestamp",0))>RUN_GAP_S: runs.append([])
        runs[-1].append(rows[i])
    return runs

def choose_run(rows, cfg):
    if cfg.get("filter_ceiling") is not None:
        fc=float(cfg["filter_ceiling"]); rows=[r for r in rows if row_ceiling(r)==fc]
    runs=split_runs(rows)
    failing=[run for run in runs if any(get_band(r).upper()=="FAILING" for r in run)]
    chosen=failing[-1] if failing else (runs[-1] if runs else [])
    while chosen and (get_rho(chosen[-1]) or 0)==0: chosen=chosen[:-1]
    return chosen

def band_arc(bands):
    seen,arc=set(),[]
    for b in bands:
        bl=b.strip()
        if bl and bl.upper() not in seen:
            seen.add(bl.upper()); arc.append("FAILING" if bl.upper()=="FAILING" else bl.lower())
    return arc

def robust(vals):
    v=sorted(x for x in vals if x is not None)
    if not v: return None,None,0
    med=statistics.median(v); thr=max(3*med,1.5)
    kept=[x for x in v if x<=thr] or v
    return round(statistics.mean(kept),3), round(kept[int(0.95*(len(kept)-1))],3), len(v)-len(kept)

def confidence(fs): return "high" if fs>=60 else "medium" if fs>=30 else "low"

def phase(run):
    cong=[get_rho(r) for r in run if get_band(r).lower() in ("congestion","failing")]
    base=[get_rho(r) for r in run if get_band(r).lower()=="normal"]
    s_mean,s_p95,dropped=robust(cong); b_mean,_,_=robust(base)
    fs=sum(1 for r in run if get_band(r).upper()=="FAILING")
    cpus=[get_cpu(r) for r in run if get_cpu(r) is not None]
    return dict(sustained_rho=s_mean, peak_rho=s_p95, baseline_rho=b_mean, transients_dropped=dropped,
                failing_s=fs, confidence=confidence(fs), band_arc=band_arc([get_band(r) for r in run]),
                cpu_peak=round(max(cpus),1) if cpus else None, samples=len(run))

def distill(cfg, rows):
    if cfg.get("multilayer"):
        layers=sorted({get_layer(r) for r in rows}); per_layer={}; result=None; best=-1
        for ly in layers:
            lrows=[r for r in rows if get_layer(r)==ly]
            while lrows and (get_rho(lrows[-1]) or 0)==0: lrows=lrows[:-1]
            st=phase(lrows); st["ceiling"]=row_ceiling(lrows[0]) if lrows else None; per_layer[ly]=st
            if st["failing_s"]>best: result,best=ly,st["failing_s"]
        r=dict(per_layer[result]); r["result_layer"]=result; r["per_layer"]=per_layer
        r["peak_pps"]=None; r["baseline_pps"]=None; return r
    run=choose_run(rows,cfg); st=phase(run)
    st["result_layer"]=get_layer(run[0]) if run else "single"; st["per_layer"]=None
    st["ceiling"]=cfg["ceiling_value"] if not cfg.get("ceiling_from_file") else (row_ceiling(run[0]) if run else None)
    if cfg["unit"]=="pps":
        ppss=[get_pps(r) for r in run if get_pps(r) is not None]
        bp=[get_pps(r) for r in run if get_band(r).lower()=="normal" and get_pps(r) is not None]
        st["peak_pps"]=round(max(ppss),1) if ppss else None
        st["baseline_pps"]=round(min(bp),1) if bp else (round(min(ppss),1) if ppss else None)
    else: st["peak_pps"]=None; st["baseline_pps"]=None
    return st

def render_doc(cfg, sig):
    if cfg.get("ceiling_note"): ceil=cfg["ceiling_note"]
    else: ceil=(f"{sig['ceiling']:.0f} {cfg['unit']} ({cfg.get('ceiling_status','unknown')})" if sig["ceiling"] is not None else "unrecorded")
    arc=" -> ".join(sig["band_arc"]) if sig["band_arc"] else "n/a"
    lines=[f"{cfg['name']} on the {cfg['plane']} plane, interface {cfg['interface']}, network function {cfg['nf']}.",
           f"Mechanism: {cfg['mechanism']}.",
           f"Observed arc: {arc}. Sustained utilisation rho about {sig['sustained_rho']} through congestion "
           f"(near peak p95 {sig['peak_rho']}; baseline about {sig['baseline_rho']}). Calibrated ceiling {ceil}.",
           f"The collapse aware detector latched FAILING for {sig['failing_s']} s (confidence: {sig['confidence']})."]
    if cfg.get("multilayer") and sig.get("per_layer"):
        pl="; ".join(f"{k}: arc {'->'.join(v['band_arc'])}, sustained rho {v['sustained_rho']}, FAILING {v['failing_s']}s"
                     for k,v in sig["per_layer"].items())
        lines.append(f"Per layer, {pl}. Bottleneck layer: {sig['result_layer'].upper()}.")
    if cfg["caveat"]: lines.append(f"Caveat: {cfg['caveat']}")
    return " ".join(lines)

def embed(text):
    if MOCK_EMBED:
        h=hashlib.sha256(text.encode()).digest(); return [((h[i%len(h)]/255.0)*2-1) for i in range(768)]
    import requests
    r=requests.post(OLLAMA_URL, json={"model":EMBED_MODEL,"prompt":text}, timeout=90); r.raise_for_status()
    return r.json()["embedding"]

def write_sqlite(cfg, sig, dataset_file, captured_at):
    if not DB_PATH.exists(): sys.exit(f"[ABORT] {DB_PATH} not found.")
    conn=sqlite3.connect(DB_PATH)
    known=[x[0] for x in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    if "scenario_capture" not in known or "interface_baselines" not in known:
        conn.close(); sys.exit(f"[ABORT] {DB_PATH} is not the KB DB.")
    conn.execute("""INSERT OR REPLACE INTO scenario_capture
        (run_id,scenario,interface,mcp_key,baseline_pps,peak_pps,tf,rho,cpu_peak,recovery_s,dataset_file,valid,captured_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (cfg["key"], cfg["name"], cfg["interface"], f"{sig['result_layer']}:{cfg['interface']}",
         sig["baseline_pps"], sig["peak_pps"], sig["peak_rho"], sig["sustained_rho"],
         sig["cpu_peak"], None, dataset_file, 1, captured_at))
    conn.commit(); conn.close()

def write_chroma(cfg, sig, doc):
    import chromadb
    client=chromadb.PersistentClient(path=str(CHROMA_DIR))
    known=[c.name for c in client.list_collections()]
    if CHROMA_DIR==(ROOT/"data"/"chroma_db") and "normative" not in known:
        sys.exit(f"[ABORT] {CHROMA_DIR} is not the KB chroma dir.")
    col=client.get_or_create_collection(name=COLLECTION, metadata={"description":"testbed congestion captures"})
    meta=dict(scenario=cfg["name"], interface=cfg["interface"], nf=cfg["nf"], plane=cfg["plane"],
              load_unit=cfg["unit"], ceiling=sig["ceiling"] if sig["ceiling"] is not None else -1,
              ceiling_status=cfg.get("ceiling_status","unknown"),
              sustained_rho=sig["sustained_rho"] if sig["sustained_rho"] else -1,
              peak_rho_p95=sig["peak_rho"] if sig["peak_rho"] else -1,
              failing_seconds=sig["failing_s"], confidence=sig["confidence"],
              dataset_file=cfg["file"], caveat_present=bool(cfg["caveat"]), source="testbed_capture")
    col.upsert(ids=[f"empirical::{cfg['key']}"], documents=[doc], metadatas=[meta], embeddings=[embed(doc)])
    return col.count()

def archive_raw(path):
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True); shutil.copy2(path, ARCHIVE_DIR/path.name)

def main():
    print(f"empirical ingester | data={DATA_DIR}{' | MOCK EMBED' if MOCK_EMBED else ''}\n")
    done=0
    for cfg in SCENARIOS:
        p=DATA_DIR/cfg["file"]
        if not p.exists(): print(f"  !! MISSING {cfg['file']}"); continue
        rows=load_rows(p)
        if not rows: print(f"  !! {cfg['file']} empty"); continue
        sig=distill(cfg, rows); doc=render_doc(cfg, sig)
        ca=datetime.fromtimestamp(p.stat().st_mtime, timezone.utc).isoformat()
        write_sqlite(cfg, sig, str(p), ca); cnt=write_chroma(cfg, sig, doc); archive_raw(p); done+=1
        print(f"  [{cfg['key']:>18}] arc={'->'.join(sig['band_arc']):<40} "
              f"sustained_rho={sig['sustained_rho']} p95={sig['peak_rho']} FAIL={sig['failing_s']}s "
              f"conf={sig['confidence']:<6} ceil={sig['ceiling']} drop={sig['transients_dropped']} -> n={cnt}")
    print(f"\nDone. {done}/{len(SCENARIOS)} ingested. Raw archived under {ARCHIVE_DIR}")

if __name__=="__main__": main()
