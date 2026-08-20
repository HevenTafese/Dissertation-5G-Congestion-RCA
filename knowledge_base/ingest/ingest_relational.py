#!/usr/bin/env python3
"""    Ingest Relational

Author: Heven Tafese

This builds the SQLite relational store for the congestion RCA knowledge
base. It feeds both the RAG's deterministic rho computation and the
verification gate's R1 to R6 rules. Every row carries source_ref for
provenance.

"""
import sqlite3, re, sys
from pathlib import Path

ROOT           = Path(__file__).resolve().parent.parent.parent
RELATIONAL_DIR = ROOT / "data" / "relational_sources" / "add"
DB_PATH        = ROOT / "data" / "kb.sqlite"

SCHEMA = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS interface_baselines (
    id INTEGER PRIMARY KEY,
    interface TEXT NOT NULL, metric TEXT NOT NULL,
    normal_value REAL, ceiling_value REAL, congestion_threshold REAL,
    unit TEXT NOT NULL, link_rate_r REAL, source_ref TEXT NOT NULL,
    UNIQUE(interface, metric)
);
CREATE TABLE IF NOT EXISTS verification_rules (
    rule_id TEXT PRIMARY KEY, rho_band TEXT NOT NULL,
    causal_root INTEGER NOT NULL, severity TEXT NOT NULL,
    action TEXT NOT NULL, description TEXT
);
CREATE TABLE IF NOT EXISTS kpi_definitions (
    kpi_name TEXT PRIMARY KEY, ts_ref TEXT, formula TEXT,
    interface TEXT, unit TEXT, source_ref TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS mitigation_actions (
    action_id TEXT PRIMARY KEY, class TEXT NOT NULL,
    applies_to TEXT NOT NULL, trigger_condition TEXT,
    cost TEXT, source_ref TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS scenario_capture (
    run_id TEXT PRIMARY KEY, scenario TEXT NOT NULL,
    interface TEXT NOT NULL, mcp_key TEXT NOT NULL,
    baseline_pps REAL, peak_pps REAL, tf REAL, rho REAL,
    cpu_peak REAL, recovery_s REAL, dataset_file TEXT,
    valid INTEGER NOT NULL DEFAULT 0, captured_at TEXT
);
CREATE TABLE IF NOT EXISTS literature_baselines (
    id INTEGER PRIMARY KEY, nf TEXT NOT NULL, metric TEXT NOT NULL,
    value REAL NOT NULL, unit TEXT NOT NULL, condition TEXT,
    paper_ref TEXT NOT NULL, notes TEXT
);
"""

BASELINES = [
    ("N3","throughput_pps",None,18500,18500,"pps",18500,"testbed calibration mnf_flooding.jsonl"),
    ("N3","throughput_mbps",None,138,138,"mbps",138,"testbed calibration mnf_flooding.jsonl"),
    ("N2","registration_rate",None,None,None,"reg_per_s",None,"pending N2 recapture"),
    ("N4","pfcp_msg_rate",None,None,None,"msg_per_s",None,"pending N4 build"),
    ("N6","throughput_mbps",None,None,None,"mbps",None,"pending N6 calibration"),
]
RULES = [
    ("R1","congestion",1,"critical","allow_mitigation","rho>=1.0 and causal root found"),
    ("R2","congestion",0,"critical","flag_ambiguous","rho>=1.0 but no causal root"),
    ("R3","onset",1,"warning","allow_mitigation_with_log","0.85<=rho<1.0 and causal root found"),
    ("R4","onset",0,"warning","flag_ambiguous","0.85<=rho<1.0 no causal root"),
    ("R5","normal",0,"normal","log_only","rho<0.85 no causal root"),
    ("R6","normal",1,"normal","log_potential_early_warning","rho<0.85 but causal structure present"),
]
KPIS = [
    ("traffic_intensity","supervisor model","TF = L / a","all","bits/s","supervisor"),
    ("utilisation","supervisor model","rho = (L / a) / R","all","ratio","supervisor"),
    ("mean_packet_delay","TS 28.552 clause 5.1",None,"N3","ms","TS 28.552"),
    ("packet_loss_rate","TS 28.552 clause 5.1",None,"N3","ratio","TS 28.552"),
    ("registration_rate","TS 28.552 clause 5.2",None,"N2","reg/s","TS 28.552"),
    ("pfcp_session_rate","TS 28.552 clause 5.3",None,"N4","sess/s","TS 28.552"),
    ("amf_cpu_utilisation","TS 28.552 clause 5.2",None,"N2","percent","TS 28.552"),
    ("upf_cpu_utilisation","TS 28.552 clause 5.1",None,"N3","percent","TS 28.552"),
]
MITIGATIONS = [
    ("M1","vertical_scale","AMF","rho>=1.0 AND root=amf_cpu_rise","medium","pending literature extraction"),
    ("M2","horizontal_scale","AMF","rho>=1.0 AND amplification_loop_present","high","pending literature extraction"),
    ("M3","rate_limit","gNB","rho>=0.85 AND root=mass_ue_registration","low","pending literature extraction"),
    ("M4","vertical_scale","SMF","rho>=1.0 AND root=smf_pfcp_handler_saturation","medium","pending literature extraction"),
    ("M5","horizontal_scale","UPF","rho>=1.0 AND root=upf_forwarding_cpu_rise","high","pending literature extraction"),
    ("M6","admission_control","AMF","rho in onset AND cross_plane_cascade","low","pending literature extraction"),
]

def extract_numbers_from_pdf(path):
    import fitz
    doc   = fitz.open(str(path))
    paper = path.stem[:80]
    text  = re.sub(r"\s+", " ", " ".join(p.get_text() for p in doc))
    results = []
    nf_pat  = r"(AMF|SMF|UPF|NRF|PCF|AUSF|UDM|gNB|UE)"
    met_pat = r"(throughput|latency|delay|CPU|memory|request rate|packet loss|registration rate|session rate|processing time|response time)"
    num_pat = r"(\d+(?:\.\d+)?)\s*(ms|s|Mbps|Gbps|kbps|pps|%|requests?/s|msg/s|sessions?/s|packets?/s)"
    for sent in re.split(r'[.!?]', text):
        nf  = re.search(nf_pat,  sent, re.IGNORECASE)
        met = re.search(met_pat, sent, re.IGNORECASE)
        num = re.search(num_pat, sent, re.IGNORECASE)
        if nf and met and num:
            results.append((
                nf.group(1).upper(), met.group(1).lower(),
                float(num.group(1)), num.group(2),
                "extracted", paper, sent.strip()[:200]
            ))
    return results

def main():
    print("=== building relational store ===")
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA)
    conn.commit()
    print("[schema] tables created")
    conn.executemany("INSERT OR IGNORE INTO interface_baselines (interface,metric,normal_value,ceiling_value,congestion_threshold,unit,link_rate_r,source_ref) VALUES (?,?,?,?,?,?,?,?)", BASELINES)
    conn.executemany("INSERT OR IGNORE INTO verification_rules (rule_id,rho_band,causal_root,severity,action,description) VALUES (?,?,?,?,?,?)", RULES)
    conn.executemany("INSERT OR IGNORE INTO kpi_definitions (kpi_name,ts_ref,formula,interface,unit,source_ref) VALUES (?,?,?,?,?,?)", KPIS)
    conn.executemany("INSERT OR IGNORE INTO mitigation_actions (action_id,class,applies_to,trigger_condition,cost,source_ref) VALUES (?,?,?,?,?,?)", MITIGATIONS)
    conn.commit()
    print("=== extracting from benchmarking papers ===")
    all_rows = []
    for pdf in sorted(RELATIONAL_DIR.glob("*.pdf")):
        print(f"  extracting from {pdf.name} ...")
        rows = extract_numbers_from_pdf(pdf)
        print(f"    {len(rows)} measurements found")
        all_rows.extend(rows)
    if all_rows:
        conn.executemany("INSERT OR IGNORE INTO literature_baselines (nf,metric,value,unit,condition,paper_ref,notes) VALUES (?,?,?,?,?,?,?)", all_rows)
        conn.commit()
    print("\n=== verification ===")
    for table in ["interface_baselines","verification_rules","kpi_definitions","mitigation_actions","scenario_capture","literature_baselines"]:
        n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {n} rows")
    conn.close()
    print(f"\nRelational store built at {DB_PATH}")

if __name__ == "__main__":
    main()
