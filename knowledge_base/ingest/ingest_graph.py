#!/usr/bin/env python3
"""  Ingest Graph

Author: Heven Tafese

This builds the Neo4j graph database for the congestion RCA knowledge
base. Sources are yaml OpenAPI specs, 3GPP docx procedure specs, and
academic papers. Every node and edge carries source_ref for provenance.
"""
import re, sys, yaml
from pathlib import Path
from neo4j import GraphDatabase

ROOT      = Path(__file__).resolve().parent.parent.parent
GRAPH_DIR = ROOT / "data" / "graph_sources" / "add"
NEO4J_URI  = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "heven123"

NF_INTERFACES = {
    "AMF":  ["N1","N2","N8","N11","N12","N15","N22"],
    "SMF":  ["N4","N7","N10","N11","N16"],
    "UPF":  ["N3","N4","N6","N9"],
    "NRF":  ["N27"],
    "PCF":  ["N5","N7","N15"],
    "AUSF": ["N12","N13"],
    "UDM":  ["N8","N10","N13"],
    "UDR":  ["N35","N36","N37"],
    "NSSF": ["N22"],
    "NEF":  ["N29","N30","N33"],
}

CAUSAL_CHAINS = [
    {"from":"mass_ue_registration","from_type":"CongestionEvent","to":"AMF","to_type":"NetworkFunction","rel":"OVERLOADS","properties":{"interface":"N2","lag_windows":0,"source_ref":"TS 38.413 clause 8.6.1 NGAP Overload Start","cross_plane":False}},
    {"from":"AMF","from_type":"NetworkFunction","to":"ngap_processing_delay","to_type":"CongestionEvent","rel":"CAUSES","properties":{"interface":"N2","lag_windows":None,"source_ref":"TS 38.413 clause 8.6.2 Overload Stop procedure","cross_plane":False}},
    {"from":"ngap_processing_delay","from_type":"CongestionEvent","to":"registration_timeout","to_type":"CongestionEvent","rel":"CAUSES","properties":{"interface":"N2","lag_windows":0,"source_ref":"TS 38.413 clause 9.2 NGAP elementary procedures","cross_plane":False}},
    {"from":"registration_timeout","from_type":"CongestionEvent","to":"mass_ue_registration","to_type":"CongestionEvent","rel":"AMPLIFIES","properties":{"interface":"N2","lag_windows":None,"source_ref":"TS 38.413 clause 8.6 UE retry on timeout","cross_plane":False}},
    {"from":"pfcp_heartbeat_flood","from_type":"CongestionEvent","to":"SMF","to_type":"NetworkFunction","rel":"OVERLOADS","properties":{"interface":"N4","lag_windows":0,"source_ref":"TS 29.244 clause 7.4 PFCP Overload Control","cross_plane":False}},
    {"from":"SMF","from_type":"NetworkFunction","to":"session_establishment_delay","to_type":"CongestionEvent","rel":"CAUSES","properties":{"interface":"N4","lag_windows":None,"source_ref":"TS 29.244 clause 7.3 Load Control Information","cross_plane":False}},
    {"from":"gtpu_flood","from_type":"CongestionEvent","to":"UPF","to_type":"NetworkFunction","rel":"OVERLOADS","properties":{"interface":"N3","lag_windows":0,"source_ref":"TS 29.281 clause 5.1 GTP-U header fields","cross_plane":False}},
    {"from":"UPF","from_type":"NetworkFunction","to":"packet_drop","to_type":"CongestionEvent","rel":"CAUSES","properties":{"interface":"N3","lag_windows":None,"source_ref":"TS 29.281 clause 7.1 Error Indication","cross_plane":False}},
    {"from":"registration_timeout","from_type":"CongestionEvent","to":"session_establishment_delay","to_type":"CongestionEvent","rel":"CROSS_PLANE","properties":{"interface":"N2-N4","lag_windows":None,"source_ref":"TS 23.502 clause 4.3.2 PDU Session Establishment","cross_plane":True}},
    {"from":"session_establishment_delay","from_type":"CongestionEvent","to":"packet_drop","to_type":"CongestionEvent","rel":"CROSS_PLANE","properties":{"interface":"N4-N3","lag_windows":None,"source_ref":"TS 23.502 clause 4.3.2 GTP-U tunnel setup","cross_plane":True}},
]

def create_constraints(session):
    for c in [
        "CREATE CONSTRAINT nf_name IF NOT EXISTS FOR (n:NetworkFunction) REQUIRE n.name IS UNIQUE",
        "CREATE CONSTRAINT event_name IF NOT EXISTS FOR (e:CongestionEvent) REQUIRE e.name IS UNIQUE",
        "CREATE CONSTRAINT iface_name IF NOT EXISTS FOR (i:Interface) REQUIRE i.name IS UNIQUE",
    ]:
        try: session.run(c)
        except: pass

def ingest_nf_nodes(session):
    for nf, interfaces in NF_INTERFACES.items():
        session.run("MERGE (n:NetworkFunction {name:$name}) SET n.interfaces=$interfaces, n.source_ref='TS 23.501 clause 6'", name=nf, interfaces=interfaces)
    print(f"  NetworkFunction nodes: {len(NF_INTERFACES)}")

def ingest_interface_nodes(session):
    interfaces = set(i for ifaces in NF_INTERFACES.values() for i in ifaces)
    for iface in interfaces:
        session.run("MERGE (i:Interface {name:$name}) SET i.source_ref='TS 23.501 clause 5'", name=iface)
    print(f"  Interface nodes: {len(interfaces)}")

def ingest_causal_chains(session):
    for edge in CAUSAL_CHAINS:
        session.run(f"MERGE (a:{edge['from_type']} {{name:$n}})", n=edge["from"])
        session.run(f"MERGE (b:{edge['to_type']} {{name:$n}})", n=edge["to"])
        p = edge["properties"]
        session.run(f"""
            MATCH (a:{edge['from_type']} {{name:$fn}})
            MATCH (b:{edge['to_type']} {{name:$tn}})
            MERGE (a)-[r:{edge['rel']}]->(b)
            SET r.interface=$interface, r.lag_windows=$lag, r.source_ref=$src, r.cross_plane=$cp
        """, fn=edge["from"], tn=edge["to"],
            interface=p["interface"], lag=p["lag_windows"],
            src=p["source_ref"], cp=p["cross_plane"])
    print(f"  Causal chain edges: {len(CAUSAL_CHAINS)}")

def ingest_yaml_operations(session):
    yaml_files = list(GRAPH_DIR.glob("*.yaml"))
    if not yaml_files: print("  no yaml files"); return
    total = 0
    for yf in yaml_files:
        try:
            with open(yf) as f: spec = yaml.safe_load(f)
        except Exception as e: print(f"  skip {yf.name}: {e}"); continue
        nf_owner = next((nf for nf in NF_INTERFACES if nf.lower() in yf.stem.lower()), None)
        if not nf_owner: continue
        paths = spec.get("paths", {}) if isinstance(spec, dict) else {}
        count = 0
        for path, methods in paths.items():
            if not isinstance(methods, dict): continue
            for method, op in methods.items():
                if not isinstance(op, dict): continue
                op_id = op.get("operationId", f"{method}_{path}")
                proc  = f"{nf_owner}:{op_id}"
                session.run("MERGE (p:Procedure {name:$name}) SET p.operation_id=$oid, p.method=$m, p.path=$path, p.source_ref=$src",
                    name=proc, oid=op_id, m=method.upper(), path=path, src=yf.stem)
                session.run("MATCH (nf:NetworkFunction {name:$nf}) MATCH (p:Procedure {name:$proc}) MERGE (nf)-[:EXPOSES]->(p)",
                    nf=nf_owner, proc=proc)
                count += 1
        print(f"  {yf.stem}: {count} operations")
        total += count
    print(f"  total yaml operations: {total}")

def ingest_docx_procedures(session):
    from docx import Document
    docx_files = list(GRAPH_DIR.glob("*.docx"))
    if not docx_files: print("  no docx files"); return
    nf_names = list(NF_INTERFACES.keys())
    total = 0
    for dp in docx_files:
        doc = Document(dp)
        stem = dp.stem
        heading_re = re.compile(r"^\d+(\.\d+)*\s+\S")
        current_heading = "preamble"
        for para in doc.paragraphs:
            txt = para.text.strip()
            if not txt: continue
            if para.style.name.startswith("Heading") or heading_re.match(txt):
                current_heading = txt[:120]; continue
            found = [nf for nf in nf_names if re.search(rf'\b{nf}\b', txt)]
            if len(found) < 2: continue
            for i in range(len(found)-1):
                if found[i] == found[i+1]: continue
                session.run("""
                    MATCH (a:NetworkFunction {name:$src})
                    MATCH (b:NetworkFunction {name:$tgt})
                    MERGE (a)-[r:TRIGGERS]->(b)
                    SET r.source_ref=$src_ref, r.clause=$clause, r.cross_plane=false
                """, src=found[i], tgt=found[i+1], src_ref=stem, clause=current_heading[:120])
                total += 1
        print(f"  {stem}: procedure edges extracted")
    print(f"  total procedure edges: {total}")

def print_summary(session):
    print("\n=== graph summary ===")
    for r in session.run("MATCH (n) RETURN labels(n)[0] as label, count(n) as cnt"):
        print(f"  {r['label']}: {r['cnt']} nodes")
    for r in session.run("MATCH ()-[r]->() RETURN type(r) as rel, count(r) as cnt ORDER BY cnt DESC"):
        print(f"  {r['rel']}: {r['cnt']} edges")

def main():
    print("=== building graph database ===")
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        driver.verify_connectivity()
        print("Neo4j connected OK")
    except Exception as e:
        print(f"ERROR: {e}"); sys.exit(1)
    with driver.session() as session:
        print("\n[1] creating constraints...")
        create_constraints(session)
        print("[2] ingesting NetworkFunction nodes...")
        ingest_nf_nodes(session)
        print("[3] ingesting Interface nodes...")
        ingest_interface_nodes(session)
        print("[4] ingesting causal chain edges...")
        ingest_causal_chains(session)
        print("[5] ingesting yaml OpenAPI operations...")
        ingest_yaml_operations(session)
        print("[6] extracting procedure flows from docx...")
        ingest_docx_procedures(session)
        print_summary(session)
    driver.close()
    print("\nGraph database built.")

if __name__ == "__main__":
    main()
