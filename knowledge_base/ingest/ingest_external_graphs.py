#!/usr/bin/env python3
"""  Ingest External Graphs

Author: Heven Tafese

This ingests O-RAN architecture documents, vendor white papers, and AI and
explainability papers into a fourth ChromaDB collection, external_graphs.

"""
import re, hashlib, time, sys, requests
from pathlib import Path
import chromadb

ROOT          = Path(__file__).resolve().parent.parent.parent
EXTERNAL_DIR  = ROOT / "data" / "external_graphs" / "add"
CHROMA_DIR    = ROOT / "data" / "chroma_db"
OLLAMA_URL    = "http://192.168.56.1:11434/api/embeddings"
EMBED_MODEL   = "nomic-embed-text"
CHUNK_SIZE    = 300
CHUNK_OVERLAP = 50
MAX_WORDS     = 380

DOC_METADATA = {
    "O-RAN.WG1.OAM-Architecture-v04.00": {
        "doc_type": "architecture",
        "contributes": "O-RAN OAM architecture, NF management flows, fault supervision",
        "store_role": "external_graphs"
    },
    "O-RAN.WG1.TS.OAD-R005-v17.00": {
        "doc_type": "architecture",
        "contributes": "O-RAN operations and maintenance architecture, NF lifecycle",
        "store_role": "external_graphs"
    },
    "ai-agents-in-the-telecommunication-network": {
        "doc_type": "paper",
        "contributes": "AI agent architecture in telecom, multi-agent coordination patterns",
        "store_role": "external_graphs"
    },
    "explainable-ai-how-humans-can-trust-ai_whitepaper": {
        "doc_type": "whitepaper",
        "contributes": "Explainability principles, human trust in AI decisions, XAI methods",
        "store_role": "external_graphs"
    }
}

def get_collection():
    client = chromadb.PersistentClient(path=str(CHROMA_DIR))
    return client.get_or_create_collection(
        name="external_graphs",
        metadata={"description": "Architecture diagrams, protocol flows, AI/explainability papers"}
    )

def embed(text, retries=3):
    for attempt in range(retries):
        try:
            r = requests.post(OLLAMA_URL, json={"model": EMBED_MODEL, "prompt": text}, timeout=90)
            r.raise_for_status()
            return r.json()["embedding"]
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2)
            else:
                raise

def chunk_text(text):
    words = text.split()
    if not words: return []
    chunks, start = [], 0
    while start < len(words):
        end = min(start + CHUNK_SIZE, len(words))
        chunk = " ".join(words[start:end])
        if len(chunk.split()) > MAX_WORDS:
            w = chunk.split()
            for s in range(0, len(w), MAX_WORDS - 20):
                sub = " ".join(w[s:s+MAX_WORDS])
                if len(sub.strip()) > 80: chunks.append(sub)
        else:
            if len(chunk.strip()) > 80: chunks.append(chunk)
        if end == len(words): break
        start += CHUNK_SIZE - CHUNK_OVERLAP
    return chunks

def chunk_id(source, text):
    return hashlib.sha256(f"{source}|{text}".encode()).hexdigest()[:24]

def get_doc_meta(filename):
    stem = Path(filename).stem.strip()
    for key, meta in DOC_METADATA.items():
        if key.lower() in stem.lower():
            return meta
    return {"doc_type": "document", "contributes": "general reference", "store_role": "external_graphs"}

def ingest_pdf(path, collection, existing_ids):
    import fitz
    doc   = fitz.open(str(path))
    title = path.stem[:80].strip()
    body  = re.sub(r"\s+", " ", " ".join(p.get_text() for p in doc)).strip()
    meta  = get_doc_meta(path.name)
    added, skipped, failed = 0, 0, 0
    for chunk in chunk_text(body):
        cid = chunk_id(title, chunk)
        if cid in existing_ids: skipped += 1; continue
        try: vec = embed(chunk)
        except Exception: failed += 1; continue
        collection.add(ids=[cid], embeddings=[vec], documents=[chunk],
            metadatas=[{"source": title, "filename": path.name,
                        "doc_type": meta["doc_type"],
                        "contributes": meta["contributes"],
                        "store": "external_graphs"}])
        existing_ids.add(cid)
        added += 1
    print(f"  {title[:60]}: {added} added, {skipped} skipped, {failed} failed")

def ingest_docx(path, collection, existing_ids):
    from docx import Document
    doc   = Document(path)
    title = path.stem[:80].strip()
    meta  = get_doc_meta(path.name)
    heading_re = re.compile(r"^\d+(\.\d+)*\s+\S")
    blocks, current_heading, current_text = [], "preamble", []
    for para in doc.paragraphs:
        txt = para.text.strip()
        if not txt: continue
        is_heading = para.style.name.startswith("Heading") or heading_re.match(txt)
        if is_heading:
            if current_text: blocks.append((current_heading, " ".join(current_text)))
            current_heading, current_text = txt[:120], []
        else:
            current_text.append(txt)
    if current_text: blocks.append((current_heading, " ".join(current_text)))
    added, skipped, failed = 0, 0, 0
    for clause, body in blocks:
        if len(body.split()) < 30: continue
        for chunk in chunk_text(body):
            cid = chunk_id(f"{title}|{clause}", chunk)
            if cid in existing_ids: skipped += 1; continue
            try: vec = embed(chunk)
            except Exception: failed += 1; continue
            collection.add(ids=[cid], embeddings=[vec], documents=[chunk],
                metadatas=[{"source": title, "filename": path.name,
                            "clause": clause[:120],
                            "doc_type": meta["doc_type"],
                            "contributes": meta["contributes"],
                            "store": "external_graphs"}])
            existing_ids.add(cid)
            added += 1
    print(f"  {title[:60]}: {added} added, {skipped} skipped, {failed} failed")

def main():
    try:
        r = requests.get("http://192.168.56.1:11434/api/tags", timeout=5)
        models = [m["name"] for m in r.json().get("models", [])]
        if not any("nomic-embed-text" in m for m in models):
            print("ERROR: nomic-embed-text not in Ollama."); sys.exit(1)
        print("Ollama OK.")
    except Exception as e:
        print(f"ERROR: Ollama unreachable, {e}"); sys.exit(1)
    col = get_collection()
    existing_ids = set(col.get()["ids"])
    print(f"external_graphs: {len(existing_ids)} chunks already present\n")
    print("=== ingesting PDFs ===")
    for f in sorted(EXTERNAL_DIR.glob("*.pdf")):
        print(f"processing {f.name} ...")
        ingest_pdf(f, col, existing_ids)
    print("\n=== ingesting docx ===")
    for f in sorted(EXTERNAL_DIR.glob("*.docx")):
        print(f"processing {f.name} ...")
        ingest_docx(f, col, existing_ids)
    print(f"\n=== done ===")
    print(f"external_graphs: {col.count()} chunks total")

if __name__ == "__main__":
    main()
