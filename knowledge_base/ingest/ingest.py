#!/usr/bin/env python3
"""Ingest

Author: Heven Tafese

This is a robust clause aware ingester. Handles any file size, oversized
paragraphs, and Ollama context limits. Idempotent, uses content hash IDs,
safe to rerun.
"""
import os, sys, hashlib, re, time, requests
from pathlib import Path
import chromadb

ROOT        = Path(__file__).resolve().parent.parent.parent
SPECS_DIR   = ROOT / "data" / "specs" / "add"
PAPERS_DIR  = ROOT / "data" / "papers"
CHROMA_DIR  = ROOT / "data" / "chroma_db"
OLLAMA_URL  = "http://192.168.56.1:11434/api/embeddings"
EMBED_MODEL = "nomic-embed-text"

# nomic-embed-text has 2048 token context, reduce chunks well under that target words 
CHUNK_SIZE     = 300
CHUNK_OVERLAP  = 50
# anything larger than this is split before embedding
MAX_WORDS_HARD = 400

def get_collections():
    client = chromadb.PersistentClient(path=str(CHROMA_DIR))
    normative = client.get_or_create_collection(name="normative")
    remedial  = client.get_or_create_collection(name="remedial")
    return normative, remedial

def embed(text, retries=2):
    for attempt in range(retries + 1):
        try:
            resp = requests.post(OLLAMA_URL, json={"model": EMBED_MODEL, "prompt": text}, timeout=90)
            resp.raise_for_status()
            return resp.json()["embedding"]
        except Exception as e:
            if attempt < retries:
                time.sleep(1)
                continue
            raise

def chunk_text(text):
    words = text.split()
    if not words: return []
    chunks, start = [], 0
    while start < len(words):
        end = min(start + CHUNK_SIZE, len(words))
        chunks.append(" ".join(words[start:end]))
        if end == len(words): break
        start += CHUNK_SIZE - CHUNK_OVERLAP
    # further split any chunk exceeding hard limit
    final = []
    for c in chunks:
        if len(c.split()) <= MAX_WORDS_HARD:
            final.append(c)
        else:
            w = c.split()
            for s in range(0, len(w), MAX_WORDS_HARD - 20):
                final.append(" ".join(w[s:s + MAX_WORDS_HARD]))
    return [c for c in final if len(c.strip()) > 80]

def chunk_id(source, chunk_text_str):
    """Content based hash. Same text = same id, always."""
    h = hashlib.sha256(f"{source}|{chunk_text_str}".encode()).hexdigest()
    return h[:24]

def ingest_docx(path, collection, existing_ids):
    from docx import Document
    doc  = Document(path)
    stem = path.stem
    raw  = stem.split("-")[0]
    spec_id = f"TS {raw[:2]}.{raw[2:]}" if len(raw)==5 else stem
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
            doc_id = chunk_id(spec_id, chunk)
            if doc_id in existing_ids:
                skipped += 1
                continue
            try:
                vec = embed(chunk)
            except Exception as e:
                failed += 1
                continue
            collection.add(ids=[doc_id], embeddings=[vec], documents=[chunk],
                metadatas=[{"source": spec_id, "filename": path.name,
                            "clause": clause[:120], "store": "normative"}])
            existing_ids.add(doc_id)
            added += 1
    print(f"  {spec_id}: {added} added, {skipped} skipped, {failed} failed")
    return added

def ingest_pdf(path, collection, existing_ids):
    import fitz
    doc   = fitz.open(str(path))
    title = path.stem[:80]
    body  = re.sub(r"\s+", " ", " ".join(p.get_text() for p in doc)).strip()
    added, skipped, failed = 0, 0, 0
    for chunk in chunk_text(body):
        doc_id = chunk_id(title, chunk)
        if doc_id in existing_ids:
            skipped += 1
            continue
        try:
            vec = embed(chunk)
        except Exception as e:
            failed += 1
            continue
        collection.add(ids=[doc_id], embeddings=[vec], documents=[chunk],
            metadatas=[{"source": title, "filename": path.name, "store": "remedial"}])
        existing_ids.add(doc_id)
        added += 1
    print(f"  {title}: {added} added, {skipped} skipped, {failed} failed")
    return added

def main():
    try:
        r = requests.get("http://192.168.56.1:11434/api/tags", timeout=5)
        models = [m["name"] for m in r.json().get("models", [])]
        if not any("nomic-embed-text" in m for m in models):
            print("ERROR: nomic-embed-text not in Ollama."); sys.exit(1)
        print("Ollama OK.")
    except Exception as e:
        print(f"ERROR: cannot reach Ollama, {e}"); sys.exit(1)

    normative, remedial = get_collections()
    norm_ids = set(normative.get()["ids"])
    rem_ids  = set(remedial.get()["ids"])
    print(f"Starting: normative={len(norm_ids)}, remedial={len(rem_ids)}\n")

    print("=== ingesting specs (normative) ===")
    for f in sorted(SPECS_DIR.glob("*.docx")):
        print(f"processing {f.name} ...")
        ingest_docx(f, normative, norm_ids)

    print("\n=== ingesting papers (remedial) ===")
    for f in sorted(PAPERS_DIR.glob("*.pdf")):
        print(f"processing {f.name} ...")
        ingest_pdf(f, remedial, rem_ids)

    print(f"\n=== done ===")
    print(f"normative: {normative.count()}")
    print(f"remedial:  {remedial.count()}")

if __name__ == "__main__":
    main()
