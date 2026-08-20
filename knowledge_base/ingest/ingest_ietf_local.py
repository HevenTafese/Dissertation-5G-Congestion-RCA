#!/usr/bin/env python3
"""   Ingest IETF Local

Author: Heven Tafese

This is a one time ingester for the 13 IETF ccwg documents. It reads only
data/ietf_docs/*.txt,  and follows the
same chunk_text and chunk_id conventions as ingest.py.
"""
import hashlib
import json
import re
from pathlib import Path

import httpx

# knowledge_base/ingest/ -> repo root
ROOT        = Path(__file__).resolve().parent.parent.parent
DOCS_DIR    = ROOT / "data" / "ietf_docs"
MANIFEST    = DOCS_DIR / "manifest.json"
CHROMA_DIR  = str(ROOT / "data" / "chroma_db")
OLLAMA_BASE = "http://192.168.56.1:11434"
EMBED_MODEL = "nomic-embed-text"
COLLECTION  = "remedial"

# same as ingest.py's chunk_text, in words
CHUNK_SIZE    = 400
CHUNK_OVERLAP = 80

# matches "5.1.2. Title" etc, same numbered section pattern ingest_docx uses
HEADING_RE = re.compile(r"^\d+(\.\d+)*\.?\s+\S")


def chunk_text(text: str, size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list:
    """Identical to ingest.py's chunk_text: word based sliding window."""
    words = text.split()
    chunks = []
    start = 0
    while start < len(words):
        end = min(start + size, len(words))
        chunks.append(" ".join(words[start:end]))
        if end == len(words):
            break
        start += size - overlap
    return [c for c in chunks if len(c.strip()) > 80]


def chunk_id(source: str, chunk_text_str: str) -> str:
    """Same scheme as ingest.py's chunk_id, content addressed SHA256."""
    h = hashlib.sha256(f"{source}|{chunk_text_str}".encode()).hexdigest()
    return h[:24]


def clean_rfc_text(raw: str) -> str:
    """Strip page break form feeds and running [Page N] footers, which
    ingest_docx never has to deal with since Word docs don't have them."""
    text = raw.replace("\x0c", "\n")
    lines = [ln for ln in text.split("\n")
             if not re.search(r"\[Page\s+\d+\]\s*$", ln.strip())]
    return "\n".join(lines)


def split_into_heading_blocks(text: str) -> list:
    """Same approach as ingest_docx, adapted for plain text. A numbered
    line only counts as a heading if preceded by a blank line, otherwise
    an inline numbered list in body text gets misread as new headings."""
    lines = text.split("\n")
    blocks = []
    current_heading, current_text = "preamble", []
    # start of document counts as preceded by blank
    prev_line_blank = True
    for line in lines:
        stripped = line.strip()
        if not stripped:
            prev_line_blank = True
            continue
        is_heading_candidate = HEADING_RE.match(stripped) and len(stripped) < 120
        if is_heading_candidate and prev_line_blank:
            if current_text:
                blocks.append((current_heading, " ".join(current_text)))
            current_heading, current_text = stripped[:120], []
        else:
            current_text.append(stripped)
        prev_line_blank = False
    if current_text:
        blocks.append((current_heading, " ".join(current_text)))
    return [(h, b) for h, b in blocks if len(b.split()) >= 30]


def embed(text: str) -> list:
    r = httpx.post(f"{OLLAMA_BASE}/api/embeddings",
                    json={"model": EMBED_MODEL, "prompt": text}, timeout=60.0)
    r.raise_for_status()
    return r.json()["embedding"]


def check_ollama_ready() -> bool:
    """Same precheck pattern as ingest.py's main(): fail clearly before
    doing any work."""
    try:
        r = httpx.get(f"{OLLAMA_BASE}/api/tags", timeout=5.0)
        models = [m["name"] for m in r.json().get("models", [])]
        if not any("nomic-embed-text" in m for m in models):
            print("ERROR: nomic-embed-text not pulled in Ollama.")
            return False
        return True
    except Exception as e:
        print(f"ERROR: cannot reach Ollama at {OLLAMA_BASE}, {e}")
        return False


def get_collection():
    import chromadb
    client = chromadb.PersistentClient(path=CHROMA_DIR)
    return client.get_or_create_collection(COLLECTION)


def ingest_document(doc_id: str, meta: dict, existing_ids: set, collection) -> tuple:
    path = DOCS_DIR / f"{doc_id}.txt"
    if not path.exists():
        print(f"  [MISSING] {path.name} not found, run download_ietf_ccwg.py first")
        return 0, 0, 0

    raw = path.read_text(encoding="utf-8")
    cleaned = clean_rfc_text(raw)
    blocks = split_into_heading_blocks(cleaned)

    added, skipped, failed = 0, 0, 0
    for clause, body in blocks:
        for chunk in chunk_text(body):
            cid = chunk_id(doc_id, chunk)
            if cid in existing_ids:
                skipped += 1
                continue
            try:
                vec = embed(chunk)
            except Exception:
                failed += 1
                continue
            collection.add(
                ids=[cid], embeddings=[vec], documents=[chunk],
                metadatas=[{
                    "source": doc_id, "filename": path.name,
                    "clause": clause[:120], "store": "remedial",
                    "title": meta["title"], "source_url": meta["url"],
                    "doc_status": meta["doc_status"],
                    "also_known_as": meta.get("also_known_as", ""),
                }])
            existing_ids.add(cid)
            added += 1
    print(f"  {doc_id}: {added} added, {skipped} skipped, {failed} failed "
          f"({len(blocks)} section(s))")
    return added, skipped, failed


def main():
    if not check_ollama_ready():
        return
    if not MANIFEST.exists():
        print(f"ERROR: no manifest at {MANIFEST}. Run download_ietf_ccwg.py first.")
        return

    manifest = json.loads(MANIFEST.read_text())
    verified_docs = {k: v for k, v in manifest.items() if v.get("status") == "verified"}
    if not verified_docs:
        print("ERROR: manifest has no verified documents. Nothing to ingest.")
        return

    collection = get_collection()
    existing_ids = set(collection.get()["ids"])
    print(f"collection '{COLLECTION}': {len(existing_ids)} chunk(s) already present "
          f"(this includes your existing remedial papers, untouched)")
    print(f"{len(verified_docs)} verified IETF document(s) to process\n")

    total_added = total_skipped = total_failed = 0
    for doc_id, meta in verified_docs.items():
        a, s, f = ingest_document(doc_id, meta, existing_ids, collection)
        total_added += a
        total_skipped += s
        total_failed += f

    print("\n" + "=" * 70)
    print("INGESTION SUMMARY")
    print("=" * 70)
    print(f"New chunks added     : {total_added}")
    print(f"Already present      : {total_skipped}")
    print(f"Embed failures       : {total_failed}")
    print(f"Total in '{COLLECTION}' now: {collection.count()}")


if __name__ == "__main__":
    main()
