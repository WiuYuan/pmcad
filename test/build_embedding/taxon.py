import json
import sys
import time
from pathlib import Path
from tqdm import tqdm
import torch

# ============================================================
# Path / imports（与你 SO 完全一致）
# ============================================================

PROJECT_ROOT = Path("/data/wyuan/workspace/pmcdata_pro/pmcad")
sys.path.insert(0, str(PROJECT_ROOT))

from sentence_transformers import SparseEncoder
from src.services.elasticsearch import (
    index_exists,
    add_index_cli,
    delete_index_cli,
    bulk_insert,
)

# ============================================================
# Config
# ============================================================

NAMES_DMP = "/data/wyuan/workspace/pmcdata_pro/data/ncbi/taxonomy/names.dmp"
ES_CONFIG = "/data/wyuan/workspace/pmcdata_pro/pmcad/config/elasticsearch.yaml"
INDEX = "taxon_index"

SPLADE_MODEL_PATH = (
    "/data/wyuan/.cache/huggingface/hub/"
    "models--NeuML--pubmedbert-base-splade/"
    "snapshots/f284fcbafe4761108f27357f5278d846a630059e"
)

# ===== device（修复点）=====
device = "cuda:1"

# ============================================================
# Mapping / Settings（BM25 + SPLADE）
# ============================================================

MAPPING = {
    "properties": {
        "id": {"type": "keyword"},
        "label": {"type": "text"},
        "text_all": {"type": "text"},
        "splade": {"type": "rank_features"},
    }
}

SETTINGS = {"index": {"mapping": {"total_fields": {"limit": 200000}}}}

# ============================================================
# Helpers
# ============================================================


def build_text_all(label, all_names):
    parts = []
    if label:
        parts.append(label)
    synonyms = [n for n in all_names if n != label]
    if synonyms:
        parts.append("Synonyms: " + "; ".join(synonyms))
    return "; ".join(parts) if parts else ""


def build_splade_batch(texts, model):
    """
    Batch SPLADE encode (GPU-friendly)
    """
    sparse_batch = model.encode(texts)
    results = []

    for sparse in sparse_batch:
        sparse = sparse.coalesce()
        idx = sparse.indices()[0].tolist()
        val = sparse.values().tolist()
        tokens = model.tokenizer.convert_ids_to_tokens(idx)

        out = {}
        for tok, w in zip(tokens, val):
            w = float(w)
            if w <= 0:
                continue
            if "." in tok:
                continue
            if not tok.strip():
                continue
            out[tok] = w

        results.append(out)

    return results


# ============================================================
# 1. recreate index
# ============================================================

if index_exists(ES_CONFIG, INDEX):
    delete_index_cli(ES_CONFIG, INDEX)

add_index_cli(
    ES_CONFIG,
    INDEX,
    mapping=MAPPING,
    settings=SETTINGS,
    description="NCBI Taxonomy (names.dmp) with SPLADE sparse vectors",
)

# ============================================================
# 2. load & parse names.dmp
# ============================================================

taxid_to_names = {}
taxid_to_scientific = {}

with open(NAMES_DMP, "r", encoding="utf-8") as f:
    for line in f:
        parts = [p.strip() for p in line.split("|")]
        if len(parts) < 4:
            continue
        taxid, name_txt, _, name_class = parts[:4]
        if not taxid or not name_txt:
            continue
        taxid_to_names.setdefault(taxid, []).append(name_txt)
        if name_class == "scientific name":
            taxid_to_scientific[taxid] = name_txt

taxa = []
for taxid, names in taxid_to_names.items():
    label = taxid_to_scientific.get(taxid, names[0])
    taxa.append({"taxid": taxid, "label": label, "names": list(dict.fromkeys(names))})

print(f"[INFO] taxonomy usable taxa: {len(taxa)}")

# ============================================================
# 3. load SPLADE model
# ============================================================

splade_model = SparseEncoder(
    model_name_or_path=SPLADE_MODEL_PATH, device=device, local_files_only=True
)

# ============================================================
# 4. indexing loop (batch SPLADE)
# ============================================================

ENCODE_BATCH = 64  # GPU sweet spot
BULK_SIZE = 200  # docs
bulk_lines = []
buffer = []
total_docs = 0

pbar = tqdm(taxa, desc="Indexing Taxonomy (SPLADE)")

for node in pbar:
    # cache text_all（避免重复计算）
    node["_text_all"] = build_text_all(node["label"], node["names"])
    buffer.append(node)

    if len(buffer) < ENCODE_BATCH:
        continue

    texts = [n["_text_all"] for n in buffer]
    splade_vecs = build_splade_batch(texts, splade_model)

    for n, splade_vec in zip(buffer, splade_vecs):
        bulk_lines.append(json.dumps({"index": {"_index": INDEX, "_id": n["taxid"]}}))
        bulk_lines.append(
            json.dumps(
                {
                    "id": n["taxid"],
                    "label": n["label"],
                    "text_all": n["_text_all"],
                    "splade": splade_vec,
                }
            )
        )
        total_docs += 1

    buffer.clear()

    if len(bulk_lines) >= BULK_SIZE * 2:
        bulk_insert(ES_CONFIG, bulk_lines, index_name=INDEX)
        bulk_lines = []
        time.sleep(0.2)

# ===== flush remaining buffer（修复点）=====
if buffer:
    texts = [n["_text_all"] for n in buffer]
    splade_vecs = build_splade_batch(texts, splade_model)

    for n, splade_vec in zip(buffer, splade_vecs):
        bulk_lines.append(json.dumps({"index": {"_index": INDEX, "_id": n["taxid"]}}))
        bulk_lines.append(
            json.dumps(
                {
                    "id": n["taxid"],
                    "label": n["label"],
                    "text_all": n["_text_all"],
                    "splade": splade_vec,
                }
            )
        )
        total_docs += 1

    buffer.clear()

# final bulk flush
if bulk_lines:
    bulk_insert(ES_CONFIG, bulk_lines, index_name=INDEX)

print(f"✅ DONE: inserted {total_docs} taxonomy documents")
print("[DONE] Taxonomy indexed with SPLADE sparse vectors")
