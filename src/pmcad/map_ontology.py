from src.services.llm import LLM
from src.services.elasticsearch import search_via_curl
import os
import json

def search_ontology(
    config_path,
    query,
    index_name,
    type="dense+splade",
    k=10,
    vec_topn=200,
    w_dense=0.5,
    w_splade=0.5,
    dense_model=None,
    splade_model=None,
    verbose=True,
):
    if type == "dense+splade":
        # ============================================================
        # 1. Dense Recall (KNN)
        # ============================================================
        qvec_dense = dense_model.encode(query, normalize_embeddings=True).tolist()

        knn_body = {
            "size": vec_topn,
            "knn": {
                "field": "vector",
                "query_vector": qvec_dense,
                "k": vec_topn,
                "num_candidates": max(vec_topn * 3, 1000),
            },
            "_source": ["id", "label", "text_all", "splade"],
        }

        hits_knn = search_via_curl(config_path, index_name, knn_body)
        if not hits_knn:
            return []

        # ============================================================
        # 2. Build SPLADE query vector
        # ============================================================
        sparse_vec = splade_model.encode([query])[0].coalesce()
        idx = sparse_vec.indices()[0].tolist()
        val = sparse_vec.values().tolist()
        tokens = splade_model.tokenizer.convert_ids_to_tokens(idx)

        q_splade = {tok: float(v) for tok, v in zip(tokens, val) if float(v) > 0}

        # ============================================================
        # 3. Build candidate list
        # ============================================================
        items = []
        for h in hits_knn:
            src = h["_source"]
            items.append(
                {
                    "id": src["id"],
                    "label": src["label"],
                    "text_all": src.get("text_all", ""),
                    "dense": h["_score"],
                    "splade": 0.0,
                    "doc_splade": src.get("splade", {}),
                    "final": 0.0,
                }
            )

        # ============================================================
        # 4. SPLADE dot-product reranking
        # ============================================================
        for it in items:
            score = 0.0
            doc_spl = it["doc_splade"]
            for tok, wq in q_splade.items():
                wd = doc_spl.get(tok, 0.0)
                if wd > 0:
                    score += wq * wd
            it["splade"] = score

        # ============================================================
        # 5. Normalize + fuse
        # ============================================================
        max_dense = max(it["dense"] for it in items) or 1e-9
        max_splade = max(it["splade"] for it in items) or 1e-9

        for it in items:
            it["final"] = w_dense * (it["dense"] / max_dense) + w_splade * (
                it["splade"] / max_splade
            )

        # ============================================================
        # 6. Final ranking
        # ============================================================
        items = sorted(items, key=lambda x: x["final"], reverse=True)[:k]
        label_width = max(40, max(len(it["label"]) for it in items[:min(30, len(items))]))

        if verbose:
            print("=== HYBRID SEARCH (Dense + SPLADE) ===")
            for it in items:
                print(
                    f"{it['id']:12s} | "
                    f"{it['label']:<{label_width}s} | "
                    f"dense={it['dense']:.4f} | "
                    f"splade={it['splade']:.4f} | "
                    f"final={it['final']:.4f}"
                )

        return items



def process_one_folder_get_db_id(
    folder: str,
    input_name: str,
    output_name: str,
    search_func,
):
    """
    输入 JSON:
      - pmid
      - abstract
      - relations (sentence → rel_from_this_sent)

    输出 JSON（仅）:
      - pmid
      - abstract
      - diease_mapping
    """

    pmid = os.path.basename(folder)
    in_path = os.path.join(folder, input_name)
    out_path = os.path.join(folder, output_name)

    # ---------------------------
    # 加载输入 JSON
    # ---------------------------
    try:
        with open(in_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return None, [
            {"type": "status", "name": f"pmid:{pmid} (load error)"},
            {"type": "metric", "correct": 0, "total": 0},
        ]

    abstract = data.get("abstract")
    relations = data.get("relations", [])

    # ---------------------------
    # 收集所有 cl（去重）
    # ---------------------------
    cl_items = {}  # (name, desc) -> None

    def collect_cl_from_entity(ent):
        if not isinstance(ent, dict):
            return
        if ent.get("type") == "cell_type":
            name = ent.get("name")
            desc = ent.get("description", "")
            if name:
                cl_items[(name, desc)] = None

        # 扫 meta
        for m in ent.get("meta", []):
            if m.get("type") == "cell_type":
                name = m.get("name")
                desc = m.get("description", "")
                if name:
                    cl_items[(name, desc)] = None

    for sent_block in relations:
        for rel in sent_block.get("rel_from_this_sent", []):
            for field in ("components", "target", "context"):
                for ent in rel.get(field, []):
                    collect_cl_from_entity(ent)

    # ---------------------------
    # 如果没有 cl
    # ---------------------------
    if not cl_items:
        out = {"pmid": pmid, "abstract": abstract, "cl_map": []}
        try:
            with open(out_path, "w", encoding="utf-8") as fw:
                json.dump(out, fw, ensure_ascii=False, indent=2)
        except Exception:
            pass

        return out, [
            {"type": "status", "name": f"pmid:{pmid} (no cl)"},
            {"type": "metric", "correct": 0, "total": 0},
        ]

    # ---------------------------
    # cl hybrid search
    # ---------------------------
    cl_map = []
    judge = False

    for name, desc in cl_items.keys():
        query = f"{name}, {desc}" if desc else name

        try:
            items = search_func(query)
        except Exception:
            items = []

        hits = []
        for rank, it in enumerate(items, start=1):
            hits.append(
                {
                    "id": it.get("id"),
                    "name": it.get("label"),
                    "description": it.get("text_all"),
                    "score": round(float(it.get("final", 0.0)), 4),
                    "rank": rank,
                }
            )

        if hits:
            judge = True

        cl_map.append(
            {
                "name": name,
                "description": desc,
                "hits": hits,
            }
        )

    # ---------------------------
    # 写最终输出（只保留 3 个 key）
    # ---------------------------
    out = {
        "pmid": pmid,
        "abstract": abstract,
        "cl_map": cl_map,
    }

    try:
        with open(out_path, "w", encoding="utf-8") as fw:
            json.dump(out, fw, ensure_ascii=False, indent=2)
    except Exception:
        return None, [
            {"type": "status", "name": f"pmid:{pmid} (write error)"},
            {"type": "metric", "correct": 0, "total": 1},
        ]

    # ---------------------------
    # tqdm 统计信息
    # ---------------------------
    info = [
        {"type": "status", "name": f"pmid:{pmid}"},
        {"type": "metric", "correct": 1 if judge else 0, "total": 1},
    ]

    return out, info
