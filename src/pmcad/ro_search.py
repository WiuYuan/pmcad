from src.services.elasticsearch import search_via_curl

def search_ro(
    config_path,
    dense_model,
    query,
    index_name="ro_relations",
    k=10,
    vec_topn=200,
    verbose=True,
):
    """
    RO Hybrid search (目前仅 Dense KNN，结构完全对齐 SO search)
    """

    # ============================================================
    # 1. Dense Recall (KNN)
    # ============================================================
    qvec_dense = dense_model.encode(
        query,
        normalize_embeddings=True
    ).tolist()

    knn_body = {
        "size": vec_topn,
        "knn": {
            "field": "embedding",
            "query_vector": qvec_dense,
            "k": vec_topn,
            "num_candidates": max(vec_topn * 3, 1000),
        },
        "_source": [
            "relation_id",
            "label",
            "desc",
        ]
    }

    hits_knn = search_via_curl(config_path, index_name, knn_body)
    if not hits_knn:
        return []

    # ============================================================
    # 2. Build candidate list（对齐 SO schema）
    # ============================================================
    items = []
    for h in hits_knn:
        src = h["_source"]
        label = src.get("label", "")
        desc = src.get("desc", "")

        items.append({
            "ro_id": src.get("relation_id"),
            "label": label,
            "definition": desc,
            "text_all": f"{label}. {desc}".strip(),
            "dense": h["_score"],
            "splade": 0.0,
            "doc_splade": {},
            "final": 0.0,
        })

    # ============================================================
    # 3. Normalize + fuse（即使只有 dense，也保持一致）
    # ============================================================
    max_dense = max(it["dense"] for it in items) or 1e-9

    for it in items:
        it["final"] = it["dense"] / max_dense

    # ============================================================
    # 4. Final ranking
    # ============================================================
    items = sorted(items, key=lambda x: x["final"], reverse=True)[:k]

    if verbose:
        print("=== RO SEARCH (Dense, SO-compatible schema) ===")
        for it in items:
            print(
                f"{it['ro_id']:12s} | "
                f"{it['label']:<30s} | "
                f"dense={it['dense']:.4f} | "
                f"final={it['final']:.4f}"
            )

    return items