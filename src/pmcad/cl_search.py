from src.services.llm import LLM
from src.services.elasticsearch import search_via_curl

from src.services.elasticsearch import search_via_curl


def search_cl(
    config_path,
    dense_model,
    splade_model,
    query,
    index_name="cl_index",
    k=10,
    vec_topn=200,
    w_dense=0.5,
    w_splade=0.5,
    verbose=True,
):
    """
    CL Hybrid search (Dense recall + SPLADE rerank)
    完全对齐 search_dense_knn 的工程风格
    """

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
    N = min(30, len(items))
    label_width = max(40, max(len(it["label"]) for it in items[:N]))

    if verbose:
        print("=== CL HYBRID SEARCH (Dense + SPLADE) ===")
        for it in items:
            print(
                f"{it['id']:12s} | "
                f"{it['label']:<{label_width}s} | "
                f"dense={it['dense']:.4f} | "
                f"splade={it['splade']:.4f} | "
                f"final={it['final']:.4f}"
            )

    return items
