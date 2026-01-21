from src.services.elasticsearch import search_via_curl


def search_taxon(
    config_path,
    query,
    index_name="taxon_index",
    k=20,
    verbose=False,
):
    """
    Exact token matching + name length scoring
    Adds: taxid 去重，只保留同一个 taxid 的最高分记录
    """

    # ============================================================
    # 0. Tokenize query
    # ============================================================
    q_tokens = [t for t in query.lower().split() if t]
    if not q_tokens:
        return []

    # ============================================================
    # 1. Elasticsearch recall
    # ============================================================
    body = {
        "size": k,
        "query": {
            "script_score": {
                "query": {
                    "bool": {
                        "should": [{"term": {"tokens": t}} for t in q_tokens],
                        "minimum_should_match": 1,
                    }
                },
                "script": {
                    "source": """
int matched = 0;
for (t in params.q_tokens) {
  if (doc['tokens'].contains(t)) {
    matched += 1;
  }
}
return matched * 100 - doc['ntokens'].value;
""",
                    "params": {"q_tokens": q_tokens},
                },
            }
        },
        "_source": ["id", "name", "ntokens", "text_all"],
    }

    hits = search_via_curl(config_path, index_name, body)
    if not hits:
        return []

    # ============================================================
    # 2. Build items
    # ============================================================
    items = []
    for h in hits:
        src = h["_source"]
        items.append(
            {
                "id": src["id"],
                "name": src["name"],
                "score": h["_score"],
                "ntokens": src["ntokens"],
                "text_all": src["text_all"],
            }
        )

    # ============================================================
    # 3. Sort by score descending
    # ============================================================
    items = sorted(items, key=lambda x: x["score"], reverse=True)

    # ============================================================
    # 3.5 —— NEW: taxid 去重（只保留每个 taxid 的最高分记录）
    # ============================================================
    unique = {}
    for it in items:
        tid = it["id"]
        if tid not in unique:
            unique[tid] = it  # 因为 items 已经按 score 降序，所以第一个是最佳
    items = list(unique.values())

    # 再次按 score 排序（保持一致性）
    items = sorted(items, key=lambda x: x["score"], reverse=True)

    # 截取 top-k
    items = items[:k]

    # ============================================================
    # 3.6 —— Normalize score (score / max_score)
    # ============================================================
    if items:
        max_score = items[0]["score"]  # 因为已按 score 降序
        if max_score > 0:
            for it in items:
                it["score"] = it["score"] / max_score
        else:
            for it in items:
                it["score"] = 0.0

    # ============================================================
    # 4. Assign rank
    # ============================================================
    for i, it in enumerate(items, start=1):
        it["rank"] = i

    # ============================================================
    # 5. Pretty print
    # ============================================================
    if verbose:
        print("=== TAXON EXACT SEARCH (Unique taxid + Ranked) ===")
        name_width = max(40, max(len(it["name"]) for it in items))
        for it in items:
            print(
                f"{it['id']:8s} | "
                f"{it['name']:<{name_width}s} | "
                f"score={it['score']:.2f}"
            )

    # ============================================================
    # 6. Return result
    # ============================================================
    return [
        {
            "rank": it["rank"],
            "id": it["id"],
            "name": it["name"],
            "text_all": it["text_all"],
            "score": it["score"],
        }
        for it in items
    ]
