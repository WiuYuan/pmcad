import os
import json

def process_one_folder_get_so_id(
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
      - so_mapping
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
            {"type": "metric", "correct": 0, "total": 1},
        ]

    abstract = data.get("abstract")
    relations = data.get("relations", [])

    # ---------------------------
    # 收集所有 so（去重）
    # ---------------------------
    so_items = {}  # (name, desc) -> None

    def collect_so_from_entity(ent):
        if not isinstance(ent, dict):
            return
        if ent.get("type") == "SO":
            name = ent.get("name")
            desc = ent.get("description", "")
            if name:
                so_items[(name, desc)] = None

        # 扫 meta
        for m in ent.get("meta", []):
            if m.get("type") == "SO":
                name = m.get("name")
                desc = m.get("description", "")
                if name:
                    so_items[(name, desc)] = None

    for sent_block in relations:
        for rel in sent_block.get("rel_from_this_sent", []):
            for field in ("components", "target", "context"):
                for ent in rel.get(field, []):
                    collect_so_from_entity(ent)

    # ---------------------------
    # 如果没有 so
    # ---------------------------
    if not so_items:
        out = {
            "pmid": pmid,
            "abstract": abstract,
            "so_mapping": []
        }
        try:
            with open(out_path, "w", encoding="utf-8") as fw:
                json.dump(out, fw, ensure_ascii=False, indent=2)
        except Exception:
            pass

        return out, [
            {"type": "status", "name": f"pmid:{pmid} (no so)"},
            {"type": "metric", "correct": 0, "total": 1},
        ]

    # ---------------------------
    # so hybrid search
    # ---------------------------
    so_mapping = []
    judge = False

    for (name, desc) in so_items.keys():
        query = f"{name}, {desc}" if desc else name

        try:
            items = search_func(query)
        except Exception:
            items = []

        hits = []
        for rank, it in enumerate(items, start=1):
            hits.append({
                "id": it.get("so_id"),
                "name": it.get("label"),
                "description": it.get("text_all"),
                "score": round(float(it.get("final", 0.0)), 4),
                "rank": rank,
            })

        if hits:
            judge = True

        so_mapping.append({
            "name": name,
            "description": desc,
            "hits": hits,
        })

    # ---------------------------
    # 写最终输出（只保留 3 个 key）
    # ---------------------------
    out = {
        "pmid": pmid,
        "abstract": abstract,
        "so_mapping": so_mapping,
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