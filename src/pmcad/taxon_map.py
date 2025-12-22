import os
import json


def process_one_folder_get_taxon_id(
    folder: str,
    input_name: str,
    output_name: str,
    search_func,
):
    """
    species grounding pipeline (SIMPLIFIED):

    对每个 species：
    - 构造一个 unified query（name + description）
    - 只搜索一次
    - 按搜索结果顺序直接给 rank
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
            {"type": "status", "name": f"{pmid} (load error)"},
            {"type": "metric", "correct": 0, "total": 0},
        ]

    abstract = data.get("abstract")
    relations = data.get("relations", [])

    # ---------------------------
    # 收集 species（name + description）
    # ---------------------------
    species_dict = {}  # name → description

    def add_species(name, desc):
        if not name:
            return
        if name not in species_dict or (species_dict[name] is None and desc):
            species_dict[name] = desc

    def collect_species(ent):
        if not isinstance(ent, dict):
            return

        if ent.get("type") == "species":
            add_species(ent.get("name"), ent.get("description"))

        for m in ent.get("meta", []):
            if m.get("type") == "species":
                add_species(m.get("name"), m.get("description"))

    for block in relations:
        for rel in block.get("rel_from_this_sent", []):
            for field in ("components", "target", "context"):
                for ent in rel.get(field, []):
                    collect_species(ent)

    species_list = sorted(species_dict.keys())

    if not species_list:
        out = {"pmid": pmid, "abstract": abstract, "taxon_map": []}
        with open(out_path, "w", encoding="utf-8") as fw:
            json.dump(out, fw, ensure_ascii=False, indent=2)
        return out, [
            {"type": "status", "name": f"{pmid} (no species)"},
            {"type": "metric", "correct": 0, "total": 0},
        ]

    # ---------------------------
    # 搜索（unified query）
    # ---------------------------
    taxon_map = []
    judge = False

    for sp in species_list:
        desc = species_dict.get(sp)

        # --- unified query ---
        if desc:
            query = f"{sp} {desc}"
        else:
            query = sp

        try:
            hits = search_func(query)
        except Exception:
            hits = []

        if hits:
            judge = True

        # 直接按搜索结果顺序给 rank
        merged = []
        for idx, h in enumerate(hits, start=1):
            merged.append(
                {
                    "id": h.get("id"),
                    "name": h.get("name"),
                    "description": h.get("text_all"),
                    "score": round(float(h.get("score", 0.0)), 4),
                    "rank": idx,
                }
            )

        taxon_map.append(
            {
                "name": sp,
                "description": desc,
                "query": query,
                "hits": merged,
            }
        )

    # ---------------------------
    # 写文件
    # ---------------------------
    out = {
        "pmid": pmid,
        "abstract": abstract,
        "taxon_map": taxon_map,
    }

    with open(out_path, "w", encoding="utf-8") as fw:
        json.dump(out, fw, ensure_ascii=False, indent=2)

    return out, [
        {"type": "status", "name": pmid},
        {"type": "metric", "correct": 1 if judge else 0, "total": 1},
    ]
