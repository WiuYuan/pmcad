import os
import json

def process_one_folder_get_species_id(
    folder: str,
    input_name: str,
    output_name: str,
    search_func,
):
    """
    species grounding pipeline:

    对每个 species：
    - name 搜一次
    - description 搜一次
    - 按交叉方式合并：
         n1, d1, n2, d2, ...
    - 剩余项追加
    - rank 按交叉后的顺序编号
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
            {"type": "metric", "correct": 0, "total": 1},
        ]

    abstract = data.get("abstract")
    relations = data.get("relations", [])

    # ---------------------------
    # 收集 species（name + description）
    # ---------------------------
    species_dict = {}   # key → description

    def add_species(name, desc):
        if not name:
            return
        # 如果没描述或新描述更好（你可改策略）
        if name not in species_dict or (species_dict[name] is None and desc):
            species_dict[name] = desc

    def collect_species(ent):
        if not isinstance(ent, dict):
            return

        if ent.get("type") == "species":
            nm = ent.get("name")
            desc = ent.get("description")
            add_species(nm, desc)
            if desc:
                add_species(desc, desc)

        for m in ent.get("meta", []):
            if m.get("type") == "species":
                nm = m.get("name")
                desc = m.get("description")
                add_species(nm, desc)
                if desc:
                    add_species(desc, desc)

    for block in relations:
        for rel in block.get("rel_from_this_sent", []):
            for field in ("components", "target", "context"):
                for ent in rel.get(field, []):
                    collect_species(ent)

    species_list = sorted(species_dict.keys())

    if not species_list:
        out = {
            "pmid": pmid,
            "abstract": abstract,
            "species_mapping": []
        }
        with open(out_path, "w", encoding="utf-8") as fw:
            json.dump(out, fw, ensure_ascii=False, indent=2)
        return out, [
            {"type": "status", "name": f"{pmid} (no species)"},
            {"type": "metric", "correct": 0, "total": 1},
        ]

    # ---------------------------
    # 搜索（name + desc），交叉合并
    # ---------------------------
    species_mapping = []
    judge = False

    for sp in species_list:
        desc = species_dict[sp]

        # --- 搜 name ---
        try:
            hits_name = search_func(sp)
        except Exception:
            hits_name = []

        # --- 搜 description ---
        try:
            hits_desc = search_func(desc) if desc else []
        except Exception:
            hits_desc = []

        if hits_name or hits_desc:
            judge = True

        # ---------------------------
        # 交叉合并 hits_name 和 hits_desc
        # ---------------------------
        merged = []
        i = j = 0

        while i < len(hits_name) or j < len(hits_desc):
            # 先放 name(i)
            if i < len(hits_name):
                merged.append({
                    "id":   hits_name[i].get("taxid"),
                    "name": hits_name[i].get("name"),
                    "score": float(hits_name[i].get("score", 0)),
                })
                i += 1

            # 再放 desc(j)
            if j < len(hits_desc):
                merged.append({
                    "id":   hits_desc[j].get("taxid"),
                    "name": hits_desc[j].get("name"),
                    "score": float(hits_desc[j].get("score", 0)),
                })
                j += 1

        # 给 merged hits 重新 rank（按交叉后的顺序）
        for idx, h in enumerate(merged, start=1):
            h["rank"] = idx

        species_mapping.append({
            "name": sp,
            "description": desc,
            "hits": merged,
        })

    # ---------------------------
    # 写文件
    # ---------------------------
    out = {
        "pmid": pmid,
        "abstract": abstract,
        "species_mapping": species_mapping,
    }

    with open(out_path, "w", encoding="utf-8") as fw:
        json.dump(out, fw, ensure_ascii=False, indent=2)

    return out, [
        {"type": "status", "name": pmid},
        {"type": "metric", "correct": 1 if judge else 0, "total": 1},
    ]