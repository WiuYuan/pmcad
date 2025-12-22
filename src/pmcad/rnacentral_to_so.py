import os
import json
def collect_unresolved_rna(ds_rnacentral):
    """
    返回需要转 SO 的 RNA 实体列表
    """
    unresolved = []

    for it in ds_rnacentral.get("rnacentral_map", []):
        if it.get("llm_best_match") is None:
            unresolved.append(it)

    return unresolved

def build_so_selection_prompt(query_name: str, query_desc: str, abstract: str, hits: list) -> str:
    """
    要求 LLM 从 hits 中选出一个【语义上精确等价（exact match）】的 SO term。
    如果不存在精确等价项，必须输出 None。
    """

    hits_text = "\n".join(
        [
            f"- {h.get('id', 'NA')} | {h.get('name', 'NA')} | {h.get('description', 'NA')} | score={h.get('score', 'N/A')}"
            for h in hits
        ]
    )

    return f"""
You are an expert in Sequence Ontology (SO).

Below is a QUERY TERM that was originally extracted from text and may have been
incorrectly typed as an RNA entity. Your task is to determine whether this term
corresponds to an EXACT Sequence Ontology (SO) concept.

Your task:
- Select ONE SO term from the candidates ONLY IF it is a clear semantic match
  to the query term.
- A valid match should: Refer to the same biological concept
- If no reasonable match exists, output "None".

OUTPUT FORMAT:
- Output EXACTLY ONE of the following:
    * A SO ID (e.g., SO:0000167) that appears in the candidate list
    * OR the string "None"
- Do NOT output explanations, additional text, or quotes.

ABSTRACTL:
{abstract}

QUERY:
Name: "{query_name}"
Description: "{query_desc}"

CANDIDATE SO TERMS:
{hits_text}

Your answer:
"""

def normalize(s: str):
    return s.strip().lower().replace('"', "").replace("'", "")


def match_llm_output_to_hit(llm_output: str, hits: list):
    """
    将 LLM 输出与 hits 中的 SO ID 或 name 做匹配。

    现在允许两种合法输出：
      1. "SO:0006413" 这种 SO_ID（推荐）
      2. 候选 name（作为兜底，虽然 prompt 要求输出 ID）

    如果匹配失败 → 返回 None
    """
    out = normalize(llm_output)

    if out == "none":
        return None

    # 先尝试按 SO_ID 匹配
    for h in hits:
        so_id = h.get("id")
        if so_id and normalize(so_id) == out:
            return h

    # 再尝试按 name 匹配（兜底）
    for h in hits:
        name = h.get("name")
        if name and normalize(name) == out:
            return h

    return None


def process_rnacentral_failed_rna_to_so(
    folder,
    ds_json_name,
    ds_rnacentral_name,
    output_name,
    so_search_func,
    llm,
):
    pmid = os.path.basename(folder)

    ds_path = os.path.join(folder, ds_json_name)
    rna_path = os.path.join(folder, ds_rnacentral_name)
    out_path = os.path.join(folder, output_name)

    # ---------------------------
    # 加载 ds.json
    # ---------------------------
    try:
        with open(ds_path, "r", encoding="utf-8") as f:
            ds = json.load(f)
    except Exception:
        return None, [
            {"type": "status", "name": f"pmid:{pmid} (load ds error)"},
            {"type": "metric", "correct": 0, "total": 0},
        ]

    # ---------------------------
    # 加载 ds_rnacentral.json
    # ---------------------------
    try:
        with open(rna_path, "r", encoding="utf-8") as f:
            ds_rna = json.load(f)
    except Exception:
        return None, [
            {"type": "status", "name": f"pmid:{pmid} (load rnacentral error)"},
            {"type": "metric", "correct": 0, "total": 0},
        ]

    abstract = ds.get("abstract", "")

    # ---------------------------
    # 收集 RNAcentral 失败的 RNA
    # ---------------------------
    unresolved = collect_unresolved_rna(ds_rna)

    # 👉 没有需要转 SO 的 RNA：直接 skip
    if not unresolved:
        out = {
            "pmid": pmid,
            "abstract": abstract,
            "so_map": [],
        }
        try:
            with open(out_path, "w", encoding="utf-8") as fw:
                json.dump(out, fw, ensure_ascii=False, indent=2)
        except Exception:
            pass

        return out, [
            {"type": "status", "name": f"pmid:{pmid} (skip no unresolved RNA)"},
            {"type": "metric", "correct": 0, "total": 0},
        ]

    # ---------------------------
    # SO search + judge
    # ---------------------------
    so_map = []
    n_total = 0
    n_correct = 0

    for rna in unresolved:
        name = rna.get("name")
        if not name:
            continue

        n_total += 1

        # ---- SO search ----
        try:
            hits_raw = so_search_func(name)
        except Exception:
            hits_raw = []

        hits = []
        for rank, it in enumerate(hits_raw, start=1):
            hits.append({
                "id": it.get("id"),
                "name": it.get("label"),
                "description": it.get("text_all"),
                "score": round(float(it.get("final", 0.0)), 4),
                "rank": rank,
            })

        entry = {
            "name": name,
            "description": "",
            "hits": hits,
        }

        # ---- SO judge（exact match）----
        if hits and llm is not None:
            prompt = build_so_selection_prompt(name, "", abstract, hits)
            try:
                llm_output = llm.query(prompt)
            except Exception as e:
                llm_output = f"ERROR: {e}"

            best = match_llm_output_to_hit(llm_output, hits)

            entry["llm_raw_output"] = llm_output
            entry["llm_best_match"] = best

            if best is not None:
                n_correct += 1
        else:
            entry["llm_best_match"] = None

        so_map.append(entry)

    # ---------------------------
    # 写输出
    # ---------------------------
    out = {
        "pmid": pmid,
        "abstract": abstract,
        "so_map": so_map,
    }

    try:
        with open(out_path, "w", encoding="utf-8") as fw:
            json.dump(out, fw, ensure_ascii=False, indent=2)
    except Exception:
        return None, [
            {"type": "status", "name": f"pmid:{pmid} (write error)"},
            {"type": "metric", "correct": 0, "total": n_total},
        ]

    # ---------------------------
    # tqdm / parallel 统计信息
    # ---------------------------
    info = [
        {"type": "status", "name": f"pmid:{pmid}"},
        {"type": "metric", "correct": n_correct, "total": n_total},
    ]

    return out, info

def merge_rnacentral_to_so_and_cleanup(
    folder,
    ds_json_name="ds.json",
    ds_so_name="ds_so.json",
    ds_rnacentral_to_so_name="ds_rnacentral_so.json",
    ds_rnacentral_name="ds_rnacentral.json",
):
    pmid = os.path.basename(folder)

    p_ds = os.path.join(folder, ds_json_name)
    p_so = os.path.join(folder, ds_so_name)
    p_rna_so = os.path.join(folder, ds_rnacentral_to_so_name)
    p_rna = os.path.join(folder, ds_rnacentral_name)

    # ---------------------------
    # 必要文件检查
    # ---------------------------
    if not os.path.exists(p_rna_so):
        return None, [
            {"type": "status", "name": f"pmid:{pmid} (skip no rnacentral_to_so)"},
            {"type": "metric", "correct": 0, "total": 0},
        ]

    try:
        with open(p_ds, "r", encoding="utf-8") as f:
            ds = json.load(f)
        with open(p_so, "r", encoding="utf-8") as f:
            ds_so = json.load(f)
        with open(p_rna_so, "r", encoding="utf-8") as f:
            ds_rna_so = json.load(f)
    except Exception as e:
        return None, [
            {"type": "status", "name": f"pmid:{pmid} (load error)"},
            {"type": "error", "msg": str(e)},
        ]

    # ---------------------------
    # 1️⃣ 收集 RNA → SO 成功映射
    # ---------------------------
    # name -> SO best match
    rna_to_so = {}
    for it in ds_rna_so.get("so_map", []):
        best = it.get("llm_best_match")
        if best and it.get("name"):
            rna_to_so[it["name"]] = best

    if not rna_to_so:
        return None, [
            {"type": "status", "name": f"pmid:{pmid} (skip no successful RNA→SO)"},
            {"type": "metric", "correct": 0, "total": 0},
        ]

    # ---------------------------
    # 2️⃣ 合并进 ds_so.json（原样复制 entry，保留 hits / rank）
    # ---------------------------
    n_added_so = 0

    existing_pairs = {
        (it.get("name"), it.get("llm_best_match", {}).get("id"))
        for it in ds_so.get("so_map", [])
    }

    for entry in ds_rna_so.get("so_map", []):
        best = entry.get("llm_best_match")
        if not best:
            continue

        key = (entry.get("name"), best.get("id"))
        if key in existing_pairs:
            continue

        ds_so.setdefault("so_map", []).append(
            json.loads(json.dumps(entry))
        )
        n_added_so += 1

    # ---------------------------
    # 3️⃣ 修改 ds.json 中的实体类型
    # ---------------------------
    n_retyped = 0

    def relabel_entity(ent):
        nonlocal n_retyped
        if not isinstance(ent, dict):
            return
        if ent.get("type") == "RNA":
            name = ent.get("name")
            if name in rna_to_so:
                ent["type"] = "SO"
                # 可选：补 description
                if not ent.get("description"):
                    ent["description"] = rna_to_so[name].get("description")
                n_retyped += 1

        # meta 递归
        for m in ent.get("meta", []):
            relabel_entity(m)

    for blk in ds.get("relations", []):
        for rel in blk.get("rel_from_this_sent", []):
            for field in ("components", "targets", "contexts"):
                for ent in rel.get(field, []):
                    relabel_entity(ent)

    # ---------------------------
    # 4️⃣ 写回 ds.json / ds_so.json
    # ---------------------------
    try:
        with open(p_ds, "w", encoding="utf-8") as f:
            json.dump(ds, f, ensure_ascii=False, indent=2)
        with open(p_so, "w", encoding="utf-8") as f:
            json.dump(ds_so, f, ensure_ascii=False, indent=2)
    except Exception as e:
        return None, [
            {"type": "status", "name": f"pmid:{pmid} (write error)"},
            {"type": "error", "msg": str(e)},
        ]

    # ---------------------------
    # 5️⃣ 清理 ds_rnacentral.json：只删除已成功转 SO 的条目
    # ---------------------------
    n_removed_rna = 0

    if os.path.exists(p_rna):
        try:
            with open(p_rna, "r", encoding="utf-8") as f:
                ds_rna = json.load(f)

            # 只移除“在 rnacentral_to_so 中成功判为 SO”的 name
            converted_names = {
                it.get("name")
                for it in ds_rna_so.get("so_map", [])
                if it.get("name") and it.get("llm_best_match") is not None
            }

            old_list = ds_rna.get("rnacentral_map", []) or []
            new_list = [it for it in old_list if it.get("name") not in converted_names]

            n_removed_rna = len(old_list) - len(new_list)
            ds_rna["rnacentral_map"] = new_list

            with open(p_rna, "w", encoding="utf-8") as f:
                json.dump(ds_rna, f, ensure_ascii=False, indent=2)

        except Exception:
            # 清理失败不影响主流程
            pass
        
    try:
        os.remove(p_rna_so)
    except Exception:
        pass

    # ---------------------------
    # 返回统计信息
    # ---------------------------
    info = [
        {"type": "status", "name": f"pmid:{pmid}"},
        {"type": "metric", "name": "added_so", "correct": n_added_so, "total": len(rna_to_so)},
        {"type": "metric", "name": "retyped_entities", "correct": n_retyped, "total": n_retyped},
        {"type": "metric", "name": "removed_from_rnacentral", "correct": n_removed_rna, "total": len(rna_to_so)},
    ]

    return None, info