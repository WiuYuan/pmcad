import os
import json

def build_ro_selection_prompt(query_name: str, query_desc: str, hits: list) -> str:
    """
    要求 LLM 从 hits 中选出“语义上最正确（最符合 query_name/query_desc 的 RO）”。

    LLM 输出格式要求严格：
      - 必须输出一个 RO ID（与 hits 中的 id 完全一致）
      - 或输出 "None"（仅当没有候选）
    """

    hits_text = "\n".join(
        [
            f"- {h.get('id', 'NA')} | {h.get('name', 'NA')} | {h.get('description', 'NA')} | score={h.get('score', 'N/A')}"
            for h in hits
        ]
    )

    return f"""
You are an expert in Relation Ontology (RO).

Below is a QUERY RELATION and its DESCRIPTION, followed by CANDIDATE RO RELATIONS.

Your task:
- Select the **single most semantically appropriate RO relation** for the query.
- You MUST select one candidate if any are provided.
- Only output "None" if the candidate list is empty.

SELECTION CRITERIA:
- Prefer semantic equivalence or closest relational meaning.
- Directionality and relational intent matter (e.g. causes vs affects).
- If multiple candidates are plausible, choose the most specific one.

OUTPUT FORMAT:
- ONLY output EXACTLY ONE STRING:
    - either a RO ID (for example: RO:0002326), which MUST be one of the candidate IDs listed below,
    - or the string "None" ONLY IF no candidates are provided.
- Do NOT output explanations, extra text, or quotes.

QUERY:
Name: "{query_name}"
Description: "{query_desc}"

CANDIDATE RO RELATIONS:
{hits_text}

Your answer:
"""
def normalize(s: str):
    return s.strip().lower().replace('"', '').replace("'", "")


def match_llm_output_to_hit(llm_output: str, hits: list):
    """
    将 LLM 输出与 hits 中的 RO ID 或 name 做匹配。

    允许两种合法输出：
      1. RO:xxxxxxx（推荐）
      2. 候选 name（兜底）

    匹配失败 → 返回 None
    """
    out = normalize(llm_output)

    if out == "none":
        return None

    # 先按 RO_ID 匹配
    for h in hits:
        ro_id = h.get("id")
        if ro_id and normalize(ro_id) == out:
            return h

    # 再按 name 匹配（兜底）
    for h in hits:
        name = h.get("name")
        if name and normalize(name) == out:
            return h

    return None

def process_one_folder_judge_ro_id(
    folder: str,
    input_name: str,
    output_name: str,
    llm=None
):
    pmid = os.path.basename(folder)
    path = os.path.join(folder, input_name)

    if not os.path.exists(path):
        return None, [{"type": "status", "name": f"skip pmid {pmid} (no file)"}]

    # === 读取 JSON ===
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        return None, [
            {"type": "status", "name": f"load fail pmid {pmid}"},
            {"type": "error", "msg": str(e)},
        ]

    ro_list = data.get("ro_mapping", [])

    # === 处理每个 RO mapping ===
    n_total = 0
    n_selected = 0

    for entry in ro_list:
        query_name = entry.get("name", "")
        query_desc = entry.get("description", "")
        hits = entry.get("hits", [])

        if not hits or llm is None:
            entry["llm_raw_output"] = None
            entry["llm_best_match"] = None
            continue

        prompt = build_ro_selection_prompt(query_name, query_desc, hits)

        try:
            llm_output = llm.query(prompt).strip()
        except Exception as e:
            llm_output = f"ERROR: {e}"

        best_hit = match_llm_output_to_hit(llm_output, hits)

        entry["llm_raw_output"] = llm_output
        entry["llm_best_match"] = best_hit

        n_total += 1
        if best_hit is not None:
            n_selected += 1

    data["ro_mapping"] = ro_list

    # === 写回 JSON ===
    out_path = os.path.join(folder, output_name)
    try:
        with open(out_path, "w", encoding="utf-8") as fw:
            json.dump(data, fw, ensure_ascii=False, indent=2)
    except Exception as e:
        return None, [
            {"type": "status", "name": f"write fail pmid {pmid}"},
            {"type": "error", "msg": str(e)},
        ]

    return data, [
        {"type": "status", "name": f"ok pmid {pmid}"},
        {"type": "metric", "correct": n_selected, "total": n_total},
    ]