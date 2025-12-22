import os
import json


def build_uberon_selection_prompt(query_name: str, query_desc: str, hits: list) -> str:
    """
    Ask the LLM to select the single most semantically relevant UBERON anatomical term
    for the given query.
    """

    hits_text = "\n".join(
        [
            f"- {h.get('id', 'NA')} | {h.get('name', 'NA')} | {h.get('description', 'NA')} | score={h.get('score', 'N/A')}"
            for h in hits
        ]
    )

    return f"""
You are an expert in anatomy and the UBERON ontology.

Below is a QUERY ANATOMICAL CONCEPT and its DESCRIPTION, followed by CANDIDATE UBERON TERMS.

Your task:
- Select the **single most relevant UBERON anatomical term** from the candidates.
- You MUST select one candidate if any are provided.
- Output "None" ONLY if the candidate list is empty.

SELECTION CRITERIA:
- Choose the UBERON term that best matches the anatomical meaning of the query.
- Exact semantic equivalence is NOT required.
- Developmental, structural, or anatomical relevance is acceptable.
- Prefer canonical anatomical structures over vague or overly generic terms.
- If multiple candidates are plausible, choose the **most specific and informative** one.

OUTPUT FORMAT (STRICT):
- ONLY output EXACTLY ONE STRING:
    - either a UBERON ID (for example: UBERON:0000955), which MUST be one of the candidate IDs listed below,
    - or the string "None" ONLY IF no candidates are provided.
- Do NOT output explanations, extra text, or quotes.

QUERY:
Name: "{query_name}"
Description: "{query_desc}"

CANDIDATE UBERON TERMS:
{hits_text}

Your answer:
"""


def normalize(s: str):
    return s.strip().lower().replace('"', "").replace("'", "")


def match_llm_output_to_hit(llm_output: str, hits: list):
    """
    将 LLM 输出与 hits 中的 SO ID 或 name 做匹配。

    现在允许两种合法输出：
      1. "SO:0006413" 这种 uberon_ID（推荐）
      2. 候选 name（作为兜底，虽然 prompt 要求输出 ID）

    如果匹配失败 → 返回 None
    """
    out = normalize(llm_output)

    if out == "none":
        return None

    # 先尝试按 uberon_ID 匹配
    for h in hits:
        uberon_id = h.get("id")
        if uberon_id and normalize(uberon_id) == out:
            return h

    # 再尝试按 name 匹配（兜底）
    for h in hits:
        name = h.get("name")
        if name and normalize(name) == out:
            return h

    return None


def process_one_folder_judge_uberon_id(
    folder: str, input_name: str, output_name: str, llm=None
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

    uberon_list = data.get("uberon_map", [])

    # === 处理每个 mapping ===
    n_total = 0
    n_selected = 0
    for entry in uberon_list:

        query_name = entry.get("name", "")
        query_desc = entry.get("description", "")
        hits = entry.get("hits", [])

        if not hits or llm is None:
            entry["llm_raw_output"] = None
            entry["llm_best_match"] = None
            continue

        # ---- 构建 prompt ----
        prompt = build_uberon_selection_prompt(query_name, query_desc, hits)

        try:
            llm_output = llm.query(prompt).strip()
        except Exception as e:
            llm_output = f"ERROR: {e}"

        # ---- 匹配 LLM 输出 ----
        best_hit = match_llm_output_to_hit(llm_output, hits)

        entry["llm_raw_output"] = llm_output
        entry["llm_best_match"] = best_hit  # 若失败则为 None
        n_total += 1
        if best_hit is not None:
            n_selected += 1

    data["uberon_map"] = uberon_list

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
