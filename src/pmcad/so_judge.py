import os
import json

def build_so_selection_prompt(query_name: str, query_desc: str, hits: list) -> str:
    """
    要求 LLM 从 hits 中选出 “语义上最正确（最符合 query_name/query_desc 的 SO）” 的那个。

    LLM 输出格式要求严格：必须输出一个 SO 名称（与 hits 中收到的 name 完全一致）
    或输出 "None" 表示没有一个是正确的。
    """

    hits_text = "\n".join(
        [
            f"- {h.get('id', 'NA')} | {h.get('name', 'NA')} | {h.get('description', 'NA')} | score={h.get('score', 'N/A')}"
            for h in hits
        ]
    )

    return f"""
You are an expert in Sequence Ontology (SO).

Below is a QUERY SO TERM and its DESCRIPTION, followed by CANDIDATE SO TERMS.

Your task:
- Select the **single most relevant SO term** from the candidates with respect to the query.
- You MUST select one candidate if any are provided.
- Only output "None" if the candidate list is empty.

SELECTION CRITERIA:
- Choose the SO term that is most closely related in meaning to the query.
- Exact semantic equivalence is NOT required.
- Structural, functional, or attribute-level relevance is acceptable.
- If multiple candidates are plausible, choose the most specific one.

OUTPUT FORMAT:
- ONLY output EXACTLY ONE STRING:
    - either a SO ID (for example: SO:0000167), which MUST be one of the candidate IDs listed below,
    - or the string "None" ONLY IF no candidates are provided.
- Do NOT output explanations, extra text, or quotes.

QUERY:
Name: "{query_name}"
Description: "{query_desc}"

CANDIDATE SO TERMS:
{hits_text}

Your answer:
"""
def normalize(s: str):
    return s.strip().lower().replace('"', '').replace("'", "")

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

def process_one_folder_judge_so_id(folder: str, input_name: str, output_name: str, llm=None):

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

    so_list = data.get("so_mapping", [])

    # === 处理每个 mapping ===
    n_total = 0
    n_selected = 0
    for entry in so_list:

        query_name = entry.get("name", "")
        query_desc = entry.get("description", "")
        hits = entry.get("hits", [])

        if not hits or llm is None:
            entry["llm_raw_output"] = None
            entry["llm_best_match"] = None
            continue

        # ---- 构建 prompt ----
        prompt = build_so_selection_prompt(query_name, query_desc, hits)

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

    data["so_mapping"] = so_list

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
        {"type": "metric", "correct": n_selected, "total": n_total}
    ]