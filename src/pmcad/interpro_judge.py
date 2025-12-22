import os
import json


def build_interpro_selection_prompt(
    query_name: str, query_desc: str, hits: list
) -> str:
    """
    Ask the LLM to select the single most semantically relevant InterPro entry
    (domain / family / site / superfamily) for the given query.
    """

    hits_text = "\n".join(
        [
            f"- {h.get('id', 'NA')} | {h.get('name', 'NA')} | {h.get('description', 'NA')} | score={h.get('score', 'N/A')}"
            for h in hits
        ]
    )

    return f"""
You are an expert in protein domains and the InterPro database.

Below is a QUERY CONCEPT and its DESCRIPTION, followed by CANDIDATE InterPro ENTRIES.

Your task:
- Select the **single most semantically relevant InterPro entry** from the candidates.
- You MUST select one candidate if any are provided.
- Output "None" ONLY if the candidate list is empty.

SELECTION GUIDELINES:
- Exact semantic equivalence is NOT required.
- Structural, functional, or domain-level relevance is acceptable.
- Prefer entries that describe a **protein domain, family, site, or superfamily**
  that best matches the biological meaning of the query.
- If multiple candidates are plausible, choose the **most specific and informative** one.
- Ignore overly generic or weakly related entries if a more specific one exists.

OUTPUT FORMAT (STRICT):
- Output EXACTLY ONE STRING:
    - either an InterPro ID (e.g. IPR000504), which MUST be one of the candidate IDs listed below,
    - or the string "None" ONLY IF no candidates are provided.
- Do NOT output explanations, extra text, or quotes.

QUERY:
Name: "{query_name}"
Description: "{query_desc}"

CANDIDATE INTERPRO ENTRIES:
{hits_text}

Your answer:
"""


def normalize(s: str):
    return s.strip().lower().replace('"', "").replace("'", "")


def match_llm_output_to_hit(llm_output: str, hits: list):
    out = normalize(llm_output)

    if out == "none":
        return None

    for h in hits:
        interpro_id = h.get("id")
        if interpro_id and normalize(interpro_id) == out:
            return h

    # 再尝试按 name 匹配（兜底）
    for h in hits:
        name = h.get("name")
        if name and normalize(name) == out:
            return h

    return None


def process_one_folder_judge_interpro_id(
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

    interpro_list = data.get("interpro_map", [])

    # === 处理每个 mapping ===
    n_total = 0
    n_selected = 0
    for entry in interpro_list:

        query_name = entry.get("name", "")
        query_desc = entry.get("description", "")
        hits = entry.get("hits", [])

        if not hits or llm is None:
            entry["llm_raw_output"] = None
            entry["llm_best_match"] = None
            continue

        # ---- 构建 prompt ----
        prompt = build_interpro_selection_prompt(query_name, query_desc, hits)

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

    data["interpro_map"] = interpro_list

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
