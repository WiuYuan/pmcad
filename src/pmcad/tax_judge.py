import os
import json

def build_species_selection_prompt(name: str, hits: list, abstract: str) -> str:
    """
    构造 LLM 的 species 选择 prompt。
    """

    hits_text = "\n".join([
        f"- TAXID {h['id']}: {h['name']} (rank: {h.get('rank','')}, score: {h.get('score','')})"
        for h in hits
    ])

    return f"""
You are an expert in biological taxonomy identification.

Your task:
- Select **ONE best taxonomic match** (taxid) from the candidate species list.
- The choice must best match the meaning of the entity name as used in the abstract.
- If none of the candidates is appropriate, output "None".

Rules:
- Output ONLY a taxid from the list, or "None".
- No explanation.

ENTITY NAME:
"{name}"

ABSTRACT CONTEXT:
\"\"\"{abstract}\"\"\"

CANDIDATE TAXONOMY MATCHES:
{hits_text}

Your answer:
"""


def normalize_taxid(s: str):
    return s.strip().replace('"', '').replace("'", "")


def match_llm_output_to_taxid(llm_output: str, hits: list):
    """
    将 LLM 输出的 taxid 匹配到 hits 中。
    """
    out = normalize_taxid(llm_output)

    if out.lower() == "none":
        return None

    for h in hits:
        if str(h["id"]) == out:
            return h

    return None


def process_one_folder_judge_species(
    folder: str,
    input_name: str,
    output_name: str,
    llm=None
):
    """
    对 species_mapping 中的每个 species，用 LLM 从 hits 中挑选最正确的物种。
    """

    pmid = os.path.basename(folder)
    in_path = os.path.join(folder, input_name)
    out_path = os.path.join(folder, output_name)

    if not os.path.exists(in_path):
        return None, [
            {"type": "status", "name": f"skip pmid {pmid} (no file)"}
        ]

    # --- load json ---
    try:
        with open(in_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        return None, [
            {"type": "status", "name": f"load fail pmid {pmid}"},
            {"type": "error", "msg": str(e)}
        ]

    abstract = data.get("abstract", "")
    sp_list = data.get("species_mapping", [])

    total = 0
    correct = 0
    errors = 0

    # -----------------------------------------
    # 遍历 species
    # -----------------------------------------
    for item in sp_list:
        name = item.get("name", "")
        hits = item.get("hits", [])

        if not hits:
            item["llm_raw_output"] = None
            item["llm_best_match"] = None
            continue

        prompt = build_species_selection_prompt(name, hits, abstract)

        try:
            llm_out = llm.query(prompt).strip()
        except Exception as e:
            item["llm_raw_output"] = f"ERROR: {e}"
            item["llm_best_match"] = None
            errors += 1
            continue

        best = match_llm_output_to_taxid(llm_out, hits)

        item["llm_raw_output"] = llm_out
        item["llm_best_match"] = best

        if best is not None:
            correct += 1
        total += 1

    data["species_mapping"] = sp_list

    # --- write output ---
    try:
        with open(out_path, "w", encoding="utf-8") as fw:
            json.dump(data, fw, ensure_ascii=False, indent=2)
    except Exception as e:
        return None, [
            {"type": "status", "name": f"write fail pmid {pmid}"},
            {"type": "error", "msg": str(e)}
        ]

    return data, [
        {"type": "status", "name": f"ok pmid {pmid}"},
        {"type": "metric", "name": "judge_species", "correct": correct, "total": total},
        {"type": "metric", "name": "error", "correct": errors, "total": total},
    ]