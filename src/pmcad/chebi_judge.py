import os
import json


def build_chebi_selection_prompt(original_name: str, hits: list, abstract: str) -> str:
    """
    构建让 LLM 选择最正确 ChEBI ID 的 prompt。
    """

    hits_text = "\n".join([f"- {h['id']}: {h.get('description', '')}" for h in hits])

    return f"""
You are an expert in chemical entity normalization using ChEBI.

Below is a CHEMICAL ENTITY mentioned in a biomedical context,
followed by CANDIDATE ChEBI entries.

Your task:
- Select **ONE best ChEBI ID** from the candidate list.
- The correct choice must match the chemical meaning of the entity.
- If none of the candidates is correct, output "None".

RULES:
- Reason based on the chemical name and the biological context.
- Prefer exact chemical entities over classes if applicable.
- If multiple candidates are plausible, choose the most specific one.

OUTPUT FORMAT:
- ONLY output ONE ChEBI ID exactly from the candidate list, OR output "None".
No explanations.

CHEMICAL ENTITY:
Name: "{original_name}"

ABSTRACT:
\"\"\"{abstract}\"\"\"

CANDIDATE ChEBI ENTRIES:
{hits_text}

Your answer:
"""


def normalize_chebi(s: str):
    return s.strip().upper().replace('"', "").replace("'", "")


def match_llm_output_to_chebi(llm_output: str, hits: list):
    """
    匹配 LLM 返回的 ChEBI ID 到 hits。
    """
    out = normalize_chebi(llm_output)

    if out == "NONE":
        return None

    for h in hits:
        if normalize_chebi(h["id"]) in out:
            return h

    return None


def process_one_folder_judge_chebi_id(
    folder: str,
    input_name: str,
    output_name: str,
    llm=None,
):
    pmid = os.path.basename(folder)
    path = os.path.join(folder, input_name)

    if not os.path.exists(path):
        return None, [{"type": "status", "name": f"skip pmid {pmid} (no file)"}]

    # === load JSON ===
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        return None, [
            {"type": "status", "name": f"load fail pmid {pmid}"},
            {"type": "error", "msg": str(e)},
        ]

    abstract = data.get("abstract", "")
    chebi_list = data.get("chebi_map", [])

    total = 0
    correct = 0
    total_errors = 0

    for entry in chebi_list:
        original_name = entry.get("name", "")
        hits = entry.get("hits", [])

        if not hits:
            entry["llm_best_match"] = None
            continue

        prompt = build_chebi_selection_prompt(
            original_name=original_name,
            hits=hits,
            abstract=abstract,
        )

        try:
            llm_output = llm.query(prompt).strip()
        except Exception as e:
            llm_output = f"ERROR: {e}"
            total_errors += 1
            entry["llm_raw_output"] = llm_output
            entry["llm_best_match"] = None
            continue

        best_hit = match_llm_output_to_chebi(llm_output, hits)

        entry["llm_raw_output"] = llm_output
        entry["llm_best_match"] = best_hit

        if best_hit is not None:
            correct += 1
        total += 1

    data["chebi_map"] = chebi_list

    out_path = os.path.join(folder, output_name)
    with open(out_path, "w", encoding="utf-8") as fw:
        json.dump(data, fw, ensure_ascii=False, indent=2)

    return data, [
        {"type": "status", "name": f"ok pmid {pmid}"},
        {"type": "metric", "name": "judge", "correct": correct, "total": total},
        {"type": "metric", "name": "error", "correct": total_errors, "total": total},
    ]
