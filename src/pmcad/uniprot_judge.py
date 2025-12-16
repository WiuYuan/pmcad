import os
import json

def build_uniprot_selection_prompt(original_name: str, species: str, entity_type: str, hits: list, abstract: str) -> str:
    """
    构建让 LLM 选择最正确 UniProt accession 的 prompt。
    """
    hits_text = "\n".join([
        f"- {h['id']}: {h.get('description', '')}"
        for h in hits
    ])

    return f"""
You are an expert in UniProt protein selection.

Below is a biological ENTITY and its CANDIDATE UniProt entries.

Your task:
- Select **ONE best UniProt accession** from the candidate list.
- The correct choice must match the entity's biological meaning, name, and species.
- If none of the candidates is correct, output "None".

RULES:
- Reason based on entity name, species, and protein/RNA type.
- Prefer canonical entries over fragments or unrelated proteins.
- If multiple entries represent the same protein, choose the canonical reviewed (Swiss-Prot) one when possible.

OUTPUT FORMAT:
- ONLY output ONE accession string exactly from the candidate list, OR output "None".
No explanations.

ENTITY:
Name: "{original_name}"
Species: "{species}"
Type: "{entity_type}"

ABSTRACT:
\"\"\"{abstract}\"\"\"

CANDIDATE UniProt ENTRIES:
{hits_text}

Your answer:
"""


def normalize_uniprot(s: str):
    return s.strip().upper().replace('"', '').replace("'", "")


def match_llm_output_to_uniprot(llm_output: str, hits: list):
    """
    匹配 LLM 返回的 accession 到 hits。
    """
    out = normalize_uniprot(llm_output)

    if out == "NONE":
        return None

    for h in hits:
        if normalize_uniprot(h["id"]) in out:
            return h

    return None


def process_one_folder_judge_uniprot_id(
    folder: str,
    input_name="ds_go_uniprotapi.json",
    output_name="ds_go_uniprotapi.json",
    llm=None
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
    uni_list = data.get("uniprot_mapping", [])

    total = 0
    correct = 0
    total_errors = 0

    for entry in uni_list:
        original_name = entry.get("name", "")
        species = entry.get("species", "")
        entity_type = entry.get("entity_type", "")
        hits = entry.get("hits", [])

        if not hits:
            entry["llm_best_match"] = None
            continue

        prompt = build_uniprot_selection_prompt(original_name, species, entity_type, hits, abstract)

        try:
            llm_output = llm.query(prompt).strip()
        except Exception as e:
            llm_output = f"ERROR: {e}"
            total_errors += 1
            entry["llm_raw_output"] = llm_output
            entry["llm_best_match"] = None
            continue

        # ---- 匹配 accession ----
        best_hit = match_llm_output_to_uniprot(llm_output, hits)

        entry["llm_raw_output"] = llm_output
        entry["llm_best_match"] = best_hit

        if best_hit is not None:
            correct += 1
        total += 1

    data["uniprot_match"] = uni_list

    out_path = os.path.join(folder, output_name)
    with open(out_path, "w", encoding="utf-8") as fw:
        json.dump(data, fw, ensure_ascii=False, indent=2)

    return data, [
        {"type": "status", "name": f"ok pmid {pmid}"},
        {"type": "metric", "name": "judge", "correct": correct, "total": total},
        {"type": "metric", "name": "error", "correct": total_errors, "total": total},
    ]