import os
import json

def build_rnacentral_selection_prompt(
    original_name: str,
    species: str,
    hits: list,
    abstract: str,
) -> str:
    """
    构建让 LLM 选择 RNAcentral ID 的 prompt（严格精确匹配）。
    """

    hits_text = "\n".join([f"- {h['id']}: {h.get('description', '')}" for h in hits])

    return f"""
You are an expert in RNA annotation and RNAcentral identifiers.

Below is a QUERY RNA ENTITY extracted from text, and a list of CANDIDATE
RNAcentral entries.

Your task:
- Select ONE RNAcentral ID **ONLY IF** it is an **EXACT semantic match**
  to the query RNA entity.
- If there is NO exact match, you MUST output "None".

IMPORTANT — DEFINITION OF EXACT MATCH:
An exact match requires ALL of the following:
1. The candidate represents the SAME biological entity as the query,
   not a subclass, member, example, or variant.
2. The level of specificity must be the SAME:
   - A generic class (e.g. "small nuclear RNA", "snRNA", "lncRNA")
     must NOT be matched to a specific RNA molecule (e.g. U1, U2, U6).
   - A specific RNA (e.g. "U1 snRNA", "RNU1-1") must NOT be matched
     to a generic class.
3. The RNA type must match exactly (snRNA vs snoRNA vs lncRNA, etc.).
4. Species must be consistent if species information is provided.

DO NOT SELECT a candidate if it is:
- A specific member of an RNA family when the query is a family/class.
- A broader or narrower concept than the query.
- A related but different RNA.

If the query refers to an RNA CLASS or CATEGORY rather than a specific RNA,
you should almost always output "None".

OUTPUT FORMAT:
- Output EXACTLY ONE of the following:
  - A RNAcentral ID from the candidate list
  - OR the string "None"
- Do NOT output explanations, extra text, or quotes.

QUERY RNA ENTITY:
Name: "{original_name}"
Species: "{species}"

ABSTRACT CONTEXT:
\"\"\"{abstract}\"\"\"

CANDIDATE RNAcentral ENTRIES:
{hits_text}

Your answer:
"""

def normalize_rnacentral(s: str):
    return s.strip().upper().replace('"', "").replace("'", "")


def match_llm_output_to_rnacentral(llm_output: str, hits: list):
    """
    匹配 LLM 返回的 RNAcentral ID 到 hits。
    """
    out = normalize_rnacentral(llm_output)

    if out == "NONE":
        return None

    for h in hits:
        if normalize_rnacentral(h["id"]) in out:
            return h

    return None


def process_one_folder_judge_rnacentral_id(
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
    rna_list = data.get("rnacentral_map", [])

    total = 0
    correct = 0
    total_errors = 0

    for entry in rna_list:
        original_name = entry.get("name", "")
        species = entry.get("species", "")
        hits = entry.get("hits", [])

        if not hits:
            entry["llm_best_match"] = None
            continue

        prompt = build_rnacentral_selection_prompt(
            original_name,
            species,
            hits,
            abstract,
        )

        try:
            llm_output = llm.query(prompt).strip()
        except Exception as e:
            llm_output = f"ERROR: {e}"
            total_errors += 1
            entry["llm_raw_output"] = llm_output
            entry["llm_best_match"] = None
            continue

        best_hit = match_llm_output_to_rnacentral(llm_output, hits)

        entry["llm_raw_output"] = llm_output
        entry["llm_best_match"] = best_hit

        if best_hit is not None:
            correct += 1
        total += 1

    data["rnacentral_map"] = rna_list

    out_path = os.path.join(folder, output_name)
    with open(out_path, "w", encoding="utf-8") as fw:
        json.dump(data, fw, ensure_ascii=False, indent=2)

    return data, [
        {"type": "status", "name": f"ok pmid {pmid}"},
        {"type": "metric", "name": "judge", "correct": correct, "total": total},
        {"type": "metric", "name": "error", "correct": total_errors, "total": total},
    ]
