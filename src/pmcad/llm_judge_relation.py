import os
import json


def build_relation_validation_prompt(abstract: str, relation: dict, judge_method: str) -> str:
    """
    给单条 relation 构建验证 prompt。
    LLM 必须回答 Yes 或 No。
    """
    rel_json = json.dumps(relation, ensure_ascii=False, indent=2)

    if judge_method == "loose":
        return f"""
You are a biomedical relation validator.

Your task:
Determine whether the following extracted relation is SEMANTICALLY SUPPORTED
by the abstract, even if the direction, role (e.g., substrate vs product),
or specificity is imperfect.

Rules:
- Answer Yes if the abstract clearly discusses a biological process or mechanism
  that involves BOTH the component(s) and the target(s), even if:
    - the relation direction is reversed,
    - the role (substrate/product/regulator) is loosely stated,
    - the relation is an abstraction of the described mechanism.
- Answer No only if the relation is unrelated, fabricated, or contradicts the abstract.
- Do NOT require exact wording matches.
- Generic biological roles are acceptable
  if the abstract clearly refers to such activity.
- Output only one word: Yes or No.

=== ABSTRACT ===
\"\"\"{abstract}\"\"\"

=== RELATION ===
{rel_json}

Output: Yes or No
"""
    if judge_method == "strict":

        return f"""
You are a biomedical relation validator.

Your task:
Determine whether the following extracted relation is TRUE based ONLY on the abstract.

Rules:
- Answer Yes if the relation can be reasonably inferred from the abstract.
- Logical abstraction and weakening are allowed (the relation may be less specific or less strong than the wording in the abstract).
- Output only one word: Yes or No.

=== ABSTRACT ===
\"\"\"{abstract}\"\"\"

=== RELATION ===
{rel_json}

Output: Yes or No
"""

def process_one_folder(
    folder: str, input_name: str, output_name: str, llm, skip_existing=True, judge_method="strict",
):
    """
    Validates nested relations (relations → rel_from_this_sent list).
    - For each relation in each sentence, if no 'valid' field, query LLM to verify.
    - Keeps all data structure identical; only adds 'valid': True/False.
    """
    pmid = os.path.basename(folder)
    in_path = os.path.join(folder, input_name)
    out_path = os.path.join(folder, output_name)

    # === 1. Load input JSON ===
    try:
        with open(in_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        return None, [
            {"type": "error", "msg": f"load fail pmid: {pmid}"},
            {"type": "metric", "correct": 0, "total": 1},
        ]

    abstract = data.get("abstract", "")
    relations = data.get("relations", [])
    if not isinstance(relations, list):
        relations = []

    # === 2. Count relations to process ===
    total = 0
    correct = 0
    for sent in relations:
        rel_list = sent.get("rel_from_this_sent", [])
        total += len(rel_list)
        for rel in rel_list:
            if rel.get("valid") is True:
                correct += 1

    if skip_existing and total == correct and total > 0:
        return None, [
            {"type": "error", "msg": f"skip pmid: {pmid} (already validated)"},
            {"type": "metric", "correct": correct, "total": total},
        ]

    # === 3. Validate missing ones ===
    for sent in relations:
        rel_list = sent.get("rel_from_this_sent", [])
        for rel in rel_list:
            if skip_existing and "valid" in rel:
                continue

            prompt = build_relation_validation_prompt(abstract, rel, judge_method=judge_method)
            try:
                resp = llm.query(prompt).strip()
                is_valid = resp.lower().startswith("y")
            except Exception as e:
                is_valid = False
                rel["validation_error"] = str(e)

            rel["valid"] = is_valid

    # === 4. Write back output ===
    data["relations"] = relations
    try:
        with open(out_path, "w", encoding="utf-8") as fw:
            json.dump(data, fw, ensure_ascii=False, indent=2)
    except Exception as e:
        return None, [
            {"type": "error", "msg": f"write fail pmid: {pmid}"},
            {"type": "metric", "correct": correct, "total": total},
        ]

    # Recount
    total = sum(len(s.get("rel_from_this_sent", [])) for s in relations)
    correct = sum(
        1
        for s in relations
        for r in s.get("rel_from_this_sent", [])
        if r.get("valid") is True
    )

    info = [
        {"type": "status", "name": "success", "description": f"pmid: {pmid}"},
        {"type": "metric", "correct": correct, "total": total},
    ]
    return data, info
