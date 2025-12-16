import os
import json


def build_relation_validation_prompt(abstract: str, relation: dict) -> str:
    """
    给单条 relation 构建验证 prompt。
    LLM 必须回答 Yes 或 No。
    """
    rel_json = json.dumps(relation, ensure_ascii=False, indent=2)

    return f"""
You are a biomedical relation validator.

Your task:
Determine whether the following extracted relation is TRUE based ONLY on the abstract.

Rules:
- Answer Yes if the relation can be reasonably inferred from the abstract.
- Logical abstraction and weakening are allowed (the relation may be less specific or less strong than the wording in the abstract).
- Do NOT require exact wording matches between the relation and the abstract.
- The entities (gene, protein, RNA, GO term) must be sufficiently specific within the context of this abstract:
    - A knowledgeable reader should be able to tell exactly which biological entity is being referred to.
    - Context-specific terms (e.g., viral proteins in a virus paper) are acceptable.
- Output only one word: Yes or No.

=== ABSTRACT ===
\"\"\"{abstract}\"\"\"

=== RELATION ===
{rel_json}

Output: Yes or No
"""

def process_one_folder(folder: str, input_name: str, output_name: str, llm, skip_existing=True):
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
            {"type": "status", "name": f"load fail pmid: {pmid}"},
            {"type": "error", "msg": str(e)},
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
            {"type": "status", "name": f"skip pmid: {pmid} (already validated)"},
            {"type": "metric", "correct": correct, "total": total},
        ]

    # === 3. Validate missing ones ===
    for sent in relations:
        rel_list = sent.get("rel_from_this_sent", [])
        for rel in rel_list:
            if skip_existing and "valid" in rel:
                continue

            prompt = build_relation_validation_prompt(abstract, rel)
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
            {"type": "status", "name": f"write fail pmid: {pmid}"},
            {"type": "error", "msg": str(e)},
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
        {"type": "status", "name": f"pmid: {pmid}"},
        {"type": "metric", "correct": correct, "total": total},
    ]
    return data, info