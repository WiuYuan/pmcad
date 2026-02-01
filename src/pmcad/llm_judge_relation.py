
# src/pmcad/llm_judge_relation.py
import json
from typing import Union, Optional

from src.pmcad.pmidstore import PMIDStore


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
    - a gene mention can be normalized/mapped to a UniProt protein (gene→UniProt mapping is allowed).
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
- A gene mention can be normalized/mapped to a UniProt protein (gene→UniProt mapping is allowed).
- Output only one word: Yes or No.

=== ABSTRACT ===
\"\"\"{abstract}\"\"\"

=== RELATION ===
{rel_json}

Output: Yes or No
"""

    raise ValueError(f"unknown judge_method: {judge_method}")


def process_one_pmid_validate_relations(
    *,
    pmid: Union[int, str],
    store: PMIDStore,
    llm,
    input_name: str = "qw_final.json",
    output_name: str = "qw_final.json",
    prereq_name: str = "qw_final.json",
    skip_existing: bool = True,
    judge_method: str = "strict",
    **kwargs,
):
    """
    ✅ parallel_process 标准格式（DB 模式）：
      - 读：store.get(pmid, input_name)
      - 写：store.put(pmid, output_name, data)

    同时增加“前置文件 gating”：
      - 只有当 store.get(pmid, prereq_name) 存在（dict）时才执行
      - 用于保证：qw_final.json 出现后才能跑本 stage（op 约束）
    """
    if store is None:
        raise ValueError("store is required")
    if llm is None:
        raise ValueError("llm is required")

    pmid_int = int(pmid)
    pmid_str = str(pmid_int)

    # ------------------------------------------------------------
    # 0) prerequisite: qw_final.json must exist
    # ------------------------------------------------------------
    prereq = store.get(pmid_int, prereq_name) if prereq_name else None
    if prereq_name and (not isinstance(prereq, dict)):
        return None, [
            {"type": "error", "msg": f"pmid:{pmid_str} prereq_missing: {prereq_name}"},
            {"type": "metric", "name": "judged", "correct": 0, "total": 0},
        ]

    # ------------------------------------------------------------
    # 1) load input
    # ------------------------------------------------------------
    data = store.get(pmid_int, input_name)
    if not isinstance(data, dict):
        return None, [
            {"type": "error", "msg": f"pmid:{pmid_str} missing input: {input_name}"},
            {"type": "metric", "name": "judged", "correct": 0, "total": 0},
        ]

    abstract = data.get("abstract", "") or ""
    relations = data.get("relations", [])
    if not isinstance(relations, list):
        relations = []
        data["relations"] = relations

    # ------------------------------------------------------------
    # 2) scan + skip_existing
    # ------------------------------------------------------------
    total = 0
    yes_cnt = 0
    all_have_valid = True

    for sent in relations:
        rel_list = sent.get("rel_from_this_sent", [])
        if not isinstance(rel_list, list):
            continue
        for rel in rel_list:
            total += 1
            if "valid" not in rel:
                all_have_valid = False
            if rel.get("valid") is True:
                yes_cnt += 1

    if skip_existing and total > 0 and all_have_valid:
        return None, [
            {"type": "status", "name": "skip", "description": f"pmid:{pmid_str} already_validated"},
            {"type": "metric", "name": "valid_yes", "correct": yes_cnt, "total": total},
        ]

    # ------------------------------------------------------------
    # 3) validate missing ones
    # ------------------------------------------------------------
    judged = 0
    for sent in relations:
        rel_list = sent.get("rel_from_this_sent", [])
        if not isinstance(rel_list, list):
            continue

        for rel in rel_list:
            if skip_existing and ("valid" in rel):
                continue

            prompt = build_relation_validation_prompt(abstract, rel, judge_method=judge_method)
            try:
                resp = (llm.query(prompt) or "").strip().lower()
                is_valid = resp.startswith("y")  # Yes
            except Exception as e:
                is_valid = False
                rel["validation_error"] = str(e)

            rel["valid"] = is_valid
            judged += 1

    # ------------------------------------------------------------
    # 4) write output
    # ------------------------------------------------------------
    store.put(pmid_int, output_name, data)

    # recount
    total2 = 0
    yes_cnt2 = 0
    for sent in relations:
        rel_list = sent.get("rel_from_this_sent", [])
        if not isinstance(rel_list, list):
            continue
        for rel in rel_list:
            total2 += 1
            if rel.get("valid") is True:
                yes_cnt2 += 1

    return data, [
        {"type": "status", "name": "success", "description": f"pmid:{pmid_str} judged={judged}"},
        {"type": "metric", "name": "valid_yes", "correct": yes_cnt2, "total": total2},
        {"type": "metric", "name": "judged", "correct": judged, "total": total2},
    ]
