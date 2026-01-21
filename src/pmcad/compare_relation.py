import os
import json
import threading
import concurrent.futures
from typing import Any, Dict, List, Tuple, Optional
from tqdm import tqdm


# -----------------------------
# 1) 扁平化：把嵌套 relations -> rel_from_this_sent 展开成一个 list
# -----------------------------
def _iter_relations_nested(data: Dict[str, Any]):
    relations = data.get("relations", [])
    if not isinstance(relations, list):
        return
    for si, sent_block in enumerate(relations):
        rel_list = sent_block.get("rel_from_this_sent", [])
        if not isinstance(rel_list, list):
            continue
        for ri, rel in enumerate(rel_list):
            if isinstance(rel, dict):
                yield si, ri, rel


def _flatten_relations(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [rel for _, _, rel in _iter_relations_nested(data)]


# -----------------------------
# 2) 让 data2 更“短”：只保留最关键字段，减少 token
# -----------------------------
DEFAULT_KEEP_KEYS = [
    "components", "targets", "contexts", "relation",
]


def _compact_relation(rel: Dict[str, Any], keep_keys: List[str]) -> Dict[str, Any]:
    out = {}
    for k in keep_keys:
        if k in rel:
            out[k] = rel[k]
    return out if out else rel

import json
from typing import Any, Dict, List, Tuple, Optional


# -----------------------------
# 3) Prompt（one-shot）: Yes/No only
# -----------------------------
def build_relation_coverage_prompt_yesno_joint(
    target_relation: dict,
    candidate_relations_with_index: list[dict],
) -> str:
    rel_json = json.dumps(target_relation, ensure_ascii=False, indent=2)
    cands_json = json.dumps(candidate_relations_with_index, ensure_ascii=False, indent=2)

    return f"""
You are a biomedical relation matcher.

Task:
Determine whether the TARGET relation is semantically supported by (covered by)
the CANDIDATE relation set extracted from another run/model.

Coverage definition (lenient + allow inference + allow multi-hop composition):
Answer Yes if the candidate set provides either:
(A) DIRECT support: at least one candidate explicitly states the same relation
    between the same two entities (allowing synonyms/aliases/canonicalization),
OR
(B) REASONABLE INFERENCE, including MULTI-HOP reasoning:
    The TARGET relation can be inferred by COMBINING TWO OR MORE candidate relations
    that form a coherent biological chain/mechanism connecting the same two entities.
    Examples of acceptable composition patterns:
    - Regulator/ligand -> receptor -> downstream effect on target gene/protein/phenotype
    - Enzyme -> substrate AND enzyme -> product (implies substrate -> product via enzyme)
    - A -> activates B AND B -> induces C (implies A -> induces C through B)
    - A correlated with B AND B explains variation in disease D (supports association B-D;
      and possibly A-D if chain is explicit)
    Multi-hop inference must be mechanistically plausible and not speculative.

You may treat relation labels as coarse:
- direction may be reversed,
- roles may be swapped or loosely defined,
- specificity may differ (general vs specific),
- target may be a high-level abstraction of candidates,
- synonyms/aliases/canonicalization apply (e.g., "dietary bile acids" -> "bile acids").

Answer No only if:
- there is no direct candidate relation connecting the entities, AND
- there is no plausible multi-hop chain in the candidate set that links the same entities/mechanism.

Output format (STRICT):
- Output EXACTLY one token: "Yes" or "No"
- Do NOT output any other words, punctuation, indices, or explanations.

=== CANDIDATE RELATIONS===
{cands_json}

=== TARGET RELATION ===
{rel_json}
""".strip()

def _parse_yes_no(resp: str) -> bool:
    """
    Strict-ish: accept if the first meaningful token starts with yes/no.
    Return True for Yes, False for No/others.
    """
    if not resp:
        return False
    s = resp.strip()
    if not s:
        return False
    low = s.lower()

    # allow some models to answer "Yes." / "Yes\n" / "Yes - ..."
    if low.startswith("yes"):
        return True
    if low.startswith("no"):
        return False
    # if it outputs junk, treat as No (conservative)
    return False


def _filter_valid_relations(rel_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    规则：
    - 如果存在 valid key，仅 valid == True 才保留
    - 如果不存在 valid key，默认保留
    """
    out = []
    for r in rel_list:
        if "valid" in r:
            if r.get("valid") is True:
                out.append(r)
            # valid == False → 丢弃
        else:
            out.append(r)
    return out


# -----------------------------
# 4) 你要的“单 folder 处理函数”：签名匹配 process_folder_parallel
#    process_one_folder(folder, input1_name, input2_name, output_name, llm, ...)
# -----------------------------
def process_one_folder_rel_coverage_one_shot(
    folder: str,
    input1_name: str,
    input2_name: str,
    output_name: str,
    llm,
    *,
    keep_keys: Optional[List[str]] = None,
    skip_existing: bool = True,
    write_raw_llm: bool = True,
    max_candidates: Optional[int] = None,
):
    """
    - 读取 folder/input1_name (data1) 和 folder/input2_name (data2)
    - 把 data2 的所有关系一次性塞进 prompt 作为候选
    - 对 data1 每条关系发一次 LLM，给出 covered / covered_by
    - 输出到 folder/output_name
    - 返回 (result, info_list) 供 process_folder_parallel 汇总指标与状态

    参数：
      - keep_keys: data2 关系裁剪字段
      - skip_existing: 如果 output 已存在且结构里已 coverage_checked 则跳过（可自行定义）
      - write_raw_llm: 是否在每条 relation 里写 coverage_raw 方便 debug
      - max_candidates: 仅用于兜底限制候选数量（不建议，但避免 prompt 爆长）；None=不限制
    """
    pmid = os.path.basename(folder)
    in1 = os.path.join(folder, input1_name)
    in2 = os.path.join(folder, input2_name)
    outp = os.path.join(folder, output_name)

    # 0) skip_existing（如果你希望：已有 outp 就跳过）
    if skip_existing and os.path.exists(outp):
        try:
            with open(outp, "r", encoding="utf-8") as f:
                old = json.load(f)
            # 如果 old 里已经有 coverage_report 且 relations 都打过 coverage_checked，就跳过
            rels = _flatten_relations(old)
            if rels and all(r.get("coverage_checked") is True for r in rels):
                covered = sum(1 for r in rels if r.get("covered") is True)
                total = len(rels)
                return None, [
                    {"type": "status", "name": "skip", "description": f"pmid:{pmid} (already covered)"},
                    {"type": "metric", "name": "coverage", "correct": covered, "total": total},
                ]
        except Exception:
            # 读失败就当作不跳过，继续跑
            pass

    # 1) load input files
    try:
        with open(in1, "r", encoding="utf-8") as f:
            data1 = json.load(f)
    except Exception as e:
        return None, [
            {"type": "error", "msg": f"load fail pmid:{pmid} file:{input1_name} err:{repr(e)}"},
            {"type": "metric", "name": "coverage", "correct": 0, "total": 0},
        ]

    try:
        with open(in2, "r", encoding="utf-8") as f:
            data2 = json.load(f)
    except Exception as e:
        return None, [
            {"type": "error", "msg": f"load fail pmid:{pmid} file:{input2_name} err:{repr(e)}"},
            {"type": "metric", "name": "coverage", "correct": 0, "total": 0},
        ]

    # 2) flatten relations
    r1_list = _flatten_relations(data1)
    r2_list = _flatten_relations(data2)
    r1_list = _filter_valid_relations(r1_list)
    r2_list = _filter_valid_relations(r2_list)

    if keep_keys is None:
        keep_keys = DEFAULT_KEEP_KEYS

    # 3) build candidates (with idx + compact)
    candidates = []
    for j, r2 in enumerate(r2_list):
        candidates.append(_compact_relation(r2, keep_keys))

    if max_candidates is not None:
        candidates = candidates[:max_candidates]

    # 4) cover check
        # 4) cover check
    covered_count = 0
    for r1 in r1_list:
        if skip_existing and r1.get("coverage_checked") is True:
            if r1.get("covered") is True:
                covered_count += 1
            continue

        prompt = build_relation_coverage_prompt_yesno_joint(r1, candidates)

        try:
            resp = (llm.query(prompt) or "").strip()
            yes = _parse_yes_no(resp)

            r1["covered"] = bool(yes)
            r1["coverage_checked"] = True
            # 不再输出 idx
            if "covered_by" in r1:
                r1.pop("covered_by", None)
            if write_raw_llm:
                r1["coverage_raw"] = resp

        except Exception as e:
            r1["covered"] = False
            r1["coverage_checked"] = True
            r1["coverage_error"] = repr(e)

        if r1.get("covered") is True:
            covered_count += 1

    total1 = len(r1_list)
    data1.setdefault("_coverage_report", {})
    data1["_coverage_report"].update({
        "pmid": pmid,
        "total_r1": total1,
        "total_r2": len(r2_list),
        "covered_r1": covered_count,
        "coverage_ratio": (covered_count / total1) if total1 else 0.0,
        "mode": "one_shot_all_candidates",
        "kept_keys_for_data2": keep_keys,
        "max_candidates": max_candidates,
    })

    # 5) write
    try:
        with open(outp, "w", encoding="utf-8") as fw:
            json.dump(data1, fw, ensure_ascii=False, indent=2)
    except Exception as e:
        return None, [
            {"type": "error", "msg": f"write fail pmid:{pmid} out:{output_name} err:{repr(e)}"},
            {"type": "metric", "name": "coverage", "correct": 0, "total": total1},
        ]

    return data1, [
        {"type": "status", "name": "success", "description": f"pmid:{pmid} covered:{covered_count}/{total1}"},
        {"type": "metric", "name": "coverage", "correct": covered_count, "total": total1},
    ]


# -----------------------------
# 5) 你的 process_folder_parallel（原封不动即可用）
# -----------------------------
def process_folder_parallel(
    folder: str,
    process_one_folder: callable,
    workers: int = 16,
    pmidlist: list = None,
    limit: int | None = None,
    **kwargs,
):
    leaf_folders = []
    for root, dirs, files in os.walk(folder):
        if not dirs and root != folder:
            leaf_folders.append(root)

    print(f"Total leaf folders detected: {len(leaf_folders)}")

    pmid_paths = {}
    for path in leaf_folders:
        pmid = os.path.basename(path)
        if pmidlist is not None and pmid not in pmidlist:
            continue
        pmid_paths[pmid] = path

    if limit is not None:
        pmid_paths = dict(list(pmid_paths.items())[:limit])

    results = {}
    postfix = {}
    global_stats = {}

    pbar_lock = threading.Lock()

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(process_one_folder, path, **kwargs): pmid
            for pmid, path in pmid_paths.items()
        }

        pbar = tqdm(total=len(futures), desc="Processing folders", dynamic_ncols=True)

        for future in concurrent.futures.as_completed(futures):
            pmid = futures[future]
            try:
                result, info_list = future.result()
            except Exception as e:
                result, info_list = None, [{"type": "error", "msg": str(e)}]

            results[pmid] = result

            for info in info_list:
                if info["type"] == "status":
                    status_name = info.get("name", "status")
                    postfix[status_name] = info.get("description", "")

                elif info["type"] == "metric":
                    name = info.get("name", "default")
                    if name not in global_stats:
                        global_stats[name] = {"correct": 0, "total": 0}
                    c = info.get("correct", 0)
                    t = info.get("total", 0)

                    global_stats[name]["correct"] += c
                    global_stats[name]["total"] += t

                    g_c = global_stats[name]["correct"]
                    g_t = global_stats[name]["total"]
                    g_acc = g_c / g_t if g_t else 0
                    postfix[f"{name}_acc"] = round(g_acc, 3)

                elif info["type"] == "error":
                    postfix["error"] = info.get("msg")

            with pbar_lock:
                pbar.set_postfix(postfix)
                pbar.update(1)

        pbar.close()

    return results


# -----------------------------
# 用法示例
# -----------------------------
# results = process_folder_parallel(
#     folder="/data/pmcdata/pmid_folders",
#     process_one_folder=process_one_folder_rel_coverage_one_shot,
#     workers=16,
#     pmidlist=None,
#     limit=None,
#     input1_name="run1.json",
#     input2_name="run2.json",
#     output_name="run1.coverage.oneshot.json",
#     llm=llm,
#     keep_keys=None,
#     skip_existing=True,
#     write_raw_llm=True,
#     max_candidates=None,  # 如果 data2 太长导致 prompt 爆掉，可先临时设个上限
# )