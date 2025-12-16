import requests
import time
import re
import os
import json


def chebi_query_ols(
    name,
    max_sleep=30,
    max_retries_per_item=3,
    top_k=5,
):
    base_url = "https://www.ebi.ac.uk/ols4/api/search"

    # 保持“最少清洗”，避免引入歧义
    term = str(name).strip()

    params = {"q": term, "ontology": "chebi", "rows": top_k}

    sleep_time = 1
    attempts = 0

    while attempts < max_retries_per_item:
        attempts += 1
        try:
            r = requests.get(base_url, params=params, timeout=10)

            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(sleep_time)
                sleep_time = min(sleep_time * 2, max_sleep)
                continue

            if r.status_code != 200:
                time.sleep(sleep_time)
                sleep_time = min(sleep_time * 2, max_sleep)
                continue

            data = r.json()
            docs = data.get("response", {}).get("docs", [])
            if not docs:
                return {"results": None, "timeout": False}

            return {"results": docs[:top_k], "timeout": False}

        except Exception:
            time.sleep(sleep_time)
            sleep_time = min(sleep_time * 2, max_sleep)

    return {"results": None, "timeout": True}


def extract_chebi_info(doc, rank):
    return {
        "chebi_id": doc.get("obo_id"),
        "label": doc.get("label"),
        "synonyms": doc.get("synonym", []),
        "rank": rank,
    }


def process_one_folder_get_chebi_id(
    folder: str,
    input_name: str,
    output_name: str,
    skip_existing=True,
    max_retries_per_item=3,
    top_candidates=5,
):

    pmid = os.path.basename(folder)
    in_path = os.path.join(folder, input_name)
    out_path = os.path.join(folder, output_name)

    try:
        with open(in_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except:
        return None, [
            {"type": "status", "name": f"load fail pmid: {pmid}"},
        ]

    relations = data.get("relations", [])
    all_relations = []
    for rel in relations:
        all_relations.extend(rel.get("rel_from_this_sent", []))

    if not all_relations:
        with open(out_path, "w", encoding="utf-8") as fw:
            json.dump(data, fw, ensure_ascii=False, indent=2)
        return data, [
            {"type": "status", "name": f"no relations pmid: {pmid}"},
        ]

    # =========================
    # 1. 收集 chemical
    # =========================
    needed_keys = set()

    for rel in all_relations:
        for comp in rel.get("components", []):
            if comp.get("type") == "chemical":
                needed_keys.add(comp["name"])

        for tgt in rel.get("target", []):
            if tgt.get("type") == "chemical":
                needed_keys.add(tgt["name"])

    # =========================
    # 2. 读取已有结果
    # =========================
    existing_map = {}
    if skip_existing and os.path.exists(out_path):
        try:
            with open(out_path, "r", encoding="utf-8") as f:
                old_data = json.load(f)
            for item in old_data.get("chebi_match", []):
                existing_map[item["original_name"]] = item
        except:
            pass

    remain_keys = [k for k in needed_keys if k not in existing_map]

    if skip_existing and len(remain_keys) == 0:
        return None, [
            {
                "type": "status",
                "name": f"skip pmid: {pmid} (all chemical mappings exist)",
            },
        ]

    chebi_match_list = list(existing_map.values())
    success_queries = 0
    timeout_count = 0
    total_queries = 0

    # =========================
    # 3. 查询 ChEBI (OLS)
    # =========================
    for name in remain_keys:
        total_queries += 1

        res = chebi_query_ols(
            name,
            max_retries_per_item=max_retries_per_item,
            top_k=top_candidates,
        )

        if res["timeout"]:
            timeout_count += 1

        hits = res["results"]

        if hits:
            success_queries += 1
            processed_hits = []
            for rank, doc in enumerate(hits, start=1):
                processed_hits.append(extract_chebi_info(doc, rank))

            chebi_match_list.append(
                {
                    "original_name": name,
                    "entity_type": "chemical",
                    "hits": processed_hits,
                }
            )
        else:
            chebi_match_list.append(
                {
                    "original_name": name,
                    "entity_type": "chemical",
                    "hits": [],
                }
            )

    data["chebi_match"] = chebi_match_list

    try:
        with open(out_path, "w", encoding="utf-8") as fw:
            json.dump(data, fw, ensure_ascii=False, indent=2)
    except:
        return None, [
            {"type": "status", "name": f"write fail pmid: {pmid}"},
            {"type": "metric", "name": "error", "correct": 1, "total": 1},
        ]

    return data, [
        {"type": "status", "name": f"pmid: {pmid}"},
        {
            "type": "metric",
            "name": "find",
            "correct": success_queries,
            "total": total_queries,
        },
        {
            "type": "metric",
            "name": "timeout",
            "correct": timeout_count,
            "total": total_queries,
        },
    ]
