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
    chebi_id = doc.get("obo_id")
    label = doc.get("label", "")
    synonyms = doc.get("synonym", [])

    desc_parts = []
    if label:
        desc_parts.append(label)
    if synonyms:
        desc_parts.append("ALIASES: " + "; ".join(synonyms[:10]))

    return {
        "id": chebi_id,
        "name": chebi_id,
        "description": " | ".join(desc_parts),
        "rank": rank,
    }


def process_one_folder_get_chebi_id(
    folder: str,
    relation_file: str,
    output_file: str,
    top_candidates=5,
    max_retries_per_item=3,
):
    pmid = os.path.basename(folder)

    # ---------- load relation.json ----------
    try:
        with open(os.path.join(folder, relation_file), "r", encoding="utf-8") as f:
            rel_data = json.load(f)
    except Exception:
        return None, [{"type": "status", "name": f"{pmid} (relation load error)"}]

    relations = rel_data.get("relations", [])

    # ---------- collect chemicals ----------
    needed = set()

    for block in relations:
        for rel in block.get("rel_from_this_sent", []):
            for field in ("components", "target", "context"):
                for ent in rel.get(field, []):
                    if ent.get("type") == "chemical" and ent.get("name"):
                        needed.add(ent["name"])

    if not needed:
        out = {"pmid": pmid, "chebi_map": []}
        with open(os.path.join(folder, output_file), "w", encoding="utf-8") as fw:
            json.dump(out, fw, ensure_ascii=False, indent=2)
        return out, [
            {"type": "status", "name": f"{pmid} (no chemical entities)"},
        ]

    # ---------- ChEBI search ----------
    chebi_map = []
    judge = False

    for name in needed:
        try:
            res = chebi_query_ols(
                name,
                max_retries_per_item=max_retries_per_item,
                top_k=top_candidates,
            )
            hits_raw = res.get("results") or []
        except Exception:
            hits_raw = []

        hits = []
        for rank, doc in enumerate(hits_raw, start=1):
            hits.append(extract_chebi_info(doc, rank))

        if hits:
            judge = True

        chebi_map.append(
            {
                "name": name,
                "hits": hits,
            }
        )

    # ---------- write output ----------
    out = {
        "pmid": pmid,
        "chebi_map": chebi_map,
    }

    with open(os.path.join(folder, output_file), "w", encoding="utf-8") as fw:
        json.dump(out, fw, ensure_ascii=False, indent=2)

    return out, [
        {"type": "status", "name": f"{pmid}"},
        {"type": "metric", "correct": 1 if judge else 0, "total": 1},
    ]
