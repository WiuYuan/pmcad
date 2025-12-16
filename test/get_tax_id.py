import os
import json
import requests
import pandas as pd
from tqdm import tqdm
import numpy as np
import requests
import json

ES = "https://localhost:9200"
USER = "elastic"
PWD = "0zROyDv0xcK843uOI_4W"
CERT = "/data/wyuan/workspace/pmcdata_pro/elasticsearch-8.12.2/config/certs/http_ca.crt"
INDEX = "taxonomy_index"
AUTH = ("elastic", PWD)

def search_species(query, top_k=30):
    """
    从 ES 物种索引中搜索 query（模糊 BM25），
    返回 taxid, 标准名, rank, score(0-1 归一化)。
    """

    body = {
        "query": {
            "multi_match": {
                "query": query,
                "fields": ["name^3", "synonym", "other_names"],
                "fuzziness": "AUTO"
            }
        },
        "size": top_k
    }

    r = requests.get(
        f"{ES}/{INDEX}/_search",
        auth=(USER, PWD),
        verify=CERT,
        json=body
    )
    data = r.json()
    hits = data.get("hits", {}).get("hits", [])

    # ---- 提取原始 score ----
    scores = [h.get("_score", 0.0) for h in hits if h.get("_score") is not None]

    if scores:
        max_score = max(scores)
        if max_score == 0:
            max_score = 1.0
    else:
        max_score = 1.0

    out = []
    for h in hits:
        src = h.get("_source", {})
        raw_score = h.get("_score", 0.0)

        out.append({
            "taxid": src.get("taxid"),
            "name": src.get("name"),
            "rank": src.get("rank"),
            "score": round(raw_score / max_score, 3)   # ⭐归一化到 0–1
        })

    return out

from pmcad.src.pmcad.taxonomy_map import process_one_folder_get_species_id
from src.pmcad.parallel_process import process_folder_parallel

process_folder_parallel(
    folder="/data/wyuan/workspace/pmcdata_pro/data/pattern/rna_capping",
    process_one_folder=process_one_folder_get_species_id,
    workers=32, 
    input_name="ds.json", 
    output_name="ds_taxid.json", 
    search_func=search_species
)