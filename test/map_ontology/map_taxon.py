import os
import json
import requests
import pandas as pd
from tqdm import tqdm
import numpy as np

# ============================================================
# Elasticsearch config
# ============================================================
ES_CONFIG = "/data/wyuan/workspace/pmcdata_pro/pmcad/config/elasticsearch.yaml"

from src.pmcad.taxon_search import search_taxon

from src.pmcad.taxon_map import process_one_folder_get_taxon_id
from src.pmcad.taxon_judge import process_one_folder_judge_taxon_id
from src.pmcad.parallel_process import process_folder_parallel
from src.services.llm import LLM

folder = "/data/wyuan/workspace/pmcdata_pro/data/pattern/rna_capping"
limit = 1024
process_folder_parallel(
    folder=folder,
    process_one_folder=process_one_folder_get_taxon_id,
    workers=32,
    input_name="ds.json",
    output_name="ds_taxon.json",
    limit=limit,
    search_func=lambda query: search_taxon(
        query=query,
        config_path=ES_CONFIG,
        k=30,
        verbose=False,
    ),
)
llm = LLM(
    model_name="deepseek-chat",
    llm_url="https://api.deepseek.com/chat/completions",
    api_key="sk-b1a56f9730e44715a64d31364f508593",
    format="openai",
)
results = process_folder_parallel(
    folder=folder,
    process_one_folder=process_one_folder_judge_taxon_id,
    input_name="ds_taxon.json",
    output_name="ds_taxon.json",
    workers=16,
    limit=limit,
    llm=llm,
)
