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
from src.pmcad.ontology_map import Ontology, process_one_folder_get_db_id, process_one_folder_judge_db_id
from src.pmcad.parallel_process import process_folder_parallel
from src.services.llm import LLM

folder = "/data/wyuan/workspace/pmcdata_pro/data/pattern/rna_capping"
limit = 1024
search_func=lambda query: search_taxon(
    query=query,
    config_path=ES_CONFIG,
    k=30,
    verbose=False,
)

ot = Ontology(ontology_type="species", db_type="taxon", search_func=search_func, filename="ds_taxon.json", judge_method="strict")

folder = "/data/wyuan/workspace/pmcdata_pro/data/pattern/rna_capping"
limit = 16
pmidlist = ["461190"]
process_folder_parallel(
    folder=folder, 
    process_one_folder=process_one_folder_get_db_id,
    workers=32, 
    input_name="ds.json", 
    limit=limit,
    ot=ot,
    pmidlist=pmidlist,
)
llm = LLM(
    model_name="deepseek-chat",
    llm_url="https://api.deepseek.com/chat/completions",
    api_key="sk-b1a56f9730e44715a64d31364f508593",
    format="openai",
)
results = process_folder_parallel(
    folder=folder,
    process_one_folder=process_one_folder_judge_db_id,
    ot=ot,
    workers=16,
    limit=limit,
    llm=llm,
    pmidlist=pmidlist,
)