import os
import json
import requests
import pandas as pd
from tqdm import tqdm
import numpy as np
import torch

from sentence_transformers import SentenceTransformer
from sentence_transformers import SparseEncoder


# ============================================================
# Elasticsearch config
# ============================================================
ES_CONFIG = "/data/wyuan/workspace/pmcdata_pro/pmcad/config/elasticsearch.yaml"

device = "cpu"
# Dense model
dense_model = SentenceTransformer(
    "/data/wyuan/.cache/huggingface/hub/models--pritamdeka--BioBERT-mnli-snli-scinli-scitail-mednli-stsb/snapshots/82d44689be9cf3c6c6a6f77cc3171c93282873a1",
    device=device
)
# SPLADE model
splade_model = SparseEncoder(
    model_name_or_path="/data/wyuan/.cache/huggingface/hub/models--NeuML--pubmedbert-base-splade/snapshots/f284fcbafe4761108f27357f5278d846a630059e",
    device=device
)

from src.pmcad.so_search import search_so

from src.pmcad.rnacentral_to_so import process_rnacentral_failed_rna_to_so, merge_rnacentral_to_so_and_cleanup
from src.pmcad.parallel_process import process_folder_parallel
from src.services.llm import LLM

folder = "/data/wyuan/workspace/pmcdata_pro/data/pattern/rna_capping"
limit = 1024
llm = LLM(
    model_name="deepseek-chat",
    llm_url="https://api.deepseek.com/chat/completions",
    api_key="sk-b1a56f9730e44715a64d31364f508593",
    format="openai",
)
process_folder_parallel(
    folder=folder, 
    process_one_folder=process_rnacentral_failed_rna_to_so,
    workers=16, 
    ds_json_name="ds.json",
    ds_rnacentral_name="ds_rnacentral.json",
    output_name="ds_rnacentral_so.json",
    limit=limit,
    so_search_func= lambda query: search_so(
        query=query,
        config_path=ES_CONFIG,
        dense_model=dense_model,
        splade_model=splade_model,
        k=30,
        verbose=False,
    ),
    llm=llm
)
results = process_folder_parallel(
    folder=folder,
    process_one_folder=merge_rnacentral_to_so_and_cleanup,
    ds_json_name="ds.json",
    ds_so_name="ds_so.json",
    ds_rnacentral_to_so_name="ds_rnacentral_so.json",
    ds_rnacentral_name="ds_rnacentral.json",
    workers=32,
    limit=1024,
)