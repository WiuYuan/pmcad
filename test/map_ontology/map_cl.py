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
    device=device,
)
# SPLADE model
splade_model = SparseEncoder(
    model_name_or_path="/data/wyuan/.cache/huggingface/hub/models--NeuML--pubmedbert-base-splade/snapshots/f284fcbafe4761108f27357f5278d846a630059e",
    device=device,
)

from src.pmcad.cl_search import search_cl

from src.pmcad.cl_map import process_one_folder_get_cl_id
from src.pmcad.cl_judge import process_one_folder_judge_cl_id
from src.pmcad.parallel_process import process_folder_parallel
from src.services.llm import LLM

folder = "/data/wyuan/workspace/pmcdata_pro/data/pattern/rna_capping"
limit = 1024
process_folder_parallel(
    folder=folder,
    process_one_folder=process_one_folder_get_cl_id,
    workers=32,
    input_name="ds.json",
    output_name="ds_cl.json",
    limit=limit,
    search_func=lambda query: search_cl(
        query=query,
        config_path=ES_CONFIG,
        dense_model=dense_model,
        splade_model=splade_model,
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
    process_one_folder=process_one_folder_judge_cl_id,
    input_name="ds_cl.json",
    output_name="ds_cl.json",
    workers=16,
    limit=limit,
    llm=llm,
)
