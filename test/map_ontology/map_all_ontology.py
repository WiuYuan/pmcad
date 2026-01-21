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
# dense_model = SentenceTransformer("pritamdeka/BioBERT-mnli-snli-scinli-scitail-mednli-stsb")

# SPLADE model
splade_model = SparseEncoder(
    model_name_or_path="/data/wyuan/.cache/huggingface/hub/models--NeuML--pubmedbert-base-splade/snapshots/f284fcbafe4761108f27357f5278d846a630059e",
    device=device,
)
# splade_model = SparseEncoder("NeuML/pubmedbert-base-splade")
"./bin/elasticsearch"

from src.pmcad.ontology_map import (
    process_one_folder_get_db_id,
    process_one_folder_judge_db_id,
    search_ontology,
    Ontology,
)
from src.pmcad.parallel_process import process_folder_parallel
from src.services.llm import LLM
from src.services.uniprot import search_uniprot
from src.services.rnacentral import search_rnacentral
# from src.services.chebi import search_chebi
from src.pmcad.taxon_search import search_taxon
from src.pmcad.db_change import process_one_folder_convert_failed

llm = LLM(
    model_name="deepseek-chat",
    llm_url="https://api.deepseek.com/chat/completions",
    api_key="sk-b1a56f9730e44715a64d31364f508593",
    format="openai",
)
llm = LLM(
    model_name="/data/wyuan/workspace/model/Qwen3-1.7B",
    llm_url="http://localhost:18000/v1/chat/completions",
    format="openai",
)

# ============================================================
# ontology config
# ============================================================
FILE_PREFIX = "qw"


def build_search_func(db_type):
    # ---- special DBs ----
    if db_type in ["uniprot", "rnacentral", "taxon"]:
        if db_type == "uniprot":
            return lambda query: search_uniprot(query=query, k=30)
        if db_type == "rnacentral":
            return lambda query: search_rnacentral(query=query, k=30)
        if db_type == "taxon":
            return lambda query: search_taxon(config_path=ES_CONFIG, query=query, k=30)

        raise ValueError(f"Unknown special search builder: {db_type}")

    # ---- default ontology DBs ----
    extra_source_fields=None
    if db_type == "cvcl":
        extra_source_fields = ["species"]
    return lambda query: search_ontology(
        query=query,
        search_type="dense+splade",
        index_name=f"{db_type}_index",
        config_path=ES_CONFIG,
        dense_model=dense_model,
        splade_model=splade_model,
        k=30,
        extra_source_fields=extra_source_fields,
        verbose=False,
    )


def build_ontology(cfg, judge_method="strict", use_species=False):
    db_type = cfg["db_type"]

    return Ontology(
        ontology_type=cfg["ontology_type"],
        db_type=db_type,
        search_func=build_search_func(db_type),
        filename=f"{FILE_PREFIX}_{db_type}.json",
        judge_method=judge_method,
        use_species=use_species,
    )

core_file = f"{FILE_PREFIX}_decomposed.json"

ONTOLOGY_CONFIGS = [
    {"ontology_type": "MeSH", "db_type": "mesh"},
    {"ontology_type": "species", "db_type": "taxon"},
    {"ontology_type": "cell_line", "db_type": "cvcl"},
    {"ontology_type": "cell_type", "db_type": "cl"},
    {"ontology_type": ["protein", "gene"], "db_type": "uniprot"},
    {"ontology_type": "RNA", "db_type": "rnacentral"},
    {"ontology_type": "chemical", "db_type": "chebi"},
    {"ontology_type": "domain", "db_type": "interpro"},
    {"ontology_type": "GO", "db_type": "go"},
    {"ontology_type": "SO", "db_type": "so"},
    {"ontology_type": "disease", "db_type": "doid"},
    {"ontology_type": "anatomy", "db_type": "uberon"},
    {"ontology_type": "relation", "db_type": "ro"},
]

ontologies = []

for cfg in ONTOLOGY_CONFIGS:
    if cfg["db_type"] in ["ro"]:
        judge_method = "forced"
    elif cfg["db_type"] in ["so", "go"]:
        judge_method = "relaxed"
    else:
        judge_method = "strict"
    if cfg["db_type"] in ["uniprot", "rnacentral"]:
        use_species = True
    else:
        use_species = False
    ontologies.append(build_ontology(cfg=cfg, judge_method=judge_method, use_species=use_species))

mesh_ot = ontologies[0]
species_ot = ontologies[1]
cvcl_ot = ontologies[2]
go_ot = build_ontology(cfg={"ontology_type": "GO", "db_type": "go"}, judge_method="strict", use_species=False)
so_ot = build_ontology(cfg={"ontology_type": "SO", "db_type": "so"}, judge_method="strict", use_species=False)


folder = "/data/wyuan/workspace/pmcdata_pro/data/protease"
limit = 1
limit = None
pmidlist = ["14716684"]
pmidlist = None

for ot in ontologies:
    workers = 64
    if ot.db_type in ["uniprot", "rnacentral"]:
        workers1 = 3
    else:
        workers1 = 64

    print(
        f"\n[INFO] Dealing with {str(ot.ontology_type)} in {ot.db_type} with workers {workers}\n"
    )

    process_folder_parallel(
        folder=folder,
        process_one_folder=[process_one_folder_get_db_id, process_one_folder_judge_db_id],
        workers=workers,
        input_name=core_file,
        limit=limit,
        ot=ot,
        species_ot=species_ot,
        cvcl_ot=cvcl_ot,
        llm=llm,
        max_worker_list=[workers1, workers],
        pmidlist=pmidlist,
    )

    # process_folder_parallel(
    #     folder=folder,
    #     process_one_folder=process_one_folder_judge_db_id,
    #     ot=ot,
    #     workers=workers,
    #     limit=limit,
    #     llm=llm,
    #     pmidlist=pmidlist,
    # )
    
    if ot.db_type == "cl":
        tgt_ot = cvcl_ot
        print(
            f"\n[INFO] Dealing with {ot.db_type} to {tgt_ot.db_type}\n"
        )
        process_folder_parallel(
            folder=folder,
            process_one_folder=process_one_folder_convert_failed,
            workers=workers,
            input_name=core_file,
            limit=limit,
            src_ot=ot,
            tgt_ot=tgt_ot,
            llm=llm,
            pmidlist=pmidlist,
        )
    
    if ot.db_type in ["rnacentral"]:
        tgt_ot = so_ot
        print(
            f"\n[INFO] Dealing with {ot.db_type} to {tgt_ot.db_type}\n"
        )
        process_folder_parallel(
            folder=folder,
            process_one_folder=process_one_folder_convert_failed,
            workers=workers,
            input_name=core_file,
            limit=limit,
            src_ot=ot,
            tgt_ot=tgt_ot,
            llm=llm,
            pmidlist=pmidlist,
        )
        
    if ot.db_type in ["rnacentral"]:
        tgt_ot = go_ot
        print(
            f"\n[INFO] Dealing with {ot.db_type} to {tgt_ot.db_type}\n"
        )
        process_folder_parallel(
            folder=folder,
            process_one_folder=process_one_folder_convert_failed,
            workers=workers,
            input_name=core_file,
            limit=limit,
            src_ot=ot,
            tgt_ot=tgt_ot,
            llm=llm,
            pmidlist=pmidlist,
        )
        
    if ot.db_type in ["rnacentral", "uniprot", "so", "go", "chebi", "interpro", "cvcl"]:
        tgt_ot = mesh_ot
        print(
            f"\n[INFO] Dealing with {ot.db_type} to {tgt_ot.db_type}\n"
        )
        ot.use_species = False
        process_folder_parallel(
            folder=folder,
            process_one_folder=process_one_folder_convert_failed,
            workers=workers,
            input_name=core_file,
            limit=limit,
            src_ot=ot,
            tgt_ot=tgt_ot,
            llm=llm,
            pmidlist=pmidlist,
        )
