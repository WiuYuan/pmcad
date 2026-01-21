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
    process_one_folder_apply_llm_best,
    process_one_folder_judge_db_id,
    search_ontology,
    Ontology,
)
from src.pmcad.parallel_process import process_folder_parallel
from src.services.llm import LLM
from src.services.uniprot import search_uniprot
from src.services.rnacentral import search_rnacentral
from src.services.chebi import search_chebi
from src.pmcad.taxon_search import search_taxon
from src.pmcad.db_change import process_one_folder_convert_failed

# ============================================================
# ontology config
# ============================================================
FILE_PREFIX = "ds"


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


ONTOLOGY_CONFIGS = [
    {"ontology_type": "MeSH", "db_type": "mesh"},
    {"ontology_type": "cell_type", "db_type": "cl"},
    {"ontology_type": ["protein", "gene"], "db_type": "uniprot"},
    {"ontology_type": "RNA", "db_type": "rnacentral"},
    {"ontology_type": "chemical", "db_type": "chebi"},
    {"ontology_type": "domain", "db_type": "interpro"},
    {"ontology_type": "GO", "db_type": "go"},
    {"ontology_type": "SO", "db_type": "so"},
    {"ontology_type": "disease", "db_type": "doid"},
    {"ontology_type": "anatomy", "db_type": "uberon"},
    # {"ontology_type": "relation", "db_type": "ro"},
    {"ontology_type": "cell_line", "db_type": "cvcl"},
    {"ontology_type": "species", "db_type": "taxon"},
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

species_ot = build_ontology(cfg={"ontology_type": "species", "db_type": "taxon"}, judge_method="strict", use_species=False)
cvcl_ot = build_ontology(cfg={"ontology_type": "cell_line", "db_type": "cvcl"}, judge_method="strict", use_species=False)

folder = "/data/wyuan/workspace/pmcdata_pro/data/pattern/chemprot_test"
limit = 16
limit = None
pmidlist = ["14716684"]
pmidlist = None

process_folder_parallel(
    folder=folder,
    process_one_folder=process_one_folder_apply_llm_best,
    workers=32,
    input_name="ds_decomposed.json",
    output_name="ds_final.json",
    limit=limit,
    species_ot=species_ot,
    cvcl_ot=cvcl_ot,
    ot_list=ontologies,
    pmidlist=pmidlist,
)