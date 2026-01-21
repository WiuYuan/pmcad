from src.services.uniprot import search_uniprot
from src.pmcad.ontology_map import Ontology, process_one_folder_get_db_id, process_one_folder_judge_db_id
from src.pmcad.parallel_process import process_folder_parallel

search_func= lambda query: search_uniprot(
    query=query,
    k=30,
)

ot = Ontology(ontology_type=["gene", "protein"], db_type="uniprot", use_species=True, search_func=search_func, filename="ds_uniprot.json", judge_method="strict")

species_ot = Ontology(ontology_type="species", db_type="taxon", filename="ds_taxon.json", judge_method="strict")


limit = 16

process_folder_parallel(
    folder="/data/wyuan/workspace/pmcdata_pro/data/pattern/rna_capping",
    process_one_folder=process_one_folder_get_db_id,
    input_name="ds.json",
    ot=ot,
    species_ot=species_ot,
    limit=limit,
    workers=3,
)


from src.pmcad.parallel_process import process_folder_parallel
from src.pmcad.uniprot_judge import process_one_folder_judge_uniprot_id
from src.services.llm import LLM


llm = LLM(
    model_name="deepseek-chat",
    llm_url="https://api.deepseek.com/chat/completions",
    api_key="sk-b1a56f9730e44715a64d31364f508593",
    format="openai",
)

folder = "/data/wyuan/workspace/pmcdata_pro/data/pattern/rna_capping"
results = process_folder_parallel(
    folder=folder,
    process_one_folder=process_one_folder_judge_db_id,
    ot=ot,
    workers=16,
    limit=limit,
    llm=llm
)