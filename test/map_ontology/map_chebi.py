from src.services.chebi import process_one_folder_get_chebi_id
from src.pmcad.parallel_process import process_folder_parallel

limit = 1024

process_folder_parallel(
    folder="/data/wyuan/workspace/pmcdata_pro/data/pattern/rna_capping",
    process_one_folder=process_one_folder_get_chebi_id,
    relation_file="ds.json",
    output_file="ds_chebi.json",
    limit=limit,
    max_retries_per_item=10,
    workers=3,
    top_candidates=30,
)


from src.pmcad.parallel_process import process_folder_parallel
from src.pmcad.chebi_judge import process_one_folder_judge_chebi_id
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
    process_one_folder=process_one_folder_judge_chebi_id,
    input_name="ds_chebi.json",
    output_name="ds_chebi.json",
    limit=limit,
    workers=16,
    llm=llm,
)
