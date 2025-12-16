from src.pmcad.parallel_process import process_folder_parallel
from src.pmcad.go_judge import process_one_folder
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
    process_one_folder=process_one_folder,
    input_name="ds_uniprotid_go.json",
    output_name="ds_uniprotid_go_gomap.json",
    workers=16,
    llm=llm
)