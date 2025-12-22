from src.services.llm import LLM
from src.pmcad.parallel_process import process_folder_parallel
from src.pmcad.extract_relations import process_one_folder_llm_get_relations

llm = LLM(
    model_name="deepseek-chat",
    llm_url="https://api.deepseek.com/chat/completions",
    api_key="sk-b1a56f9730e44715a64d31364f508593",
    format="openai",
)

# pmidlist = ["27427769", "18495773", "18305027", "17267492", "20862256", "22022449", "19850911", "26098995", "18571739", "16912287"]
# pmidlist = ["27427769"]
process_folder_parallel(
    "/data/wyuan/workspace/pmcdata_pro/data/pattern/rna_capping",
    process_one_folder_llm_get_relations,
    workers=16,
    output_file_name="ds.json",
    llm=llm,
    # pmidlist=pmidlist,
    limit=1024,
    # require_file="final_file_old"
)
