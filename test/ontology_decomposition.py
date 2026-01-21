from src.services.llm import LLM
from src.pmcad.parallel_process import process_folder_parallel
from src.pmcad.ontology_decompostion import process_one_folder_entity_decomposition

llm = LLM(
    model_name="qwen3-32b",
    llm_url="http://127.0.0.1:18000/v1/chat/completions",
    # api_key="sk-b1a56f9730e44715a64d31364f508593",
    format="openai",
)

predix = "qw"

# pmidlist = ["27427769", "18495773", "18305027", "17267492", "20862256", "22022449", "19850911", "26098995", "18571739", "16912287"]
pmidlist = ["14716684"]
pmidlist = None
process_folder_parallel(
    "/data/wyuan/workspace/pmcdata_pro/data/protease",
    process_one_folder_entity_decomposition,
    workers=64,
    input_name=f"{predix}.json",
    output_name=f"{predix}_decomposed.json",
    llm=llm,
    pmidlist=pmidlist,
    # limit=16,
    # require_file="final_file_old"
)
