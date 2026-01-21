from src.services.llm import LLM
from src.pmcad.parallel_process import process_folder_parallel
from src.pmcad.extract_relations import process_one_folder_llm_get_relations

llm = LLM(
    model_name="deepseek-chat",
    llm_url="https://api.deepseek.com/chat/completions",
    api_key="sk-b1a56f9730e44715a64d31364f508593",
    format="openai",
)

llm = LLM(
    model_name="qwen3-32b",
    llm_url="https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
    api_key="sk-46aa8d44210c46678de14c0a631d2b5c",
    format="qwen",
)

# pmidlist = ["27427769", "18495773", "18305027", "17267492", "20862256", "22022449", "19850911", "26098995", "18571739", "16912287"]
pmidlist = ["14716684"]
pmidlist = None
process_folder_parallel(
    "/data/wyuan/workspace/pmcdata_pro/data/pattern/chemprot_test",
    process_one_folder_llm_get_relations,
    workers=10,
    output_file_name="qw.json",
    llm=llm,
    pmidlist=pmidlist,
    # limit=1,
    # require_file="final_file_old"
)