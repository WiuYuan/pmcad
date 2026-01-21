from src.pmcad.compare_relation import process_one_folder_rel_coverage_one_shot
from src.pmcad.parallel_process import process_folder_parallel
from src.services.llm import LLM

# llm = LLM(
#     model_name="gpt-5-2025-08-07",
#     llm_url="https://api.aimlapi.com/v1/chat/completions",
#     api_key="9ce046a9681446c48427b3fe4dd7cdd4",
#     format="openai",
#     proxy_url="http://127.0.0.1:7897",
# )
llm = LLM(
    model_name="deepseek-chat",
    llm_url="https://api.deepseek.com/chat/completions",
    api_key="sk-b1a56f9730e44715a64d31364f508593",
    format="openai",
)


# process_one_folder(folder="/data/wyuan/workspace/pmcdata_pro/ai_pattern/output/32999025",
#                    input_name="ds.json",
#                    output_name="ds_judge.json",
#                    skip_existing=False,
#                    llm=llm)

results = process_folder_parallel(
    folder="/data/wyuan/workspace/pmcdata_pro/data/pattern/chemprot_test",
    process_one_folder=process_one_folder_rel_coverage_one_shot,
    workers=16,
    pmidlist=None,
    limit=None,
    input1_name="cpr_judge.json",
    input2_name="ds.json",
    output_name="cpr_qw.coverage.oneshot.json",
    llm=llm,
    keep_keys=None,
    skip_existing=False,
    write_raw_llm=True,
    max_candidates=None,
)