# test/get_llm_relation.py
import os
from src.services.llm import LLM
from src.pmcad.parallel_process import process_folder_parallel
from src.pmcad.extract_relations import process_one_folder_llm_get_relations
from src.pmcad.pmidstore import PMIDStore

DB_PATH = "/data/wyuan/workspace/pmcdata_pro/pmid_abs.sqlite3"
DONE_QUEUE_NAME = "llm_relations_qwen3"
OUTPUT_FILE_NAME = "qw.json"

# 你本地起了两个 openai-compatible server：18001 / 18002
# model_name 默认用你 curl 里看到的 id（也可用环境变量覆盖）
llm_list = [
    LLM(
        model_name="qwen3-14b",
        llm_url="http://0.0.0.0:18001/v1/chat/completions",
        format="openai",
    )
]

store = PMIDStore(DB_PATH, readonly=False)
# store.queue_done_clear(DONE_QUEUE_NAME)

pmidlist = None  # None => 跑完整个 DB 的 pmids；也可传 list[int] 控制子集
process_folder_parallel(
    store=store,
    llm_list=llm_list,
    process_one_folder=process_one_folder_llm_get_relations,
    workers=32,
    pmidlist=pmidlist,
    output_file_name=OUTPUT_FILE_NAME,  # 传给 process_one_folder_llm_get_relations
    op_queue_names=None,                # 按你的要求：不使用 op 队列
    done_queue_name=DONE_QUEUE_NAME,    # 用 done 队列做断点/去重
    clear_done_on_start=True,
    # limit=1,
)
