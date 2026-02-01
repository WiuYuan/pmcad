
# test/ontology_decomposition.py
import os
from src.services.llm import LLM
from src.pmcad.parallel_process import process_folder_parallel
from src.pmcad.ontology_decompostion import process_one_folder_entity_decomposition
from src.pmcad.pmidstore import PMIDStore

DB_PATH = "/data/wyuan/workspace/pmcdata_pro/pmid_abs.sqlite3"

# 上游：get_llm_relation.py 用的 done queue（本脚本将其作为 op 输入来源）
REL_DONE_QUEUE_NAME = "llm_relations_qwen3"

# 下游：本 stage 自己的 done queue
DECOMP_DONE_QUEUE_NAME = "llm_relations_qwen3_decomp"

# 上游产物文件名（存在 files 表里）
INPUT_FILE_NAME = "qw.json"
OUTPUT_FILE_NAME = "qw_decomposed.json"

llm_list = [
    LLM(
        model_name="qwen3-8b",
        llm_url="http://0.0.0.0:18002/v1/chat/completions",
        format="openai",
    ),
]

store = PMIDStore(DB_PATH, readonly=False)

# queue mode: pmidlist=None => process_folder_parallel 内部自动用 store.get_pmids() 作为退出目标集合
# 下游会等待上游逐步把 pmid 写入 op 队列（由 queue_mark_done 同步 done->op 实现）
pmidlist = None


# 如需重跑本 stage，可清空本 stage done
# store.queue_done_clear(DECOMP_DONE_QUEUE_NAME)

process_folder_parallel(
    store=store,
    llm_list=llm_list,
    process_one_folder=process_one_folder_entity_decomposition,
    workers=1,
    pmidlist=pmidlist,
    input_name=INPUT_FILE_NAME,
    output_name=OUTPUT_FILE_NAME,
    op_queue_names=[REL_DONE_QUEUE_NAME],        # 关键：op 接上游 stage 的 done
    done_queue_name=DECOMP_DONE_QUEUE_NAME,      # 关键：本 stage 自己的 done
    clear_done_on_start=True,
)