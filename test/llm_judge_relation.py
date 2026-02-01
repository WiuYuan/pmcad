
# test/llm_judge_relation.py
import os

from src.pmcad.llm_judge_relation import process_one_pmid_validate_relations
from src.pmcad.parallel_process import process_folder_parallel
from src.pmcad.pmidstore import PMIDStore
from src.services.llm import LLM

# ============================================================
# DB / files
# ============================================================
DB_PATH = os.environ.get("PMCAD_DB_PATH", "/data/wyuan/workspace/pmcdata_pro/pmid_abs.sqlite3")

FILE_PREFIX = "qw"
INPUT_NAME = f"{FILE_PREFIX}_final.json"   # op 要求：qw_final.json 出现后才能跑
OUTPUT_NAME = f"{FILE_PREFIX}_final.json"  # 原地写回（只补充 valid 字段）

# ============================================================
# Queue names
# ============================================================
UPSTREAM_DONE_QUEUE_NAME = f"{FILE_PREFIX}__final"          # get_final.py 的 FINAL_DONE_QUEUE_NAME
DONE_QUEUE_NAME = f"{FILE_PREFIX}__judge_relation"          # 本 stage 自己的 done 队列

# ============================================================
# LLM config (align with other code: 18001/18002)
# ============================================================
llm_list = [
    LLM(
        model_name="qwen3-8b",
        llm_url="http://0.0.0.0:18002/v1/chat/completions",
        format="openai",
    ),
]


def main(*, workers: int = 16, clear_done_on_start: bool = False):
    store = PMIDStore(DB_PATH, readonly=False)
    try:
        process_folder_parallel(
            store=store,
            llm_list=llm_list,
            process_one_folder=process_one_pmid_validate_relations,
            workers=workers,
            pmidlist=None,  # queue mode：以 abs 表全集为 target，并等待上游队列产出
            op_queue_names=[UPSTREAM_DONE_QUEUE_NAME],
            done_queue_name=DONE_QUEUE_NAME,
            clear_done_on_start=clear_done_on_start,
            # stage args
            input_name=INPUT_NAME,
            output_name=OUTPUT_NAME,
            prereq_name=INPUT_NAME,     # ✅ gating：qw_final.json 存在才会执行
            skip_existing=False,
            judge_method="strict",
        )
    finally:
        store.close()


if __name__ == "__main__":
    workers = 2
    clear = True
    print(
        f"[llm_judge_relation] DB_PATH={DB_PATH} input={INPUT_NAME} output={OUTPUT_NAME} "
        f"workers={workers} clear_done={clear} upstream_done={UPSTREAM_DONE_QUEUE_NAME} done={DONE_QUEUE_NAME}"
    )
    main(workers=workers, clear_done_on_start=clear)
