
# test/map_ontology/get_final.py
"""
最终汇总阶段（DB + Queue mode）：
- 读取：qw_decomposed.json
- 结合各 ontology stage 的 llm_best_match
- 输出：qw_final.json

要求：
- op_queue_names = test/map_ontology/map_all_ontology.py 中“所有 stage 的 done queue”（map + conv + 上游 done）
- 代码尽量精简，删除不需要的依赖与模型加载
"""

import os
from typing import List, Tuple

from src.pmcad.pmidstore import PMIDStore
from src.pmcad.parallel_process import process_folder_parallel
from src.pmcad.ontology_map import Ontology, process_one_folder_apply_llm_best


# ============================================================
# DB / files
# ============================================================
DB_PATH = os.environ.get("PMCAD_DB_PATH", "/data/wyuan/workspace/pmcdata_pro/pmid_abs.sqlite3")

FILE_PREFIX = "qw"
CORE_FILE_NAME = f"{FILE_PREFIX}_decomposed.json"
OUTPUT_NAME = f"{FILE_PREFIX}_final.json"


# ============================================================
# Queue names (必须与 test/map_ontology/map_all_ontology.py 保持一致)
# ============================================================
UPSTREAM_DONE_QUEUE_NAME = "llm_relations_qwen3_decomp"

def q_map(db_type: str) -> str:
    return f"{FILE_PREFIX}__map__{db_type}"

def q_conv(src_db: str, tgt_db: str) -> str:
    return f"{FILE_PREFIX}__conv__{src_db}__to__{tgt_db}"

FINAL_DONE_QUEUE_NAME = f"{FILE_PREFIX}__final"


# ============================================================
# Ontology list (apply_llm_best 只用到 filename / type / use_species)
# 不需要 search model / ES config
# ============================================================
ONTOLOGY_CONFIGS = [
    {"ontology_type": "MeSH", "db_type": "mesh"},
    {"ontology_type": "species", "db_type": "taxon"},
    {"ontology_type": "cell_line", "db_type": "cvcl"},
    {"ontology_type": "cell_type", "db_type": "cl"},
    {"ontology_type": ["protein", "gene"], "db_type": "uniprot"},
    {"ontology_type": "RNA", "db_type": "rnacentral"},
    {"ontology_type": "chemical", "db_type": "chebi"},
    {"ontology_type": "domain", "db_type": "interpro"},
    {"ontology_type": "GO", "db_type": "go"},
    {"ontology_type": "SO", "db_type": "so"},
    {"ontology_type": "disease", "db_type": "doid"},
    {"ontology_type": "anatomy", "db_type": "uberon"},
    # {"ontology_type": "relation", "db_type": "ro"},
]

def _use_species_for_db(db_type: str) -> bool:
    return db_type in ["uniprot", "rnacentral"]

def build_ontology(cfg) -> Ontology:
    db_type = cfg["db_type"]
    return Ontology(
        ontology_type=cfg["ontology_type"],
        db_type=db_type,
        use_species=_use_species_for_db(db_type),
        filename=f"{FILE_PREFIX}_{db_type}.json",
        # apply_llm_best 不会调用 search_func / judge_method
        search_func=None,
    )

# apply 需要的 ot_list（可包含全部 ontology；即使某些类型不在 relations 中也无害）
OT_LIST = [build_ontology(cfg) for cfg in ONTOLOGY_CONFIGS]

# species / cvcl 用于物种对齐（resolve_species / cell_line->species 兜底）
SPECIES_OT = build_ontology({"ontology_type": "species", "db_type": "taxon"})
CVCL_OT = build_ontology({"ontology_type": "cell_line", "db_type": "cvcl"})


# ============================================================
# op queues = “所有 done queues”
# ============================================================
def build_all_op_done_queues() -> List[str]:
    op: List[str] = [UPSTREAM_DONE_QUEUE_NAME]

    # 1) all mapping stages
    for cfg in ONTOLOGY_CONFIGS:
        op.append(q_map(cfg["db_type"]))

    # 2) all convert stages（与 map_all_ontology.py 的 conv_deps / mesh_chain 对齐）
    conv_pairs: List[Tuple[str, str]] = [
        ("chebi", "uniprot"),
        ("cl", "cvcl"),
        ("rnacentral", "so"),
        ("rnacentral", "go"),
        ("rnacentral", "mesh"),
        ("uniprot", "mesh"),
        ("so", "mesh"),
        ("go", "mesh"),
        ("chebi", "mesh"),
        ("interpro", "mesh"),
        ("cvcl", "mesh"),
        ("doid", "mesh"),
        ("uberon", "mesh"),
    ]
    for s, t in conv_pairs:
        op.append(q_conv(s, t))

    # 去重保序
    seen = set()
    out = []
    for x in op:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out

OP_DONE_QUEUES = build_all_op_done_queues()


def main(*, workers: int = 64, clear_done_on_start: bool = False):
    store = PMIDStore(DB_PATH, readonly=False)
    try:
        process_folder_parallel(
            store=store,
            llm_list=[None],  # 本 stage 不用 llm，但 parallel_process 会透传一个 llm 参数
            process_one_folder=process_one_folder_apply_llm_best,
            workers=workers,
            pmidlist=None,  # queue mode: 以 abs 表全集作为 target
            input_name=CORE_FILE_NAME,
            output_name=OUTPUT_NAME,
            ot_list=OT_LIST,
            species_ot=SPECIES_OT,
            cvcl_ot=CVCL_OT,
            op_queue_names=OP_DONE_QUEUES,
            done_queue_name=FINAL_DONE_QUEUE_NAME,
            clear_done_on_start=clear_done_on_start,
        )
    finally:
        store.close()


if __name__ == "__main__":
    workers = int(os.environ.get("PMCAD_FINAL_WORKERS", "64"))
    clear = True
    print(
        f"[get_final] DB_PATH={DB_PATH} input={CORE_FILE_NAME} output={OUTPUT_NAME} "
        f"workers={workers} clear_done={clear} done_queue={FINAL_DONE_QUEUE_NAME}"
    )
    print(f"[get_final] op_queues={OP_DONE_QUEUES}")
    main(workers=workers, clear_done_on_start=clear)
