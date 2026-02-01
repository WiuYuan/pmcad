
# test/map_ontology/map_all_ontology.py
import os
import sys
import time
import contextlib
import multiprocessing
from typing import Optional

import torch
from sentence_transformers import SentenceTransformer, SparseEncoder

from src.services.llm import LLM
from src.pmcad.pmidstore import PMIDStore
from src.pmcad.parallel_process import process_folder_parallel
from src.pmcad.ontology_map import (
    search_ontology,
    Ontology,
    process_one_folder_get_db_id,
    process_one_folder_judge_db_id,
)
from src.pmcad.db_change import process_one_folder_convert_failed
from src.services.uniprot import search_uniprot
from src.services.rnacentral import search_rnacentral
from src.pmcad.taxon_search import search_taxon


# ============================================================
# DB / Queue config
# ============================================================
DB_PATH = "/data/wyuan/workspace/pmcdata_pro/pmid_abs.sqlite3"

# 上游：ontology_decomposition.py 跑完后的 done queue（它代表 qw_decomposed.json 已产出）
UPSTREAM_DONE_QUEUE_NAME = "llm_relations_qwen3_decomp"

# 上游产物文件名（存在 files 表里）
FILE_PREFIX = "qw"
CORE_FILE_NAME = f"{FILE_PREFIX}_decomposed.json"

ES_CONFIG = "/data/wyuan/workspace/pmcdata_pro/pmcad/config/elasticsearch.yaml"

# ============================================================
# Logging
# ============================================================
# 所有后台子进程 stdout/stderr 都会重定向到该目录下的独立 log 文件
LOG_DIR = os.environ.get(
    "PMCAD_LOG_DIR",
    os.path.join(os.path.dirname(__file__), "logs"),
)

@contextlib.contextmanager
def _redirect_to_log(log_path: Optional[str]):
    if not log_path:
        yield
        return
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    # buffering=1: line-buffered（尽量实时写入）
    with open(log_path, "a", encoding="utf-8", buffering=1) as f:
        with contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
            yield


def q_map(db_type: str) -> str:
    # 每个 ontology mapping stage 自己的 done queue
    return f"{FILE_PREFIX}__map__{db_type}"


def q_conv(src_db: str, tgt_db: str) -> str:
    # convert_failed stage 自己的 done queue
    return f"{FILE_PREFIX}__conv__{src_db}__to__{tgt_db}"


# ============================================================
# LLM config (align with other code: 18001/18002)
# ============================================================
llm_list = [
    LLM(
        model_name="qwen3-8b",
        llm_url="http://0.0.0.0:18002/v1/chat/completions",
        format="openai",
    )
]


# ============================================================
# Models (dense + SPLADE)
# ============================================================
device = "cpu"

dense_model = SentenceTransformer(
    "/data/wyuan/.cache/huggingface/hub/models--pritamdeka--BioBERT-mnli-snli-scinli-scitail-mednli-stsb/snapshots/82d44689be9cf3c6c6a6f77cc3171c93282873a1",
    device=device,
)

splade_model = SparseEncoder(
    model_name_or_path="/data/wyuan/.cache/huggingface/hub/models--NeuML--pubmedbert-base-splade/snapshots/f284fcbafe4761108f27357f5278d846a630059e",
    device=device,
)


# ============================================================
# Ontology builders
# ============================================================
def build_search_func(db_type: str):
    # ---- special DBs ----
    if db_type in ["uniprot", "rnacentral", "taxon"]:
        if db_type == "uniprot":
            return lambda query: search_uniprot(query=query, k=30)
        if db_type == "rnacentral":
            return lambda query: search_rnacentral(query=query, k=30)
        if db_type == "taxon":
            return lambda query: search_taxon(config_path=ES_CONFIG, query=query, k=30)
        raise ValueError(f"Unknown special search builder: {db_type}")

    # ---- default ontology DBs ----
    extra_source_fields = None
    if db_type == "cvcl":
        extra_source_fields = ["species"]

    return lambda query: search_ontology(
        query=query,
        search_type="dense+splade",
        index_name=f"{db_type}_index",
        config_path=ES_CONFIG,
        dense_model=dense_model,
        splade_model=splade_model,
        k=30,
        extra_source_fields=extra_source_fields,
        verbose=False,
    )


def build_ontology(cfg, judge_method="strict", use_species=False) -> Ontology:
    db_type = cfg["db_type"]
    return Ontology(
        ontology_type=cfg["ontology_type"],
        db_type=db_type,
        search_func=build_search_func(db_type),
        filename=f"{FILE_PREFIX}_{db_type}.json",
        judge_method=judge_method,
        use_species=use_species,
    )


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


def _judge_method_for_db(db_type: str) -> str:
    if db_type in ["ro"]:
        return "forced"
    if db_type in ["so", "go"]:
        return "relaxed"
    return "strict"


def _use_species_for_db(db_type: str) -> bool:
    # 与原脚本一致：uniprot/rnacentral 拼 species 作为 query
    return db_type in ["uniprot", "rnacentral"]


# ============================================================
# Stage runners (后台子进程)
# ============================================================
def run_map_stage(
    *,
    db_type: str,
    ot: Ontology,
    species_ot: Optional[Ontology],
    cvcl_ot: Optional[Ontology],
    op_done_queues: list[str],
    workers: int = 64,
    log_path: Optional[str] = None,
):
    with _redirect_to_log(log_path):
        print(f"[{time.strftime('%F %T')}] START map db={db_type} pid={os.getpid()} deps={op_done_queues}", flush=True)

        store = PMIDStore(DB_PATH, readonly=False)

        # 两步流水：get_db_id -> judge_db_id
        # 对 uniprot/rnacentral，第一步（检索）限流一下
        if db_type in ["uniprot", "rnacentral"]:
            max_worker_list = [3, workers]
        else:
            max_worker_list = [workers, workers]

        process_folder_parallel(
            store=store,
            llm_list=llm_list,
            process_one_folder=[process_one_folder_get_db_id, process_one_folder_judge_db_id],
            workers=workers,
            pmidlist=None,  # queue mode: 用 abs 表全量作为退出目标集合
            input_name=CORE_FILE_NAME,
            ot=ot,
            species_ot=species_ot,
            cvcl_ot=cvcl_ot,
            max_worker_list=max_worker_list,
            op_queue_names=op_done_queues,   # 关键：op 直接接上游 done
            done_queue_name=q_map(db_type),  # 关键：本 stage 自己的 done
            clear_done_on_start=True,
        )

        store.close()
        print(f"[{time.strftime('%F %T')}] END   map db={db_type} pid={os.getpid()}", flush=True)


def run_convert_stage(
    *,
    src_ot: Ontology,
    tgt_ot: Ontology,
    species_ot: Optional[Ontology],
    cvcl_ot: Optional[Ontology],
    op_done_queues: list[str],
    workers: int = 64,
    log_path: Optional[str] = None,
):
    with _redirect_to_log(log_path):
        print(
            f"[{time.strftime('%F %T')}] START conv {src_ot.db_type}->{tgt_ot.db_type} pid={os.getpid()} deps={op_done_queues}",
            flush=True,
        )

        store = PMIDStore(DB_PATH, readonly=False)

        process_folder_parallel(
            store=store,
            llm_list=llm_list,
            process_one_folder=process_one_folder_convert_failed,
            workers=workers,
            pmidlist=None,
            input_name=CORE_FILE_NAME,
            src_ot=src_ot,
            tgt_ot=tgt_ot,
            species_ot=species_ot,
            cvcl_ot=cvcl_ot,
            op_queue_names=op_done_queues,
            done_queue_name=q_conv(src_ot.db_type, tgt_ot.db_type),
            clear_done_on_start=True,
        )

        store.close()
        print(f"[{time.strftime('%F %T')}] END   conv {src_ot.db_type}->{tgt_ot.db_type} pid={os.getpid()}", flush=True)


if __name__ == "__main__":
    # 由于 target 里使用了闭包/大模型对象，这里强制用 fork，避免 spawn 需要 pickle
    ctx = multiprocessing.get_context("fork")

    os.makedirs(LOG_DIR, exist_ok=True)
    print(f"[main] LOG_DIR={LOG_DIR}", flush=True)

    # ============================================================
    # NOTE:
    # uniprot / rnacentral 需要访问网络；你计划不在计算节点跑。
    # 因此本脚本会“跳过启动”所有包含 uniprot / rnacentral 的后台子进程：
    # - map-uniprot / map-rnacentral
    # - conv-chebi-to-uniprot
    # - conv-rnacentral-to-*
    # - conv-uniprot-to-*
    #
    # 这些 stage 请使用单独脚本运行（见你让我新增的程序）。
    # 依赖(done queue) 与顺序(op_queue_names) 在这里保持不变：
    # 其它 stage 仍然会等待相应 done queue（由新脚本产出）。
    # ============================================================
    NETWORK_DBS = {"uniprot", "rnacentral"}

    # ---- build Ontology objects once (fork 后子进程可复用内存页) ----
    ontologies = []
    for cfg in ONTOLOGY_CONFIGS:
        db_type = cfg["db_type"]
        ontologies.append(
            build_ontology(
                cfg=cfg,
                judge_method=_judge_method_for_db(db_type),
                use_species=_use_species_for_db(db_type),
            )
        )
    ot_by_db = {ot.db_type: ot for ot in ontologies}

    mesh_ot = ot_by_db["mesh"]
    species_ot = ot_by_db["taxon"]
    cvcl_ot = ot_by_db["cvcl"]

    go_ot = ot_by_db["go"]
    so_ot = ot_by_db["so"]

    # ============================================================
    # 依赖图（op = 上游 done 队列）
    # 只保证：每个 stage 在“调用时”其输入一定已经由上游产出完成（用 op_done_intersection 实现）
    #
    # 约束：
    # - mesh 必须第一：所有其它 stage 都依赖 mesh_done
    # - uniprot / rnacentral：依赖 taxon_done + cl->cvcl_done
    # - so / go：依赖 rnacentral->(so/go) convert_done（该步骤会改写 CORE_FILE_NAME 的 type）
    # ============================================================

    mesh_done = q_map("mesh")

    # mapping deps：必须覆盖所有 ONTOLOGY_CONFIGS，否则下面启动会 KeyError
    map_deps: dict[str, list[str]] = {}
    map_deps["mesh"] = [UPSTREAM_DONE_QUEUE_NAME]

    # 默认：其它全部先依赖 (UPSTREAM + mesh_done)，保证 mesh-first
    for cfg in ONTOLOGY_CONFIGS:
        db = cfg["db_type"]
        if db == "mesh":
            continue
        map_deps[db] = [UPSTREAM_DONE_QUEUE_NAME, mesh_done]

    # convert deps
    # 注意：convert_failed 会写回 CORE_FILE_NAME + 写回 src_ot.filename + 写回 tgt_ot.filename
    # 因此必须用 done 链条串行，避免同 PMID 并发写导致“后写覆盖先写”。
    conv_deps: dict[tuple[str, str], list[str]] = {}

    # ============================================================
    # ✅ NEW: chemical(chebi) -> uniprot 的 fail 映射链
    # 逻辑：
    #   1) 先完成 chebi mapping（chemical 先找自己的）
    #   2) 再把 chebi 中 unresolved 的条目 convert 到 uniprot
    #   3) 后续再允许进入 chebi->mesh 等操作（见 mesh_chain deps）
    #
    # 并发安全：
    # - 该 convert 会写回 uniprot 文件与 CORE_FILE_NAME，因此必须让 uniprot mapping 等它完成
    # ============================================================
    conv_deps[("chebi", "uniprot")] = [
        UPSTREAM_DONE_QUEUE_NAME,
        mesh_done,
        q_map("chebi"),
    ]

    # cl -> cvcl：需要 cl/cvcl mapping 已完成（且 mesh-first）
    conv_deps[("cl", "cvcl")] = [
        UPSTREAM_DONE_QUEUE_NAME,
        mesh_done,
        q_map("cl"),
        q_map("cvcl"),
    ]

    # uniprot / rnacentral：需要 taxon + cl->cvcl 已完成（且 mesh-first）
    map_deps["uniprot"] = [
        UPSTREAM_DONE_QUEUE_NAME,
        mesh_done,
        q_map("taxon"),
        q_conv("cl", "cvcl"),
        q_conv("chebi", "uniprot"),  # ✅ NEW: 等 chemical->uniprot convert 完成，避免并发写 uniprot 文件
    ]
    map_deps["rnacentral"] = [
        UPSTREAM_DONE_QUEUE_NAME,
        mesh_done,
        q_map("taxon"),
        q_conv("cl", "cvcl"),
    ]

    # rnacentral -> so -> go：必须串行（两者都会改写 CORE_FILE_NAME + 清理 rnacentral 文件）
    conv_deps[("rnacentral", "so")] = [
        UPSTREAM_DONE_QUEUE_NAME,
        mesh_done,
        q_map("rnacentral"),
    ]
    conv_deps[("rnacentral", "go")] = [
        UPSTREAM_DONE_QUEUE_NAME,
        mesh_done,
        q_map("rnacentral"),
        q_conv("rnacentral", "so"),  # ✅ 串行：go 必须等 so 完成
    ]

    # so / go mapping：必须等对应 convert 完成
    map_deps["so"] = [
        UPSTREAM_DONE_QUEUE_NAME,
        mesh_done,
        q_conv("rnacentral", "so"),
    ]
    map_deps["go"] = [
        UPSTREAM_DONE_QUEUE_NAME,
        mesh_done,
        q_conv("rnacentral", "go"),
    ]

    # ================
    # 串行 mesh convert 链
    # ================
    mesh_chain = [
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

    prev_done: Optional[str] = None
    for (src, tgt) in mesh_chain:
        deps = [
            UPSTREAM_DONE_QUEUE_NAME,
            mesh_done,   # mesh-first + 避免与 mesh 初始 mapping 冲突
            q_map(src),  # src mapping 必须先有
        ]

        # rnacentral->mesh 必须等 rnacentral->go 完成（确保 rnacentral 清理完成、CORE_FILE_NAME 最终态）
        if src == "rnacentral":
            deps.append(q_conv("rnacentral", "go"))

        # ✅ NEW: chebi->mesh 必须等 chebi->uniprot 先跑完（chemical 先自查，再尝试转 uniprot，最后再转 mesh）
        if src == "chebi":
            deps.append(q_conv("chebi", "uniprot"))

        # 串行化：每个 mesh convert 等上一个 mesh convert done
        if prev_done:
            deps.append(prev_done)

        conv_deps[(src, tgt)] = deps
        prev_done = q_conv(src, tgt)

    # ============================================================
    # 启动所有后台 stage：主进程不串行阻塞；由 op_done_intersection 自己“等上游”
    # ============================================================
    procs: list[multiprocessing.Process] = []

    # ---- mapping processes ----
    for db_type in [cfg["db_type"] for cfg in ONTOLOGY_CONFIGS]:
        # 跳过需要网络的 stage（抽离到独立脚本运行）
        if db_type in NETWORK_DBS:
            # 原本会启动的子进程（已抽离）：
            #   pname = f"map-{db_type}"
            #   ... run_map_stage(...)
            continue

        ot = ot_by_db[db_type]
        pname = f"map-{db_type}"
        p = ctx.Process(
            target=run_map_stage,
            kwargs=dict(
                db_type=db_type,
                ot=ot,
                species_ot=species_ot,
                cvcl_ot=cvcl_ot,
                op_done_queues=map_deps[db_type],
                workers=4,
                log_path=os.path.join(LOG_DIR, f"{pname}.log"),
            ),
            name=pname,
            daemon=False,
        )
        p.start()
        procs.append(p)

    # ---- convert processes ----
    for (src_db, tgt_db), deps in conv_deps.items():
        # 跳过所有“包含 uniprot / rnacentral”的 convert stage（抽离到独立脚本运行）
        if (src_db in NETWORK_DBS) or (tgt_db in NETWORK_DBS):
            # 原本会启动的子进程（已抽离）：
            #   pname = f"conv-{src_db}-to-{tgt_db}"
            #   ... run_convert_stage(...)
            continue

        pname = f"conv-{src_db}-to-{tgt_db}"
        p = ctx.Process(
            target=run_convert_stage,
            kwargs=dict(
                src_ot=ot_by_db[src_db],
                tgt_ot=ot_by_db[tgt_db],
                species_ot=species_ot,
                cvcl_ot=cvcl_ot,
                op_done_queues=deps,
                workers=1,
                log_path=os.path.join(LOG_DIR, f"{pname}.log"),
            ),
            name=pname,
            daemon=False,
        )
        p.start()
        procs.append(p)

    # （已删除重复的 convert 启动循环）
    # convert 子进程只应在上面的“带 NETWORK_DBS skip 判断”的循环中启动一次，
    # 否则会导致包含 uniprot/rnacentral 的 convert 仍在本脚本被启动。

    # 主进程等待所有后台 stage 结束
    for p in procs:
        p.join()

    # 若需要严格失败检测：任何子进程 exitcode!=0 则 raise
    bad = [(p.name, p.exitcode) for p in procs if p.exitcode not in (0, None)]
    if bad:
        raise RuntimeError(f"Some stages failed: {bad}")
