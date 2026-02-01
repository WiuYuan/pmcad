# test/map_ontology/map_all_ontology_net.py
import os
import time
import multiprocessing
from typing import Optional

# 直接复用原脚本里的所有实现（Ontology 构建、run_map_stage/run_convert_stage、queue 命名等）
import test.map_ontology.map_all_ontology as base


if __name__ == "__main__":
    # 与原脚本一致：强制 fork
    ctx = multiprocessing.get_context("fork")

    os.makedirs(base.LOG_DIR, exist_ok=True)
    print(f"[main-net] LOG_DIR={base.LOG_DIR}", flush=True)

    NETWORK_DBS = {"uniprot", "rnacentral"}

    # ---- build Ontology objects once ----
    ontologies = []
    for cfg in base.ONTOLOGY_CONFIGS:
        db_type = cfg["db_type"]
        ontologies.append(
            base.build_ontology(
                cfg=cfg,
                judge_method=base._judge_method_for_db(db_type),
                use_species=base._use_species_for_db(db_type),
            )
        )
    ot_by_db = {ot.db_type: ot for ot in ontologies}

    mesh_done = base.q_map("mesh")

    species_ot = ot_by_db["taxon"]
    cvcl_ot = ot_by_db["cvcl"]

    # ============================================================
    # 依赖图：保持与原脚本一致（不改变 op 要求/顺序）
    # ============================================================
    map_deps: dict[str, list[str]] = {}
    map_deps["mesh"] = [base.UPSTREAM_DONE_QUEUE_NAME]
    for cfg in base.ONTOLOGY_CONFIGS:
        db = cfg["db_type"]
        if db == "mesh":
            continue
        map_deps[db] = [base.UPSTREAM_DONE_QUEUE_NAME, mesh_done]

    conv_deps: dict[tuple[str, str], list[str]] = {}

    # chebi -> uniprot（抽离运行）
    conv_deps[("chebi", "uniprot")] = [
        base.UPSTREAM_DONE_QUEUE_NAME,
        mesh_done,
        base.q_map("chebi"),
    ]

    # cl -> cvcl（不在本脚本跑，但 uniprot/rnacentral 的 deps 需要它的 done queue）
    # conv_deps[("cl","cvcl")] 这里不定义也行；只要 done queue 已由另一脚本产出即可。

    # uniprot mapping deps（保持原样）
    map_deps["uniprot"] = [
        base.UPSTREAM_DONE_QUEUE_NAME,
        mesh_done,
        base.q_map("taxon"),
        base.q_conv("cl", "cvcl"),
        base.q_conv("chebi", "uniprot"),
    ]

    # rnacentral mapping deps（保持原样）
    map_deps["rnacentral"] = [
        base.UPSTREAM_DONE_QUEUE_NAME,
        mesh_done,
        base.q_map("taxon"),
        base.q_conv("cl", "cvcl"),
    ]

    # rnacentral -> so -> go（抽离运行，保持串行）
    conv_deps[("rnacentral", "so")] = [
        base.UPSTREAM_DONE_QUEUE_NAME,
        mesh_done,
        base.q_map("rnacentral"),
    ]
    conv_deps[("rnacentral", "go")] = [
        base.UPSTREAM_DONE_QUEUE_NAME,
        mesh_done,
        base.q_map("rnacentral"),
        base.q_conv("rnacentral", "so"),
    ]

    # ================
    # mesh convert 链（本脚本只跑 rnacentral/uniprot 相关两段，并保持原链路顺序）
    # rnacentral->mesh 还必须等 rnacentral->go 完成（与原脚本一致）
    # ================
    prev_done: Optional[str] = None

    # rnacentral -> mesh
    deps = [
        base.UPSTREAM_DONE_QUEUE_NAME,
        mesh_done,
        base.q_map("rnacentral"),
        base.q_conv("rnacentral", "go"),
    ]
    if prev_done:
        deps.append(prev_done)
    conv_deps[("rnacentral", "mesh")] = deps
    prev_done = base.q_conv("rnacentral", "mesh")

    # uniprot -> mesh（串行：等 rnacentral->mesh）
    deps = [
        base.UPSTREAM_DONE_QUEUE_NAME,
        mesh_done,
        base.q_map("uniprot"),
        prev_done,
    ]
    conv_deps[("uniprot", "mesh")] = deps
    prev_done = base.q_conv("uniprot", "mesh")

    # ============================================================
    # 启动：仅启动包含 uniprot / rnacentral 的后台 stage
    # ============================================================
    procs: list[multiprocessing.Process] = []

    # ---- mapping processes（仅网络 db）----
    for db_type in ["uniprot", "rnacentral"]:
        pname = f"map-{db_type}"
        p = ctx.Process(
            target=base.run_map_stage,
            kwargs=dict(
                db_type=db_type,
                ot=ot_by_db[db_type],
                species_ot=species_ot,
                cvcl_ot=cvcl_ot,
                op_done_queues=map_deps[db_type],
                workers=1,
                log_path=os.path.join(base.LOG_DIR, f"{pname}.log"),
            ),
            name=pname,
            daemon=False,
        )
        p.start()
        procs.append(p)

    # ---- convert processes（仅包含网络 db 的）----
    for (src_db, tgt_db), deps in conv_deps.items():
        # 这里 conv_deps 已经只放了“抽离出来的”几条
        pname = f"conv-{src_db}-to-{tgt_db}"
        p = ctx.Process(
            target=base.run_convert_stage,
            kwargs=dict(
                src_ot=ot_by_db[src_db],
                tgt_ot=ot_by_db[tgt_db],
                species_ot=species_ot,
                cvcl_ot=cvcl_ot,
                op_done_queues=deps,
                workers=1,
                log_path=os.path.join(base.LOG_DIR, f"{pname}.log"),
            ),
            name=pname,
            daemon=False,
        )
        p.start()
        procs.append(p)

    for p in procs:
        p.join()

    bad = [(p.name, p.exitcode) for p in procs if p.exitcode not in (0, None)]
    if bad:
        raise RuntimeError(f"Some stages failed: {bad}")