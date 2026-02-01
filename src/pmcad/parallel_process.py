# src/pmcad/parallel_process.py
import os
import itertools
import json
from tqdm import tqdm
import concurrent.futures
import threading
from typing import Union
from pathlib import Path
from src.pmcad.pmidstore import PMIDStore
import time


def process_one_folder_delete_file(folder: str, filename: str, **kwargs):
    """
    删除 folder 下的某个文件。
    parallel_process 兼容：返回 (None, info_list)
    """
    pmid = os.path.basename(folder)
    target_path = os.path.join(folder, filename)

    if os.path.exists(target_path):
        try:
            os.remove(target_path)
            return None, [
                {"type": "status", "description": f"deleted {filename} pmid:{pmid}"},
                {"type": "metric", "correct": 1, "total": 1},
            ]
        except Exception as e:
            return None, [
                {"type": "error", "msg": f"delete fail pmid:{pmid}"},
                {"type": "metric", "correct": 0, "total": 1},
            ]
    else:
        return None, [
            {"type": "error", "msg": f"skip pmid:{pmid} (no file)"},
            {"type": "metric", "correct": 1, "total": 1},
        ]


def _dict_similarity(d1, d2):
    """
    计算两个 dict 之间有多少个 key-value 完全相同。
    """
    if not isinstance(d1, dict) or not isinstance(d2, dict):
        return 0
    count = 0
    for k, v in d1.items():
        if k in d2 and d2[k] == v:
            count += 1
    return count


def _deep_merge_json(base, override):
    """
    深度合并 JSON：
      - dict-dict: 递归
      - list-list: 智能匹配（≥2相同key-value则合并），否则 append
      - 其他: override 覆盖 base
    """

    # -----------------------------
    # case 1: dict-dict
    # -----------------------------
    if isinstance(base, dict) and isinstance(override, dict):
        merged = base.copy()
        for k, v_override in override.items():
            if k in merged:
                merged[k] = _deep_merge_json(merged[k], v_override)
            else:
                merged[k] = v_override
        return merged

    # -----------------------------
    # case 2: list-list (智能匹配)
    # -----------------------------
    if isinstance(base, list) and isinstance(override, list):

        # base 复制一份用于结果
        merged = base.copy()

        for o in override:
            if not isinstance(o, dict):
                # 如果不是 dict，就直接 append（无法智能匹配）
                merged.append(o)
                continue

            # 尝试在 base 匹配 ≥2 key-value 的对象
            best_match = None
            for b in merged:
                if isinstance(b, dict):
                    same = _dict_similarity(b, o)
                    if same >= 2:  # ★ 你的智能匹配规则
                        best_match = b
                        break

            if best_match is not None:
                # 找到匹配对象 → 递归合并
                merged[merged.index(best_match)] = _deep_merge_json(best_match, o)
            else:
                # 没找到 → 新对象 append
                merged.append(o)

        return merged

    # -----------------------------
    # case 3: 其他 → 直接覆盖
    # -----------------------------
    return override


def process_one_folder_merge_json(
    folder: str, file1: str, file2: str, output_name: str = "merged.json", **kwargs
):
    """
    合并两个 JSON 文件：
      - 递归合并所有 key
      - key 冲突时，file1 的值优先
      - 子对象为 dict 时递归合并
    parallel_process 兼容：返回 (merged_dict, info_list)
    """
    pmid = os.path.basename(folder)
    path1 = os.path.join(folder, file1)
    path2 = os.path.join(folder, file2)
    out_path = os.path.join(folder, output_name)

    # 读取 file1（高优先级）
    try:
        with open(path1, "r", encoding="utf-8") as f:
            data1 = json.load(f)
    except Exception:
        return None, [
            {"type": "error", "msg": f"skip pmid:{pmid} (missing {file1})"},
            {"type": "metric", "correct": 0, "total": 1},
        ]

    # 读取 file2（低优先级）
    try:
        with open(path2, "r", encoding="utf-8") as f:
            data2 = json.load(f)
    except Exception:
        # file2 不存在 → 直接写出 file1
        try:
            with open(out_path, "w", encoding="utf-8") as fw:
                json.dump(data1, fw, ensure_ascii=False, indent=2)
            return data1, [
                {"type": "error", "msg": f"only file1 present pmid:{pmid}"},
                {"type": "metric", "correct": 1, "total": 1},
            ]
        except Exception as e:
            return None, [
                {"type": "error", "msg": f"write fail pmid:{pmid}"},
                {"type": "metric", "correct": 0, "total": 1},
            ]

    # ⭐ 递归合并：file1 覆盖 file2
    merged = _deep_merge_json(data2, data1)

    # 写出
    try:
        with open(out_path, "w", encoding="utf-8") as fw:
            json.dump(merged, fw, ensure_ascii=False, indent=2)

        return merged, [
            {"type": "status", "description": f"merged pmid:{pmid}"},
            {"type": "metric", "correct": 1, "total": 1},
        ]

    except Exception as e:
        return None, [
            {"type": "status", "description": f"write fail pmid:{pmid}"},
            {"type": "error", "msg": str(e)},
            {"type": "metric", "correct": 0, "total": 1},
        ]


def process_folder_parallel(
    store: PMIDStore,
    llm_list: list,
    process_one_folder: Union[list[callable], callable],
    workers: int = 16,
    pmidlist: list = None,
    limit: int | None = None,
    max_worker_list: list[int] | None = None,
    op_queue_names: list[str] | None = None,
    done_queue_name: str | None = None,
    queue_sleep: float = 5.0,
    clear_done_on_start: bool = False,   # ✅ NEW: 启动时是否清空 done 队列
    **kwargs,
):
    if callable(process_one_folder):
        process_fns = [process_one_folder]
    else:
        process_fns = list(process_one_folder)

    # ✅ NEW: 启动时清空 done（用于重跑整个 stage）
    if clear_done_on_start and done_queue_name and (not store.readonly):
        cleared_done = store.queue_done_clear(done_queue_name)
        if cleared_done:
            print(f"Cleared done={cleared_done} for stage={done_queue_name}")

    # 启动时：如果指定了 done_queue_name，就清空该 stage 的 inflight
    # （避免上次中断遗留 inflight，导致永远无法再 claim）
    if done_queue_name and (not store.readonly):
        cleared = store.queue_inflight_clear(done_queue_name)
        if cleared:
            print(f"Cleared inflight={cleared} for stage={done_queue_name}")

    # 每个 step 的并发上限：None 表示不限制
    if max_worker_list is None:
        max_worker_list = [workers] * len(process_fns)  # 默认每步都不超过总 workers
    else:
        if len(max_worker_list) == 0:
            raise ValueError("max_worker_list cannot be empty; use None or provide at least one positive int")

        if len(max_worker_list) < len(process_fns):
            max_worker_list = max_worker_list + [max_worker_list[-1]] * (len(process_fns) - len(max_worker_list))
                
    if any((not isinstance(n, int)) or n <= 0 for n in max_worker_list):
        raise ValueError(f"max_worker_list must be positive ints, got: {max_worker_list}")

    step_sems = [threading.Semaphore(n) for n in max_worker_list]
    llm_cycle = itertools.cycle(llm_list)

    # IMPORTANT:
    # SQLite connection is NOT safe for concurrent transactions across threads.
    # Here we create one PMIDStore (one SQLite connection) per worker thread.
    _thread_local = threading.local()

    def _get_worker_store() -> PMIDStore:
        if not hasattr(_thread_local, "store") or _thread_local.store is None:
            _thread_local.store = PMIDStore(
                store.db_path,
                readonly=store.readonly,
            )
        return _thread_local.store

    def _run_pipeline(pmid):
        final_result = None
        all_info_list = []
        worker_store = _get_worker_store()

        for idx, fn in enumerate(process_fns, start=1):
            sem = step_sems[idx - 1]

            prefix = "" if idx == 1 else f"{idx}_"
            retries = 0

            # 默认值，保证即使 3 次都异常也有返回
            result, info_list = None, [{"type": "error", "msg": f"pmid:{pmid} step:{idx} not executed"}]

            while retries < 3:
                sem.acquire()
                try:
                    llm = next(llm_cycle)
                    result, info_list = fn(pmid=pmid, store=worker_store, llm=llm, **kwargs)

                    if any(info.get("type") == "error" for info in info_list):
                        retries += 1
                        time.sleep(2)
                        continue

                    break  # success
                except Exception as e:
                    retries += 1
                    time.sleep(2)
                    result, info_list = None, [{"type": "error", "msg": f"Extra: {pmid} {str(e)}"}]
                finally:
                    sem.release()

            # 给 info 打前缀（关键：用于 tqdm 显示）
            for info in info_list:
                info2 = dict(info)
                info2["__prefix"] = prefix
                all_info_list.append(info2)

            final_result = result

            # 你想遇到 error 就停就加这个逻辑（可选）
            if any(i.get("type") == "error" for i in info_list):
                break

        return final_result, all_info_list
    
    # ---------------------------
    # queue mode (DB-backed)
    # ---------------------------
    # 只有同时提供 op_queue_names + done_queue_name 才进入完整 queue mode（claim/requeue/inflight）
    # 如果只提供 done_queue_name（op_queue_names=None），则走 original mode，但支持：
    # - 跳过 done 队列里已完成的 pmid（若 store 提供 contains 查询）
    # - 每个 pmid 完成后 mark done，支持断点续跑
    use_queue_mode = (op_queue_names is not None) and (done_queue_name is not None)
    if use_queue_mode:
        if not op_queue_names or not isinstance(op_queue_names, list):
            raise ValueError("queue mode requires op_queue_names: list[str]")
        if not done_queue_name or not isinstance(done_queue_name, str):
            raise ValueError("queue mode requires done_queue_name: str")

        # queue mode 的退出条件集合：
        # - pmidlist=None => 自动使用 abs 表中的所有 pmid
        #   （用于实现“下游等待上游逐步产出 op 队列，直到全量完成”的流式联动）
        if pmidlist is None:
            target_pmids = [int(p) for p in store.get_pmids()]
        else:
            target_pmids = [int(p) for p in pmidlist]

        if limit is not None:
            target_pmids = target_pmids[:limit]
        pmidset = set(target_pmids)

        total = len(target_pmids)
        already_done = store.queue_done_count_in(done_queue_name, pmidset)
        print(f"Queue mode: target={total}, already_done={already_done}, op_queues={op_queue_names}")

        results = {}
        postfix = {}
        global_stats = {}

        # ⭐ tqdm 在多线程环境必须加锁
        pbar_lock = threading.Lock()
        pbar = tqdm(
            total=total,
            initial=already_done,
            desc="Processing pmids (queue)",
            dynamic_ncols=True,
        )

        done_count = already_done

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            inflight: dict[concurrent.futures.Future, int] = {}

            def _submit(pmid: int):
                fut = executor.submit(_run_pipeline, pmid)
                inflight[fut] = pmid

            while True:
                # fill free slots
                while len(inflight) < workers and done_count < total:
                    # claim one task (prevents double-use until finished)
                    # 这里把“上游 done 队列”当作“下游 op 队列”，不再要求同步写 queue_items
                    pmid = store.queue_claim_done_intersection(op_queue_names, done_queue_name)
                    if pmid is None:
                        break

                    # claim到不在pmidlist的：释放 inflight，并放回各op队列末尾
                    if pmid not in pmidset:
                        # op 来源是上游 queue_done（非 queue_items），无需/不应 requeue
                        store.queue_inflight_remove(done_queue_name, pmid)
                        continue

                    # 注意：不能在任务未结束时写 done
                    # 这里仅 submit；任务结束后（无论成功失败、包括重试结束）再 mark done
                    _submit(pmid)

                # exit
                if done_count >= total and not inflight:
                    break

                # collect finished jobs (update postfix / results)
                if inflight:
                    done_futs, _ = concurrent.futures.wait(
                        inflight,
                        timeout=0.2,
                        return_when=concurrent.futures.FIRST_COMPLETED,
                    )
                    for future in done_futs:
                        pmid = inflight.pop(future)

                        try:
                            result, info_list = future.result()
                        except Exception as e:
                            result, info_list = None, [{"type": "error", "msg": str(e)}]

                        # 任务真正结束后：写 done（不管成功失败）
                        store.queue_mark_done(done_queue_name, pmid)

                        done_count += 1
                        with pbar_lock:
                            pbar.update(1)

                        results[pmid] = result

                        for info in info_list:
                            prefix = info.get("__prefix", "")
                            if info["type"] == "status":
                                status_name = info.get("name", "status")
                                postfix[f"{prefix}{status_name}"] = info.get("description", "")

                            elif info["type"] == "metric":
                                name = info.get("name", "default")
                                key = f"{prefix}{name}"

                                if key not in global_stats:
                                    global_stats[key] = {"correct": 0, "total": 0}

                                c = info.get("correct", 0)
                                t = info.get("total", 0)
                                global_stats[key]["correct"] += c
                                global_stats[key]["total"] += t

                                g_c = global_stats[key]["correct"]
                                g_t = global_stats[key]["total"]
                                g_acc = g_c / g_t if g_t else 0
                                postfix[f"{key}_acc"] = round(g_acc, 3)

                            elif info["type"] == "error":
                                postfix[f"{prefix}error"] = info.get("msg")

                        with pbar_lock:
                            pbar.set_postfix(postfix)
                else:
                    # 队列空但还没done完：等待后继续
                    time.sleep(queue_sleep)

        pbar.close()
        return results

    # ---------------------------
    # original mode (static pmid list from DB)
    # + optional done queue (op_queue_names=None, done_queue_name!=None)
    # ---------------------------
    db_pmids = store.get_pmids()
    if pmidlist is not None:
        pmidset = {int(p) for p in pmidlist}
        db_pmids = [p for p in db_pmids if p in pmidset]

    if limit is not None:
        db_pmids = db_pmids[:limit]

    # done-only 模式：尽量跳过已经 done 的 pmid（如果 PMIDStore 提供 contains API）
    already_done = 0
    db_pmids_todo = db_pmids
    if done_queue_name:
        done_contains = None
        for _cand in ("queue_done_contains", "queue_done_has", "queue_contains_done"):
            if hasattr(store, _cand):
                done_contains = getattr(store, _cand)
                break

        if callable(done_contains):
            db_pmids_todo = [p for p in db_pmids if not done_contains(done_queue_name, p)]
            already_done = len(db_pmids) - len(db_pmids_todo)
            print(f"Done-queue mode: total={len(db_pmids)}, already_done={already_done}, todo={len(db_pmids_todo)}")
        else:
            # 没有 contains 方法：不做跳过（但仍会在完成后 mark done）
            print(f"Done-queue mode: cannot find done-contains method on PMIDStore; will not skip existing done")

    print(f"Total pmidss detected: {len(db_pmids)}")

    results = {}
    postfix = {}
    global_stats = {}

    # ⭐ tqdm 在多线程环境必须加锁
    pbar_lock = threading.Lock()

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_run_pipeline, pmid): pmid for pmid in db_pmids_todo}

        pbar = tqdm(
            total=len(db_pmids),
            initial=already_done if already_done else 0,
            desc="Processing pmids",
            dynamic_ncols=True,
        )

        for future in concurrent.futures.as_completed(futures):
            pmid = futures[future]

            try:
                result, info_list = future.result()
            except Exception as e:
                result, info_list = None, [{"type": "error", "msg": str(e)}]

            # 每个任务结束后：mark done（不论成功失败），用于断点续跑
            if done_queue_name:
                try:
                    store.queue_mark_done(done_queue_name, pmid)
                except Exception:
                    pass

            results[pmid] = result

            # ---------------------------
            #  Collect postfix info
            # ---------------------------
            for info in info_list:
                prefix = info.get("__prefix", "")
                if info["type"] == "status":
                    status_name = info.get("name", "status")
                    postfix[f"{prefix}{status_name}"] = info.get("description", "")

                elif info["type"] == "metric":
                    name = info.get("name", "default")
                    key = f"{prefix}{name}"

                    if key not in global_stats:
                        global_stats[key] = {"correct": 0, "total": 0}

                    c = info.get("correct", 0)
                    t = info.get("total", 0)
                    global_stats[key]["correct"] += c
                    global_stats[key]["total"] += t

                    g_c = global_stats[key]["correct"]
                    g_t = global_stats[key]["total"]
                    g_acc = g_c / g_t if g_t else 0
                    postfix[f"{key}_acc"] = round(g_acc, 3)

                elif info["type"] == "error":
                    postfix[f"{prefix}error"] = info.get("msg")

            # ---------------------------
            #  ⭐ update tqdm must be locked
            # ---------------------------
            with pbar_lock:
                pbar.set_postfix(postfix)
                pbar.update(1)

        pbar.close()

    # print("All PMIDs processed.")
    return results


def process_one_folder_count_file(folder: str, filename: str, **kwargs):
    """
    统计 folder 下 filename 是否存在。
    返回格式兼容 parallel_process：
        result = True/False
        info_list = 统计 + 显示信息
    """
    pmid = os.path.basename(folder)
    target_path = os.path.join(folder, filename)

    if os.path.exists(target_path):
        # 文件存在
        return True, [
            {"type": "status", "description": f"exists pmid:{pmid}"},
            {"type": "metric", "correct": 1, "total": 1},  # 也可以都记为1
        ]
    else:
        # 文件不存在
        return False, [
            {"type": "error", "msg": f"missing pmid:{pmid}"},
            {"type": "metric", "correct": 0, "total": 1},
        ]


def process_one_folder_read_json(folder: str, filename: str, **kwargs):
    """
    在 folder 下读取 JSON 文件 filename。
    返回：
        result = 解析后的 JSON dict，或 None
        info_list = 状态与 metric
    兼容 parallel_process。
    """
    pmid = os.path.basename(folder)
    path = os.path.join(folder, filename)

    if not os.path.exists(path):
        return None, [
            {"type": "error", "msg": f"missing {filename} pmid:{pmid}"},
            {"type": "metric", "correct": 0, "total": 1},
        ]

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        return data, [
            {"type": "error", "msg": f"read {filename} pmid:{pmid}"},
            {"type": "metric", "correct": 1, "total": 1},
        ]

    except Exception as e:
        return None, [
            {"type": "error", "msg": f"read_fail {filename} pmid:{pmid}"},
            {"type": "metric", "correct": 0, "total": 1},
        ]


def process_one_folder_read_many(folder: str, filenames: list[str], **kwargs):
    """
    在一个 PMID folder 下读取多个 ds_*.json
    返回 dict: { filename: json_data }
    """
    pmid = os.path.basename(folder)
    out = {}
    info = []

    for fn in filenames:
        path = os.path.join(folder, fn)
        if not os.path.exists(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                out[fn] = json.load(f)
        except Exception as e:
            info.append({"type": "error", "msg": f"{pmid}:{fn} {e}"})

    return out, [
        {"type": "status", "description": f"read {len(out)} files pmid:{pmid}"},
        {"type": "metric", "correct": 1, "total": 1},
    ]
