import os
import json
from tqdm import tqdm
import concurrent.futures
import threading
from typing import Union
from pathlib import Path


def process_one_folder_delete_file(folder: str, filename: str):
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
    folder: str, file1: str, file2: str, output_name: str = "merged.json"
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
    folder: str,
    process_one_folder: Union[list[callable], callable],
    workers: int = 16,
    pmidlist: list = None,
    limit: int | None = None,
    max_worker_list: list[int] | None = None,
    **kwargs,
):
    if callable(process_one_folder):
        process_fns = [process_one_folder]
    else:
        process_fns = list(process_one_folder)
        
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
        
    def _run_pipeline(path):
        final_result = None
        all_info_list = []

        for idx, fn in enumerate(process_fns, start=1):
            sem = step_sems[idx - 1]
            # try:
            #     print(f"\n[pmid={os.path.basename(path)} step={idx}] before acquire: sem={getattr(sem, '_value', 'NA')}/{max_worker_list[idx-1]}\n")
            # except Exception:
            #     pass
            sem.acquire()
            prefix = "" if idx == 1 else f"{idx}_"

            try:
                result, info_list = fn(path, **kwargs)
            except Exception as e:
                result, info_list = None, [{"type": "error", "msg": f"Extra: {Path(path).name} {str(e)}"}]
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
    leaf_folders = []
    for root, dirs, files in os.walk(folder):
        # 如果当前目录没有子文件夹，则它是一个叶子文件夹
        if not dirs and root != folder:  # 排除根文件夹本身
            leaf_folders.append(root)

    print(f"Total leaf folders detected: {len(leaf_folders)}")

    pmid_paths = {}
    for path in leaf_folders:
        pmid = os.path.basename(path)  # 获取文件夹名称作为PMID
        if pmidlist is not None and pmid not in pmidlist:
            continue
        pmid_paths[pmid] = path
    if limit is not None:
        pmid_paths = dict(list(pmid_paths.items())[:limit])

    results = {}
    postfix = {}
    global_stats = {}

    # ⭐ tqdm 在多线程环境必须加锁
    pbar_lock = threading.Lock()

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:

        futures = {
            executor.submit(_run_pipeline, path): pmid
            for pmid, path in pmid_paths.items()
        }

        pbar = tqdm(total=len(futures), desc="Processing folders", dynamic_ncols=True)

        for future in concurrent.futures.as_completed(futures):

            pmid = futures[future]

            try:
                result, info_list = future.result()
            except Exception as e:
                result, info_list = None, [{"type": "error", "msg": str(e)}]

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


def process_one_folder_count_file(folder: str, filename: str):
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


def process_one_folder_read_json(folder: str, filename: str):
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


def process_one_folder_read_many(folder: str, filenames: list[str]):
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
