# test/test_queue.py
import os
import time
import multiprocessing as mp
import threading
from pathlib import Path
import contextlib
import sys

from src.pmcad.pmidstore import PMIDStore
from src.pmcad.parallel_process import process_folder_parallel


DB_PATH = Path("test") / "queue_test.db"
LOG_DIR = Path("test") / "queue_logs"


def _bulk_seed_queue(db_path: str, queue_name: str, pmids: list[int]):
    """
    更快地一次性初始化队列（避免逐条 queue_append 的事务开销）。
    """
    with PMIDStore(db_path, readonly=False) as store:
        now = time.time()
        store.conn.execute("BEGIN;")
        try:
            store.conn.executemany(
                "INSERT OR IGNORE INTO queue_items(queue_name, pmid, created_at) VALUES (?, ?, ?)",
                [(queue_name, int(p), now) for p in pmids],
            )
            store.conn.execute("COMMIT;")
        except Exception:
            store.conn.execute("ROLLBACK;")
            raise


def _redirect_all_output(log_path: Path):
    """
    将该进程的 stdout/stderr 全部写入文件（包含 tqdm 输出）。
    注意：多进程写同一个文件可能互相穿插；因此每个进程单独一个 log 更清晰。
    """
    log_path.parent.mkdir(parents=True, exist_ok=True)
    f = open(log_path, "a", encoding="utf-8", buffering=1)  # line-buffered
    return f, contextlib.redirect_stdout(f), contextlib.redirect_stderr(f)


def run_stage_A(db_path: str, pmidlist: list[int]):
    """
    A: 从 qA 取任务，分配时写 doneA（由 process_folder_parallel 完成），执行后 push 到 qC_fromA
    """
    log_f, r_out, r_err = _redirect_all_output(LOG_DIR / "A.log")
    with log_f, r_out, r_err:
        print(f"[A] started, db={db_path}, n={len(pmidlist)}", flush=True)

        db_lock = threading.Lock()

        def fn_A(*, pmid: int, store: PMIDStore, llm=None, **kwargs):
            time.sleep(1.0)
            # 任务执行结束后，把 pmid 放入 C 的 A-就绪队列
            with db_lock:
                store.queue_append("qC_fromA", pmid)
            return None, [{"type": "status", "description": f"A done pmid={pmid}"}]

        with PMIDStore(db_path, readonly=False) as store:
            process_folder_parallel(
                store=store,
                llm_list=[None],
                process_one_folder=fn_A,
                workers=2,
                pmidlist=pmidlist,
                op_queue_names=["qA"],
                done_queue_name="doneA",
                queue_sleep=5.0,
            )

        print("[A] finished", flush=True)


def run_stage_B(db_path: str, pmidlist: list[int]):
    """
    B: 从 qB 取任务，分配时写 doneB，执行后 push 到 qC_fromB
    """
    log_f, r_out, r_err = _redirect_all_output(LOG_DIR / "B.log")
    with log_f, r_out, r_err:
        print(f"[B] started, db={db_path}, n={len(pmidlist)}", flush=True)

        db_lock = threading.Lock()

        def fn_B(*, pmid: int, store: PMIDStore, llm=None, **kwargs):
            time.sleep(2.0)
            with db_lock:
                store.queue_append("qC_fromB", pmid)
            return None, [{"type": "status", "description": f"B done pmid={pmid}"}]

        with PMIDStore(db_path, readonly=False) as store:
            process_folder_parallel(
                store=store,
                llm_list=[None],
                process_one_folder=fn_B,
                workers=2,
                pmidlist=pmidlist,
                op_queue_names=["qB"],
                done_queue_name="doneB",
                queue_sleep=5.0,
            )

        print("[B] finished", flush=True)


def run_stage_C(db_path: str, pmidlist: list[int]):
    """
    C: 与 A/B 同时启动。
       只有当 qC_fromA 和 qC_fromB 都有该 pmid（交集），且 doneC 没有，才会被分配执行。
    """
    log_f, r_out, r_err = _redirect_all_output(LOG_DIR / "C.log")
    with log_f, r_out, r_err:
        print(f"[C] started, db={db_path}, n={len(pmidlist)}", flush=True)

        def fn_C(*, pmid: int, store: PMIDStore, llm=None, **kwargs):
            time.sleep(0.2)
            return None, [{"type": "status", "description": f"C done pmid={pmid}"}]

        with PMIDStore(db_path, readonly=False) as store:
            process_folder_parallel(
                store=store,
                llm_list=[None],
                process_one_folder=fn_C,
                workers=2,
                pmidlist=pmidlist,
                op_queue_names=["qC_fromA", "qC_fromB"],
                done_queue_name="doneC",
                queue_sleep=5.0,
            )

        print("[C] finished", flush=True)


def main():
    # 可用环境变量快速缩小规模：
    # N_PMIDS=50 python -m test.test_queue
    n = int(os.getenv("N_PMIDS", "1000"))
    pmids = list(range(1, n + 1))

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    if DB_PATH.exists():
        DB_PATH.unlink()

    # 初始化队列：A/B 的输入队列
    _bulk_seed_queue(str(DB_PATH), "qA", pmids)
    _bulk_seed_queue(str(DB_PATH), "qB", pmids)

    main_log = LOG_DIR / "main.log"
    with open(main_log, "a", encoding="utf-8", buffering=1) as f:
        print(f"[main] DB: {DB_PATH.resolve()}", file=f, flush=True)
        print(f"[main] logs: {LOG_DIR.resolve()}", file=f, flush=True)
        print(f"[main] Seeded qA/qB with {n} pmids", file=f, flush=True)

    # A, B, C 同时运行；C 会自然等待 op_queue_names 的交集出现
    pA = mp.Process(target=run_stage_A, args=(str(DB_PATH), pmids), name="Process-A")
    pB = mp.Process(target=run_stage_B, args=(str(DB_PATH), pmids), name="Process-B")
    pC = mp.Process(target=run_stage_C, args=(str(DB_PATH), pmids), name="Process-C")

    t0 = time.time()
    pA.start()
    pB.start()
    pC.start()

    pA.join()
    pB.join()
    pC.join()

    t1 = time.time()

    # 简单验证 done 数（写入 main.log）
    with PMIDStore(str(DB_PATH), readonly=False) as store:
        doneA = store.queue_done_count_in("doneA", set(pmids))
        doneB = store.queue_done_count_in("doneB", set(pmids))
        doneC = store.queue_done_count_in("doneC", set(pmids))

    with open(main_log, "a", encoding="utf-8", buffering=1) as f:
        print(f"[main] total_time={t1 - t0:.1f}s", file=f, flush=True)
        print(f"[main] doneA={doneA}/{n}, doneB={doneB}/{n}, doneC={doneC}/{n}", file=f, flush=True)


if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    main()