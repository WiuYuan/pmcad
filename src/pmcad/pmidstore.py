# src/pmcad/pmidstore.py
import sqlite3
import json
import time
from typing import Optional, Any, Union


class PMIDStore:
    """
    SQLite-backed PMID store.

    Schema (created automatically):

      abs(
        pmid INTEGER PRIMARY KEY,
        abstract TEXT
      )

      files(
        pmid INTEGER,
        name TEXT,
        content TEXT,
        PRIMARY KEY (pmid, name)
      )

    Design principles:
    - filename (name) is DATA, not schema
    - you can add unlimited new "files" per pmid
    - safe for Lustre / HPC
    """

    def __init__(
        self,
        db_path: str,
        *,
        readonly: bool = False,
        timeout: float = 60.0,
    ):
        self.db_path = db_path
        self.readonly = readonly

        uri = f"file:{db_path}?mode={'ro' if readonly else 'rwc'}"
        self.conn = sqlite3.connect(
            uri,
            uri=True,
            timeout=timeout,
            isolation_level=None,
            check_same_thread=False,
        )

        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")

        if not readonly:
            self._init_schema()

    # --------------------------
    # schema
    # --------------------------
    def _init_schema(self):
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS abs(
              pmid INTEGER PRIMARY KEY,
              abstract TEXT
            );

            CREATE TABLE IF NOT EXISTS files(
              pmid INTEGER,
              name TEXT,
              content TEXT,
              PRIMARY KEY (pmid, name)
            );

            CREATE INDEX IF NOT EXISTS idx_files_name ON files(name);

            -- --------------------------
            -- queue system
            -- --------------------------
            -- operation queues (can have multiple queue_name)
            CREATE TABLE IF NOT EXISTS queue_items(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              queue_name TEXT NOT NULL,
              pmid INTEGER NOT NULL,
              created_at REAL NOT NULL,
              UNIQUE(queue_name, pmid)
            );

            CREATE INDEX IF NOT EXISTS idx_queue_items_qname_id ON queue_items(queue_name, id);
            CREATE INDEX IF NOT EXISTS idx_queue_items_qname_pmid ON queue_items(queue_name, pmid);

            -- done queue (usually one queue_name per pipeline stage)
            CREATE TABLE IF NOT EXISTS queue_done(
              queue_name TEXT NOT NULL,
              pmid INTEGER NOT NULL,
              created_at REAL NOT NULL,
              PRIMARY KEY(queue_name, pmid)
            );

            CREATE INDEX IF NOT EXISTS idx_queue_done_qname ON queue_done(queue_name);

            -- inflight queue: tasks claimed but not finished yet (prevents double-use)
            CREATE TABLE IF NOT EXISTS queue_inflight(
              stage_name TEXT NOT NULL,
              pmid INTEGER NOT NULL,
              started_at REAL NOT NULL,
              PRIMARY KEY(stage_name, pmid)
            );

            CREATE INDEX IF NOT EXISTS idx_queue_inflight_stage ON queue_inflight(stage_name);
            """
        )

    # --------------------------
    # lifecycle
    # --------------------------
    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    # --------------------------
    # abstract
    # --------------------------
    def get_abstract(self, pmid: Union[int, str]) -> Optional[str]:
        pmid = int(pmid)
        row = self.conn.execute(
            "SELECT abstract FROM abs WHERE pmid=?",
            (pmid,),
        ).fetchone()
        return row[0] if row else None

    def put_abstract(self, pmid: Union[int, str], text: str):
        if self.readonly:
            raise RuntimeError("PMIDStore opened readonly")

        pmid = int(pmid)
        self.conn.execute("BEGIN;")
        try:
            self.conn.execute(
                "INSERT OR REPLACE INTO abs(pmid, abstract) VALUES (?, ?)",
                (pmid, text),
            )
            self.conn.execute("COMMIT;")
        except Exception:
            self.conn.execute("ROLLBACK;")
            raise

    # --------------------------
    # generic file (json / text)
    # --------------------------
    def get(self, pmid: Union[int, str], name: str) -> Optional[Any]:
        """
        Get content under (pmid, name).
        If content is valid JSON, return decoded object.
        Otherwise return raw string.
        """
        pmid = int(pmid)
        row = self.conn.execute(
            "SELECT content FROM files WHERE pmid=? AND name=?",
            (pmid, name),
        ).fetchone()

        if not row:
            return None

        content = row[0]
        try:
            return json.loads(content)
        except Exception:
            return content

    def put(
        self,
        pmid: Union[int, str],
        name: str,
        value: Union[str, dict, list],
    ):
        if self.readonly:
            raise RuntimeError("PMIDStore opened readonly")

        pmid = int(pmid)

        if isinstance(value, (dict, list)):
            content = json.dumps(value, ensure_ascii=False)
        else:
            content = str(value)

        self.conn.execute("BEGIN;")
        try:
            self.conn.execute(
                "INSERT OR REPLACE INTO files(pmid, name, content) VALUES (?, ?, ?)",
                (pmid, name, content),
            )
            self.conn.execute("COMMIT;")
        except Exception:
            self.conn.execute("ROLLBACK;")
            raise

    # --------------------------
    # helpers
    # --------------------------
    def has(self, pmid: Union[int, str], name: str) -> bool:
        pmid = int(pmid)
        row = self.conn.execute(
            "SELECT 1 FROM files WHERE pmid=? AND name=? LIMIT 1",
            (pmid, name),
        ).fetchone()
        return row is not None

    def list_files(self, pmid: Union[int, str]):
        pmid = int(pmid)
        rows = self.conn.execute(
            "SELECT name FROM files WHERE pmid=? ORDER BY name",
            (pmid,),
        ).fetchall()
        return [r[0] for r in rows]

    def count_files(self, name: Optional[str] = None) -> int:
        if name is None:
            row = self.conn.execute(
                "SELECT COUNT(*) FROM files"
            ).fetchone()
        else:
            row = self.conn.execute(
                "SELECT COUNT(*) FROM files WHERE name=?",
                (name,),
            ).fetchone()
        return row[0]
    
    def get_pmids(self):
        return [row[0] for row in self.conn.execute("SELECT pmid FROM abs")]

    # --------------------------
    # queue helpers
    # --------------------------
    @staticmethod
    def _chunked(seq, chunk_size: int):
        for i in range(0, len(seq), chunk_size):
            yield seq[i : i + chunk_size]

    def queue_append(self, queue_name: str, pmid: Union[int, str]):
        """
        Append pmid to tail of queue_name (dedup by UNIQUE(queue_name, pmid)).

        NOTE: single INSERT is atomic in SQLite; avoid explicit BEGIN to prevent
        "cannot start a transaction within a transaction" when callers nest logic.
        """
        if self.readonly:
            raise RuntimeError("PMIDStore opened readonly")

        pmid = int(pmid)
        now = time.time()
        self.conn.execute(
            "INSERT OR IGNORE INTO queue_items(queue_name, pmid, created_at) VALUES (?, ?, ?)",
            (queue_name, pmid, now),
        )

    def queue_requeue_many(self, queue_names: list[str], pmid: Union[int, str]):
        """
        Move pmid to the tail for each queue in queue_names:
        implemented as DELETE + INSERT (so id increases => tail).
        """
        if self.readonly:
            raise RuntimeError("PMIDStore opened readonly")

        pmid = int(pmid)
        now = time.time()
        self.conn.execute("BEGIN;")
        try:
            for qn in queue_names:
                self.conn.execute(
                    "DELETE FROM queue_items WHERE queue_name=? AND pmid=?",
                    (qn, pmid),
                )
                self.conn.execute(
                    "INSERT OR IGNORE INTO queue_items(queue_name, pmid, created_at) VALUES (?, ?, ?)",
                    (qn, pmid, now),
                )
            self.conn.execute("COMMIT;")
        except Exception:
            self.conn.execute("ROLLBACK;")
            raise

    def queue_done_has(self, done_queue_name: str, pmid: Union[int, str]) -> bool:
        pmid = int(pmid)
        row = self.conn.execute(
            "SELECT 1 FROM queue_done WHERE queue_name=? AND pmid=? LIMIT 1",
            (done_queue_name, pmid),
        ).fetchone()
        return row is not None

    def queue_done_add(self, done_queue_name: str, pmid: Union[int, str]):
        """
        Mark pmid as done (idempotent).

        NOTE: single INSERT is atomic in SQLite; avoid explicit BEGIN to prevent
        nested transaction errors.
        """
        if self.readonly:
            raise RuntimeError("PMIDStore opened readonly")

        pmid = int(pmid)
        now = time.time()
        self.conn.execute(
            "INSERT OR IGNORE INTO queue_done(queue_name, pmid, created_at) VALUES (?, ?, ?)",
            (done_queue_name, pmid, now),
        )

    def queue_done_clear(self, done_queue_name: str) -> int:
        """
        Clear ALL done items for this done_queue_name.
        Returns number of rows deleted.

        NOTE:
        - 当前设计中 done 与 op 逻辑分离；本函数仅清理 queue_done。
        """
        if self.readonly:
            raise RuntimeError("PMIDStore opened readonly")

        self.conn.execute("BEGIN;")
        try:
            cur = self.conn.execute(
                "DELETE FROM queue_done WHERE queue_name=?",
                (done_queue_name,),
            )
            self.conn.execute("COMMIT;")
            return int(cur.rowcount or 0)
        except Exception:
            self.conn.execute("ROLLBACK;")
            raise

    def queue_done_list(self, done_queue_name: str) -> list[int]:
        """
        列出某个 done_queue_name 下所有已完成的 pmid（按 created_at 升序）。
        用于把“上游 stage 的 done”当作“下游 stage 的 pmidlist / op 输入集合”。
        """
        rows = self.conn.execute(
            "SELECT pmid FROM queue_done WHERE queue_name=? ORDER BY created_at ASC",
            (done_queue_name,),
        ).fetchall()
        return [int(r[0]) for r in rows]

    def queue_seed_from_done(self, done_queue_name: str, op_queue_name: str | None = None) -> int:
        """
        将 done_queue_name 中的所有 pmid 复制/灌入到 queue_items(op_queue_name) 里，
        作为下游 stage 的 op_queue 输入。

        返回：本次实际插入 queue_items 的行数（INSERT OR IGNORE 后的 rowcount）。
        """
        if self.readonly:
            raise RuntimeError("PMIDStore opened readonly")

        if op_queue_name is None:
            op_queue_name = done_queue_name

        now = time.time()
        self.conn.execute("BEGIN IMMEDIATE;")
        try:
            cur = self.conn.execute(
                """
                INSERT OR IGNORE INTO queue_items(queue_name, pmid, created_at)
                SELECT ?, qd.pmid, ?
                FROM queue_done qd
                WHERE qd.queue_name=?
                """,
                (op_queue_name, now, done_queue_name),
            )
            self.conn.execute("COMMIT;")
            return int(cur.rowcount or 0)
        except Exception:
            self.conn.execute("ROLLBACK;")
            raise

    def queue_inflight_remove(self, stage_name: str, pmid: Union[int, str]):
        """
        Remove pmid from inflight (idempotent).
        """
        if self.readonly:
            raise RuntimeError("PMIDStore opened readonly")
        pmid = int(pmid)
        self.conn.execute(
            "DELETE FROM queue_inflight WHERE stage_name=? AND pmid=?",
            (stage_name, pmid),
        )

    def queue_inflight_clear(self, stage_name: str) -> int:
        """
        Clear ALL inflight items for this stage_name.
        Returns number of rows deleted.
        """
        if self.readonly:
            raise RuntimeError("PMIDStore opened readonly")

        cur = self.conn.execute(
            "DELETE FROM queue_inflight WHERE stage_name=?",
            (stage_name,),
        )
        return int(cur.rowcount or 0)

    def queue_mark_done(self, done_queue_name: str, pmid: Union[int, str]):
        """
        Finish a claimed task:
          - remove from inflight
          - add to done (even if task failed; caller decides policy)

        Atomic across both operations.

        NOTE:
        - 当前流水线做法是：下游直接从上游的 queue_done 中 claim；
          本函数仅负责写 queue_done + 清理 inflight。
        """
        if self.readonly:
            raise RuntimeError("PMIDStore opened readonly")

        pmid = int(pmid)
        now = time.time()
        self.conn.execute("BEGIN IMMEDIATE;")
        try:
            self.conn.execute(
                "DELETE FROM queue_inflight WHERE stage_name=? AND pmid=?",
                (done_queue_name, pmid),
            )
            self.conn.execute(
                "INSERT OR IGNORE INTO queue_done(queue_name, pmid, created_at) VALUES (?, ?, ?)",
                (done_queue_name, pmid, now),
            )
            self.conn.execute("COMMIT;")
        except Exception:
            self.conn.execute("ROLLBACK;")
            raise

    def queue_claim_intersection(self, op_queue_names: list[str], stage_name: str) -> Optional[int]:
        """
        Atomically claim ONE pmid that:
          - exists in ALL op_queue_names (from queue_items)
          - NOT in done(stage_name)
          - NOT in inflight(stage_name)

        Claim is recorded into queue_inflight(stage_name, pmid).
        (Unlike pop, this does not delete from op queues; done/inflight gates selection.)
        """
        if self.readonly:
            raise RuntimeError("PMIDStore opened readonly")
        if not op_queue_names:
            raise ValueError("op_queue_names cannot be empty")

        op_queue_names = list(op_queue_names)
        n = len(op_queue_names)
        q_placeholders = ",".join(["?"] * n)

        sql_pick = f"""
        SELECT qi.pmid
        FROM queue_items qi
        LEFT JOIN queue_done qd
          ON qd.queue_name=? AND qd.pmid=qi.pmid
        LEFT JOIN queue_inflight qf
          ON qf.stage_name=? AND qf.pmid=qi.pmid
        WHERE qi.queue_name IN ({q_placeholders})
          AND qd.pmid IS NULL
          AND qf.pmid IS NULL
        GROUP BY qi.pmid
        HAVING COUNT(DISTINCT qi.queue_name)=?
        ORDER BY MAX(qi.id) ASC
        LIMIT 1
        """

        self.conn.execute("BEGIN IMMEDIATE;")
        try:
            row = self.conn.execute(
                sql_pick,
                (stage_name, stage_name, *op_queue_names, n),
            ).fetchone()
            if not row:
                self.conn.execute("COMMIT;")
                return None

            pmid = int(row[0])
            now = time.time()

            self.conn.execute(
                "INSERT OR IGNORE INTO queue_inflight(stage_name, pmid, started_at) VALUES (?, ?, ?)",
                (stage_name, pmid, now),
            )
            self.conn.execute("COMMIT;")
            return pmid
        except Exception:
            self.conn.execute("ROLLBACK;")
            raise

    def queue_claim_done_intersection(self, op_done_queue_names: list[str], stage_name: str) -> Optional[int]:
        """
        Atomically claim ONE pmid that:
          - exists in ALL op_done_queue_names (from queue_done; i.e. treat upstream done as downstream op)
          - NOT in done(stage_name)
          - NOT in inflight(stage_name)

        用于“不要区分 done 序列和 op 序列”的流水：下游直接从上游 queue_done 里 claim。
        """
        if self.readonly:
            raise RuntimeError("PMIDStore opened readonly")
        if not op_done_queue_names:
            raise ValueError("op_done_queue_names cannot be empty")

        op_done_queue_names = list(op_done_queue_names)
        n = len(op_done_queue_names)
        q_placeholders = ",".join(["?"] * n)

        sql_pick = f"""
        SELECT qd_src.pmid
        FROM queue_done qd_src
        LEFT JOIN queue_done qd_stage
          ON qd_stage.queue_name=? AND qd_stage.pmid=qd_src.pmid
        LEFT JOIN queue_inflight qf
          ON qf.stage_name=? AND qf.pmid=qd_src.pmid
        WHERE qd_src.queue_name IN ({q_placeholders})
          AND qd_stage.pmid IS NULL
          AND qf.pmid IS NULL
        GROUP BY qd_src.pmid
        HAVING COUNT(DISTINCT qd_src.queue_name)=?
        ORDER BY MAX(qd_src.created_at) ASC
        LIMIT 1
        """

        self.conn.execute("BEGIN IMMEDIATE;")
        try:
            row = self.conn.execute(
                sql_pick,
                (stage_name, stage_name, *op_done_queue_names, n),
            ).fetchone()
            if not row:
                self.conn.execute("COMMIT;")
                return None

            pmid = int(row[0])
            now = time.time()

            self.conn.execute(
                "INSERT OR IGNORE INTO queue_inflight(stage_name, pmid, started_at) VALUES (?, ?, ?)",
                (stage_name, pmid, now),
            )
            self.conn.execute("COMMIT;")
            return pmid
        except Exception:
            self.conn.execute("ROLLBACK;")
            raise

    def queue_done_count_in(self, done_queue_name: str, pmidset: set[int]) -> int:
        """
        Count how many pmids in pmidset are already in done queue.
        (Chunked to avoid SQLite variable limits.)
        """
        pmids = list(pmidset)
        if not pmids:
            return 0

        total = 0
        for chunk in self._chunked(pmids, 900):
            placeholders = ",".join(["?"] * len(chunk))
            row = self.conn.execute(
                f"SELECT COUNT(*) FROM queue_done WHERE queue_name=? AND pmid IN ({placeholders})",
                (done_queue_name, *chunk),
            ).fetchone()
            total += int(row[0])
        return total

    def queue_pop_intersection(self, op_queue_names: list[str], done_queue_name: str) -> Optional[int]:
        """
        Atomically pick ONE pmid that:
          - exists in ALL op_queue_names
          - NOT in done_queue_name
        Then remove it from ALL op queues (so it won't be picked again).
        Return pmid or None if no candidate.

        Ordering: choose smallest MAX(id) among the op queues (approx FIFO).
        """
        if self.readonly:
            raise RuntimeError("PMIDStore opened readonly")

        if not op_queue_names:
            raise ValueError("op_queue_names cannot be empty")

        op_queue_names = list(op_queue_names)
        n = len(op_queue_names)
        q_placeholders = ",".join(["?"] * n)

        sql_pick = f"""
        SELECT qi.pmid
        FROM queue_items qi
        LEFT JOIN queue_done qd
          ON qd.queue_name=? AND qd.pmid=qi.pmid
        WHERE qi.queue_name IN ({q_placeholders})
          AND qd.pmid IS NULL
        GROUP BY qi.pmid
        HAVING COUNT(DISTINCT qi.queue_name)=?
        ORDER BY MAX(qi.id) ASC
        LIMIT 1
        """

        sql_del = f"""
        DELETE FROM queue_items
        WHERE pmid=?
          AND queue_name IN ({q_placeholders})
        """

        self.conn.execute("BEGIN IMMEDIATE;")
        try:
            row = self.conn.execute(
                sql_pick,
                (done_queue_name, *op_queue_names, n),
            ).fetchone()
            if not row:
                self.conn.execute("COMMIT;")
                return None

            pmid = int(row[0])

            self.conn.execute(
                sql_del,
                (pmid, *op_queue_names),
            )
            self.conn.execute("COMMIT;")
            return pmid
        except Exception:
            self.conn.execute("ROLLBACK;")
            raise


# --------------------------
# example usage
# --------------------------
if __name__ == "__main__":
    db = "pmid.db"

    with PMIDStore(db) as store:
        pmid = 163688

        print("abstract:", store.get_abstract(pmid)[:120])

        store.put(pmid, "ner", {"entities": [{"text": "TP53", "type": "gene"}]})
        store.put(pmid, "summary", "This paper studies ...")

        print("ner:", store.get(pmid, "ner"))
        print("files:", store.list_files(pmid))