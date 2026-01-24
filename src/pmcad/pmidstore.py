import sqlite3
import json
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
