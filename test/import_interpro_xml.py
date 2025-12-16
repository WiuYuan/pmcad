import os
import sys
import gzip
import ctypes
from lxml import etree
from tqdm import tqdm
import tempfile

# =============== 初始化路径与依赖 ===============
sys.path.append("/data/wyuan/workspace/pmcdata_pro/pmcad")

os.environ["LD_LIBRARY_PATH"] = "/data/wyuan/pgsql/lib:" + os.environ.get(
    "LD_LIBRARY_PATH", ""
)
ctypes.CDLL("/data/wyuan/pgsql/lib/libpq.so", mode=ctypes.RTLD_GLOBAL)
ctypes.CDLL("/data/wyuan/pgsql/lib/libpqxx.so", mode=ctypes.RTLD_GLOBAL)

from src.pmcad.core import pg_exec

dbpath = "/data/wyuan/workspace/pmcdata_pro/database/protease_pgdb"
xml_path = "/data/wyuan/workspace/pmcdata_pro/data/interpro/interpro.xml.gz"

entry_table = "interpro_entry"
member_table = "interpro_member"
relation_table = "interpro_relation"
class_table = "interpro_classification"

TMP_DIR = None  # 可改为 /data/wyuan/tmp

# =============== 工具函数 ===============
def sql_escape(val):
    if val is None:
        return ""
    s = str(val).replace("\r", " ").replace("\t", " ").replace("\n", " ")
    s = s.replace("\\", "\\\\")
    return s

def copy_via_tempfile(table_name, columns, rows):
    if not rows:
        return
    tmp = tempfile.NamedTemporaryFile(delete=False, mode="w", encoding="utf-8", dir=TMP_DIR)
    try:
        for r in rows:
            tmp.write("\t".join(sql_escape(x) for x in r) + "\n")
        tmp.flush(); tmp.close()
        sql = f"\\copy {table_name} ({','.join(columns)}) FROM '{tmp.name}' WITH (FORMAT text, DELIMITER E'\\t');"
        pg_exec(dbpath=dbpath, sql=sql)
    finally:
        try:
            os.unlink(tmp.name)
        except Exception:
            pass

# =============== 删除旧表并重建 ===============
for tbl in [entry_table, member_table, relation_table, class_table]:
    pg_exec(dbpath=dbpath, sql=f"DROP TABLE IF EXISTS {tbl};")

pg_exec(dbpath=dbpath, sql=f"""
CREATE TABLE {entry_table} (
    ipr_id     TEXT PRIMARY KEY,
    type       TEXT,
    short_name TEXT,
    name       TEXT,
    abstract   TEXT
);
""")

pg_exec(dbpath=dbpath, sql=f"""
CREATE TABLE {member_table} (
    ipr_id TEXT,
    db     TEXT,
    dbkey  TEXT,
    name   TEXT
);
""")

pg_exec(dbpath=dbpath, sql=f"""
CREATE TABLE {relation_table} (
    parent TEXT,
    child  TEXT,
    type   TEXT
);
""")

pg_exec(dbpath=dbpath, sql=f"""
CREATE TABLE {class_table} (
    ipr_id      TEXT,
    class_id    TEXT,
    class_type  TEXT,
    category    TEXT,
    description TEXT
);
""")

# 建索引以加速后续查询
pg_exec(dbpath=dbpath, sql=f"CREATE INDEX IF NOT EXISTS idx_member_ipr   ON {member_table}(ipr_id);")
pg_exec(dbpath=dbpath, sql=f"CREATE INDEX IF NOT EXISTS idx_rel_parent   ON {relation_table}(parent);")
pg_exec(dbpath=dbpath, sql=f"CREATE INDEX IF NOT EXISTS idx_rel_child    ON {relation_table}(child);")
pg_exec(dbpath=dbpath, sql=f"CREATE INDEX IF NOT EXISTS idx_class_ipr    ON {class_table}(ipr_id);")

print("✅ Tables created successfully.")

# =============== 解析 XML 并导入 ===============
BATCH_SIZE = 2000
entries, members, relations, classes = [], [], [], []

context = etree.iterparse(
    gzip.open(xml_path),
    events=("end",),
    tag="{*}interpro"
)

for _, elem in tqdm(context, desc="Importing InterPro entries"):
    ipr_id = elem.attrib.get("id", "")
    ipr_type = elem.attrib.get("type", "")
    short_name = elem.attrib.get("short_name", "")
    name = elem.findtext("{*}name", "")

    abstract_parts = [(p.text or "").strip() for p in elem.findall(".//{*}abstract/{*}p")]
    abstract = " ".join(x for x in abstract_parts if x)

    entries.append((ipr_id, ipr_type, short_name, name, abstract))

    # === member_list ===
    for m in elem.findall(".//{*}member_list/{*}db_xref"):
        members.append((
            ipr_id,
            m.attrib.get("db"),
            m.attrib.get("dbkey"),
            m.attrib.get("name")
        ))

    # === classification（GO注释）===
    for c in elem.findall(".//{*}class_list/{*}classification"):
        class_id = c.attrib.get("id")
        class_type = c.attrib.get("class_type")
        category = c.findtext("{*}category", "")
        desc = c.findtext("{*}description", "")
        if class_id:
            classes.append((ipr_id, class_id, class_type, category, desc))

    # === 关系 parent_list / child_list ===
    for r in elem.findall(".//{*}parent_list/{*}rel_ref"):
        parent_id = r.attrib.get("ipr_ref")
        if parent_id:
            relations.append((parent_id, ipr_id, "parent"))

    for r in elem.findall(".//{*}child_list/{*}rel_ref"):
        child_id = r.attrib.get("ipr_ref")
        if child_id:
            relations.append((ipr_id, child_id, "child"))

    elem.clear()
    while elem.getprevious() is not None:
        del elem.getparent()[0]

    # === 批量写入 ===
    if len(entries) >= BATCH_SIZE:
        copy_via_tempfile(entry_table,  ["ipr_id","type","short_name","name","abstract"], entries)
        copy_via_tempfile(member_table, ["ipr_id","db","dbkey","name"], members)
        copy_via_tempfile(relation_table, ["parent","child","type"], relations)
        copy_via_tempfile(class_table, ["ipr_id","class_id","class_type","category","description"], classes)
        entries.clear(); members.clear(); relations.clear(); classes.clear()

# === 写入剩余未满批的记录 ===
copy_via_tempfile(entry_table,  ["ipr_id","type","short_name","name","abstract"], entries)
copy_via_tempfile(member_table, ["ipr_id","db","dbkey","name"], members)
copy_via_tempfile(relation_table, ["parent","child","type"], relations)
copy_via_tempfile(class_table, ["ipr_id","class_id","class_type","category","description"], classes)

print("✅ Import completed successfully (COPY mode).")

# =============== 验证导入结果 ===============
check_sql = "SELECT ipr_id, type, short_name, name FROM interpro_entry WHERE ipr_id='IPR011009';"
print("🔎 Check IPR011009:\n", pg_exec(dbpath=dbpath, sql=check_sql))

print("📊 Counts:")
print(pg_exec(
    dbpath=dbpath,
    sql=f"""
        SELECT 'entry' AS t, COUNT(*) FROM {entry_table}
        UNION ALL SELECT 'member', COUNT(*) FROM {member_table}
        UNION ALL SELECT 'relation', COUNT(*) FROM {relation_table}
        UNION ALL SELECT 'class', COUNT(*) FROM {class_table};
    """
))

print("🔎 relation samples:")
print(pg_exec(dbpath=dbpath, sql=f"SELECT * FROM {relation_table} LIMIT 10;"))

print("🔎 classification samples:")
print(pg_exec(dbpath=dbpath, sql=f"SELECT * FROM {class_table} LIMIT 10;"))