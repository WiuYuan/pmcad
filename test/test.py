# %%
import os
import ctypes
import sys

sys.path.append("/data/wyuan/workspace/pmcdata_pro/pmcad")

# 加入环境变量
os.environ["LD_LIBRARY_PATH"] = "/data/wyuan/pgsql/lib:" + os.environ.get(
    "LD_LIBRARY_PATH", ""
)

# 先加载 libpq
ctypes.CDLL("/data/wyuan/pgsql/lib/libpq.so", mode=ctypes.RTLD_GLOBAL)

# 再加载 libpqxx
ctypes.CDLL("/data/wyuan/pgsql/lib/libpqxx.so", mode=ctypes.RTLD_GLOBAL)

from src.pmcad.core import import_gz_table, pg_exec

pvpath = "/data/wyuan/local/bin/pv"
dbpath = "/data/wyuan/workspace/pmcdata_pro/database/protease_pgdb"
sql = "SELECT * FROM gene_info ORDER BY GeneID DESC LIMIT 5;"
print(
    pg_exec(
        dbpath,
        """
    SELECT *
    FROM gene_info
    WHERE LocusTag = 'CHC_T00010258001';
    """,
    )
)
