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

from src.pmcad.core import pg_exec, import_uniprot_sq

pvpath = "/data/wyuan/local/bin/pv"
dbpath = "/data/wyuan/workspace/pmcdata_pro/database/protease_pgdb"

from src.pmcad.core import import_uniprot_sq

table_name = "uniprot_trembl_sq"

pg_exec(dbpath=dbpath, sql=f"DROP TABLE IF EXISTS {table_name};")
# 仅需两行即可：
UNIPROT_GZ = "/data/wyuan/workspace/pmcdata_pro/data/uniprot/uniprot_trembl.dat.gz"


import_uniprot_sq(dbpath, UNIPROT_GZ, table_name=table_name, verbose=False)