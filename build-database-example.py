# %%
import os
import ctypes

HOME = os.path.expanduser("~")

# 加入环境变量
os.environ["LD_LIBRARY_PATH"] = os.path.join(HOME, "pgsql/lib") + ":" + os.environ.get("LD_LIBRARY_PATH", "")

# 先加载 libpq
ctypes.CDLL(os.path.join(HOME, "pgsql/lib/libpq.so"), mode=ctypes.RTLD_GLOBAL)

# 再加载 libpqxx
ctypes.CDLL(os.path.join(HOME, "pgsql/lib/libpqxx.so"), mode=ctypes.RTLD_GLOBAL)

from src.pmcad.core import find_files, insert_files_to_pgdb, init_pgdb, remove_pgdb

port = "55433"
pgbinpath = os.path.join(HOME, "pgsql/bin")
dbpath = os.path.join(HOME, "workspace/pmcdata_pro/database/999_pgdb")

# %%
# 第一步：初始化数据库和管理员用户
remove_pgdb(dbpath)
init_pgdb(
    dbpath=dbpath,
    pgbinpath=pgbinpath,
    port=port
)

# %%

# 在workspace/pmcdata_pro/data/999文件夹下所有子文件夹中获取所有final_file_old_999.tsv类型的文件名
filelist = find_files(
    os.path.join(HOME, "workspace/pmcdata_pro/data/999"),
    r"^final_file_old_(\d+)\.tsv$",
)

# %%
print(f"START:{len(filelist)}")

# 把这些文件名的tsv格式文件插入数据库, 表头取第一个文件的表头, 每个文件以\t分隔
insert_files_to_pgdb(filelist, "final_file", dbpath, verbose=True)

# %%
