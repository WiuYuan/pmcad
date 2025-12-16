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

from src.pmcad.core import find_files, insert_files_to_pgdb, init_pgdb, remove_pgdb, pg_exec

port = "55433"
pgbinpath = os.path.join(HOME, "pgsql/bin")
dbpath = os.path.join(HOME, "/data/wyuan/workspace/pmcdata_pro/database/protease_pgdb")


# %%

# 在workspace/pmcdata_pro/data/999文件夹下所有子文件夹中获取所有final_file_old_999.tsv类型的文件名
filelist = find_files(
    os.path.join(HOME, "workspace/pmcdata_pro/data/999"),
    r"^final_file_old_(\d+)\.tsv$",
)

# %%
print(f"START:{len(filelist)}")
table_name = "final_file_test"

# 把这些文件名的tsv格式文件插入数据库, 表头取第一个文件的表头, 每个文件以\t分隔
insert_files_to_pgdb(filelist, table_name, dbpath, verbose=True)

# %%
# 查看有哪些表格
sql = "SELECT table_name FROM information_schema.tables WHERE table_schema='public';"
output = pg_exec(dbpath, sql)
print("Tables in database:\n", output)

# 查看 final_file 表前 5 行
sql = f"SELECT * FROM {table_name} LIMIT 5;"
output = pg_exec(dbpath, sql)
print("前 5 行数据:\n", output)