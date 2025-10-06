from typing import List, Union, Dict
import pandas as pd
from ._core import read_multi_tsv
from ._core import find_files as _find_files
from ._core import match_reference as _match_reference
from ._core import insert_files_to_pgdb as _insert_files_to_pgdb
import os
import subprocess
import time
import json
import signal


def read_tsv_files(filelist: List[str]) -> pd.DataFrame:
    """Read multiple TSV files and return a single DataFrame"""
    # 调用 read_multi_tsv 读取所有文件的数据
    data = read_multi_tsv(filelist)  # 假设返回的是一个合并后的数据列表

    # 如果数据非空，使用第一行作为列名
    if data:
        columns = data[0]  # 假设第一行是列名
        return pd.DataFrame(data[1:], columns=columns)  # 返回合并后的 DataFrame
    else:
        return pd.DataFrame()  # 如果没有数据，返回一个空 DataFrame


def create_dict(
    x: List[str], y: List[str], splitx_by: Union[List[str], str] = []
) -> dict:
    """
    创建一个字典，将 x 中的每个元素映射到 y 中的相应元素，
    如果 splitx_by 中提供了分隔符，会将 x 中的元素按这些分隔符拆分，拆分后的子元素去除前后空格后也映射到相同的 y 元素。

    参数:
        x (List[str]): 用作字典键的列表
        y (List[str]): 用作字典值的列表
        splitx_by (Optional[Union[List[str], str]], optional):
            用于拆分 x 中每个元素的分隔符，可以是字符串（单一分隔符）或字符串列表（多个分隔符），默认为空列表

    返回:
        dict: 映射后的字典
    """

    # 处理 splitx_by 类型为列表或字符串
    if isinstance(splitx_by, str):  # 如果是单个分隔符
        splitx_by = [splitx_by]

    result_dict = {}

    # 遍历 x 和 y，假设 x 和 y 长度一致
    for xi, yi in zip(x, y):
        if not isinstance(xi, str):  # 检查 xi 是否为字符串
            xi = str(xi)
        if xi == "nan":
            continue
        # 如果指定了分隔符，则拆分 x 中的元素
        if splitx_by:
            for delimiter in splitx_by:
                xi_split = xi.split(delimiter)  # 按指定分隔符拆分
                for sub_x in xi_split:
                    sub_x = sub_x.strip().lower()  # 去除前后空格并转为小写
                    if sub_x in result_dict:
                        result_dict[sub_x].append(
                            yi
                        )  # 如果已经存在，添加到现有的列表中
                    else:
                        result_dict[sub_x] = [yi]  # 如果不存在，创建新的映射
        else:
            xi = xi.strip().lower()  # 如果没有分隔符，直接将元素映射，并去除前后空格
            if xi in result_dict:
                result_dict[xi].append(yi)
            else:
                result_dict[xi] = [yi]

    return result_dict


def match_reference(
    query: List[str], reference: Dict[str, List[str]], verbose: bool = False
) -> Dict[str, List[str]]:
    return _match_reference(query, reference, verbose)


def find_files(foldername: str, pattern: str) -> List[str]:
    return _find_files(foldername, pattern)


def insert_files_to_pgdb(filelist: List[str], table_name: str, dbpath: str, verbose: bool = False):
    """
    将文件列表插入 PostgreSQL 数据库表中，直接通过 dbpath 自动获取连接信息。
    
    参数:
        filelist (List[str]): 文件路径列表
        table_name (str): 数据库表名
        dbpath (str): 数据库路径，包含 database.info
        verbose (bool): 是否输出详细信息
    """
    info_file = os.path.join(dbpath, "database.info")
    if not os.path.exists(info_file):
        raise FileNotFoundError(f"No database.info found at {info_file}")

    with open(info_file, "r") as f:
        db_info = json.load(f)

    dbname = db_info["dbname"]
    user = db_info["user"]
    password = db_info["password"]
    host = db_info.get("host", "localhost")
    port = str(db_info.get("port", 5432))

    _insert_files_to_pgdb(
        filelist=filelist,
        table_name=table_name,
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port,
        verbose=verbose
    )

def init_pgdb(dbpath, pgbinpath, admin_user="postgres", admin_password="postgres", host="localhost", port=5432, wait_seconds=2):
    """
    初始化 PostgreSQL 数据库并创建管理员用户（可选默认账号密码）

    参数:
        dbpath (str): 数据库存放目录，会在此目录下生成 data/
        pgbinpath (str): PostgreSQL 可执行程序目录，必须包含 initdb、pg_ctl、psql
        admin_user (str): 管理员用户名（默认 "postgres"）
        admin_password (str): 管理员密码（默认 "postgres"）
        host (str): 数据库主机（默认 "localhost"）
        port (int): PostgreSQL 服务端口（默认 5432）
        wait_seconds (float): 启动数据库后的等待时间，保证数据库可连接

    返回:
        dict: 数据库连接信息
    """

    os.makedirs(dbpath, exist_ok=True)
    data_dir = os.path.join(dbpath, "data")
    os.makedirs(data_dir, exist_ok=True)

    initdb_path = os.path.join(pgbinpath, "initdb")
    pg_ctl_path = os.path.join(pgbinpath, "pg_ctl")
    psql_path = os.path.join(pgbinpath, "psql")
    dbname = "postgres"

    # 1️⃣ 初始化数据库（如果还没有 PG_VERSION）
    if not os.path.exists(os.path.join(data_dir, "PG_VERSION")):
        subprocess.run([initdb_path, "-D", data_dir, "--username", admin_user], check=True)
        print(f"Initialized PostgreSQL data directory at {data_dir} with admin user '{admin_user}'")

    # 2️⃣ 启动数据库
    subprocess.run(
        [pg_ctl_path, "-D", data_dir, "-o", f"-p {port}", "-l", os.path.join(data_dir, "logfile"), "start"],
        check=True
    )
    print("PostgreSQL server starting...")
    time.sleep(wait_seconds)

    # 3️⃣ 设置管理员密码
    set_password_sql = f"ALTER USER {admin_user} WITH PASSWORD '{admin_password}';"
    subprocess.run(
        [psql_path, "-U", admin_user, "-p", str(port), "-d", dbname, "-c", set_password_sql],
        check=True,
        env={**os.environ, "PGPASSWORD": ""}
    )

    print(f"Admin user '{admin_user}' ready on port {port}")

    # 4️⃣ 保存连接信息到 dbpath/database.info
    db_info = {
        "pgbinpath": pgbinpath,
        "dbname": dbname,
        "user": admin_user,
        "password": admin_password,
        "host": host,
        "port": port,
        "data_dir": data_dir
    }
    info_path = os.path.join(dbpath, "database.info")
    with open(info_path, "w") as f:
        json.dump(db_info, f, indent=2)
    print(f"Database connection info saved to {info_path}")

    return db_info

def remove_pgdb(dbpath):
    """
    根据 dbpath/database.info 删除数据库文件，并释放端口。
    """
    info_file = os.path.join(dbpath, "database.info")
    if not os.path.exists(info_file):
        print(f"No database.info found at {info_file}")
        return

    # 读取端口信息
    with open(info_file, "r") as f:
        db_info = json.load(f)
    port = db_info.get("port", 5432)

    # 杀掉占用端口的 postgres 进程
    try:
        result = subprocess.run(
            ["lsof", "-ti", f":{port}"], capture_output=True, text=True
        )
        pids = result.stdout.strip().split("\n")
        for pid in pids:
            if pid:
                print(f"Killing process {pid} on port {port}")
                os.kill(int(pid), signal.SIGTERM)
    except Exception as e:
        print(f"Failed to kill processes on port {port}: {e}")

    # 删除数据库目录
    data_dir = db_info.get("data_dir", os.path.join(dbpath, "data"))
    if os.path.exists(data_dir):
        for root, dirs, files in os.walk(data_dir, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(data_dir)
        print(f"Deleted data directory: {data_dir}")

    # 删除 database.info 文件
    os.remove(info_file)
    print(f"Deleted database info file: {info_file}")

    print(f"Database at {dbpath} removed and port {port} freed.")
    
def pg_exec(dbpath, sql=None, interactive=False):
    """
    在 Python 中调用 PostgreSQL 命令行工具 (psql)
    可以执行 SQL 命令或进入交互模式。

    参数:
        dbpath (str): 包含 database.info 的数据库路径
        sql (str): 要执行的 SQL 命令（如 "SELECT * FROM table;"），可选
        interactive (bool): 若为 True，则进入交互式 psql shell

    返回:
        str: SQL 命令输出结果（若非交互模式）
    """
    info_file = os.path.join(dbpath, "database.info")
    if not os.path.exists(info_file):
        raise FileNotFoundError(f"No database.info found at {info_file}")

    # 读取连接信息
    with open(info_file, "r") as f:
        db_info = json.load(f)

    psql_path = os.path.join(db_info["pgbinpath"], "psql")

    env = {**os.environ, "PGPASSWORD": db_info["password"]}

    cmd = [
        psql_path,
        "-U", db_info["user"],
        "-d", db_info["dbname"],
        "-h", db_info["host"],
        "-p", str(db_info["port"]),
    ]

    # 若 interactive=True，则直接进入 psql 终端
    if interactive:
        print(f"Connecting to PostgreSQL at port {db_info['port']}...\n")
        subprocess.run(cmd, env=env)
        return

    # 若给定 SQL，则执行并返回结果
    if sql:
        cmd += ["-c", sql]
        result = subprocess.run(cmd, capture_output=True, text=True, env=env)
        if result.returncode != 0:
            raise RuntimeError(f"SQL execution failed:\n{result.stderr}")
        return result.stdout.strip()

    raise ValueError("Must provide either sql command or set interactive=True")