import os
from io import StringIO
import json
import subprocess
from .core import pg_exec
import pandas as pd
from tqdm import tqdm
import tempfile
import time


def pg_get_table(dbpath, table_name, limit=None):
    if limit is not None:
        sql = f"COPY (SELECT * FROM {table_name} LIMIT {limit}) TO STDOUT WITH (FORMAT CSV, DELIMITER E'\t', HEADER);"
    else:
        sql = f"COPY (SELECT * FROM {table_name}) TO STDOUT WITH (FORMAT CSV, DELIMITER E'\t', HEADER);"
    output = pg_exec(dbpath, sql=sql)
    return pd.read_csv(StringIO(output), sep="\t")


def pg_get_by_index(
    dbpath, table_name, index_col, index_val, use_index=None, explain=False
):
    """
    根据索引列值查询 PostgreSQL 表，可选指定索引。
    """
    if isinstance(index_val, str):
        condition = f"{index_col} = '{index_val}'"
    else:
        condition = f"{index_col} = {index_val}"

    sql_parts = []

    # 可选：控制 planner 行为
    if use_index:
        sql_parts += [
            "SET enable_seqscan = off;",
            "SET enable_bitmapscan = off;",
            "SET enable_indexscan = on;",
        ]

    # 可选：仅调试时查看执行计划
    if explain:
        explain_sql = f"EXPLAIN ANALYZE SELECT * FROM {table_name} WHERE {condition};"
        plan = pg_exec(dbpath, sql=explain_sql)
        print("🔍 Query Plan:\n", plan)

    # 真正的数据导出
    sql_parts.append(
        f"""
    COPY (
        SELECT * FROM {table_name}
        WHERE {condition}
    ) TO STDOUT WITH (FORMAT CSV, DELIMITER E'\\t', HEADER);
    """
    )

    sql = "\n".join(sql_parts)
    output = pg_exec(dbpath, sql=sql)

    # 防御性：过滤非 CSV 内容（有时前几行可能包含 notice）
    csv_part = "\n".join(
        [line for line in output.splitlines() if "\t" in line or "accession" in line]
    )

    if not csv_part.strip():
        return pd.DataFrame()

    return pd.read_csv(StringIO(csv_part), sep="\t")


def convert_gene_to_protein(dbpath, df_gene_go, batch_size=5000):
    """
    将 (gene_id, go_id, weight, ...) 映射为 (uniprot_id, gene_id, go_id, weight, ...)
    仅查询 df_gene_go 中涉及的 GeneID（自动去重 + 分批处理）
    保留原始 df_gene_go 的所有列。
    """
    # 去重后的 GeneID 列表
    gene_ids = df_gene_go["gene_id"].astype(str).unique().tolist()
    print(f"🔍 检测到 {len(gene_ids)} 个唯一基因ID，开始分批查询...")

    all_batches = []
    for i in range(0, len(gene_ids), batch_size):
        batch = gene_ids[i : i + batch_size]
        id_list = "', '".join(batch)

        sql = f"""
        COPY (
            SELECT db_id AS gene_id, uniprot_id
            FROM uniprot_idmapping
            WHERE db_name = 'GeneID'
              AND db_id IN ('{id_list}')
        ) TO STDOUT WITH (FORMAT CSV, DELIMITER E'\\t', HEADER);
        """
        output = pg_exec(dbpath, sql=sql)
        if not output.strip():
            continue

        batch_df = pd.read_csv(StringIO(output), sep="\t")
        all_batches.append(batch_df)
        print(f"  ✅ 已完成 {i + len(batch)} / {len(gene_ids)}")

    # 合并所有批次
    if not all_batches:
        print("⚠️ 未找到任何映射记录。")
        return pd.DataFrame(columns=["uniprot_id", *df_gene_go.columns])

    mapping_df = pd.concat(all_batches, ignore_index=True).drop_duplicates()

    # 统一类型，避免合并时类型不匹配
    df_gene_go["gene_id"] = df_gene_go["gene_id"].astype(str)
    mapping_df["gene_id"] = mapping_df["gene_id"].astype(str)

    # 🔗 合并到原始 gene–GO 表（保留所有列）
    merged = pd.merge(df_gene_go, mapping_df, on="gene_id", how="inner")

    # 保留 uniprot_id + 原始所有列（gene_id 包含在内）
    df_protein_go = merged[["uniprot_id", *df_gene_go.columns]]

    print(f"✅ 成功映射 {len(df_protein_go)} 条记录，来自 {len(gene_ids)} 个基因。")
    return df_protein_go


def get_protein_domains(dbpath, protein_list, batch_size=500):
    """
    从 uniprot_sprot_ft 和 uniprot_trembl_ft 中批量获取蛋白的 DOMAIN 注释。

    参数:
        dbpath (str): 数据库路径（包含 database.info）
        protein_list (list[str]): 要查询的 Uniprot ID 列表
        batch_size (int): 每批查询数量，默认 500，防止 SQL 太长

    返回:
        pd.DataFrame: 包含 (uniprot_id, feature_type, start_pos, end_pos, note, evidence)
                      仅保留 feature_type='DOMAIN' 的行
    """
    all_results = []

    # 去重
    protein_list = list(set(protein_list))
    print(f"🔍 共需查询 {len(protein_list)} 个 Uniprot ID")

    for i in tqdm(range(0, len(protein_list), batch_size)):
        batch = protein_list[i : i + batch_size]
        id_list = "', '".join(batch)

        sql = f"""
        COPY (
            SELECT accession AS uniprot_id, feature_type, start_pos, end_pos, note, evidence
            FROM uniprot_sprot_ft
            WHERE feature_type = 'DOMAIN' AND accession IN ('{id_list}')
            UNION ALL
            SELECT accession AS uniprot_id, feature_type, start_pos, end_pos, note, evidence
            FROM uniprot_trembl_ft
            WHERE feature_type = 'DOMAIN' AND accession IN ('{id_list}')
        ) TO STDOUT WITH (FORMAT CSV, DELIMITER E'\\t', HEADER);
        """

        try:
            output = pg_exec(dbpath, sql=sql)
            if output.strip():
                df = pd.read_csv(StringIO(output), sep="\t")
                all_results.append(df)
        except Exception as e:
            print(f"⚠️ 批次 {i // batch_size + 1} 查询失败: {e}")

    if all_results:
        df_all = pd.concat(all_results, ignore_index=True)
        print(f"✅ 共获取 {len(df_all)} 条 domain 记录。")
        return df_all
    else:
        print("⚠️ 未查询到任何 DOMAIN 注释。")
        return pd.DataFrame(
            columns=[
                "uniprot_id",
                "feature_type",
                "start_pos",
                "end_pos",
                "note",
                "evidence",
            ]
        )


def get_protein_go_terms(dbpath, protein_list, batch_size=500):
    """
    从 uniprot_sprot_dr 和 uniprot_trembl_dr 中批量获取蛋白对应的 GO 注释。

    参数:
        dbpath (str): 数据库路径（包含 database.info）
        protein_list (list[str]): 要查询的 Uniprot accession 列表
        batch_size (int): 每批查询数量，默认 500

    返回:
        pd.DataFrame: 包含两列 ["uniprot_id", "go_id"]
    """
    all_results = []
    protein_list = list(set(protein_list))
    print(f"🔍 共需查询 {len(protein_list)} 个 Uniprot ID")

    for i in tqdm(range(0, len(protein_list), batch_size)):
        batch = protein_list[i : i + batch_size]
        id_list = "', '".join(batch)

        sql = f"""
        COPY (
            SELECT accession AS uniprot_id, db_id AS go_id
            FROM uniprot_sprot_dr
            WHERE db_name = 'GO' AND accession IN ('{id_list}')
            UNION ALL
            SELECT accession AS uniprot_id, db_id AS go_id
            FROM uniprot_trembl_dr
            WHERE db_name = 'GO' AND accession IN ('{id_list}')
        ) TO STDOUT WITH (FORMAT CSV, DELIMITER E'\\t', HEADER);
        """

        try:
            output = pg_exec(dbpath, sql=sql)
            if output.strip():
                df = pd.read_csv(StringIO(output), sep="\t")
                all_results.append(df)
        except Exception as e:
            print(f"⚠️ 批次 {i // batch_size + 1} 查询失败: {e}")

    if all_results:
        df_all = pd.concat(all_results, ignore_index=True)
        print(f"✅ 共获取 {len(df_all)} 条 GO 注释记录。")
        return df_all
    else:
        print("⚠️ 未查询到任何 GO 注释。")
        return pd.DataFrame(columns=["uniprot_id", "go_id"])

def get_protein_interpro_terms(
    dbpath,
    protein_list,
    filter_types=None,
    remove_child_relations=False,
):
    """
    从 uniprot_sprot_dr 和 uniprot_trembl_dr 中批量获取蛋白对应的 InterPro 注释。
    （无 for 循环版，使用 COPY TO 文件导出全量结果）

    参数:
        dbpath (str): 数据库路径（包含 database.info）
        protein_list (list[str]): 要查询的 Uniprot accession 列表
        filter_types (list[str] | None): 限定 interpro_entry.type，例如 ["Domain", "Binding_site"]
        remove_child_relations (bool): 是否消除父子关系（保留父节点）

    返回:
        pd.DataFrame: 包含 ["uniprot_id", "interpro_id"]
    """

    protein_list = list(set(protein_list))
    print(f"🔍 共需查询 {len(protein_list)} 个 Uniprot ID")

    if len(protein_list) == 0:
        return pd.DataFrame(columns=["uniprot_id", "interpro_id"])

    # === Step 1: 创建临时表并导入蛋白ID ===
    tmp_table_name = "tmp_protein_ids"
    tmp_file_in = tempfile.mktemp(suffix=".csv")
    pd.Series(protein_list, name="uniprot_id").to_csv(tmp_file_in, index=False, header=False)

    sql_create = f"""
    DROP TABLE IF EXISTS {tmp_table_name};
    CREATE TABLE {tmp_table_name} (uniprot_id TEXT);
    """
    pg_exec(dbpath=dbpath, sql=sql_create)

    sql_copy_in = f"\\COPY {tmp_table_name} FROM '{tmp_file_in}' WITH (FORMAT csv);"
    pg_exec(dbpath=dbpath, sql=sql_copy_in)
    os.remove(tmp_file_in)
    print(f"✅ {len(protein_list)} 条蛋白 accession 已导入 {tmp_table_name}。")

    # === Step 2: 如果限定类型，先取出允许的 InterPro ID 集合 ===
    interpro_filter_set = None
    if filter_types:
        type_list = "', '".join(filter_types)
        filter_sql = f"""
        COPY (
            SELECT ipr_id AS interpro_id
            FROM interpro_entry
            WHERE type IN ('{type_list}')
        ) TO STDOUT WITH (FORMAT CSV, DELIMITER E'\\t', HEADER);
        """
        df_type = pd.read_csv(StringIO(pg_exec(dbpath=dbpath, sql=filter_sql)), sep="\t")
        interpro_filter_set = set(df_type["interpro_id"].tolist())
        print(f"✅ 限定 InterPro 类型为 {filter_types}，保留 {len(interpro_filter_set)} 条记录。")

    # === Step 3: 一次性 JOIN 导出所有匹配结果 ===
    tmp_file_out = tempfile.mktemp(suffix=".csv")
    sql_export = f"""
    COPY (
        SELECT s.accession AS uniprot_id, s.db_id AS interpro_id
        FROM uniprot_sprot_dr s
        WHERE s.db_name = 'InterPro'
        AND s.accession IN (SELECT uniprot_id FROM {tmp_table_name})
        UNION ALL
        SELECT t1.accession AS uniprot_id, t1.db_id AS interpro_id
        FROM uniprot_trembl_dr t1
        WHERE t1.db_name = 'InterPro'
        AND t1.accession IN (SELECT uniprot_id FROM {tmp_table_name})
    ) TO '{tmp_file_out}' WITH (FORMAT csv, DELIMITER E'\\t', HEADER);
    """
    pg_exec(dbpath=dbpath, sql=sql_export)
    pg_exec(dbpath=dbpath, sql=f"DROP TABLE IF EXISTS {tmp_table_name};")
    print(f"📂 已生成匹配结果文件: {tmp_file_out}")

    # === Step 4: 读取结果并过滤 ===
    df_all = pd.read_csv(tmp_file_out, sep="\t").drop_duplicates()
    os.remove(tmp_file_out)
    print(f"✅ 共读取 {len(df_all)} 条 InterPro 注释。")

    if interpro_filter_set is not None:
        before = len(df_all)
        df_all = df_all[df_all["interpro_id"].isin(interpro_filter_set)]
        print(f"🧩 已根据类型筛选，保留 {len(df_all)} 条（过滤掉 {before - len(df_all)} 条）。")

    # === Step 5: 去除父子关系的子节点（可选） ===
    if remove_child_relations:
        print("🧬 正在加载 InterPro 父子关系表以移除子项...")
        rel_sql = """
        COPY (
            SELECT parent, child
            FROM interpro_relation
        ) TO STDOUT WITH (FORMAT CSV, DELIMITER E'\\t', HEADER);
        """
        df_rel = pd.read_csv(StringIO(pg_exec(dbpath=dbpath, sql=rel_sql)), sep="\t")
        child_set = set(df_rel["child"])
        before = len(df_all)
        df_all = df_all[~df_all["interpro_id"].isin(child_set)]
        after = len(df_all)
        print(f"✅ 已移除 {before - after} 条子层级注释，仅保留父层级。")

    print(f"🎯 最终输出 {len(df_all)} 条 InterPro 注释记录。")
    return df_all

def convert_gene_to_best_protein_with_interpro(
    dbpath,
    df_gene_go,
    filter_types=None,
    remove_child_relations=False,
):
    """
    将 (gene_id, go_id, weight, ...) 映射为最优 uniprot_id
    使用 PostgreSQL COPY TO 文件方式（高速、低内存）
    改为永久表模式，自动检查/删除旧表。

    步骤:
      1. 将 gene_id 写入持久表 tmp_gene_ids
      2. JOIN uniprot_idmapping 表获得 gene→uniprot 映射
      3. 导出到临时 CSV 文件并读入 pandas
      4. 计算 InterPro 注释数量并挑选最优蛋白
      5. 合并原 gene–GO 表，返回 df_protein_go
    """

    start_time = time.time()
    gene_ids = df_gene_go["gene_id"].astype(str).unique().tolist()
    print(f"🔍 检测到 {len(gene_ids)} 个唯一 GeneID，准备创建持久表...")

    # === Step 1. 将 GeneID 写入持久表 ===
    tmp_table_name = "tmp_gene_ids"

    sql_check_drop = f"""
    DROP TABLE IF EXISTS {tmp_table_name};
    CREATE TABLE {tmp_table_name} (gene_id TEXT);
    """
    pg_exec(dbpath=dbpath, sql=sql_check_drop)

    tmp_gene_path = tempfile.mktemp(suffix=".csv")
    pd.Series(gene_ids, name="gene_id").to_csv(tmp_gene_path, index=False, header=False)

    sql_copy_in = f"\\COPY {tmp_table_name} FROM '{tmp_gene_path}' WITH (FORMAT csv);"
    pg_exec(dbpath=dbpath, sql=sql_copy_in)
    os.remove(tmp_gene_path)
    print(f"✅ GeneID 已导入表 {tmp_table_name}。")

    # === Step 2. COPY JOIN 结果直接写到文件 ===
    tmp_out_path = tempfile.mktemp(suffix=".csv")
    sql_export = f"""
    COPY (
        SELECT t.gene_id, m.uniprot_id
        FROM {tmp_table_name} t
        JOIN uniprot_idmapping m
        ON t.gene_id = m.db_id
        WHERE m.db_name = 'GeneID'
    ) TO '{tmp_out_path}' WITH (FORMAT csv, DELIMITER E'\\t', HEADER);
    """
    pg_exec(dbpath=dbpath, sql=sql_export)
    print(f"📂 已生成映射结果文件: {tmp_out_path}")

    # === Step 3. 读入结果 ===
    df_map = pd.read_csv(tmp_out_path, sep="\t").drop_duplicates()
    os.remove(tmp_out_path)
    print(f"✅ 获取 {len(df_map)} 条 Gene–Uniprot 映射记录。")

    # === Step 4. 清理持久表 ===
    pg_exec(dbpath=dbpath, sql=f"DROP TABLE IF EXISTS {tmp_table_name};")
    print(f"🧹 已清理表 {tmp_table_name}。")

    if df_map.empty:
        print("⚠️ 未找到任何 GeneID–Uniprot 映射。")
        return pd.DataFrame(columns=["uniprot_id", *df_gene_go.columns])

    # === Step 5. 获取 InterPro 注释信息 ===
    all_proteins = df_map["uniprot_id"].unique().tolist()
    print(f"🔬 检测到 {len(all_proteins)} 个唯一蛋白，开始获取 InterPro 注释...")

    df_ipr = get_protein_interpro_terms(
        dbpath=dbpath,
        protein_list=all_proteins,
        filter_types=filter_types,
        remove_child_relations=remove_child_relations,
    )

    if df_ipr.empty:
        print("⚠️ 未获取到 InterPro 注释，默认保留首个映射。")
        best_map = df_map.groupby("gene_id").first().reset_index()
    else:
        ipr_count = (
            df_ipr.groupby("uniprot_id")["interpro_id"]
            .nunique()
            .reset_index(name="ipr_count")
        )
        df_map = df_map.merge(ipr_count, on="uniprot_id", how="left").fillna({"ipr_count": 0})

        # === Step 6. 选择最优蛋白（sprot 优先） ===
        print("⚙️ 正在选择最优蛋白...")
        def select_best(group):
            sprot = group[group["uniprot_id"].str.startswith("P")]
            trembl = group[~group["uniprot_id"].str.startswith("P")]
            if len(sprot) >= 1:
                return sprot.loc[sprot["ipr_count"].idxmax()]
            elif len(trembl) > 0:
                return trembl.loc[trembl["ipr_count"].idxmax()]
            else:
                return group.iloc[0]

        best_map = (
            df_map.groupby("gene_id", group_keys=False)
            .apply(select_best)
            .reset_index(drop=True)
        )
        print(f"✅ 已为 {len(best_map)} 个基因选择最优蛋白。")

    # === Step 7. 合并原始 gene–GO 表 ===
    df_gene_go["gene_id"] = df_gene_go["gene_id"].astype(str)
    best_map["gene_id"] = best_map["gene_id"].astype(str)
    merged = pd.merge(df_gene_go, best_map[["gene_id", "uniprot_id"]], on="gene_id", how="inner")
    df_protein_go = merged[["uniprot_id", *df_gene_go.columns]]

    total_genes = df_gene_go["gene_id"].nunique()
    mapped_genes = best_map["gene_id"].nunique()
    unmapped = total_genes - mapped_genes

    print(f"🎯 成功生成 {len(df_protein_go)} 条映射记录，覆盖 {mapped_genes}/{total_genes} 个基因。")
    if unmapped > 0:
        print(f"⚠️ 其中 {unmapped} 个基因未能找到对应的 Uniprot ID。")
    print(f"⏱️ 总耗时 {time.time() - start_time:.2f} 秒。")

    return df_protein_go