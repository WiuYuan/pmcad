import json
import networkx as nx
import re
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import random
import matplotlib.colors as mcolors
import matplotlib.cm as cm
import pandas as pd
from typing import List, Union

def read_go_plus_to_graph(file_path: str) -> nx.DiGraph:
    """
    读取 GO-Plus JSON 文件并构建有向图 (networkx.DiGraph)
    - 解析 nodes 与 edges
    - 解析 meta.basicPropertyValues 中的 val--pred-->当前节点 映射
    - 统一去除 URI 前缀（删除 '/obo/' 及其前的所有部分）
    """

    try:
        with open(file_path, "r") as f:
            data = json.load(f)
    except Exception as e:
        raise RuntimeError(f"❌ Failed to read GO JSON: {e}")

    G = nx.DiGraph()

    # 通用去前缀函数：删除 URI 前缀及 /obo/ 之前所有内容
    def clean_uri(s: str) -> str:
        if not isinstance(s, str):
            return s
        # 删除 /obo/ 及其之前所有部分
        s = re.sub(r".*/obo/", "", s)
        # 删除 URI 前缀如 http://、https://、oboInOwl# 等
        s = re.sub(r".*[#/]", "", s)
        s = s.strip()
        # 统一成 PREFIX:NUMBER（将 PREFIX_NUMBER 变成 PREFIX:NUMBER）
        s = re.sub(r"^([A-Za-z]+)_(\d+)$", r"\1:\2", s)
        return s.strip()

    # ----------- 添加节点 -----------
    for item in data["graphs"][0]["nodes"]:
        node_id = item["id"]
        new_name = clean_uri(node_id)
        lbl = item.get("lbl", "")
        meta = item.get("meta", {})
        definition = meta.get("definition", {}).get("val", "")
        comments = meta.get("comments", [])

        # 添加节点
        G.add_node(
            new_name,
            lbl=lbl,
            type=item.get("type", ""),
            definition=definition,
            comments=comments,
        )

        # ---------- 处理 basicPropertyValues ----------
        for bpv in meta.get("basicPropertyValues", []):
            pred = clean_uri(bpv.get("pred", ""))
            val = clean_uri(bpv.get("val", ""))
            if not val or not pred:
                continue
            if not pred.endswith("hasAlternativeId"):
                continue

            # 若值节点不存在则添加
            if val not in G:
                G.add_node(val, lbl=f"[ref] {val}")

            # 添加边: val --pred--> 当前节点
            G.add_edge(val, new_name, relation=pred)

    # ----------- 添加显式 edges -----------
    for edge in data["graphs"][0].get("edges", []):
        u = clean_uri(edge["sub"])
        v = clean_uri(edge["obj"])
        rel = clean_uri(edge.get("pred", ""))
        G.add_edge(u, v, relation=rel)

    print(f"✅ Graph loaded: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges.")
    return G


def get_ancestor_subgraph(G: nx.DiGraph, go_id: str) -> nx.DiGraph:
    """
    输入 GO term，返回所有祖先节点构成的子图（含自身）。
    """
    if go_id not in G:
        raise ValueError(f"{go_id} not found in graph.")

    ancestors = nx.ancestors(G, go_id)
    ancestors.add(go_id)
    return G.subgraph(ancestors).copy()


def get_descendant_subgraph(G: nx.DiGraph, go_id: str) -> nx.DiGraph:
    """
    输入 GO term，返回所有子节点（后代）构成的子图（含自身）。
    """
    if go_id not in G:
        raise ValueError(f"{go_id} not found in graph.")

    descendants = nx.descendants(G, go_id)
    descendants.add(go_id)
    return G.subgraph(descendants).copy()


def get_local_context_subgraph(G: nx.DiGraph, go_id: str) -> nx.DiGraph:
    """
    输入 GO term，返回包含该节点的所有祖先 + 后代 + 它们的 hasAlternativeId 别名节点 构成的局部子图。
    步骤：
      1. 若 go_id 是别名，通过 hasAlternativeId 找主节点；
      2. 以主节点为核心，使用 is_a 关系获取祖先和后代；
      3. 对这些节点，再附加其所有 hasAlternativeId 节点；
      4. 返回完整子图。

    参数:
        G : nx.DiGraph
            GO 全图
        go_id : str
            目标 GO term ID（例如 "GO:0008150"）

    返回:
        nx.DiGraph
            包含上下文的子图
    """
    if go_id not in G:
        raise ValueError(f"{go_id} not found in graph.")

    # --- Step 1️⃣: 构建 hasAlternativeId 子图（无向） ---
    alt_edges = [(u, v) for u, v, d in G.edges(data=True)
                 if d.get("relation") == "hasAlternativeId"]
    G_alt = nx.Graph()
    G_alt.add_edges_from(alt_edges)

    # --- Step 2️⃣: 找到 go_id 所属的“别名组” ---
    if go_id in G_alt:
        alias_group = list(nx.node_connected_component(G_alt, go_id))
        # 选主节点（字典序最小）
        main_node = sorted(alias_group)[0]
    else:
        alias_group = [go_id]
        main_node = go_id

    print(f"🔗 {go_id} 属于别名组 {alias_group}，主节点 = {main_node}")

    # --- Step 3️⃣: 构建 is_a 子图 ---
    is_a_edges = [(u, v) for u, v, d in G.edges(data=True)
                  if d.get("relation") == "is_a"]
    G_is_a = G.edge_subgraph(is_a_edges).copy()

    if main_node not in G_is_a:
        print(f"⚠️ 主节点 {main_node} 不在 is_a 层级中，返回仅含别名组的子图。")
        return G.subgraph(alias_group).copy()

    # --- Step 4️⃣: 基于主节点找祖先与后代 ---
    ancestors = nx.ancestors(G_is_a, main_node)
    descendants = nx.descendants(G_is_a, main_node)
    core_nodes = set(ancestors) | set(descendants) | {main_node}

    print(f"📚 层级节点数: 祖先 {len(ancestors)}, 后代 {len(descendants)}, 核心 {len(core_nodes)}")

    # --- Step 5️⃣: 为每个核心节点补充其 hasAlternativeId 节点 ---
    extended_nodes = set(core_nodes)
    for node in core_nodes:
        if node in G_alt:
            alias_nodes = list(nx.node_connected_component(G_alt, node))
            extended_nodes.update(alias_nodes)

    # --- Step 6️⃣: 提取完整子图 ---
    subG = G.subgraph(extended_nodes).copy()

    print(f"✅ {go_id}: 子图包含 {len(subG.nodes())} 个节点, {len(subG.edges())} 条边 "
          f"(祖先 {len(ancestors)}, 后代 {len(descendants)}, 含别名 {len(extended_nodes - core_nodes)})")

    return subG

def visualize_go_subgraph(
    G: nx.DiGraph,
    G_full: nx.DiGraph = None,
    go_weight_table=None,
    title="GO Hierarchy (Heatmap)",
    lable_weight=False,
    font_size=8,
    seed=42,
):
    """
    可视化 GO 子图（可选热力图风格）：
    - 若提供 go_weight_table，则节点按 weight 从白→红渐变；
    - 若无 go_weight_table，则节点为默认浅蓝色；
    - is_a 为黑色实线；
    - 其他关系自动分配颜色与线型；
    - 若 relation 在 G_full 中存在节点，则边标签替换为 lbl；
    - 右侧显示关系图例；若启用 heatmap，则显示颜色条。

    参数:
        G : nx.DiGraph
            GO 子图
        go_weight_table : pd.DataFrame, optional
            包含 'go_id' 与 'weight' 两列
        G_full : nx.DiGraph, optional
            完整 GO 图（用于 relation→lbl 替换）
        title : str
            图标题
        node_size : int
            节点方框大小
        font_size : int
            字体大小
        seed : int
            随机颜色控制种子
    """
    random.seed(seed)

    if G.number_of_nodes() == 0:
        print("⚠️ Graph is empty.")
        return

     # --- Heatmap 权重映射 ---
    weight_map, min_w, max_w = {}, 0, 1
    if go_weight_table is not None and len(go_weight_table) > 0:
        for _, row in go_weight_table.iterrows():
            weight_map[row["go_id"]] = row["weight"]
        min_w = go_weight_table["weight"].min()
        max_w = go_weight_table["weight"].max()

    # --- 🔁 合并并删除 hasAlternativeId 别名节点 ---
    alias_edges = [(u, v) for u, v, d in G.edges(data=True)
                   if d.get("relation") == "hasAlternativeId"]
    if alias_edges:
        G_alias = nx.Graph()
        G_alias.add_edges_from(alias_edges)
        alias_groups = list(nx.connected_components(G_alias))

        nodes_to_remove = set()
        for group in alias_groups:
            group = list(group)
            # 主节点优先选择：lbl 最长或字典序最小者
            main = sorted(group)[0]
            # 汇总组内权重（取平均）
            group_weights = [weight_map[g] for g in group if g in weight_map]
            if group_weights:
                merged_w = sum(group_weights) / len(group_weights)
                weight_map[main] = merged_w

            # 其他节点全部标记删除
            for g in group:
                if g != main:
                    nodes_to_remove.add(g)

        # 真正删除别名节点
        G.remove_nodes_from(nodes_to_remove)

        print(f"🔄 Merged & removed {len(nodes_to_remove)} alias nodes via hasAlternativeId.")
    
    # --- 🧹 删除所有未参与 is_a 关系的节点 ---
    is_a_edges = [(u, v) for u, v, d in G.edges(data=True)
                  if d.get("relation") == "is_a"]
    if is_a_edges:
        is_a_nodes = set([u for u, _ in is_a_edges] + [v for _, v in is_a_edges])
        removed_nodes = set(G.nodes()) - is_a_nodes
        for n in removed_nodes:
            print(f" - {n}: {G.nodes[n].get('lbl', '')} ({G.nodes[n].get('type', '')})")
        before = G.number_of_nodes()
        G = G.subgraph(is_a_nodes).copy()
        after = G.number_of_nodes()
        print(f"🧹 Removed {before - after} non-hierarchical nodes (not linked by is_a).")
    else:
        print("⚠️ No is_a edges found — skipped pruning.")
    
    # --- 🔁 删除节点后重新计算有效权重范围 ---
    if go_weight_table is not None and len(weight_map) > 0:
        valid_weights = [w for n, w in weight_map.items() if n in G.nodes()]
        if valid_weights:
            min_w, max_w = min(valid_weights), max(valid_weights)
        else:
            min_w, max_w = 0, 1

    norm = mcolors.Normalize(vmin=min_w, vmax=max_w)
    cmap = cm.Reds  # 从白到红

    # ------- 提取 is_a 层级结构 -------
    is_a_edges = [(u, v) for u, v, d in G.edges(data=True)
                  if d.get("relation") == "is_a"]
    G_is_a = G.edge_subgraph(is_a_edges).copy()

    # ------- 拓扑排序确定层级 -------
    if len(G_is_a) > 0:
        try:
            topo_order = list(nx.topological_sort(G_is_a))
        except nx.NetworkXUnfeasible:
            topo_order = list(G.nodes())
    else:
        topo_order = list(G.nodes())

    levels = {}

    # ------- 1️⃣ 先基于 is_a 构建基础层级 -------
    for node in reversed(topo_order):
        if node in G_is_a:
            children = list(G_is_a.successors(node))
        else:
            children = []
        if not children:
            levels[node] = 0
        else:
            levels[node] = max(levels.get(c, 0) + 1 for c in children)

    # ------- 2️⃣ 对无 is_a 边的节点进行修正 -------
    for node in G.nodes():
        if node not in levels:
            # 找出所有邻接节点（双向考虑）
            neighbors = list(G.predecessors(node)) + list(G.successors(node))
            # 过滤掉那些根本没有层级信息的邻居
            valid_neighbors = [levels[n] for n in neighbors if n in levels]
            if valid_neighbors:
                # 若存在有层级的邻居，就设为邻居中最大层级+1
                levels[node] = max(valid_neighbors) + 1
            else:
                # 完全孤立节点：默认层级0
                levels[node] = 0

    # 层级→坐标
    level_to_nodes = {}
    for node, level in levels.items():
        level_to_nodes.setdefault(level, []).append(node)

    pos = {}
    for level, nodes in sorted(level_to_nodes.items()):
        for i, node in enumerate(nodes):
            pos[node] = (i - len(nodes)/2, -level)

    # ------- 节点与边标签 -------
    if lable_weight and go_weight_table is not None:
        node_labels = {
            n: f"{n} w={weight_map[n]:.4f}\n{d.get('lbl','')}" if n in weight_map
            else f"{n} w=NaN\n{d.get('lbl','')}"
            for n, d in G.nodes(data=True)
        }
    else:
        node_labels = {n: f"{n}\n{d.get('lbl','')}" for n, d in G.nodes(data=True)}
    edge_labels = {}
    for u, v, data in G.edges(data=True):
        rel = data.get("relation", "")
        rel_lbl = rel
        if G_full is not None and rel in G_full.nodes:
            rel_lbl = G_full.nodes[rel].get("lbl", rel)
        edge_labels[(u, v)] = rel_lbl

    # ------- 随机样式生成 -------
    def random_style():
        colors = ["#4ECDC4", "#FFD93D", "#6A4C93", "#1B9AAA", "#E76F51",
                  "#2A9D8F", "#F4A261", "#118AB2"]
        dashes = ["solid", "dashed", "dotted", "dashdot",
                  (0, (5, 2)), (0, (3, 5, 1, 5))]
        return random.choice(colors), random.choice(dashes)

    unique_relations = sorted({d.get("relation", "") for _, _, d in G.edges(data=True)})
    relation_styles = {}
    for rel in unique_relations:
        if rel == "is_a":
            relation_styles[rel] = {"color": "black", "style": "solid"}
        else:
            c, s = random_style()
            relation_styles[rel] = {"color": c, "style": s}

    # ------- 绘图 -------
    fig, ax = plt.subplots(figsize=(max(8, len(G.nodes()) * 0.8),
                                    max(6, len(level_to_nodes) * 1.0)))

    # 节点方框（heatmap or 默认颜色）
    for n, (x, y) in pos.items():
        lbl = node_labels[n]
        if go_weight_table is not None:
            w = weight_map.get(n, min_w)
            color = cmap(norm(w))
        else:
            color = "#ADD8E6"  # 默认浅蓝色

        plt.text(
            x, y, lbl,
            ha='center', va='center',
            fontsize=font_size, fontweight='bold',
            wrap=True,
            bbox=dict(boxstyle="round,pad=0.3",
                      facecolor=color,
                      edgecolor="black",
                      linewidth=1.2)
        )

    # 绘制边
    for rel_type, style in relation_styles.items():
        rel_edges = [(u, v) for u, v, d in G.edges(data=True)
                     if d.get("relation") == rel_type]
        if rel_edges:
            nx.draw_networkx_edges(
                G, pos,
                edgelist=rel_edges,
                edge_color=style["color"],
                style=style["style"],
                arrows=True, arrowsize=12, width=1.3, ax=ax
            )

    # 边标签
    nx.draw_networkx_edge_labels(
        G, pos, edge_labels=edge_labels,
        font_size=font_size - 1, rotate=False, label_pos=0.5, ax=ax
    )

    # ------- 图例 -------
    legend_elems = []
    for rel, style in relation_styles.items():
        rel_lbl = rel
        if G_full is not None and rel in G_full.nodes:
            lbl = G_full.nodes[rel].get("lbl", rel)
            rel_lbl = f"{rel} ({lbl})"
        legend_elems.append(
            Line2D([0], [0], color=style["color"], linestyle=style["style"],
                   linewidth=2, label=rel_lbl)
        )

    ax.legend(handles=legend_elems, loc='upper right',
              bbox_to_anchor=(1.25, 1.0),
              frameon=True, title="Relations",
              fontsize=font_size - 1, title_fontsize=font_size)

    # ------- 可选 Heatmap 颜色条 -------
    if go_weight_table is not None:
        sm = cm.ScalarMappable(cmap=cmap, norm=norm)
        sm.set_array([])
        cbar = plt.colorbar(sm, ax=ax, fraction=0.046, pad=0.04)
        cbar.ax.set_title("Weight", fontsize=font_size)
        cbar.ax.tick_params(labelsize=font_size - 1)
        # 添加最小最大值标注
        cbar.ax.text(0.5, -0.1, f"min={min_w:.0f}", ha='center', va='top', fontsize=font_size-2, transform=cbar.ax.transAxes)
        cbar.ax.text(0.5, 1.05, f"max={max_w:.0f}", ha='center', va='bottom', fontsize=font_size-2, transform=cbar.ax.transAxes)

    plt.title(title, fontsize=font_size + 4)
    plt.axis("off")
    plt.tight_layout()
    plt.show()
    
def extract_go_only_subgraph(G: nx.DiGraph) -> nx.DiGraph:
    """
    从原始 GO-Plus 图中提取仅包含 GO term (GO:xxxxxxx) 的子图。

    参数:
        G : nx.DiGraph
            原始图（可能包含非 GO 节点，如 CHEBI, BFO, RO, etc.）

    返回:
        G_go : nx.DiGraph
            仅包含 GO term 节点的子图。
    """
    if not isinstance(G, nx.DiGraph):
        raise TypeError("Input must be a networkx.DiGraph.")

    # 仅保留节点ID匹配 GO:xxxxx 格式的
    go_nodes = [n for n in G.nodes if re.match(r"^GO:\d+$", str(n))]

    # 生成子图
    G_go = G.subgraph(go_nodes).copy()

    print(f"✅ Extracted GO-only subgraph: {len(G_go)} nodes, {G_go.number_of_edges()} edges.")
    return G_go

def search_go_by_keywords(
    G: nx.DiGraph,
    keywords: Union[str, List[str]],
    case_sensitive=False,
    regex=False,
    only_go=True,
):
    """
    🔍 在 GO 图中搜索名称 (lbl) 或定义 (definition) 同时包含多个关键词的节点（AND逻辑）
    
    参数:
        G (nx.DiGraph): GO-Plus 图
        keywords (str | list[str]): 单个或多个搜索关键词（必须全部匹配）
        case_sensitive (bool): 是否区分大小写
        regex (bool): 是否使用正则表达式
        only_go (bool): 是否仅保留 GO: 开头的节点
    
    返回:
        pandas.DataFrame: ['go_id', 'name', 'definition']
    """

    # --- 参数检查与预处理 ---
    if isinstance(keywords, str):
        keywords = [keywords]
    elif not isinstance(keywords, (list, tuple)):
        raise ValueError("keywords 必须为字符串或字符串列表。")

    if not keywords:
        raise ValueError("请至少提供一个关键词。")

    flags = 0 if case_sensitive else re.IGNORECASE
    compiled_patterns = [
        re.compile(k, flags) if regex else k.lower() for k in keywords
    ]

    results = []
    for node, attrs in G.nodes(data=True):
        if only_go and not str(node).startswith("GO:"):
            continue

        lbl = attrs.get("lbl", "") or ""
        definition = attrs.get("definition", "") or ""

        text_lbl = lbl if case_sensitive else lbl.lower()
        text_def = definition if case_sensitive else definition.lower()

        # ✅ 必须全部匹配
        matched_all = True
        for pattern in compiled_patterns:
            if regex:
                matched = bool(pattern.search(lbl)) or bool(pattern.search(definition))
            else:
                matched = (pattern in text_lbl) or (pattern in text_def)
            if not matched:
                matched_all = False
                break

        if matched_all:
            results.append({
                "go_id": node,
                "name": lbl,
                "definition": definition,
            })

    if not results:
        print(f"⚠️ 未找到同时包含 {keywords} 的 GO term。")
        return pd.DataFrame(columns=["go_id", "name", "definition"])

    df = pd.DataFrame(results).sort_values(by="go_id")
    print(f"✅ 找到 {len(df)} 条同时包含 {keywords} 的 GO term。")
    return df