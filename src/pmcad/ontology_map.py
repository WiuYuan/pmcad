# src/pmcad/ontology_map.py
from src.services.llm import LLM
from src.services.elasticsearch import search_via_curl
import os
import json
from typing import Optional, List, Dict, Any, Tuple, Union, Literal
from src.pmcad.prompts import get_prompt
import re
from src.pmcad.pmidstore import PMIDStore

class Ontology:
    def __init__(self, ontology_type, db_type, use_species : bool = False, key_in_map = None, search_func = None, filename = None, index_name = None, judge_method: Literal["strict", "relaxed", "forced"] = "strict"):
        if not isinstance(ontology_type, list):
            ontology_type = [ontology_type]
        self.ontology_type = ontology_type
        self.db_type = db_type
        self.use_species = use_species
        if key_in_map is None:
            key_in_map = f"{db_type}_map"
        self.key_in_map = key_in_map
        self.search_func = search_func
        self.filename = filename
        if index_name is None:
            index_name = f"{self.db_type}_index"
        self.index_name = index_name
        self.judge_method = judge_method
        
def search_ontology(
    config_path,
    query,
    index_name,
    search_type="dense+splade",
    k=10,
    vec_topn=200,
    w_dense=0.5,
    w_splade=0.5,
    dense_model=None,
    splade_model=None,
    verbose=True,
    extra_source_fields=None,   # ✅ NEW: 额外要从 ES _source 取回的字段列表
):
    if search_type == "dense+splade":
        assert dense_model is not None
        assert splade_model is not None

        # ============================================================
        # 0. Build _source fields
        # ============================================================
        base_fields = ["id", "label", "text_all", "splade"]
        if extra_source_fields is None:
            source_fields = base_fields
        else:
            # 去重且保持 base_fields 优先顺序
            seen = set()
            source_fields = []
            for x in (base_fields + list(extra_source_fields)):
                if x and x not in seen:
                    seen.add(x)
                    source_fields.append(x)

        # ============================================================
        # 1. Dense Recall (KNN)
        # ============================================================
        qvec_dense = dense_model.encode(query, normalize_embeddings=True).tolist()

        knn_body = {
            "size": vec_topn,
            "knn": {
                "field": "vector",
                "query_vector": qvec_dense,
                "k": vec_topn,
                "num_candidates": max(vec_topn * 3, 1000),
            },
            "_source": source_fields,   # ✅ 使用动态字段
        }

        hits_knn = search_via_curl(config_path, index_name, knn_body)
        if not hits_knn:
            return []

        # ============================================================
        # 2. Build SPLADE query vector
        # ============================================================
        sparse_vec = splade_model.encode([query])[0].coalesce()
        idx = sparse_vec.indices()[0].tolist()
        val = sparse_vec.values().tolist()
        tokens = splade_model.tokenizer.convert_ids_to_tokens(idx)
        q_splade = {tok: float(v) for tok, v in zip(tokens, val) if float(v) > 0}

        # ============================================================
        # 3. Build candidate list
        # ============================================================
        items = []
        for h in hits_knn:
            src = h["_source"]

            extra = {}
            if extra_source_fields:
                for f in extra_source_fields:
                    if f in src:
                        extra[f] = src.get(f)

            items.append(
                {
                    "id": src["id"],
                    "label": src["label"],
                    "text_all": src.get("text_all", ""),
                    "dense_score": h["_score"],
                    "splade_score": 0.0,
                    "doc_splade": src.get("splade", {}),
                    "final_score": 0.0,
                    "extra": extra,   # ✅ 把额外字段带着
                }
            )

        # ============================================================
        # 4. SPLADE dot-product reranking
        # ============================================================
        for it in items:
            score = 0.0
            doc_spl = it["doc_splade"]
            for tok, wq in q_splade.items():
                wd = doc_spl.get(tok, 0.0)
                if wd > 0:
                    score += wq * wd
            it["splade_score"] = round(float(score), 4)
            it["dense_score"] = round(float(it.get("dense_score", 0.0)), 4)

        # ============================================================
        # 5. Normalize + fuse
        # ============================================================
        max_dense = max(it["dense_score"] for it in items) or 1e-9
        max_splade = max(it["splade_score"] for it in items) or 1e-9

        for it in items:
            it["final_score"] = w_dense * (it["dense_score"] / max_dense) + w_splade * (
                it["splade_score"] / max_splade
            )
            it["final_score"] = round(float(it.get("final_score", 0.0)), 4)

        # ============================================================
        # 6. Assign ranks (NO scores in final output)
        # ============================================================
        dense_sorted = sorted(items, key=lambda x: x["dense_score"], reverse=True)
        dense_rank_map = {it["id"]: i + 1 for i, it in enumerate(dense_sorted)}

        splade_sorted = sorted(items, key=lambda x: x["splade_score"], reverse=True)
        splade_rank_map = {it["id"]: i + 1 for i, it in enumerate(splade_sorted)}

        final_sorted = sorted(items, key=lambda x: x["final_score"], reverse=True)[:k]

        final_items = []
        for i, it in enumerate(final_sorted):
            out = {
                "id": it["id"],
                "name": it["label"],
                "description": it.get("text_all", ""),
                "dense_rank": dense_rank_map[it["id"]],
                "splade_rank": splade_rank_map[it["id"]],
                "rank": i + 1,
            }
            # ✅ 把 extra 字段展开回传（或你也可以保留在 out["extra"]）
            if it.get("extra"):
                out.update(it["extra"])
            final_items.append(out)

        # ============================================================
        # 7. Verbose print (rank-only)
        # ============================================================
        if verbose:
            print("=== HYBRID SEARCH (Rank-only) ===")
            for it in final_items:
                print(
                    f"{it['id']:12s} | "
                    f"{it['name']:<40s} | "
                    f"dense_rank={it['dense_rank']:>3d} | "
                    f"splade_rank={it['splade_rank']:>3d} | "
                    f"final_rank={it['rank']:>3d}"
                )

        return final_items

    raise ValueError(f"unknown search_type: {search_type}")

# =====================================
# Extract species from relation (ALL META INCLUDED)
# =====================================
CELL_SPECIES_TAG = "__CELL__::"

def load_best_cell_line_species(
    cvcl_ot: "Ontology",
    *,
    store: PMIDStore,
    pmid: Union[int, str],
) -> dict:
    """
    仅支持 DB 模式（已彻底删除 folder 模式）：

    读取 store.get(pmid, cvcl_ot.filename)，构建：
      best_cell_line_species[cell_name] = scientific_species
    """
    if cvcl_ot is None or not cvcl_ot.filename:
        return {}

    if store is None or pmid is None:
        raise ValueError("load_best_cell_line_species requires store + pmid (folder mode removed)")

    data = store.get(int(pmid), cvcl_ot.filename)
    if not isinstance(data, dict):
        return {}

    m = {}
    for it in (data.get(cvcl_ot.key_in_map, []) or []):
        if not isinstance(it, dict):
            continue
        cell = (it.get("name") or "").strip()
        best = it.get("llm_best_match") or {}
        sp = (best.get("species") or "").strip()
        if cell and sp:
            m[cell] = sp
    return m


def resolve_species(raw_sp: str, best_species: dict, best_cell_line_species: dict) -> str:
    """
    raw_sp 可能是：
      - LLM 抽到的物种 mention（例如 'rat'）
      - 或者 CELL_SPECIES_TAG + cell_name（例如 '__CELL__::HeLa'）

    返回：scientific name（例如 'Rattus norvegicus' / 'Homo sapiens'）
    """
    if not raw_sp:
        return ""

    raw_sp = str(raw_sp).strip()

    # 1) 来自 cell line 的“伪 species”
    if raw_sp.startswith(CELL_SPECIES_TAG):
        cell = raw_sp[len(CELL_SPECIES_TAG):].strip()
        return best_cell_line_species.get(cell, "")

    # 2) 正常物种 mapping
    return best_species.get(raw_sp, "")

def extract_species_from_relation(rel, cvcl_ot: Optional["Ontology"] = None, best_cell_line_species: Optional[dict] = None):
    """
    Species resolution priority:
    1) entity.meta
    2) relation.contexts (+ contexts.meta)
    3) relation.components + relation.targets (+ their meta)
    4) 如果以上都没有 species：用 relation 内的 cell line/type（cvcl_ot.ontology_type）去 best_cell_line_species 推断
       返回 "__CELL__::<cell_name>" 作为 species proxy
    """

    best_cell_line_species = best_cell_line_species or {}

    # ---- collect relation-level species (order matters) ----
    rel_species = []

    # ---------- 1. contexts (+ meta) ----------
    for c in rel.get("contexts", []):
        if c.get("type") == "species" and c.get("name"):
            rel_species.append(c["name"])
        for m in c.get("meta", []):
            if m.get("type") == "species" and m.get("name"):
                rel_species.append(m["name"])

    # ---------- 2. components + targets (+ meta) ----------
    for field in ("components", "targets"):
        for ent in rel.get(field, []):
            if ent.get("type") == "species" and ent.get("name"):
                rel_species.append(ent["name"])
            for m in ent.get("meta", []):
                if m.get("type") == "species" and m.get("name"):
                    rel_species.append(m["name"])

    # ---- deduplicate but keep order ----
    seen = set()
    rel_species = [x for x in rel_species if not (x in seen or seen.add(x))]

    # ---------- 如果 relation-level 完全没 species：用 cell line/type 兜底 ----------
    if (not rel_species) and cvcl_ot is not None and best_cell_line_species:
        cvcl_types = set(cvcl_ot.ontology_type if isinstance(cvcl_ot.ontology_type, list) else [cvcl_ot.ontology_type])

        def scan_entities():
            for field in ("components", "targets", "contexts"):
                for ent in rel.get(field, []):
                    yield ent
                    for mm in ent.get("meta", []):
                        yield mm

        for ent in scan_entities():
            if ent.get("type") in cvcl_types:
                cell = (ent.get("name") or "").strip()
                if cell and (cell in best_cell_line_species):
                    rel_species = [CELL_SPECIES_TAG + cell]
                    break

    # ---------- entity-level resolver ----------
    def _inner(ent):
        # 1️⃣ entity.meta has highest priority
        for m in ent.get("meta", []):
            if m.get("type") == "species" and m.get("name"):
                return m["name"]

        # 2️⃣ fallback to relation-level species OR cell-derived proxy
        if rel_species:
            return rel_species[0]

        return ""

    return _inner

def collect_type(ent, ontology_type, needed_keys, get_species = None):
    if ent.get("type") in ontology_type:
        nm = ent.get("name")
        desc = ent.get("description", "")
        if nm:
            if get_species is None:
                needed_keys.add((nm, desc))
            else:
                sp = get_species(ent)
                needed_keys.add((nm, desc, sp))

    for m in ent.get("meta", []):
        if m.get("type") in ontology_type:
            nm = m.get("name")
            desc = m.get("description", "")
            if nm:
                if get_species is None:
                    needed_keys.add((nm, desc))
                else:
                    sp = get_species(m)
                    needed_keys.add((nm, desc, sp))


def dedup_needed_by_name_longest_desc(needed: set, use_species: bool):
    """
    needed:
      - use_species=False: {(name, desc), ...}
      - use_species=True : {(name, desc, sp), ...}

    return:
      - use_species=False: [(name, best_desc), ...]
      - use_species=True : [(name, best_desc, one_sp), ...]
        说明：如果同名但 species 不同，这里会选“desc 最长”的那条，并把它携带的 sp 一起保留。
        （你现在的“唯一指标只用 name”，所以 species 也会被合并掉）
    """
    best = {}  # name -> tuple(...)
    for item in needed:
        if use_species:
            nm, desc, sp = item
        else:
            nm, desc = item
            sp = None

        nm = (nm or "").strip()
        desc = (desc or "").strip()
        if not nm:
            continue

        prev = best.get(nm)
        if prev is None:
            best[nm] = (nm, desc, sp) if use_species else (nm, desc)
            continue

        prev_desc = prev[1] if use_species else prev[1]
        # 选择 description 更长的；若等长，保留原来的
        if len(desc) > len(prev_desc or ""):
            best[nm] = (nm, desc, sp) if use_species else (nm, desc)

    # 返回 list，保持 deterministic（按 name 排序）
    return [best[k] for k in sorted(best.keys())]

def process_one_folder_get_db_id(
    *,
    input_name: str = "",
    ot: Ontology = None,
    species_ot: Ontology = None,
    cvcl_ot: Ontology = None,
    pmid: Union[int, str],
    store: PMIDStore,
    **kwargs,
):
    """
    仅支持 DB 模式：
      - data = store.get(pmid, input_name)
      - store.put(pmid, ot.filename, out)
    """
    if ot is None:
        raise ValueError("ot is None")
    if store is None:
        raise ValueError("store is required (folder mode removed)")

    pmid_int = int(pmid)
    pmid_str = str(pmid_int)

    # -------- load input --------
    data = store.get(pmid_int, input_name)
    if not isinstance(data, dict):
        return None, [
            {"type": "error", "msg": f"pmid:{pmid_str} (load error)"},
            {"type": "metric", "correct": 0, "total": 0},
        ]

    abstract = data.get("abstract", "")
    relations = data.get("relations", [])

    if ot.use_species and species_ot is None:
        raise ValueError("ot.use_species=True but species_ot is None")

    # ------------- load taxon_map --------------
    if ot.use_species:
        sp_data = store.get(pmid_int, species_ot.filename)
        if not isinstance(sp_data, dict):
            return None, [{"type": "error", "msg": f"{pmid_str} (species load error)"}]

        best_species = {}
        for item in sp_data.get(species_ot.key_in_map, []):
            nm = item.get("name")
            best = item.get("llm_best_match")
            if nm and best:
                best_species[nm] = best.get("name")

        best_cell_line_species = load_best_cell_line_species(
            cvcl_ot,
            store=store,
            pmid=pmid_int,
        )

        # collect document-level species
        doc_species = []
        for block in relations:
            for rel in block.get("rel_from_this_sent", []):
                get_sp = extract_species_from_relation(
                    rel,
                    cvcl_ot=cvcl_ot,
                    best_cell_line_species=best_cell_line_species,
                )
                sp = get_sp({})  # relation-level fallback
                if sp:
                    doc_species.append(sp)
        seen = set()
        doc_species = [x for x in doc_species if not (x in seen or seen.add(x))]
        doc_level_species = doc_species[0] if doc_species else ""
    else:
        best_species = {}
        best_cell_line_species = {}
        doc_level_species = ""

    # ------------------------------------------------
    # collect ontology_type entities
    # ------------------------------------------------
    needed = set()

    def _is_relation_ot(ontology_type):
        if isinstance(ontology_type, str):
            return ontology_type == "relation"
        if isinstance(ontology_type, (list, tuple, set)):
            return "relation" in ontology_type
        return False

    is_relation_ot = _is_relation_ot(ot.ontology_type)

    for block in relations:
        for rel in block.get("rel_from_this_sent", []):
            if ot.use_species:
                get_sp = extract_species_from_relation(
                    rel, cvcl_ot=cvcl_ot, best_cell_line_species=best_cell_line_species
                )

            if is_relation_ot:
                rel_obj = rel.get("relation") or {}
                rname = rel_obj.get("name", "")
                rdesc = rel_obj.get("description", "")
                if rname:
                    ent_relation = {
                        "name": rname,
                        "type": "relation",
                        "description": rdesc,
                        "meta": [],
                    }
                    # ot.ontology_type 在 Ontology.__init__ 已保证为 list
                    if ot.use_species:
                        collect_type(ent_relation, ot.ontology_type, needed, get_species=get_sp)
                    else:
                        collect_type(ent_relation, ot.ontology_type, needed)
                continue

            for field in ("components", "targets", "contexts"):
                for ent in rel.get(field, []):
                    if ot.use_species:
                        collect_type(ent, ot.ontology_type, needed, get_species=get_sp)
                    else:
                        collect_type(ent, ot.ontology_type, needed)

    needed = dedup_needed_by_name_longest_desc(needed, use_species=ot.use_species)

    # ------------------------------------------------
    # unify species with fallback
    # ------------------------------------------------
    filtered_items = []
    for item in needed:
        if ot.use_species:
            nm, desc, sp = item
        else:
            nm, desc = item
            sp = ""

        species_final = ""
        if ot.use_species:
            if sp:
                species_final = resolve_species(sp, best_species, best_cell_line_species)
            elif doc_level_species:
                species_final = resolve_species(doc_level_species, best_species, best_cell_line_species)

        if ot.use_species:
            filtered_items.append((nm, desc, species_final))
        else:
            filtered_items.append((nm, desc))

    if not filtered_items:
        out = {"pmid": pmid_str, "abstract": abstract, ot.key_in_map: []}
        store.put(pmid_int, ot.filename, out)
        return None, [{"type": "error", "msg": f"{pmid_str} (no {str(ot.ontology_type)})"}]

    # ------------------------------------------------
    # db search
    # ------------------------------------------------
    final_map = []
    num_hit = 0
    num_total = 0

    for item in filtered_items:
        if ot.use_species:
            name, desc, species = item
        else:
            name, desc = item
            species = ""

        query = name
        if desc:
            query += f", {desc}"
        if ot.use_species and species:
            query += f", {species}"

        query = re.sub(r"\([^)]*\)", "", str(query))
        query = re.sub(r"\[[^\]]*\]", "", str(query))

        if (not ot.use_species) or (ot.use_species and species):
            try:
                hits = ot.search_func(query)
            except Exception:
                hits = []
        else:
            hits = []

        if hits:
            num_hit += 1
        num_total += 1

        e = {"name": name, "description": desc, "hits": hits}
        if ot.use_species:
            e["species"] = species
        final_map.append(e)

    # ------------------------------------------------
    # merge existing (避免覆盖已有 llm_best_match，例如 convert_failed 写入的)
    # ------------------------------------------------
    def _key(e: dict) -> tuple:
        if ot.use_species:
            return (
                (e.get("name") or "").strip(),
                (e.get("description") or "").strip(),
                (e.get("species") or "").strip(),
            )
        return ((e.get("name") or "").strip(), (e.get("description") or "").strip())

    existing = store.get(pmid_int, ot.filename)

    merged_list = []
    seen = set()

    if isinstance(existing, dict):
        for old in (existing.get(ot.key_in_map, []) or []):
            if not isinstance(old, dict):
                continue
            k = _key(old)
            if k in seen:
                continue
            seen.add(k)
            merged_list.append(old)

    for new in final_map:
        k = _key(new)
        if k in seen:
            # 如果已有条目但 hits 为空，就补一下；保留 llm_best_match 等字段
            for old in merged_list:
                if _key(old) == k:
                    if (not old.get("hits")) and new.get("hits"):
                        old["hits"] = new["hits"]
                    break
            continue
        seen.add(k)
        merged_list.append(new)

    out = {"pmid": pmid_str, "abstract": abstract, ot.key_in_map: merged_list}
    store.put(pmid_int, ot.filename, out)

    return None, [
        {"type": "status", "name": "success", "description": f"{pmid_str}"},
        {"type": "metric", "correct": num_hit, "total": num_total},
    ]


def build_selection_prompt(
    name: str,
    hits: list,
    abstract: str,
    description: str = "",
    species: str = "",
    judge_method="strict",
    relation_example: str = "",
) -> str:
    """
    构建让 LLM 选择最正确 DB id 的 prompt。

    ✅ NEW:
    - relation_example：额外提供一条“包含该实体的 relation”（或其简化 JSON），辅助 disambiguation
    """
    # -----------------------------
    # Limit prompt size (approx by chars)
    # -----------------------------
    max_hits = 30               # 也可以改小，比如 10/15
    max_desc_chars = 400        # 每条 description 最多保留多少字符
    max_hits_text_chars = 8000  # hits_text 总预算（越小越不容易超上下文）

    hits_lines = []
    budget = max_hits_text_chars

    for h in (hits or [])[:max_hits]:
        hid = str(h.get("id", "") or "").strip()
        desc = str(h.get("description", "") or "")

        # 压缩空白，减少无意义 token
        desc = re.sub(r"\s+", " ", desc).strip()

        # 截断每条描述
        if len(desc) > max_desc_chars:
            desc = desc[:max_desc_chars].rstrip() + "..."

        line = f"- {hid}: {desc}".strip()

        # 总预算控制：超了就不再追加更多 hits
        if budget - (len(line) + 1) < 0:
            break

        hits_lines.append(line)
        budget -= (len(line) + 1)

    hits_text = "\n".join(hits_lines)

    prompt = get_prompt(f"select_db_id/{judge_method}.txt")

    query = f"Name: {name}"
    if description:
        query += f"\nDescription: {description}"
    if species:
        query += f"\nSpecies: {species}"

    abstract2 = abstract or ""
    if relation_example:
        abstract2 = (abstract2 + "\n\n[Relation example containing this entity]\n" + relation_example).strip()

    return prompt.format(query=query, abstract=abstract2, hits_text=hits_text)


def normalize_db(s: str):
    return s.strip().upper().replace('"', "").replace("'", "")


def match_llm_output_to_db_id(llm_output: str, hits: list):
    """
    匹配 LLM 返回的 accession 到 hits。
    """
    out = normalize_db(llm_output)

    if out == "NONE":
        return None

    for h in hits:
        if normalize_db(h["id"]) in out:
            return h

    return None


def process_one_folder_judge_db_id(
    *,
    ot: Ontology,
    llm: LLM,
    input_name: str = "",
    pmid: Union[int, str],
    store: PMIDStore,
    **kwargs,
):
    """
    仅支持 DB 模式：
      - data = store.get(pmid, ot.filename)
      - store.put(pmid, ot.filename, data)

    关键修复：
    - 如果 entry 已有 llm_best_match（例如 convert_failed 已经写入），则跳过，不要覆盖。
    """
    if ot is None:
        raise ValueError("ot is None")
    if llm is None:
        raise ValueError("llm is None")
    if store is None:
        raise ValueError("store is required (folder mode removed)")

    pmid_int = int(pmid)
    pmid_str = str(pmid_int)

    # -------- load mapping json --------
    data = store.get(pmid_int, ot.filename)
    if not isinstance(data, dict):
        return None, [{"type": "error", "msg": f"skip pmid {pmid_str} (no file)"}]

    abstract = data.get("abstract", "")
    db_list = data.get(ot.key_in_map, []) or []
    if not isinstance(db_list, list):
        db_list = []

    # ✅ 为每个 entry 提供一条“包含该实体的 relation”作为额外上下文
    ds = None
    if input_name:
        ds = store.get(pmid_int, input_name)

    def _relation_example_for(name: str) -> str:
        """
        从 ds.relations 中找一条包含该实体 name 的 relation，返回简化 JSON 字符串。
       ⧖⧖⧖⧖ ✅ 限制 token：这里会递归删除所有字段里的 "description"，避免把大段文本送进 LLM。
        找不到则返回空串。
        """
        if not isinstance(ds, dict):
            return ""
        rel_blocks = ds.get("relations") or []
        if not isinstance(rel_blocks, list):
            return ""

        ot_types = ot.ontology_type or []
        if not isinstance(ot_types, list):
            ot_types = [ot_types]
        ot_types = set(ot_types)

        def _strip_description(obj):
            if isinstance(obj, dict):
                out = {}
                for k, v in obj.items():
                    if k == "description":
                        continue
                    out[k] = _strip_description(v)
                return out
            if isinstance(obj, list):
                return [_strip_description(x) for x in obj]
            return obj

        def _ent_name_matches(ent: dict) -> bool:
            return (ent.get("name") or "").strip() == (name or "").strip()

        def _scan_entity(ent: dict) -> bool:
            if not isinstance(ent, dict):
                return False
            # relation ontology 特判：看 rel["relation"]["name"]
            if "relation" in ot_types:
                return False
            et = ent.get("type")
            if et in ot_types and _ent_name_matches(ent):
                return True
            for m in (ent.get("meta") or []):
                if _scan_entity(m):
                    return True
            return False

        for blk in rel_blocks:
            for rel in (blk.get("rel_from_this_sent") or []):
                # relation ontology 特判
                if "relation" in ot_types:
                    r = rel.get("relation") or {}
                    if (r.get("name") or "").strip() == (name or "").strip():
                        slim = {
                            "relation": r,
                            "components": rel.get("components", []),
                            "targets": rel.get("targets", []),
                            "contexts": rel.get("contexts", []),
                        }
                        return json.dumps(_strip_description(slim), ensure_ascii=False)

                for field in ("components", "targets", "contexts"):
                    for ent in (rel.get(field) or []):
                        if _scan_entity(ent):
                            slim = {
                                "relation": rel.get("relation", {}),
                                "components": rel.get("components", []),
                                "targets": rel.get("targets", []),
                                "contexts": rel.get("contexts", []),
                            }
                            return json.dumps(_strip_description(slim), ensure_ascii=False)

        return ""

    attempted = 0
    correct = 0
    error_count = 0

    for entry in db_list:
        if not isinstance(entry, dict):
            continue

        # ✅ 已有 best_match 则不再 judge（避免覆盖 convert_failed 的结果）
        if entry.get("llm_best_match") is not None:
            continue

        name = entry.get("name", "")
        description = entry.get("description", "")
        species = entry.get("species", "")
        hits = entry.get("hits", [])

        if not hits:
            entry["llm_best_match"] = None
            continue

        prompt = build_selection_prompt(
            name=name,
            description=description,
            species=species,
            hits=hits,
            abstract=abstract,
            judge_method=ot.judge_method,
            relation_example=_relation_example_for(name),
        )

        attempted += 1
        try:
            llm_output = llm.query(prompt)
        except Exception as e:
            llm_output = f"ERROR: {e}"
            entry["llm_raw_output"] = llm_output
            entry["llm_best_match"] = None
            error_count += 1
            continue

        best_hit = match_llm_output_to_db_id(llm_output, hits)
        entry["llm_raw_output"] = llm_output
        entry["llm_best_match"] = best_hit
        if best_hit is not None:
            correct += 1

    data[ot.key_in_map] = db_list
    store.put(pmid_int, ot.filename, data)

    return None, [
        {"type": "status", "name": "success", "description": f"ok pmid {pmid_str}"},
        {"type": "metric", "name": "judge", "correct": correct, "total": attempted},
        {"type": "metric", "name": "llm_error", "correct": error_count, "total": attempted},
    ]

def process_one_folder_apply_llm_best(
    *,
    pmid: Union[int, str],
    store: PMIDStore,
    input_name: str,
    output_name: str,
    ot_list: List["Ontology"],
    species_ot: Optional["Ontology"] = None,
    cvcl_ot: Optional["Ontology"] = None,
    **kwargs,
):
    """
    仅支持 DB 模式（folder 模式删除）：
      - 读：store.get(pmid, input_name) / store.get(pmid, ot.filename)
      - 写：store.put(pmid, output_name, data)
    """
    if store is None:
        raise ValueError("store is required (folder mode removed)")

    pmid_int = int(pmid)
    pmid_str = str(pmid_int)

    # -----------------------------
    # 0) load relation file
    # -----------------------------
    data = store.get(pmid_int, input_name)
    if not isinstance(data, dict):
        return None, [{"type": "error", "msg": f"{pmid_str} missing input: {input_name}"}]

    relations = data.get("relations", [])
    if not relations:
        store.put(pmid_int, output_name, data)
        return None, [{"type": "status", "name": "no_relations"}]

    # -----------------------------
    # 1) load species maps
    # -----------------------------
    best_species_name_map: Dict[str, str] = {}
    best_species_hit_map: Dict[str, Dict[str, Any]] = {}

    best_cell_line_species: Dict[str, str] = {}

    if species_ot and species_ot.filename:
        sp_data = store.get(pmid_int, species_ot.filename)
        if isinstance(sp_data, dict):
            for it in sp_data.get(species_ot.key_in_map, []):
                raw_nm = (it.get("name") or "").strip()
                best = it.get("llm_best_match")
                if raw_nm and isinstance(best, dict) and best.get("name"):
                    best_species_name_map[raw_nm] = best.get("name")
                    best_species_hit_map[raw_nm] = best

    # ✅ 潜在问题修复：加载 cell_line -> species，用于 extract_species_from_relation 的 cell 兜底
    if cvcl_ot and cvcl_ot.filename:
        best_cell_line_species = load_best_cell_line_species(
            cvcl_ot,
            store=store,
            pmid=pmid_int,
        )

    # -----------------------------
    # 2) compute document-level fallback species (raw)
    # -----------------------------
    def compute_doc_level_species_raw() -> str:
        doc_species = []
        for block in relations:
            for rel in block.get("rel_from_this_sent", []):
                get_sp = extract_species_from_relation(
                    rel,
                    cvcl_ot=cvcl_ot,
                    best_cell_line_species=best_cell_line_species,
                )
                sp = (get_sp({}) or "").strip()
                if sp:
                    doc_species.append(sp)

        seen = set()
        doc_species2 = [x for x in doc_species if not (x in seen or seen.add(x))]
        return doc_species2[0] if doc_species2 else ""

    doc_level_species_raw = compute_doc_level_species_raw()

    # -----------------------------
    # 3) build ontology lookup tables from each ot.filename (judge输出)
    # -----------------------------
    ot_lookup: Dict["Ontology", Dict[str, Any]] = {}

    for ot in ot_list:
        if not ot.filename:
            continue

        mdata = store.get(pmid_int, ot.filename)
        if not isinstance(mdata, dict):
            continue

        lut_exact: Dict[Tuple[str, ...], Dict[str, Any]] = {}
        lut_by_name: Dict[str, List[Tuple[str, Dict[str, Any]]]] = {}

        for it in mdata.get(ot.key_in_map, []):
            name = (it.get("name") or "").strip()
            sp = (it.get("species") or "").strip() if ot.use_species else ""
            best = it.get("llm_best_match")

            if not name or not isinstance(best, dict):
                continue

            if ot.use_species:
                key = (name, sp)
                lut_exact[key] = best
                lut_by_name.setdefault(name, []).append((sp, best))
            else:
                key = (name,)
                lut_exact[key] = best

        ot_lookup[ot] = {"exact": lut_exact, "by_name": lut_by_name}

    # -----------------------------
    # 4) helpers
    # -----------------------------
    def resolve_species_final_for_entity(ent: Dict[str, Any], get_sp_func) -> str:
        sp_raw = (get_sp_func(ent) or "").strip()
        if not sp_raw and doc_level_species_raw:
            sp_raw = doc_level_species_raw
        if not sp_raw:
            return ""
        return (resolve_species(sp_raw, best_species_name_map, best_cell_line_species) or "").strip()

    def lookup_best_for_ot(ot: "Ontology", name: str, species_final: str) -> Optional[Dict[str, Any]]:
        info = ot_lookup.get(ot)
        if not info:
            return None

        lut_exact = info["exact"]
        lut_by_name = info.get("by_name", {})

        if not ot.use_species:
            return lut_exact.get((name,))

        best = lut_exact.get((name, species_final))
        if best is not None:
            return best

        best = lut_exact.get((name, ""))
        if best is not None:
            return best

        cands = lut_by_name.get(name, [])
        uniq = {}
        for sp, b in cands:
            if (sp or "") not in uniq:
                uniq[(sp or "")] = b
        if len(uniq) == 1:
            return next(iter(uniq.values()))

        return None

    # -----------------------------
    # 5) core mapper
    # -----------------------------
    mapped_success = 0
    attempted = 0

    def map_one_meta_entity(m: Dict[str, Any], get_sp_func) -> Optional[Dict[str, Any]]:
        nonlocal attempted, mapped_success
        if not isinstance(m, dict):
            return None

        mtype = m.get("type")
        mname = (m.get("name") or "").strip()
        if not mtype or not mname:
            return None

        mm_list = m.get("meta", [])
        if not isinstance(mm_list, list):
            mm_list = []
        new_mm = []
        for mm in mm_list:
            if not isinstance(mm, dict):
                continue
            mm2 = map_one_meta_entity(mm, get_sp_func)
            if mm2 is not None:
                new_mm.append(mm2)

        m2 = dict(m)
        m2["meta"] = new_mm

        for ot in ot_list:
            if mtype not in (ot.ontology_type or []):
                continue
            attempted += 1

            species_final = ""
            if ot.use_species:
                species_final = resolve_species_final_for_entity(m, get_sp_func)

            best = lookup_best_for_ot(ot, mname, species_final)
            if best is None:
                # ✅ relation ontology 特例：relation 没映射成功也要保留（不删除该实体）
                if mtype == "relation":
                    return m2
                return None
            mapped_success += 1

            m2["id"] = best.get("id", m2.get("id"))
            m2["name"] = best.get("name") or best.get("id") or m2.get("name")
            if "description" in best:
                m2["description"] = best.get("description", m2.get("description", ""))

            if ot.use_species and species_final:
                meta_wo_species = [mm for mm in m2.get("meta", []) if mm.get("type") != "species"]

                sp_meta = {"name": species_final, "type": "species", "description": ""}
                sp_meta_mapped = map_one_meta_entity(sp_meta, get_sp_func)
                if sp_meta_mapped is None:
                    sp_meta_mapped = {"name": species_final, "type": "species", "description": ""}
                sp_meta_mapped.pop("description", None)

                meta_wo_species.append(sp_meta_mapped)
                m2["meta"] = meta_wo_species

            return m2

        return m2

    def map_one_entity(ent: Dict[str, Any], get_sp_func) -> Optional[Dict[str, Any]]:
        nonlocal attempted, mapped_success
        if not isinstance(ent, dict):
            return None

        etype = ent.get("type")
        name = (ent.get("name") or "").strip()
        if not name or not etype:
            return None

        meta_list = ent.get("meta", [])
        if not isinstance(meta_list, list):
            meta_list = []
        new_meta = []
        for m in meta_list:
            if not isinstance(m, dict):
                continue
            mapped_m = map_one_meta_entity(m, get_sp_func)
            if mapped_m is not None:
                new_meta.append(mapped_m)

        ent2 = dict(ent)
        ent2["meta"] = new_meta

        for ot in ot_list:
            if etype not in (ot.ontology_type or []):
                continue

            attempted += 1
            species_final = ""
            if ot.use_species:
                species_final = resolve_species_final_for_entity(ent, get_sp_func)

            best = lookup_best_for_ot(ot, name, species_final)
            if best is None:
                # ✅ relation ontology 特例：relation 没映射成功也要保留（不删除该实体）
                if etype == "relation":
                    return ent2
                return None
            mapped_success += 1

            ent2["id"] = best.get("id", ent2.get("id"))
            ent2["name"] = best.get("name") or best.get("id") or ent2.get("name")
            if "description" in best:
                ent2["description"] = best.get("description", ent2.get("description", ""))

            if ot.use_species and species_final:
                meta_wo_species = [mm for mm in ent2.get("meta", []) if mm.get("type") != "species"]

                sp_meta = {"name": species_final, "type": "species", "description": ""}
                sp_meta_mapped = map_one_meta_entity(sp_meta, get_sp_func)
                if sp_meta_mapped is None:
                    sp_meta_mapped = {"name": species_final, "type": "species", "description": ""}
                sp_meta_mapped.pop("description", None)

                meta_wo_species.append(sp_meta_mapped)
                ent2["meta"] = meta_wo_species

            return ent2

        return ent2

    # -----------------------------
    # 6) apply mapping to all relations/entities
    # -----------------------------
    for block in relations:
        for rel in block.get("rel_from_this_sent", []):
            get_sp = extract_species_from_relation(
                rel,
                cvcl_ot=cvcl_ot,
                best_cell_line_species=best_cell_line_species,
            )

            # ✅ 单独处理 relation ontology：映射失败也保留原 relation
            rel_obj = rel.get("relation")
            if isinstance(rel_obj, dict) and (rel_obj.get("name") or "").strip():
                rel_ent = {
                    "type": "relation",
                    "name": (rel_obj.get("name") or "").strip(),
                    "description": rel_obj.get("description", "") or "",
                    "meta": [],
                }
                rel_ent2 = map_one_entity(rel_ent, get_sp)
                if isinstance(rel_ent2, dict):
                    rel_obj2 = dict(rel_obj)
                    if rel_ent2.get("id"):
                        rel_obj2["id"] = rel_ent2.get("id")
                    if rel_ent2.get("name"):
                        rel_obj2["name"] = rel_ent2.get("name")
                    if "description" in rel_ent2:
                        rel_obj2["description"] = rel_ent2.get("description", rel_obj2.get("description", ""))
                    rel["relation"] = rel_obj2

            for field in ("components", "targets", "contexts"):
                ents = rel.get(field, [])
                if not isinstance(ents, list):
                    rel[field] = []
                    continue

                new_ents = []
                for ent in ents:
                    ent2 = map_one_entity(ent, get_sp)
                    if ent2 is None:
                        continue
                    new_ents.append(ent2)

                rel[field] = new_ents

    # -----------------------------
    # 7) write output
    # -----------------------------
    data.setdefault("_apply_llm_best_report", {})
    data["_apply_llm_best_report"].update(
        {
            "pmid": pmid_str,
            "doc_level_species_raw": doc_level_species_raw,
            "mode": "apply_llm_best_with_meta_and_species_alignment",
        }
    )

    store.put(pmid_int, output_name, data)

    return None, [
        {"type": "status", "name": "success", "description": f"{pmid_str} mapped"},
        {"type": "metric", "name": "success", "correct": mapped_success, "total": attempted},
    ]
