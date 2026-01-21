from src.services.llm import LLM
from src.services.elasticsearch import search_via_curl
import os
import json
from typing import Optional, List, Dict, Any, Tuple, Union, Literal
from src.pmcad.prompts import get_prompt
import re

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

def load_best_cell_line_species(folder: str, cvcl_ot: "Ontology") -> dict:
    """
    从 folder/cvcl_ot.filename 读取：
      cvcl_ot.key_in_map: [{name, ..., llm_best_match:{..., species: <scientific>}}, ...]
    构建映射：
      best_cell_line_species[cell_name] = scientific_species
    """
    if cvcl_ot is None or not cvcl_ot.filename:
        return {}

    path = os.path.join(folder, cvcl_ot.filename)
    if not os.path.exists(path):
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {}

    m = {}
    for it in data.get(cvcl_ot.key_in_map, []):
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
    folder: str,
    input_name: str,
    ot: Ontology,
    species_ot: Ontology = None,
    cvcl_ot: Ontology = None,
    **kwargs,
):
    """
    输入 JSON:
      - pmid
      - abstract
      - relations (sentence → rel_from_this_sent)

    输出 JSON（仅）:
      - pmid
      - abstract
      - key_map
    """
    pmid = os.path.basename(folder)
    in_path = os.path.join(folder, input_name)
    out_path = os.path.join(folder, ot.filename)

    # ---------------------------
    # 加载输入 JSON
    # ---------------------------
    try:
        with open(in_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return None, [
            {"type": "error", "msg": f"pmid:{pmid} (load error)"},
            {"type": "metric", "correct": 0, "total": 0},
        ]

    abstract = data.get("abstract", "")
    relations = data.get("relations", [])
    
    if ot.use_species and species_ot is None:
        raise ValueError("ot.use_species=True but species_ot is None")
    
    # ------------- load taxon_map --------------
    if ot.use_species:
        try:
            with open(os.path.join(folder, species_ot.filename), "r", encoding="utf-8") as f:
                sp_data = json.load(f)
        except Exception:
            return None, [
                {
                    "type": "error",
                    "msg": f"{pmid} (species load error)"
                }
            ]

        # ------------------------------------------------
        # 1️⃣ build best_species map (LLM-resolved taxonomy)
        # ------------------------------------------------
        best_species = {}  # raw name → resolved taxonomy name
        for item in sp_data.get(species_ot.key_in_map, []):
            nm = item.get("name")
            best = item.get("llm_best_match")
            if nm and best:
                best_species[nm] = best.get("name")
        # ------------------------------------------------
        # 1️⃣bis build best_cell_line_species (cell line -> scientific name)
        # ------------------------------------------------
        best_cell_line_species = load_best_cell_line_species(folder, cvcl_ot)

        # ------------------------------------------------
        # 2️⃣ collect document-level species from ALL relations
        # ------------------------------------------------
        doc_species = []
        for block in relations:
            for rel in block.get("rel_from_this_sent", []):
                get_sp = extract_species_from_relation(rel, cvcl_ot=cvcl_ot, best_cell_line_species=best_cell_line_species)
                sp = get_sp({})  # trick: force relation-level fallback
                if len(sp) != 0:
                    doc_species.append(sp)

        # 去重但保序
        seen = set()
        doc_species = [x for x in doc_species if not (x in seen or seen.add(x))]

        # document-level fallback species（raw name）
        doc_level_species = doc_species[0] if doc_species else ""
        

    # ------------------------------------------------
    # 3️⃣ collect ontology_type entities
    # ------------------------------------------------
    needed = set()

    # helper: whether this Ontology wants relations
    def _is_relation_ot(ontology_type):
        if isinstance(ontology_type, str):
            return ontology_type == "relation"
        if isinstance(ontology_type, (list, tuple, set)):
            return "relation" in ontology_type
        return False

    is_relation_ot = _is_relation_ot(ot.ontology_type)

    for block in relations:
        for rel in block.get("rel_from_this_sent", []):
            # keep per-relation species extractor if needed
            if ot.use_species:
                get_sp = extract_species_from_relation(rel, cvcl_ot=cvcl_ot, best_cell_line_species=best_cell_line_species)

            # ---- special handling for relation ontology ----
            if is_relation_ot:
                rel_obj = rel.get("relation") or {}
                rname = rel_obj.get("name", "")
                rdesc = rel_obj.get("description", "")

                if rname:
                    # pack it into the same "entity-like" shape that collect_type expects
                    ent_relation = {
                        "name": rname,
                        "type": "relation",
                        "description": rdesc,
                        "meta": [],  # relation itself usually has no meta
                    }
                    if ot.use_species:
                        collect_type(
                            ent=ent_relation,
                            ontology_type="relation",
                            needed_keys=needed,
                            get_species=get_sp,
                        )
                    else:
                        collect_type(
                            ent=ent_relation,
                            ontology_type="relation",
                            needed_keys=needed,
                        )
                # relation-only mapping时，通常不再去 components/targets/contexts 里抓别的
                continue

            # ---- default: collect components/targets/contexts ----
            for field in ("components", "targets", "contexts"):
                for ent in rel.get(field, []):
                    if ot.use_species:
                        collect_type(
                            ent=ent,
                            ontology_type=ot.ontology_type,
                            needed_keys=needed,
                            get_species=get_sp,
                        )
                    else:
                        collect_type(
                            ent=ent,
                            ontology_type=ot.ontology_type,
                            needed_keys=needed,
                        )

    needed = dedup_needed_by_name_longest_desc(needed, use_species=ot.use_species)
        
    # ------------------------------------------------
    # 4️⃣ unify species with fallback
    # ------------------------------------------------
    filtered_items = []

    for item in needed:
        if ot.use_species:
            nm, desc, sp = item
        else:
            nm, desc = item
        species_final = ""

        # case 1: entity/relation-level species + resolved taxonomy
        if ot.use_species:
            if len(sp) != 0:
                species_final = resolve_species(sp, best_species, best_cell_line_species)
            elif len(doc_level_species) != 0:
                species_final = resolve_species(doc_level_species, best_species, best_cell_line_species)

        # else: species_final stays None (global UniProt search)

        if ot.use_species:
            filtered_items.append((nm, desc, species_final))
        else:
            filtered_items.append((nm, desc))

    if not filtered_items:
        out = {"pmid": pmid, "abstract": abstract, ot.key_in_map: []}
        with open(out_path, "w", encoding="utf-8") as fw:
            json.dump(out, fw, ensure_ascii=False, indent=2)
        return out, [
            {
                "type": "error",
                "msg": f"{pmid} (no {str(ot.ontology_type)})",
            }
        ]

    # ------------------------------------------------
    # 5️⃣ db search
    # ------------------------------------------------
    final_map = []
    num_hit = 0
    num_total = 0

    for item in filtered_items:
        if ot.use_species:
            name, desc, species = item
        else:
            name, desc = item
        query = name
        if len(desc) != 0:
            query += f", {desc}"
        if ot.use_species and len(species) != 0:
            query += f", {species}"
        query = re.sub(r"\([^)]*\)", "", str(query))
        query = re.sub(r"\[[^\]]*\]", "", query)
        if ot.use_species == False or (ot.use_species and len(species) != 0):
            try:
                hits = ot.search_func(query)
            except Exception:
                hits = []
        else:
            hits = []

        if hits:
            num_hit += 1
        num_total += 1
        final_map_item = {
            "name": name,
            "description": desc,
        }
        if ot.use_species:
            final_map_item["species"] = species
        final_map_item["hits"] = hits

        final_map.append(final_map_item)

    # ------------- write output --------------
    out = {
        "pmid": pmid,
        "abstract": abstract,
        ot.key_in_map: final_map,
    }

    with open(out_path, "w", encoding="utf-8") as fw:
        json.dump(out, fw, ensure_ascii=False, indent=2)

    return out, [
        {"type": "status", "name": "success", "description": f"{pmid}"},
        {"type": "metric", "correct": num_hit, "total": num_total},
    ]
    

def build_selection_prompt(
    name: str, hits: list, abstract: str, description: str = "", species: str = "", judge_method = "strict"
) -> str:
    """
    构建让 LLM 选择最正确 UniProt accession 的 prompt。
    """
    hits_text = "\n".join([f"- {h['id']}: {h.get('description', '')}" for h in hits])
    prompt = get_prompt(f"select_db_id/{judge_method}.txt")
    query = f"Name: {name}"
    if len(description) != 0:
        query += f"\nDescription: {description}"
    if len(species) != 0:
        query += f"\nSpecies: {species}"

    return prompt.format(query=query, abstract=abstract, hits_text=hits_text)


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
    folder: str, ot: Ontology, llm=None, **kwargs
):
    if llm is None:
        raise ValueError("llm is None")
    pmid = os.path.basename(folder)
    path = os.path.join(folder, ot.filename)

    if not os.path.exists(path):
        return None, [{"type": "error", "msg": f"skip pmid {pmid} (no file)"}]

    # === load JSON ===
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        return None, [
            {"type": "error", "msg": f"load fail pmid {pmid}"},
        ]

    abstract = data.get("abstract", "")
    db_list = data.get(ot.key_in_map, [])

    total = 0
    correct = 0

    for entry in db_list:
        name = entry.get("name", "")
        description = entry.get("description", "")
        species = entry.get("species", "")
        hits = entry.get("hits", [])

        if not hits:
            entry["llm_best_match"] = None
            continue

        prompt = build_selection_prompt(
            name=name, description=description, species=species, hits=hits, abstract=abstract, judge_method=ot.judge_method
        )

        try:
            llm_output = llm.query(prompt).strip()
        except Exception as e:
            llm_output = f"ERROR: {e}"
            entry["llm_raw_output"] = llm_output
            entry["llm_best_match"] = None
            continue

        # ---- 匹配 accession ----
        best_hit = match_llm_output_to_db_id(llm_output, hits)

        entry["llm_raw_output"] = llm_output
        entry["llm_best_match"] = best_hit

        if best_hit is not None:
            correct += 1
        total += 1

    data[ot.key_in_map] = db_list

    out_path = os.path.join(folder, ot.filename)
    with open(out_path, "w", encoding="utf-8") as fw:
        json.dump(data, fw, ensure_ascii=False, indent=2)

    return data, [
        {"type": "status", "name": "success", "description": f"ok pmid {pmid}"},
        {"type": "metric", "name": "judge", "correct": correct, "total": total},
    ]

def process_one_folder_apply_llm_best(
    folder: str,
    input_name: str,
    output_name: str,
    ot_list: List["Ontology"],
    species_ot: Optional["Ontology"] = None,
    cvcl_ot: Optional["Ontology"] = None,
):
    """
    使用各 ontology 的 llm_best_match 回写 entity（权威映射阶段）

    ✅ 核心保证：
    1) entity & meta entity 都按同样规则映射（命中 ontology 但无 best -> 删除）
    2) species 解析逻辑与 get_db_id/judge 对齐：
       - entity.meta species 优先
       - relation-level fallback
       - cell-line fallback
       - document-level fallback（全局兜底）
    3) 匹配与 judge 对齐：优先 (name, species_final)，并做安全 fallback
    4) 回写字段：id / name / description（保留其它字段）
    """

    pmid = os.path.basename(folder)
    in_path = os.path.join(folder, input_name)
    out_path = os.path.join(folder, output_name)

    # -----------------------------
    # 0) load relation file
    # -----------------------------
    if not os.path.exists(in_path):
        return None, [{"type": "error", "msg": f"{pmid} missing input file: {input_name}"}]

    try:
        with open(in_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        return None, [{"type": "error", "msg": f"{pmid} load fail: {repr(e)}"}]

    relations = data.get("relations", [])
    if not relations:
        with open(out_path, "w", encoding="utf-8") as fw:
            json.dump(data, fw, ensure_ascii=False, indent=2)
        return data, [{"type": "status", "name": "no_relations"}]

    # -----------------------------
    # 1) load species maps (for resolve_species + optional id mapping)
    # -----------------------------
    best_species_name_map: Dict[str, str] = {}   # raw -> scientific
    best_species_hit_map: Dict[str, Dict[str, Any]] = {}  # raw -> best_hit(dict)

    best_cell_line_species: Dict[str, str] = {}
    if cvcl_ot:
        best_cell_line_species = load_best_cell_line_species(folder, cvcl_ot)

    if species_ot:
        sp_path = os.path.join(folder, species_ot.filename)
        if os.path.exists(sp_path):
            try:
                with open(sp_path, "r", encoding="utf-8") as f:
                    sp_data = json.load(f)
                for it in sp_data.get(species_ot.key_in_map, []):
                    raw_nm = (it.get("name") or "").strip()
                    best = it.get("llm_best_match")
                    if raw_nm and isinstance(best, dict) and best.get("name"):
                        best_species_name_map[raw_nm] = best.get("name")
                        best_species_hit_map[raw_nm] = best
            except Exception:
                # species map 读失败不硬炸：只是不做 species canonicalize
                pass

    # -----------------------------
    # 2) compute document-level fallback species (raw)
    #    与 get_db_id 完全一致：对每个 relation 取 get_sp({}) 结果
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
                sp = (get_sp({}) or "").strip()  # 强制 relation-level fallback
                if sp:
                    doc_species.append(sp)

        # 去重保序
        seen = set()
        doc_species = [x for x in doc_species if not (x in seen or seen.add(x))]
        return doc_species[0] if doc_species else ""

    doc_level_species_raw = compute_doc_level_species_raw()

    # -----------------------------
    # 3) build ontology lookup tables from each ot.filename (judge输出)
    #    lut_exact:
    #      - use_species=False: (name,) -> best
    #      - use_species=True : (name, species_final) -> best
    #    lut_by_name (only for use_species=True):
    #      name -> list[(species_final, best)]
    # -----------------------------
    ot_lookup: Dict["Ontology", Dict[str, Any]] = {}

    for ot in ot_list:
        if not ot.filename:
            continue

        path = os.path.join(folder, ot.filename)
        if not os.path.exists(path):
            continue

        try:
            with open(path, "r", encoding="utf-8") as f:
                mdata = json.load(f)
        except Exception:
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

        ot_lookup[ot] = {
            "exact": lut_exact,
            "by_name": lut_by_name,  # 空dict也行
        }

    # -----------------------------
    # 4) helpers: species resolution (align get_db_id)
    # -----------------------------
    def resolve_species_final_for_entity(ent: Dict[str, Any], get_sp_func) -> str:
        """
        产生用于匹配映射表的 species_final（scientific）
        与 get_db_id 对齐：
          1) entity.meta 中 species 优先（由 get_sp_func 体现）
          2) relation-level fallback
          3) cell-line fallback
          4) document-level fallback
        """
        sp_raw = (get_sp_func(ent) or "").strip()
        if not sp_raw and doc_level_species_raw:
            sp_raw = doc_level_species_raw

        if not sp_raw:
            return ""

        return (resolve_species(sp_raw, best_species_name_map, best_cell_line_species) or "").strip()

    # -----------------------------
    # 5) helpers: lookup best (match judge)
    # -----------------------------
    def lookup_best_for_ot(ot: "Ontology", name: str, species_final: str) -> Optional[Dict[str, Any]]:
        info = ot_lookup.get(ot)
        if not info:
            return None

        lut_exact = info["exact"]
        lut_by_name = info.get("by_name", {})

        if not ot.use_species:
            return lut_exact.get((name,))

        # use_species=True
        # 1) exact match
        best = lut_exact.get((name, species_final))
        if best is not None:
            return best

        # 2) fallback to (name, "")
        best = lut_exact.get((name, ""))
        if best is not None:
            return best

        # 3) safe fallback: if this name has exactly ONE candidate species in mapping file, use it
        cands = lut_by_name.get(name, [])
        # 去重 species 候选
        uniq = {}
        for sp, b in cands:
            if (sp or "") not in uniq:
                uniq[(sp or "")] = b
        if len(uniq) == 1:
            return next(iter(uniq.values()))

        return None

    # -----------------------------
    # 6) core mapper: map one entity (and its meta recursively)
    # -----------------------------
    def map_one_entity(ent: Dict[str, Any], get_sp_func) -> Optional[Dict[str, Any]]:
        """
        返回：
          - dict: 映射后 entity
          - None: 该 entity 应被删除
        规则：
          - 若 ent.type 命中某个 ot.ontology_type：
                - 找到 best -> 回写 id/name/description + 映射其 meta
                - 找不到 best -> 删除 ent
          - 若不命中任何 ot -> 只映射它的 meta（meta 若命中且无 best 仍会被删）
        """
        nonlocal attempted, mapped_success
        if not isinstance(ent, dict):
            return None

        etype = ent.get("type")
        name = (ent.get("name") or "").strip()
        if not name or not etype:
            return None

        # 先递归处理 meta（无论 entity 本体是否命中 ontology）
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

        # 默认先保留 meta 更新
        ent2 = dict(ent)
        ent2["meta"] = new_meta

        # 处理 species 类型（即使不在 ot_list 里，也尽量 canonicalize 到 scientific）
        # 这里仅 canonicalize name；id 只有在 best_species_hit_map 命中 raw name 时可写入
        # if etype == "species":
        #     raw = name
        #     sci = best_species_name_map.get(raw, "")
        #     if sci:
        #         ent2["name"] = sci
        #         best_hit = best_species_hit_map.get(raw)
        #         if isinstance(best_hit, dict) and best_hit.get("id"):
        #             ent2["id"] = best_hit.get("id")
        #         # species 一般不需要 description，但你要求 id/name/description 都映射，就不强删字段
        #     return ent2

        # 看 entity 本体是否命中某个 ontology
        for ot in ot_list:
            if etype not in (ot.ontology_type or []):
                continue

            # 该 entity 被这个 ontology 管辖：必须能映射，否则删除
            attempted += 1
            species_final = ""
            if ot.use_species:
                species_final = resolve_species_final_for_entity(ent, get_sp_func)

            best = lookup_best_for_ot(ot, name, species_final)
            if best is None:
                return None  # ❌ 命中 ontology 但无 best -> 删除
            mapped_success += 1

            # ✅ 回写 id/name/description
            ent2["id"] = best.get("id", ent2.get("id"))
            ent2["name"] = best.get("name") or best.get("id") or ent2.get("name")
            if "description" in best:
                ent2["description"] = best.get("description", ent2.get("description", ""))

            # ✅ 若该 ontology 要求 species：规范化 species meta（与 get_db_id 对齐）
            if ot.use_species and species_final:
                meta_wo_species = [mm for mm in ent2.get("meta", []) if mm.get("type") != "species"]

                # ✅ 先构造，再走一次 meta 映射（让 taxon 补齐 id）
                sp_meta = {"name": species_final, "type": "species", "description": ""}

                sp_meta_mapped = map_one_meta_entity(sp_meta, get_sp_func)
                if sp_meta_mapped is None:
                    # 理论上不该发生（除非 taxon 没有 best），那就退化成裸 meta
                    sp_meta_mapped = {"name": species_final, "type": "species", "description": ""}

                # ✅ species 按你 schema：不需要 description（建议删掉）
                sp_meta_mapped.pop("description", None)

                meta_wo_species.append(sp_meta_mapped)
                ent2["meta"] = meta_wo_species

            return ent2

        # 不命中任何 ontology：仅 meta 已被映射，entity 本体原样保留（但 meta 已清洗）
        return ent2

    def map_one_meta_entity(m: Dict[str, Any], get_sp_func) -> Optional[Dict[str, Any]]:
        """
        meta entity 与主 entity 同规则：
          - 命中 ontology 但没 best -> 删除这个 meta entry
          - 命中且有 best -> 回写 id/name/description，并递归处理它自己的 meta（如果有）
          - 不命中任何 ot -> 原样保留（但递归清洗其 meta）
        """
        nonlocal attempted, mapped_success
        if not isinstance(m, dict):
            return None

        mtype = m.get("type")
        mname = (m.get("name") or "").strip()
        if not mtype or not mname:
            return None

        # 递归清洗 meta.meta（通常不会有，但允许）
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

        # species meta 特判：尽量 canonicalize
        # if mtype == "species":
        #     raw = mname
        #     sci = best_species_name_map.get(raw, "")
        #     if sci:
        #         m2["name"] = sci
        #         best_hit = best_species_hit_map.get(raw)
        #         if isinstance(best_hit, dict) and best_hit.get("id"):
        #             m2["id"] = best_hit.get("id")
        #     return m2

        # 命中 ontology 的 meta：必须能映射
        for ot in ot_list:
            if mtype not in (ot.ontology_type or []):
                continue
            attempted += 1

            species_final = ""
            if ot.use_species:
                species_final = resolve_species_final_for_entity(m, get_sp_func)

            best = lookup_best_for_ot(ot, mname, species_final)
            if best is None:
                return None  # ❌ 命中 ontology 但无 best -> 删除 meta
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

        # 不命中任何 ontology：保留（但 meta.meta 已清洗）
        return m2

    # -----------------------------
    # 7) apply mapping to all relations/entities
    # -----------------------------
    mapped_success = 0
    attempted = 0
    for block in relations:
        for rel in block.get("rel_from_this_sent", []):
            get_sp = extract_species_from_relation(
                rel,
                cvcl_ot=cvcl_ot,
                best_cell_line_species=best_cell_line_species,
            )

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
    # 8) write output
    # -----------------------------
    data.setdefault("_apply_llm_best_report", {})
    data["_apply_llm_best_report"].update({
        "pmid": pmid,
        "doc_level_species_raw": doc_level_species_raw,
        "mode": "apply_llm_best_with_meta_and_species_alignment",
    })

    with open(out_path, "w", encoding="utf-8") as fw:
        json.dump(data, fw, ensure_ascii=False, indent=2)

    return data, [
        {"type": "status", "name": "success", "description": f"{pmid} mapped"},
        {"type": "metric", "name": "success", "correct": mapped_success, "total": attempted},
    ]