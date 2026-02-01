# src/pmcad/db_change.py
import os
import json
from src.pmcad.prompts import get_prompt
from src.pmcad.ontology_map import (
    Ontology,
    extract_species_from_relation,
    load_best_cell_line_species,
    resolve_species,
)
from src.pmcad.pmidstore import PMIDStore
from typing import Optional, Union

def collect_unresolved(ds_src: dict, src_ot: Ontology) -> list:
    unresolved = []
    for it in (ds_src.get(src_ot.key_in_map, []) or []):
        if it.get("llm_best_match") is None:
            unresolved.append(it)
    return unresolved


def _normalize(s: str) -> str:
    return str(s).strip().lower().replace('"', "").replace("'", "")

def match_llm_output_to_hit(llm_output: str, hits: list):
    out = _normalize(llm_output)
    if out == "none":
        return None

    # 先按 id
    for h in hits:
        hid = h.get("id")
        if hid and _normalize(hid) == out:
            return h

    # 再按 name 兜底
    for h in hits:
        nm = h.get("name")
        if nm and _normalize(nm) == out:
            return h

    return None
def build_selection_prompt(
    tgt_ot: Ontology,
    name: str,
    description: str,
    abstract: str,
    hits: list,
    *,
    species: str = "",
    relation_example: str = "",
) -> str:
    """
    ✅ NEW:
    - species: 物种（scientific name），用于辅助 LLM disambiguation
    - relation_example: 一条包含该实体的 relation（简化 JSON），用于辅助 LLM 判断
    """
    prompt = get_prompt(f"select_db_id/{tgt_ot.judge_method}.txt")

    hits_text = "\n".join(
        [f"- {h.get('id','NA')}: {h.get('description','')}" for h in (hits or [])]
    )

    query = f"Name: {name}"
    if description:
        query += f"\nDescription: {description}"
    if species:
        query += f"\nSpecies: {species}"

    abstract2 = abstract or ""
    if relation_example:
        abstract2 = (abstract2 + "\n\n[Relation example containing this entity]\n" + relation_example).strip()

    return prompt.format(query=query, abstract=abstract2, hits_text=hits_text)

def process_one_folder_convert_failed(
    *,
    input_name: str = "",
    src_ot: Ontology = None,
    tgt_ot: Ontology = None,
    llm=None,
    species_ot: Optional[Ontology] = None,
    cvcl_ot: Optional[Ontology] = None,
    pmid: Union[int, str],
    store: "PMIDStore",
    **kwargs,
):
    """
    仅支持 DB 模式（folder 模式删除）：
      - ds / src / tgt: store.get
      - 写回：store.put
    """
    if src_ot is None or tgt_ot is None:
        raise ValueError("src_ot/tgt_ot is None")
    if llm is None:
        raise ValueError("llm is None")
    if store is None:
        raise ValueError("store is required (folder mode removed)")

    pmid_int = int(pmid)
    pmid_str = str(pmid_int)

    # -------- load ds / src / tgt --------
    ds = store.get(pmid_int, input_name)
    ds_src = store.get(pmid_int, src_ot.filename)
    ds_tgt = store.get(pmid_int, tgt_ot.filename)

    if not isinstance(ds, dict):
        return None, [{"type": "error", "msg": f"pmid:{pmid_str} (load ds error)"}]
    if not isinstance(ds_src, dict):
        return None, [{"type": "error", "msg": f"pmid:{pmid_str} (load src error)"}]
    if not isinstance(ds_tgt, dict):
        ds_tgt = {}

    original_tgt_map = ds_tgt.get(tgt_ot.key_in_map, []) or []
    if not isinstance(original_tgt_map, list):
        original_tgt_map = []

    abstract = ds.get("abstract", "")
    unresolved = collect_unresolved(ds_src, src_ot)

    relations = ds.get("relations", []) or []
    src_types = set(src_ot.ontology_type if isinstance(src_ot.ontology_type, list) else [src_ot.ontology_type])

    best_species = {}
    best_cell_line_species = {}

    if tgt_ot.use_species:
        # 1) taxon best map (raw -> scientific)
        if species_ot is not None and species_ot.filename:
            sp_data = store.get(pmid_int, species_ot.filename)
            if isinstance(sp_data, dict):
                for item in sp_data.get(species_ot.key_in_map, []) or []:
                    nm = (item.get("name") or "").strip()
                    best = item.get("llm_best_match") or {}
                    if nm and isinstance(best, dict):
                        best_species[nm] = (best.get("name") or "").strip()

        # 2) cell-line -> species map
        best_cell_line_species = load_best_cell_line_species(
            cvcl_ot,
            store=store,
            pmid=pmid_int,
        )

        # 3) doc-level fallback species raw
        doc_species_raw = []
        for block in relations:
            for rel in (block.get("rel_from_this_sent") or []):
                get_sp = extract_species_from_relation(
                    rel, cvcl_ot=cvcl_ot, best_cell_line_species=best_cell_line_species
                )
                sp_raw = (get_sp({}) or "").strip()
                if sp_raw:
                    doc_species_raw.append(sp_raw)
        seen = set()
        doc_species_raw = [x for x in doc_species_raw if not (x in seen or seen.add(x))]
        doc_level_species_raw = doc_species_raw[0] if doc_species_raw else ""
    else:
        doc_level_species_raw = ""

    def _species_for_name(name: str) -> str:
        if not tgt_ot.use_species:
            return ""

        name = (name or "").strip()
        if not name:
            return ""

        for block in relations:
            for rel in (block.get("rel_from_this_sent") or []):
                get_sp = extract_species_from_relation(
                    rel, cvcl_ot=cvcl_ot, best_cell_line_species=best_cell_line_species
                )

                def _scan(ent: dict) -> bool:
                    if not isinstance(ent, dict):
                        return False
                    if ent.get("type") in src_types and (ent.get("name") or "").strip() == name:
                        return True
                    for m in (ent.get("meta") or []):
                        if _scan(m):
                            return True
                    return False

                for field in ("components", "targets", "contexts"):
                    for ent in (rel.get(field) or []):
                        if _scan(ent):
                            sp_raw = (get_sp(ent) or "").strip()
                            if not sp_raw:
                                sp_raw = (get_sp({}) or "").strip()
                            if not sp_raw and doc_level_species_raw:
                                sp_raw = doc_level_species_raw
                            return (resolve_species(sp_raw, best_species, best_cell_line_species) or "").strip()

        if doc_level_species_raw:
            return (resolve_species(doc_level_species_raw, best_species, best_cell_line_species) or "").strip()

        return ""

    def _relation_example_for_name(name: str) -> str:
        name = (name or "").strip()
        if not name:
            return ""

        def _scan(ent: dict) -> bool:
            if not isinstance(ent, dict):
                return False
            if ent.get("type") in src_types and (ent.get("name") or "").strip() == name:
                return True
            for m in (ent.get("meta") or []):
                if _scan(m):
                    return True
            return False

        for block in relations:
            for rel in (block.get("rel_from_this_sent") or []):
                for field in ("components", "targets", "contexts"):
                    for ent in (rel.get(field) or []):
                        if _scan(ent):
                            slim = {
                                "relation": rel.get("relation", {}),
                                "components": rel.get("components", []),
                                "targets": rel.get("targets", []),
                                "contexts": rel.get("contexts", []),
                            }
                            return json.dumps(slim, ensure_ascii=False)

        return ""

    if not unresolved:
        out = {"pmid": pmid_str, "abstract": abstract, tgt_ot.key_in_map: original_tgt_map}
        store.put(pmid_int, tgt_ot.filename, out)
        return {}, [{"type": "error", "msg": f"pmid:{pmid_str} (skip no unresolved)"}]

    mapped_list = []
    n_total = 0
    n_correct = 0
    n_error = 0
    converted_src_keys = set()

    for it in unresolved:
        name = (it.get("name") or "").strip()
        if not name:
            continue
        desc = (it.get("description") or "").strip()

        src_species = (it.get("species") or "").strip() if src_ot.use_species else ""

        species = ""
        if tgt_ot.use_species:
            species = _species_for_name(name)

        query = name
        if desc:
            query += f", {desc}"
        if tgt_ot.use_species and species:
            query += f", {species}"

        n_total += 1

        try:
            hits = tgt_ot.search_func(query) or []
        except Exception:
            hits = []

        entry = {"name": name, "description": desc, "hits": hits}
        if tgt_ot.use_species:
            entry["species"] = species

        if hits:
            prompt = build_selection_prompt(
                tgt_ot=tgt_ot,
                name=name,
                description=desc,
                abstract=abstract,
                hits=hits,
                species=species,
                relation_example=_relation_example_for_name(name),
            )
            try:
                llm_output = llm.query(prompt).strip()
            except Exception as e:
                llm_output = f"ERROR: {e}"
                n_error += 1

            best = match_llm_output_to_hit(llm_output, hits)
            entry["llm_raw_output"] = llm_output
            entry["llm_best_match"] = best
            if best is not None:
                n_correct += 1
                if src_ot.use_species:
                    converted_src_keys.add((name, desc, src_species))
                else:
                    converted_src_keys.add((name, desc))
        else:
            entry["llm_best_match"] = None

        mapped_list.append(entry)

    # =========================
    # (A) merge + dedup tgt map
    # =========================
    seen = set()
    new_tgt = []

    def _dedup_key(e: dict):
        if tgt_ot.use_species:
            return (
                (e.get("name", "") or "").strip(),
                (e.get("description", "") or "").strip(),
                (e.get("species", "") or "").strip(),
            )
        return ((e.get("name") or "").strip(), (e.get("description") or "").strip())

    for e in original_tgt_map:
        key = _dedup_key(e)
        if key in seen:
            continue
        seen.add(key)
        new_tgt.append(e)

    for e in mapped_list:
        key = _dedup_key(e)
        if key in seen:
            continue
        seen.add(key)
        new_tgt.append(e)

    out = {
        "pmid": pmid_str,
        "abstract": abstract,
        tgt_ot.key_in_map: [e for e in new_tgt if e.get("llm_best_match") is not None],
    }
    store.put(pmid_int, tgt_ot.filename, out)

    # =========================================
    # (B) relabel entities in ds.json: src -> tgt
    # =========================================
    success_map = {}
    for e in mapped_list:
        best = e.get("llm_best_match")
        if not best:
            continue
        nm = (e.get("name") or "").strip()
        dscr = (e.get("description") or "").strip()
        if not nm:
            continue

        if tgt_ot.use_species:
            sp = (e.get("species") or "").strip()
            if sp:
                success_map[(nm, dscr, sp)] = best
        else:
            success_map[(nm, dscr)] = best

    tgt_type = (
        tgt_ot.ontology_type[0]
        if isinstance(tgt_ot.ontology_type, list) and tgt_ot.ontology_type
        else tgt_ot.ontology_type
    )

    n_retyped = 0

    def _species_final_for_entity(ent: dict, get_sp_func) -> str:
        if not tgt_ot.use_species:
            return ""

        sp_raw = (get_sp_func(ent) or "").strip()
        if not sp_raw:
            sp_raw = (get_sp_func({}) or "").strip()
        if not sp_raw and doc_level_species_raw:
            sp_raw = doc_level_species_raw
        if not sp_raw:
            return ""
        return (resolve_species(sp_raw, best_species, best_cell_line_species) or "").strip()

    def relabel_entity(ent: dict, get_sp_func):
        nonlocal n_retyped
        if not isinstance(ent, dict):
            return

        ent_type = ent.get("type")
        ent_name = (ent.get("name") or "").strip()
        ent_desc = (ent.get("description") or "").strip()

        if ent_type in src_types and ent_name:
            if tgt_ot.use_species:
                sp_final = _species_final_for_entity(ent, get_sp_func)
                best = success_map.get((ent_name, ent_desc, sp_final)) if sp_final else None
            else:
                best = success_map.get((ent_name, ent_desc))

            if best is not None:
                ent["type"] = tgt_type
                if not ent.get("description") and best.get("description"):
                    ent["description"] = best.get("description")
                n_retyped += 1

        for m in (ent.get("meta") or []):
            relabel_entity(m, get_sp_func)

    for blk in (ds.get("relations") or []):
        for rel in (blk.get("rel_from_this_sent") or []):
            get_sp = extract_species_from_relation(
                rel,
                cvcl_ot=cvcl_ot,
                best_cell_line_species=best_cell_line_species,
            )
            for field in ("components", "targets", "contexts"):
                for ent in (rel.get(field) or []):
                    relabel_entity(ent, get_sp)

    store.put(pmid_int, input_name, ds)

    # =========================================
    # (C) cleanup src file: remove converted ones
    # =========================================
    src_list = ds_src.get(src_ot.key_in_map, []) or []
    if not isinstance(src_list, list):
        src_list = []

    new_src_list = []
    for it in src_list:
        nm = (it.get("name") or "").strip()
        dscr = (it.get("description") or "").strip()

        if src_ot.use_species:
            sp = (it.get("species") or "").strip()
            key = (nm, dscr, sp)
        else:
            key = (nm, dscr)

        if key in converted_src_keys:
            continue
        new_src_list.append(it)

    ds_src[src_ot.key_in_map] = new_src_list
    store.put(pmid_int, src_ot.filename, ds_src)

    return {}, [
        {"type": "status", "name": "success", "description": f"pmid:{pmid_str}"},
        {"type": "metric", "name": "judge", "correct": n_correct, "total": n_total},
        {"type": "metric", "name": "llm_error", "correct": n_error, "total": n_total},
        {"type": "metric", "name": "retyped_entities", "correct": n_retyped, "total": n_retyped},
    ]
