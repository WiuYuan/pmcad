import os
import json
from src.pmcad.prompts import get_prompt
from src.pmcad.ontology_map import Ontology
from typing import Optional
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
) -> str:
    prompt = get_prompt(f"select_db_id/{tgt_ot.judge_method}.txt")

    hits_text = "\n".join(
        [f"- {h.get('id','NA')}: {h.get('description','')}" for h in (hits or [])]
    )

    query = f"Name: {name}"
    if description:
        query += f"\nDescription: {description}"

    return prompt.format(query=query, abstract=abstract, hits_text=hits_text)

def process_one_folder_convert_failed(
    folder: str,
    input_name: str,
    src_ot: Ontology,
    tgt_ot: Ontology,
    llm,
):
    pmid = os.path.basename(folder)

    p_ds = os.path.join(folder, input_name)
    p_src = os.path.join(folder, src_ot.filename)
    p_tgt = os.path.join(folder, tgt_ot.filename)

    # ---- load ds.json ----
    try:
        with open(p_ds, "r", encoding="utf-8") as f:
            ds = json.load(f)
    except Exception:
        return None, [{"type": "error", "msg": f"pmid:{pmid} (load ds error)"}]

    # ---- load src file ----
    try:
        with open(p_src, "r", encoding="utf-8") as f:
            ds_src = json.load(f)
    except Exception:
        return None, [{"type": "error", "msg": f"pmid:{pmid} (load src error)"}]

    # ---- load tgt file ----
    try:
        with open(p_tgt, "r", encoding="utf-8") as f:
            ds_tgt = json.load(f)
    except Exception:
        ds_tgt = {}
    original_tgt_map = ds_tgt.get(tgt_ot.key_in_map, []) or []
    if not isinstance(original_tgt_map, list):
        original_tgt_map = []

    abstract = ds.get("abstract", "")
    unresolved = collect_unresolved(ds_src, src_ot)

    if not unresolved:
        out = {"pmid": pmid, "abstract": abstract, tgt_ot.key_in_map: original_tgt_map}
        with open(p_tgt, "w", encoding="utf-8") as fw:
            json.dump(out, fw, ensure_ascii=False, indent=2)
        return out, [{"type": "error", "msg": f"pmid:{pmid} (skip no unresolved)"}]

    mapped_list = []
    n_total = 0
    n_correct = 0
    for it in unresolved:
        name = (it.get("name") or "").strip()
        if not name:
            continue

        desc = (it.get("description") or "").strip()

        # query = name + description（不拼 species）
        query = name
        if desc:
            query += f", {desc}"

        n_total += 1

        # ---- search via tgt_ot ----
        try:
            hits = tgt_ot.search_func(query) or []
        except Exception:
            hits = []

        entry = {"name": name, "description": desc, "hits": hits}

        if hits and llm is not None:
            prompt = build_selection_prompt(
                tgt_ot=tgt_ot,
                name=name,
                description=desc,
                abstract=abstract,
                hits=hits,
            )
            try:
                llm_output = llm.query(prompt).strip()
            except Exception as e:
                llm_output = f"ERROR: {e}"

            best = match_llm_output_to_hit(llm_output, hits)
            entry["llm_raw_output"] = llm_output
            entry["llm_best_match"] = best
            if best is not None:
                n_correct += 1
        else:
            entry["llm_best_match"] = None

        mapped_list.append(entry)

    # =========================
    # (A) merge + dedup tgt map (ONLY name+description)
    # =========================
    seen = set()
    new_tgt = []

    def _dedup_key(e: dict):
        return (e.get("name", "") or "", e.get("description", "") or "")

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
        "pmid": pmid,
        "abstract": abstract,
        tgt_ot.key_in_map: [
            e for e in new_tgt if e.get("llm_best_match") is not None
        ]
    }

    # ---- write tgt file ----
    with open(p_tgt, "w", encoding="utf-8") as fw:
        json.dump(out, fw, ensure_ascii=False, indent=2)

    # =========================================
    # (B) relabel entities in ds.json: src -> tgt
    #     mapping key: (name, description)
    # =========================================
    success_map = {}
    for e in mapped_list:
        best = e.get("llm_best_match")
        if not best:
            continue
        nm = (e.get("name") or "").strip()
        dscr = (e.get("description") or "").strip()
        if nm:
            success_map[(nm, dscr)] = best

    src_types = set(src_ot.ontology_type if isinstance(src_ot.ontology_type, list) else [src_ot.ontology_type])
    tgt_type = (
        tgt_ot.ontology_type[0]
        if isinstance(tgt_ot.ontology_type, list) and tgt_ot.ontology_type
        else tgt_ot.ontology_type
    )

    n_retyped = 0

    def relabel_entity(ent: dict):
        nonlocal n_retyped
        if not isinstance(ent, dict):
            return

        ent_type = ent.get("type")
        ent_name = (ent.get("name") or "").strip()
        ent_desc = (ent.get("description") or "").strip()

        if ent_type in src_types and ent_name:
            best = success_map.get((ent_name, ent_desc))
            # 如果 ds.json 里的 description 缺失，但 unresolved 里有 desc，允许回退按 name-only（可选）
            # if best is None and ent_desc == "":
            #     best = success_map.get((ent_name, ""))

            if best is not None:
                ent["type"] = tgt_type
                if not ent.get("description") and best.get("description"):
                    ent["description"] = best.get("description")
                n_retyped += 1

        for m in (ent.get("meta") or []):
            relabel_entity(m)

    for blk in (ds.get("relations") or []):
        for rel in (blk.get("rel_from_this_sent") or []):
            for field in ("components", "targets", "contexts"):
                for ent in (rel.get(field) or []):
                    relabel_entity(ent)

    # ---- write ds.json back ----
    with open(p_ds, "w", encoding="utf-8") as f:
        json.dump(ds, f, ensure_ascii=False, indent=2)
        
    # =========================================
    # (C) cleanup src file: remove converted ones then rewrite p_src
    #     key: (name, description)
    # =========================================
    converted_keys = set()
    for e in mapped_list:
        if e.get("llm_best_match") is None:
            continue
        converted_keys.add(((e.get("name") or "").strip(), (e.get("description") or "").strip()))

    src_list = ds_src.get(src_ot.key_in_map, []) or []
    if not isinstance(src_list, list):
        src_list = []

    new_src_list = []
    for it in src_list:
        nm = (it.get("name") or "").strip()
        dscr = (it.get("description") or "").strip()
        if (nm, dscr) in converted_keys:
            continue
        new_src_list.append(it)

    ds_src[src_ot.key_in_map] = new_src_list

    with open(p_src, "w", encoding="utf-8") as f:
        json.dump(ds_src, f, ensure_ascii=False, indent=2)

    return out, [
        {"type": "status", "name": "success", "description": f"pmid:{pmid}"},
        {"type": "metric", "name": "judge", "correct": n_correct, "total": n_total},
        {"type": "metric", "name": "retyped_entities", "correct": n_retyped, "total": n_retyped},
    ]