import os
import json
from src.pmcad.parallel_process import process_folder_parallel, process_one_folder_merge_json

def process_one_folder_relations(
    folder: str,
    input_name="merged.json"
):
    pmid = os.path.basename(folder)
    path = os.path.join(folder, input_name)

    if not os.path.exists(path):
        return None, [{"type": "status", "name": f"skip pmid {pmid} (no file)"}]

    # === load JSON ===
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        return None, [
            {"type": "status", "name": f"load fail pmid {pmid}"},
            {"type": "error", "msg": str(e)},
        ]

    pmid_val = data.get("pmid", pmid)
    abstract_val = data.get("abstract", "")

    relations_blocks = data.get("relations", [])
    go_mapping = data.get("go_mapping", [])
    uniprot_match = data.get("uniprot_match", [])

    # --------------------------------------------------
    # 1) GO name → llm_best_match
    # --------------------------------------------------
    go_by_name = {g.get("name"): g.get("llm_best_match") for g in go_mapping}

    # --------------------------------------------------
    # 2) UniProt 映射
    # --------------------------------------------------
    def _norm(x):
        return (x or "").strip().lower()

    uniprot_by_key = {}

    for u in uniprot_match:
        key = (
            _norm(u.get("original_name")),
            _norm(u.get("species")),
            _norm(u.get("entity_type")),
        )

        best = u.get("llm_best_match")
        if isinstance(best, dict):
            acc = best.get("accession")
            desc = best.get("description")

            uniprot_by_key[key] = {
                "accession": acc,
                "description": desc
            }

    # --------------------------------------------------
    # 3) 处理 relations（⭐关键修改点）
    # --------------------------------------------------
    result_relations = []
    total = 0
    mapped_count = 0

    for sent_block in relations_blocks:
        rel_list = sent_block.get("rel_from_this_sent", [])

        for rel in rel_list:
            total += 1

            rel_species = rel.get("species", "")
            rel_species_norm = _norm(rel_species)

            # ---------- components ----------
            norm_components = []
            any_mapped_component = False

            for comp in rel.get("components", []):
                cname = comp.get("name", "")
                ctype = comp.get("type", "")

                key = (_norm(cname), rel_species_norm, _norm(ctype))
                uniprot_info = uniprot_by_key.get(key)

                comp_out = {
                    "name": cname,
                    "type": ctype,
                    "uniprot": (
                        uniprot_info
                        if uniprot_info and uniprot_info.get("accession")
                        else None
                    )
                }

                if comp_out["uniprot"]:
                    any_mapped_component = True

                norm_components.append(comp_out)

            # ---------- targets（现在是 list） ----------
            norm_targets = []
            any_mapped_target = False

            for tgt in rel.get("target", []):
                tgt_name = tgt.get("name", "")
                tgt_type = tgt.get("type", "")
                tgt_desc = tgt.get("description", "")

                tgt_out = {
                    "name": tgt_name,
                    "type": tgt_type,
                    "description": tgt_desc,
                }

                if tgt_type == "GO":
                    go_info = go_by_name.get(tgt_name)
                    if isinstance(go_info, dict):
                        tgt_out["go_id"] = go_info.get("id")
                        tgt_out["go_name"] = go_info.get("name")
                        any_mapped_target = True
                    else:
                        tgt_out["go_id"] = None
                        tgt_out["go_name"] = None

                elif tgt_type in ("protein", "gene"):
                    key = (_norm(tgt_name), rel_species_norm, _norm(tgt_type))
                    uniprot_info = uniprot_by_key.get(key)
                    tgt_out["uniprot"] = (
                        uniprot_info
                        if uniprot_info and uniprot_info.get("accession")
                        else None
                    )
                    if tgt_out.get("uniprot"):
                        any_mapped_target = True

                norm_targets.append(tgt_out)

            if any_mapped_component or any_mapped_target:
                mapped_count += 1

            result_relations.append({
                "pmid": pmid_val,
                "abstract": abstract_val,
                "sentence": sent_block.get("sentence"),
                "relation": rel.get("relation"),
                "species": rel_species,
                "components": norm_components,
                "target": norm_targets,
                "justification": rel.get("justification", "")
            })

    return result_relations, [
        {"type": "status", "name": f"ok pmid {pmid}"},
        {"type": "metric", "correct": mapped_count, "total": total}
    ]
    
def get_protein_gene_map_go(results):
    # ---------- 1) expand components × targets ----------
    expanded = []

    for rel in results:
        comps = rel.get("components", [])
        targets = rel.get("target", [])  # ⭐ 现在是 list

        base = {
            "pmid": rel["pmid"],
            "abstract": rel["abstract"],
            "relation": rel["relation"],
            "species": rel["species"],
            "justification": rel.get("justification", "")
        }

        for comp in comps:
            for tgt in targets:
                r2 = base.copy()
                r2["component"] = comp
                r2["target"] = tgt
                expanded.append(r2)


    # ---------- 2) filter gene/protein -> GO (KEEP unmapped UniProt) ----------
    filtered = []

    for r in expanded:
        comp = r.get("component", {})
        tgt = r.get("target", {})

        # --- component must be gene/protein ---
        if comp.get("type") not in ("gene", "protein"):
            continue

        # --- target must be GO and mapped ---
        if tgt.get("type") != "GO":
            continue
        if not tgt.get("go_id"):
            continue

        # --- UniProt mapping status ---
        uniprot_info = comp.get("uniprot")
        has_uniprot = bool(uniprot_info and uniprot_info.get("accession"))

        # ⭐ 显式标记
        r["uniprot_mapped"] = has_uniprot
        r["uniprot_id"] = uniprot_info.get("accession") if has_uniprot else None

        filtered.append(r)
    return filtered

if __name__ == "__main__":
    folder = "/data/wyuan/workspace/pmcdata_pro/data/pattern/rna_capping"

    results = process_folder_parallel(
        folder=folder,
        process_one_folder=process_one_folder_relations,
        workers=32,
        input_name="ds_uniprotid_go_gomap_uniprotidmap.json"
    )
    results = list(results.values())
    results = [r for r in results if r is not None]   # ⭐ 过滤 None
    results = [item for sublist in results for item in sublist]