import os
import json
import re
import time
import requests

# =====================================
# UniProt search
# =====================================
def uniprot_query(term, organism=None, max_sleep=30, max_retries_per_item=5, top_k=5):
    base_url = "https://rest.uniprot.org/uniprotkb/search"

    term = re.sub(r"\([^)]*\)", "", str(term))
    term = re.sub(r"\[[^\]]*\]", "", term)

    if organism:
        q = f"{term} {organism}"
    else:
        q = term

    params = {"query": q, "format": "json", "size": top_k}
    sleep_time = 1

    for _ in range(max_retries_per_item):
        try:
            r = requests.get(base_url, params=params, timeout=10)

            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(sleep_time)
                sleep_time = min(sleep_time * 2, max_sleep)
                continue

            if r.status_code != 200:
                time.sleep(sleep_time)
                sleep_time = min(sleep_time * 2, max_sleep)
                continue

            data = r.json()
            results = data.get("results") or []
            return {"results": results[:top_k], "timeout": False}

        except Exception:
            time.sleep(sleep_time)
            sleep_time = min(sleep_time * 2, max_sleep)

    return {"results": None, "timeout": True}


# =====================================
# Extract UniProt info
# =====================================
def extract_uniprot_info(entry, rank):
    acc = entry.get("primaryAccession")

    organism = ""
    org_data = entry.get("organism", {})
    if org_data.get("scientificName"):
        organism = org_data["scientificName"]
    if org_data.get("commonName"):
        organism += f" ({org_data['commonName']})"
    if org_data.get("mnemonic"):
        organism += f" [{org_data['mnemonic']}]"

    names = []
    aliases = []

    rec = entry.get("proteinDescription", {}).get("recommendedName")
    if rec and "fullName" in rec:
        names.append(rec["fullName"]["value"])

    for alt in entry.get("proteinDescription", {}).get("alternativeNames", []):
        if "fullName" in alt:
            aliases.append(alt["fullName"]["value"])
        if "shortName" in alt:
            aliases.append(alt["shortName"]["value"])

    genes = []
    for g in entry.get("genes", []):
        if g.get("geneName", {}).get("value"):
            genes.append(g["geneName"]["value"])

    desc = []
    if names:
        desc.append("; ".join(names))
    if aliases:
        desc.append("ALIASES: " + "; ".join(sorted(set(aliases))))
    if genes:
        desc.append("GENES: " + "; ".join(sorted(set(genes))))
    if organism:
        desc.append("ORGANISM: " + organism)

    return {
        "accession": acc,
        "description": " | ".join(desc),
        "rank": rank,
    }


# =====================================
# Extract species from entity.meta
# =====================================
def extract_species_from_relation(rel):
    ctx_species = None
    for c in rel.get("context", []):
        if c.get("type") == "species":
            ctx_species = c.get("name")

    def _inner(ent):
        # meta first
        for m in ent.get("meta", []):
            if m.get("type") == "species":
                return m.get("name")
        return ctx_species

    return _inner


# =====================================
# Collect gene/protein names
# =====================================
def collect_gene_protein(ent, get_species, needed_keys):
    if ent.get("type") in ("gene", "protein"):
        nm = ent.get("name")
        if nm:
            sp = get_species(ent)
            needed_keys.add((nm, sp, ent.get("type")))

    for m in ent.get("meta", []):
        if m.get("type") in ("gene", "protein"):
            nm = m.get("name")
            if nm:
                sp = get_species(m)
                needed_keys.add((nm, sp, m.get("type")))


# =====================================
# NEW: MAIN FUNCTION that reads 2 files
# =====================================
def process_one_folder_get_uniprot_id(
    folder,
    relation_file,
    species_file,
    output_file,
    top_candidates=5,
    max_retries_per_item=5,
):
    pmid = os.path.basename(folder)

    # ------------- load relation.json --------------
    try:
        with open(os.path.join(folder, relation_file), "r", encoding="utf-8") as f:
            rel_data = json.load(f)
    except Exception:
        return None, [{"type": "status", "name": f"{pmid} (relation load error)"}]

    relations = rel_data.get("relations", [])

    # ------------- load species_mapping --------------
    try:
        with open(os.path.join(folder, species_file), "r", encoding="utf-8") as f:
            sp_data = json.load(f)
    except Exception:
        return None, [{"type": "status", "name": f"{pmid} (species load error)"}]

    best_species = {}  # name → best_match_name

    for item in sp_data.get("species_mapping", []):
        nm = item["name"]
        best = item.get("llm_best_match")
        if best:
            best_species[nm] = best["name"]

    # ------------- collect gene/protein --------------
    needed = set()

    for block in relations:
        for rel in block.get("rel_from_this_sent", []):
            get_sp = extract_species_from_relation(rel)
            for field in ("components", "target", "context"):
                for ent in rel.get(field, []):
                    collect_gene_protein(ent, get_sp, needed)

    # ------------- unify species via best match --------------
    filtered_items = []

    for (nm, sp, etype) in needed:
        
        # case 1: species has llm_best_match → use matched taxonomy name
        if sp in best_species:
            species_final = best_species[sp]
        
        # case 2: species missing or not matched → fallback to global search
        else:
            species_final = None   # means uniprot_query(term) will ignore species

        filtered_items.append((nm, species_final, etype))

    if not filtered_items:
        out = {"pmid": pmid, "uniprot_mapping": []}
        with open(os.path.join(folder, output_file), "w", encoding="utf-8") as fw:
            json.dump(out, fw, ensure_ascii=False, indent=2)
        return out, [{"type": "status", "name": f"{pmid} (no gene/protein after species filtering)"}]

    # ------------- UniProt search --------------
    uniprot_mapping = []
    judge = False

    for (name, species, etype) in filtered_items:
        try:
            res = uniprot_query(name, species, max_retries_per_item=max_retries_per_item, top_k=top_candidates)
            hits_raw = res.get("results") or []
        except Exception:
            hits_raw = []

        hits = []
        for rank, entry in enumerate(hits_raw, start=1):
            info = extract_uniprot_info(entry, rank)
            hits.append({
                "id": info["accession"],
                "name": info["accession"],
                "description": info["description"],
                "rank": rank,
            })

        if hits:
            judge = True

        uniprot_mapping.append({
            "name": name,
            "species": species,
            "entity_type": etype,
            "hits": hits,
        })

    # ------------- write output --------------
    out = {
        "pmid": pmid,
        "uniprot_mapping": uniprot_mapping,
    }

    with open(os.path.join(folder, output_file), "w", encoding="utf-8") as fw:
        json.dump(out, fw, ensure_ascii=False, indent=2)

    return out, [
        {"type": "status", "name": f"{pmid}"},
        {"type": "metric", "correct": 1 if judge else 0, "total": 1},
    ]