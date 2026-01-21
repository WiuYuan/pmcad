import os
import json
import re
import time
import requests


# =====================================
# UniProt search
# =====================================
def uniprot_query(query, max_sleep=30, max_retries_per_item=5, k=5):
    base_url = "https://rest.uniprot.org/uniprotkb/search"

    params = {"query": query, "format": "json", "size": k}
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
            return {"results": results[:k], "timeout": False}

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

def search_uniprot(query, k=30, max_retries_per_item=5, max_sleep=30):
    try:
        res = uniprot_query(
            query=query,
            max_sleep=max_sleep,
            max_retries_per_item=max_retries_per_item,
            k=k,
        )
        hits_raw = res.get("results") or []
    except Exception:
        hits_raw = []
    hits = []
    for rank, entry in enumerate(hits_raw, start=1):
        info = extract_uniprot_info(entry, rank)
        hits.append(
            {
                "id": info["accession"],
                "name": info["accession"],
                "description": info["description"],
                "rank": rank,
            }
        )
    return hits


# =====================================
# Extract species from relation (ALL META INCLUDED)
# =====================================
def extract_species_from_relation(rel):
    """
    Species resolution priority:
    1) entity.meta
    2) relation.context (+ context.meta)
    3) relation.components + relation.target (+ their meta)
    """

    # ---- collect relation-level species (order matters) ----
    rel_species = []

    # ---------- 1. context (+ meta) ----------
    for c in rel.get("context", []):
        # context itself
        if c.get("type") == "species" and c.get("name"):
            rel_species.append(c["name"])

        # context.meta
        for m in c.get("meta", []):
            if m.get("type") == "species" and m.get("name"):
                rel_species.append(m["name"])

    # ---------- 2. components + target (+ meta) ----------
    for field in ("components", "target"):
        for ent in rel.get(field, []):
            # entity itself
            if ent.get("type") == "species" and ent.get("name"):
                rel_species.append(ent["name"])

            # entity.meta
            for m in ent.get("meta", []):
                if m.get("type") == "species" and m.get("name"):
                    rel_species.append(m["name"])

    # ---- deduplicate but keep order ----
    seen = set()
    rel_species = [x for x in rel_species if not (x in seen or seen.add(x))]

    # ---------- entity-level resolver ----------
    def _inner(ent):
        # 1️⃣ entity.meta has highest priority
        for m in ent.get("meta", []):
            if m.get("type") == "species" and m.get("name"):
                return m["name"]

        # 2️⃣ fallback to relation-level species
        if rel_species:
            return rel_species[0]

        return None

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

    # ------------- load taxon_map --------------
    try:
        with open(os.path.join(folder, species_file), "r", encoding="utf-8") as f:
            sp_data = json.load(f)
    except Exception:
        return None, [{"type": "status", "name": f"{pmid} (species load error)"}]

    # ------------------------------------------------
    # 1️⃣ build best_species map (LLM-resolved taxonomy)
    # ------------------------------------------------
    best_species = {}  # raw name → resolved taxonomy name
    for item in sp_data.get("taxon_map", []):
        nm = item.get("name")
        best = item.get("llm_best_match")
        if nm and best:
            best_species[nm] = best.get("name")

    # ------------------------------------------------
    # 2️⃣ collect document-level species from ALL relations
    # ------------------------------------------------
    doc_species = []
    for block in relations:
        for rel in block.get("rel_from_this_sent", []):
            get_sp = extract_species_from_relation(rel)
            sp = get_sp({})  # trick: force relation-level fallback
            if sp:
                doc_species.append(sp)

    # 去重但保序
    seen = set()
    doc_species = [x for x in doc_species if not (x in seen or seen.add(x))]

    # document-level fallback species（raw name）
    doc_level_species = doc_species[0] if doc_species else None

    # ------------------------------------------------
    # 3️⃣ collect gene/protein entities
    # ------------------------------------------------
    needed = set()

    for block in relations:
        for rel in block.get("rel_from_this_sent", []):
            get_sp = extract_species_from_relation(rel)
            for field in ("components", "target", "context"):
                for ent in rel.get(field, []):
                    collect_gene_protein(ent, get_sp, needed)

    # ------------------------------------------------
    # 4️⃣ unify species with fallback
    # ------------------------------------------------
    filtered_items = []

    for nm, sp, etype in needed:
        species_final = None

        # case 1: entity/relation-level species + resolved taxonomy
        if sp and sp in best_species:
            species_final = best_species[sp]

        # case 2: entity-level species but no mapping
        elif sp:
            species_final = sp

        # case 3: fallback to document-level species
        elif doc_level_species:
            species_final = best_species.get(doc_level_species, doc_level_species)

        # else: species_final stays None (global UniProt search)

        filtered_items.append((nm, species_final, etype))

    if not filtered_items:
        out = {"pmid": pmid, "uniprot_map": []}
        with open(os.path.join(folder, output_file), "w", encoding="utf-8") as fw:
            json.dump(out, fw, ensure_ascii=False, indent=2)
        return out, [
            {
                "type": "status",
                "name": f"{pmid} (no gene/protein after species filtering)",
            }
        ]

    # ------------------------------------------------
    # 5️⃣ UniProt search
    # ------------------------------------------------
    uniprot_map = []
    judge = False

    for name, species, etype in filtered_items:
        try:
            res = uniprot_query(
                name,
                species,
                max_retries_per_item=max_retries_per_item,
                top_k=top_candidates,
            )
            hits_raw = res.get("results") or []
        except Exception:
            hits_raw = []

        hits = []
        for rank, entry in enumerate(hits_raw, start=1):
            info = extract_uniprot_info(entry, rank)
            hits.append(
                {
                    "id": info["accession"],
                    "name": info["accession"],
                    "description": info["description"],
                    "rank": rank,
                }
            )

        if hits:
            judge = True

        uniprot_map.append(
            {
                "name": name,
                "species": species,
                "entity_type": etype,
                "hits": hits,
            }
        )

    # ------------- write output --------------
    out = {
        "pmid": pmid,
        "uniprot_map": uniprot_map,
    }

    with open(os.path.join(folder, output_file), "w", encoding="utf-8") as fw:
        json.dump(out, fw, ensure_ascii=False, indent=2)

    return out, [
        {"type": "status", "name": f"{pmid}"},
        {"type": "metric", "correct": 1 if judge else 0, "total": 1},
    ]
