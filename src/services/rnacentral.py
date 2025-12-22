import os
import json
import time
import requests


# =====================================
# RNAcentral search (EBI Search API)
# =====================================
def rnacentral_query(
    term,
    organism=None,
    max_sleep=30,
    max_retries_per_item=5,
    top_k=5,
):
    base_url = "https://www.ebi.ac.uk/ebisearch/ws/rest/rnacentral"

    q = str(term).strip()
    if organism:
        q = f'{q} "{organism}"'

    params = {
        "query": q,
        "size": top_k,
        "format": "json",
        # 🔑 explicitly request annotations
        "fields": "description,species,gene,rna_type",
    }

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
            return {"results": data.get("entries", []), "timeout": False}

        except Exception:
            time.sleep(sleep_time)
            sleep_time = min(sleep_time * 2, max_sleep)

    return {"results": None, "timeout": True}


# =====================================
# Build unified RNAcentral description
# =====================================
def build_rnacentral_description(entry):
    fields = entry.get("fields", {})

    parts = []

    if fields.get("description"):
        parts.append(" | ".join(fields["description"]))

    if fields.get("gene"):
        parts.append("GENE: " + ", ".join(fields["gene"]))

    if fields.get("rna_type"):
        parts.append("TYPE: " + ", ".join(fields["rna_type"]))

    if fields.get("species"):
        parts.append("SPECIES: " + ", ".join(fields["species"]))

    return " | ".join(parts)


# =====================================
# Extract species from relation (same logic as UniProt)
# =====================================
def extract_species_from_relation(rel):
    rel_species = []

    # contexts + meta
    for c in rel.get("contexts", []):
        if c.get("type") == "species" and c.get("name"):
            rel_species.append(c["name"])
        for m in c.get("meta", []):
            if m.get("type") == "species" and m.get("name"):
                rel_species.append(m["name"])

    # components + targets + meta
    for field in ("components", "targets"):
        for ent in rel.get(field, []):
            if ent.get("type") == "species" and ent.get("name"):
                rel_species.append(ent["name"])
            for m in ent.get("meta", []):
                if m.get("type") == "species" and m.get("name"):
                    rel_species.append(m["name"])

    # dedup keep order
    seen = set()
    rel_species = [x for x in rel_species if not (x in seen or seen.add(x))]

    def _inner(ent):
        # entity.meta first
        for m in ent.get("meta", []):
            if m.get("type") == "species" and m.get("name"):
                return m["name"]
        if rel_species:
            return rel_species[0]
        return None

    return _inner


# =====================================
# MAIN: RNAcentral mapping (UniProt-aligned)
# =====================================
def process_one_folder_get_rnacentral_id(
    folder,
    relation_file,
    species_file,
    output_file,
    top_candidates=5,
    max_retries_per_item=5,
):
    pmid = os.path.basename(folder)

    # ---------- load relation ----------
    with open(os.path.join(folder, relation_file), "r", encoding="utf-8") as f:
        rel_data = json.load(f)

    relations = rel_data.get("relations", [])
    abstract = rel_data.get("abstract", "")

    # ---------- load taxon_map ----------
    with open(os.path.join(folder, species_file), "r", encoding="utf-8") as f:
        sp_data = json.load(f)

    # ---------- build best_species map ----------
    best_species = {}
    for item in sp_data.get("taxon_map", []):
        nm = item.get("name")
        best = item.get("llm_best_match")
        if nm and best:
            best_species[nm] = best.get("name")

    # ---------- collect document-level species ----------
    doc_species = []
    for block in relations:
        for rel in block.get("rel_from_this_sent", []):
            get_sp = extract_species_from_relation(rel)
            sp = get_sp({})
            if sp:
                doc_species.append(sp)

    seen = set()
    doc_species = [x for x in doc_species if not (x in seen or seen.add(x))]
    doc_level_species = doc_species[0] if doc_species else None

    # ---------- collect RNA entities ----------
    needed = set()

    for block in relations:
        for rel in block.get("rel_from_this_sent", []):
            get_sp = extract_species_from_relation(rel)
            for field in ("components", "targets", "contexts"):
                for ent in rel.get(field, []):
                    if ent.get("type") == "RNA" and ent.get("name"):
                        needed.add((ent["name"], get_sp(ent)))
                    for m in ent.get("meta", []):
                        if m.get("type") == "RNA" and m.get("name"):
                            needed.add((m["name"], get_sp(m)))

    if not needed:
        out = {"pmid": pmid, "rnacentral_map": []}
        with open(os.path.join(folder, output_file), "w") as fw:
            json.dump(out, fw, ensure_ascii=False, indent=2)
        return out, [{"type": "status", "name": f"{pmid} (no RNA)"}]

    # ---------- unify species ----------
    normalized = []
    for name, sp in needed:
        if sp and sp in best_species:
            normalized.append((name, best_species[sp]))
        elif sp:
            normalized.append((name, sp))
        elif doc_level_species:
            normalized.append((name, best_species.get(doc_level_species, doc_level_species)))
        else:
            normalized.append((name, None))

    # ---------- RNAcentral search ----------
    rnacentral_map = []
    judge = 0
    total = 0

    for name, species in sorted(set(normalized)):
        total += 1
        if not species:
            rnacentral_map.append(
                {
                    "name": name,
                    "species": None,
                    "hits": [],
                }
            )
            continue
        res = rnacentral_query(
            name,
            organism=species,
            max_retries_per_item=max_retries_per_item,
            top_k=top_candidates,
        )
        hits_raw = res.get("results") or []

        hits = []
        for rank, entry in enumerate(hits_raw, start=1):
            hits.append(
                {
                    "id": entry.get("id"),
                    "name": entry.get("id"),
                    "description": build_rnacentral_description(entry),
                    "rank": rank,
                }
            )

        if hits:
            judge += 1

        rnacentral_map.append(
            {
                "name": name,
                "species": species,
                "hits": hits,
            }
        )

    out = {
        "pmid": pmid,
        "abstract": abstract,
        "rnacentral_map": rnacentral_map,
    }

    with open(os.path.join(folder, output_file), "w", encoding="utf-8") as fw:
        json.dump(out, fw, ensure_ascii=False, indent=2)

    return out, [
        {"type": "status", "name": f"{pmid}"},
        {"type": "metric", "correct": int(judge), "total": total},
    ]