# src/pmcad/ontology_decompostion.py
import os
import json


def build_entity_decomposition_prompt(entity: dict) -> str:
    ent_json = json.dumps(entity, ensure_ascii=False, indent=2)

    return """
You are a biomedical entity canonicalization and decomposition system.

IMPORTANT:
You MUST follow EXACTLY the same entity ontology, naming rules, and constraints
as the original biomedical relation extraction system.


============================================================
ENTITY NAMING RULES:

gene:
  - Exact **ONE** gene symbol or ORF label (“TP53”, “J4R”, “ORF1a”).
  - MUST include species type in meta.

protein:
  - Exact **ONE** protein name (“p53 protein”, “spike glycoprotein”).
  - OR a single, well-defined protein or protein domain defined by a specific biochemical function or structural role explicitly stated in the text.
  - No accessions.
  - MUST include species type in meta.

domain:
  - A structurally or functionally defined region of a protein explicitly described in the text,
    including but not limited to:
    binding site,
    binding pocket,
    active site,
    catalytic site,
    enzymatic domain,
    interaction site,
    interaction surface,
    substrate-binding site,
    ligand-binding site,
    N-terminal domain,
    C-terminal domain.

RNA:
  - Exact **ONE** RNA entity.
  - MUST refer to a **specific, biologically instantiated RNA molecule tied to a concrete organism, virus, or genomic source** 
    (e.g., “human TP53 mRNA”).
  - Generic or class-level RNA terms that do NOT specify an exact biological instance
    (e.g., “mRNA”, “tRNA”, “rRNA”, “viral RNA”, “host mRNA”) 
    MUST NOT be annotated as RNA.
  - MUST include species type in meta.

GO:
  - Any phrase describing a molecular function, biological process or cellular component.
  - MUST NOT include numeric GO identifiers (e.g., GO:0003677 forbidden).
  - MUST with a description
  - MUST be not specific to any particular organism
  - MUST NOT include any specific species in name and description
  
chemical:
  - Exact chemical name.
  - MUST refer to a specific, concrete chemical entity

species:
  - Exact species or taxonomic name.
  - The term may correspond to any biological taxonomic rank.
  — MUST NOT include description field.

SO:
  - MUST correspond to an explicit Sequence Ontology (SO) term present in the text,
    describing a sequence feature on DNA, RNA, or protein.
  - Examples include:
      “5' cap”, “poly(A) site”, “splice acceptor site”, 
      “promoter”, “enhancer”, “exon”, “intron”, “UTR”, “open reading frame”,
      “TATA box”, “CpG island”.
  - The mention MUST describe a real sequence feature, not a general concept.
  - MUST provide a brief description copied or summarized from the ontology (SO).
  - MUST NOT include numeric accessions (SO:0000204 forbidden in extracted text).
  - If multiple SO terms appear, extract them individually as separate features.
  - MUST be not specific to any particular organism

disease:
  - name MUST be a specific, named disease entity.
  - Examples:
      “Alzheimer's disease”, “hepatocellular carcinoma”, “COVID-19”, 
      “acute myeloid leukemia”, “breast cancer”.
  - MUST NOT use DOID identifiers (DOID:1234 forbidden).
  - MUST provide a disease description (from DOID ontology).
  - The pathogen itself (such as a virus or bacterium) is not the disease. The disease is the result caused by the pathogen.
  
anatomy:
  - MUST correspond to an anatomical structure (multi-cellular or organ-level)
    explicitly mentioned in the text.
  - Examples:
      “lung”, “heart”, “nasal epithelium”, “lymph node”, 
      “skeletal muscle”, “blood vessel”, “intestine”.
  - MUST provide a UBERON-derived description.
  - MUST NOT include numerical ontology IDs (UBERON:nnnn forbidden in text).
  - MUST NOT include cell types (handled by cell_type).
  - MUST NOT include subcellular organelles (handled by location).
  
cell_type:
  - MUST correspond to a specific CL term.
  - name MUST NOT contain species words or adjectives.
    If the text says "rat stem cell", extract:
      cell_type: "stem cell"
      contexts: species "Rattus norvegicus"
  - MUST provide a CL-based description.
    
cell_line:
  - MUST correspond to a specific named cell line.
    (e.g., "HeLa", "HEK293", "Jurkat", "CHO cells").
  - name MUST be exactly the cell line name without species words.
  - MUST provide a cell-line description (from Cellosaurus/CLO-style definition).
  - Species MUST NOT appear in the name.
  - SHOULD include species as a meta entry when known or implied
    (e.g., HeLa -> Homo sapiens).

============================================================
TASK

Given ONE extracted biomedical entity, decide whether it should be kept as-is
or rewritten into a canonical form.

Decision rules:
1. If the entity is already a valid atomic concept under the above rules,
   DO NOT decompose it.
   In this case, output ONLY 'None'.
2. If the entity improperly fuses a biological PROCESS with a specific
   gene/protein/hormone name (e.g., "FSHβ transcription", "TP53 expression"),
   you MUST decompose it.

Decomposition rules:
- Output ONLY ONE JSON object representing the rewritten entity.
- Do NOT invent new biology.
- Do NOT create relations.
- Do NOT output multiple entities.

Every entity must include:
- name: the canonical entity name used to represent the concept in the schema, MUST be not equal to type.
- type: one of the allowed ontology-aligned categories
- description: a concise semantic explanation (required for GO, SO (sequence ontology), domain, anatomy, cell_type, disease; For gene, protein, species, chemical, and RNA entities: description MUST NOT be included.)
- meta: optional list of additional entity attributes (each with name, type, description)
- For gene and protein components/targets, meta SHOULD include species whenever the sentence makes it known.

============================================================
OUTPUT FORMAT (STRICT)

CASE 1 — keep:
None

CASE 2 — rewrite:
Output ONLY a JSON object:
{{
  "name": "",
  "type": "",
  "description": "",
  "meta": [
    {{
      "name": "",
      "type": ",
      "description": ""
    }}
  ]
}}
""" + f"""
============================================================
ENTITY TO PROCESS
{ent_json}
""".strip()

def iter_all_entities(data):
    for block in data.get("relations", []):
        for rel in block.get("rel_from_this_sent", []):
            for k in ("components", "targets", "contexts"):
                for ent in rel.get(k, []):
                    yield ent
                    
def postprocess_entity(ent: dict, llm) -> dict:
    prompt = build_entity_decomposition_prompt(ent)
    raw = (llm.query(prompt) or "").strip()

    # CASE 1: 明确 keep
    if raw == "None":
        return ent

    # CASE 2: rewrite
    if raw.startswith("{"):
        try:
            new_ent = json.loads(raw)
            return new_ent
        except Exception:
            return ent

    # fallback（任何异常输出都当 keep）
    return ent

def process_one_folder_entity_decomposition(
    pmid: int,
    store,
    llm,
    *,
    input_name: str,
    output_name: str,
    skip_existing: bool = False,
):
    """
    基于 PMIDStore 的 entity decomposition / canonicalization 后处理（不再使用 folder 文件系统）。

    - 从 store 读取 pmid/input_name (JSON)
    - 对 relations[*].rel_from_this_sent[*] 的 components/targets/contexts 中的 entity 调用 LLM 判断是否需要分解
      （当前仅处理 type in ["GO", "cell_type"]，保持原逻辑）
    - 写回 store: pmid/output_name
    """
    pmid = int(pmid)

    # -----------------------------
    # 0) skip_existing
    # -----------------------------
    if skip_existing and store.has(pmid, output_name):
        return None, [
            {
                "type": "status",
                "name": "skip",
                "description": f"pmid:{pmid} (already normalized)",
            },
        ]

    # -----------------------------
    # 1) load input
    # -----------------------------
    data = store.get(pmid, input_name)
    if data is None:
        return None, [
            {
                "type": "error",
                "msg": f"missing input pmid:{pmid} name:{input_name}",
            },
        ]
    if not isinstance(data, dict):
        return None, [
            {
                "type": "error",
                "msg": f"bad input type pmid:{pmid} name:{input_name}",
            },
        ]

    if not data.get("relations"):
        # 没关系，直接原样写出
        try:
            store.put(pmid, output_name, data)
            return None, [
                {
                    "type": "status",
                    "name": "success",
                    "description": f"pmid:{pmid} (no relations)",
                },
            ]
        except Exception:
            return None, [
                {
                    "type": "error",
                    "msg": f"write fail pmid:{pmid} name:{output_name}",
                },
            ]

    # -----------------------------
    # 2) normalize entities (core)
    # -----------------------------
    total_ents = 0
    rewritten_ents = 0

    for block in data.get("relations", []):
        for rel in block.get("rel_from_this_sent", []):
            for k in ("components", "targets", "contexts"):
                ents = rel.get(k, [])
                if not isinstance(ents, list):
                    continue

                new_ents = []
                for ent in ents:
                    ent_type = ent.get("type")
                    if ent_type not in ["GO", "cell_type"]:
                        new_ents.append(ent)
                        continue

                    total_ents += 1
                    try:
                        new_ent = postprocess_entity(ent, llm)
                        if new_ent != ent:
                            rewritten_ents += 1
                        new_ents.append(new_ent)
                    except Exception:
                        new_ents.append(ent)

                rel[k] = new_ents

    # -----------------------------
    # 3) 写回 store
    # -----------------------------
    data.setdefault("_entity_normalization_report", {})
    data["_entity_normalization_report"].update(
        {
            "pmid": pmid,
            "total_entities": total_ents,
            "rewritten_entities": rewritten_ents,
            "rewrite_ratio": (rewritten_ents / total_ents) if total_ents else 0.0,
            "mode": "llm_entity_decomposition",
            "input_name": input_name,
            "output_name": output_name,
        }
    )

    try:
        store.put(pmid, output_name, data)
    except Exception as e:
        return None, [
            {"type": "error", "msg": f"write fail pmid:{pmid} err:{repr(e)}"},
        ]

    status_name = "maintained" if rewritten_ents == 0 else "success"
    return None, [
        {
            "type": "status",
            "name": status_name,
            "description": f"pmid:{pmid}",
        },
        {
            "type": "metric",
            "name": "rewrite",
            "correct": rewritten_ents,
            "total": total_ents,
        },
    ]